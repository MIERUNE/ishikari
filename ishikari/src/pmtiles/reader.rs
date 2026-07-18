//! PMTiles archive reader over an abstract storage interface.

use std::{future::Future, hash::Hash, sync::Arc};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    singleflight::{Flight, Follower, LeaderGuard, SingleFlight},
};

use super::{
    cache::{ArchiveBootstrap, ArchiveCache, LeafCacheKey},
    format::{Compression, Directory, DirectoryEntry, HEADER_SIZE, Header, TileData, TileId},
    metadata::Metadata,
};

const INITIAL_BYTES_LEN: usize = 16_384;
const MAX_DIRECTORY_DEPTH: u8 = 64;

/// Errors returned by PMTiles storage reads.
#[derive(Clone, Debug, Error)]
pub enum StorageError {
    #[error("archive not found")]
    NotFound,
    /// A backend read timed out. Typed so the service layer maps it to a 504
    /// without matching on the message string.
    #[error("{0}")]
    Timeout(String),
    /// Backend fetch admission is saturated; the request is shed. Typed so the
    /// service layer maps it to a 503 without matching on the message string.
    #[error("{0}")]
    Overload(String),
    #[error("{0}")]
    Message(String),
}

/// Bootstrap bytes transferred from a peer, optionally including metadata.
pub struct BootstrapTransfer {
    pub bootstrap: Bytes,
    pub metadata: Option<Bytes>,
}

/// Storage capabilities required by the PMTiles reader.
pub trait Storage: Send + Sync {
    /// Reads a range of bytes for the given PMTiles archive.
    fn read_range<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> impl Future<Output = Result<Bytes, StorageError>> + Send + 'a;

    /// Fetches archive bootstrap bytes from a peer, optionally including metadata.
    fn fetch_bootstrap_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        include_metadata: bool,
    ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a;

    /// Fetches leaf bytes.
    fn fetch_leaf_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        offset: u64,
        length: usize,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a;
}

/// PMTiles archive reader backed by shared chunked range reads and index caches.
pub struct Reader<R> {
    pub archive_cache: ArchiveCache,
    storage: R,
    bootstrap_inflight: SingleFlight<TilesetId, ReaderFlightError>,
    metadata_inflight: SingleFlight<TilesetId, ReaderFlightError>,
    leaf_inflight: SingleFlight<LeafCacheKey, ReaderFlightError>,
}

#[derive(Clone)]
struct EntryResolution {
    entry: DirectoryEntry,
    leaf_ranges: Vec<ArchiveRange>,
}

/// Cloneable error snapshot shared with single-flight followers. Storage errors
/// remain typed because the service layer uses them to distinguish timeouts;
/// other reader errors retain their complete printable context.
#[derive(Clone, Debug)]
struct ReaderFlightError {
    message: Arc<str>,
    storage: Option<StorageError>,
}

impl ReaderFlightError {
    fn capture(error: &anyhow::Error) -> Self {
        let storage = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<StorageError>())
            .cloned();
        let message = if storage.is_some() {
            error.to_string()
        } else {
            format!("{error:#}")
        };
        Self {
            message: Arc::from(message),
            storage,
        }
    }

    fn into_error(self) -> anyhow::Error {
        let Some(storage) = self.storage else {
            return anyhow!(self.message.to_string());
        };
        if self.message.as_ref() == storage.to_string() {
            anyhow::Error::new(storage)
        } else {
            anyhow::Error::new(storage).context(self.message.to_string())
        }
    }
}

fn complete_reader_flight<K, T>(
    guard: LeaderGuard<K, ReaderFlightError>,
    result: Result<T>,
) -> Result<T>
where
    K: Eq + Hash,
{
    if let Err(error) = &result {
        guard.complete_with_error(ReaderFlightError::capture(error));
    }
    result
}

async fn wait_reader_flight(follower: Follower<ReaderFlightError>) -> Result<()> {
    if let Some(error) = follower.wait().await {
        return Err(error.into_error());
    }
    Ok(())
}

/// Byte range occupied by one tile in a PMTiles archive.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TileLocation {
    pub offset: u64,
    pub length: u32,
    pub archive_len: u64,
}

/// A byte range consulted while resolving a tile directory entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ArchiveRange {
    pub offset: u64,
    pub length: u32,
}

/// Logical PMTiles reads needed to resolve and fetch one tile.
#[cfg(feature = "simulator-support")]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TileAccessPlan {
    pub bootstrap: ArchiveRange,
    pub leaves: Vec<ArchiveRange>,
    pub tile: TileLocation,
}

impl<S> Reader<S>
where
    S: Storage,
{
    /// Creates a PMTiles archive reader over the provided storage implementation.
    pub fn new(storage: S) -> Result<Self> {
        Ok(Self {
            archive_cache: ArchiveCache::new(),
            storage,
            bootstrap_inflight: SingleFlight::default(),
            metadata_inflight: SingleFlight::default(),
            leaf_inflight: SingleFlight::default(),
        })
    }

    /// Returns a reference to the underlying storage implementation.
    pub fn storage(&self) -> &S {
        &self.storage
    }

    /// Returns a tile by PMTiles tile id, fetching missing archive chunks as needed.
    pub async fn get_tile(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>> {
        let Some((location, header, _)) = self.resolve_tile_location(tileset_id, tile_id).await?
        else {
            return Ok(None);
        };
        let bytes = self
            .storage
            .read_range(
                tileset_id,
                location.offset,
                location.length as usize,
                Some(location.archive_len),
            )
            .await
            .context("failed to read PMTiles tile bytes")?;

        tracing::debug!(
            tileset_id = %tileset_id,
            tile_offset = location.offset,
            tile_length = location.length,
            "resolved tile bytes"
        );

        Ok(Some(TileData {
            bytes,
            content_type: header.tile_type.content_type(),
            content_encoding: header.tile_compression.content_encoding(),
        }))
    }

    /// Resolves a tile to its archive byte range without reading its payload.
    pub async fn locate_tile(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileLocation>> {
        Ok(self
            .resolve_tile_location(tileset_id, tile_id)
            .await?
            .map(|(location, _, _)| location))
    }

    /// Returns the logical archive reads needed by the modeled simulator.
    #[cfg(feature = "simulator-support")]
    pub async fn plan_tile_access(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileAccessPlan>> {
        Ok(self
            .resolve_tile_location(tileset_id, tile_id)
            .await?
            .map(|(tile, _, leaves)| TileAccessPlan {
                bootstrap: ArchiveRange {
                    offset: 0,
                    length: INITIAL_BYTES_LEN as u32,
                },
                leaves,
                tile,
            }))
    }

    async fn resolve_tile_location(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<(TileLocation, Header, Vec<ArchiveRange>)>> {
        let tile_id = TileId::new(tile_id)?;
        let Some(archive) = self.load_bootstrap(tileset_id).await? else {
            return Ok(None);
        };
        let Some(resolution) = self
            .resolve_entry(tileset_id, &archive.header, archive.root, tile_id)
            .await?
        else {
            return Ok(None);
        };
        let offset = checked_section_offset(
            "tile data",
            archive.header.data_offset,
            archive.header.data_length,
            resolution.entry.offset,
            u64::from(resolution.entry.length),
        )?;
        Ok(Some((
            TileLocation {
                offset,
                length: resolution.entry.length,
                archive_len: archive_end(&archive.header),
            },
            archive.header,
            resolution.leaf_ranges,
        )))
    }

    /// Returns the parsed PMTiles header for a tileset.
    pub async fn header(self: &Arc<Self>, tileset_id: &TilesetId) -> Result<Option<Header>> {
        let Some(archive) = self.load_bootstrap(tileset_id).await? else {
            return Ok(None);
        };
        Ok(Some(archive.header))
    }

    /// Returns archive metadata if present.
    pub async fn metadata(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<Arc<Metadata>>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id)
                && let Some(metadata) = archive.metadata
            {
                return Ok(Some(metadata));
            }
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.metadata_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self.load_metadata_uncached(tileset_id).await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_metadata_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<Arc<Metadata>>> {
        match self.storage.fetch_bootstrap_bytes(tileset_id, true).await {
            Ok(Some(transfer)) => {
                let mut archive = decode_bootstrap_bytes(transfer.bootstrap)
                    .context("failed to decode bootstrap from peer")?;
                if let Some(metadata_bytes) = transfer.metadata {
                    let metadata = Arc::new(
                        parse_metadata_bytes(&archive.header, metadata_bytes)
                            .context("failed to decode metadata from peer")?,
                    );
                    archive.metadata = Some(metadata.clone());
                    self.archive_cache.put(tileset_id, archive);
                    return Ok(Some(metadata));
                }
                self.archive_cache.put(tileset_id, archive);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    tileset_id = %tileset_id,
                    error = %error,
                    "bootstrap+metadata forward failed; falling back"
                );
            }
        }

        let Some(archive) = self.load_bootstrap_local(tileset_id).await? else {
            return Ok(None);
        };
        let metadata = Arc::new(
            self.load_metadata_from_backend(tileset_id, &archive.header)
                .await?,
        );
        self.archive_cache
            .put_metadata(tileset_id, metadata.clone());
        Ok(Some(metadata))
    }

    /// Loads a routed archive bootstrap, reusing a peer before falling back to backend reads.
    async fn load_bootstrap(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id) {
                return Ok(Some(archive));
            }
            // Skip the peer forward and backend read for an archive we just found
            // absent (short TTL), so enumerating a missing tileset cannot amplify
            // into unbounded peer round-trips and object-store probes.
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.bootstrap_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self.load_bootstrap_uncached(tileset_id).await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_bootstrap_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        match self.storage.fetch_bootstrap_bytes(tileset_id, false).await {
            Ok(Some(transfer)) => {
                let archive = decode_bootstrap_bytes(transfer.bootstrap)
                    .context("failed to decode bootstrap from peer")?;
                self.archive_cache.put(tileset_id, archive.clone());
                return Ok(Some(archive));
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    tileset_id = %tileset_id,
                    error = %error,
                    "bootstrap forward failed; falling back"
                );
            }
        }

        self.load_bootstrap_local_uncached(tileset_id).await
    }

    /// Loads or reuses the cached header/root bootstrap from local backend storage.
    pub async fn load_bootstrap_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        loop {
            if let Some(archive) = self.archive_cache.get(tileset_id) {
                return Ok(Some(archive));
            }
            if self.archive_cache.is_known_absent(tileset_id) {
                return Ok(None);
            }
            match self.bootstrap_inflight.begin(tileset_id.clone()) {
                Flight::Leader(guard) => {
                    let result = self.load_bootstrap_local_uncached(tileset_id).await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_bootstrap_local_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        let initial_bytes = match self
            .storage
            .read_range(tileset_id, 0, INITIAL_BYTES_LEN, None)
            .await
        {
            Ok(bytes) => bytes,
            // Authoritative absence (shared object storage): remember it briefly
            // so repeat/burst lookups for this missing archive don't re-probe.
            Err(StorageError::NotFound) => {
                self.archive_cache.mark_absent(tileset_id);
                return Ok(None);
            }
            Err(error) => return Err(error).context("failed to read PMTiles header"),
        };

        if initial_bytes.len() < HEADER_SIZE {
            bail!("PMTiles archive header is truncated");
        }

        let header = Header::parse(initial_bytes.slice(..HEADER_SIZE))?;
        debug!(
            tileset_id = %tileset_id,
            version = header.version,
            root_offset = header.root_offset,
            root_length = header.root_length,
            metadata_offset = header.metadata_offset,
            metadata_length = header.metadata_length,
            leaf_offset = header.leaf_offset,
            leaf_length = header.leaf_length,
            data_offset = header.data_offset,
            data_length = header.data_length,
            "parsed PMTiles header"
        );
        let root_start = header.root_offset as usize;
        let root_end = root_start
            .checked_add(header.root_length as usize)
            .context("invalid root directory range")?;
        if root_end > initial_bytes.len() {
            bail!("PMTiles root directory must fit in the initial read window");
        }
        let root_bytes = initial_bytes.slice(root_start..root_end);
        let root = Arc::new(Directory::parse(header.internal_compression, root_bytes)?);
        let archive = ArchiveBootstrap::new(header, root, None);
        self.archive_cache.put(tileset_id, archive.clone());

        Ok(Some(archive))
    }

    /// Loads local raw bootstrap bytes for internal forwarding, optionally including metadata.
    pub async fn load_bootstrap_bytes_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>> {
        let Some(archive) = self.load_bootstrap_local(tileset_id).await? else {
            return Ok(None);
        };
        let end = archive_end(&archive.header);

        let bootstrap = self
            .storage
            .read_range(tileset_id, 0, INITIAL_BYTES_LEN, Some(end))
            .await
            .context("failed to read archive bootstrap bytes")?;

        let metadata = if include_metadata && archive.header.metadata_length > 0 {
            Some(
                self.storage
                    .read_range(
                        tileset_id,
                        archive.header.metadata_offset,
                        usize::try_from(archive.header.metadata_length)
                            .context("PMTiles metadata length exceeds usize")?,
                        Some(end),
                    )
                    .await
                    .context("failed to read PMTiles metadata")?,
            )
        } else {
            None
        };

        Ok(Some(BootstrapTransfer {
            bootstrap,
            metadata,
        }))
    }

    /// Resolves a PMTiles tile id to the archive entry that stores its bytes.
    async fn resolve_entry(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        header: &Header,
        directory: Arc<Directory>,
        tile_id: TileId,
    ) -> Result<Option<EntryResolution>> {
        self.resolve_in_directory(tileset_id, header, directory, tile_id, 0)
            .await
    }

    /// Recursively resolves a tile id within the current directory tree.
    async fn resolve_in_directory(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        header: &Header,
        directory: Arc<Directory>,
        tile_id: TileId,
        depth: u8,
    ) -> Result<Option<EntryResolution>> {
        let Some((_, entry)) = directory.find_tile_id(tile_id) else {
            return Ok(None);
        };
        let entry = entry.clone();

        if entry.is_leaf() {
            if depth >= MAX_DIRECTORY_DEPTH {
                bail!("PMTiles directory depth exceeds {MAX_DIRECTORY_DEPTH}");
            }

            let absolute_offset = checked_section_offset(
                "leaf directory",
                header.leaf_offset,
                header.leaf_length,
                entry.offset,
                u64::from(entry.length),
            )?;
            let child = self
                .load_leaf_directory(
                    tileset_id,
                    absolute_offset,
                    entry.length as usize,
                    header.internal_compression,
                    archive_end(header),
                )
                .await?;

            let mut resolution =
                Box::pin(self.resolve_in_directory(tileset_id, header, child, tile_id, depth + 1))
                    .await?;
            if let Some(resolution) = &mut resolution {
                resolution.leaf_ranges.insert(
                    0,
                    ArchiveRange {
                        offset: absolute_offset,
                        length: entry.length,
                    },
                );
            }
            return Ok(resolution);
        }

        Ok(Some(EntryResolution {
            entry,
            leaf_ranges: Vec::new(),
        }))
    }

    /// Loads a routed leaf directory from the tileset owner, falling back to local backend reads.
    async fn load_leaf_directory(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_end: u64,
    ) -> Result<Arc<Directory>> {
        let leaf_key = LeafCacheKey::new(tileset_id, offset);
        loop {
            if let Some(directory) = self.archive_cache.get_leaf(&leaf_key) {
                return Ok(directory);
            }
            match self.leaf_inflight.begin(leaf_key.clone()) {
                Flight::Leader(guard) => {
                    let result = self
                        .load_leaf_directory_uncached(
                            tileset_id,
                            leaf_key,
                            offset,
                            length,
                            compression,
                            archive_end,
                        )
                        .await;
                    return complete_reader_flight(guard, result);
                }
                Flight::Follower(follower) => {
                    wait_reader_flight(follower).await?;
                }
            }
        }
    }

    async fn load_leaf_directory_uncached(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        leaf_key: LeafCacheKey,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_end: u64,
    ) -> Result<Arc<Directory>> {
        match self
            .storage
            .fetch_leaf_bytes(tileset_id, offset, length)
            .await
        {
            Ok(Some(body)) => {
                let directory = Directory::parse(compression, body)
                    .context("failed to decode leaf directory from peer")?;
                let directory = Arc::new(directory);
                self.archive_cache
                    .put_leaf(leaf_key.clone(), directory.clone());
                return Ok(directory);
            }
            Ok(None) => {}
            Err(error) => {
                warn!(
                    tileset_id = %tileset_id,
                    offset = offset,
                    error = %error,
                    "leaf forward failed; falling back"
                );
            }
        }

        let directory = Arc::new(
            self.read_directory_from_backend(tileset_id, offset, length, compression, archive_end)
                .await?,
        );
        self.archive_cache.put_leaf(leaf_key, directory.clone());
        Ok(directory)
    }

    /// Loads raw PMTiles leaf bytes from local storage for internal requests.
    pub async fn load_leaf_bytes_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>> {
        let Some(archive) = self.load_bootstrap_local(tileset_id).await? else {
            return Ok(None);
        };
        let leaf = self
            .storage
            .read_range(
                tileset_id,
                offset,
                length,
                Some(archive_end(&archive.header)),
            )
            .await
            .context("failed to read leaf bytes")?;
        Ok(Some(leaf))
    }

    /// Reads and decodes a PMTiles directory block from local backend storage.
    async fn read_directory_from_backend(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        offset: u64,
        length: usize,
        compression: Compression,
        archive_end: u64,
    ) -> Result<Directory> {
        let bytes = self
            .storage
            .read_range(tileset_id, offset, length, Some(archive_end))
            .await
            .context("failed to read directory")?;
        Directory::parse(compression, bytes)
    }

    /// Loads and decodes the metadata section for a tileset from backend storage.
    async fn load_metadata_from_backend(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
        header: &Header,
    ) -> Result<Metadata> {
        if header.metadata_length == 0 {
            return Ok(Metadata::default());
        }
        let bytes = self
            .storage
            .read_range(
                tileset_id,
                header.metadata_offset,
                usize::try_from(header.metadata_length)
                    .context("PMTiles metadata length exceeds usize")?,
                Some(archive_end(header)),
            )
            .await
            .context("failed to read PMTiles metadata")?;
        let metadata = super::format::decompress_bytes(header.internal_compression, bytes)?;
        serde_json::from_slice::<Metadata>(&metadata)
            .context("failed to parse PMTiles metadata JSON")
    }
}

/// Returns the exclusive end offset of the PMTiles archive contents.
fn archive_end(header: &Header) -> u64 {
    let root_end = header.root_offset.saturating_add(header.root_length);
    let metadata_end = header
        .metadata_offset
        .saturating_add(header.metadata_length);
    let leaf_end = header.leaf_offset.saturating_add(header.leaf_length);
    let data_end = header.data_offset.saturating_add(header.data_length);
    root_end.max(metadata_end).max(leaf_end).max(data_end)
}

fn checked_section_offset(
    name: &str,
    section_offset: u64,
    section_length: u64,
    relative_offset: u64,
    length: u64,
) -> Result<u64> {
    let relative_end = relative_offset
        .checked_add(length)
        .with_context(|| format!("PMTiles {name} entry range overflows u64"))?;
    if relative_end > section_length {
        bail!(
            "PMTiles {name} entry range {relative_offset}..{relative_end} exceeds section length {section_length}"
        );
    }
    section_offset
        .checked_add(relative_offset)
        .with_context(|| format!("PMTiles {name} absolute offset overflows u64"))
}

/// Decodes archive bootstrap bytes from a peer into a cached bootstrap.
fn decode_bootstrap_bytes(body: Bytes) -> Result<ArchiveBootstrap> {
    if body.len() < HEADER_SIZE {
        bail!("bootstrap transfer header is truncated");
    }

    let header = Header::parse(body.slice(..HEADER_SIZE))?;
    let root_start = header.root_offset as usize;
    let root_end = root_start
        .checked_add(header.root_length as usize)
        .context("invalid root directory range")?;
    if root_end > body.len() {
        bail!("bootstrap transfer root exceeds bootstrap bytes");
    }
    let root = Arc::new(Directory::parse(
        header.internal_compression,
        body.slice(root_start..root_end),
    )?);

    Ok(ArchiveBootstrap::new(header, root, None))
}

/// Parses raw PMTiles metadata bytes using the archive's internal compression.
fn parse_metadata_bytes(header: &Header, bytes: Bytes) -> Result<Metadata> {
    let metadata = super::format::decompress_bytes(header.internal_compression, bytes)?;
    serde_json::from_slice::<Metadata>(&metadata).context("failed to parse PMTiles metadata JSON")
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use anyhow::Result;
    use bytes::Bytes;

    use super::{BootstrapTransfer, Reader, Storage, StorageError, checked_section_offset};
    use crate::interned::TilesetId;

    struct SlowFailingStorage {
        reads: Arc<AtomicUsize>,
    }

    impl Storage for SlowFailingStorage {
        async fn read_range<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _start: u64,
            _length: usize,
            _archive_len: Option<u64>,
        ) -> Result<Bytes, StorageError> {
            self.reads.fetch_add(1, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            Err(StorageError::Timeout("injected timeout".into()))
        }

        fn fetch_bootstrap_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _include_metadata: bool,
        ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a {
            std::future::ready(Ok(None))
        }

        fn fetch_leaf_bytes<'a>(
            &'a self,
            _tileset_id: &'a TilesetId,
            _offset: u64,
            _length: usize,
        ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a {
            std::future::ready(Ok(None))
        }
    }

    #[test]
    fn section_entry_must_stay_within_declared_range() {
        assert_eq!(
            checked_section_offset("tile data", 100, 50, 20, 10).unwrap(),
            120
        );
        assert!(
            checked_section_offset("tile data", 100, 50, 45, 10)
                .unwrap_err()
                .to_string()
                .contains("exceeds section length")
        );
        assert!(
            checked_section_offset("tile data", 100, u64::MAX, u64::MAX, 1)
                .unwrap_err()
                .to_string()
                .contains("overflows")
        );
    }

    #[tokio::test]
    async fn concurrent_bootstrap_failures_share_one_backend_attempt() {
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = Arc::new(
            Reader::new(SlowFailingStorage {
                reads: Arc::clone(&reads),
            })
            .expect("reader"),
        );
        let tileset_id = TilesetId::new_unchecked("failing/archive");

        let (first, second) = tokio::join!(
            reader.load_bootstrap_local(&tileset_id),
            reader.load_bootstrap_local(&tileset_id)
        );

        let first_error = match first {
            Err(error) => error,
            Ok(_) => panic!("first request unexpectedly succeeded"),
        };
        let second_error = match second {
            Err(error) => error,
            Ok(_) => panic!("second request unexpectedly succeeded"),
        };
        assert!(format!("{first_error:#}").contains("injected timeout"));
        assert!(format!("{second_error:#}").contains("injected timeout"));
        assert!(matches!(
            first_error.downcast_ref::<StorageError>(),
            Some(StorageError::Timeout(_))
        ));
        assert!(matches!(
            second_error.downcast_ref::<StorageError>(),
            Some(StorageError::Timeout(_))
        ));
        assert_eq!(reads.load(Ordering::SeqCst), 1);
    }
}

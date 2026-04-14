//! PMTiles archive reader over an abstract storage interface.

use std::future::Future;
use std::sync::Arc;

use anyhow::{Context, Result, bail};
use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, warn};

use crate::interned::TilesetId;

use super::{
    cache::{ArchiveBootstrap, ArchiveCache, LeafCacheKey},
    format::{Compression, Directory, DirectoryEntry, Header, TileData, TileId},
    metadata::Metadata,
};

const HEADER_SIZE: usize = 127;
const INITIAL_BYTES_LEN: usize = 16_384;

/// Errors returned by PMTiles storage reads.
#[derive(Clone, Debug, Error)]
pub enum StorageError {
    #[error("archive not found")]
    NotFound,
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
}

#[derive(Clone)]
struct EntryResolution {
    entry: DirectoryEntry,
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
        let tile_id = TileId::new(tile_id)?;
        let archive = match self.load_bootstrap(tileset_id).await? {
            Some(archive) => archive,
            None => return Ok(None),
        };
        let Some(resolution) = self
            .resolve_entry(tileset_id, &archive.header, archive.root, tile_id)
            .await?
        else {
            return Ok(None);
        };
        let bytes = self
            .storage
            .read_range(
                tileset_id,
                archive.header.data_offset + resolution.entry.offset,
                resolution.entry.length as usize,
                Some(archive_end(&archive.header)),
            )
            .await
            .context("failed to read PMTiles tile bytes")?;

        tracing::debug!(
            tileset_id = %tileset_id,
            tile_offset = archive.header.data_offset + resolution.entry.offset,
            tile_length = resolution.entry.length,
            "resolved tile bytes"
        );

        Ok(Some(TileData {
            bytes,
            content_type: archive.header.tile_type.content_type(),
            content_encoding: archive.header.tile_compression.content_encoding(),
        }))
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
        if let Some(archive) = self.archive_cache.get(tileset_id) {
            if let Some(metadata) = archive.metadata {
                return Ok(Some(metadata));
            }
        }

        match self
            .storage
            .fetch_bootstrap_bytes(tileset_id, true)
            .await
        {
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
        if let Some(archive) = self.archive_cache.get(tileset_id) {
            return Ok(Some(archive));
        }

        match self
            .storage
            .fetch_bootstrap_bytes(tileset_id, false)
            .await
        {
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

        self.load_bootstrap_local(tileset_id).await
    }

    /// Loads or reuses the cached header/root bootstrap from local backend storage.
    pub async fn load_bootstrap_local(
        self: &Arc<Self>,
        tileset_id: &TilesetId,
    ) -> Result<Option<ArchiveBootstrap>> {
        if let Some(archive) = self.archive_cache.get(tileset_id) {
            return Ok(Some(archive));
        }

        let initial_bytes = match self
            .storage
            .read_range(tileset_id, 0, INITIAL_BYTES_LEN, None)
            .await
        {
            Ok(bytes) => bytes,
            Err(StorageError::NotFound) => return Ok(None),
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
            if depth > 4 {
                return Ok(None);
            }

            let absolute_offset = header.leaf_offset + entry.offset;
            let child = self
                .load_leaf_directory(
                    tileset_id,
                    absolute_offset,
                    entry.length as usize,
                    header.internal_compression,
                    archive_end(header),
                )
                .await?;

            return Box::pin(self.resolve_in_directory(
                tileset_id,
                header,
                child,
                tile_id,
                depth + 1,
            ))
            .await;
        }

        Ok(Some(EntryResolution { entry }))
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
        if let Some(directory) = self.archive_cache.get_leaf(&leaf_key) {
            return Ok(directory);
        }
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

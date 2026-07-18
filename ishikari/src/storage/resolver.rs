//! Tileset serving, forwarding, and cache orchestration.

use std::sync::Arc;
use std::time::Duration;

use super::peer::InternalFetchResponse;
use anyhow::{Context, Result};
use bytes::Bytes;
#[cfg(feature = "simulator-support")]
use bytes::{BufMut, BytesMut};
use reqwest::Client;
use thiserror::Error;
use tracing::{debug, warn};

use super::{
    chunked_store::{BackendLatencyModel, ChunkedStore, ChunkedStoreConfig},
    peer::{InternalTileSource, PeerBackend, PeerFetchError},
    pmtiles::{DistributedPmtilesStorage, PmtilesReadSource},
    routing::HrwRouter,
};
use crate::{
    cache::{CachedTile, ResourceCache, TileCache, TileCacheKey},
    interned::TilesetId,
    membership::{Membership, Peer},
    metrics::NodeMetrics,
    pmtiles::{
        BootstrapTransfer, Header, Metadata, Reader as PmtilesReader, StorageError, TileData,
    },
};

const RESOURCE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const INTERNAL_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);

#[derive(Debug, Clone)]
pub struct TilesetInfo {
    pub header: Header,
    pub metadata: Arc<Metadata>,
}

impl TilesetInfo {
    /// Estimates the heap footprint of cached tileset metadata.
    pub(crate) fn approx_byte_size(&self) -> usize {
        std::mem::size_of::<Header>() + self.metadata.approx_byte_size()
    }
}

/// Runtime configuration for constructing a [`ResourceResolver`].
pub struct ResourceResolverConfig {
    pub self_node_id: String,
    pub membership: Membership,
    pub tileset_sources: String,
    pub candidate_count: usize,
    pub tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window: Duration,
    pub backend_fetch_concurrency: usize,
    pub artificial_backend_delay_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    /// How long a negative (tile-absent) L1 entry lives before re-resolution.
    pub tile_negative_ttl: Duration,
    pub object_store_registry: Arc<super::ObjectStoreRegistry>,
    pub metrics: NodeMetrics,
}

/// Storage and cache configuration shared by production and in-process resolvers.
pub struct ResourceResolverStorageConfig {
    pub tileset_sources: String,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window: Duration,
    pub backend_fetch_concurrency: usize,
    pub backend_latency: BackendLatencyModel,
    pub tile_cache_max_bytes: u64,
    pub peer_tile_cache_policy: PeerTileCachePolicy,
    pub chunk_cache_max_bytes: u64,
    /// How long a negative (tile-absent) L1 entry lives before re-resolution.
    pub tile_negative_ttl: Duration,
    pub object_store_registry: Arc<super::ObjectStoreRegistry>,
    pub metrics: NodeMetrics,
}

/// High-level resource resolver that combines routing, forwarding, and caches.
pub struct ResourceResolver {
    peer_backend: PeerBackend,
    pmtiles: Arc<PmtilesReader<DistributedPmtilesStorage>>,
    resource_cache: ResourceCache,
    tile_cache: TileCache,
    peer_tile_cache_policy: PeerTileCachePolicy,
}

/// Whether a successful peer response is also retained in the entry node's L1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PeerTileCachePolicy {
    /// Keep a replicated near-entry hot tier (the production default).
    EntryAndOwner,
    /// Keep positive tile bytes only on the HRW owner.
    #[cfg_attr(not(feature = "simulator-support"), allow(dead_code))]
    OwnerOnly,
}

enum CachedTileLookup {
    Found(TileData),
    NotFound,
    None,
}

/// Where a routed tile response was served from, for metrics.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum TileSource {
    /// Positive hit in the entry node's L1 tile cache.
    SelfTileCache,
    /// Resolved locally using PMTiles/index and chunk caches only.
    SelfChunkCache,
    /// Resolved locally after waiting for object-storage work.
    SelfBackend,
    /// Negative hit in the local L1 tile cache.
    NegativeCache,
    /// Fetched from a peer that used only its caches.
    PeerCache,
    /// Fetched from a peer that waited for object-storage work.
    PeerBackend,
    /// Local PMTiles lookup missed and inserted a negative cache entry.
    SelfMiss,
    /// A reachable owner authoritatively reported the tile absent; a negative
    /// cache entry was inserted locally.
    PeerMiss,
    /// Not found without changing the local cache.
    Miss,
}

impl TileSource {
    /// Returns the stable `ishikari_tiles_served_total{source}` label.
    pub fn served_label(self) -> &'static str {
        match self {
            Self::SelfTileCache | Self::SelfChunkCache => "self_cache",
            Self::SelfBackend => "self_backend",
            Self::PeerCache => "peer_cache",
            Self::PeerBackend => "peer_backend",
            Self::NegativeCache | Self::SelfMiss | Self::PeerMiss | Self::Miss => "miss",
        }
    }
}

impl ResourceResolver {
    /// Builds the resource resolver and its local caches.
    pub async fn new(config: ResourceResolverConfig) -> Result<Self> {
        let ResourceResolverConfig {
            self_node_id,
            membership,
            tileset_sources,
            candidate_count,
            tile_group_size,
            chunk_size_bytes,
            max_fetch_chunks,
            chunk_fetch_merge_window,
            backend_fetch_concurrency,
            artificial_backend_delay_ms,
            tile_cache_max_bytes,
            chunk_cache_max_bytes,
            tile_negative_ttl,
            object_store_registry,
            metrics,
        } = config;
        // Relies on `reqwest` being built without transfer-decompression
        // features (see the workspace dependency): a peer forwards provider
        // bodies with their `Content-Encoding` intact as representation
        // metadata, so transparent decompression here would corrupt them.
        let http_client = Client::builder()
            .connect_timeout(INTERNAL_HTTP_CONNECT_TIMEOUT)
            .use_rustls_tls()
            .build()
            .context("failed to build HTTP client")?;
        let router = HrwRouter::new(candidate_count, tile_group_size);
        let peer_backend = PeerBackend::new(
            self_node_id,
            membership,
            router,
            http_client,
            metrics.clone(),
        );
        Self::build_with_peer_backend(
            ResourceResolverStorageConfig {
                tileset_sources,
                chunk_size_bytes,
                max_fetch_chunks,
                chunk_fetch_merge_window,
                backend_fetch_concurrency,
                backend_latency: BackendLatencyModel::fixed(artificial_backend_delay_ms),
                tile_cache_max_bytes,
                peer_tile_cache_policy: PeerTileCachePolicy::EntryAndOwner,
                chunk_cache_max_bytes,
                tile_negative_ttl,
                object_store_registry,
                metrics,
            },
            peer_backend,
        )
    }

    /// Builds a resolver around an injected peer backend.
    #[cfg(feature = "simulator-support")]
    pub fn with_peer_backend(
        config: ResourceResolverStorageConfig,
        peer_backend: PeerBackend,
    ) -> Result<Self> {
        Self::build_with_peer_backend(config, peer_backend)
    }

    fn build_with_peer_backend(
        config: ResourceResolverStorageConfig,
        peer_backend: PeerBackend,
    ) -> Result<Self> {
        let chunked_store = ChunkedStore::new(
            ChunkedStoreConfig {
                tileset_sources: config.tileset_sources,
                chunk_size: config.chunk_size_bytes,
                max_fetch_chunks: config.max_fetch_chunks,
                chunk_fetch_merge_window: config.chunk_fetch_merge_window,
                backend_fetch_concurrency: config.backend_fetch_concurrency,
                backend_latency: config.backend_latency,
                chunk_cache_max_bytes: config.chunk_cache_max_bytes,
            },
            &config.object_store_registry,
            config.metrics.clone(),
        )?;
        let pmtiles_storage = DistributedPmtilesStorage::new(chunked_store, peer_backend.clone());
        let pmtiles = Arc::new(PmtilesReader::new(pmtiles_storage)?);
        Ok(Self {
            peer_backend,
            pmtiles,
            resource_cache: ResourceCache::new(RESOURCE_CACHE_MAX_BYTES),
            tile_cache: TileCache::new(config.tile_cache_max_bytes, config.tile_negative_ttl),
            peer_tile_cache_policy: config.peer_tile_cache_policy,
        })
    }

    /// Returns stable tile-cache metric outcomes for this resolver's insertion policy.
    pub fn cache_outcomes(&self, source: TileSource) -> &'static [&'static str] {
        match source {
            TileSource::SelfTileCache => &["hit"],
            TileSource::NegativeCache => &["negative", "negative_hit"],
            TileSource::SelfChunkCache | TileSource::SelfBackend => &["miss", "insert"],
            TileSource::PeerCache | TileSource::PeerBackend
                if self.peer_tile_cache_policy == PeerTileCachePolicy::EntryAndOwner =>
            {
                &["miss", "insert"]
            }
            TileSource::PeerCache | TileSource::PeerBackend | TileSource::Miss => &["miss"],
            TileSource::SelfMiss | TileSource::PeerMiss => &["miss", "negative"],
        }
    }

    /// Returns the current weighted byte sizes of the tile and chunk caches.
    pub fn tile_cache_weighted_size(&self) -> u64 {
        self.tile_cache.weighted_size()
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub fn chunk_cache_weighted_size(&self) -> u64 {
        self.pmtiles.storage().chunk_cache_weighted_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.pmtiles.storage().received_bytes()
    }

    /// Routes a non-PMTiles provider resource by a stable HRW key.
    ///
    /// Returns `None` when the local node should fetch the resource itself.
    pub(crate) async fn route_provider_resource(
        &self,
        key: &str,
        internal_path: &str,
        kind: &str,
    ) -> Result<Option<InternalFetchResponse>> {
        self.peer_backend
            .route_fetch_optional_by_key(key, internal_path, kind)
            .await
    }

    /// Routes a typed generated-tile resource by the normal tile-group HRW
    /// policy. `None` means this node should produce the resource locally.
    pub async fn route_derived_resource(
        &self,
        routing_id: &TilesetId,
        tile_id: u64,
        internal_path: &str,
    ) -> Result<Option<Bytes>> {
        self.peer_backend
            .route_fetch_optional_by_tile(routing_id, tile_id, internal_path, "derived")
            .await
    }

    /// Serves an external tile request addressed by PMTiles tile id.
    pub async fn route_tile(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
            CachedTileLookup::Found(tile) => {
                return Ok((Some(tile), TileSource::SelfTileCache));
            }
            CachedTileLookup::NotFound => return Ok((None, TileSource::NegativeCache)),
            CachedTileLookup::None => {}
        }

        let candidates = self.peer_backend.route_tile(&tileset_id, tile_id).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.peer_backend.is_self(&peer.peer))
        {
            return self.load_local_tile_with_source(&tileset_id, tile_id).await;
        }

        for peer in candidates {
            if self.peer_backend.is_self(&peer.peer) {
                return self.load_local_tile_with_source(&tileset_id, tile_id).await;
            }

            match self
                .load_tile_from_peer(&peer.peer, &tileset_id, tile_id)
                .await
            {
                Ok(Some((tile, peer_source))) => {
                    let source = match peer_source {
                        InternalTileSource::Cache => TileSource::PeerCache,
                        InternalTileSource::Backend => TileSource::PeerBackend,
                    };
                    return Ok((Some(tile), source));
                }
                // A reachable owner resolves the tile against the same shared
                // object storage as every other node, so its "not found" is
                // authoritative: no other candidate and no local re-resolution
                // can find it. Record a negative L1 entry and stop, rather than
                // forwarding the same 404 to the remaining candidates and then
                // resolving it locally — which turned every absent tile (common
                // in sparse archives) into up to `candidate_count` peer
                // round-trips plus a full local backend resolve. (Staleness of
                // that negative entry is bounded by cache eviction today; a TTL
                // on negative entries is the separate, correct fix.)
                Err(TilesetError::Miss) => {
                    debug!(
                        peer_id = %peer.peer.id,
                        tileset_id = %tileset_id,
                        tile_id = tile_id,
                        "peer reported tile absent; negative-caching and stopping"
                    );
                    self.cache_tile_miss(&tileset_id, tile_id);
                    return Ok((None, TileSource::PeerMiss));
                }
                // The peer served tile bytes but the tileset header could not be
                // resolved here — inconclusive, so fall back rather than
                // negative-caching a tile the peer actually has.
                Ok(None) => {
                    debug!(
                        peer_id = %peer.peer.id,
                        tileset_id = %tileset_id,
                        tile_id = tile_id,
                        "peer returned tile without resolvable header; trying fallback"
                    );
                }
                Err(error) if error.is_retryable() => {
                    warn!(peer_id = %peer.peer.id, error = %error, "tile forward failed; trying fallback");
                }
                Err(error) => return Err(error),
            }
        }

        self.load_local_tile_with_source(&tileset_id, tile_id).await
    }

    /// Loads a tile from local storage and tags whether it was found.
    async fn load_local_tile_with_source(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
        let (tile, read_source) = self.load_local_tile(tileset_id, tile_id).await?;
        let source = if tile.is_some() && read_source == PmtilesReadSource::Cache {
            TileSource::SelfChunkCache
        } else if tile.is_some() {
            TileSource::SelfBackend
        } else {
            TileSource::SelfMiss
        };
        Ok((tile, source))
    }

    /// Serves an internal tile request addressed by PMTiles tile id.
    pub async fn load_tile_by_id(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>, TilesetError> {
        Ok(self
            .load_tile_by_id_with_source(tileset_id, tile_id)
            .await?
            .0)
    }

    /// Serves an internal tile request and reports whether caches were sufficient.
    pub(crate) async fn load_tile_by_id_with_source(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<(Option<TileData>, TileSource), TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "internal tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
            CachedTileLookup::Found(tile) => {
                return Ok((Some(tile), TileSource::SelfTileCache));
            }
            CachedTileLookup::NotFound => return Ok((None, TileSource::NegativeCache)),
            CachedTileLookup::None => {}
        }

        self.load_local_tile_with_source(&tileset_id, tile_id).await
    }

    /// Loads tileset metadata, reusing the local resource cache when present.
    pub async fn load_tileset_info(
        &self,
        tileset_id: TilesetId,
    ) -> Result<Option<Arc<TilesetInfo>>, TilesetError> {
        if let Some(info) = self.resource_cache.get_tileset_info(&tileset_id) {
            debug!(
                tileset_id = %tileset_id,
                "tileset info cache hit"
            );
            return Ok(Some(info));
        }

        debug!(
            tileset_id = %tileset_id,
            "tileset info request"
        );

        let Some((header, metadata)) = self.read_tileset_info(&tileset_id).await? else {
            return Ok(None);
        };
        let info = Arc::new(TilesetInfo { header, metadata });
        self.resource_cache
            .put_tileset_info(&tileset_id, info.clone());
        Ok(Some(info))
    }

    /// Loads local raw bootstrap bytes for internal forwarding, optionally including metadata.
    pub(crate) async fn load_bootstrap_bytes(
        &self,
        tileset_id: TilesetId,
        include_metadata: bool,
    ) -> Result<Option<BootstrapTransfer>, TilesetError> {
        self.pmtiles
            .load_bootstrap_bytes_local(&tileset_id, include_metadata)
            .await
            .map_err(internal_tileset_error)
    }

    /// Loads local raw PMTiles leaf bytes for internal forwarding.
    pub(crate) async fn load_leaf_bytes(
        &self,
        tileset_id: TilesetId,
        offset: u64,
        length: usize,
    ) -> Result<Option<Bytes>, TilesetError> {
        self.pmtiles
            .load_leaf_bytes_local(&tileset_id, offset, length)
            .await
            .map_err(internal_tileset_error)
    }

    /// Executes the internal peer protocol without HTTP for the simulator.
    #[cfg(feature = "simulator-support")]
    #[doc(hidden)]
    pub async fn fetch_internal_for_simulator(
        &self,
        path: &str,
    ) -> Result<InternalFetchResponse, PeerFetchError> {
        if let Some(rest) = path.strip_prefix("/_internal/tiles/") {
            let (tileset, tile_id) = rest.rsplit_once('/').ok_or_else(|| {
                PeerFetchError::Fatal(format!("invalid internal tile path {path}"))
            })?;
            let tileset_id = decode_internal_tileset(tileset)?;
            let tile_id = tile_id.parse::<u64>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal tile id: {error}"))
            })?;
            let (tile, source) = self
                .load_tile_by_id_with_source(tileset_id, tile_id)
                .await
                .map_err(simulator_fetch_error)?;
            let source = match source {
                TileSource::SelfTileCache | TileSource::SelfChunkCache => InternalTileSource::Cache,
                TileSource::SelfBackend => InternalTileSource::Backend,
                _ => return Err(PeerFetchError::NotFound),
            };
            return tile
                .map(|tile| InternalFetchResponse::tile(tile.bytes, source))
                .ok_or(PeerFetchError::NotFound);
        }

        let Some(rest) = path.strip_prefix("/_internal/pmtiles/") else {
            return Err(PeerFetchError::Fatal(format!(
                "unsupported simulator internal path {path}"
            )));
        };
        let (path_only, query) = rest
            .split_once('?')
            .map_or((rest, None), |(path, query)| (path, Some(query)));
        let (tileset, operation) = path_only.split_once('/').ok_or_else(|| {
            PeerFetchError::Fatal(format!("invalid internal PMTiles path {path}"))
        })?;
        let tileset_id = decode_internal_tileset(tileset)?;

        if operation == "bootstrap" {
            let include_metadata = query == Some("metadata=true");
            let transfer = self
                .load_bootstrap_bytes(tileset_id, include_metadata)
                .await
                .map_err(simulator_fetch_error)?
                .ok_or(PeerFetchError::NotFound)?;
            if let Some(metadata) = transfer.metadata {
                let mut body =
                    BytesMut::with_capacity(8 + transfer.bootstrap.len() + metadata.len());
                body.put_u64_le(transfer.bootstrap.len() as u64);
                body.extend_from_slice(&transfer.bootstrap);
                body.extend_from_slice(&metadata);
                return Ok(InternalFetchResponse::bytes(body.freeze()));
            }
            return Ok(InternalFetchResponse::bytes(transfer.bootstrap));
        }

        if let Some(arguments) = operation.strip_prefix("leaf/") {
            let (offset, length) = arguments.split_once('/').ok_or_else(|| {
                PeerFetchError::Fatal(format!("invalid internal leaf path {path}"))
            })?;
            let offset = offset.parse::<u64>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal leaf offset: {error}"))
            })?;
            let length = length.parse::<usize>().map_err(|error| {
                PeerFetchError::Fatal(format!("invalid internal leaf length: {error}"))
            })?;
            return self
                .load_leaf_bytes(tileset_id, offset, length)
                .await
                .map_err(simulator_fetch_error)?
                .map(InternalFetchResponse::bytes)
                .ok_or(PeerFetchError::NotFound);
        }

        Err(PeerFetchError::Fatal(format!(
            "unsupported simulator PMTiles path {path}"
        )))
    }

    /// Loads the common header and metadata inputs shared by tileset HTTP endpoints.
    async fn read_tileset_info(
        &self,
        tileset_id: &TilesetId,
    ) -> Result<Option<(Header, Arc<Metadata>)>, TilesetError> {
        let header = self
            .pmtiles
            .header(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(header) = header else {
            return Ok(None);
        };

        let metadata = self
            .pmtiles
            .metadata(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(metadata) = metadata else {
            return Ok(None);
        };

        Ok(Some((header, metadata)))
    }

    /// Fetches a tile from the local PMTiles-backed storage path.
    async fn load_local_tile(
        &self,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<(Option<TileData>, PmtilesReadSource), TilesetError> {
        let (tile, source) = self
            .pmtiles
            .storage()
            .observe_reads(self.pmtiles.get_tile(tileset_id, tile_id))
            .await;
        let tile = tile.map_err(internal_tileset_error)?;

        let Some(tile) = tile else {
            self.cache_tile_miss(tileset_id, tile_id);
            return Ok((None, source));
        };

        self.cache_tile_hit(tileset_id, tile_id, &tile);
        Ok((Some(tile), source))
    }

    /// Forwards a tile request to the selected peer over the internal HTTP API.
    async fn load_tile_from_peer(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<(TileData, InternalTileSource)>, TilesetError> {
        let response = self
            .peer_backend
            .fetch_tile_bytes(peer, tileset_id, tile_id)
            .await
            .map_err(|error| match error {
                PeerFetchError::NotFound => TilesetError::Miss,
                PeerFetchError::Retryable(message) => TilesetError::retryable_upstream(message),
                PeerFetchError::Fatal(message) => TilesetError::Upstream(message),
            })?;

        let header = self
            .pmtiles
            .header(tileset_id)
            .await
            .map_err(internal_tileset_error)?;
        let Some(header) = header else {
            return Ok(None);
        };
        let tile = TileData {
            bytes: response.bytes,
            content_type: header.tile_type.content_type(),
            content_encoding: header.tile_compression.content_encoding(),
        };
        if self.peer_tile_cache_policy == PeerTileCachePolicy::EntryAndOwner {
            self.cache_tile_hit(tileset_id, tile_id, &tile);
        }
        Ok(Some((
            tile,
            response.tile_source.unwrap_or(InternalTileSource::Backend),
        )))
    }

    /// Returns a tile from the local L1 tile cache when present.
    fn load_cached_tile(&self, tileset_id: &TilesetId, tile_id: u64) -> CachedTileLookup {
        let Some(entry) = self.tile_cache.get(&TileCacheKey::new(tileset_id, tile_id)) else {
            return CachedTileLookup::None;
        };
        tracing::debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile cache hit"
        );
        match entry {
            CachedTile::Found {
                bytes,
                content_type,
                content_encoding,
            } => CachedTileLookup::Found(TileData {
                bytes,
                content_type,
                content_encoding,
            }),
            CachedTile::NotFound => CachedTileLookup::NotFound,
        }
    }

    /// Stores a positive tile cache entry in the local L1 tile cache.
    fn cache_tile_hit(&self, tileset_id: &TilesetId, tile_id: u64, tile: &TileData) {
        self.tile_cache.put(
            TileCacheKey::new(tileset_id, tile_id),
            CachedTile::Found {
                bytes: tile.bytes.clone(),
                content_type: tile.content_type,
                content_encoding: tile.content_encoding,
            },
        );
    }

    /// Stores a negative tile cache entry in the local L1 tile cache.
    fn cache_tile_miss(&self, tileset_id: &TilesetId, tile_id: u64) {
        self.tile_cache
            .put(TileCacheKey::new(tileset_id, tile_id), CachedTile::NotFound);
    }
}

/// Errors returned by the tileset service before HTTP status mapping.
#[derive(Debug, Error)]
pub enum TilesetError {
    #[error("{0}")]
    Upstream(String),
    #[error("{0}")]
    RetryableUpstream(String),
    #[error("{0}")]
    Timeout(String),
    /// Backend fetch admission is saturated; shed with 503 rather than queue.
    #[error("{0}")]
    Overload(String),
    #[error("forward miss")]
    Miss,
    #[error("{0}")]
    Internal(String),
}

impl TilesetError {
    /// Wraps an upstream error that should trigger peer fallback.
    fn retryable_upstream(message: String) -> Self {
        Self::RetryableUpstream(message)
    }

    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::RetryableUpstream(_))
    }
}

fn format_error_chain(error: &anyhow::Error) -> String {
    error
        .chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

fn internal_tileset_error(error: anyhow::Error) -> TilesetError {
    // Classify by the typed error in the chain, not by matching the message
    // string: a backend read timeout surfaces as `StorageError::Timeout`.
    let storage_error = error
        .chain()
        .find_map(|cause| cause.downcast_ref::<StorageError>());
    let message = format_error_chain(&error);
    match storage_error {
        Some(StorageError::Timeout(_)) => TilesetError::Timeout(message),
        Some(StorageError::Overload(_)) => TilesetError::Overload(message),
        _ => TilesetError::Internal(message),
    }
}

#[cfg(feature = "simulator-support")]
fn decode_internal_tileset(encoded: &str) -> Result<TilesetId, PeerFetchError> {
    let decoded = encoded.replace("%2F", "/").replace("%2f", "/");
    if decoded.contains('%') {
        return Err(PeerFetchError::Fatal(
            "unsupported percent encoding in internal tileset path".to_string(),
        ));
    }
    TilesetId::try_new(&decoded)
        .map_err(|error| PeerFetchError::Fatal(format!("invalid internal tileset id: {error}")))
}

#[cfg(feature = "simulator-support")]
fn simulator_fetch_error(error: TilesetError) -> PeerFetchError {
    match error {
        TilesetError::RetryableUpstream(message)
        | TilesetError::Timeout(message)
        | TilesetError::Overload(message) => PeerFetchError::Retryable(message),
        TilesetError::Miss => PeerFetchError::NotFound,
        TilesetError::Upstream(message) | TilesetError::Internal(message) => {
            PeerFetchError::Fatal(message)
        }
    }
}

#[cfg(all(test, feature = "simulator-support"))]
mod tests {
    use std::net::SocketAddr;
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };

    use super::*;
    use crate::membership::Peer;
    use crate::metrics::NodeMetrics;
    use crate::storage::ObjectStoreRegistry;
    use crate::storage::peer::{
        FetchFuture, InternalTransport, PeerBackend, PeerDirectory, PeerFetchError, PeerFuture,
    };
    use crate::storage::routing::HrwRouter;

    /// Peer directory returning a fixed peer set (none of them the local node).
    struct StaticDirectory {
        peers: Vec<Peer>,
    }

    impl PeerDirectory for StaticDirectory {
        fn peers(&self) -> PeerFuture<'_> {
            Box::pin(std::future::ready(self.peers.clone().into()))
        }
    }

    /// Transport that counts tile fetches and always reports the tile absent.
    #[derive(Default)]
    struct NotFoundTransport {
        calls: AtomicUsize,
    }

    impl InternalTransport for NotFoundTransport {
        fn fetch<'a>(&'a self, _peer: &'a Peer, _path: &'a str) -> FetchFuture<'a> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(std::future::ready(Err(PeerFetchError::NotFound)))
        }
    }

    fn peer(id: &str, port: u16) -> Peer {
        Peer {
            id: id.to_string(),
            addr: SocketAddr::from(([127, 0, 0, 1], port)),
        }
    }

    fn resolver_with_transport(transport: Arc<NotFoundTransport>) -> ResourceResolver {
        // Three peer owners, none of which is the local node, so every candidate
        // forces the peer-forwarding path rather than a local fallback.
        let peers = vec![
            peer("node-a", 8001),
            peer("node-b", 8002),
            peer("node-c", 8003),
        ];
        let metrics = NodeMetrics::new();
        let peer_backend = PeerBackend::with_dependencies(
            "entry".to_string(),
            Arc::new(StaticDirectory { peers }),
            HrwRouter::new(3, 512),
            transport,
            metrics.clone(),
        );
        // The local path is never read (the peer 404 short-circuits before any
        // local resolve), but it is resolved eagerly at construction, so point
        // it at a directory that exists.
        let tileset_sources = std::env::temp_dir().to_string_lossy().into_owned();
        ResourceResolver::with_peer_backend(
            ResourceResolverStorageConfig {
                tileset_sources,
                chunk_size_bytes: 1024 * 1024,
                max_fetch_chunks: 4,
                chunk_fetch_merge_window: Duration::from_millis(10),
                backend_fetch_concurrency: 32,
                backend_latency: BackendLatencyModel::fixed(0),
                tile_cache_max_bytes: 1024 * 1024,
                peer_tile_cache_policy: PeerTileCachePolicy::EntryAndOwner,
                chunk_cache_max_bytes: 1024 * 1024,
                tile_negative_ttl: Duration::from_secs(60),
                object_store_registry: Arc::new(ObjectStoreRegistry::new()),
                metrics,
            },
            peer_backend,
        )
        .expect("build resolver")
    }

    #[tokio::test]
    async fn authoritative_peer_miss_stops_after_one_forward_and_negative_caches() {
        let transport = Arc::new(NotFoundTransport::default());
        let resolver = resolver_with_transport(transport.clone());
        let tileset = TilesetId::new_unchecked("demo/streets");

        // A reachable owner's 404 is authoritative: exactly one forward, no
        // walk over the remaining candidates and no local re-resolution.
        let (tile, source) = resolver
            .route_tile(tileset.clone(), 700)
            .await
            .expect("route tile");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::PeerMiss);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

        // The negative entry is now cached locally, so a repeat is a negative
        // L1 hit with no further peer forwarding.
        let (tile, source) = resolver
            .route_tile(tileset, 700)
            .await
            .expect("route tile again");
        assert!(tile.is_none());
        assert_eq!(source, TileSource::NegativeCache);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);

        assert_eq!(
            resolver.cache_outcomes(TileSource::NegativeCache),
            ["negative", "negative_hit"]
        );
        assert_eq!(
            resolver.cache_outcomes(TileSource::PeerMiss),
            ["miss", "negative"]
        );
    }
}

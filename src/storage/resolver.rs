//! Tileset serving, forwarding, and cache orchestration.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use bytes::Bytes;
use reqwest::Client;
use thiserror::Error;
use tracing::{debug, warn};

use super::{
    chunked_store::ChunkedStore,
    peer::{PeerBackend, PeerFetchError},
    pmtiles::DistributedPmtilesStorage,
    routing::HrwRouter,
};
use crate::{
    cache::{CachedTile, ResourceCache, TileCache, TileCacheKey},
    interned::TilesetId,
    membership::{Membership, Peer},
    pmtiles::{BootstrapTransfer, Header, Metadata, Reader as PmtilesReader, TileData},
};

const RESOURCE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const INTERNAL_HTTP_CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
const INTERNAL_HTTP_REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

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
    pub data_url: String,
    pub candidate_count: usize,
    pub tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub debug_fetch_delay_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
}

/// High-level resource resolver that combines routing, forwarding, and caches.
pub struct ResourceResolver {
    peer_backend: PeerBackend,
    pmtiles: Arc<PmtilesReader<DistributedPmtilesStorage>>,
    resource_cache: ResourceCache,
    tile_cache: TileCache,
}

enum CachedTileLookup {
    Found(TileData),
    NotFound,
    None,
}

impl ResourceResolver {
    /// Builds the resource resolver and its local caches.
    pub async fn new(config: ResourceResolverConfig) -> Result<Self> {
        let http_client = Client::builder()
            .connect_timeout(INTERNAL_HTTP_CONNECT_TIMEOUT)
            .timeout(INTERNAL_HTTP_REQUEST_TIMEOUT)
            .use_rustls_tls()
            .build()
            .context("failed to build HTTP client")?;
        let router = HrwRouter::new(config.candidate_count, config.tile_group_size);
        let peer_backend = PeerBackend::new(
            config.self_node_id.clone(),
            config.membership.clone(),
            router.clone(),
            http_client.clone(),
        );
        let chunked_store = ChunkedStore::new(
            config.data_url,
            config.chunk_size_bytes,
            config.max_fetch_chunks,
            config.debug_fetch_delay_ms,
            config.chunk_cache_max_bytes,
        )?;
        let pmtiles_storage =
            DistributedPmtilesStorage::new(chunked_store, peer_backend.clone());
        let pmtiles = Arc::new(PmtilesReader::new(pmtiles_storage)?);
        Ok(Self {
            peer_backend,
            pmtiles,
            resource_cache: ResourceCache::new(RESOURCE_CACHE_MAX_BYTES),
            tile_cache: TileCache::new(config.tile_cache_max_bytes),
        })
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

    /// Serves an external tile request addressed by PMTiles tile id.
    pub async fn route_tile(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>, TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
            CachedTileLookup::Found(tile) => return Ok(Some(tile)),
            CachedTileLookup::NotFound => return Ok(None),
            CachedTileLookup::None => {}
        }

        let candidates = self.peer_backend.route_tile(&tileset_id, tile_id).await;

        if candidates.is_empty()
            || candidates
                .first()
                .is_some_and(|peer| self.peer_backend.is_self(&peer.peer))
        {
            return self.load_local_tile(&tileset_id, tile_id).await;
        }

        for peer in candidates {
            if self.peer_backend.is_self(&peer.peer) {
                return self.load_local_tile(&tileset_id, tile_id).await;
            }

            match self
                .load_tile_from_peer(&peer.peer, &tileset_id, tile_id)
                .await
            {
                Ok(Some(tile)) => return Ok(Some(tile)),
                Ok(None) => return Ok(None),
                Err(TilesetError::Miss) => {}
                Err(error) if error.is_retryable() => {
                    warn!(peer_id = %peer.peer.id, error = %error, "tile forward failed; trying fallback");
                }
                Err(error) => return Err(error),
            }
        }

        self.load_local_tile(&tileset_id, tile_id).await
    }

    /// Serves an internal tile request addressed by PMTiles tile id.
    pub async fn load_tile_by_id(
        &self,
        tileset_id: TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>, TilesetError> {
        debug!(
            tileset_id = %tileset_id,
            tile_id = tile_id,
            "internal tile request"
        );

        match self.load_cached_tile(&tileset_id, tile_id) {
            CachedTileLookup::Found(tile) => return Ok(Some(tile)),
            CachedTileLookup::NotFound => return Ok(None),
            CachedTileLookup::None => {}
        }

        self.load_local_tile(&tileset_id, tile_id).await
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
    ) -> Result<Option<TileData>, TilesetError> {
        let tile = self
            .pmtiles
            .get_tile(tileset_id, tile_id)
            .await
            .map_err(internal_tileset_error)?;

        let Some(tile) = tile else {
            self.cache_tile_miss(tileset_id, tile_id);
            return Ok(None);
        };

        self.cache_tile_hit(tileset_id, tile_id, &tile);
        Ok(Some(tile))
    }

    /// Forwards a tile request to the selected peer over the internal HTTP API.
    async fn load_tile_from_peer(
        &self,
        peer: &Peer,
        tileset_id: &TilesetId,
        tile_id: u64,
    ) -> Result<Option<TileData>, TilesetError> {
        let bytes = self
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
            bytes,
            content_type: header.tile_type.content_type(),
            content_encoding: header.tile_compression.content_encoding(),
        };
        self.cache_tile_hit(tileset_id, tile_id, &tile);
        Ok(Some(tile))
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
    let message = format_error_chain(&error);
    if message.contains("timed out") {
        return TilesetError::Timeout(message);
    }
    TilesetError::Internal(message)
}


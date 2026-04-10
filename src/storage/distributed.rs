//! Distributed storage implementation for PMTiles reads.

use crate::{
    interned_str::TilesetId,
    pmtiles::{RangeRead, RangeStoreError, Storage},
    storage::chunked_store::object_store::byte_range_chunk_indices,
};
use anyhow::Result;
use bytes::Bytes;

use super::{ChunkedStore, PeerBackend, chunked_store::fetch_aggregator::ChunkFetchError};

/// Distributed storage implementation used by the PMTiles reader.
#[derive(Clone)]
pub struct DistributedStorage {
    chunked_store: ChunkedStore,
    peer_backend: PeerBackend,
}

impl DistributedStorage {
    /// Creates the PMTiles storage implementation from local reads and peer routing state.
    pub(crate) fn new(chunked_store: ChunkedStore, peer_backend: PeerBackend) -> Self {
        Self {
            chunked_store,
            peer_backend,
        }
    }
}

impl Storage for DistributedStorage {
    fn chunk_size_bytes(&self) -> u64 {
        self.chunked_store.chunk_size_bytes()
    }

    fn read_range<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<RangeRead, RangeStoreError>> + Send + 'a {
        async move {
            if length == 0 {
                return Ok(RangeRead {
                    bytes: Bytes::new(),
                    cache_hit: true,
                });
            }

            let end = start + length as u64;
            let chunk_indices =
                byte_range_chunk_indices(start, end, self.chunked_store.chunk_size_bytes());
            let cache_hit = chunk_indices.iter().all(|chunk_index| {
                self.chunked_store
                    .chunk_cache_get(tileset_id, *chunk_index)
                    .is_some()
            });

            let bytes = self
                .chunked_store
                .read_bytes(tileset_id, start, length, archive_len)
                .await
                .map_err(|error| match error {
                    ChunkFetchError::NotFound => RangeStoreError::NotFound,
                    ChunkFetchError::Message(message) => RangeStoreError::Message(message),
                })?;

            Ok(RangeRead { bytes, cache_hit })
        }
    }

    fn fetch_archive_bootstrap_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + 'a {
        async move {
            let candidates = self.peer_backend.route_tileset(tileset_id).await;

            if candidates.is_empty()
                || candidates
                    .first()
                    .is_some_and(|peer| self.peer_backend.is_self(peer))
            {
                return Ok(None);
            }

            for peer in candidates {
                if self.peer_backend.is_self(&peer) {
                    return Ok(None);
                }

                match self
                    .peer_backend
                    .fetch_archive_index_bytes(&peer, tileset_id)
                    .await
                {
                    Ok(Some(body)) => return Ok(Some(body)),
                    Ok(None) => return Ok(None),
                    Err(_) => continue,
                }
            }

            Ok(None)
        }
    }

    fn fetch_metadata_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + 'a {
        async move {
            let candidates = self.peer_backend.route_tileset(tileset_id).await;

            if candidates.is_empty()
                || candidates
                    .first()
                    .is_some_and(|peer| self.peer_backend.is_self(peer))
            {
                return Ok(None);
            }

            for peer in candidates {
                if self.peer_backend.is_self(&peer) {
                    return Ok(None);
                }

                match self.peer_backend.fetch_metadata_bytes(&peer, tileset_id).await {
                    Ok(Some(body)) => return Ok(Some(body)),
                    Ok(None) => return Ok(None),
                    Err(_) => continue,
                }
            }

            Ok(None)
        }
    }

    fn fetch_leaf_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        offset: u64,
        length: usize,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + 'a {
        async move {
            let candidates = self.peer_backend.route_tileset(tileset_id).await;

            if candidates.is_empty()
                || candidates
                    .first()
                    .is_some_and(|peer| self.peer_backend.is_self(peer))
            {
                return Ok(None);
            }

            for peer in candidates {
                if self.peer_backend.is_self(&peer) {
                    return Ok(None);
                }

                match self
                    .peer_backend
                    .fetch_leaf_bytes(&peer, tileset_id, offset, length)
                    .await
                {
                    Ok(Some(body)) => return Ok(Some(body)),
                    Ok(None) => return Ok(None),
                    Err(_) => continue,
                }
            }

            Ok(None)
        }
    }
}

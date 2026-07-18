//! Distributed storage implementation for PMTiles reads.

use std::{cell::Cell, future::Future};

use crate::{
    interned::TilesetId,
    pmtiles::{BootstrapTransfer, Storage as PmtilesStorage, StorageError},
};
use anyhow::Result;
use anyhow::bail;
use bytes::Bytes;

use super::{
    chunked_store::{ChunkFetchError, ChunkReadSource, ChunkedStore},
    peer::PeerBackend,
};

const READ_CHUNK_LIMIT: u64 = 8;

tokio::task_local! {
    static BACKEND_WAITED: Cell<bool>;
}

/// Whether local PMTiles resolution used only caches or waited for object storage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum PmtilesReadSource {
    Cache,
    Backend,
}

/// Distributed storage implementation used by the PMTiles reader.
#[derive(Clone)]
pub struct DistributedPmtilesStorage {
    chunked_store: ChunkedStore,
    peer_backend: PeerBackend,
}

impl DistributedPmtilesStorage {
    /// Creates the PMTiles storage implementation from local reads and peer routing state.
    pub(crate) fn new(chunked_store: ChunkedStore, peer_backend: PeerBackend) -> Self {
        Self {
            chunked_store,
            peer_backend,
        }
    }

    pub fn chunk_cache_weighted_size(&self) -> u64 {
        self.chunked_store.chunk_cache_weighted_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.chunked_store.received_bytes()
    }

    /// Observes all chunk reads made while `future` resolves one tile.
    pub(crate) async fn observe_reads<F>(&self, future: F) -> (F::Output, PmtilesReadSource)
    where
        F: Future,
    {
        BACKEND_WAITED
            .scope(Cell::new(false), async move {
                let output = future.await;
                let source = if BACKEND_WAITED.with(Cell::get) {
                    PmtilesReadSource::Backend
                } else {
                    PmtilesReadSource::Cache
                };
                (output, source)
            })
            .await
    }
}

impl PmtilesStorage for DistributedPmtilesStorage {
    #[allow(clippy::manual_async_fn)]
    fn read_range<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> impl std::future::Future<Output = Result<Bytes, StorageError>> + Send + 'a {
        async move {
            if length == 0 {
                return Ok(Bytes::new());
            }

            enforce_chunk_limit(
                "range",
                start,
                length as u64,
                self.chunked_store.chunk_size(),
            )
            .map_err(|error| StorageError::Message(error.to_string()))?;

            let read = self
                .chunked_store
                .read_bytes(tileset_id, start, length, archive_len)
                .await
                .map_err(|error| match error {
                    ChunkFetchError::NotFound => StorageError::NotFound,
                    ChunkFetchError::Timeout(message) => StorageError::Timeout(message),
                    ChunkFetchError::Overload(message) => StorageError::Overload(message),
                    ChunkFetchError::Message(message) => StorageError::Message(message),
                })?;

            if read.source == ChunkReadSource::Backend {
                let _ = BACKEND_WAITED.try_with(|waited| waited.set(true));
            }

            Ok(read.bytes)
        }
    }

    fn fetch_bootstrap_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        include_metadata: bool,
    ) -> impl std::future::Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a {
        self.peer_backend
            .route_bootstrap(tileset_id, include_metadata)
    }

    fn fetch_leaf_bytes<'a>(
        &'a self,
        tileset_id: &'a TilesetId,
        offset: u64,
        length: usize,
    ) -> impl std::future::Future<Output = Result<Option<Bytes>>> + Send + 'a {
        self.peer_backend.route_leaf(tileset_id, offset, length)
    }
}

fn enforce_chunk_limit(kind: &str, start: u64, length: u64, chunk_size_bytes: u64) -> Result<()> {
    let end = start
        .checked_add(length)
        .ok_or_else(|| anyhow::anyhow!("invalid {kind} byte range"))?;
    let chunk_count = ((end - 1) / chunk_size_bytes)
        .saturating_sub(start / chunk_size_bytes)
        .saturating_add(1);
    if chunk_count > READ_CHUNK_LIMIT {
        bail!(
            "{kind} spans too many chunks: start={start} length={length} chunks={chunk_count} limit={READ_CHUNK_LIMIT}"
        );
    }
    Ok(())
}

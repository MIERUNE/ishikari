//! Object-store and cache backed chunked byte-range reader.

use std::{collections::HashMap, ops::RangeInclusive, time::Duration};

use anyhow::{Context, Result, bail};
use bytes::{Bytes, BytesMut};

use crate::interned::TilesetId;
use crate::metrics::NodeMetrics;
use crate::storage::ObjectStoreRegistry;

use super::{
    cache::{ChunkCache, ChunkCacheKey},
    coordinator::ChunkFetchCoordinator,
    fetcher::{BackendLatencyModel, ChunkFetchError, ChunkFetcher},
};

const MAX_BACKEND_FETCH_CONCURRENCY: usize = 1_024;
const MAX_CHUNK_SIZE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_FETCH_CHUNKS: u64 = 64;
const MAX_BACKEND_FETCH_BYTES: u64 = 64 * 1024 * 1024;
// Bound the worst-case bytes represented by all active range fetches without
// rejecting the largest geometry used by the simulator's documented sweep
// matrix (64 MiB per fetch at the default concurrency of 32).
const MAX_BACKEND_ACTIVE_FETCH_BYTES: u64 = 8 * 1024 * 1024 * 1024;
/// A batching delay is useful in the low-millisecond range. Keep it well below
/// the 30-second request deadline so every admitted request can be dispatched.
const MAX_CHUNK_FETCH_MERGE_WINDOW: Duration = Duration::from_secs(1);

/// Validates every value that controls chunk-range allocation, scheduling, or
/// semaphore construction. Kept beside `ChunkedStore::new` so production,
/// simulator, and future callers cannot diverge on safety invariants.
#[doc(hidden)]
pub fn validate_chunked_store_limits(
    chunk_size: u64,
    max_fetch_chunks: u64,
    merge_window: Duration,
    backend_fetch_concurrency: usize,
) -> Result<()> {
    if chunk_size == 0 {
        bail!("chunk size bytes must be greater than zero");
    }
    if chunk_size > MAX_CHUNK_SIZE_BYTES {
        bail!("chunk size bytes {chunk_size} exceeds the safe maximum {MAX_CHUNK_SIZE_BYTES}");
    }
    if max_fetch_chunks == 0 {
        bail!("max fetch chunks must be greater than zero");
    }
    if max_fetch_chunks > MAX_FETCH_CHUNKS {
        bail!("max fetch chunks {max_fetch_chunks} exceeds the safe maximum {MAX_FETCH_CHUNKS}");
    }
    let fetch_bytes = chunk_size
        .checked_mul(max_fetch_chunks)
        .context("backend fetch span overflows u64")?;
    if fetch_bytes > MAX_BACKEND_FETCH_BYTES {
        bail!(
            "backend fetch span {fetch_bytes} bytes exceeds the safe maximum {MAX_BACKEND_FETCH_BYTES}"
        );
    }
    if merge_window > MAX_CHUNK_FETCH_MERGE_WINDOW {
        bail!(
            "chunk fetch merge window {}ms exceeds the safe maximum {}ms",
            merge_window.as_millis(),
            MAX_CHUNK_FETCH_MERGE_WINDOW.as_millis()
        );
    }
    if backend_fetch_concurrency == 0 {
        bail!("backend fetch concurrency must be greater than zero");
    }
    if backend_fetch_concurrency > MAX_BACKEND_FETCH_CONCURRENCY {
        bail!(
            "backend fetch concurrency {backend_fetch_concurrency} exceeds the safe maximum {MAX_BACKEND_FETCH_CONCURRENCY}"
        );
    }
    let active_fetch_bytes = fetch_bytes
        .checked_mul(backend_fetch_concurrency as u64)
        .context("active backend fetch byte budget overflows u64")?;
    if active_fetch_bytes > MAX_BACKEND_ACTIVE_FETCH_BYTES {
        bail!(
            "active backend fetch budget {active_fetch_bytes} bytes exceeds the safe maximum {MAX_BACKEND_ACTIVE_FETCH_BYTES}"
        );
    }
    Ok(())
}

/// Chunked byte-range reader backed by an object store.
#[derive(Clone)]
pub struct ChunkedStore {
    cache: ChunkCache,
    coordinator: ChunkFetchCoordinator,
}

pub(crate) struct ChunkedStoreConfig {
    pub tileset_sources: String,
    pub chunk_size: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window: Duration,
    pub backend_fetch_concurrency: usize,
    pub backend_latency: BackendLatencyModel,
    pub chunk_cache_max_bytes: u64,
}

/// Whether a range read was satisfied immediately or waited for backend work.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ChunkReadSource {
    Cache,
    Backend,
}

/// Bytes returned by a chunked range read together with request provenance.
pub(crate) struct ChunkRead {
    pub bytes: Bytes,
    pub source: ChunkReadSource,
}

impl ChunkedStore {
    /// Creates a chunked object-store reader over the configured tileset sources.
    pub(crate) fn new(
        config: ChunkedStoreConfig,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        validate_chunked_store_limits(
            config.chunk_size,
            config.max_fetch_chunks,
            config.chunk_fetch_merge_window,
            config.backend_fetch_concurrency,
        )?;
        let backend_fetch_concurrency = config.backend_fetch_concurrency;
        let fetcher = ChunkFetcher::new(
            config.tileset_sources,
            config.chunk_size,
            backend_fetch_concurrency,
            config.backend_latency,
            registry,
            metrics.clone(),
        )?;
        // Admit far more requests than can execute at once so genuine bursts
        // coalesce, but keep admitted work (coordinator state, waiters, detached
        // tasks) bounded well below "one per distinct cold tileset".
        let max_admitted_fetches = backend_fetch_concurrency.saturating_mul(64);
        Ok(Self {
            cache: ChunkCache::new(config.chunk_cache_max_bytes),
            coordinator: ChunkFetchCoordinator::new(
                fetcher,
                config.max_fetch_chunks,
                config.chunk_fetch_merge_window,
                max_admitted_fetches,
                metrics,
            ),
        })
    }

    /// Returns the configured fixed chunk size in bytes.
    pub fn chunk_size(&self) -> u64 {
        self.coordinator.chunk_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.coordinator.received_bytes()
    }

    /// Reads a tileset byte range through the shared chunk cache and inflight fetcher.
    pub async fn read_bytes(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> std::result::Result<ChunkRead, ChunkFetchError> {
        if length == 0 {
            return Ok(ChunkRead {
                bytes: Bytes::new(),
                source: ChunkReadSource::Cache,
            });
        }

        let end = start.checked_add(length as u64).ok_or_else(|| {
            ChunkFetchError::Message(format!(
                "byte range overflow: start={start} length={length}"
            ))
        })?;
        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let mut missing_chunks = Vec::new();
        let mut resolved_chunks = HashMap::new();
        for chunk_index in chunk_range.clone() {
            if let Some(chunk) = self.chunk_cache_get(tileset_id, chunk_index) {
                self.coordinator.metrics().record_chunk_cache("hit");
                resolved_chunks.insert(chunk_index, chunk);
            } else {
                self.coordinator.metrics().record_chunk_cache("miss");
                missing_chunks.push(chunk_index);
            }
        }

        if !missing_chunks.is_empty() {
            let last_missing_chunk = *missing_chunks
                .last()
                .expect("missing_chunks must be non-empty here");
            let fetch_end =
                archive_len.unwrap_or_else(|| (last_missing_chunk + 1) * self.chunk_size());
            let fetched = self
                .coordinator
                .fetch_chunks(self.clone(), tileset_id, &missing_chunks, fetch_end)
                .await?;
            resolved_chunks.extend(fetched);
        }

        let source = if missing_chunks.is_empty() {
            ChunkReadSource::Cache
        } else {
            ChunkReadSource::Backend
        };
        let bytes = self
            .read_resolved_bytes(start, length, &resolved_chunks)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))?;
        Ok(ChunkRead { bytes, source })
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub fn chunk_cache_weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }

    pub fn chunk_cache_get(&self, tileset_id: &TilesetId, chunk_index: u64) -> Option<Bytes> {
        self.cache.get(&ChunkCacheKey::new(tileset_id, chunk_index))
    }

    fn read_resolved_bytes(
        &self,
        start: u64,
        length: usize,
        chunks: &HashMap<u64, Bytes>,
    ) -> Result<Bytes> {
        let end = start
            .checked_add(length as u64)
            .context("byte range overflow computing cached read")?;
        let chunk_range = byte_range_to_chunk_range(start, end, self.chunk_size());
        let chunk_offset = (start % self.chunk_size()) as usize;
        let first_chunk = *chunk_range.start();
        let last_chunk = *chunk_range.end();

        if first_chunk == last_chunk {
            let chunk = chunks
                .get(&first_chunk)
                .context("resolved chunk missing after fetch")?;
            self.coordinator
                .metrics()
                .record_chunk_cache("post_fetch_hit");
            if chunk_offset + length > chunk.len() {
                anyhow::bail!(
                    "cached chunk is shorter than requested range: chunk_index={} chunk_len={} chunk_offset={} length={}",
                    first_chunk,
                    chunk.len(),
                    chunk_offset,
                    length
                );
            }
            return Ok(chunk.slice(chunk_offset..chunk_offset + length));
        }

        let mut bytes = BytesMut::with_capacity(length);
        let mut remaining = length;
        let mut current_offset = chunk_offset;
        for chunk_idx in chunk_range {
            let chunk = chunks
                .get(&chunk_idx)
                .context("resolved chunk missing after fetch")?;
            self.coordinator
                .metrics()
                .record_chunk_cache("post_fetch_hit");
            let take = remaining.min(chunk.len().saturating_sub(current_offset));
            bytes.extend_from_slice(&chunk[current_offset..current_offset + take]);
            remaining -= take;
            current_offset = 0;
        }

        if remaining != 0 {
            anyhow::bail!("failed to reconstruct tileset bytes from chunk cache");
        }

        Ok(bytes.freeze())
    }

    pub fn cache_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: std::ops::Range<u64>,
        archive_len: u64,
        bytes: Bytes,
    ) -> Result<HashMap<u64, Bytes>> {
        let chunk_size = self.chunk_size();
        let range_start = chunk_range.start * chunk_size;
        let mut chunks = HashMap::with_capacity((chunk_range.end - chunk_range.start) as usize);

        for chunk_index in chunk_range.start..chunk_range.end {
            let absolute_start = chunk_index * chunk_size;
            let absolute_end = ((chunk_index + 1) * chunk_size).min(archive_len);
            let relative_start = (absolute_start - range_start) as usize;
            let relative_end = (absolute_end - range_start) as usize;
            let chunk = bytes.slice(relative_start..relative_end);
            self.cache
                .put(ChunkCacheKey::new(tileset_id, chunk_index), chunk.clone());
            chunks.insert(chunk_index, chunk);
        }

        Ok(chunks)
    }
}

/// Maps a byte range to the owning fixed-size chunk index range.
fn byte_range_to_chunk_range(start: u64, end: u64, chunk_size: u64) -> RangeInclusive<u64> {
    let first_chunk = chunk_index(start, chunk_size);
    let last_chunk = chunk_index(end.saturating_sub(1), chunk_size);
    first_chunk..=last_chunk
}

fn chunk_index(offset: u64, chunk_size: u64) -> u64 {
    offset / chunk_size
}

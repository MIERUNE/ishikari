//! Object-store-backed chunked byte-range reader.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Context, Result, anyhow};
use bytes::{Bytes, BytesMut};
use object_store::{ObjectStore, ObjectStoreExt, parse_url_opts, path::Path as ObjectPath};
use tracing::debug;
use url::Url;

use crate::{
    interned_str::InternedStr,
};

use super::{
    cache::{ChunkCache, ChunkCacheKey},
    fetch_aggregator::{ChunkFetchBackend, ChunkFetchError, FetchAggregator},
};

const MAX_CHUNK_GAP: u64 = 1;
const BACKEND_FETCH_TIMEOUT: Duration = Duration::from_secs(10);

/// Chunked byte-range reader backed by an object store.
#[derive(Clone)]
pub struct ChunkedStore {
    node_id: String,
    object_store: Arc<dyn ObjectStore>,
    base_path: ObjectPath,
    chunk_cache: ChunkCache,
    fetch_aggregator: FetchAggregator,
    chunk_size_bytes: u64,
    max_fetch_chunks: u64,
    backend_fetch_delay: Duration,
}

impl ChunkedStore {
    /// Creates a chunked object-store reader rooted at the configured data URL.
    pub fn new(
        node_id: String,
        data_url: String,
        chunk_size_bytes: u64,
        max_fetch_chunks: u64,
        backend_fetch_delay_ms: u64,
        chunk_cache_max_bytes: u64,
    ) -> Result<Self> {
        let url = normalize_data_url(&data_url)?;
        let (object_store, base_path) = parse_url_opts(&url, std::env::vars())
            .with_context(|| format!("failed to parse object store URL {url}"))?;
        let object_store: Arc<dyn ObjectStore> = object_store.into();

        Ok(Self {
            node_id,
            object_store,
            base_path,
            chunk_cache: ChunkCache::new(chunk_cache_max_bytes),
            fetch_aggregator: FetchAggregator::default(),
            chunk_size_bytes,
            max_fetch_chunks,
            backend_fetch_delay: Duration::from_millis(backend_fetch_delay_ms),
        })
    }

    /// Returns the local node identifier used by backend logs.
    pub fn node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns the configured fixed chunk size in bytes.
    pub fn chunk_size_bytes(&self) -> u64 {
        self.chunk_size_bytes
    }

    /// Returns a cached object chunk when present.
    pub fn chunk_cache_get(&self, object_id: &InternedStr, chunk_index: u64) -> Option<Bytes> {
        self.chunk_cache
            .get(&ChunkCacheKey::new(object_id, chunk_index))
    }

    /// Reads a tileset byte range through the shared chunk cache and inflight fetcher.
    pub async fn read_bytes(
        &self,
        tileset_id: &InternedStr,
        start: u64,
        length: usize,
        archive_len: Option<u64>,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if length == 0 {
            return Ok(Bytes::new());
        }

        let end = start + length as u64;
        let chunk_indices = byte_range_chunk_indices(start, end, self.chunk_size_bytes);
        let missing_chunks: Vec<u64> = chunk_indices
            .iter()
            .copied()
            .filter(|chunk_index| self.chunk_cache_get(tileset_id, *chunk_index).is_none())
            .collect();

        if !missing_chunks.is_empty() {
            let last_missing_chunk = *missing_chunks
                .last()
                .expect("missing_chunks must be non-empty here");
            let fetch_end =
                archive_len.unwrap_or_else(|| chunk_end(last_missing_chunk, self.chunk_size_bytes));
            self.fetch_aggregator
                .ensure_chunks((*self).clone(), tileset_id, &missing_chunks, fetch_end)
                .await?;
        }

        self.read_cached_bytes(tileset_id, start, length)
            .map_err(|error| ChunkFetchError::Message(error.to_string()))
    }

    /// Ensures the required chunks are present locally before a range is reconstructed.
    pub async fn ensure_chunks(
        &self,
        tileset_id: &InternedStr,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<(), ChunkFetchError> {
        self.fetch_aggregator
            .ensure_chunks((*self).clone(), tileset_id, required_chunks, archive_len)
            .await
    }

    /// Reconstructs a previously fetched byte range from cached chunks.
    pub fn read_cached_bytes(
        &self,
        tileset_id: &InternedStr,
        start: u64,
        length: usize,
    ) -> Result<Bytes> {
        let chunk_indices =
            byte_range_chunk_indices(start, start + length as u64, self.chunk_size_bytes);
        let chunk_offset = (start % self.chunk_size_bytes) as usize;

        if chunk_indices.len() == 1 {
            let chunk = self
                .chunk_cache_get(tileset_id, chunk_indices[0])
                .context("chunk missing from cache after fetch")?;
            if chunk_offset + length > chunk.len() {
                anyhow::bail!(
                    "cached chunk is shorter than requested range: chunk_index={} chunk_len={} chunk_offset={} length={}",
                    chunk_indices[0],
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
        for chunk_idx in chunk_indices {
            let chunk = self
                .chunk_cache_get(tileset_id, chunk_idx)
                .context("chunk missing from cache after fetch")?;
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

    /// Adds an optional artificial delay before backend object-store reads.
    async fn sleep_before_backend_fetch(&self) {
        if self.backend_fetch_delay.is_zero() {
            return;
        }
        tokio::time::sleep(self.backend_fetch_delay).await;
    }

    /// Resolves an interned tileset id into an object-store path.
    fn object_path(&self, tileset_id: &InternedStr) -> ObjectPath {
        self.base_path.clone().join(format!("{tileset_id}.pmtiles"))
    }
}

impl ChunkFetchBackend for ChunkedStore {
    /// Returns the backend node identifier for chunk fetch logging.
    fn backend_node_id(&self) -> &str {
        &self.node_id
    }

    /// Returns whether the requested tileset chunk is already cached.
    fn chunk_present(&self, tileset_id: &InternedStr, chunk_index: u64) -> bool {
        self.chunk_cache_get(tileset_id, chunk_index).is_some()
    }

    /// Fetches tileset chunks from object storage into the shared chunk cache.
    fn fetch_chunk_batch(
        &self,
        tileset_id: &InternedStr,
        chunks: &BTreeSet<u64>,
        archive_len: u64,
        batch_age_ms: u64,
    ) -> impl std::future::Future<Output = std::result::Result<(), ChunkFetchError>> + Send {
        async move {
            if chunks.is_empty() {
                return Ok(());
            }

            let chunk_size_bytes = self.chunk_size_bytes;
            let path = self.object_path(tileset_id);
            for (start_chunk, end_chunk) in
                contiguous_chunk_ranges(chunks, self.max_fetch_chunks, MAX_CHUNK_GAP)
            {
                let range_start = start_chunk * chunk_size_bytes;
                let range_end = (end_chunk * chunk_size_bytes).min(archive_len);
                debug!(
                    node_id = %self.node_id,
                    tileset_id = %tileset_id,
                    start_chunk = start_chunk,
                    end_chunk = end_chunk,
                    prefetched_chunks = end_chunk - start_chunk,
                    prefetched_bytes = range_end - range_start,
                    batch_age_ms = batch_age_ms,
                    "fetching backend chunks"
                );
                self.sleep_before_backend_fetch().await;
                let bytes = tokio::time::timeout(
                    BACKEND_FETCH_TIMEOUT,
                    self.object_store.get_range(&path, range_start..range_end),
                )
                .await
                .map_err(|error| {
                    ChunkFetchError::Message(format!(
                        "timed out fetching chunk range from object store: path={path} range={range_start}..{range_end}: {error}"
                    ))
                })?
                .map_err(ChunkFetchError::from)?;
                let expected_len = (range_end - range_start) as usize;
                if bytes.len() != expected_len {
                    return Err(ChunkFetchError::Message(format!(
                        "short range read from object store: path={path} range={range_start}..{range_end} expected_bytes={expected_len} actual_bytes={}",
                        bytes.len()
                    )));
                }
                debug!(
                    node_id = %self.node_id,
                    tileset_id = %tileset_id,
                    start_chunk = start_chunk,
                    end_chunk = end_chunk,
                    backend_fetched_bytes = bytes.len(),
                    "fetched backend chunk bytes"
                );

                for chunk_index in start_chunk..end_chunk {
                    let absolute_start = chunk_index * chunk_size_bytes;
                    let absolute_end = ((chunk_index + 1) * chunk_size_bytes).min(archive_len);
                    let relative_start = (absolute_start - range_start) as usize;
                    let relative_end = (absolute_end - range_start) as usize;
                    self.chunk_cache.put(
                        ChunkCacheKey::new(tileset_id, chunk_index),
                        bytes.slice(relative_start..relative_end),
                    );
                }
            }

            Ok(())
        }
    }
}

/// Maps a byte range to the owning fixed-size chunk indices.
pub(crate) fn byte_range_chunk_indices(start: u64, end: u64, chunk_size_bytes: u64) -> Vec<u64> {
    let first_chunk = chunk_index(start, chunk_size_bytes);
    let last_chunk = chunk_index(end.saturating_sub(1), chunk_size_bytes);
    (first_chunk..=last_chunk).collect()
}

/// Returns the exclusive end offset of a chunk.
pub(crate) fn chunk_end(chunk_index: u64, chunk_size_bytes: u64) -> u64 {
    (chunk_index + 1) * chunk_size_bytes
}

/// Groups chunk indices into fetch ranges subject to gap and size limits.
fn contiguous_chunk_ranges(
    chunks: &BTreeSet<u64>,
    max_fetch_chunks: u64,
    max_chunk_gap: u64,
) -> Vec<(u64, u64)> {
    let mut ranges = Vec::new();
    let mut iter = chunks.iter().copied();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let max_fetch_chunks = max_fetch_chunks.max(1);
    let mut end = start + 1;

    for chunk in iter {
        if chunk <= end + max_chunk_gap && chunk + 1 - start <= max_fetch_chunks {
            end = chunk + 1;
            continue;
        }
        ranges.push((start, end));
        start = chunk;
        end = chunk + 1;
    }
    ranges.push((start, end));
    ranges
}

/// Maps a byte offset to the owning fixed-size chunk index.
fn chunk_index(offset: u64, chunk_size_bytes: u64) -> u64 {
    offset / chunk_size_bytes
}

/// Normalizes filesystem paths and URLs into an object_store-compatible URL.
fn normalize_data_url(data_url: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(data_url) {
        return Ok(url);
    }

    let path = std::fs::canonicalize(PathBuf::from(data_url))
        .with_context(|| format!("failed to resolve local data path {data_url}"))?;
    Url::from_directory_path(path)
        .map_err(|_| anyhow!("failed to convert local path to file:// URL"))
}

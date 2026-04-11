//! Per-tileset chunk caches used by chunked byte-range readers.

use bytes::Bytes;
use moka::sync::Cache;

use crate::interned_str::InternedStr;

const CHUNK_CACHE_MAX_BYTES: u64 = 1024 * 1024 * 1024;

/// Identifies a cached fixed-size chunk within an object.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ChunkCacheKey {
    pub tileset_id: InternedStr,
    pub chunk_index: u64,
}

impl ChunkCacheKey {
    /// Builds a chunk cache key from an object id and fixed-size chunk index.
    pub fn new(tileset_id: &InternedStr, chunk_index: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            chunk_index,
        }
    }
}

/// Per-node cache of backend tileset chunks.
#[derive(Clone)]
pub struct ChunkCache {
    cache: Cache<ChunkCacheKey, Bytes>,
}

impl ChunkCache {
    /// Creates a chunk cache with a byte-based capacity limit.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes.min(CHUNK_CACHE_MAX_BYTES))
            .weigher(chunk_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached chunk if present.
    pub fn get(&self, key: &ChunkCacheKey) -> Option<Bytes> {
        self.cache.get(key)
    }

    /// Inserts or replaces a cached chunk.
    pub fn put(&self, key: ChunkCacheKey, data: Bytes) {
        self.cache.insert(key, data);
    }

    /// Returns the current weighted byte size of the chunk cache.
    pub fn weighted_size(&self) -> u64 {
        self.cache.weighted_size()
    }
}

/// Estimates the weight of a cached chunk entry.
fn chunk_cache_weight(key: &ChunkCacheKey, value: &Bytes) -> u32 {
    let key_size = std::mem::size_of_val(key);
    let total = key_size.saturating_add(value.len());
    total.min(u32::MAX as usize) as u32
}

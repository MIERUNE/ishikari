//! Per-node L1 caches for tiles and small resources.

use std::sync::Arc;

use moka::sync::Cache;

use crate::{interned_str::TilesetId, tilesets::TilesetInfo};

/// Identifies a cached tile payload within a tileset.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct TileCacheKey {
    pub tileset_id: TilesetId,
    pub tile_id: u64,
}

impl TileCacheKey {
    /// Builds a tile cache key from a tileset id and PMTiles tile id.
    pub fn new(tileset_id: &TilesetId, tile_id: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            tile_id,
        }
    }
}

/// Identifies a cached resource.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ResourceCacheKey {
    TilesetInfo { tileset_id: TilesetId },
    // Reserved for future resource kinds:
    // - Style (style_id)
    // - SpriteImage (style_id, sprite_id)
    // - SpriteSdf (style_id, sprite_id)
    // - SpriteJson (style_id, sprite_id)
    // - Glyphs (fontstack, range)
}

impl ResourceCacheKey {
    /// Builds the cache key for a cached tileset metadata resource.
    pub fn tileset_info(tileset_id: &TilesetId) -> Self {
        Self::TilesetInfo {
            tileset_id: tileset_id.clone(),
        }
    }
}

/// Cacheable resources.
#[derive(Clone)]
pub enum Resource {
    TilesetInfo(Arc<TilesetInfo>),
}

/// Cache entry for a tile, including negative lookups.
#[derive(Clone)]
pub enum CachedTile {
    Found(bytes::Bytes),
    NotFound,
}

/// Per-node L1 cache of tile payloads.
#[derive(Clone)]
pub struct TileCache {
    cache: Cache<TileCacheKey, CachedTile>,
}

/// Per-node cache of resources such as [`TilesetInfo`].
#[derive(Clone)]
pub struct ResourceCache {
    cache: Cache<ResourceCacheKey, Resource>,
}

impl TileCache {
    /// Creates a tile cache with a byte-based capacity limit.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(tile_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached tile payload if present.
    pub fn get(&self, key: &TileCacheKey) -> Option<CachedTile> {
        self.cache.get(key)
    }

    /// Inserts or replaces a cached tile payload.
    pub fn put(&self, key: TileCacheKey, value: CachedTile) {
        self.cache.insert(key, value);
    }
}

impl ResourceCache {
    /// Creates a resource cache with a byte-based capacity limit.
    pub fn new(max_capacity_bytes: u64) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(resource_cache_weight)
            .build();
        Self { cache }
    }

    /// Returns a cached tileset metadata bundle if present.
    pub fn get_tileset_info(&self, tileset_id: &TilesetId) -> Option<Arc<TilesetInfo>> {
        let key = ResourceCacheKey::tileset_info(tileset_id);
        match self.cache.get(&key) {
            Some(Resource::TilesetInfo(info)) => Some(info),
            None => None,
        }
    }

    /// Inserts or replaces a cached tileset metadata bundle.
    pub fn put_tileset_info(&self, tileset_id: &TilesetId, info: Arc<TilesetInfo>) {
        self.cache.insert(
            ResourceCacheKey::tileset_info(tileset_id),
            Resource::TilesetInfo(info),
        );
    }
}

/// Estimates the weight of a cached tile entry.
fn tile_cache_weight(key: &TileCacheKey, value: &CachedTile) -> u32 {
    let value_size = match value {
        CachedTile::Found(bytes) => bytes.len(),
        CachedTile::NotFound => 0,
    };
    let total = std::mem::size_of_val(key).saturating_add(value_size);
    total.min(u32::MAX as usize) as u32
}

/// Estimates the weight of a cached resource entry.
fn resource_cache_weight(key: &ResourceCacheKey, value: &Resource) -> u32 {
    match (key, value) {
        (ResourceCacheKey::TilesetInfo { tileset_id }, Resource::TilesetInfo(info)) => {
            let total = std::mem::size_of_val(tileset_id).saturating_add(info.approx_byte_size());
            total.min(u32::MAX as usize) as u32
        }
    }
}

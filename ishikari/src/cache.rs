//! Per-node L1 caches for tiles and small resources.

use std::sync::Arc;
use std::time::{Duration, Instant};

use moka::{Expiry, sync::Cache};

use crate::{interned::TilesetId, storage::TilesetInfo};

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
    Found {
        bytes: bytes::Bytes,
        content_type: &'static str,
        content_encoding: Option<&'static str>,
    },
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

/// Per-entry expiry policy for the tile cache.
///
/// Positive (`Found`) entries never expire on their own — PMTiles archives are
/// treated as immutable, so a tile that exists keeps its bytes until capacity
/// eviction. Negative (`NotFound`) entries expire after `negative_ttl`: absence
/// is the *mutable* state (a tile can be published later, or a whole archive
/// republished), so bounding the negative lifetime caps how long a newly-added
/// tile stays hidden — and stops a caller from poisoning the cache with lookups
/// of not-yet-existing tiles to delay their rollout. A cache *hit* does not
/// extend the entry (`expire_after_read` keeps the default), so hammering an
/// absent tile cannot keep its negative entry alive past `negative_ttl`.
struct TileExpiry {
    negative_ttl: Duration,
}

impl TileExpiry {
    fn ttl_for(&self, value: &CachedTile) -> Option<Duration> {
        match value {
            CachedTile::Found { .. } => None,
            CachedTile::NotFound => Some(self.negative_ttl),
        }
    }
}

impl Expiry<TileCacheKey, CachedTile> for TileExpiry {
    fn expire_after_create(
        &self,
        _key: &TileCacheKey,
        value: &CachedTile,
        _created_at: Instant,
    ) -> Option<Duration> {
        self.ttl_for(value)
    }

    fn expire_after_update(
        &self,
        _key: &TileCacheKey,
        value: &CachedTile,
        _updated_at: Instant,
        _current: Option<Duration>,
    ) -> Option<Duration> {
        // Recompute from the new value so a NotFound→Found transition (tile
        // published) clears the short TTL and vice versa.
        self.ttl_for(value)
    }
}

impl TileCache {
    /// Creates a tile cache with a byte-based capacity limit. Negative entries
    /// expire after `negative_ttl`; positive entries live until eviction.
    pub fn new(max_capacity_bytes: u64, negative_ttl: Duration) -> Self {
        let cache = Cache::builder()
            .max_capacity(max_capacity_bytes)
            .weigher(tile_cache_weight)
            .expire_after(TileExpiry { negative_ttl })
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

    /// Returns the current weighted byte size of the tile cache.
    ///
    /// Flushes pending maintenance first so the value reflects recent inserts
    /// and evictions rather than moka's lazily-updated estimate.
    pub fn weighted_size(&self) -> u64 {
        self.cache.run_pending_tasks();
        self.cache.weighted_size()
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
        self.cache.get(&key).map(|Resource::TilesetInfo(info)| info)
    }

    /// Inserts or replaces a cached tileset metadata bundle.
    pub fn put_tileset_info(&self, tileset_id: &TilesetId, info: Arc<TilesetInfo>) {
        self.cache.insert(
            ResourceCacheKey::tileset_info(tileset_id),
            Resource::TilesetInfo(info),
        );
    }
}

/// Minimum weight charged for any tile-cache entry. `TileCacheKey`'s inline
/// size excludes the heap bytes of its interned tileset id (up to 256 bytes),
/// and every entry also pays moka's per-entry bookkeeping. Without a floor a
/// flood of distinct negative (`NotFound`) lookups — enumerating unique valid
/// tileset ids over the public path — would blow far past the byte capacity
/// while the cache reports near-zero usage.
pub const MIN_TILE_CACHE_ENTRY_WEIGHT: u32 = 128;

/// Logical byte weight of a tile-cache entry: the inline key, the interned
/// tileset id bytes, and the payload (`None` for a negative entry), floored at
/// [`MIN_TILE_CACHE_ENTRY_WEIGHT`]. Shared with the modeled simulator so its
/// capacity sweeps charge the same weights as production.
pub fn tile_cache_logical_weight(tileset_id: &str, payload_len: Option<usize>) -> u32 {
    let total = std::mem::size_of::<TileCacheKey>()
        .saturating_add(tileset_id.len())
        .saturating_add(payload_len.unwrap_or(0));
    (total.min(u32::MAX as usize) as u32).max(MIN_TILE_CACHE_ENTRY_WEIGHT)
}

/// Estimates the weight of a cached tile entry.
fn tile_cache_weight(key: &TileCacheKey, value: &CachedTile) -> u32 {
    let payload_len = match value {
        CachedTile::Found { bytes, .. } => Some(bytes.len()),
        CachedTile::NotFound => None,
    };
    tile_cache_logical_weight(key.tileset_id.as_str(), payload_len)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn found() -> CachedTile {
        CachedTile::Found {
            bytes: bytes::Bytes::from_static(b"tile"),
            content_type: "application/x-protobuf",
            content_encoding: None,
        }
    }

    #[test]
    fn only_negative_entries_expire() {
        let expiry = TileExpiry {
            negative_ttl: Duration::from_secs(60),
        };
        // Absent tiles get the short TTL; present tiles never expire on their own.
        assert_eq!(
            expiry.ttl_for(&CachedTile::NotFound),
            Some(Duration::from_secs(60))
        );
        assert_eq!(expiry.ttl_for(&found()), None);
    }

    #[test]
    fn logical_weight_helper_is_the_shared_production_contract() {
        // The modeled simulator calls this same helper, so its capacity sweeps
        // charge production weights. A negative entry is floored; identifier
        // bytes and payload add on top.
        assert_eq!(
            tile_cache_logical_weight("demo/streets", None),
            MIN_TILE_CACHE_ENTRY_WEIGHT
        );
        let big_payload = tile_cache_logical_weight("demo/streets", Some(64 * 1024));
        assert!(big_payload > MIN_TILE_CACHE_ENTRY_WEIGHT);
        assert!(
            tile_cache_logical_weight(&"a".repeat(256), None)
                > tile_cache_logical_weight("a", None)
        );
        // The struct weigher agrees with the helper for both variants.
        let key = TileCacheKey::new(&TilesetId::new_unchecked("demo/streets"), 1);
        assert_eq!(
            tile_cache_weight(&key, &CachedTile::NotFound),
            tile_cache_logical_weight("demo/streets", None)
        );
    }

    #[test]
    fn negative_entries_are_charged_a_floor_weight() {
        let key = TileCacheKey::new(&TilesetId::new_unchecked("demo/streets"), 7);
        // A NotFound carries no bytes but still costs key + interned id + moka
        // bookkeeping, so it must be charged at least the floor.
        assert_eq!(
            tile_cache_weight(&key, &CachedTile::NotFound),
            MIN_TILE_CACHE_ENTRY_WEIGHT
        );
        // A long tileset id pushes the weight above the floor via its bytes.
        let long = "a".repeat(256);
        let long_key = TileCacheKey::new(&TilesetId::new_unchecked(&long), 7);
        assert!(tile_cache_weight(&long_key, &CachedTile::NotFound) > MIN_TILE_CACHE_ENTRY_WEIGHT);
    }

    #[test]
    fn negative_entries_are_evicted_under_a_tight_byte_budget() {
        // 100 negative entries at a 128-byte floor need ~12.8 KiB; a 4 KiB cap
        // must therefore evict rather than retain them all — which a weight of 0
        // (the previous behavior) would wrongly allow.
        let cache = TileCache::new(4 * 1024, Duration::from_secs(60));
        let tileset = TilesetId::new_unchecked("demo/streets");
        for tile_id in 0..100 {
            cache.put(TileCacheKey::new(&tileset, tile_id), CachedTile::NotFound);
        }
        assert!(
            cache.weighted_size() <= 4 * 1024,
            "weighted size {} exceeds the byte budget",
            cache.weighted_size()
        );
        let retained = (0..100)
            .filter(|&tile_id| cache.get(&TileCacheKey::new(&tileset, tile_id)).is_some())
            .count();
        assert!(
            retained < 100,
            "expected eviction, retained {retained} entries"
        );
    }

    #[test]
    fn expiry_recomputes_on_update() {
        let expiry = TileExpiry {
            negative_ttl: Duration::from_secs(30),
        };
        let now = Instant::now();
        let key = TileCacheKey::new(&TilesetId::new_unchecked("demo/streets"), 42);
        // A NotFound→Found update (tile published) must clear the negative TTL.
        assert_eq!(
            expiry.expire_after_update(&key, &found(), now, Some(Duration::from_secs(30))),
            None
        );
        // A Found→NotFound update must (re)apply the negative TTL.
        assert_eq!(
            expiry.expire_after_update(&key, &CachedTile::NotFound, now, None),
            Some(Duration::from_secs(30))
        );
    }
}

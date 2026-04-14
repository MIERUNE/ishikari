//! Caches for PMTiles archive bootstraps and leaf directories.

use std::sync::Arc;

use moka::sync::Cache;

use super::{
    format::{Directory, Header},
    metadata::Metadata,
};
use crate::interned::TilesetId;

const ARCHIVE_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;
const LEAF_CACHE_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Shared caches for per-tileset PMTiles bootstraps and leaf directories.
#[derive(Clone)]
pub struct ArchiveCache {
    archives: Cache<TilesetId, ArchiveBootstrap>,
    leafs: Cache<LeafCacheKey, Arc<Directory>>,
}

/// Cached PMTiles header, root directory, and optional metadata section.
#[derive(Clone)]
pub struct ArchiveBootstrap {
    pub header: Header,
    pub root: Arc<Directory>,
    pub metadata: Option<Arc<Metadata>>,
}

impl ArchiveBootstrap {
    /// Builds an archive bootstrap from header, root directory, and optional metadata.
    pub fn new(header: Header, root: Arc<Directory>, metadata: Option<Arc<Metadata>>) -> Self {
        Self {
            header,
            root,
            metadata,
        }
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct LeafCacheKey {
    pub tileset_id: TilesetId,
    pub offset: u64,
}

impl LeafCacheKey {
    /// Builds a leaf cache key from a tileset id and absolute leaf offset.
    pub fn new(tileset_id: &TilesetId, offset: u64) -> Self {
        Self {
            tileset_id: tileset_id.clone(),
            offset,
        }
    }
}

impl ArchiveCache {
    /// Creates the shared PMTiles metadata caches.
    pub fn new() -> Self {
        Self {
            archives: Cache::builder()
                .max_capacity(ARCHIVE_CACHE_MAX_BYTES)
                .weigher(|tileset_id: &TilesetId, archive: &ArchiveBootstrap| -> u32 {
                    (std::mem::size_of_val(tileset_id)
                        + std::mem::size_of::<Header>()
                        + archive.root.approx_byte_size()
                        + archive
                            .metadata
                            .as_ref()
                            .map_or(0, |metadata| metadata.approx_byte_size()))
                    .min(u32::MAX as usize) as u32
                })
                .build(),
            leafs: Cache::builder()
                .max_capacity(LEAF_CACHE_MAX_BYTES)
                .weigher(|key: &LeafCacheKey, directory: &Arc<Directory>| -> u32 {
                    (std::mem::size_of_val(key) + directory.approx_byte_size())
                        .min(u32::MAX as usize) as u32
                })
                .build(),
        }
    }

    /// Returns the cached bootstrap for a tileset if present.
    pub fn get(&self, tileset_id: &TilesetId) -> Option<ArchiveBootstrap> {
        self.archives.get(tileset_id)
    }

    /// Inserts or replaces the cached header/root index for a tileset.
    pub fn put(&self, tileset_id: &TilesetId, archive: ArchiveBootstrap) {
        self.archives.insert(tileset_id.clone(), archive);
    }

    /// Replaces the cached metadata for a tileset while preserving header and root.
    pub fn put_metadata(&self, tileset_id: &TilesetId, metadata: Arc<Metadata>) {
        if let Some(mut archive) = self.archives.get(tileset_id) {
            archive.metadata = Some(metadata);
            self.archives.insert(tileset_id.clone(), archive);
        }
    }

    /// Returns a cached leaf directory if present.
    pub fn get_leaf(&self, key: &LeafCacheKey) -> Option<Arc<Directory>> {
        self.leafs.get(key)
    }

    /// Inserts or replaces a cached leaf directory.
    pub fn put_leaf(&self, key: LeafCacheKey, directory: Arc<Directory>) {
        self.leafs.insert(key, directory);
    }
}

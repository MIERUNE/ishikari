use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    fs::File,
    future::Future,
    io::{Read, Seek, SeekFrom},
    path::PathBuf,
    sync::Arc,
};

use anyhow::{Context, Result, bail, ensure};
use bytes::Bytes;
use ishikari::{
    membership::Peer,
    metrics::NodeMetricsSnapshot,
    pmtiles::{
        BootstrapTransfer, Reader as PmtilesReader, Storage as PmtilesStorage, StorageError,
        TileAccessPlan, TileCoord, TileId,
    },
    storage::{HrwRouter, TilesetId, plan_chunk_fetch_ranges},
};
use moka::{policy::EvictionPolicy, sync::Cache};

use crate::{
    TraceEntry,
    cluster::{ClusterConfig, simulated_peer, simulated_peers},
    report::{ClusterObservation, NodeReport, SchedulerReport, SimReport},
};

const MAX_PRODUCTION_CHUNK_CACHE_BYTES: u64 = 1024 * 1024 * 1024;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct TileKey {
    tileset_id: TilesetId,
    tile_id: u64,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
struct ChunkKey {
    tileset_id: TilesetId,
    chunk_index: u64,
}

#[derive(Clone, Copy)]
enum ModeledTile {
    Found { length: u32, weight: u32 },
    NotFound { weight: u32 },
}

impl ModeledTile {
    fn weight(self) -> u32 {
        match self {
            Self::Found { weight, .. } | Self::NotFound { weight } => weight,
        }
    }

    fn length(self) -> Option<u32> {
        match self {
            Self::Found { length, .. } => Some(length),
            Self::NotFound { .. } => None,
        }
    }
}

#[derive(Clone, Copy)]
struct ModeledChunk {
    weight: u32,
}

/// PMTiles access plans for every unique tile in a trace.
pub struct TileCatalog {
    entries: HashMap<TileKey, Option<TileAccessPlan>>,
}

impl TileCatalog {
    /// Builds a catalog from a local PMTiles root without reading tile payloads.
    pub async fn build(tileset_source: &str, trace: &[TraceEntry]) -> Result<Self> {
        let storage = LocalCatalogStorage::new(tileset_source)?;
        let reader = Arc::new(PmtilesReader::new(storage)?);
        let mut keys = BTreeSet::new();
        for entry in trace {
            keys.insert(tile_key(entry)?);
        }

        let mut entries = HashMap::with_capacity(keys.len());
        for key in keys {
            let plan = reader
                .plan_tile_access(&key.tileset_id, key.tile_id)
                .await
                .with_context(|| {
                    format!("resolve modeled tile {} id={}", key.tileset_id, key.tile_id)
                })?;
            entries.insert(key, plan);
        }
        Ok(Self { entries })
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn get(&self, key: &TileKey) -> Option<&TileAccessPlan> {
        self.entries.get(key).and_then(Option::as_ref)
    }

    fn contains(&self, key: &TileKey) -> bool {
        self.entries.contains_key(key)
    }
}

#[derive(Clone)]
struct LocalCatalogStorage {
    root: Arc<PathBuf>,
}

impl LocalCatalogStorage {
    fn new(source: &str) -> Result<Self> {
        ensure!(!source.is_empty(), "modeled tileset source is empty");
        if source.contains(';') || source.contains('=') || source.contains("://") {
            bail!("modeled cache currently requires one local tileset root, got {source}");
        }
        Ok(Self {
            root: Arc::new(PathBuf::from(source)),
        })
    }

    fn archive_path(&self, tileset_id: &TilesetId) -> PathBuf {
        self.root.join(format!("{tileset_id}.pmtiles"))
    }
}

impl PmtilesStorage for LocalCatalogStorage {
    async fn read_range(
        &self,
        tileset_id: &TilesetId,
        start: u64,
        length: usize,
        _archive_len: Option<u64>,
    ) -> Result<Bytes, StorageError> {
        let path = self.archive_path(tileset_id);
        let mut file = match File::open(&path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(StorageError::NotFound);
            }
            Err(error) => {
                return Err(StorageError::Message(format!(
                    "open {}: {error}",
                    path.display()
                )));
            }
        };
        file.seek(SeekFrom::Start(start)).map_err(|error| {
            StorageError::Message(format!("seek {} to {start}: {error}", path.display()))
        })?;
        let mut bytes = vec![0; length];
        file.read_exact(&mut bytes).map_err(|error| {
            StorageError::Message(format!(
                "read {} range {start}..{}: {error}",
                path.display(),
                start.saturating_add(length as u64)
            ))
        })?;
        Ok(Bytes::from(bytes))
    }

    fn fetch_bootstrap_bytes<'a>(
        &'a self,
        _tileset_id: &'a TilesetId,
        _include_metadata: bool,
    ) -> impl Future<Output = Result<Option<BootstrapTransfer>>> + Send + 'a {
        std::future::ready(Ok(None))
    }

    fn fetch_leaf_bytes<'a>(
        &'a self,
        _tileset_id: &'a TilesetId,
        _offset: u64,
        _length: usize,
    ) -> impl Future<Output = Result<Option<Bytes>>> + Send + 'a {
        std::future::ready(Ok(None))
    }
}

struct ModelNode {
    id: String,
    tile_cache: Cache<TileKey, ModeledTile>,
    chunk_cache: Cache<ChunkKey, ModeledChunk>,
    loaded_bootstraps: HashSet<TilesetId>,
    loaded_leaves: HashSet<(TilesetId, u64)>,
    requests: u64,
    served_bytes: u64,
    by_source: BTreeMap<String, u64>,
    backend_bytes: u64,
    metrics: NodeMetricsSnapshot,
}

impl ModelNode {
    fn new(id: String, config: &ClusterConfig) -> Self {
        Self {
            id,
            tile_cache: Cache::builder()
                .max_capacity(config.tile_cache_max_bytes)
                .weigher(|_key: &TileKey, value: &ModeledTile| value.weight())
                .build(),
            chunk_cache: Cache::builder()
                .eviction_policy(EvictionPolicy::lru())
                .max_capacity(
                    config
                        .chunk_cache_max_bytes
                        .min(MAX_PRODUCTION_CHUNK_CACHE_BYTES),
                )
                .weigher(|_key: &ChunkKey, value: &ModeledChunk| value.weight)
                .build(),
            loaded_bootstraps: HashSet::new(),
            loaded_leaves: HashSet::new(),
            requests: 0,
            served_bytes: 0,
            by_source: BTreeMap::new(),
            backend_bytes: 0,
            metrics: NodeMetricsSnapshot::default(),
        }
    }

    fn put_tile(&self, key: TileKey, length: Option<u32>) {
        // Share the production logical-weight formula (interned id bytes plus a
        // per-entry floor) so capacity sweeps evict like production instead of
        // charging negative/small entries only their inline-key bytes.
        let weight = ishikari::tile_cache_logical_weight(
            key.tileset_id.as_str(),
            length.map(|length| length as usize),
        );
        let value = length.map_or(ModeledTile::NotFound { weight }, |length| {
            ModeledTile::Found { length, weight }
        });
        self.tile_cache.insert(key, value);
    }

    fn finish_maintenance(&self) {
        self.tile_cache.run_pending_tasks();
        self.chunk_cache.run_pending_tasks();
    }
}

struct PendingTile {
    entry_node: usize,
    owner_node: usize,
    key: TileKey,
    plan: TileAccessPlan,
    backend_waited: bool,
}

#[derive(Clone)]
struct PlannedRange {
    node: usize,
    tileset_id: TilesetId,
    offset: u64,
    length: u32,
    archive_len: u64,
    request_indices: Vec<usize>,
}

/// Metadata-only cluster model with production HRW and Moka eviction policies.
pub struct ModeledCluster {
    config: ClusterConfig,
    catalog: Arc<TileCatalog>,
    peers: Vec<Peer>,
    router: HrwRouter,
    nodes: Vec<ModelNode>,
    retired_nodes: Vec<NodeReport>,
    next_node_index: usize,
    report: SimReport,
}

impl ModeledCluster {
    pub fn new(config: ClusterConfig, catalog: impl Into<Arc<TileCatalog>>) -> Result<Self> {
        config.validate()?;
        let next_node_index = config.node_count;
        let peers = simulated_peers(config.node_count);
        let nodes = peers
            .iter()
            .map(|peer| ModelNode::new(peer.id.clone(), &config))
            .collect();
        Ok(Self {
            router: HrwRouter::new(config.candidate_count, config.tile_group_size),
            config,
            catalog: catalog.into(),
            peers,
            nodes,
            retired_nodes: Vec::new(),
            next_node_index,
            report: SimReport::default(),
        })
    }

    pub fn catalog_len(&self) -> usize {
        self.catalog.len()
    }

    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn active_node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.id.clone()).collect()
    }

    pub fn add_node(&mut self) -> Result<String> {
        let peer = simulated_peer(self.next_node_index)?;
        let id = peer.id.clone();
        self.nodes.push(ModelNode::new(id.clone(), &self.config));
        self.peers.push(peer);
        self.next_node_index += 1;
        Ok(id)
    }

    pub fn remove_node(&mut self, id: &str) -> Result<()> {
        ensure!(self.nodes.len() > 1, "cannot remove the last active node");
        let index = self
            .nodes
            .iter()
            .position(|node| node.id == id)
            .with_context(|| format!("active node {id} does not exist"))?;
        let node = self.nodes.remove(index);
        self.peers.remove(index);
        self.retired_nodes.push(modeled_node_report(&node, false));
        Ok(())
    }

    pub fn serve(&mut self, entry: &TraceEntry) -> Result<()> {
        let entry_node = entry
            .entry_node
            .context("trace has no entry_node; generate it with node_count > 0")?;
        self.serve_on(entry, entry_node)
    }

    pub fn serve_on(&mut self, entry: &TraceEntry, entry_node: usize) -> Result<()> {
        self.serve_viewport_on(std::slice::from_ref(entry), &[entry_node])
    }

    pub fn serve_viewport(&mut self, entries: &[TraceEntry]) -> Result<()> {
        let entry_nodes = entries
            .iter()
            .map(|entry| {
                entry
                    .entry_node
                    .context("trace has no entry_node; generate it with node_count > 0")
            })
            .collect::<Result<Vec<_>>>()?;
        self.serve_viewport_on(entries, &entry_nodes)
    }

    pub fn serve_viewport_on(
        &mut self,
        entries: &[TraceEntry],
        entry_nodes: &[usize],
    ) -> Result<()> {
        ensure!(
            entries.len() == entry_nodes.len(),
            "entry node assignment length does not match viewport"
        );
        let mut pending = Vec::new();
        for (entry, &entry_node) in entries.iter().zip(entry_nodes) {
            self.prepare(entry, entry_node, &mut pending)?;
        }

        self.process_bootstraps(&mut pending);
        self.process_leaves(&mut pending);
        self.process_tile_ranges(&mut pending);

        for tile in pending {
            let length = tile.plan.tile.length;
            self.nodes[tile.owner_node].put_tile(tile.key.clone(), Some(length));
            if tile.entry_node != tile.owner_node {
                if self.config.cache_peer_tiles {
                    self.nodes[tile.entry_node].put_tile(tile.key, Some(length));
                }
                self.report.peer_requests += 1;
                self.report.peer_bytes += u64::from(length);
            }
            let source = match (tile.entry_node == tile.owner_node, tile.backend_waited) {
                (true, false) => "self_cache",
                (true, true) => "self_backend",
                (false, false) => "peer_cache",
                (false, true) => "peer_backend",
            };
            self.record(tile.entry_node, source, Some(u64::from(length)));
        }
        self.finish_cache_maintenance();
        Ok(())
    }

    fn finish_cache_maintenance(&self) {
        for node in &self.nodes {
            node.finish_maintenance();
        }
    }

    fn prepare(
        &mut self,
        entry: &TraceEntry,
        entry_node: usize,
        pending: &mut Vec<PendingTile>,
    ) -> Result<()> {
        ensure!(
            entry_node < self.nodes.len(),
            "entry_node {entry_node} is outside the modeled cluster"
        );
        self.nodes[entry_node].requests += 1;
        self.report.requests += 1;
        let key = tile_key(entry)?;

        if let Some(cached) = self.nodes[entry_node].tile_cache.get(&key) {
            let source = if cached.length().is_some() {
                self.report.l1_cache_hits += 1;
                "self_cache"
            } else {
                self.report.negative_cache_hits += 1;
                "miss"
            };
            self.record(entry_node, source, cached.length().map(u64::from));
            return Ok(());
        }

        let owner_node = self
            .router
            .route_tile(&self.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                self.nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .context("HRW returned no modeled owner")?;

        if owner_node != entry_node
            && let Some(cached) = self.nodes[owner_node].tile_cache.get(&key)
        {
            if let Some(length) = cached.length() {
                if self.config.cache_peer_tiles {
                    self.nodes[entry_node].put_tile(key, Some(length));
                }
                self.report.peer_requests += 1;
                self.report.peer_bytes += u64::from(length);
                self.record(entry_node, "peer_cache", Some(u64::from(length)));
            } else {
                self.nodes[entry_node].put_tile(key, None);
                self.record(entry_node, "miss", None);
            }
            return Ok(());
        }

        if !self.catalog.contains(&key) {
            bail!(
                "modeled catalog is missing {} id={}",
                key.tileset_id,
                key.tile_id
            );
        }
        let Some(plan) = self.catalog.get(&key).cloned() else {
            self.nodes[owner_node].put_tile(key.clone(), None);
            self.nodes[entry_node].put_tile(key, None);
            self.record(entry_node, "miss", None);
            return Ok(());
        };

        pending.push(PendingTile {
            entry_node,
            owner_node,
            key,
            plan,
            backend_waited: false,
        });
        Ok(())
    }

    fn process_bootstraps(&mut self, pending: &mut [PendingTile]) {
        let ranges = pending
            .iter()
            .enumerate()
            .filter(|(_, tile)| {
                !self.nodes[tile.owner_node]
                    .loaded_bootstraps
                    .contains(&tile.key.tileset_id)
            })
            .map(|(request_index, tile)| PlannedRange {
                node: tile.owner_node,
                tileset_id: tile.key.tileset_id.clone(),
                offset: tile.plan.bootstrap.offset,
                length: tile.plan.bootstrap.length,
                archive_len: tile.plan.tile.archive_len,
                request_indices: vec![request_index],
            })
            .collect::<Vec<_>>();
        for request_index in self.process_ranges(&ranges, true) {
            pending[request_index].backend_waited = true;
        }
        for tile in pending {
            self.nodes[tile.owner_node]
                .loaded_bootstraps
                .insert(tile.key.tileset_id.clone());
        }
    }

    fn process_leaves(&mut self, pending: &mut [PendingTile]) {
        let max_depth = pending
            .iter()
            .map(|tile| tile.plan.leaves.len())
            .max()
            .unwrap_or(0);
        for depth in 0..max_depth {
            let mut ranges = Vec::new();
            for (request_index, tile) in pending.iter().enumerate() {
                let Some(leaf) = tile.plan.leaves.get(depth) else {
                    continue;
                };
                let loaded_key = (tile.key.tileset_id.clone(), leaf.offset);
                if self.nodes[tile.owner_node]
                    .loaded_leaves
                    .contains(&loaded_key)
                {
                    continue;
                }
                ranges.push(PlannedRange {
                    node: tile.owner_node,
                    tileset_id: tile.key.tileset_id.clone(),
                    offset: leaf.offset,
                    length: leaf.length,
                    archive_len: tile.plan.tile.archive_len,
                    request_indices: vec![request_index],
                });
            }
            for request_index in self.process_ranges(&ranges, false) {
                pending[request_index].backend_waited = true;
            }
            for tile in pending.iter() {
                if let Some(leaf) = tile.plan.leaves.get(depth) {
                    self.nodes[tile.owner_node]
                        .loaded_leaves
                        .insert((tile.key.tileset_id.clone(), leaf.offset));
                }
            }
        }
    }

    fn process_tile_ranges(&mut self, pending: &mut [PendingTile]) {
        let ranges: Vec<_> = pending
            .iter()
            .enumerate()
            .map(|(request_index, tile)| PlannedRange {
                node: tile.owner_node,
                tileset_id: tile.key.tileset_id.clone(),
                offset: tile.plan.tile.offset,
                length: tile.plan.tile.length,
                archive_len: tile.plan.tile.archive_len,
                request_indices: vec![request_index],
            })
            .collect();
        for request_index in self.process_ranges(&ranges, false) {
            pending[request_index].backend_waited = true;
        }
    }

    fn process_ranges(
        &mut self,
        ranges: &[PlannedRange],
        bootstrap_phase: bool,
    ) -> BTreeSet<usize> {
        let mut backend_waiters = BTreeSet::new();
        let mut grouped: BTreeMap<(usize, TilesetId), Vec<&PlannedRange>> = BTreeMap::new();
        for range in ranges {
            grouped
                .entry((range.node, range.tileset_id.clone()))
                .or_default()
                .push(range);
        }

        for ((node_index, tileset_id), ranges) in grouped {
            let node = &mut self.nodes[node_index];
            let mut missing = BTreeSet::new();
            let mut waiters: HashMap<u64, u64> = HashMap::new();
            let archive_len = ranges
                .iter()
                .map(|range| range.archive_len)
                .max()
                .unwrap_or(0);

            for range in ranges {
                let mut range_missed = false;
                for chunk_index in chunks_for_range(
                    range.offset,
                    u64::from(range.length),
                    self.config.chunk_size_bytes,
                ) {
                    let key = ChunkKey {
                        tileset_id: tileset_id.clone(),
                        chunk_index,
                    };
                    if node.chunk_cache.get(&key).is_some() {
                        node.metrics.chunk_cache_hits += 1;
                    } else {
                        range_missed = true;
                        node.metrics.chunk_cache_misses += 1;
                        *waiters.entry(chunk_index).or_default() += 1;
                        if missing.insert(chunk_index) {
                            node.metrics.chunk_fetch_queued += 1;
                        } else {
                            node.metrics.chunk_fetch_joined_pending += 1;
                        }
                    }
                    node.metrics.chunk_cache_post_fetch_hits += 1;
                }
                if range_missed {
                    backend_waiters.extend(range.request_indices.iter().copied());
                }
            }

            if missing.is_empty() {
                continue;
            }
            let fetches = plan_chunk_fetch_ranges(&missing, self.config.max_fetch_chunks);
            if bootstrap_phase && missing.contains(&0) {
                node.metrics.chunk_dispatch_immediate += 1;
            } else {
                node.metrics.chunk_dispatch_window += 1;
            }
            node.metrics.chunk_dispatch_pending_chunks += missing.len() as u64;
            for fetch in fetches {
                let start = fetch.start * self.config.chunk_size_bytes;
                let end = (fetch.end * self.config.chunk_size_bytes).min(archive_len);
                let bytes = end.saturating_sub(start);
                node.backend_bytes += bytes;
                node.metrics.backend_fetches += 1;
                node.metrics.backend_fetch_successes += 1;
                node.metrics.backend_fetched_chunks += fetch.end - fetch.start;
                node.metrics.chunk_waiters_released += (fetch.start..fetch.end)
                    .map(|chunk| waiters.get(&chunk).copied().unwrap_or(0))
                    .sum::<u64>();
                for chunk_index in fetch {
                    let chunk_start = chunk_index * self.config.chunk_size_bytes;
                    let chunk_end =
                        ((chunk_index + 1) * self.config.chunk_size_bytes).min(archive_len);
                    let length = chunk_end.saturating_sub(chunk_start);
                    let key_bytes = std::mem::size_of::<TilesetId>() + std::mem::size_of::<u64>();
                    let weight = key_bytes
                        .saturating_add(length as usize)
                        .min(u32::MAX as usize) as u32;
                    node.chunk_cache.insert(
                        ChunkKey {
                            tileset_id: tileset_id.clone(),
                            chunk_index,
                        },
                        ModeledChunk { weight },
                    );
                }
            }
        }
        backend_waiters
    }

    fn record(&mut self, node_index: usize, source: &'static str, bytes: Option<u64>) {
        let node = &mut self.nodes[node_index];
        *node.by_source.entry(source.to_string()).or_default() += 1;
        *self.report.by_source.entry(source.to_string()).or_default() += 1;
        if let Some(bytes) = bytes {
            node.served_bytes += bytes;
            self.report.found += 1;
            self.report.served_bytes += bytes;
        } else {
            self.report.not_found += 1;
        }
    }

    pub fn request_count(&self) -> u64 {
        self.report.requests
    }

    pub fn observation(&self) -> ClusterObservation {
        let mut metrics = NodeMetricsSnapshot::default();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
        }
        for node in &self.nodes {
            metrics.merge(&node.metrics);
        }
        ClusterObservation {
            requests: self.report.requests,
            active_nodes: self.nodes.len(),
            virtual_elapsed_ms: None,
            gossip_messages: 0,
            gossip_bytes: 0,
            membership_converged_nodes: self.nodes.len(),
            membership_stale_nodes: 0,
            membership_missing_peer_refs: 0,
            membership_extra_peer_refs: 0,
            membership_min_peer_count: self.nodes.len(),
            membership_max_peer_count: self.nodes.len(),
            cache_hits: self
                .report
                .l1_cache_hits
                .saturating_add(self.report.negative_cache_hits),
            l1_cache_hits: self.report.l1_cache_hits,
            negative_cache_hits: self.report.negative_cache_hits,
            by_source: self.report.by_source.clone(),
            node_requests: self
                .nodes
                .iter()
                .map(|node| (node.id.clone(), node.requests))
                .collect(),
            peer_requests: self.report.peer_requests,
            peer_unavailable_requests: 0,
            peer_retryable_failures: metrics.peer_forward_retryable,
            peer_backoff_skips: metrics.peer_forward_backoff_skips,
            backend_fetches: metrics.backend_fetches,
            backend_bytes: self
                .retired_nodes
                .iter()
                .map(|node| node.backend_bytes)
                .sum::<u64>()
                + self
                    .nodes
                    .iter()
                    .map(|node| node.backend_bytes)
                    .sum::<u64>(),
            served_bytes: self.report.served_bytes,
            tile_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.tile_cache.weighted_size())
                .sum(),
            chunk_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.chunk_cache.weighted_size())
                .sum(),
        }
    }

    pub fn report(mut self) -> SimReport {
        self.finish_cache_maintenance();
        let mut metrics = NodeMetricsSnapshot::default();
        let active_nodes: Vec<_> = self
            .nodes
            .iter()
            .map(|node| {
                metrics.merge(&node.metrics);
                modeled_node_report(node, true)
            })
            .collect();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
        }
        self.report.backend_bytes = self
            .retired_nodes
            .iter()
            .chain(&active_nodes)
            .map(|node| node.backend_bytes)
            .sum();
        self.report.tile_cache_bytes = active_nodes.iter().map(|node| node.tile_cache_bytes).sum();
        self.report.chunk_cache_bytes =
            active_nodes.iter().map(|node| node.chunk_cache_bytes).sum();
        self.report.metrics = metrics;
        let mut nodes = self.retired_nodes;
        nodes.extend(active_nodes);
        self.report.nodes = nodes;
        self.report.set_node_request_load();
        self.report.finalize_derived_metrics();
        self.report
    }
}

fn modeled_node_report(node: &ModelNode, active: bool) -> NodeReport {
    NodeReport {
        id: node.id.clone(),
        active,
        requests: node.requests,
        served_bytes: node.served_bytes,
        by_source: node.by_source.clone(),
        backend_bytes: node.backend_bytes,
        tile_cache_bytes: node.tile_cache.weighted_size(),
        chunk_cache_bytes: node.chunk_cache.weighted_size(),
        metrics: node.metrics,
        scheduler: SchedulerReport::default(),
        histograms: Default::default(),
    }
}

fn tile_key(entry: &TraceEntry) -> Result<TileKey> {
    let tileset_id = TilesetId::try_new(&entry.tileset).context("invalid trace tileset")?;
    let tile_id = TileId::from(
        TileCoord::new(entry.z, entry.x, entry.y).context("invalid trace tile coordinate")?,
    )
    .value();
    Ok(TileKey {
        tileset_id,
        tile_id,
    })
}

fn chunks_for_range(start: u64, length: u64, chunk_size: u64) -> std::ops::RangeInclusive<u64> {
    let first = start / chunk_size;
    let last = start.saturating_add(length).saturating_sub(1) / chunk_size;
    first..=last
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, mem::size_of};

    use ishikari::pmtiles::{ArchiveRange, TileAccessPlan, TileLocation};

    use super::{
        ClusterConfig, ModeledCluster, ModeledTile, TileCatalog, chunks_for_range, tile_key,
    };
    use crate::TraceEntry;

    fn tile_fixture() -> (TraceEntry, TileCatalog) {
        let entry = TraceEntry {
            step: 0,
            user: 0,
            ordinal: 0,
            tileset: "japan".to_string(),
            z: 0,
            x: 0,
            y: 0,
            entry_node: Some(0),
        };
        let key = tile_key(&entry).expect("tile key");
        let catalog = TileCatalog {
            entries: HashMap::from([(
                key,
                Some(TileAccessPlan {
                    bootstrap: ArchiveRange {
                        offset: 0,
                        length: 16_384,
                    },
                    leaves: Vec::new(),
                    tile: TileLocation {
                        offset: 2 * 1024 * 1024,
                        length: 128 * 1024,
                        archive_len: 4 * 1024 * 1024,
                    },
                }),
            )]),
        };
        (entry, catalog)
    }

    #[test]
    fn maps_byte_ranges_to_chunks() {
        assert_eq!(chunks_for_range(0, 1, 1024), 0..=0);
        assert_eq!(chunks_for_range(1023, 2, 1024), 0..=1);
        assert_eq!(chunks_for_range(2048, 1024, 1024), 2..=2);
    }

    #[test]
    fn modeled_cache_uses_logical_weight_without_tile_payloads() {
        assert!(size_of::<ModeledTile>() <= 16);

        let (entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                tile_cache_max_bytes: 8 * 1024 * 1024 * 1024,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&entry).expect("cold request");
        cluster.serve(&entry).expect("cached request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("self_backend"), Some(&1));
        assert_eq!(report.by_source.get("self_cache"), Some(&1));
        assert_eq!(report.l1_cache_hits, 1);
        assert_eq!(report.metrics.backend_fetches, 2);
        assert!(report.tile_cache_bytes >= 128 * 1024);
        assert!(report.chunk_cache_bytes <= 1024 * 1024 * 1024);
    }

    #[test]
    fn observations_split_positive_and_negative_l1_hits() {
        let (entry, mut catalog) = tile_fixture();
        let key = tile_key(&entry).expect("tile key");
        catalog.entries.insert(key, None);
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&entry).expect("cold negative request");
        cluster.serve(&entry).expect("negative cache hit");
        let observation = cluster.observation();

        assert_eq!(observation.l1_cache_hits, 0);
        assert_eq!(observation.negative_cache_hits, 1);
        assert_eq!(observation.cache_hits, 1);
    }

    #[test]
    fn peer_source_distinguishes_backend_wait_from_chunk_cache() {
        let (mut entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                tile_cache_max_bytes: 1,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");
        entry.entry_node = Some(1 - owner);

        cluster.serve(&entry).expect("cold peer request");
        cluster.serve(&entry).expect("chunk-cached peer request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("peer_backend"), Some(&1));
        assert_eq!(report.by_source.get("peer_cache"), Some(&1));
    }

    #[test]
    fn owner_only_policy_does_not_populate_entry_l1() {
        let (mut entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                tile_cache_max_bytes: 1024 * 1024,
                chunk_cache_max_bytes: 1024 * 1024 * 1024,
                cache_peer_tiles: false,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");
        let key = tile_key(&entry).expect("tile key");
        let owner = cluster
            .router
            .route_tile(&cluster.peers, key.tileset_id.as_ref(), key.tile_id)
            .first()
            .and_then(|candidate| {
                cluster
                    .nodes
                    .iter()
                    .position(|node| node.id == candidate.peer.id)
            })
            .expect("HRW owner");
        entry.entry_node = Some(1 - owner);

        cluster.serve(&entry).expect("cold peer request");
        cluster.serve(&entry).expect("owner-cached peer request");
        let report = cluster.report();

        assert_eq!(report.by_source.get("peer_backend"), Some(&1));
        assert_eq!(report.by_source.get("peer_cache"), Some(&1));
        assert_eq!(report.by_source.get("self_cache"), None);
        assert_eq!(report.l1_cache_hits, 0);
    }

    #[test]
    fn modeled_cache_enforces_capacity_at_viewport_boundaries() {
        let (entry, catalog) = tile_fixture();
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 1,
                tile_cache_max_bytes: 1,
                chunk_cache_max_bytes: 1,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        cluster.serve(&entry).expect("cold request");

        assert!(cluster.nodes[0].tile_cache.weighted_size() <= 1);
        assert!(cluster.nodes[0].chunk_cache.weighted_size() <= 1);
    }

    #[test]
    fn modeled_node_lifecycle_uses_empty_new_node_and_keeps_history() {
        let catalog = TileCatalog {
            entries: HashMap::new(),
        };
        let mut cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 2,
                ..ClusterConfig::default()
            },
            catalog,
        )
        .expect("modeled cluster");

        assert_eq!(cluster.add_node().expect("add node"), "node-2");
        assert_eq!(cluster.nodes[2].tile_cache.weighted_size(), 0);
        cluster.remove_node("node-1").expect("remove node");

        assert_eq!(cluster.active_node_ids(), ["node-0", "node-2"]);
        let report = cluster.report();
        assert!(
            report
                .nodes
                .iter()
                .any(|node| node.id == "node-1" && !node.active)
        );
    }

    #[test]
    fn modeled_membership_is_reported_as_instantly_converged() {
        let cluster = ModeledCluster::new(
            ClusterConfig {
                node_count: 3,
                ..ClusterConfig::default()
            },
            TileCatalog {
                entries: HashMap::new(),
            },
        )
        .expect("modeled cluster");

        let observation = cluster.observation();
        assert_eq!(observation.membership_converged_nodes, 3);
        assert_eq!(observation.membership_stale_nodes, 0);
        assert_eq!(observation.membership_min_peer_count, 3);
        assert_eq!(observation.membership_max_peer_count, 3);
        assert_eq!(observation.virtual_elapsed_ms, None);
        assert_eq!(observation.gossip_messages, 0);
    }
}

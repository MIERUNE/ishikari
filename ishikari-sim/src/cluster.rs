use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    net::SocketAddr,
    sync::{
        Arc, RwLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::Instant;

use anyhow::{Context, Result, ensure};
use ishikari::{
    membership::{Membership, MembershipConfig, Peer},
    metrics::{NodeHistogramSnapshot, NodeMetrics, NodeMetricsSnapshot},
    pmtiles::{TileCoord, TileId},
    storage::{
        FetchFuture, HrwRouter, InternalTransport, ObjectStoreRegistry, PeerBackend, PeerDirectory,
        PeerFetchError, PeerFuture, PeerTileCachePolicy, ResourceResolver,
        ResourceResolverStorageConfig, TileSource, TilesetId, internal_resource_kind,
        validate_chunked_store_limits,
    },
};
use serde::{Deserialize, Serialize};

use crate::{
    BackendLatencyConfig, TraceEntry,
    membership::SimGossipTransport,
    report::{ClusterObservation, NodeReport, SchedulerReport, SimReport},
};

const SIM_NODE_BASE_PORT: u16 = 10_000;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
pub struct ClusterConfig {
    pub node_count: usize,
    pub tileset_sources: String,
    pub candidate_count: usize,
    pub tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub chunk_fetch_merge_window_ms: u64,
    pub backend_fetch_concurrency: usize,
    #[serde(flatten, default)]
    pub backend_latency: BackendLatencyConfig,
    pub peer_latency_ms: u64,
    pub gossip_interval_ms: u64,
    pub gossip_hop_latency_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    pub cache_peer_tiles: bool,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            node_count: 3,
            tileset_sources: "data".to_string(),
            candidate_count: 3,
            tile_group_size: 512,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window_ms: 10,
            backend_fetch_concurrency: 32,
            backend_latency: BackendLatencyConfig::default(),
            peer_latency_ms: 0,
            gossip_interval_ms: 200,
            gossip_hop_latency_ms: 1,
            tile_cache_max_bytes: 512 * 1024 * 1024,
            chunk_cache_max_bytes: 512 * 1024 * 1024,
            cache_peer_tiles: true,
        }
    }
}

impl ClusterConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        ensure!(self.node_count > 0, "node_count must be greater than zero");
        ensure!(
            self.node_count <= usize::from(u16::MAX - SIM_NODE_BASE_PORT) + 1,
            "node_count exceeds the simulator address range"
        );
        ensure!(
            self.candidate_count > 0,
            "candidate_count must be greater than zero"
        );
        ensure!(
            self.tile_group_size > 0,
            "tile_group_size must be greater than zero"
        );
        validate_chunked_store_limits(
            self.chunk_size_bytes,
            self.max_fetch_chunks,
            Duration::from_millis(self.chunk_fetch_merge_window_ms),
            self.backend_fetch_concurrency,
        )
        .context("invalid chunk storage configuration")?;
        ensure!(
            self.gossip_interval_ms > 0,
            "gossip_interval_ms must be greater than zero"
        );
        self.backend_latency.model_for_node(0)?;
        Ok(())
    }
}

pub(crate) fn simulated_peers(node_count: usize) -> Vec<Peer> {
    (0..node_count)
        .map(|index| Peer {
            id: format!("node-{index}"),
            addr: SocketAddr::from((
                [127, 0, 0, 1],
                SIM_NODE_BASE_PORT + u16::try_from(index).expect("validated node index"),
            )),
        })
        .collect()
}

pub(crate) fn simulated_peer(index: usize) -> Result<Peer> {
    ensure!(
        index <= usize::from(u16::MAX - SIM_NODE_BASE_PORT),
        "node index exceeds the simulator address range"
    );
    Ok(Peer {
        id: format!("node-{index}"),
        addr: SocketAddr::from((
            [127, 0, 0, 1],
            SIM_NODE_BASE_PORT + u16::try_from(index).expect("validated node index"),
        )),
    })
}

struct SimNode {
    id: String,
    gossip_addr: SocketAddr,
    membership: Membership,
    resolver: Arc<ResourceResolver>,
    metrics: NodeMetrics,
    requests: u64,
    served_bytes: u64,
    by_source: BTreeMap<String, u64>,
}

pub(crate) struct PreparedRequest {
    pub(crate) node_index: usize,
    resolver: Arc<ResourceResolver>,
    metrics: NodeMetrics,
    tileset_id: TilesetId,
    tile_id: u64,
}

pub(crate) struct ServedRequest {
    pub(crate) node_index: usize,
    pub(crate) source: TileSource,
    bytes: Option<u64>,
}

#[derive(Clone)]
struct ChitchatPeerDirectory {
    membership: Membership,
}

impl PeerDirectory for ChitchatPeerDirectory {
    fn peers(&self) -> PeerFuture<'_> {
        Box::pin(self.membership.peers())
    }
}

#[derive(Default)]
struct NodeRegistry {
    nodes: RwLock<HashMap<String, RegisteredNode>>,
}

struct RegisteredNode {
    resolver: Weak<ResourceResolver>,
    metrics: NodeMetrics,
}

impl NodeRegistry {
    fn register(&self, id: String, resolver: &Arc<ResourceResolver>, metrics: NodeMetrics) {
        self.nodes.write().expect("node registry poisoned").insert(
            id,
            RegisteredNode {
                resolver: Arc::downgrade(resolver),
                metrics,
            },
        );
    }

    fn get(&self, id: &str) -> Option<(Arc<ResourceResolver>, NodeMetrics)> {
        let nodes = self.nodes.read().expect("node registry poisoned");
        let node = nodes.get(id)?;
        Some((node.resolver.upgrade()?, node.metrics.clone()))
    }

    fn remove(&self, id: &str) {
        self.nodes
            .write()
            .expect("node registry poisoned")
            .remove(id);
    }
}

#[derive(Default)]
struct TransportCounters {
    requests: AtomicU64,
    bytes: AtomicU64,
    unavailable_requests: AtomicU64,
}

struct SimInternalTransport {
    registry: Arc<NodeRegistry>,
    counters: Arc<TransportCounters>,
    latency: Duration,
}

impl InternalTransport for SimInternalTransport {
    fn fetch<'a>(&'a self, peer: &'a Peer, path: &'a str) -> FetchFuture<'a> {
        Box::pin(async move {
            self.counters.requests.fetch_add(1, Ordering::Relaxed);
            if !self.latency.is_zero() {
                tokio::time::sleep(self.latency).await;
            }
            let (resolver, metrics) = self.registry.get(&peer.id).ok_or_else(|| {
                self.counters
                    .unavailable_requests
                    .fetch_add(1, Ordering::Relaxed);
                PeerFetchError::Retryable(format!("simulator peer {} is unavailable", peer.id))
            })?;
            let result = resolver.fetch_internal_for_simulator(path).await;
            if let Some(resource) = internal_resource_kind(path) {
                let outcome = match &result {
                    Ok(_) => "success",
                    Err(PeerFetchError::NotFound) => "not_found",
                    Err(PeerFetchError::Retryable(_)) => "retryable",
                    Err(PeerFetchError::Fatal(_)) => "error",
                };
                metrics.record_internal_resource_request(resource, outcome);
            }
            let response = result?;
            self.counters
                .bytes
                .fetch_add(response.bytes.len() as u64, Ordering::Relaxed);
            Ok(response)
        })
    }
}

/// In-process Ishikari cluster using production routing, PMTiles, and caches.
pub struct SimCluster {
    config: ClusterConfig,
    nodes: Vec<SimNode>,
    retired_nodes: Vec<NodeReport>,
    gossip_transport: SimGossipTransport,
    registry: Arc<NodeRegistry>,
    transport: Arc<dyn InternalTransport>,
    transport_counters: Arc<TransportCounters>,
    next_node_index: usize,
    simulation_started_at: Instant,
    report: SimReport,
}

impl SimCluster {
    pub async fn new(config: ClusterConfig) -> Result<Self> {
        config.validate()?;
        let registry = Arc::new(NodeRegistry::default());
        let transport_counters = Arc::new(TransportCounters::default());
        let transport: Arc<dyn InternalTransport> = Arc::new(SimInternalTransport {
            registry: registry.clone(),
            counters: transport_counters.clone(),
            latency: Duration::from_millis(config.peer_latency_ms),
        });

        let initial_node_count = config.node_count;
        let mut cluster = Self {
            config: config.clone(),
            nodes: Vec::with_capacity(initial_node_count),
            retired_nodes: Vec::new(),
            gossip_transport: SimGossipTransport::new(Duration::from_millis(
                config.gossip_hop_latency_ms,
            )),
            registry,
            transport,
            transport_counters,
            next_node_index: 0,
            simulation_started_at: Instant::now(),
            report: SimReport::default(),
        };
        for _ in 0..initial_node_count {
            cluster.add_node().await?;
        }
        cluster.wait_for_membership_convergence().await?;
        cluster.simulation_started_at = Instant::now();
        Ok(cluster)
    }

    pub(crate) fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn active_node_ids(&self) -> Vec<String> {
        self.nodes.iter().map(|node| node.id.clone()).collect()
    }

    pub async fn add_node(&mut self) -> Result<String> {
        let node_index = self.next_node_index;
        let peer = simulated_peer(node_index)?;
        let seed_nodes = self
            .nodes
            .iter()
            .map(|node| node.gossip_addr.to_string())
            .collect();
        let membership = Membership::spawn_for_simulator(
            MembershipConfig {
                node_id: peer.id.clone(),
                listen_addr: peer.addr,
                advertise_addr: peer.addr,
                http_advertise_addr: peer.addr,
                http_port: peer.addr.port(),
                seed_nodes,
                gossip_interval: Duration::from_millis(self.config.gossip_interval_ms),
            },
            node_index as u64 + 1,
            &self.gossip_transport,
        )
        .await?;
        let directory: Arc<dyn PeerDirectory> = Arc::new(ChitchatPeerDirectory {
            membership: membership.clone(),
        });
        let node = build_node(
            &self.config,
            node_index,
            peer.clone(),
            membership,
            directory,
            self.transport.clone(),
        )?;
        self.registry
            .register(peer.id.clone(), &node.resolver, node.metrics.clone());
        self.nodes.push(node);
        self.next_node_index += 1;
        Ok(peer.id)
    }

    pub async fn remove_node(&mut self, id: &str) -> Result<()> {
        ensure!(self.nodes.len() > 1, "cannot remove the last active node");
        let index = self
            .nodes
            .iter()
            .position(|node| node.id == id)
            .with_context(|| format!("active node {id} does not exist"))?;
        self.registry.remove(id);
        let node = self.nodes.remove(index);
        node.membership.shutdown()?;
        self.retired_nodes.push(real_node_report(&node, false));
        Ok(())
    }

    /// Waits until every active node independently sees the same live set.
    pub async fn wait_for_membership_convergence(&self) -> Result<()> {
        let mut expected = self.active_node_ids();
        expected.sort();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut converged = true;
                for node in &self.nodes {
                    let actual = node
                        .membership
                        .peers()
                        .await
                        .iter()
                        .map(|peer| peer.id.clone())
                        .collect::<Vec<_>>();
                    if actual != expected {
                        converged = false;
                        break;
                    }
                }
                if converged {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(self.config.gossip_interval_ms)).await;
            }
        })
        .await
        .context("simulated chitchat cluster did not converge")?;
        Ok(())
    }

    #[cfg(test)]
    async fn membership_peer_ids(&self) -> BTreeMap<String, Vec<String>> {
        let mut views = BTreeMap::new();
        for node in &self.nodes {
            views.insert(
                node.id.clone(),
                node.membership
                    .peers_for_simulator_observation()
                    .await
                    .iter()
                    .map(|peer| peer.id.clone())
                    .collect(),
            );
        }
        views
    }

    /// Executes one trace request through its recorded entry node.
    pub async fn serve(&mut self, entry: &TraceEntry) -> Result<()> {
        let entry_node = entry
            .entry_node
            .context("trace has no entry_node; generate it with node_count > 0")?;
        self.serve_on(entry, entry_node).await
    }

    pub async fn serve_on(&mut self, entry: &TraceEntry, entry_node: usize) -> Result<()> {
        let request = self.prepare_on(entry, entry_node)?;
        let served = execute_request(request).await?;
        self.record(served);
        Ok(())
    }

    /// Polls one viewport's newly visible tiles concurrently in trace order.
    pub async fn serve_viewport(&mut self, entries: &[TraceEntry]) -> Result<()> {
        let entry_nodes = entries
            .iter()
            .map(|entry| {
                entry
                    .entry_node
                    .context("trace has no entry_node; generate it with node_count > 0")
            })
            .collect::<Result<Vec<_>>>()?;
        self.serve_viewport_on(entries, &entry_nodes).await
    }

    pub async fn serve_viewport_on(
        &mut self,
        entries: &[TraceEntry],
        entry_nodes: &[usize],
    ) -> Result<()> {
        ensure!(
            entries.len() == entry_nodes.len(),
            "entry node assignment length does not match viewport"
        );
        let requests = entries
            .iter()
            .zip(entry_nodes)
            .map(|(entry, &entry_node)| self.prepare_on(entry, entry_node))
            .collect::<Result<Vec<_>>>()?;
        let mut tasks = tokio::task::JoinSet::new();
        for request in requests {
            tasks.spawn(execute_request(request));
        }
        while let Some(result) = tasks.join_next().await {
            let served = result.context("viewport request task failed")??;
            self.record(served);
        }
        Ok(())
    }

    pub(crate) fn prepare_on(
        &self,
        entry: &TraceEntry,
        entry_node: usize,
    ) -> Result<PreparedRequest> {
        let node = self
            .nodes
            .get(entry_node)
            .with_context(|| format!("entry_node {entry_node} is outside the simulated cluster"))?;
        let tileset_id = TilesetId::try_new(&entry.tileset).context("invalid trace tileset")?;
        let tile_id = TileId::from(
            TileCoord::new(entry.z, entry.x, entry.y).context("invalid trace tile coordinate")?,
        )
        .value();
        Ok(PreparedRequest {
            node_index: entry_node,
            resolver: node.resolver.clone(),
            metrics: node.metrics.clone(),
            tileset_id,
            tile_id,
        })
    }

    pub(crate) fn prepare(&self, entry: &TraceEntry) -> Result<PreparedRequest> {
        let entry_node = entry
            .entry_node
            .context("trace has no entry_node; generate it with node_count > 0")?;
        self.prepare_on(entry, entry_node)
    }

    pub fn request_count(&self) -> u64 {
        self.report.requests
    }

    pub async fn observation(&self) -> ClusterObservation {
        let mut metrics = NodeMetricsSnapshot::default();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
        }
        for node in &self.nodes {
            metrics.merge(&node.metrics.snapshot());
        }
        let expected = self.active_node_ids().into_iter().collect::<BTreeSet<_>>();
        let mut membership_converged_nodes = 0;
        let mut membership_missing_peer_refs = 0;
        let mut membership_extra_peer_refs = 0;
        let mut membership_min_peer_count = usize::MAX;
        let mut membership_max_peer_count = 0;
        for node in &self.nodes {
            let actual = node
                .membership
                .peers_for_simulator_observation()
                .await
                .iter()
                .map(|peer| peer.id.clone())
                .collect::<BTreeSet<_>>();
            let missing = expected.difference(&actual).count();
            let extra = actual.difference(&expected).count();
            if missing == 0 && extra == 0 {
                membership_converged_nodes += 1;
            }
            membership_missing_peer_refs += missing;
            membership_extra_peer_refs += extra;
            membership_min_peer_count = membership_min_peer_count.min(actual.len());
            membership_max_peer_count = membership_max_peer_count.max(actual.len());
        }
        let (gossip_messages, gossip_bytes) = self.gossip_transport.statistics();
        ClusterObservation {
            requests: self.report.requests,
            active_nodes: self.nodes.len(),
            virtual_elapsed_ms: Some(
                self.simulation_started_at
                    .elapsed()
                    .as_millis()
                    .min(u128::from(u64::MAX)) as u64,
            ),
            gossip_messages,
            gossip_bytes,
            membership_converged_nodes,
            membership_stale_nodes: self.nodes.len() - membership_converged_nodes,
            membership_missing_peer_refs,
            membership_extra_peer_refs,
            membership_min_peer_count: if self.nodes.is_empty() {
                0
            } else {
                membership_min_peer_count
            },
            membership_max_peer_count,
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
            peer_requests: self.transport_counters.requests.load(Ordering::Relaxed),
            peer_unavailable_requests: self
                .transport_counters
                .unavailable_requests
                .load(Ordering::Relaxed),
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
                    .map(|node| node.resolver.received_bytes())
                    .sum::<u64>(),
            served_bytes: self.report.served_bytes,
            tile_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.resolver.tile_cache_weighted_size())
                .sum(),
            chunk_cache_bytes: self
                .nodes
                .iter()
                .map(|node| node.resolver.chunk_cache_weighted_size())
                .sum(),
        }
    }

    pub(crate) fn record(&mut self, served: ServedRequest) {
        let node = &mut self.nodes[served.node_index];
        self.report.requests += 1;
        node.requests += 1;
        // Positive and negative L1 hits are separate report fields: HTTP
        // replay derives them from distinct production outcomes, and mixing
        // them made absent-tile workloads incomparable across modes.
        match served.source {
            TileSource::SelfTileCache => self.report.l1_cache_hits += 1,
            TileSource::NegativeCache => self.report.negative_cache_hits += 1,
            _ => {}
        }
        *node
            .by_source
            .entry(source_name(served.source).to_string())
            .or_default() += 1;
        *self
            .report
            .by_source
            .entry(source_name(served.source).to_string())
            .or_default() += 1;
        if let Some(bytes) = served.bytes {
            node.served_bytes += bytes;
            self.report.found += 1;
            self.report.served_bytes += bytes;
        } else {
            self.report.not_found += 1;
        }
    }

    pub fn report(mut self) -> SimReport {
        for node in &self.nodes {
            let _ = node.membership.shutdown();
        }
        self.report.peer_requests = self.transport_counters.requests.load(Ordering::Relaxed);
        self.report.peer_bytes = self.transport_counters.bytes.load(Ordering::Relaxed);
        self.report.peer_unavailable_requests = self
            .transport_counters
            .unavailable_requests
            .load(Ordering::Relaxed);
        (self.report.gossip_messages, self.report.gossip_bytes) =
            self.gossip_transport.statistics();
        self.report.backend_bytes = self
            .retired_nodes
            .iter()
            .map(|node| node.backend_bytes)
            .sum::<u64>()
            + self
                .nodes
                .iter()
                .map(|node| node.resolver.received_bytes())
                .sum::<u64>();
        self.report.tile_cache_bytes = self
            .nodes
            .iter()
            .map(|node| node.resolver.tile_cache_weighted_size())
            .sum();
        self.report.chunk_cache_bytes = self
            .nodes
            .iter()
            .map(|node| node.resolver.chunk_cache_weighted_size())
            .sum();
        self.report.finalize_derived_metrics();
        let mut metrics = NodeMetricsSnapshot::default();
        let mut histograms = NodeHistogramSnapshot::default();
        let active_nodes = self
            .nodes
            .iter()
            .map(|node| {
                let node_metrics = node.metrics.snapshot();
                metrics.merge(&node_metrics);
                let node_report = real_node_report(node, true);
                histograms.merge(&node_report.histograms);
                node_report
            })
            .collect::<Vec<_>>();
        for node in &self.retired_nodes {
            metrics.merge(&node.metrics);
            histograms.merge(&node.histograms);
        }
        let mut nodes = self.retired_nodes;
        nodes.extend(active_nodes);
        self.report.metrics = metrics;
        self.report.set_histograms(&histograms);
        self.report.nodes = nodes;
        self.report.set_node_request_load();
        self.report
    }
}

fn build_node(
    config: &ClusterConfig,
    node_index: usize,
    peer: Peer,
    membership: Membership,
    directory: Arc<dyn PeerDirectory>,
    transport: Arc<dyn InternalTransport>,
) -> Result<SimNode> {
    let backend_latency = config.backend_latency.model_for_node(node_index)?;
    let metrics = NodeMetrics::new();
    metrics.set_chunk_config(config.chunk_size_bytes, config.max_fetch_chunks);
    let peer_backend = PeerBackend::with_dependencies(
        peer.id.clone(),
        directory,
        HrwRouter::new(config.candidate_count, config.tile_group_size),
        transport,
        metrics.clone(),
    );
    let resolver = Arc::new(ResourceResolver::with_peer_backend(
        ResourceResolverStorageConfig {
            tileset_sources: config.tileset_sources.clone(),
            chunk_size_bytes: config.chunk_size_bytes,
            max_fetch_chunks: config.max_fetch_chunks,
            chunk_fetch_merge_window: Duration::from_millis(config.chunk_fetch_merge_window_ms),
            backend_fetch_concurrency: config.backend_fetch_concurrency,
            backend_latency,
            tile_cache_max_bytes: config.tile_cache_max_bytes,
            peer_tile_cache_policy: if config.cache_peer_tiles {
                PeerTileCachePolicy::EntryAndOwner
            } else {
                PeerTileCachePolicy::OwnerOnly
            },
            chunk_cache_max_bytes: config.chunk_cache_max_bytes,
            // Mirrors the production default. Simulator runs never republish an
            // archive mid-run, so negative entries do not expire during a run
            // regardless of this value.
            tile_negative_ttl: std::time::Duration::from_secs(60),
            object_store_registry: Arc::new(ObjectStoreRegistry::new()),
            metrics: metrics.clone(),
        },
        peer_backend,
    )?);
    Ok(SimNode {
        id: peer.id,
        gossip_addr: peer.addr,
        membership,
        resolver,
        metrics,
        requests: 0,
        served_bytes: 0,
        by_source: BTreeMap::new(),
    })
}

fn real_node_report(node: &SimNode, active: bool) -> NodeReport {
    let histograms = node.metrics.histogram_snapshot();
    NodeReport {
        id: node.id.clone(),
        active,
        requests: node.requests,
        served_bytes: node.served_bytes,
        by_source: node.by_source.clone(),
        backend_bytes: node.resolver.received_bytes(),
        tile_cache_bytes: node.resolver.tile_cache_weighted_size(),
        chunk_cache_bytes: node.resolver.chunk_cache_weighted_size(),
        metrics: node.metrics.snapshot(),
        scheduler: SchedulerReport::from_histograms(&histograms),
        histograms,
    }
}

pub(crate) async fn execute_request(request: PreparedRequest) -> Result<ServedRequest> {
    let (tile, source) = request
        .resolver
        .route_tile(request.tileset_id, request.tile_id)
        .await?;
    request.metrics.record_tile_served(source.served_label());
    for outcome in request.resolver.cache_outcomes(source) {
        request.metrics.record_tile_cache(outcome);
    }
    let bytes = tile.map(|tile| {
        let bytes = tile.bytes.len() as u64;
        request.metrics.add_egress_bytes(bytes);
        bytes
    });
    Ok(ServedRequest {
        node_index: request.node_index,
        source,
        bytes,
    })
}

pub(crate) fn source_name(source: TileSource) -> &'static str {
    match source {
        TileSource::SelfTileCache | TileSource::SelfChunkCache => "self_cache",
        TileSource::SelfBackend => "self_backend",
        TileSource::NegativeCache => "negative_cache",
        TileSource::PeerCache => "peer_cache",
        TileSource::PeerBackend => "peer_backend",
        TileSource::SelfMiss => "self_miss",
        TileSource::PeerMiss => "peer_miss",
        TileSource::Miss => "miss",
    }
}

#[cfg(test)]
mod tests {
    use super::{ClusterConfig, SimCluster};
    use crate::BackendLatencyConfig;

    #[test]
    fn rejects_empty_cluster() {
        let result = ClusterConfig {
            node_count: 0,
            ..ClusterConfig::default()
        }
        .validate();

        assert!(result.is_err());
    }

    #[test]
    fn rejects_invalid_cluster_dimensions_and_latency() {
        let invalid = [
            ClusterConfig {
                candidate_count: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                tile_group_size: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                chunk_size_bytes: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                max_fetch_chunks: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                backend_fetch_concurrency: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                chunk_fetch_merge_window_ms: 1_001,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                backend_fetch_concurrency: usize::MAX,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                gossip_interval_ms: 0,
                ..ClusterConfig::default()
            },
            ClusterConfig {
                backend_latency: BackendLatencyConfig {
                    lognormal_sigma: f64::NAN,
                    ..BackendLatencyConfig::default()
                },
                ..ClusterConfig::default()
            },
        ];

        for config in invalid {
            assert!(config.validate().is_err(), "accepted {config:?}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn configurable_merge_window_reaches_node_metrics() {
        let cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            chunk_fetch_merge_window_ms: 25,
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        assert!(
            cluster.nodes[0]
                .metrics
                .encode()
                .contains("ishikari_chunk_fetch_merge_window_seconds 0.025")
        );
    }

    #[tokio::test(start_paused = true)]
    async fn empty_viewport_does_not_record_requests() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        cluster.serve_viewport(&[]).await.expect("empty viewport");

        assert_eq!(cluster.report().requests, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn node_lifecycle_preserves_retired_node_report() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 2,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        let added = cluster.add_node().await.expect("add node");
        assert_eq!(added, "node-2");
        cluster.remove_node("node-0").await.expect("remove node");

        assert_eq!(cluster.active_node_ids(), ["node-1", "node-2"]);
        let report = cluster.report();
        assert_eq!(report.nodes.len(), 3);
        assert!(
            report
                .nodes
                .iter()
                .any(|node| node.id == "node-0" && !node.active)
        );
        assert_eq!(report.nodes.iter().filter(|node| node.active).count(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn rejects_removing_last_node() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        assert!(cluster.remove_node("node-0").await.is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn each_node_routes_from_its_own_converged_chitchat_view() {
        let cluster = SimCluster::new(ClusterConfig {
            node_count: 3,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            gossip_interval_ms: 20,
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        let expected = vec![
            "node-0".to_string(),
            "node-1".to_string(),
            "node-2".to_string(),
        ];
        let views = cluster.membership_peer_ids().await;
        assert_eq!(views.len(), 3);
        assert!(views.values().all(|peers| peers == &expected));
    }

    #[tokio::test(start_paused = true)]
    async fn removed_node_remains_stale_then_leaves_each_chitchat_view() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 3,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            gossip_interval_ms: 20,
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");

        cluster.remove_node("node-0").await.expect("remove node");

        let stale_observation = cluster.observation().await;
        assert_eq!(stale_observation.membership_stale_nodes, 2);
        assert_eq!(stale_observation.membership_missing_peer_refs, 0);
        assert_eq!(stale_observation.membership_extra_peer_refs, 2);
        assert!(stale_observation.gossip_messages > 0);
        assert!(stale_observation.gossip_bytes > 0);
        assert!(stale_observation.virtual_elapsed_ms.is_some());

        let stale_views = cluster.membership_peer_ids().await;
        assert!(
            stale_views
                .values()
                .all(|peers| peers.iter().any(|peer| peer == "node-0")),
            "the production peer-list TTL should preserve the pre-churn view briefly"
        );

        cluster
            .wait_for_membership_convergence()
            .await
            .expect("membership convergence");
        let converged_views = cluster.membership_peer_ids().await;
        let expected = vec!["node-1".to_string(), "node-2".to_string()];
        assert!(converged_views.values().all(|peers| peers == &expected));
        let converged_observation = cluster.observation().await;
        assert_eq!(converged_observation.membership_converged_nodes, 2);
        assert_eq!(converged_observation.membership_stale_nodes, 0);
        assert_eq!(converged_observation.membership_extra_peer_refs, 0);
    }
}

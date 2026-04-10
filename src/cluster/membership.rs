//! Cluster membership built on chitchat.

use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, Result};
use chitchat::{
    ChitchatConfig, ChitchatHandle, ChitchatId, ClusterStateSnapshot, FailureDetectorConfig,
    NodeState, spawn_chitchat, transport::UdpTransport,
};
use tracing::info;

const CLUSTER_ID: &str = "ishikari";
const HTTP_PORT_KEY: &str = "http_port";
const DRAINING_KEY: &str = "draining";
const DEFAULT_HTTP_PORT: u16 = 8080;

/// Runtime configuration for the chitchat membership node.
pub struct MembershipConfig {
    pub node_id: String,
    pub listen_addr: SocketAddr,
    pub advertise_addr: SocketAddr,
    pub http_port: u16,
    pub seed_nodes: Vec<String>,
    pub gossip_interval: Duration,
}

/// Handle for querying and updating cluster membership state.
#[derive(Clone)]
pub struct Membership {
    node_id: String,
    handle: Arc<ChitchatHandle>,
}

/// Snapshot of the current cluster state exposed by the HTTP API.
#[derive(serde::Serialize)]
pub struct ClusterView {
    pub cluster_id: String,
    pub cluster_state: ClusterStateSnapshot,
    pub live_ids: Vec<String>,
    pub dead_ids: Vec<String>,
}

/// Reachable peer information derived from membership gossip state.
#[derive(Clone, Eq, PartialEq)]
pub struct Peer {
    pub id: String,
    pub addr: SocketAddr,
}

impl Membership {
    /// Starts chitchat and begins logging membership changes.
    pub async fn spawn(config: MembershipConfig) -> Result<Self> {
        let generation_id = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("system clock is before unix epoch")?
            .as_millis() as u64;
        let chitchat_id =
            ChitchatId::new(config.node_id.clone(), generation_id, config.advertise_addr);
        let chitchat_config = ChitchatConfig {
            chitchat_id,
            cluster_id: CLUSTER_ID.to_string(),
            gossip_interval: config.gossip_interval,
            listen_addr: config.listen_addr,
            seed_nodes: config.seed_nodes,
            failure_detector_config: FailureDetectorConfig {
                dead_node_grace_period: Duration::from_secs(10),
                ..FailureDetectorConfig::default()
            },
            marked_for_deletion_grace_period: Duration::from_secs(30),
            catchup_callback: None,
            extra_liveness_predicate: Some(Box::new(|node_state| {
                node_state.get(DRAINING_KEY) != Some("true")
            })),
        };
        let initial_key_values = vec![
            (HTTP_PORT_KEY.to_string(), config.http_port.to_string()),
            (DRAINING_KEY.to_string(), "false".to_string()),
        ];
        let handle = spawn_chitchat(chitchat_config, initial_key_values, &UdpTransport)
            .await
            .context("failed to start chitchat")?;
        let membership = Self {
            node_id: config.node_id,
            handle: Arc::new(handle),
        };

        membership.spawn_logger().await;

        Ok(membership)
    }

    /// Marks this node as draining or active in membership state.
    pub async fn set_draining(&self, draining: bool) {
        self.handle
            .with_chitchat(|chitchat| {
                chitchat.self_node_state().set(DRAINING_KEY, draining);
            })
            .await;
    }

    /// Returns whether this node currently advertises a draining state.
    pub async fn is_draining(&self) -> bool {
        let node_id = self.node_id.clone();
        self.handle
            .with_chitchat(move |chitchat| {
                chitchat
                    .node_states()
                    .iter()
                    .find(|(peer_id, _)| peer_id.node_id == node_id)
                    .and_then(|(_, node_state)| node_state.get(DRAINING_KEY))
                    .map(|v| v == "true")
                    .unwrap_or(false)
            })
            .await
    }

    /// Starts a chitchat shutdown sequence.
    pub fn shutdown(&self) -> Result<()> {
        self.handle
            .initiate_shutdown()
            .context("failed to initiate chitchat shutdown")
    }

    /// Returns a cluster-wide membership snapshot.
    pub async fn cluster_view(&self) -> ClusterView {
        self.handle
            .with_chitchat(|chitchat| {
                let mut live_ids: Vec<_> = chitchat
                    .live_nodes()
                    .map(|node| node.node_id.clone())
                    .collect();
                live_ids.sort();

                let mut dead_ids: Vec<_> = chitchat
                    .dead_nodes()
                    .map(|node| node.node_id.clone())
                    .collect();
                dead_ids.sort();

                ClusterView {
                    cluster_id: chitchat.cluster_id().to_string(),
                    cluster_state: chitchat.state_snapshot(),
                    live_ids,
                    dead_ids,
                }
            })
            .await
    }

    /// Returns routable live peers, excluding draining nodes.
    pub async fn peers(&self) -> Vec<Peer> {
        self.handle
            .with_chitchat(|chitchat| {
                let live_nodes = chitchat
                    .live_nodes()
                    .filter_map(|peer_id| {
                        chitchat
                            .node_state(peer_id)
                            .cloned()
                            .map(|node_state| (peer_id.clone(), node_state))
                    })
                    .collect::<BTreeMap<_, _>>();
                collect_live_peers_from_nodes(&live_nodes)
            })
            .await
    }

    /// Spawns a background task that logs membership changes.
    async fn spawn_logger(&self) {
        let mut live_nodes = self
            .handle
            .with_chitchat(|chitchat| chitchat.live_nodes_watcher())
            .await;
        let node_id = self.node_id.clone();

        tokio::spawn(async move {
            loop {
                let peers = collect_live_peers_from_nodes(&live_nodes.borrow());
                let peers_str = format!(
                    "[{}]",
                    peers
                        .iter()
                        .map(|peer| format!("\"{}\"", peer.addr))
                        .collect::<Vec<_>>()
                        .join(", ")
                );
                info!(node_id = %node_id, peers = %peers_str, "membership changed");

                if live_nodes.changed().await.is_err() {
                    break;
                }
            }
        });
    }
}

/// Converts live chitchat nodes into routable HTTP peers.
fn collect_live_peers_from_nodes(live_nodes: &BTreeMap<ChitchatId, NodeState>) -> Vec<Peer> {
    let mut peers: Vec<_> = live_nodes
        .iter()
        .map(|(peer_id, node_state)| {
            let http_port = node_state
                .get(HTTP_PORT_KEY)
                .and_then(|port| port.parse::<u16>().ok())
                .unwrap_or(DEFAULT_HTTP_PORT);

            Peer {
                id: peer_id.node_id.clone(),
                addr: SocketAddr::new(peer_id.gossip_advertise_addr.ip(), http_port),
            }
        })
        .collect();
    peers.sort_by(|left, right| left.id.cmp(&right.id));
    peers
}

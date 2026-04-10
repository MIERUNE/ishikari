//! Runtime configuration loading from CLI flags and environment variables.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use crate::cluster::membership;

/// Resolved application configuration used at startup.
pub struct Config {
    pub node_id: String,
    pub http_port: u16,
    pub http_listen_addr: SocketAddr,
    pub membership: membership::MembershipConfig,
    pub data_url: String,
    pub router_candidate_count: usize,
    pub router_tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    pub backend_fetch_delay_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
}

/// CLI flags and environment variables for configuring the server.
#[derive(Parser, Debug)]
pub struct Cli {
    #[arg(long, value_delimiter = ',', value_name = "ADDR")]
    seeds: Option<Vec<String>>,
    #[arg(long, env = "ADVERTISE_ADDR")]
    advertise_addr: Option<SocketAddr>,
    #[arg(long, env = "LISTEN_ADDR", default_value = "0.0.0.0:7946")]
    listen_addr: SocketAddr,
    #[arg(long, env = "HTTP_PORT", default_value_t = 8080)]
    http_port: u16,
    #[arg(long, env = "DATA_URL", default_value = "data")]
    data_url: String,
    #[arg(long, env = "ROUTER_TOP_K", default_value_t = 3)]
    router_candidate_count: usize,
    #[arg(long, env = "ROUTER_TILE_GROUP_SIZE", default_value_t = 1024)]
    router_tile_group_size: u64,
    #[arg(long, env = "GOSSIP_INTERVAL_MS", default_value_t = 200)]
    gossip_interval_ms: u64,
    #[arg(long, env = "CHUNK_SIZE_BYTES", default_value_t = 1 * 1024 * 1024)]
    chunk_size_bytes: u64,
    #[arg(long, env = "MAX_FETCH_CHUNKS", default_value_t = 4)]
    max_fetch_chunks: u64,
    #[arg(long, env = "BACKEND_FETCH_DELAY_MS", default_value_t = 0)]
    backend_fetch_delay_ms: u64,
    #[arg(long, env = "TILE_CACHE_MAX_BYTES", default_value_t = 64 * 1024 * 1024)]
    tile_cache_max_bytes: u64,
    #[arg(long, env = "CHUNK_CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    chunk_cache_max_bytes: u64,
}

impl Config {
    /// Parses CLI arguments and environment variables into runtime configuration.
    pub fn load() -> Self {
        Self::from_cli(Cli::parse())
    }

    /// Resolves derived settings and defaults from parsed CLI input.
    fn from_cli(cli: Cli) -> Self {
        let node_id = auto_node_id();
        let advertise_addr = cli.advertise_addr.unwrap_or(cli.listen_addr);
        let http_listen_addr = SocketAddr::new(cli.listen_addr.ip(), cli.http_port);
        let seed_nodes = cli
            .seeds
            .filter(|values| !values.is_empty())
            .unwrap_or_default();

        Self {
            node_id: node_id.clone(),
            http_port: cli.http_port,
            http_listen_addr,
            membership: membership::MembershipConfig {
                node_id,
                listen_addr: cli.listen_addr,
                advertise_addr,
                http_port: cli.http_port,
                seed_nodes,
                gossip_interval: Duration::from_millis(cli.gossip_interval_ms.max(1)),
            },
            data_url: cli.data_url,
            router_candidate_count: cli.router_candidate_count,
            router_tile_group_size: cli.router_tile_group_size,
            chunk_size_bytes: cli.chunk_size_bytes,
            max_fetch_chunks: cli.max_fetch_chunks.max(1),
            backend_fetch_delay_ms: cli.backend_fetch_delay_ms,
            tile_cache_max_bytes: cli.tile_cache_max_bytes,
            chunk_cache_max_bytes: cli.chunk_cache_max_bytes,
        }
    }
}

/// Generates a process-local node id for ad-hoc local runs.
fn auto_node_id() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("node-{}-{now}", std::process::id())
}

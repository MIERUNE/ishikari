//! Runtime configuration loading from CLI flags and environment variables.

use std::{
    net::SocketAddr,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use clap::Parser;

use crate::{membership, storage::validate_chunked_store_limits};

const MAX_CPU_WORK_CONCURRENCY: usize = 1_024;
const MAX_CPU_WORK_INFLIGHT: usize = MAX_CPU_WORK_CONCURRENCY * 64;

/// Resolved application configuration used at startup.
pub struct Config {
    pub node_id: String,
    pub http_port: u16,
    pub http_listen_addr: SocketAddr,
    /// Cluster-internal listener (metrics, peer forwarding); not exposed
    /// through the Gateway.
    pub internal_listen_addr: SocketAddr,
    pub membership: membership::MembershipConfig,
    pub tileset_sources: String,
    pub router_candidate_count: usize,
    pub router_tile_group_size: u64,
    pub chunk_size_bytes: u64,
    pub max_fetch_chunks: u64,
    /// Delay used to collect nearby missing chunks before dispatching a range fetch.
    pub chunk_fetch_merge_window: Duration,
    /// Maximum concurrent object-storage range fetches across all tilesets.
    pub backend_fetch_concurrency: usize,
    pub artificial_backend_delay_ms: u64,
    pub tile_cache_max_bytes: u64,
    pub chunk_cache_max_bytes: u64,
    /// How long a negative (tile-absent) L1 cache entry lives before the tile is
    /// re-resolved, bounding how long a newly-published tile can stay hidden.
    pub tile_negative_ttl: Duration,
    pub style_templates: Option<String>,
    pub glyph_url_template: Option<String>,
    pub sprite_templates: Option<String>,
    /// Logical tileset key served as a Mapterhorn composite (base `planet`
    /// archive for z<=12, per-region detail archives for z>12). `None` disables
    /// composite serving.
    pub mapterhorn_tileset: Option<String>,
    /// Maximum zoom advertised for the Mapterhorn composite tileset, so clients
    /// request the z13+ tiles backed by detail archives.
    pub mapterhorn_maxzoom: Option<u8>,
    /// How long an absent Mapterhorn detail archive stays negative-cached.
    pub mapterhorn_negative_ttl: Duration,
    /// Maximum number of CPU-heavy decode/generate/transcode/validation/rewrite
    /// jobs running per pod.
    pub cpu_work_concurrency: usize,
    /// Maximum admitted CPU-work units per class before new work in that class
    /// is shed with 503.
    pub cpu_work_max_inflight: usize,
}

/// CLI flags and environment variables for configuring the server.
#[derive(Parser, Debug)]
pub struct Cli {
    #[arg(
        long = "gossip-seeds",
        env = "ISKR_GOSSIP_SEEDS",
        value_delimiter = ',',
        value_name = "ADDR"
    )]
    gossip_seeds: Option<Vec<String>>,
    #[arg(long, env = "ISKR_NODE_ID")]
    node_id: Option<String>,
    #[arg(long = "gossip-advertise-addr", env = "ISKR_GOSSIP_ADVERTISE_ADDR")]
    gossip_advertise_addr: Option<SocketAddr>,
    #[arg(
        long = "internal-http-advertise-addr",
        env = "ISKR_INTERNAL_HTTP_ADVERTISE_ADDR"
    )]
    internal_http_advertise_addr: Option<SocketAddr>,
    #[arg(
        long = "gossip-bind",
        env = "ISKR_GOSSIP_BIND",
        default_value = "0.0.0.0:7946"
    )]
    gossip_bind: SocketAddr,
    #[arg(long, env = "ISKR_HTTP_PORT", default_value_t = 8080)]
    http_port: u16,
    /// Cluster-internal port: metrics, `/_internal/*` and peer-to-peer
    /// forwarding. Served on a separate listener, never exposed via the Gateway.
    #[arg(
        long = "internal-http-port",
        env = "ISKR_INTERNAL_HTTP_PORT",
        default_value_t = 9090
    )]
    internal_http_port: u16,
    /// Mark this node as part of a multi-node cluster. Rejects wildcard
    /// advertise addresses even with no `--gossip-seeds` (a seed node that others will
    /// join must publish a routable address). Implied whenever `--gossip-seeds` is set.
    #[arg(long, env = "ISKR_CLUSTER", default_value_t = false)]
    cluster: bool,
    #[arg(long, env = "ISKR_TILESET_SOURCES", default_value = "data")]
    tileset_sources: String,
    #[arg(long, env = "ISKR_ROUTER_TOP_K", default_value_t = 3)]
    router_candidate_count: usize,
    #[arg(long, env = "ISKR_ROUTER_TILE_GROUP_SIZE", default_value_t = 512)]
    router_tile_group_size: u64,
    #[arg(long, env = "ISKR_GOSSIP_INTERVAL_MS", default_value_t = 200)]
    gossip_interval_ms: u64,
    #[arg(long, env = "ISKR_CHUNK_SIZE_BYTES", default_value_t = 1 * 1024 * 1024)]
    chunk_size_bytes: u64,
    #[arg(long, env = "ISKR_MAX_FETCH_CHUNKS", default_value_t = 4)]
    max_fetch_chunks: u64,
    /// Scheduler delay used to collect nearby missing chunks before dispatch.
    /// Zero removes the intentional delay while preserving pending/inflight sharing.
    #[arg(
        long = "chunk-fetch-merge-window-ms",
        env = "ISKR_CHUNK_FETCH_MERGE_WINDOW_MS",
        default_value_t = 10
    )]
    chunk_fetch_merge_window_ms: u64,
    /// Process-wide object-storage range-fetch limit. This complements the
    /// per-tileset coordinator cap and prevents distinct-id enumeration from
    /// multiplying backend concurrency.
    #[arg(
        long = "backend-fetch-concurrency",
        env = "ISKR_BACKEND_FETCH_CONCURRENCY",
        default_value_t = 32
    )]
    backend_fetch_concurrency: usize,
    #[arg(
        long = "artificial-backend-delay-ms",
        env = "ISKR_ARTIFICIAL_BACKEND_DELAY_MS",
        default_value_t = 0
    )]
    artificial_backend_delay_ms: u64,
    #[arg(long, env = "ISKR_TILE_CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    tile_cache_max_bytes: u64,
    #[arg(long, env = "ISKR_CHUNK_CACHE_MAX_BYTES", default_value_t = 512 * 1024 * 1024)]
    chunk_cache_max_bytes: u64,
    /// Seconds a negative (tile-absent) L1 cache entry lives before the tile is
    /// re-resolved. Kept short so a republished archive's newly-added tiles
    /// surface quickly and lookups of not-yet-existing tiles cannot poison the
    /// cache to delay their rollout. Positive entries are unaffected.
    #[arg(
        long = "tile-negative-ttl",
        env = "ISKR_TILE_NEGATIVE_TTL",
        default_value_t = 60
    )]
    tile_negative_ttl_secs: u64,
    #[arg(long, env = "ISKR_STYLE_TEMPLATES")]
    style_templates: Option<String>,
    #[arg(long, env = "ISKR_GLYPH_URL_TEMPLATE")]
    glyph_url_template: Option<String>,
    #[arg(long, env = "ISKR_SPRITE_TEMPLATES")]
    sprite_templates: Option<String>,
    /// Logical tileset key (e.g. `mapterhorn/planet`) to serve as a Mapterhorn
    /// composite: z<=12 from the base archive, z>12 from `6-{x6}-{y6}` detail
    /// archives in the same namespace. Unset disables composite serving.
    #[arg(long = "mapterhorn-tileset", env = "ISKR_MAPTERHORN_TILESET")]
    mapterhorn_tileset: Option<String>,
    /// Max zoom advertised in the composite tileset's TileJSON (detail coverage).
    #[arg(long = "mapterhorn-maxzoom", env = "ISKR_MAPTERHORN_MAXZOOM")]
    mapterhorn_maxzoom: Option<u8>,
    /// Seconds an absent Mapterhorn detail archive stays negative-cached. Detail
    /// coverage rarely changes, so a long TTL keeps probes off the hot path.
    #[arg(
        long = "mapterhorn-negative-ttl",
        env = "ISKR_MAPTERHORN_NEGATIVE_TTL",
        default_value_t = 3600
    )]
    mapterhorn_negative_ttl_secs: u64,
    /// Maximum number of CPU-heavy DEM decode, terrain generation, MLT
    /// transcode, provider JSON validation, and style rewriting jobs running
    /// concurrently per pod. Defaults to the effective parallelism reported by
    /// the runtime (including cgroup limits where the platform exposes them).
    #[arg(long = "cpu-work-concurrency", env = "ISKR_CPU_WORK_CONCURRENCY")]
    cpu_work_concurrency: Option<usize>,
    /// Maximum CPU-work units admitted at once per class — holding a concurrency
    /// permit or queued for one. Requests beyond the class-local limit are shed
    /// with 503 so a flood cannot grow that queue or consume another class's
    /// backlog budget. Defaults to 64x the CPU-work concurrency.
    #[arg(long = "cpu-work-max-inflight", env = "ISKR_CPU_WORK_MAX_INFLIGHT")]
    cpu_work_max_inflight: Option<usize>,
}

impl Config {
    /// Parses CLI arguments and environment variables into runtime configuration.
    pub fn load() -> Self {
        match Self::from_cli(Cli::parse()) {
            Ok(config) => config,
            Err(message) => {
                eprintln!("configuration error: {message}");
                std::process::exit(2);
            }
        }
    }

    /// Resolves derived settings and defaults from parsed CLI input.
    fn from_cli(cli: Cli) -> Result<Self, String> {
        // Use an explicit node id when configured (Kubernetes passes the pod
        // name); otherwise generate one. HOSTNAME is not auto-used because the
        // local dev cluster runs several nodes on one host.
        let node_id = cli
            .node_id
            .filter(|value| !value.is_empty())
            .unwrap_or_else(auto_node_id);
        let gossip_advertise_addr = cli.gossip_advertise_addr.unwrap_or(cli.gossip_bind);
        let http_listen_addr = SocketAddr::new(cli.gossip_bind.ip(), cli.http_port);
        let internal_listen_addr = SocketAddr::new(cli.gossip_bind.ip(), cli.internal_http_port);
        // The HTTP address peers forward to. `/_internal/*` (peer forwarding)
        // lives on the internal port, so this advertises the internal port —
        // never the Gateway-fronted public port. Published as its own gossip KV
        // rather than reconstructed from the gossip IP, which is fragile across
        // networks.
        let internal_http_advertise_addr = cli
            .internal_http_advertise_addr
            .unwrap_or_else(|| SocketAddr::new(gossip_advertise_addr.ip(), cli.internal_http_port));
        let seed_nodes = cli
            .gossip_seeds
            .filter(|values| !values.is_empty())
            .unwrap_or_default();

        // A node participates in a cluster when it joins peers (`--gossip-seeds`) or is
        // explicitly flagged as one (`--cluster`) — e.g. a seed node others join.
        // In that case a wildcard advertise address is published to peers but is
        // not routable, so reject it early.
        let in_cluster = cli.cluster || !seed_nodes.is_empty();
        if in_cluster {
            if gossip_advertise_addr.ip().is_unspecified() {
                return Err(format!(
                    "gossip advertise address {gossip_advertise_addr} is a wildcard; set --gossip-advertise-addr (ISKR_GOSSIP_ADVERTISE_ADDR) to a routable address in cluster mode"
                ));
            }
            if internal_http_advertise_addr.ip().is_unspecified() {
                return Err(format!(
                    "internal HTTP advertise address {internal_http_advertise_addr} is a wildcard; set --internal-http-advertise-addr (ISKR_INTERNAL_HTTP_ADVERTISE_ADDR) to a routable address in cluster mode"
                ));
            }
        }

        let backend_fetch_concurrency = cli.backend_fetch_concurrency.max(1);
        let chunk_fetch_merge_window = Duration::from_millis(cli.chunk_fetch_merge_window_ms);
        validate_chunked_store_limits(
            cli.chunk_size_bytes,
            cli.max_fetch_chunks,
            chunk_fetch_merge_window,
            backend_fetch_concurrency,
        )
        .map_err(|error| error.to_string())?;

        let cpu_work_concurrency = cli
            .cpu_work_concurrency
            .unwrap_or_else(default_cpu_work_concurrency)
            .max(1);
        if cpu_work_concurrency > MAX_CPU_WORK_CONCURRENCY {
            return Err(format!(
                "CPU work concurrency {cpu_work_concurrency} exceeds the safe maximum {MAX_CPU_WORK_CONCURRENCY}"
            ));
        }
        let cpu_work_max_inflight = cli
            .cpu_work_max_inflight
            .unwrap_or_else(|| cpu_work_concurrency.saturating_mul(64))
            .max(cpu_work_concurrency);
        if cpu_work_max_inflight > MAX_CPU_WORK_INFLIGHT {
            return Err(format!(
                "CPU work max inflight {cpu_work_max_inflight} exceeds the safe maximum {MAX_CPU_WORK_INFLIGHT}"
            ));
        }

        Ok(Self {
            node_id: node_id.clone(),
            http_port: cli.http_port,
            http_listen_addr,
            internal_listen_addr,
            membership: membership::MembershipConfig {
                node_id,
                listen_addr: cli.gossip_bind,
                advertise_addr: gossip_advertise_addr,
                http_advertise_addr: internal_http_advertise_addr,
                http_port: cli.http_port,
                seed_nodes,
                gossip_interval: Duration::from_millis(cli.gossip_interval_ms.max(1)),
            },
            tileset_sources: cli.tileset_sources,
            router_candidate_count: cli.router_candidate_count,
            router_tile_group_size: cli.router_tile_group_size,
            chunk_size_bytes: cli.chunk_size_bytes,
            max_fetch_chunks: cli.max_fetch_chunks,
            chunk_fetch_merge_window,
            backend_fetch_concurrency,
            artificial_backend_delay_ms: cli.artificial_backend_delay_ms,
            tile_cache_max_bytes: cli.tile_cache_max_bytes,
            chunk_cache_max_bytes: cli.chunk_cache_max_bytes,
            tile_negative_ttl: Duration::from_secs(cli.tile_negative_ttl_secs),
            style_templates: cli.style_templates.filter(|value| !value.trim().is_empty()),
            glyph_url_template: cli
                .glyph_url_template
                .filter(|value| !value.trim().is_empty()),
            sprite_templates: cli
                .sprite_templates
                .filter(|value| !value.trim().is_empty()),
            mapterhorn_tileset: cli
                .mapterhorn_tileset
                .filter(|value| !value.trim().is_empty()),
            mapterhorn_maxzoom: cli.mapterhorn_maxzoom,
            mapterhorn_negative_ttl: Duration::from_secs(cli.mapterhorn_negative_ttl_secs),
            cpu_work_concurrency,
            cpu_work_max_inflight,
        })
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

fn default_cpu_work_concurrency() -> usize {
    std::thread::available_parallelism().map_or(2, std::num::NonZeroUsize::get)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    fn cli() -> Cli {
        Cli {
            gossip_seeds: None,
            node_id: Some("node-a".to_string()),
            gossip_advertise_addr: None,
            internal_http_advertise_addr: None,
            gossip_bind: "0.0.0.0:7946".parse().unwrap(),
            http_port: 8080,
            internal_http_port: 9090,
            cluster: false,
            tileset_sources: "data".to_string(),
            router_candidate_count: 3,
            router_tile_group_size: 512,
            gossip_interval_ms: 200,
            chunk_size_bytes: 1024 * 1024,
            max_fetch_chunks: 4,
            chunk_fetch_merge_window_ms: 10,
            backend_fetch_concurrency: 32,
            artificial_backend_delay_ms: 0,
            tile_cache_max_bytes: 512 * 1024 * 1024,
            chunk_cache_max_bytes: 512 * 1024 * 1024,
            tile_negative_ttl_secs: 60,
            style_templates: None,
            glyph_url_template: None,
            sprite_templates: None,
            mapterhorn_tileset: None,
            mapterhorn_maxzoom: None,
            mapterhorn_negative_ttl_secs: 3600,
            cpu_work_concurrency: Some(2),
            cpu_work_max_inflight: None,
        }
    }

    fn config_error(cli: Cli) -> String {
        match Config::from_cli(cli) {
            Ok(_) => panic!("configuration unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    #[test]
    fn cpu_work_help_lists_provider_json_validation_and_style_rewriting() {
        let help = Cli::command().render_long_help().to_string();
        assert!(help.contains("provider JSON validation"), "{help}");
        assert!(help.contains("style rewriting"), "{help}");
    }

    #[test]
    fn cluster_seed_node_requires_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;

        let err = match Config::from_cli(cli) {
            Ok(_) => panic!("wildcard advertise is invalid in cluster mode"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn seeds_imply_cluster_validation() {
        let mut cli = cli();
        cli.gossip_seeds = Some(vec!["ishikari-gossip:7946".to_string()]);

        let err = match Config::from_cli(cli) {
            Ok(_) => panic!("seeds require routable advertise addresses"),
            Err(err) => err,
        };
        assert!(err.contains("gossip advertise address"));
    }

    #[test]
    fn local_single_node_allows_wildcard_listeners() {
        let config = Config::from_cli(cli()).expect("local single-node wildcard bind is allowed");

        assert_eq!(config.internal_listen_addr, "0.0.0.0:9090".parse().unwrap());
        assert!(config.membership.seed_nodes.is_empty());
        assert_eq!(config.cpu_work_concurrency, 2);
    }

    #[test]
    fn cpu_work_concurrency_is_always_positive() {
        assert!(default_cpu_work_concurrency() >= 1);

        let mut cli = cli();
        cli.cpu_work_concurrency = Some(0);
        let config = Config::from_cli(cli).expect("zero concurrency is clamped");
        assert_eq!(config.cpu_work_concurrency, 1);
    }

    #[test]
    fn zero_chunk_fetch_merge_window_is_allowed() {
        let mut cli = cli();
        cli.chunk_fetch_merge_window_ms = 0;
        let config = Config::from_cli(cli).expect("zero merge window is valid");
        assert_eq!(config.chunk_fetch_merge_window, Duration::ZERO);
    }

    #[test]
    fn excessive_chunk_fetch_merge_window_fails_at_startup() {
        let mut cli = cli();
        cli.chunk_fetch_merge_window_ms = 1_001;

        assert!(config_error(cli).contains("chunk fetch merge window"));
    }

    #[test]
    fn invalid_chunk_geometry_fails_at_startup() {
        let mut zero_size = cli();
        zero_size.chunk_size_bytes = 0;
        assert!(config_error(zero_size).contains("greater than zero"));

        let mut zero_count = cli();
        zero_count.max_fetch_chunks = 0;
        assert!(config_error(zero_count).contains("greater than zero"));

        let mut oversized_span = cli();
        oversized_span.chunk_size_bytes = 32 * 1024 * 1024;
        oversized_span.max_fetch_chunks = 4;
        assert!(config_error(oversized_span).contains("fetch span"));

        let mut oversized_active = cli();
        oversized_active.chunk_size_bytes = 16 * 1024 * 1024;
        oversized_active.max_fetch_chunks = 4;
        oversized_active.backend_fetch_concurrency = 256;
        assert!(config_error(oversized_active).contains("active backend fetch budget"));
    }

    #[test]
    fn backend_fetch_concurrency_is_always_positive() {
        let mut cli = cli();
        cli.backend_fetch_concurrency = 0;
        let config = Config::from_cli(cli).expect("zero concurrency is clamped");
        assert_eq!(config.backend_fetch_concurrency, 1);
    }

    #[test]
    fn excessive_semaphore_limits_fail_at_startup() {
        let mut backend = cli();
        backend.backend_fetch_concurrency = 1_025;
        assert!(config_error(backend).contains("backend fetch concurrency"));

        let mut cpu = cli();
        cpu.cpu_work_concurrency = Some(MAX_CPU_WORK_CONCURRENCY + 1);
        assert!(config_error(cpu).contains("CPU work concurrency"));

        let mut inflight = cli();
        inflight.cpu_work_max_inflight = Some(MAX_CPU_WORK_INFLIGHT + 1);
        assert!(config_error(inflight).contains("CPU work max inflight"));
    }

    #[test]
    fn cpu_work_max_inflight_defaults_to_multiple_of_concurrency() {
        let mut cli = cli();
        cli.cpu_work_concurrency = Some(4);
        cli.cpu_work_max_inflight = None;
        let config = Config::from_cli(cli).expect("defaults resolve");
        // Generous headroom over the concurrency so normal viewport bursts pass.
        assert_eq!(config.cpu_work_max_inflight, 4 * 64);
    }

    #[test]
    fn cpu_work_max_inflight_is_never_below_concurrency() {
        let mut cli = cli();
        cli.cpu_work_concurrency = Some(8);
        // A too-small explicit ceiling is clamped up to the concurrency so at
        // least every permit holder is admissible.
        cli.cpu_work_max_inflight = Some(1);
        let config = Config::from_cli(cli).expect("ceiling is clamped");
        assert_eq!(config.cpu_work_max_inflight, 8);
    }

    #[test]
    fn cluster_allows_seed_node_with_routable_advertise_addresses() {
        let mut cli = cli();
        cli.cluster = true;
        cli.gossip_advertise_addr = Some("127.0.0.1:7946".parse().unwrap());
        cli.internal_http_advertise_addr = Some("127.0.0.1:9090".parse().unwrap());

        let config = Config::from_cli(cli).expect("routable seed-node advertise addresses work");
        assert_eq!(
            config.membership.advertise_addr,
            "127.0.0.1:7946".parse().unwrap()
        );
        assert_eq!(
            config.membership.http_advertise_addr,
            "127.0.0.1:9090".parse().unwrap()
        );
        assert!(config.membership.seed_nodes.is_empty());
    }
}

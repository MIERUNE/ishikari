use std::{io::IsTerminal, sync::Arc, time::Duration};

use anyhow::Result;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

use ishikari::{
    config::Config,
    drain::DrainController,
    membership::Membership,
    metrics::NodeMetrics,
    server::{
        AppState, TileRuntimeConfig, provider::ProviderConfig, run_http_server,
        tileset::mapterhorn::MapterhornResolver,
    },
    storage::{ObjectStoreRegistry, ResourceResolver, ResourceResolverConfig, redact_source_list},
};

const DRAINING_PROPAGATION_DELAY: Duration = Duration::from_secs(2);
const STATS_REPORT_INTERVAL: Duration = Duration::from_secs(5);

#[tokio::main]
async fn main() -> Result<()> {
    // Set up logging
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("ishikari=info"));
    let use_ansi = std::io::stdout().is_terminal();
    let _ = fmt()
        .with_env_filter(env_filter)
        .with_target(false)
        .with_ansi(use_ansi)
        .compact()
        .try_init();

    // Load configuration
    let config = Config::load();
    info!(
        http_listen_addr = %config.http_listen_addr,
        internal_listen_addr = %config.internal_listen_addr,
        http_port = config.http_port,
        gossip_bind = %config.membership.listen_addr,
        gossip_advertise_addr = %config.membership.advertise_addr,
        internal_http_advertise_addr = %config.membership.http_advertise_addr,
        seed_nodes = ?config.membership.seed_nodes,
        // Redacted: the raw value can carry signed query strings, API keys, or
        // URL userinfo. Log only namespace + scheme + authority.
        tileset_sources = %redact_source_list(&config.tileset_sources),
        chunk_size_bytes = config.chunk_size_bytes,
        max_fetch_chunks = config.max_fetch_chunks,
        chunk_fetch_merge_window_ms = config.chunk_fetch_merge_window.as_millis(),
        backend_fetch_concurrency = config.backend_fetch_concurrency,
        artificial_backend_delay_ms = config.artificial_backend_delay_ms,
        tile_cache_max_bytes = config.tile_cache_max_bytes,
        cpu_work_concurrency = config.cpu_work_concurrency,
        "starting node"
    );

    let membership = Membership::spawn(config.membership).await?;
    let metrics = NodeMetrics::new();
    metrics.set_chunk_config(config.chunk_size_bytes, config.max_fetch_chunks);
    let drain = DrainController::new();
    let provider = ProviderConfig::new(
        config.style_templates,
        config.glyph_url_template,
        config.sprite_templates,
    )
    .map_err(anyhow::Error::msg)?;
    // Shared by tile reads and provider fetches so stores (connection pools and
    // credentials) are reused per bucket/host across both.
    let object_store_registry = Arc::new(ObjectStoreRegistry::new());

    let resource_resolver = Arc::new(
        ResourceResolver::new(ResourceResolverConfig {
            self_node_id: config.node_id.clone(),
            membership: membership.clone(),
            tileset_sources: config.tileset_sources,
            candidate_count: config.router_candidate_count,
            tile_group_size: config.router_tile_group_size,
            chunk_size_bytes: config.chunk_size_bytes,
            max_fetch_chunks: config.max_fetch_chunks,
            chunk_fetch_merge_window: config.chunk_fetch_merge_window,
            backend_fetch_concurrency: config.backend_fetch_concurrency,
            artificial_backend_delay_ms: config.artificial_backend_delay_ms,
            tile_cache_max_bytes: config.tile_cache_max_bytes,
            chunk_cache_max_bytes: config.chunk_cache_max_bytes,
            tile_negative_ttl: config.tile_negative_ttl,
            object_store_registry: object_store_registry.clone(),
            metrics: metrics.clone(),
        })
        .await?,
    );

    spawn_stats_reporter(
        membership.clone(),
        resource_resolver.clone(),
        metrics.clone(),
    );

    // Optional Mapterhorn composite tileset: z<=12 from the base archive, z>12
    // from per-region detail archives. Disabled unless a tileset key is set;
    // when set, the advertised detail max zoom must be configured explicitly.
    let mapterhorn = match config.mapterhorn_tileset.as_deref() {
        Some(key) => {
            let maxzoom = config.mapterhorn_maxzoom.ok_or_else(|| {
                anyhow::anyhow!(
                    "ISKR_MAPTERHORN_MAXZOOM is required when ISKR_MAPTERHORN_TILESET is set \
                     (the detail archives' max zoom, e.g. 16)"
                )
            })?;
            Some(Arc::new(MapterhornResolver::new(
                key,
                maxzoom,
                config.mapterhorn_negative_ttl,
            )?))
        }
        None => None,
    };

    run_http_server(
        AppState::new(
            membership.clone(),
            metrics,
            resource_resolver,
            drain.clone(),
            provider,
            object_store_registry,
            TileRuntimeConfig {
                mapterhorn,
                cpu_work_concurrency: config.cpu_work_concurrency,
                cpu_work_max_inflight: config.cpu_work_max_inflight,
                derived_negative_ttl: config.tile_negative_ttl,
            },
        ),
        config.http_listen_addr,
        config.internal_listen_addr,
        shutdown_signal(membership.clone(), drain),
    )
    .await?;

    let _ = membership.shutdown();

    Ok(())
}

async fn shutdown_signal(membership: Membership, drain: DrainController) {
    wait_for_shutdown_signal().await;
    info!("shutdown signal received; draining");
    // Stop admitting new data/peer requests locally first, then announce
    // draining to the cluster, then wait for the state to propagate and load
    // balancers to deregister before the graceful shutdown drains in-flight work.
    drain.begin();
    membership.set_draining(true).await;
    tokio::time::sleep(DRAINING_PROPAGATION_DELAY).await;
}

async fn wait_for_shutdown_signal() {
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = terminate => {}
    }
}

fn spawn_stats_reporter(
    membership: Membership,
    resource_resolver: Arc<ResourceResolver>,
    metrics: NodeMetrics,
) {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(STATS_REPORT_INTERVAL);
        loop {
            ticker.tick().await;
            membership
                .set_many(&[
                    (
                        "cache-tile-bytes",
                        resource_resolver.tile_cache_weighted_size().to_string(),
                    ),
                    (
                        "cache-chunk-bytes",
                        resource_resolver.chunk_cache_weighted_size().to_string(),
                    ),
                    (
                        "transfer-external-bytes",
                        metrics.egress_bytes().to_string(),
                    ),
                    (
                        "transfer-internal-bytes",
                        metrics.internal_bytes().to_string(),
                    ),
                    (
                        "transfer-backend-bytes",
                        resource_resolver.received_bytes().to_string(),
                    ),
                ])
                .await;
        }
    });
}

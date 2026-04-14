use std::{io::IsTerminal, sync::Arc, time::Duration};

use anyhow::Result;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

use ishikari::{
    config::Config,
    membership::Membership,
    metrics::NodeMetrics,
    server::{AppState, run_http_server},
    storage::{ResourceResolver, ResourceResolverConfig},
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
        http_port = config.http_port,
        listen_addr = %config.membership.listen_addr,
        advertise_addr = %config.membership.advertise_addr,
        seed_nodes = ?config.membership.seed_nodes,
        data_url = %config.data_url,
        chunk_size_bytes = config.chunk_size_bytes,
        max_fetch_chunks = config.max_fetch_chunks,
        debug_fetch_delay_ms = config.debug_fetch_delay_ms,
        tile_cache_max_bytes = config.tile_cache_max_bytes,
        "starting node"
    );

    let membership = Membership::spawn(config.membership).await?;
    let metrics = NodeMetrics::new();

    let resource_resolver = Arc::new(
        ResourceResolver::new(ResourceResolverConfig {
            self_node_id: config.node_id.clone(),
            membership: membership.clone(),
            data_url: config.data_url,
            candidate_count: config.router_candidate_count,
            tile_group_size: config.router_tile_group_size,
            chunk_size_bytes: config.chunk_size_bytes,
            max_fetch_chunks: config.max_fetch_chunks,
            debug_fetch_delay_ms: config.debug_fetch_delay_ms,
            tile_cache_max_bytes: config.tile_cache_max_bytes,
            chunk_cache_max_bytes: config.chunk_cache_max_bytes,
        })
        .await?,
    );

    spawn_stats_reporter(membership.clone(), resource_resolver.clone(), metrics.clone());

    run_http_server(
        AppState::new(membership.clone(), metrics, resource_resolver),
        config.http_listen_addr,
        shutdown_signal(membership.clone()),
    )
    .await?;

    let _ = membership.shutdown();

    Ok(())
}

async fn shutdown_signal(membership: Membership) {
    wait_for_shutdown_signal().await;
    info!("shutdown signal received");
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

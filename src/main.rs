use std::{io::IsTerminal, sync::Arc, time::Duration};

use anyhow::Result;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt};

use ishikari::{
    cluster::membership::Membership,
    config::Config,
    server::{AppState, run_http_server},
    tilesets::{TilesetService, TilesetServiceConfig},
};

const DRAINING_PROPAGATION_DELAY: Duration = Duration::from_secs(2);

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
        node_id = %config.node_id,
        http_listen_addr = %config.http_listen_addr,
        http_port = config.http_port,
        listen_addr = %config.membership.listen_addr,
        advertise_addr = %config.membership.advertise_addr,
        seed_nodes = ?config.membership.seed_nodes,
        data_url = %config.data_url,
        chunk_size_bytes = config.chunk_size_bytes,
        max_fetch_chunks = config.max_fetch_chunks,
        backend_fetch_delay_ms = config.backend_fetch_delay_ms,
        tile_cache_max_bytes = config.tile_cache_max_bytes,
        "starting node"
    );

    let membership = Membership::spawn(config.membership).await?;

    let tileset_service = Arc::new(
        TilesetService::new(TilesetServiceConfig {
            self_node_id: config.node_id.clone(),
            membership: membership.clone(),
            data_url: config.data_url,
            candidate_count: config.router_candidate_count,
            tile_group_size: config.router_tile_group_size,
            chunk_size_bytes: config.chunk_size_bytes,
            max_fetch_chunks: config.max_fetch_chunks,
            backend_fetch_delay_ms: config.backend_fetch_delay_ms,
            tile_cache_max_bytes: config.tile_cache_max_bytes,
            chunk_cache_max_bytes: config.chunk_cache_max_bytes,
        })
        .await?,
    );

    run_http_server(
        AppState::new(membership.clone(), tileset_service),
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

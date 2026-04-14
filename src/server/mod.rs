//! HTTP app wiring and shared state.

use std::{future::Future, net::SocketAddr, sync::Arc};

use crate::{membership::Membership, metrics::NodeMetrics, server, storage::ResourceResolver};
use anyhow::{Context, Result};
use axum::{
    Json, Router, ServiceExt,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::get,
};
use tokio::net::TcpListener;

pub(crate) type HttpError = (StatusCode, String);

#[derive(Clone)]
pub struct AppState {
    membership: Membership,
    pub(crate) metrics: NodeMetrics,
    resource_resolver: Arc<ResourceResolver>,
}

impl AppState {
    pub fn new(
        membership: Membership,
        metrics: NodeMetrics,
        resource_resolver: Arc<ResourceResolver>,
    ) -> Self {
        Self {
            membership,
            metrics,
            resource_resolver,
        }
    }
}

pub async fn run_http_server(
    state: AppState,
    listen_addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let app = Router::new()
        .route("/", get(root))
        .route("/_internal/cluster", get(cluster_handler))
        .route("/livez", get(livez))
        .route("/readyz", get(readyz))
        .route(
            "/tilesets/{tileset_id}",
            get(server::tileset::tilejson_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview",
            get(server::tileset::preview_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview.json",
            get(server::tileset::preview_style_handler),
        )
        .route(
            "/tilesets/{tileset_id}/{z}/{x}/{y}",
            get(server::tileset::tile_handler),
        )
        .route(
            "/_internal/tiles/{tileset_id}/{tile_id}",
            get(server::tileset::internal_tile_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/bootstrap",
            get(server::internal::internal_bootstrap_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/leaf/{offset}/{length}",
            get(server::internal::internal_leaf_handler),
        )
        .fallback(not_found)
        .with_state(state);

    // let app: NormalizePath<Router> = NormalizePath::trim_trailing_slash(app);

    let listener = TcpListener::bind(listen_addr)
        .await
        .with_context(|| format!("failed to bind {listen_addr}"))?;

    axum::serve(
        listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(app),
    )
    .with_graceful_shutdown(shutdown)
    .await
    .context("http server failed")
}

pub(crate) fn get_origin(headers: &HeaderMap) -> String {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    let origin_parts = origin.and_then(split_origin);
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or_else(|| origin_parts.map(|(origin_scheme, _)| origin_scheme))
        .unwrap_or("http");
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or_else(|| origin_parts.map(|(_, origin_host)| origin_host))
        .unwrap_or("127.0.0.1:8080");
    format!("{scheme}://{host}")
}

/// Reports whether this node process is alive.
async fn livez() -> StatusCode {
    StatusCode::OK
}

/// Reports whether this node is ready to receive traffic.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    if !state.membership.is_ready() || state.membership.is_draining().await {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

/// Serves the minimal root endpoint.
async fn root() -> &'static str {
    "ishikari\n"
}

/// Serves the default 404 response for unknown routes.
async fn not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "not found\n")
}

/// Returns the current cluster membership snapshot.
async fn cluster_handler(State(state): State<AppState>) -> Json<crate::membership::ClusterView> {
    Json(state.membership.cluster_view().await)
}

/// Splits an Origin header into scheme and host components.
fn split_origin(origin: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = origin.split_once("://")?;
    let host = rest.split('/').next()?;
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    Some((scheme, host))
}

pub mod internal;
pub mod tileset;

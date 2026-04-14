//! Internal PMTiles forwarding endpoints shared across cluster nodes.

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{
        HeaderValue, StatusCode,
        header::{self},
    },
    response::Response,
};
use bytes::BufMut;
use serde::Deserialize;
use tracing::debug;

use crate::{
    interned::TilesetId,
    server::{AppState, HttpError},
};

use super::tileset::tileset_error_response;

#[derive(Deserialize)]
pub(crate) struct BootstrapQuery {
    #[serde(default)]
    metadata: bool,
}

/// Serves PMTiles bootstrap bytes for peer cache reuse, optionally including metadata.
pub(crate) async fn internal_bootstrap_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    Query(query): Query<BootstrapQuery>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let include_metadata = query.metadata;
    let transfer = state
        .resource_resolver
        .load_bootstrap_bytes(tileset_id.clone(), include_metadata)
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;

    let body_bytes = if let Some(metadata) = transfer.metadata {
        let bootstrap_len = transfer.bootstrap.len() as u64;
        let mut buf =
            bytes::BytesMut::with_capacity(8 + transfer.bootstrap.len() + metadata.len());
        buf.put_u64_le(bootstrap_len);
        buf.extend_from_slice(&transfer.bootstrap);
        buf.extend_from_slice(&metadata);
        buf.freeze()
    } else {
        transfer.bootstrap
    };

    state.metrics.add_internal_bytes(body_bytes.len() as u64);
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_bootstrap",
            tileset_id = %tileset_id,
            include_metadata = include_metadata,
            served_bytes = body_bytes.len(),
            "served internal response"
        );
    }
    let mut response = Response::new(Body::from(body_bytes));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

/// Serves raw PMTiles leaf bytes for peer cache reuse.
pub(crate) async fn internal_leaf_handler(
    State(state): State<AppState>,
    Path((tileset_id, offset, length)): Path<(String, u64, usize)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let leaf = state
        .resource_resolver
        .load_leaf_bytes(tileset_id.clone(), offset, length)
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    state.metrics.add_internal_bytes(leaf.len() as u64);
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_leaf",
            tileset_id = %tileset_id,
            served_bytes = leaf.len(),
            "served internal response"
        );
    }
    let mut response = Response::new(Body::from(leaf));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

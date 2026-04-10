//! Internal PMTiles forwarding endpoints shared across cluster nodes.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderValue, StatusCode,
        header::{self},
    },
    response::Response,
};
use tracing::debug;

use crate::{
    interned_str::TilesetId,
    server::{AppState, HttpError},
};

use super::tileset::tileset_error_response;

/// Serves PMTiles archive bootstrap state for peer cache reuse.
pub(crate) async fn internal_archive_index_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    let archive = state
        .tileset_service
        .load_archive_index_bytes(tileset_id.clone())
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_archive_index",
            tileset_id = %tileset_id,
            served_bytes = archive.len(),
            "served internal response"
        );
    }
    let mut response = Response::new(Body::from(archive));
    *response.status_mut() = StatusCode::OK;
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/octet-stream"),
    );
    Ok(response)
}

/// Serves raw PMTiles metadata bytes for peer cache reuse.
pub(crate) async fn internal_metadata_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    let metadata = state
        .tileset_service
        .load_metadata_bytes(tileset_id.clone())
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    if tracing::enabled!(tracing::Level::DEBUG) {
        debug!(
            endpoint = "internal_metadata",
            tileset_id = %tileset_id,
            served_bytes = metadata.len(),
            "served internal response"
        );
    }
    let mut response = Response::new(Body::from(metadata));
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
    let tileset_id = TilesetId::from(tileset_id);
    let leaf = state
        .tileset_service
        .load_leaf_bytes(tileset_id.clone(), offset, length)
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
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

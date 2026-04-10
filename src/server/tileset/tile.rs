//! Axum handlers for tile-serving endpoints.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{
        HeaderValue, StatusCode,
        header::{self},
    },
    response::{IntoResponse, Response},
};
use tracing::debug;

use crate::{
    interned_str::TilesetId,
    pmtiles::{TileCoord, TileData, TileId},
    server::{AppState, HttpError},
};

use super::error::tileset_error_response;

/// Serves the external z/x/y tile endpoint.
pub(crate) async fn tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, z, x, y)): Path<(String, u8, u32, u32)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    state
        .tileset_service
        .route_tile(tileset_id, tile_id)
        .await
        .map_err(tileset_error_response)?
        .map(|tile| {
            debug!(
                endpoint = "tile",
                served_bytes = tile.bytes.len(),
                "served external response"
            );
            TilesetResponse::from(tile).into_response()
        })
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))
}

/// Serves the internal tile endpoint used for node-to-node forwarding.
pub(crate) async fn internal_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, tile_id)): Path<(String, u64)>,
) -> Result<Response<Body>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    state
        .tileset_service
        .load_tile_by_id(tileset_id, tile_id)
        .await
        .map_err(tileset_error_response)?
        .map(|tile| {
            debug!(
                endpoint = "internal_tile",
                served_bytes = tile.bytes.len(),
                "served internal response"
            );
            TilesetResponse::from(tile).into_response()
        })
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))
}

struct TilesetResponse {
    bytes: bytes::Bytes,
    content_type: &'static str,
    content_encoding: Option<&'static str>,
}

impl From<TileData> for TilesetResponse {
    /// Converts tile bytes plus headers into an HTTP response wrapper.
    fn from(tile: TileData) -> Self {
        Self {
            bytes: tile.bytes,
            content_type: tile.content_type,
            content_encoding: tile.content_encoding,
        }
    }
}

impl IntoResponse for TilesetResponse {
    /// Finalizes the wrapped tile into an HTTP response.
    fn into_response(self) -> Response {
        let mut response = Response::new(Body::from(self.bytes));
        *response.status_mut() = StatusCode::OK;
        response.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(self.content_type),
        );
        if let Some(content_encoding) = self.content_encoding {
            response.headers_mut().insert(
                header::CONTENT_ENCODING,
                HeaderValue::from_static(content_encoding),
            );
        }
        response
    }
}

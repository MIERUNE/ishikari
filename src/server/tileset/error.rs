//! HTTP error conversion helpers for tileset handlers.

use axum::http::StatusCode;
use tracing::error;

use crate::server::HttpError;
use crate::tilesets::TilesetError;

/// Converts service-layer tileset errors into HTTP status codes and messages.
pub(crate) fn tileset_error_response(error: TilesetError) -> HttpError {
    match error {
        TilesetError::InvalidInput(message) => (StatusCode::BAD_REQUEST, message),
        TilesetError::Upstream(message) | TilesetError::RetryableUpstream(message) => {
            (StatusCode::BAD_GATEWAY, message)
        }
        TilesetError::Timeout(message) => (StatusCode::GATEWAY_TIMEOUT, message),
        TilesetError::Miss => (StatusCode::NOT_FOUND, "not found".to_string()),
        TilesetError::Internal(message) => {
            error!(error = %message, "returning internal server error");
            (StatusCode::INTERNAL_SERVER_ERROR, message)
        }
    }
}

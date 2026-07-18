//! HTTP error conversion helpers for tileset handlers.

use axum::http::StatusCode;
use tracing::error;

use crate::server::HttpError;
use crate::storage::TilesetError;

/// Converts service-layer tileset errors into HTTP status codes and messages.
pub(crate) fn tileset_error_response(error: &TilesetError) -> HttpError {
    match error {
        TilesetError::Upstream(_) | TilesetError::RetryableUpstream(_) => {
            // Upstream/object-store sources may contain signed request URLs.
            // Keep both logs and the public response stable and source-free.
            error!("upstream tileset request failed");
            (
                StatusCode::BAD_GATEWAY,
                "upstream tileset request failed".to_string(),
            )
        }
        TilesetError::Timeout(_) => {
            error!("upstream tileset request timed out");
            (
                StatusCode::GATEWAY_TIMEOUT,
                "upstream tileset request timed out".to_string(),
            )
        }
        TilesetError::Overload(_) => {
            // Backend fetch admission is saturated. Shed with 503 (retryable)
            // rather than queue unboundedly.
            error!("backend fetch admission saturated");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "backend fetch temporarily unavailable".to_string(),
            )
        }
        TilesetError::Miss => (StatusCode::NOT_FOUND, "not found".to_string()),
        TilesetError::Internal(_) => {
            error!("returning internal server error");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal server error".to_string(),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_errors_do_not_expose_internal_details() {
        let secret = "gs://private-bucket/archive.pmtiles: permission denied";
        for (error, expected) in [
            (
                TilesetError::Upstream(secret.to_string()),
                "upstream tileset request failed",
            ),
            (
                TilesetError::RetryableUpstream(secret.to_string()),
                "upstream tileset request failed",
            ),
            (
                TilesetError::Internal(secret.to_string()),
                "internal server error",
            ),
            (
                TilesetError::Timeout(secret.to_string()),
                "upstream tileset request timed out",
            ),
            (
                TilesetError::Overload(secret.to_string()),
                "backend fetch temporarily unavailable",
            ),
        ] {
            let (_, body) = tileset_error_response(&error);
            assert_eq!(body, expected);
            assert!(!body.contains(secret));
        }
    }

    #[test]
    fn backend_overload_maps_to_503() {
        let (status, _) = tileset_error_response(&TilesetError::Overload("saturated".to_string()));
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }
}

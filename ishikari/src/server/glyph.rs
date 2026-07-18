//! Glyph PBF provider endpoint.

use axum::{
    body::Body,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};

use crate::server::{
    AppState, HttpError,
    provider::{path_percent_encode, provider_routing_key},
    upstream::{ProviderResource, fetch_limited_bytes_with_content_type, route_or_fetch_provider},
};

const MAX_FONTSTACK_LEN: usize = 256;
const MAX_GLYPH_BYTES: usize = 1024 * 1024;
const GLYPH_CONTENT_TYPES: &[&str] = &[
    "application/x-protobuf",
    "application/vnd.google.protobuf",
    "application/protobuf",
    "application/octet-stream",
];

pub(crate) async fn glyph_handler(
    State(state): State<AppState>,
    Path((fontstack, range)): Path<(String, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    validate_fontstack(&fontstack)?;
    let range = validate_range(&range)?;
    let upstream = resolve_glyph_url(&state, &fontstack, &range)?;
    let resource = route_glyph_bytes(&state, &fontstack, &range, &upstream).await?;
    Ok(resource.verbatim_public_response(&headers, "application/x-protobuf"))
}

pub(crate) async fn internal_glyph_handler(
    State(state): State<AppState>,
    Path((fontstack, range)): Path<(String, String)>,
) -> Result<Response<Body>, HttpError> {
    validate_fontstack(&fontstack)?;
    let range = validate_range(&range)?;
    let upstream = resolve_glyph_url(&state, &fontstack, &range)?;
    let resource = fetch_glyph_bytes_local(&state, upstream).await?;
    state
        .metrics
        .add_internal_bytes(resource.bytes().len() as u64);
    Ok(resource.verbatim_internal_response("application/x-protobuf"))
}

fn resolve_glyph_url(state: &AppState, fontstack: &str, range: &str) -> Result<String, HttpError> {
    state
        .provider
        .resolve_glyph_url(fontstack, range)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "glyph provider not configured".to_string(),
            )
        })
}

async fn route_glyph_bytes(
    state: &AppState,
    fontstack: &str,
    range: &str,
    upstream: &str,
) -> Result<ProviderResource, HttpError> {
    let key = provider_routing_key("glyph", upstream);
    let path = format!(
        "/_internal/provider/fonts/{}/{}.pbf",
        path_percent_encode(fontstack),
        range
    );
    route_or_fetch_provider(
        state,
        &key,
        &path,
        "glyph",
        fetch_glyph_bytes_local(state, upstream.to_string()),
    )
    .await
}

async fn fetch_glyph_bytes_local(
    state: &AppState,
    upstream: String,
) -> Result<ProviderResource, HttpError> {
    fetch_limited_bytes_with_content_type(
        state,
        upstream,
        MAX_GLYPH_BYTES,
        "glyph",
        GLYPH_CONTENT_TYPES,
    )
    .await
}

fn validate_fontstack(fontstack: &str) -> Result<(), HttpError> {
    if fontstack.is_empty() || fontstack.len() > MAX_FONTSTACK_LEN {
        return Err((
            StatusCode::BAD_REQUEST,
            "fontstack length invalid".to_string(),
        ));
    }
    if fontstack
        .split(',')
        .any(|part| part.trim().is_empty() || part.contains('/') || part.contains('\\'))
    {
        return Err((StatusCode::BAD_REQUEST, "fontstack invalid".to_string()));
    }
    Ok(())
}

fn validate_range(range: &str) -> Result<String, HttpError> {
    let (start, end) = range
        .strip_suffix(".pbf")
        .unwrap_or(range)
        .split_once('-')
        .ok_or_else(|| (StatusCode::BAD_REQUEST, "glyph range invalid".to_string()))?;
    let start = start
        .parse::<u32>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "glyph range invalid".to_string()))?;
    let end = end
        .parse::<u32>()
        .map_err(|_| (StatusCode::BAD_REQUEST, "glyph range invalid".to_string()))?;
    if start % 256 != 0 || start.checked_add(255) != Some(end) {
        return Err((StatusCode::BAD_REQUEST, "glyph range invalid".to_string()));
    }
    Ok(format!("{start}-{end}"))
}

#[cfg(test)]
mod tests {
    use super::{validate_fontstack, validate_range};

    #[test]
    fn validates_256_codepoint_ranges() {
        assert_eq!(validate_range("0-255").unwrap(), "0-255");
        assert_eq!(validate_range("65280-65535.pbf").unwrap(), "65280-65535");
        assert!(validate_range("1-256").is_err());
        assert!(validate_range("0-254").is_err());
    }

    #[test]
    fn rejects_bad_fontstacks() {
        assert!(validate_fontstack("Noto Sans JP,Arial").is_ok());
        assert!(validate_fontstack("").is_err());
        assert!(validate_fontstack("Noto/../../Sans").is_err());
        assert!(validate_fontstack("Noto,,Arial").is_err());
    }
}

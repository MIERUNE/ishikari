//! MapLibre sprite JSON/PNG provider endpoint.

use axum::{
    body::Body,
    http::{HeaderMap, StatusCode},
    response::Response,
};

use crate::server::{
    AppState, HttpError,
    provider::{path_percent_encode_segments, provider_routing_key},
    style::validate_style_key,
    upstream::{
        ProviderResource, fetch_limited_bytes_with_content_type, fetch_limited_json,
        route_or_fetch_provider,
    },
};

const MAX_SPRITE_JSON_BYTES: usize = 2 * 1024 * 1024;
const MAX_SPRITE_PNG_BYTES: usize = 8 * 1024 * 1024;
const SPRITE_JSON_CONTENT_TYPES: &[&str] =
    &["application/json", "text/json", "application/octet-stream"];
const SPRITE_PNG_CONTENT_TYPES: &[&str] = &["image/png", "application/octet-stream"];

pub(crate) async fn serve_sprite(
    state: AppState,
    style_key: String,
    suffix: String,
    headers: &HeaderMap,
) -> Result<Response<Body>, HttpError> {
    validate_style_key(&style_key)?;
    let upstream = resolve_sprite_url(&state, &style_key, &suffix)?;
    let resource = route_sprite_bytes(&state, &style_key, &suffix, &upstream).await?;
    Ok(resource.verbatim_public_response(headers, sprite_content_type(&suffix)))
}

pub(crate) async fn serve_sprite_local(
    state: AppState,
    style_key: String,
    suffix: String,
) -> Result<Response<Body>, HttpError> {
    validate_style_key(&style_key)?;
    let upstream = resolve_sprite_url(&state, &style_key, &suffix)?;
    let resource = fetch_sprite_bytes_local(&state, upstream, &suffix).await?;
    state
        .metrics
        .add_internal_bytes(resource.bytes().len() as u64);
    Ok(resource.verbatim_internal_response(sprite_content_type(&suffix)))
}

fn resolve_sprite_url(
    state: &AppState,
    style_key: &str,
    suffix: &str,
) -> Result<String, HttpError> {
    state
        .provider
        .resolve_sprite_url(style_key, suffix)
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                "sprite provider not configured".to_string(),
            )
        })
}

async fn route_sprite_bytes(
    state: &AppState,
    style_key: &str,
    suffix: &str,
    upstream: &str,
) -> Result<ProviderResource, HttpError> {
    let key = provider_routing_key(&format!("sprite{suffix}"), upstream);
    let path = format!(
        "/_internal/provider/styles/{}/sprite{}",
        path_percent_encode_segments(style_key),
        suffix
    );
    route_or_fetch_provider(
        state,
        &key,
        &path,
        "sprite",
        fetch_sprite_bytes_local(state, upstream.to_string(), suffix),
    )
    .await
}

async fn fetch_sprite_bytes_local(
    state: &AppState,
    upstream: String,
    suffix: &str,
) -> Result<ProviderResource, HttpError> {
    let is_png = suffix.ends_with(".png");
    let max_bytes = if is_png {
        MAX_SPRITE_PNG_BYTES
    } else {
        MAX_SPRITE_JSON_BYTES
    };
    let accepted_content_types = if is_png {
        SPRITE_PNG_CONTENT_TYPES
    } else {
        SPRITE_JSON_CONTENT_TYPES
    };
    if is_png {
        fetch_limited_bytes_with_content_type(
            state,
            upstream,
            max_bytes,
            "sprite",
            accepted_content_types,
        )
        .await
    } else {
        fetch_limited_json(state, upstream, max_bytes, "sprite", accepted_content_types).await
    }
}

fn sprite_content_type(suffix: &str) -> &'static str {
    if suffix.ends_with(".png") {
        "image/png"
    } else {
        "application/json"
    }
}

pub(crate) fn parse_sprite_path(style_path: &str) -> Option<(String, String)> {
    for (tail, suffix) in [
        ("/sprite@2x.json", "@2x.json"),
        ("/sprite@2x.png", "@2x.png"),
        ("/sprite.json", ".json"),
        ("/sprite.png", ".png"),
    ] {
        if let Some(style_key) = style_path.strip_suffix(tail)
            && !style_key.is_empty()
        {
            return Some((style_key.to_string(), suffix.to_string()));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::parse_sprite_path;

    #[test]
    fn parses_sprite_paths_under_style_key() {
        assert_eq!(
            parse_sprite_path("carto/voyager/sprite.json"),
            Some(("carto/voyager".to_string(), ".json".to_string()))
        );
        assert_eq!(
            parse_sprite_path("carto/voyager/sprite@2x.png"),
            Some(("carto/voyager".to_string(), "@2x.png".to_string()))
        );
        assert_eq!(parse_sprite_path("carto/voyager/style.json"), None);
    }
}

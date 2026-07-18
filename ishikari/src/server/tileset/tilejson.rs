//! TileJSON handler and response generation for tileset endpoints.

use std::collections::BTreeMap;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Response,
};
use serde::Deserialize;
use tracing::debug;

use crate::{
    interned::TilesetId,
    pmtiles::{TileType, Tilestats, VectorLayer},
    server::{AppState, HttpError, cache, conditional::Validators, get_origin},
    storage::TilesetInfo,
};

use super::error::tileset_error_response;

#[derive(Deserialize)]
pub(crate) struct TileJsonQuery {
    encoding: Option<String>,
}

#[derive(serde::Serialize, Debug, Clone)]
pub(crate) struct TileJson {
    pub tilejson: String,
    pub tiles: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub vector_layers: Vec<VectorLayer>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attribution: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bounds: Option<[f64; 4]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub center: Option<(f64, f64, u8)>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maxzoom: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minzoom: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tilestats: Option<Tilestats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub encoding: Option<String>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

/// Serves TileJSON for a flat tileset key.
pub(crate) async fn tilejson_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<TileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(state, tileset_id, headers, query).await
}

/// Serves TileJSON for a `{namespace}/{tileset_id}` key.
pub(crate) async fn namespaced_tilejson_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<TileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        headers,
        query,
    )
    .await
}

/// Builds TileJSON for an already-joined tileset key.
async fn serve_tilejson(
    state: AppState,
    tileset_id: String,
    headers: HeaderMap,
    query: TileJsonQuery,
) -> Result<Response, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let base_url = get_origin(&headers);
    let data = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|e| tileset_error_response(&e))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    // The Mapterhorn composite tileset advertises the detail max zoom so clients
    // request z13+ tiles; its base header only reports the z12 base archive.
    let maxzoom_override = state
        .mapterhorn()
        .filter(|resolver| resolver.matches(&tileset_id))
        .map(|resolver| resolver.maxzoom());
    let document = tilejson(
        &tileset_id,
        &base_url,
        &data,
        query.encoding.as_deref(),
        maxzoom_override,
    );
    // TileJSON embeds the request origin in its tile URLs, so it is a derived
    // representation: validate by a strong ETag over the exact bytes served
    // (like rewritten style JSON), not the immutable archive's own identity.
    let body = serde_json::to_vec(&document).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("tilejson serialization failed: {error}"),
        )
    })?;
    let validators = Validators::for_derived_body(&body);
    debug!(endpoint = "tilejson", tileset_id = %tileset_id, "served external response");
    Ok(validators.origin_varying_json_response(&headers, body, cache::TILEJSON))
}

/// Converts PMTiles header and metadata into a TileJSON document.
fn tilejson(
    tileset_id: &TilesetId,
    base_url: &str,
    data: &TilesetInfo,
    requested_encoding: Option<&str>,
    maxzoom_override: Option<u8>,
) -> TileJson {
    let metadata = &data.metadata;
    let format = data.header.tile_type.tilejson_format().map(str::to_string);
    let wants_mlt = requested_encoding.is_some_and(|encoding| encoding.eq_ignore_ascii_case("mlt"))
        && data.header.tile_type == TileType::Mvt;
    // A producer-declared encoding (e.g. a terrain `terrarium`/`mapbox` hint, which
    // the PMTiles header cannot express) wins; otherwise fall back to the header's
    // vector encoding (`mvt`/`mlt`). We must not clobber the metadata value.
    let encoding = if wants_mlt {
        Some("mlt".to_string())
    } else {
        metadata.encoding().map(str::to_string).or_else(|| {
            data.header
                .tile_type
                .tilejson_encoding()
                .map(str::to_string)
        })
    };
    let tile_suffix = if wants_mlt { ".mlt" } else { "" };

    TileJson {
        tilejson: "3.0.0".to_string(),
        tiles: vec![format!(
            "{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}{tile_suffix}"
        )],
        vector_layers: metadata.vector_layers().to_vec(),
        attribution: metadata.attribution.clone(),
        bounds: Some([
            data.header.min_longitude,
            data.header.min_latitude,
            data.header.max_longitude,
            data.header.max_latitude,
        ]),
        center: Some((
            data.header.center_longitude,
            data.header.center_latitude,
            data.header.center_zoom,
        )),
        description: metadata.description.clone(),
        maxzoom: Some(maxzoom_override.unwrap_or(data.header.max_zoom)),
        minzoom: Some(data.header.min_zoom),
        name: metadata.name.clone().or(Some(tileset_id.to_string())),
        version: metadata.version.clone(),
        tilestats: metadata.tilestats().cloned(),
        format,
        encoding,
        other: metadata.other(),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::{BufMut, BytesMut};

    use super::tilejson;
    use crate::{
        interned::TilesetId,
        pmtiles::{Header, Metadata},
        storage::TilesetInfo,
    };

    fn header_with_tile_type(tile_type: u8) -> Header {
        let mut bytes = BytesMut::with_capacity(127);
        bytes.extend_from_slice(b"PMTiles");
        bytes.put_u8(3); // version
        for _ in 0..11 {
            bytes.put_u64_le(0);
        }
        bytes.put_u8(1); // clustered
        bytes.put_u8(1); // internal compression: none
        bytes.put_u8(2); // tile compression: gzip
        bytes.put_u8(tile_type);
        bytes.put_u8(0); // min zoom
        bytes.put_u8(14); // max zoom
        bytes.put_i32_le(-1800000000); // min lon
        bytes.put_i32_le(-850000000); // min lat
        bytes.put_i32_le(1800000000); // max lon
        bytes.put_i32_le(850000000); // max lat
        bytes.put_u8(0); // center zoom
        bytes.put_i32_le(0); // center lon
        bytes.put_i32_le(0); // center lat

        Header::parse(bytes.freeze()).expect("header parses")
    }

    fn info(tile_type: u8, metadata: Metadata) -> TilesetInfo {
        TilesetInfo {
            header: header_with_tile_type(tile_type),
            metadata: Arc::new(metadata),
        }
    }

    #[test]
    fn mlt_pmtiles_tilejson_declares_mlt_encoding() {
        let tileset_id = TilesetId::try_from("demo/mlt".to_string()).expect("valid tileset id");
        let document = tilejson(
            &tileset_id,
            "https://ishikari.example",
            &info(6, Metadata::default()),
            None,
            None,
        );

        assert_eq!(document.format.as_deref(), Some("pbf"));
        assert_eq!(document.encoding.as_deref(), Some("mlt"));
        assert_eq!(
            document.tiles,
            vec!["https://ishikari.example/tilesets/demo/mlt/{z}/{x}/{y}"]
        );
    }

    #[test]
    fn metadata_encoding_takes_precedence_over_header_encoding() {
        let tileset_id = TilesetId::try_from("demo/mlt".to_string()).expect("valid tileset id");
        let document = tilejson(
            &tileset_id,
            "https://ishikari.example",
            &info(
                6,
                Metadata {
                    encoding: Some("terrarium".to_string()),
                    ..Metadata::default()
                },
            ),
            None,
            None,
        );

        assert_eq!(document.encoding.as_deref(), Some("terrarium"));
    }

    #[test]
    fn requested_mlt_encoding_rewrites_mvt_tiles_to_mlt_urls() {
        let tileset_id = TilesetId::try_from("demo/mvt".to_string()).expect("valid tileset id");
        let document = tilejson(
            &tileset_id,
            "https://ishikari.example",
            &info(1, Metadata::default()),
            Some("mlt"),
            None,
        );

        assert_eq!(document.format.as_deref(), Some("pbf"));
        assert_eq!(document.encoding.as_deref(), Some("mlt"));
        assert_eq!(
            document.tiles,
            vec!["https://ishikari.example/tilesets/demo/mvt/{z}/{x}/{y}.mlt"]
        );
    }

    #[test]
    fn requested_mlt_encoding_is_ignored_for_raster_tilesets() {
        let tileset_id = TilesetId::try_from("demo/webp".to_string()).expect("valid tileset id");
        let document = tilejson(
            &tileset_id,
            "https://ishikari.example",
            &info(4, Metadata::default()),
            Some("mlt"),
            None,
        );

        assert_eq!(document.format.as_deref(), Some("webp"));
        assert_eq!(document.encoding.as_deref(), None);
        assert_eq!(
            document.tiles,
            vec!["https://ishikari.example/tilesets/demo/webp/{z}/{x}/{y}"]
        );
    }
}

//! TileJSON handler and response generation for tileset endpoints.

use std::collections::BTreeMap;

use axum::{
    Json,
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
};
use tracing::debug;

use crate::{
    interned::TilesetId,
    pmtiles::{Tilestats, VectorLayer},
    server::{AppState, HttpError, get_origin},
    storage::TilesetInfo,
};

use super::error::tileset_error_response;

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

/// Serves TileJSON derived from PMTiles header and metadata.
pub(crate) async fn tilejson_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<TileJson>, HttpError> {
    let tileset_id = TilesetId::try_from(tileset_id)
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let base_url = get_origin(&headers);
    let data = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    let document = tilejson(&tileset_id, &base_url, &data);
    debug!(endpoint = "tilejson", tileset_id = %tileset_id, "served external response");
    Ok(Json(document))
}

/// Converts PMTiles header and metadata into a TileJSON document.
fn tilejson(tileset_id: &TilesetId, base_url: &str, data: &TilesetInfo) -> TileJson {
    let metadata = &data.metadata;
    let format = data.header.tile_type.tilejson_format().map(str::to_string);
    let encoding = data
        .header
        .tile_type
        .tilejson_encoding()
        .map(str::to_string);

    TileJson {
        tilejson: "3.0.0".to_string(),
        tiles: vec![format!(
            "{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}"
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
        maxzoom: Some(data.header.max_zoom),
        minzoom: Some(data.header.min_zoom),
        name: metadata.name.clone().or(Some(tileset_id.to_string())),
        version: metadata.version.clone(),
        tilestats: metadata.tilestats().cloned(),
        format,
        encoding,
        other: metadata.other(),
    }
}

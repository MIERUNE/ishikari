//! Preview HTML and MapLibre style generation for a tileset.

use std::hash::Hasher;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::Html,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tracing::debug;
use twox_hash::XxHash64;

use crate::{
    interned_str::TilesetId,
    pmtiles::TileType,
    server::{AppState, HttpError, get_origin},
    tilesets::{TilesetInfo, validate_tileset_id},
};

use super::error::tileset_error_response;

const MAPLIBRE_GL_VERSION: &str = "latest";
const PREVIEW_HTML_TEMPLATE: &str = include_str!("preview.html");

#[derive(Deserialize)]
pub(crate) struct PreviewQuery {
    encoding: Option<String>,
}

#[derive(Clone, Copy)]
enum DemEncoding {
    Terrarium,
    TerrainRgb,
}

impl DemEncoding {
    fn from_str(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "terrarium" => Some(Self::Terrarium),
            "terrainrgb" => Some(Self::TerrainRgb),
            _ => None,
        }
    }

    fn maplibre_encoding(self) -> &'static str {
        match self {
            Self::Terrarium => "terrarium",
            Self::TerrainRgb => "mapbox",
        }
    }
}

/// Serves the lightweight HTML preview shell for a tileset.
pub(crate) async fn preview_handler(
    State(_state): State<AppState>,
    Path(tileset_id): Path<String>,
    Query(query): Query<PreviewQuery>,
) -> Result<Html<String>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    validate_tileset_id(tileset_id.as_ref())
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    let html = preview_html(&tileset_id, query.encoding.as_deref());
    debug!(
        endpoint = "preview",
        tileset_id = %tileset_id,
        served_bytes = html.len(),
        "served external response"
    );
    Ok(Html(html))
}

/// Serves the generated MapLibre style used by the preview page.
pub(crate) async fn preview_style_handler(
    State(state): State<AppState>,
    Path(tileset_id): Path<String>,
    headers: HeaderMap,
    Query(query): Query<PreviewQuery>,
) -> Result<Json<Value>, HttpError> {
    let tileset_id = TilesetId::from(tileset_id);
    let base_url = get_origin(&headers);
    let info = state
        .tileset_service
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(tileset_error_response)?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "not found".to_string()))?;
    let style = preview_style(&tileset_id, &base_url, &info, query.encoding.as_deref());
    debug!(
        endpoint = "preview_style",
        tileset_id = %tileset_id,
        "served external response"
    );
    Ok(Json(style))
}

fn preview_html(tileset_id: &TilesetId, encoding: Option<&str>) -> String {
    let style_url = match encoding {
        Some(enc) => format!("/tilesets/{tileset_id}/preview.json?encoding={enc}"),
        None => format!("/tilesets/{tileset_id}/preview.json"),
    };
    let terrain_control = if encoding.and_then(DemEncoding::from_str).is_some() {
        r#"map.addControl(new maplibregl.TerrainControl({ source: "dem", exaggeration: 1.0 }), "top-right");"#
    } else {
        ""
    };
    PREVIEW_HTML_TEMPLATE
        .replace("__TILESET_ID__", tileset_id)
        .replace("__STYLE_URL__", &style_url)
        .replace("__MAPLIBRE_GL_VERSION__", MAPLIBRE_GL_VERSION)
        .replace("__TERRAIN_CONTROL__", terrain_control)
}

fn preview_style(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: Option<&str>,
) -> Value {
    match info.header.tile_type {
        TileType::Png | TileType::Jpeg | TileType::Webp | TileType::Avif => {
            if let Some(dem) = encoding.and_then(DemEncoding::from_str) {
                preview_style_dem(tileset_id, base_url, info, dem)
            } else {
                preview_style_raster(tileset_id, base_url, info)
            }
        }
        _ => preview_style_vector(tileset_id, base_url, info),
    }
}

fn preview_style_dem(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &TilesetInfo,
    encoding: DemEncoding,
) -> Value {
    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "dem": {
                "type": "raster-dem",
                "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}")],
                "minzoom": info.header.min_zoom,
                "maxzoom": info.header.max_zoom,
                "tileSize": 256,
                "encoding": encoding.maplibre_encoding()
            }
        },
        "layers": [
            {
                "id": "background",
                "type": "background",
                "paint": { "background-color": "white" }
            },
            {
                "id": "hillshade",
                "type": "hillshade",
                "source": "dem",
                "paint": {
                    "hillshade-shadow-color": "#5a331f"
                }
            }
        ]
    })
}

fn preview_style_raster(tileset_id: &TilesetId, base_url: &str, info: &TilesetInfo) -> Value {
    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "preview": {
                "type": "raster",
                "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}")],
                "minzoom": info.header.min_zoom,
                "maxzoom": info.header.max_zoom,
                "tileSize": 256
            }
        },
        "layers": [
            {
                "id": "raster",
                "type": "raster",
                "source": "preview"
            }
        ]
    })
}

fn preview_style_vector(tileset_id: &TilesetId, base_url: &str, info: &TilesetInfo) -> Value {
    let vector_layers = info.metadata.vector_layers();
    let mut layers = vec![json!({
        "id": "background",
        "type": "background",
        "paint": { "background-color": "#777" }
    })];

    layers.reserve(vector_layers.len() * 5);

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_fill_color(layer_id);
        let hover_color = layer_hover_fill_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-fill"),
            "type": "fill",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "Polygon"],
            "paint": {
                "fill-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "fill-opacity": 0.62,
                "fill-outline-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    "rgba(0, 0, 0, 0)"
                ]
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        let hover_color = layer_hover_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-line"),
            "type": "line",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "LineString"],
            "paint": {
                "line-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "line-width": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    2,
                    1
                ]
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_circle_color(layer_id);
        let hover_color = layer_hover_circle_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-circle"),
            "type": "circle",
            "source": "preview",
            "source-layer": layer_id,
            "filter": ["==", ["geometry-type"], "Point"],
            "paint": {
                "circle-color": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    hover_color,
                    color
                ],
                "circle-radius": [
                    "case",
                    ["boolean", ["feature-state", "hover"], false],
                    5.5,
                    3.0
                ],
                "circle-opacity": 0.8,
                "circle-stroke-width": 0.0
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-label"),
            "type": "symbol",
            "source": "preview",
            "source-layer": layer_id,
            "filter": [
                "all",
                ["==", ["geometry-type"], "Point"],
                ["has", "name"]
            ],
            "layout": {
                "text-field": ["get", "name"],
                "text-size": 11,
                "text-offset": [0, 1.1],
                "text-anchor": "top"
            },
            "paint": {
                "text-color": color,
                "text-halo-color": "rgba(255,255,255,0.85)",
                "text-halo-width": 1.2
            }
        }));
    }

    for layer in vector_layers.iter().rev() {
        let layer_id = layer.id.as_str();
        let color = layer_color(layer_id);
        layers.push(json!({
            "id": format!("{layer_id}-line-label"),
            "type": "symbol",
            "source": "preview",
            "source-layer": layer_id,
            "filter": [
                "all",
                ["==", ["geometry-type"], "LineString"],
                ["has", "name"]
            ],
            "layout": {
                "symbol-placement": "line",
                "text-field": ["get", "name"],
                "text-size": 11
            },
            "paint": {
                "text-color": color,
                "text-halo-color": "rgba(255,255,255,0.82)",
                "text-halo-width": 1.2
            }
        }));
    }

    json!({
        "version": 8,
        "name": format!("preview - {tileset_id}"),
        "glyphs": "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
        "center": [info.header.center_longitude, info.header.center_latitude],
        "zoom": info.header.center_zoom,
        "sources": {
            "preview": {
                "type": "vector",
                "tiles": [format!("{base_url}/tilesets/{tileset_id}/{{z}}/{{x}}/{{y}}")],
                "minzoom": info.header.min_zoom,
                "maxzoom": info.header.max_zoom
            }
        },
        "layers": layers
    })
}

/// Assigns a stable hue to preview layers by name, with overrides for known categories.
fn layer_hue(layer_id: &str) -> f64 {
    if let Some(hue) = layer_hue_override(layer_id) {
        return hue;
    }

    let mut hasher = XxHash64::with_seed(0x2c4a68f3);
    hasher.write(layer_id.as_bytes());
    (hasher.finish() % 360) as f64
}

/// Uses a darker fill palette so polygons sit behind lines and points.
fn layer_fill_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.40, 0.24)
}

/// Uses a brighter variant of the fill palette for hovered polygons.
fn layer_hover_fill_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.50, 0.29)
}

/// Uses a brighter stroke palette for lines, points, and labels.
fn layer_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.56, 0.55)
}

/// Uses a brighter variant of the stroke palette for hovered lines.
fn layer_hover_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.74, 0.67)
}

/// Returns a higher-saturation, lower-lightness accent color for point features.
fn layer_circle_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.82, 0.48)
}

/// Returns a brighter point color for hovered point features.
fn layer_hover_circle_color(layer_id: &str) -> String {
    hsl(layer_hue(layer_id), 0.94, 0.64)
}

/// Overrides hue assignment for well-known layer names.
fn layer_hue_override(layer_id: &str) -> Option<f64> {
    match layer_id {
        "water" | "waterway" => Some(210.0),
        _ => None,
    }
}

/// Formats an HSL color string for the generated style.
fn hsl(hue: f64, saturation: f64, lightness: f64) -> String {
    format!(
        "hsl({hue:.0} {:.0}% {:.0}%)",
        saturation * 100.0,
        lightness * 100.0
    )
}

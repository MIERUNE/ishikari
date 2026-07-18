//! On-demand vector terrain products derived from Mapterhorn Terrarium tiles.
//!
//! Source tiles always enter through the normal composite resolver and
//! `ResourceResolver::route_tile`, so detail-archive selection, HRW ownership,
//! tile/chunk caches, object-store range batching, and negative caches are
//! shared with ordinary Mapterhorn serving.

pub(crate) use ishikari_terrain::dem;
use ishikari_terrain::{contours, hillshade};

use std::io::Write;

use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
};
use bytes::Bytes;
use flate2::{Compression as GzLevel, write::GzEncoder};
use serde::Deserialize;
use serde_json::json;
use tokio::task::JoinSet;
use tracing::{debug, warn};

use crate::{
    interned::TilesetId,
    pmtiles::{MLT_CONTENT_TYPE, TileCoord, TileData, TileId, TileType},
    server::{
        AppState, HttpError, bytes_response, cache, conditional::Validators, get_origin,
        provider::path_percent_encode,
    },
};

use super::{
    error::tileset_error_response,
    mlt::{RequestedTileFormat, mlt_response_bytes, negotiate_format, transcode_mlt},
    tile::{resolve_archive, tile_data_response},
};

pub(super) fn hillshade_opacity_stops(shadow: bool) -> Vec<(u8, f64)> {
    hillshade::opacity_stops(shadow)
}

/// Bytes-per-tone-code of the neutral shade raster, so the preview's
/// `color-relief` custom encoding recovers the signed code.
pub(super) fn hillshade_shade_code_scale() -> f64 {
    hillshade::SHADE_CODE_SCALE
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum DerivedProduct {
    Contours,
    Hillshade,
    /// Experimental: the hillshade shade field as a quantized WebP raster
    /// instead of vector polygons, for the raster-vs-vector size/quality
    /// Pareto comparison. Fixed palette/sun.
    HillshadeRaster,
    /// Experimental: continuous shade as lossy WebP (neutral grayscale, colored
    /// by a style-side color-relief ramp).
    HillshadeWebpLossy,
    /// Experimental: continuous (un-quantized) shade as lossy JPEG — the size
    /// floor for fixed-palette delivery, with no tone banding.
    HillshadeJpeg,
}

impl DerivedProduct {
    fn parse(value: &str) -> Result<Self, HttpError> {
        match value {
            "contours" => Ok(Self::Contours),
            "hillshade" => Ok(Self::Hillshade),
            "hillshade-raster" => Ok(Self::HillshadeRaster),
            "hillshade-webp-lossy" => Ok(Self::HillshadeWebpLossy),
            "hillshade-jpeg" => Ok(Self::HillshadeJpeg),
            _ => Err((StatusCode::NOT_FOUND, "derived product not found".into())),
        }
    }

    fn path(self) -> &'static str {
        match self {
            Self::Contours => "contours",
            Self::Hillshade => "hillshade",
            Self::HillshadeRaster => "hillshade-raster",
            Self::HillshadeWebpLossy => "hillshade-webp-lossy",
            Self::HillshadeJpeg => "hillshade-jpeg",
        }
    }

    fn is_raster(self) -> bool {
        matches!(
            self,
            Self::HillshadeRaster | Self::HillshadeWebpLossy | Self::HillshadeJpeg
        )
    }

    fn layer(self) -> &'static str {
        self.path()
    }
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub(crate) struct DerivedTileKey {
    tileset_id: TilesetId,
    product: DerivedProduct,
    tile_id: u64,
}

#[cfg(test)]
impl DerivedTileKey {
    pub(crate) fn for_test() -> Self {
        Self {
            tileset_id: TilesetId::new_unchecked("terrain"),
            product: DerivedProduct::Hillshade,
            tile_id: 0,
        }
    }
}

/// Cached result of a derived-tile generation. `Absent` records an
/// authoritative "no DEM here" so a no-data region is served as a cacheable
/// 404 without re-running the fetch/generate pipeline; it carries a short
/// negative TTL in the cache. Transient errors are never cached (they surface
/// as `Err` and moka's `try_get_with` does not store them).
#[derive(Clone)]
pub(crate) enum DerivedOutcome {
    Tile(TileData),
    /// Generated with an edge fallback because an in-world non-center DEM
    /// source was absent or hit a transient error. Served normally, but cached
    /// only for the short negative TTL so mutable source absence or a temporary
    /// failure cannot pin a seam into derived or downstream caches indefinitely.
    Degraded(TileData),
    Absent,
}

#[derive(Debug, Deserialize)]
pub(crate) struct DerivedTileJsonQuery {
    encoding: Option<String>,
}

struct DerivedTileRequest {
    tileset_id: TilesetId,
    product: DerivedProduct,
    z: u8,
    x: u32,
    y: u32,
    tile_id: u64,
    format: RequestedTileFormat,
}

pub(crate) async fn derived_tilejson_handler(
    State(state): State<AppState>,
    Path((tileset_id, product)): Path<(String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(state, tileset_id, product, headers, query).await
}

pub(crate) async fn namespaced_derived_tilejson_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product)): Path<(String, String, String)>,
    headers: HeaderMap,
    Query(query): Query<DerivedTileJsonQuery>,
) -> Result<Response, HttpError> {
    serve_tilejson(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        product,
        headers,
        query,
    )
    .await
}

async fn serve_tilejson(
    state: AppState,
    tileset_id: String,
    product: String,
    headers: HeaderMap,
    query: DerivedTileJsonQuery,
) -> Result<Response, HttpError> {
    let tileset_id = validated_mapterhorn(&state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let info = state
        .resource_resolver
        .load_tileset_info(tileset_id.clone())
        .await
        .map_err(|error| tileset_error_response(&error))?
        .ok_or_else(|| (StatusCode::NOT_FOUND, "tileset not found".to_string()))?;
    let wants_mlt = query
        .encoding
        .as_deref()
        .is_some_and(|encoding| encoding.eq_ignore_ascii_case("mlt"));
    let base_url = get_origin(&headers);
    let maxzoom = state
        .mapterhorn()
        .expect("validated_mapterhorn checked configuration")
        .maxzoom();
    let document =
        derived_tilejson_document(&tileset_id, &base_url, &info, product, wants_mlt, maxzoom);
    // Origin-derived like the base TileJSON: validate by a strong ETag over the
    // exact bytes served so conditional requests can 304.
    let body = serde_json::to_vec(&document).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("derived tilejson serialization failed: {error}"),
        )
    })?;
    let validators = Validators::for_derived_body(&body);
    Ok(validators.origin_varying_json_response(&headers, body, cache::TILEJSON))
}

/// Builds the derived-product TileJSON. Raster products serve WebP/JPEG bytes:
/// their TileJSON advertises the image format with plain tile URLs and no
/// vector metadata, or a generic client would try to decode image bytes as
/// vector tiles. Vector products advertise `pbf` with the negotiated encoding.
fn derived_tilejson_document(
    tileset_id: &TilesetId,
    base_url: &str,
    info: &crate::storage::TilesetInfo,
    product: DerivedProduct,
    wants_mlt: bool,
    maxzoom: u8,
) -> serde_json::Value {
    let suffix = if product.is_raster() {
        ""
    } else if wants_mlt {
        ".mlt"
    } else {
        ".mvt"
    };
    let mut document = json!({
        "tilejson": "3.0.0",
        "tiles": [format!(
            "{base_url}/tilesets/{tileset_id}/derived/{}/{{z}}/{{x}}/{{y}}{suffix}",
            product.path(),
        )],
        "attribution": info.metadata.attribution.clone(),
        "bounds": [
            info.header.min_longitude,
            info.header.min_latitude,
            info.header.max_longitude,
            info.header.max_latitude
        ],
        "center": [
            info.header.center_longitude,
            info.header.center_latitude,
            info.header.center_zoom
        ],
        "minzoom": info.header.min_zoom,
        "maxzoom": maxzoom,
        "name": format!("{tileset_id} {}", product.path()),
    });
    let extra = if product.is_raster() {
        json!({
            "format": if product == DerivedProduct::HillshadeJpeg { "jpg" } else { "webp" },
        })
    } else {
        json!({
            "vector_layers": [{
                "id": product.layer(),
                "fields": match product {
                    DerivedProduct::Contours => json!({ "ele": "Number", "level": "Number" }),
                    _ => json!({ "class": "String", "level": "Number" }),
                },
                "minzoom": info.header.min_zoom,
                "maxzoom": maxzoom
            }],
            "format": "pbf",
            "encoding": if wants_mlt { "mlt" } else { "mvt" }
        })
    };
    document
        .as_object_mut()
        .expect("document is an object")
        .extend(extra.as_object().expect("extra is an object").clone());
    document
}

pub(crate) async fn derived_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, product, z, x, y_raw)): Path<(String, String, u8, u32, String)>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    serve_derived_tile(state, tileset_id, product, z, x, y_raw, headers).await
}

pub(crate) async fn namespaced_derived_tile_handler(
    State(state): State<AppState>,
    Path((namespace, tileset_id, product, z, x, y_raw)): Path<(
        String,
        String,
        String,
        u8,
        u32,
        String,
    )>,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    serve_derived_tile(
        state,
        super::join_tileset_key(&namespace, &tileset_id),
        product,
        z,
        x,
        y_raw,
        headers,
    )
    .await
}

async fn serve_derived_tile(
    state: AppState,
    tileset_id: String,
    product: String,
    z: u8,
    x: u32,
    y_raw: String,
    headers: HeaderMap,
) -> Result<Response<Body>, HttpError> {
    let request = parse_derived_tile_request(&state, tileset_id, product, z, x, &y_raw, &headers)?;
    let routing_id = derived_resource_id(&request.tileset_id, request.product);
    let y_path = match request.format {
        RequestedTileFormat::AsStored => request.y.to_string(),
        RequestedTileFormat::Mlt => format!("{}.mlt", request.y),
    };
    let internal_path = format!(
        "/_internal/derived/{}/{}/{}/{}/{y_path}",
        path_percent_encode(request.tileset_id.as_ref()),
        request.product.path(),
        request.z,
        request.x,
    );
    let routed = match state
        .resource_resolver
        .route_derived_resource(&routing_id, request.tile_id, &internal_path)
        .await
    {
        Ok(Some(wire)) => match decode_derived_wire(wire, request.product, request.format) {
            Ok(outcome) => Some(outcome),
            Err(error) => {
                // A future/older incompatible peer must not break serving
                // during a rolling update. Generate locally as the fail-safe.
                warn!(
                    tileset_id = %request.tileset_id,
                    product = request.product.path(),
                    z = request.z,
                    x = request.x,
                    y = request.y,
                    error,
                    "invalid derived peer response; falling back local"
                );
                None
            }
        },
        Ok(None) => None,
        Err(error) => {
            warn!(
                tileset_id = %request.tileset_id,
                product = request.product.path(),
                z = request.z,
                x = request.x,
                y = request.y,
                error = %error,
                "derived peer routing failed; falling back local"
            );
            None
        }
    };
    let outcome = match routed {
        Some(outcome) => outcome,
        None => local_derived_output(&state, &request).await?,
    };

    let (generated, degraded) = match outcome {
        DerivedOutcome::Tile(tile) => (tile, false),
        DerivedOutcome::Degraded(tile) => (tile, true),
        DerivedOutcome::Absent => {
            return Ok(absent_derived_response(state.derived_negative_ttl()));
        }
    };
    state.metrics.add_egress_bytes(generated.bytes.len() as u64);
    let response = if degraded {
        degraded_derived_response(generated, state.derived_negative_ttl())
    } else {
        tile_data_response(generated)
    };
    debug!(
        endpoint = "derived_tile",
        tileset_id = %request.tileset_id,
        product = request.product.path(),
        z = request.z,
        x = request.x,
        y = request.y,
        "served generated terrain tile"
    );
    Ok(response)
}

/// Serves the owner-only internal derived endpoint. It never performs peer
/// routing, which prevents forwarding loops and makes this node the failover
/// generation target selected by the caller's HRW candidate walk.
pub(crate) async fn internal_derived_tile_handler(
    State(state): State<AppState>,
    Path((tileset_id, product, z, x, y_raw)): Path<(String, String, u8, u32, String)>,
) -> Result<Response, HttpError> {
    let request =
        parse_derived_tile_request(&state, tileset_id, product, z, x, &y_raw, &HeaderMap::new())?;
    let outcome = local_derived_output(&state, &request).await?;
    let wire = encode_derived_wire(&outcome).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("cannot encode derived peer response: {error}"),
        )
    })?;
    state.metrics.add_internal_bytes(wire.len() as u64);
    Ok(bytes_response(wire, "application/octet-stream", None))
}

async fn local_derived_output(
    state: &AppState,
    request: &DerivedTileRequest,
) -> Result<DerivedOutcome, HttpError> {
    let key = DerivedTileKey {
        tileset_id: request.tileset_id.clone(),
        product: request.product,
        tile_id: request.tile_id,
    };
    let outcome = state
        .derived_tile_cache()
        .try_get_with(
            key,
            generate_tile(
                state.clone(),
                request.tileset_id.clone(),
                request.product,
                request.z,
                request.x,
                request.y,
            ),
        )
        .await
        .map_err(|error| (*error).clone())?;
    let (generated, degraded) = match outcome {
        DerivedOutcome::Absent => return Ok(DerivedOutcome::Absent),
        DerivedOutcome::Tile(tile) => (tile, false),
        DerivedOutcome::Degraded(tile) => (tile, true),
    };
    // Degraded-ness survives representation changes: an MLT transcode of a
    // degraded MVT is still degraded.
    let wrap = if degraded {
        DerivedOutcome::Degraded
    } else {
        DerivedOutcome::Tile
    };
    match request.format {
        RequestedTileFormat::AsStored => Ok(wrap(generated)),
        RequestedTileFormat::Mlt => {
            let (bytes, content_encoding) = if degraded {
                // The generic MLT cache has no per-entry expiry. Transcode a
                // refreshable source without inserting it there, otherwise the
                // MLT seam would outlive the short-lived source MVT.
                uncached_derived_mlt_response_bytes(state, generated).await?
            } else {
                let cache_id = derived_resource_id(&request.tileset_id, request.product);
                let (bytes, content_encoding, _) =
                    mlt_response_bytes(state, &cache_id, request.tile_id, generated).await?;
                (bytes, content_encoding)
            };
            Ok(wrap(TileData {
                bytes,
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            }))
        }
    }
}

fn parse_derived_tile_request(
    state: &AppState,
    tileset_id: String,
    product: String,
    z: u8,
    x: u32,
    y_raw: &str,
    headers: &HeaderMap,
) -> Result<DerivedTileRequest, HttpError> {
    let tileset_id = validated_mapterhorn(state, tileset_id)?;
    let product = DerivedProduct::parse(&product)?;
    let (y, format) = negotiate_format(y_raw, headers);
    let y = y
        .parse::<u32>()
        .map_err(|_| (StatusCode::BAD_REQUEST, format!("invalid tile y: {y}")))?;
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    Ok(DerivedTileRequest {
        tileset_id,
        product,
        z,
        x,
        y,
        tile_id,
        format: normalized_format(product, format),
    })
}

fn normalized_format(product: DerivedProduct, format: RequestedTileFormat) -> RequestedTileFormat {
    if product.is_raster() {
        RequestedTileFormat::AsStored
    } else {
        format
    }
}

/// Internal namespace shared by HRW placement and the MLT cache. `:` cannot
/// occur in validated public ids, so this cannot collide with stored tilesets.
fn derived_resource_id(tileset_id: &TilesetId, product: DerivedProduct) -> TilesetId {
    TilesetId::new_unchecked(&format!("derived:{}:{tileset_id}", product.path()))
}

const DERIVED_WIRE_MAGIC_V1: &[u8; 8] = b"ISKRDRV1";
const DERIVED_WIRE_MAGIC_V2: &[u8; 8] = b"ISKRDRV2";
const DERIVED_WIRE_MAGIC_V3: &[u8; 8] = b"ISKRDRV3";
const DERIVED_WIRE_ABSENT: u8 = 0;
const DERIVED_WIRE_TILE: u8 = 1;
const DERIVED_WIRE_DEGRADED: u8 = 2;
const DERIVED_WIRE_CONTENT_MVT: u8 = 1;
const DERIVED_WIRE_CONTENT_MLT: u8 = 2;
const DERIVED_WIRE_CONTENT_PNG: u8 = 3;
const DERIVED_WIRE_CONTENT_JPEG: u8 = 4;
const DERIVED_WIRE_CONTENT_WEBP: u8 = 5;
const DERIVED_WIRE_CONTENT_AVIF: u8 = 6;
const DERIVED_WIRE_CONTENT_OCTET_STREAM: u8 = 7;
const DERIVED_WIRE_ENCODING_NONE: u8 = 0;
const DERIVED_WIRE_ENCODING_GZIP: u8 = 1;
const DERIVED_WIRE_ENCODING_BROTLI: u8 = 2;
const DERIVED_WIRE_ENCODING_ZSTD: u8 = 3;

fn encode_derived_wire(outcome: &DerivedOutcome) -> Result<Bytes, &'static str> {
    let wire = match outcome {
        // Keep clean/absent responses on v2 so older requesters can continue to
        // consume the common cases during a rolling update. Only the new status
        // needs v3; an older requester rejects it and safely generates locally.
        DerivedOutcome::Tile(tile) => {
            encode_derived_wire_tile(DERIVED_WIRE_MAGIC_V2, DERIVED_WIRE_TILE, tile)?
        }
        DerivedOutcome::Degraded(tile) => {
            encode_derived_wire_tile(DERIVED_WIRE_MAGIC_V3, DERIVED_WIRE_DEGRADED, tile)?
        }
        DerivedOutcome::Absent => {
            let mut wire = Vec::with_capacity(DERIVED_WIRE_MAGIC_V2.len() + 1);
            wire.extend_from_slice(DERIVED_WIRE_MAGIC_V2);
            wire.push(DERIVED_WIRE_ABSENT);
            wire
        }
    };
    Ok(Bytes::from(wire))
}

fn encode_derived_wire_tile(
    magic: &[u8; 8],
    status: u8,
    tile: &TileData,
) -> Result<Vec<u8>, &'static str> {
    let mut wire = Vec::with_capacity(magic.len() + 3 + tile.bytes.len());
    wire.extend_from_slice(magic);
    wire.push(status);
    wire.push(derived_content_type_code(tile.content_type)?);
    wire.push(derived_content_encoding_code(tile.content_encoding)?);
    wire.extend_from_slice(&tile.bytes);
    Ok(wire)
}

fn decode_derived_wire(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    if wire.len() < DERIVED_WIRE_MAGIC_V2.len() + 1 {
        return Err("invalid derived wire magic");
    }
    let magic = &wire[..DERIVED_WIRE_MAGIC_V2.len()];
    if magic == DERIVED_WIRE_MAGIC_V3 {
        return decode_derived_wire_v3(wire, product, format);
    }
    if magic == DERIVED_WIRE_MAGIC_V2 {
        return decode_derived_wire_v2(wire, product, format);
    }
    if magic == DERIVED_WIRE_MAGIC_V1 {
        return decode_derived_wire_v1(wire, product, format);
    }
    Err("invalid derived wire magic")
}

fn decode_derived_wire_v3(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    let status_offset = DERIVED_WIRE_MAGIC_V3.len();
    match wire[status_offset] {
        DERIVED_WIRE_ABSENT if wire.len() == status_offset + 1 => Ok(DerivedOutcome::Absent),
        DERIVED_WIRE_ABSENT => Err("absent derived wire response has a payload"),
        status @ (DERIVED_WIRE_TILE | DERIVED_WIRE_DEGRADED) if wire.len() >= status_offset + 3 => {
            let tile = TileData {
                bytes: wire.slice(status_offset + 3..),
                content_type: derived_content_type(wire[status_offset + 1])?,
                content_encoding: derived_content_encoding(wire[status_offset + 2])?,
            };
            let tile = validate_derived_tile_data(product, format, tile)?;
            Ok(if status == DERIVED_WIRE_DEGRADED {
                DerivedOutcome::Degraded(tile)
            } else {
                DerivedOutcome::Tile(tile)
            })
        }
        DERIVED_WIRE_TILE | DERIVED_WIRE_DEGRADED => Err("derived tile wire response is truncated"),
        _ => Err("invalid derived wire status"),
    }
}

fn decode_derived_wire_v2(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    let status_offset = DERIVED_WIRE_MAGIC_V2.len();
    match wire[status_offset] {
        DERIVED_WIRE_ABSENT if wire.len() == status_offset + 1 => Ok(DerivedOutcome::Absent),
        DERIVED_WIRE_ABSENT => Err("absent derived wire response has a payload"),
        DERIVED_WIRE_TILE if wire.len() >= status_offset + 3 => {
            let tile = TileData {
                bytes: wire.slice(status_offset + 3..),
                content_type: derived_content_type(wire[status_offset + 1])?,
                content_encoding: derived_content_encoding(wire[status_offset + 2])?,
            };
            validate_derived_tile_data(product, format, tile).map(DerivedOutcome::Tile)
        }
        DERIVED_WIRE_TILE => Err("derived tile wire response is truncated"),
        _ => Err("invalid derived wire status"),
    }
}

/// Accepts v1 responses during rolling upgrades. New peers always emit v2,
/// which carries content metadata instead of reconstructing it implicitly.
fn decode_derived_wire_v1(
    wire: Bytes,
    product: DerivedProduct,
    format: RequestedTileFormat,
) -> Result<DerivedOutcome, &'static str> {
    if &wire[..DERIVED_WIRE_MAGIC_V1.len()] != DERIVED_WIRE_MAGIC_V1 {
        return Err("invalid derived wire magic");
    }
    let payload = wire.slice(DERIVED_WIRE_MAGIC_V1.len() + 1..);
    match wire[DERIVED_WIRE_MAGIC_V1.len()] {
        DERIVED_WIRE_ABSENT if payload.is_empty() => Ok(DerivedOutcome::Absent),
        DERIVED_WIRE_ABSENT => Err("absent derived wire response has a payload"),
        DERIVED_WIRE_TILE => Ok(DerivedOutcome::Tile(legacy_derived_tile_data(
            product, format, payload,
        ))),
        _ => Err("invalid derived wire status"),
    }
}

fn legacy_derived_tile_data(
    product: DerivedProduct,
    format: RequestedTileFormat,
    bytes: Bytes,
) -> TileData {
    match format {
        RequestedTileFormat::Mlt => TileData {
            bytes,
            content_type: MLT_CONTENT_TYPE,
            content_encoding: Some("gzip"),
        },
        RequestedTileFormat::AsStored if product.is_raster() => TileData {
            bytes,
            content_type: match product {
                DerivedProduct::HillshadeJpeg => TileType::Jpeg.content_type(),
                _ => TileType::Webp.content_type(),
            },
            content_encoding: None,
        },
        RequestedTileFormat::AsStored => TileData {
            bytes,
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        },
    }
}

fn validate_derived_tile_data(
    product: DerivedProduct,
    format: RequestedTileFormat,
    tile: TileData,
) -> Result<TileData, &'static str> {
    let expected_content_type = match format {
        RequestedTileFormat::Mlt => MLT_CONTENT_TYPE,
        RequestedTileFormat::AsStored if product == DerivedProduct::HillshadeJpeg => {
            TileType::Jpeg.content_type()
        }
        RequestedTileFormat::AsStored if product.is_raster() => TileType::Webp.content_type(),
        RequestedTileFormat::AsStored => TileType::Mvt.content_type(),
    };
    if tile.content_type != expected_content_type {
        return Err("derived wire content type does not match request");
    }
    // Encoding is transport metadata carried authoritatively by wire v2, not a
    // property of the requested representation. Native MLT may legitimately be
    // uncompressed, gzip, Brotli, or Zstandard; the wire decoder already rejects
    // every encoding outside that allowlist.
    Ok(tile)
}

fn derived_content_type_code(content_type: &str) -> Result<u8, &'static str> {
    match content_type {
        value if value == TileType::Mvt.content_type() => Ok(DERIVED_WIRE_CONTENT_MVT),
        MLT_CONTENT_TYPE => Ok(DERIVED_WIRE_CONTENT_MLT),
        value if value == TileType::Png.content_type() => Ok(DERIVED_WIRE_CONTENT_PNG),
        value if value == TileType::Jpeg.content_type() => Ok(DERIVED_WIRE_CONTENT_JPEG),
        value if value == TileType::Webp.content_type() => Ok(DERIVED_WIRE_CONTENT_WEBP),
        value if value == TileType::Avif.content_type() => Ok(DERIVED_WIRE_CONTENT_AVIF),
        value if value == TileType::Unknown.content_type() => Ok(DERIVED_WIRE_CONTENT_OCTET_STREAM),
        _ => Err("unsupported derived wire content type"),
    }
}

fn derived_content_type(code: u8) -> Result<&'static str, &'static str> {
    match code {
        DERIVED_WIRE_CONTENT_MVT => Ok(TileType::Mvt.content_type()),
        DERIVED_WIRE_CONTENT_MLT => Ok(MLT_CONTENT_TYPE),
        DERIVED_WIRE_CONTENT_PNG => Ok(TileType::Png.content_type()),
        DERIVED_WIRE_CONTENT_JPEG => Ok(TileType::Jpeg.content_type()),
        DERIVED_WIRE_CONTENT_WEBP => Ok(TileType::Webp.content_type()),
        DERIVED_WIRE_CONTENT_AVIF => Ok(TileType::Avif.content_type()),
        DERIVED_WIRE_CONTENT_OCTET_STREAM => Ok(TileType::Unknown.content_type()),
        _ => Err("unsupported derived wire content type"),
    }
}

fn derived_content_encoding_code(encoding: Option<&str>) -> Result<u8, &'static str> {
    match encoding {
        None => Ok(DERIVED_WIRE_ENCODING_NONE),
        Some("gzip") => Ok(DERIVED_WIRE_ENCODING_GZIP),
        Some("br") => Ok(DERIVED_WIRE_ENCODING_BROTLI),
        Some("zstd") => Ok(DERIVED_WIRE_ENCODING_ZSTD),
        Some(_) => Err("unsupported derived wire content encoding"),
    }
}

fn derived_content_encoding(code: u8) -> Result<Option<&'static str>, &'static str> {
    match code {
        DERIVED_WIRE_ENCODING_NONE => Ok(None),
        DERIVED_WIRE_ENCODING_GZIP => Ok(Some("gzip")),
        DERIVED_WIRE_ENCODING_BROTLI => Ok(Some("br")),
        DERIVED_WIRE_ENCODING_ZSTD => Ok(Some("zstd")),
        _ => Err("unsupported derived wire content encoding"),
    }
}

fn validated_mapterhorn(state: &AppState, value: String) -> Result<TilesetId, HttpError> {
    let tileset_id =
        TilesetId::try_from(value).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    match state.mapterhorn() {
        Some(resolver) if resolver.matches(&tileset_id) => Ok(tileset_id),
        _ => Err((
            StatusCode::NOT_FOUND,
            "derived terrain products require the configured Mapterhorn tileset".into(),
        )),
    }
}

async fn generate_tile(
    state: AppState,
    tileset_id: TilesetId,
    product: DerivedProduct,
    z: u8,
    x: u32,
    y: u32,
) -> Result<DerivedOutcome, HttpError> {
    // Admitted *before* the neighborhood fetch: from here to the end of
    // generation this pipeline may retain up to nine decoded DEM tiles, so the
    // number of such pipelines — not just running CPU work — stays bounded.
    // Requests beyond the shed ceiling get 503 instead of queueing memory.
    let pipeline_permit = state.admit_terrain_pipeline().await?;
    let fetch_started = std::time::Instant::now();
    let (tiles, degraded) = fetch_neighborhood(&state, tileset_id.clone(), z, x, y).await?;
    let fetch_elapsed = fetch_started.elapsed();

    // An absent center DEM authoritatively means there is no terrain for this
    // lookup. Return the short-lived `Absent` result instead of regenerating the
    // 3x3 fetch on every request; source-negative expiry permits later healing.
    if tiles[CENTER_INDEX].is_none() {
        return Ok(DerivedOutcome::Absent);
    }
    let present_sources = tiles.iter().filter(|tile| tile.is_some()).count() as u32;

    // Acquire CPU execution only around generation — never across source I/O.
    // The parent pipeline already passed bounded admission, so this child stage
    // waits for shared concurrency without reserving another in-flight slot.
    let generation_permit = state.acquire_admitted_cpu_work("terrain_generate").await?;
    let metrics = state.metrics.clone();
    tokio::task::spawn_blocking(move || {
        // Keep the permits inside the blocking task. Dropping the HTTP future
        // cannot cancel spawn_blocking, so releasing them earlier would let
        // disconnected clients exceed the configured CPU concurrency (and the
        // pipeline's retained-neighborhood bound).
        let _generation_permit = generation_permit;
        let _pipeline_permit = pipeline_permit;
        let cpu_started = std::time::Instant::now();
        let neighborhood = dem::DemNeighborhood::from_tiles(tiles).map_err(|error| {
            (
                StatusCode::BAD_GATEWAY,
                format!("assemble Mapterhorn DEM: {error:#}"),
            )
        })?;
        let payload = match product {
            DerivedProduct::Contours => contours::generate(&neighborhood, z),
            DerivedProduct::Hillshade => hillshade::generate(&neighborhood, z, y),
            DerivedProduct::HillshadeRaster => hillshade::generate_raster(&neighborhood, z, y),
            DerivedProduct::HillshadeWebpLossy => {
                hillshade::generate_raster_webp_lossy(&neighborhood, z, y, 80)
            }
            DerivedProduct::HillshadeJpeg => {
                hillshade::generate_raster_jpeg(&neighborhood, z, y, 85)
            }
        }
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("generate {}: {error:#}", product.path()),
            )
        })?;
        // Vector products gzip well and declare it; the raster WebP is already
        // compressed, so it is served as-is with its image content type.
        let (bytes, content_type, content_encoding) = if product.is_raster() {
            let content_type = match product {
                DerivedProduct::HillshadeJpeg => TileType::Jpeg.content_type(),
                _ => TileType::Webp.content_type(),
            };
            (Bytes::from(payload.clone()), content_type, None)
        } else {
            (
                Bytes::from(gzip(&payload)?),
                TileType::Mvt.content_type(),
                Some("gzip"),
            )
        };
        let generate_elapsed = cpu_started.elapsed();
        metrics.record_terrain_generation(
            product.path(),
            fetch_elapsed,
            generate_elapsed,
            present_sources as usize,
            bytes.len(),
        );
        // Splits the cold-tile cost so slow serving is attributable: source
        // acquisition (fetch + WebP decode, single-flighted per source) vs
        // local product generation CPU.
        debug!(
            tileset_id = %tileset_id,
            product = product.path(),
            z,
            x,
            y,
            source_ms = fetch_elapsed.as_millis() as u64,
            present_sources,
            generate_ms = generate_elapsed.as_millis() as u64,
            payload_bytes = payload.len(),
            degraded,
            "generated terrain tile"
        );
        let tile = TileData {
            bytes,
            content_type,
            content_encoding,
        };
        // A tile built with an edge fallback after a transient neighbor error
        // or mutable in-world absence is served, but cached briefly so the seam
        // heals on regeneration instead of persisting until eviction.
        Ok(if degraded {
            DerivedOutcome::Degraded(tile)
        } else {
            DerivedOutcome::Tile(tile)
        })
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("terrain generation task failed: {error}"),
        )
    })?
}

/// Row-major index of the center tile within the 3x3 neighborhood.
const CENTER_INDEX: usize = 4;

/// Fetches and decodes the 3x3 DEM neighborhood around a tile, returning each
/// decoded source (or `None` where a source is absent). Every source is loaded
/// through [`load_decoded_dem`], which single-flights the fetch + WebP decode
/// per source tile across concurrent derived requests (sibling products and
/// adjacent derived tiles share six of nine sources).
///
/// Only the center is required. A missing in-world *non-center* source — absent
/// or a transient fetch error — degrades to an edge fallback rather than failing
/// the whole tile. It also makes the derived result refreshable because both
/// source-negative caches and transient failures can change. A structural
/// neighbor beyond the world's Y range is clean and cannot later appear. A
/// transient error on the *center* propagates as `Err`; center absence becomes
/// `DerivedOutcome::Absent` in the caller.
async fn fetch_neighborhood(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<([Option<std::sync::Arc<dem::DemTile>>; 9], bool), HttpError> {
    let world = 1_i64 << z;
    let mut tasks = JoinSet::new();
    let mut degraded = false;
    let mut tiles: [Option<std::sync::Arc<dem::DemTile>>; 9] = std::array::from_fn(|_| None);
    for dy in -1_i64..=1 {
        for dx in -1_i64..=1 {
            let Some((index, neighbor_x, neighbor_y)) =
                neighborhood_coordinate(world, x, y, dx, dy)
            else {
                // Structural absence beyond the world's Y range is not
                // refreshable and therefore does not degrade the result.
                continue;
            };
            let state = state.clone();
            let tileset_id = tileset_id.clone();
            tasks.spawn(async move {
                let result = load_decoded_dem(&state, tileset_id, z, neighbor_x, neighbor_y).await;
                (index, result)
            });
        }
    }

    while let Some(task) = tasks.join_next().await {
        let (index, result) = task.map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("DEM fetch task failed: {error}"),
            )
        })?;
        match result {
            Ok(tile) => {
                if tile.is_none() && in_world_absence_is_refreshable(index) {
                    degraded = true;
                }
                tiles[index] = tile;
            }
            Err(error) => {
                if index != CENTER_INDEX {
                    debug!(
                        z,
                        x,
                        y,
                        index,
                        error = %error.1,
                        "neighbor DEM source failed; using edge fallback"
                    );
                    degraded = true;
                }
                tiles[index] = tolerate_neighbor_failure(index, error)?;
            }
        }
    }
    Ok((tiles, degraded))
}

fn neighborhood_coordinate(
    world: i64,
    x: u32,
    y: u32,
    dx: i64,
    dy: i64,
) -> Option<(usize, u32, u32)> {
    let neighbor_y = i64::from(y) + dy;
    if !(0..world).contains(&neighbor_y) {
        return None;
    }
    let index = ((dy + 1) * 3 + dx + 1) as usize;
    let neighbor_x = (i64::from(x) + dx).rem_euclid(world) as u32;
    Some((index, neighbor_x, neighbor_y as u32))
}

fn in_world_absence_is_refreshable(index: usize) -> bool {
    index != CENTER_INDEX
}

fn tolerate_neighbor_failure<T>(index: usize, error: HttpError) -> Result<Option<T>, HttpError> {
    if index == CENTER_INDEX {
        Err(error)
    } else {
        Ok(None)
    }
}

/// Loads and decodes a single source DEM tile, single-flighting the fetch +
/// WebP decode per source tile id so concurrent derived requests sharing a
/// source only do it once. Absent sources are cached as `None` (bounded by the
/// DEM cache's negative TTL); transient errors are not cached. Decode is child
/// work of an admitted terrain pipeline, so it queues for CPU concurrency
/// without independently shedding against the top-level in-flight ceiling.
async fn load_decoded_dem(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<std::sync::Arc<dem::DemTile>>, HttpError> {
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let cache = state.dem_tile_cache().clone();
    let state = state.clone();
    cache
        .try_get_with((tileset_id.clone(), tile_id), async move {
            let Some(raw) = fetch_source_tile(&state, tileset_id, z, x, y).await? else {
                return Ok::<Option<std::sync::Arc<dem::DemTile>>, HttpError>(None);
            };
            if raw.content_encoding.is_some() {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    format!(
                        "compressed Mapterhorn image payload is not supported: {:?}",
                        raw.content_encoding
                    ),
                ));
            }
            // Fetch first, then acquire child CPU execution only for WebP
            // decoding. The admitted parent bounds this work while the shared
            // semaphore serializes it at low concurrency without self-shedding.
            let decode_permit = state.acquire_admitted_cpu_work("dem_decode").await?;
            let decoded = tokio::task::spawn_blocking(move || {
                let _decode_permit = decode_permit;
                dem::decode_terrarium(raw.bytes.as_ref())
            })
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("DEM decode task failed: {error}"),
                )
            })?
            .map_err(|error| {
                (
                    StatusCode::BAD_GATEWAY,
                    format!("decode Mapterhorn DEM: {error:#}"),
                )
            })?;
            Ok(Some(std::sync::Arc::new(decoded)))
        })
        .await
        .map_err(|error: std::sync::Arc<HttpError>| (*error).clone())
}

async fn fetch_source_tile(
    state: &AppState,
    tileset_id: TilesetId,
    z: u8,
    x: u32,
    y: u32,
) -> Result<Option<TileData>, HttpError> {
    let Some(archive) = resolve_archive(state, tileset_id, z, x, y).await? else {
        return Ok(None);
    };
    let tile_id = TileId::from(
        TileCoord::new(z, x, y).map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?,
    )
    .value();
    let (tile, source) = state
        .resource_resolver
        .route_tile(archive, tile_id)
        .await
        .map_err(|error| tileset_error_response(&error))?;
    for outcome in state.resource_resolver.cache_outcomes(source) {
        state.metrics.record_tile_cache(outcome);
    }
    Ok(tile)
}

fn short_derived_cache_control(negative_ttl: std::time::Duration) -> Option<HeaderValue> {
    HeaderValue::from_str(&format!(
        "public, max-age={}, s-maxage={}",
        negative_ttl.as_secs(),
        negative_ttl.as_secs()
    ))
    .ok()
}

fn degraded_derived_response(tile: TileData, negative_ttl: std::time::Duration) -> Response {
    let mut response = tile_data_response(tile);
    if let Some(value) = short_derived_cache_control(negative_ttl) {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

/// Builds a cacheable `404` for a derived tile whose center DEM is absent. The
/// short browser and shared-cache lifetime lets repeated no-data requests be
/// absorbed while still surfacing a later-provisioned detail archive.
fn absent_derived_response(negative_ttl: std::time::Duration) -> Response {
    let mut response = (StatusCode::NOT_FOUND, "derived tile not available\n").into_response();
    if let Some(value) = short_derived_cache_control(negative_ttl) {
        response.headers_mut().insert(header::CACHE_CONTROL, value);
    }
    response
}

async fn uncached_derived_mlt_response_bytes(
    state: &AppState,
    tile: TileData,
) -> Result<(Bytes, Option<&'static str>), HttpError> {
    if tile.content_type != TileType::Mvt.content_type() {
        return Err((
            StatusCode::NOT_ACCEPTABLE,
            format!("cannot serve {} tile as MLT", tile.content_type),
        ));
    }
    let permit = state.admit_transcode_work().await?;
    let bytes = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        transcode_mlt(tile)
    })
    .await
    .map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("MLT transcode task failed: {error}"),
        )
    })??;
    Ok((bytes, Some("gzip")))
}

fn gzip(data: &[u8]) -> Result<Vec<u8>, HttpError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
    encoder.write_all(data).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("gzip generated tile: {error}"),
        )
    })?;
    encoder.finish().map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("gzip generated tile: {error}"),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::header;

    #[test]
    fn product_names_are_explicit() {
        assert_eq!(
            DerivedProduct::parse("contours").unwrap().path(),
            "contours"
        );
        assert_eq!(
            DerivedProduct::parse("hillshade").unwrap().path(),
            "hillshade"
        );
        assert!(DerivedProduct::parse("terrain").is_err());
    }

    #[test]
    fn derived_resource_ids_separate_products() {
        let source = TilesetId::new_unchecked("mapterhorn/planet");
        assert_ne!(
            derived_resource_id(&source, DerivedProduct::Contours),
            derived_resource_id(&source, DerivedProduct::Hillshade)
        );
    }

    #[test]
    fn derived_wire_round_trips_tile_metadata_and_absence() {
        let source = DerivedOutcome::Tile(TileData {
            bytes: Bytes::from_static(b"compressed tile"),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        });
        let decoded = decode_derived_wire(
            encode_derived_wire(&source).unwrap(),
            DerivedProduct::Hillshade,
            RequestedTileFormat::AsStored,
        )
        .unwrap();
        let DerivedOutcome::Tile(decoded) = decoded else {
            panic!("expected tile")
        };
        assert_eq!(decoded.bytes, Bytes::from_static(b"compressed tile"));
        assert_eq!(decoded.content_type, TileType::Mvt.content_type());
        assert_eq!(decoded.content_encoding, Some("gzip"));

        assert!(matches!(
            decode_derived_wire(
                encode_derived_wire(&DerivedOutcome::Absent).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .unwrap(),
            DerivedOutcome::Absent
        ));
    }

    #[test]
    fn legacy_derived_wire_reconstructs_requested_representation() {
        let payload = Bytes::from_static(b"payload");
        let mut legacy_wire = DERIVED_WIRE_MAGIC_V1.to_vec();
        legacy_wire.push(DERIVED_WIRE_TILE);
        legacy_wire.extend_from_slice(&payload);
        let decoded = decode_derived_wire(
            Bytes::from(legacy_wire),
            DerivedProduct::Hillshade,
            RequestedTileFormat::Mlt,
        )
        .unwrap();
        let DerivedOutcome::Tile(decoded) = decoded else {
            panic!("expected legacy tile")
        };
        assert_eq!(decoded.bytes, payload);
        assert_eq!(decoded.content_type, MLT_CONTENT_TYPE);
        assert_eq!(decoded.content_encoding, Some("gzip"));

        let mlt = legacy_derived_tile_data(
            DerivedProduct::Hillshade,
            RequestedTileFormat::Mlt,
            payload.clone(),
        );
        assert_eq!(mlt.content_type, MLT_CONTENT_TYPE);
        assert_eq!(mlt.content_encoding, Some("gzip"));

        let raster = legacy_derived_tile_data(
            DerivedProduct::HillshadeRaster,
            RequestedTileFormat::AsStored,
            payload.clone(),
        );
        assert_eq!(raster.content_type, TileType::Webp.content_type());
        assert_eq!(raster.content_encoding, None);

        let jpeg = legacy_derived_tile_data(
            DerivedProduct::HillshadeJpeg,
            RequestedTileFormat::AsStored,
            payload,
        );
        assert_eq!(jpeg.content_type, TileType::Jpeg.content_type());
        assert_eq!(jpeg.content_encoding, None);
    }

    #[test]
    fn derived_wire_rejects_incompatible_or_malformed_responses() {
        assert!(
            decode_derived_wire(
                Bytes::from_static(b"old peer response"),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .is_err()
        );

        let mut malformed_absent = encode_derived_wire(&DerivedOutcome::Absent)
            .unwrap()
            .to_vec();
        malformed_absent.push(1);
        assert!(
            decode_derived_wire(
                Bytes::from(malformed_absent),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            )
            .is_err()
        );

        let incompatible = DerivedOutcome::Tile(TileData {
            bytes: Bytes::from_static(b"webp"),
            content_type: TileType::Webp.content_type(),
            content_encoding: None,
        });
        assert!(matches!(
            decode_derived_wire(
                encode_derived_wire(&incompatible).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::AsStored,
            ),
            Err("derived wire content type does not match request")
        ));
    }

    #[test]
    fn derived_wire_preserves_native_mlt_encoding() {
        for content_encoding in [None, Some("br"), Some("zstd")] {
            let source = DerivedOutcome::Tile(TileData {
                bytes: Bytes::from_static(b"native mlt"),
                content_type: MLT_CONTENT_TYPE,
                content_encoding,
            });
            let decoded = decode_derived_wire(
                encode_derived_wire(&source).unwrap(),
                DerivedProduct::Hillshade,
                RequestedTileFormat::Mlt,
            )
            .unwrap();
            let DerivedOutcome::Tile(decoded) = decoded else {
                panic!("expected tile")
            };
            assert_eq!(decoded.content_type, MLT_CONTENT_TYPE);
            assert_eq!(decoded.content_encoding, content_encoding);
        }
    }

    #[test]
    fn refreshable_responses_have_short_browser_and_shared_cache_lifetimes() {
        let absent = absent_derived_response(std::time::Duration::from_secs(60));
        assert_eq!(absent.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            absent.headers().get(header::CACHE_CONTROL).unwrap(),
            "public, max-age=60, s-maxage=60"
        );

        let degraded = degraded_derived_response(
            TileData {
                bytes: Bytes::from_static(b"tile"),
                content_type: TileType::Mvt.content_type(),
                content_encoding: Some("gzip"),
            },
            std::time::Duration::from_secs(45),
        );
        let policy = degraded
            .headers()
            .get(header::CACHE_CONTROL)
            .unwrap()
            .to_str()
            .unwrap();
        assert_eq!(policy, "public, max-age=45, s-maxage=45");
        assert!(!policy.contains("stale"));
    }

    #[test]
    fn raster_tilejson_advertises_the_image_format_not_vectors() {
        let info = tilejson_test_info();
        let tileset_id = TilesetId::new_unchecked("mapterhorn/planet");

        for (product, format) in [
            (DerivedProduct::HillshadeRaster, "webp"),
            (DerivedProduct::HillshadeWebpLossy, "webp"),
            (DerivedProduct::HillshadeJpeg, "jpg"),
        ] {
            // `wants_mlt: true` must be ignored for image products.
            let document = derived_tilejson_document(
                &tileset_id,
                "https://ishikari.example",
                &info,
                product,
                true,
                13,
            );
            assert_eq!(document["format"], format, "{}", product.path());
            assert!(
                document.get("vector_layers").is_none(),
                "{}",
                product.path()
            );
            assert!(document.get("encoding").is_none(), "{}", product.path());
            let tiles = document["tiles"][0].as_str().unwrap();
            assert!(tiles.ends_with("/{z}/{x}/{y}"), "{tiles}");
        }

        let vector = derived_tilejson_document(
            &tileset_id,
            "https://ishikari.example",
            &info,
            DerivedProduct::Contours,
            false,
            13,
        );
        assert_eq!(vector["format"], "pbf");
        assert_eq!(vector["encoding"], "mvt");
        assert!(vector["vector_layers"].is_array());
        assert!(vector["tiles"][0].as_str().unwrap().ends_with(".mvt"));
    }

    fn tilejson_test_info() -> crate::storage::TilesetInfo {
        use bytes::BufMut;
        let mut bytes = bytes::BytesMut::with_capacity(127);
        bytes.extend_from_slice(b"PMTiles");
        bytes.put_u8(3);
        for _ in 0..11 {
            bytes.put_u64_le(0);
        }
        bytes.put_u8(1); // clustered
        bytes.put_u8(1); // internal compression: none
        bytes.put_u8(2); // tile compression: gzip
        bytes.put_u8(1); // tile type: mvt
        bytes.put_u8(0); // min zoom
        bytes.put_u8(12); // max zoom
        bytes.put_i32_le(-1800000000);
        bytes.put_i32_le(-850000000);
        bytes.put_i32_le(1800000000);
        bytes.put_i32_le(850000000);
        bytes.put_u8(0);
        bytes.put_i32_le(0);
        bytes.put_i32_le(0);
        crate::storage::TilesetInfo {
            header: crate::pmtiles::Header::parse(bytes.freeze()).expect("header parses"),
            metadata: std::sync::Arc::new(crate::pmtiles::Metadata::default()),
        }
    }

    #[test]
    fn degraded_tiles_preserve_refreshability_over_the_v3_wire() {
        let degraded = DerivedOutcome::Degraded(TileData {
            bytes: Bytes::from_static(b"seamed"),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        });
        let decoded = decode_derived_wire(
            encode_derived_wire(&degraded).unwrap(),
            DerivedProduct::Hillshade,
            RequestedTileFormat::AsStored,
        )
        .unwrap();
        let DerivedOutcome::Degraded(tile) = decoded else {
            panic!("degraded wire payload must preserve refreshability");
        };
        assert_eq!(tile.bytes, Bytes::from_static(b"seamed"));
        assert!(
            encode_derived_wire(&degraded)
                .unwrap()
                .starts_with(DERIVED_WIRE_MAGIC_V3)
        );

        let clean = DerivedOutcome::Tile(TileData {
            bytes: Bytes::from_static(b"clean"),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        });
        assert!(
            encode_derived_wire(&clean)
                .unwrap()
                .starts_with(DERIVED_WIRE_MAGIC_V2)
        );
    }

    #[test]
    fn only_center_source_errors_abort_generation() {
        let error = (StatusCode::BAD_GATEWAY, "source failed".to_string());

        assert_eq!(
            tolerate_neighbor_failure::<()>(0, error.clone()).unwrap(),
            None
        );
        assert_eq!(
            tolerate_neighbor_failure::<()>(CENTER_INDEX, error.clone()).unwrap_err(),
            error
        );
    }

    #[test]
    fn only_in_world_non_center_absence_is_refreshable() {
        assert!(in_world_absence_is_refreshable(0));
        assert!(!in_world_absence_is_refreshable(CENTER_INDEX));

        let world = 4;
        assert!(
            neighborhood_coordinate(world, 0, 0, 0, -1).is_none(),
            "neighbor beyond the north edge is structural"
        );
        let (index, wrapped_x, neighbor_y) =
            neighborhood_coordinate(world, 0, 1, -1, 0).expect("in-world neighbor");
        assert_eq!((index, wrapped_x, neighbor_y), (3, 3, 1));
        assert!(in_world_absence_is_refreshable(index));
    }
}

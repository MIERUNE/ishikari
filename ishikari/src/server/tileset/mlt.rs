//! MLT (MapLibre Tile) serving: request-format negotiation, on-the-fly
//! MVT → MLT transcoding, and the response bytes the tile handler emits. The
//! tile handler owns routing/orchestration; everything MLT-specific lives here.
//!
//! Uses the reference `mlt-core` encoder — the same crate Martin and the `mlt`
//! CLI use. Input is a single *decompressed* MVT tile; output is raw MLT bytes
//! (the caller applies transport compression). Encoder defaults match the
//! reference toolchain (Morton sort attempt, FSST + FastPFOR + shared dict on).
//!
//! ## Integer-property narrowing
//!
//! MVT encodes every integer attribute as a 64-bit varint, and `mlt-core`'s MVT
//! importer types them all as `I64`/`U64` columns regardless of their actual
//! range. The `@maplibre/mlt` decoder bundled in maplibre-gl materializes any
//! 64-bit column as a JS `BigInt`, and maplibre-gl's web-worker transfer cannot
//! serialize a `BigInt` ("Do not know how to serialize a BigInt"), so the whole
//! tile fails to render. This bites OMT-derived tiles hard: e.g. the `building`
//! layer's `render_height`/`render_min_height` (values like 26, 8, 0 — the
//! fill-extrusion height/base) come through as `BigInt` and crash 3D building
//! rendering from ~z15.
//!
//! We sidestep it by narrowing each `I64`/`U64` property column to the smallest
//! type that holds every value before encoding: `I32`/`U32` when in range, else
//! `F64` when within `2^53` (JS-safe). Those decode to plain JS `Number`. The
//! id column is unaffected — `mlt-core` already emits it via the dedicated id
//! path, which maplibre decodes as a `Number` for ids below `2^53`.
//!
//! TODO: remove this narrowing workaround once the MapLibre GL / `@maplibre/mlt`
//! decode path can render 64-bit property columns without producing worker
//! transfer failures or unusable `BigInt` paint/layout values.

use std::io::{Read, Write};

use anyhow::{Context, Result};
use axum::http::{HeaderMap, StatusCode, header};
use bytes::Bytes;
use flate2::{Compression as GzLevel, read::GzDecoder, write::GzEncoder};
use mlt_core::encoder::EncoderConfig;
use mlt_core::{PropKind, PropValue, TileLayer};

use crate::interned::TilesetId;
use crate::pmtiles::{MLT_CONTENT_TYPE, TileData, TileType};
use crate::server::{AppState, HttpError};

/// Transcodes one decompressed MVT tile into raw MLT bytes. Concatenates the
/// per-layer MLT encodings the same way the reference `mlt` CLI does.
pub(crate) fn mvt_to_mlt(mvt: &[u8]) -> Result<Vec<u8>> {
    let cfg = EncoderConfig::default();
    let mut out = Vec::new();
    for layer in mlt_core::mvt::mvt_to_tile_layers(mvt).context("parse MVT layers")? {
        // Only rebuild layers that actually carry 64-bit integer columns;
        // everything else (the common case) encodes straight through.
        let layer = if has_wide_int_column(&layer) {
            narrow_int_columns(&layer).context("narrow MLT integer columns")?
        } else {
            layer
        };
        out.extend_from_slice(&layer.encode(cfg).context("encode MLT layer")?);
    }
    Ok(out)
}

/// Largest unsigned integer exactly representable as an `f64` (`Number.MAX_SAFE_INTEGER + 1`).
const JS_SAFE_INT: u64 = 1 << 53;
/// Maximum decompressed MVT accepted for transcoding. A single vector tile is
/// normally well under a megabyte; this bounds gzip expansion so a compromised
/// peer cannot return a tiny high-ratio payload that inflates to gigabytes and
/// OOMs the node on one `.mlt` request. Comfortably above any legitimate tile.
const MAX_DECOMPRESSED_MVT_BYTES: usize = 16 * 1024 * 1024;

/// Whether any property column in the layer is a 64-bit integer (so would
/// decode to a `BigInt` in the browser).
fn has_wide_int_column(layer: &TileLayer) -> bool {
    let Some(first) = layer.features().first() else {
        return false;
    };
    first
        .properties()
        .iter()
        .any(|p| matches!(PropKind::from(p), PropKind::I64 | PropKind::U64))
}

/// Picks the narrowest property kind that losslessly holds every value in the
/// column. Only `I64`/`U64` are narrowed; all other kinds pass through.
fn narrowed_kind(kind: PropKind, values: impl Iterator<Item = PropValue>) -> PropKind {
    match kind {
        PropKind::I64 => {
            let (mut all_i32, mut all_safe) = (true, true);
            for v in values {
                if let PropValue::I64(Some(x)) = v {
                    all_i32 &= i32::try_from(x).is_ok();
                    all_safe &= x.unsigned_abs() < JS_SAFE_INT;
                }
            }
            if all_i32 {
                PropKind::I32
            } else if all_safe {
                PropKind::F64
            } else {
                PropKind::I64
            }
        }
        PropKind::U64 => {
            let (mut all_u32, mut all_safe) = (true, true);
            for v in values {
                if let PropValue::U64(Some(x)) = v {
                    all_u32 &= u32::try_from(x).is_ok();
                    all_safe &= x < JS_SAFE_INT;
                }
            }
            if all_u32 {
                PropKind::U32
            } else if all_safe {
                PropKind::F64
            } else {
                PropKind::U64
            }
        }
        other => other,
    }
}

/// Converts one value to the narrowed target kind (preserving nulls). Values
/// only ever shrink within their numeric range, so the casts are lossless.
fn convert_value(value: &PropValue, target: PropKind) -> PropValue {
    match (target, value) {
        (PropKind::I32, PropValue::I64(o)) => PropValue::I32(o.map(|x| x as i32)),
        (PropKind::F64, PropValue::I64(o)) => PropValue::F64(o.map(|x| x as f64)),
        (PropKind::U32, PropValue::U64(o)) => PropValue::U32(o.map(|x| x as u32)),
        (PropKind::F64, PropValue::U64(o)) => PropValue::F64(o.map(|x| x as f64)),
        _ => value.clone(),
    }
}

/// Rebuilds a layer with its `I64`/`U64` property columns narrowed. `mlt-core`
/// validates that each value matches its column's declared kind and exposes no
/// in-place column retyping, so we reconstruct through the public builder.
fn narrow_int_columns(layer: &TileLayer) -> Result<TileLayer> {
    // TODO: this rebuild is a compatibility workaround, not part of the MLT
    // format contract. Prefer deleting it over extending it once clients handle
    // wide integer columns correctly.
    let names = layer.property_names();
    let features = layer.features();
    let ncols = names.len();

    let targets: Vec<PropKind> = (0..ncols)
        .map(|j| {
            let kind = PropKind::from(&features[0].properties()[j]);
            narrowed_kind(kind, features.iter().map(|f| f.properties()[j].clone()))
        })
        .collect();

    let mut builder =
        TileLayer::builder(layer.name(), layer.extent().get()).context("create layer builder")?;
    let keys = names
        .iter()
        .zip(&targets)
        .map(|(name, &kind)| builder.add_property(name.clone(), kind))
        .collect::<Result<Vec<_>, _>>()
        .context("declare narrowed columns")?;

    for feature in features {
        let mut fb = builder.feature(feature.geometry().clone());
        fb.id(feature.id());
        for (j, &key) in keys.iter().enumerate() {
            fb.property(key, convert_value(&feature.properties()[j], targets[j]))
                .context("set narrowed property")?;
        }
        fb.finish().context("finish feature")?;
    }
    Ok(builder.finish())
}

/// External tile representation requested by path extension or `Accept`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RequestedTileFormat {
    AsStored,
    Mlt,
}

/// Picks the response format and strips any extension off the `y` segment.
/// Priority: path extension (`.mlt` — the canonical, CDN-safe form) > `Accept:
/// application/vnd.maplibre-tile` (Martin-compatible). Defaults to the native
/// tile representation stored in PMTiles.
pub(crate) fn negotiate_format(y_raw: &str, headers: &HeaderMap) -> (String, RequestedTileFormat) {
    if let Some(y) = y_raw.strip_suffix(".mlt") {
        return (y.to_string(), RequestedTileFormat::Mlt);
    }
    let y = y_raw
        .strip_suffix(".mvt")
        .or_else(|| y_raw.strip_suffix(".pbf"))
        // `.webp` matches Mapterhorn/terrain single-tile URLs; served as-stored.
        .or_else(|| y_raw.strip_suffix(".webp"))
        .unwrap_or(y_raw)
        .to_string();
    let wants_mlt = headers
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|accept| accept.contains(MLT_CONTENT_TYPE));
    (
        y,
        if wants_mlt {
            RequestedTileFormat::Mlt
        } else {
            RequestedTileFormat::AsStored
        },
    )
}

/// Returns MLT bytes for a tile. Native MLT PMTiles are served as-is; MVT
/// PMTiles are transcoded and gzip-compressed for transport.
pub(crate) async fn mlt_response_bytes(
    state: &AppState,
    tileset_id: &TilesetId,
    tile_id: u64,
    tile: TileData,
) -> Result<(Bytes, Option<&'static str>, &'static str), HttpError> {
    if is_mlt_tile(&tile) {
        // Native MLT PMTiles: serve as-is, no transcode.
        return Ok((tile.bytes, tile.content_encoding, "mlt_native"));
    }
    if tile.content_type != TileType::Mvt.content_type() {
        return Err((
            StatusCode::NOT_ACCEPTABLE,
            format!("cannot serve {} tile as MLT", tile.content_type),
        ));
    }
    let bytes = transcoded_mlt(state, tileset_id, tile_id, tile).await?;
    Ok((bytes, Some("gzip"), "mlt_transcoded"))
}

/// Whether a fetched tile is already in MLT format (native MLT PMTiles).
pub(crate) fn is_mlt_tile(tile: &TileData) -> bool {
    tile.content_type == MLT_CONTENT_TYPE
}

/// Returns gzip-compressed MLT for a tile, transcoding from MVT on the first
/// request and caching the (transport-compressed) result per pod.
async fn transcoded_mlt(
    state: &AppState,
    tileset_id: &TilesetId,
    tile_id: u64,
    tile: TileData,
) -> Result<Bytes, HttpError> {
    let key = (tileset_id.clone(), tile_id);
    let cache = state.mlt_cache().clone();
    let state = state.clone();
    cache
        .try_get_with(key, async move {
            // MLT encoding (FSST/FastPFOR plus gzip) is CPU-heavy. Keep it off
            // Tokio workers and under the shared CPU-work admission limit (which
            // sheds with 503 under extreme overload).
            let permit = state.admit_transcode_work().await?;
            tokio::task::spawn_blocking(move || {
                // A dropped request cannot cancel blocking work. Keep the permit
                // in this closure so abandoned work remains within the limit.
                let _permit = permit;
                transcode_mlt(tile)
            })
            .await
            .map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("MLT transcode task failed: {error}"),
                )
            })?
        })
        .await
        .map_err(|error: std::sync::Arc<HttpError>| (*error).clone())
}

pub(crate) fn transcode_mlt(tile: TileData) -> Result<Bytes, HttpError> {
    // `mlt-core` needs the decompressed MVT; PMTiles stores tiles gzip-encoded.
    let raw_mvt = match tile.content_encoding {
        Some("gzip") => gunzip(&tile.bytes, MAX_DECOMPRESSED_MVT_BYTES)?,
        None => {
            if tile.bytes.len() > MAX_DECOMPRESSED_MVT_BYTES {
                return Err((
                    StatusCode::BAD_GATEWAY,
                    "MVT tile too large to transcode".to_string(),
                ));
            }
            tile.bytes.to_vec()
        }
        Some(encoding) => {
            return Err((
                StatusCode::NOT_ACCEPTABLE,
                format!("cannot transcode {encoding}-encoded tile to MLT"),
            ));
        }
    };
    let mlt = mvt_to_mlt(&raw_mvt).map_err(|error| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("mlt transcode: {error}"),
        )
    })?;
    Ok(Bytes::from(gzip(&mlt)?))
}

fn gunzip(data: &[u8], max_bytes: usize) -> Result<Vec<u8>, HttpError> {
    // Read one byte past the limit so an over-budget stream is detected without
    // materializing the whole expansion — the gzip-bomb guard.
    let mut out = Vec::new();
    GzDecoder::new(data)
        .take(max_bytes.saturating_add(1) as u64)
        .read_to_end(&mut out)
        .map_err(|error| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("gunzip: {error}"),
            )
        })?;
    if out.len() > max_bytes {
        return Err((
            StatusCode::BAD_GATEWAY,
            "decompressed MVT exceeds the transcode budget".to_string(),
        ));
    }
    Ok(out)
}

fn gzip(data: &[u8]) -> Result<Vec<u8>, HttpError> {
    let mut encoder = GzEncoder::new(Vec::new(), GzLevel::default());
    encoder
        .write_all(data)
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("gzip: {error}")))?;
    encoder
        .finish()
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, format!("gzip: {error}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderValue, header};
    use mlt_core::TileLayer;
    use mlt_core::geo_types::{Geometry, Point};

    /// An empty MVT (zero-length protobuf message, no layers) transcodes to an
    /// empty MLT. Exercises the parse + encode path through `mlt-core`.
    #[test]
    fn empty_mvt_transcodes_to_empty_mlt() {
        assert!(mvt_to_mlt(&[]).unwrap().is_empty());
    }

    /// Builds a one-feature layer carrying small `I64`/`U64` property values and
    /// asserts narrowing collapses them to 32-bit columns (which decode as JS
    /// `Number`, not `BigInt`) while preserving the values.
    #[test]
    fn narrows_small_64bit_columns_to_32bit() {
        let mut b = TileLayer::builder("building", 4096).unwrap();
        let h = b.add_property("render_height", PropKind::I64).unwrap();
        let r = b.add_property("rank", PropKind::U64).unwrap();
        let mut f = b.feature(Geometry::Point(Point::new(1, 2)));
        f.id(Some(14_905_260_892)); // large OSM id: stays on the dedicated id path
        f.property(h, PropValue::I64(Some(26))).unwrap();
        f.property(r, PropValue::U64(Some(3))).unwrap();
        f.finish().unwrap();
        let layer = b.finish();

        assert!(has_wide_int_column(&layer));
        let narrowed = narrow_int_columns(&layer).unwrap();
        assert!(!has_wide_int_column(&narrowed));

        let props = narrowed.features()[0].properties();
        assert_eq!(props[0], PropValue::I32(Some(26)));
        assert_eq!(props[1], PropValue::U32(Some(3)));
        // The round-trip still encodes cleanly.
        assert!(
            !narrowed
                .encode(EncoderConfig::default())
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn extension_selects_format_and_strips_y() {
        let h = HeaderMap::new();
        assert_eq!(
            negotiate_format("6451", &h),
            ("6451".into(), RequestedTileFormat::AsStored)
        );
        assert_eq!(
            negotiate_format("6451.mvt", &h),
            ("6451".into(), RequestedTileFormat::AsStored)
        );
        assert_eq!(
            negotiate_format("6451.pbf", &h),
            ("6451".into(), RequestedTileFormat::AsStored)
        );
        // `.webp` (Mapterhorn/terrain) is served as-stored.
        assert_eq!(
            negotiate_format("6451.webp", &h),
            ("6451".into(), RequestedTileFormat::AsStored)
        );
        assert_eq!(
            negotiate_format("6451.mlt", &h),
            ("6451".into(), RequestedTileFormat::Mlt)
        );
    }

    #[test]
    fn accept_header_selects_mlt_without_extension() {
        let plain = HeaderMap::new();
        assert_eq!(
            negotiate_format("6451", &plain).1,
            RequestedTileFormat::AsStored
        );

        let mut accept = HeaderMap::new();
        accept.insert(header::ACCEPT, HeaderValue::from_static(MLT_CONTENT_TYPE));
        assert_eq!(
            negotiate_format("6451", &accept).1,
            RequestedTileFormat::Mlt
        );
    }

    #[test]
    fn extension_wins_over_conflicting_accept() {
        // `.mlt` path is canonical even when Accept asks for protobuf.
        let mut accept_mvt = HeaderMap::new();
        accept_mvt.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/x-protobuf"),
        );
        assert_eq!(
            negotiate_format("6451.mlt", &accept_mvt).1,
            RequestedTileFormat::Mlt
        );
    }

    #[test]
    fn detects_native_mlt_tiles_by_content_type() {
        let mlt = TileData {
            bytes: Bytes::new(),
            content_type: MLT_CONTENT_TYPE,
            content_encoding: Some("gzip"),
        };
        assert!(is_mlt_tile(&mlt));

        let mvt = TileData {
            bytes: Bytes::new(),
            content_type: TileType::Mvt.content_type(),
            content_encoding: Some("gzip"),
        };
        assert!(!is_mlt_tile(&mvt));
    }

    #[test]
    fn gunzip_rejects_expansion_past_the_decompressed_budget() {
        let compressed = gzip(&vec![0; 64 * 1024]).unwrap();
        let error = gunzip(&compressed, 4 * 1024).unwrap_err();

        assert_eq!(error.0, StatusCode::BAD_GATEWAY);
        assert!(error.1.contains("exceeds the transcode budget"));
    }
}

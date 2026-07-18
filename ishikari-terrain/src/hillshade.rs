//! Baked vector hillshade generation from a Terrarium DEM neighborhood.
//!
//! The shading model is a discretization of maplibre-gl's default (`standard`)
//! raster hillshade: the same per-zoom slope exaggeration, the same
//! aspect-based shadow/highlight mix, `sin(atan(slope))` intensity, and the
//! slope-only accent term. To keep tiles small, the full shader composite is
//! projected onto a single signed tone value per cell — the luminance shift it
//! would cause over a reference background using the reference palette — so
//! one polygon partition carries everything: `class` is the sign (`shadow` /
//! `highlight`), `level` the perceptually quantized magnitude, and a style
//! reproduces the raster look with two fill layers and midpoint opacity ramps
//! (shadow at the reference dark color, highlight white).

use std::cmp::Reverse;
use std::collections::{BTreeMap, VecDeque};

use anyhow::{Context, Result};
use fast_mvt::{MvtCoord, MvtFeature, MvtGeometry, MvtLayer, MvtLineString, MvtPolygon, MvtTile};
use image::{ExtendedColorType, ImageEncoder, codecs::webp::WebPEncoder};

use super::{dem::DemNeighborhood, topology::trace_interpolated_shared_rings};

/// Illumination grid resolution. Matching the 512 px source keeps band
/// boundaries at 1-source-pixel precision (8 MVT units), the main lever for
/// visual fidelity against raster hillshade.
const GRID_SIZE: usize = 512;
const GRID_BUFFER: i32 = 1;
/// Total perceptual-tone budget. It is divided between shadow and highlight by
/// the square root of each side's CIE L* span. A linear split starves the
/// shorter highlight range of spatial resolution; square-root weighting keeps
/// more levels on the larger shadow range without reducing highlights to a few
/// broad, over-bright bands.
/// 32 total: smaller budgets proved too sparse once the first band was pinned
/// near the JND, leaving deep bands visibly wide and flattening steep terrain.
const TOTAL_TONE_LEVELS: u8 = 32;
/// Keep the first shadow band close to the just-noticeable contrast around the
/// reference background, then let later bands grow progressively wider.
const FIRST_SHADOW_LEVEL_DELTA_L: f64 = 2.5;
/// Only single cells merge (salt noise). Anything larger is kept: the fine
/// texture of rough terrain IS a mass of small tonal islands. Every pruning
/// rule tried beyond this — by size, by discrete level contrast, or by
/// distance of the continuous tone code to the quantization threshold —
/// either visibly flattened detail or spread shadow/highlight patches, while
/// saving only a few percent when tuned safely.
const MIN_REGION_CELLS: usize = 2;
const SUN_AZIMUTH_DEGREES: f32 = 315.0;
const EARTH_CIRCUMFERENCE_METERS: f64 = 40_075_016.685_578_49;
/// Reference palette used by the baked tone projection and preview style.
const SHADOW_SRGB: [u8; 3] = [0x25, 0x30, 0x33];
const ACCENT_SRGB: [u8; 3] = [0x65, 0x70, 0x74];
const BACKGROUND_SRGB: [u8; 3] = [0xc9, 0xcc, 0xca];
const HIGHLIGHT_SRGB: [u8; 3] = [0xff, 0xff, 0xff];
/// Half-pixel fixed-point coordinates retain useful continuous threshold
/// crossings without paying the larger deltas of a conventional 4096 extent.
const HILLSHADE_COORD_SCALE: i32 = 2;
const HILLSHADE_EXTENT: u32 = GRID_SIZE as u32 * HILLSHADE_COORD_SCALE as u32;
/// Physically-true slopes nearly vanish at low zooms (a z7 pixel spans ~500 m,
/// so almost everything falls inside the neutral band and tiles come out almost
/// empty). MapLibre's raster hillshade solves this in its prepare shader by
/// scaling the derivative with a piecewise per-zoom exaggeration; use the
/// exact same curve so the vector product reads consistently with raster
/// hillshade at every zoom instead of following a bespoke rule: physical Horn
/// slope at z>=15, boosted by 2^(factor * (15 - z)) below, with factor 0.3
/// (0.35 under z4.5, 0.4 under z2).
fn zoom_exaggeration(zoom: u8) -> f64 {
    let zoom = f64::from(zoom);
    if zoom >= 15.0 {
        return 1.0;
    }
    let factor = if zoom < 2.0 {
        0.4
    } else if zoom < 4.5 {
        0.35
    } else {
        0.3
    };
    2_f64.powf(factor * (15.0 - zoom))
}
struct ShadeGrid {
    /// Signed tone levels: negative darkens (shadow), positive lightens.
    labels: Vec<i8>,
    /// Continuous signed level codes aligned with `labels`. Topology comes from
    /// the labels; these values only position each shared boundary sub-pixel.
    tones: Vec<f32>,
    width: usize,
    height: usize,
    origin: i32,
}

#[derive(Clone, Copy, Debug)]
struct ToneProfile {
    shadow_srgb: [f64; 3],
    accent_srgb: [f64; 3],
    background_srgb: [f64; 3],
    highlight_srgb: [f64; 3],
    background_lightness: f64,
    shadow_span: f64,
    highlight_span: f64,
    shadow_levels: u8,
    highlight_levels: u8,
    compression_mu: f64,
}

impl ToneProfile {
    fn new() -> Self {
        let shadow_srgb = normalized_srgb(SHADOW_SRGB);
        let accent_srgb = normalized_srgb(ACCENT_SRGB);
        let background_srgb = normalized_srgb(BACKGROUND_SRGB);
        let highlight_srgb = normalized_srgb(HIGHLIGHT_SRGB);
        let shadow_luminance = relative_luminance(shadow_srgb);
        let background_luminance = relative_luminance(background_srgb);
        let highlight_luminance = relative_luminance(highlight_srgb);
        let shadow_lightness = lightness(shadow_luminance);
        let background_lightness = lightness(background_luminance);
        let highlight_lightness = lightness(highlight_luminance);
        let shadow_span = background_lightness - shadow_lightness;
        let highlight_span = highlight_lightness - background_lightness;
        let shadow_weight = shadow_span.sqrt();
        let highlight_weight = highlight_span.sqrt();
        let shadow_levels = ((f64::from(TOTAL_TONE_LEVELS) * shadow_weight
            / (shadow_weight + highlight_weight))
            .round() as u8)
            .clamp(1, TOTAL_TONE_LEVELS - 1);
        let highlight_levels = TOTAL_TONE_LEVELS - shadow_levels;
        let compression_mu =
            solve_compression_mu(shadow_span, shadow_levels, FIRST_SHADOW_LEVEL_DELTA_L);
        Self {
            shadow_srgb,
            accent_srgb,
            background_srgb,
            highlight_srgb,
            background_lightness,
            shadow_span,
            highlight_span,
            shadow_levels,
            highlight_levels,
            compression_mu,
        }
    }

    fn levels(self, shadow: bool) -> u8 {
        if shadow {
            self.shadow_levels
        } else {
            self.highlight_levels
        }
    }

    fn span(self, shadow: bool) -> f64 {
        if shadow {
            self.shadow_span
        } else {
            self.highlight_span
        }
    }

    fn lightness_delta_for_code(self, shadow: bool, code: f64) -> f64 {
        compressed_lightness_delta(
            self.span(shadow),
            self.levels(shadow),
            self.compression_mu,
            code,
        )
    }

    fn code_for_lightness_delta(self, shadow: bool, delta: f64) -> f64 {
        let span = self.span(shadow);
        let levels = f64::from(self.levels(shadow));
        if self.compression_mu <= f64::EPSILON {
            return levels * delta / span;
        }
        levels * (self.compression_mu * delta / span).ln_1p() / self.compression_mu.ln_1p()
    }
}

fn compressed_lightness_delta(span: f64, levels: u8, mu: f64, code: f64) -> f64 {
    if mu <= f64::EPSILON {
        return span * code / f64::from(levels);
    }
    span * (code / f64::from(levels) * mu.ln_1p()).exp_m1() / mu
}

fn solve_compression_mu(span: f64, levels: u8, first_level_delta: f64) -> f64 {
    if span / f64::from(levels) <= first_level_delta {
        return 0.0;
    }
    let mut low = 0.0;
    let mut high = 1.0;
    while compressed_lightness_delta(span, levels, high, 1.0) > first_level_delta {
        high *= 2.0;
    }
    for _ in 0..64 {
        let middle = (low + high) * 0.5;
        if compressed_lightness_delta(span, levels, middle, 1.0) > first_level_delta {
            low = middle;
        } else {
            high = middle;
        }
    }
    high
}

pub fn generate(neighborhood: &DemNeighborhood, zoom: u8, tile_y: u32) -> Result<Vec<u8>> {
    let started = std::time::Instant::now();
    let mut grid = illumination_labels(neighborhood, zoom, tile_y);
    let labels_elapsed = started.elapsed();

    let speckles_started = std::time::Instant::now();
    remove_speckles(&mut grid.labels, grid.width, grid.height);
    let speckles_elapsed = speckles_started.elapsed();

    let polygonize_started = std::time::Instant::now();
    let polygons_by_shade = polygonize(
        &grid.labels,
        &grid.tones,
        grid.width,
        grid.height,
        grid.origin,
        GRID_SIZE,
    );
    let polygonize_elapsed = polygonize_started.elapsed();

    let encode_started = std::time::Instant::now();
    let extent = fast_mvt::MvtExtent::new(HILLSHADE_EXTENT).expect("valid extent");
    let mut layer = MvtLayer::new("hillshade", extent);
    for (shade, polygons) in polygons_by_shade {
        let class = if shade < 0 { "shadow" } else { "highlight" };
        let level = shade.unsigned_abs();
        for polygon in polygons {
            let mut feature = MvtFeature::new(MvtGeometry::Polygon(polygon));
            feature.add_tag_string("class", class);
            feature.add_tag_uint("level", u64::from(level));
            layer.add_feature(feature);
        }
    }

    let mut tile = MvtTile::new();
    tile.add_layer(layer);
    let encoded = tile.encode().context("encode hillshade MVT");
    tracing::debug!(
        labels_ms = labels_elapsed.as_millis() as u64,
        speckles_ms = speckles_elapsed.as_millis() as u64,
        polygonize_ms = polygonize_elapsed.as_millis() as u64,
        encode_ms = encode_started.elapsed().as_millis() as u64,
        "hillshade stages"
    );
    encoded
}

/// Bytes per unit tone code in the neutral shade raster. The signed tone code
/// is stored as `128 + code * SHADE_CODE_SCALE` in a grayscale (R=G=B) pixel,
/// so the data stays palette-neutral and a style-side `color-relief` ramp does
/// the coloring. The preview decodes it with the standard Terrarium unpack
/// (`color-relief`'s custom encoding does not evaluate in the GPU shader);
/// because the pixel is gray, Terrarium's high-byte sensitivity is harmless
/// even under lossy codecs — a small byte error is a small, sub-level error.
pub const SHADE_CODE_SCALE: f64 = 5.0;

fn shade_byte(code: f64) -> u8 {
    // Nodata reaches this as NaN; `clamp` propagates NaN and `as u8` casts it
    // to 0 (an extreme shadow byte). Nodata must render as the neutral 128.
    if code.is_nan() {
        return 128;
    }
    (128.0 + code * SHADE_CODE_SCALE).round().clamp(0.0, 255.0) as u8
}

/// Grayscale RGB where every pixel carries the signed tone code (see
/// [`SHADE_CODE_SCALE`]). `continuous` reads the un-rounded field (best for
/// lossy codecs, no banding); otherwise the quantized levels (best for lossless
/// codecs, few distinct values).
fn shade_raster_rgb(grid: &ShadeGrid, continuous: bool) -> Vec<u8> {
    let offset = GRID_BUFFER as usize;
    let mut rgb = vec![0u8; GRID_SIZE * GRID_SIZE * 3];
    for y in 0..GRID_SIZE {
        for x in 0..GRID_SIZE {
            let index = (y + offset) * grid.width + (x + offset);
            let code = if continuous {
                f64::from(grid.tones[index])
            } else {
                f64::from(grid.labels[index])
            };
            let byte = shade_byte(code);
            let base = (y * GRID_SIZE + x) * 3;
            rgb[base] = byte;
            rgb[base + 1] = byte;
            rgb[base + 2] = byte;
        }
    }
    rgb
}

/// Neutral shade raster as lossless WebP over the quantized levels. Losslessly
/// compressing a few dozen discrete values is compact and exact; recolor is
/// deferred to a style-side `color-relief` ramp. Trade vs vector: fixed
/// resolution (blurs when overzoomed) for far fewer bytes on rough terrain.
pub fn generate_raster(neighborhood: &DemNeighborhood, zoom: u8, tile_y: u32) -> Result<Vec<u8>> {
    let grid = illumination_labels(neighborhood, zoom, tile_y);
    let rgb = shade_raster_rgb(&grid, false);
    let mut buffer = Vec::new();
    WebPEncoder::new_lossless(&mut buffer)
        .write_image(
            &rgb,
            GRID_SIZE as u32,
            GRID_SIZE as u32,
            ExtendedColorType::Rgb8,
        )
        .context("encode hillshade raster WebP (lossless)")?;
    Ok(buffer)
}

/// Neutral shade raster as lossy WebP over the continuous field. A lossy coder
/// carries the full un-banded shade at a fraction of the bytes; its errors are
/// sub-JND tone deviations, not artifacts, because a continuous ramp — not
/// exact codes — drives the coloring. WebP keeps edges cleaner than JPEG.
pub fn generate_raster_webp_lossy(
    neighborhood: &DemNeighborhood,
    zoom: u8,
    tile_y: u32,
    quality: u8,
) -> Result<Vec<u8>> {
    let grid = illumination_labels(neighborhood, zoom, tile_y);
    let rgb = shade_raster_rgb(&grid, true);
    let encoded = webp::Encoder::from_rgb(&rgb, GRID_SIZE as u32, GRID_SIZE as u32)
        .encode(f32::from(quality));
    Ok(encoded.to_vec())
}

/// Neutral shade raster as lossy JPEG over the continuous field: the size floor
/// proxy (WebP/AVIF lossy beat it, and JPEG has no alpha). Same continuous,
/// un-banded shade as [`generate_raster_webp_lossy`].
pub fn generate_raster_jpeg(
    neighborhood: &DemNeighborhood,
    zoom: u8,
    tile_y: u32,
    quality: u8,
) -> Result<Vec<u8>> {
    let grid = illumination_labels(neighborhood, zoom, tile_y);
    let rgb = shade_raster_rgb(&grid, true);
    let mut buffer = Vec::new();
    image::codecs::jpeg::JpegEncoder::new_with_quality(&mut buffer, quality)
        .write_image(
            &rgb,
            GRID_SIZE as u32,
            GRID_SIZE as u32,
            ExtendedColorType::Rgb8,
        )
        .context("encode hillshade raster JPEG")?;
    Ok(buffer)
}

fn illumination_labels(neighborhood: &DemNeighborhood, zoom: u8, tile_y: u32) -> ShadeGrid {
    let width = neighborhood.width();
    let height = neighborhood.height();
    let step_x = width as f32 / GRID_SIZE as f32;
    let step_y = height as f32 / GRID_SIZE as f32;
    let sample_step = step_x.max(step_y).round().max(1.0) as i32;
    // Matches maplibre-gl's `standard` hillshade shader: the light azimuth is
    // offset by pi, aspect mixes shadow vs highlight, and atan of the
    // (exaggerated) slope drives intensity.
    let azimuth = f64::from(SUN_AZIMUTH_DEGREES.to_radians()) + std::f64::consts::PI;
    let grid_width = (GRID_SIZE as i32 + 2 * GRID_BUFFER) as usize;
    let grid_height = grid_width;
    let tone_profile = ToneProfile::new();

    let field_buffer = GRID_BUFFER;
    let field_width = grid_width;
    let mut field = vec![f64::NAN; field_width * field_width];
    let sampling_started = std::time::Instant::now();

    // Sample the DEM directly, as maplibre's raster hillshade shader does: a
    // [1 2 1]^2 pre-blur before the derivatives visibly softened the result
    // against the raster reference. The memoized grid keeps the eight Sobel
    // taps of neighboring field cells from re-reading pixels through the
    // neighborhood indirection.
    let tap = sample_step;
    let source_x_at = |grid: i32| ((grid as f32 + 0.5) * step_x).floor() as i32;
    let source_y_at = |grid: i32| ((grid as f32 + 0.5) * step_y).floor() as i32;
    let elevation_min_x = source_x_at(-field_buffer) - tap;
    let elevation_max_x = source_x_at(GRID_SIZE as i32 - 1 + field_buffer) + tap;
    let elevation_min_y = source_y_at(-field_buffer) - tap;
    let elevation_max_y = source_y_at(GRID_SIZE as i32 - 1 + field_buffer) + tap;
    let elevation_width = (elevation_max_x - elevation_min_x + 1) as usize;
    let mut elevation_grid =
        vec![f32::NAN; elevation_width * (elevation_max_y - elevation_min_y + 1) as usize];
    for y in elevation_min_y..=elevation_max_y {
        let row = (y - elevation_min_y) as usize * elevation_width;
        for x in elevation_min_x..=elevation_max_x {
            elevation_grid[row + (x - elevation_min_x) as usize] = neighborhood.get(x, y);
        }
    }
    let elevation_at = |x: i32, y: i32| -> f32 {
        elevation_grid
            [(y - elevation_min_y) as usize * elevation_width + (x - elevation_min_x) as usize]
    };

    let exaggeration = zoom_exaggeration(zoom);
    for local_y in 0..field_width {
        let grid_y = local_y as i32 - field_buffer;
        let source_y = ((grid_y as f32 + 0.5) * step_y).floor() as i32;
        let meters_per_pixel = meters_per_pixel(zoom, tile_y, source_y, height);
        let denominator = 8.0 * meters_per_pixel * f64::from(sample_step) / exaggeration;
        for local_x in 0..field_width {
            let grid_x = local_x as i32 - field_buffer;
            let source_x = ((grid_x as f32 + 0.5) * step_x).floor() as i32;
            let s = sample_step;
            let tl = elevation_at(source_x - s, source_y - s);
            let top = elevation_at(source_x, source_y - s);
            let tr = elevation_at(source_x + s, source_y - s);
            let left = elevation_at(source_x - s, source_y);
            let right = elevation_at(source_x + s, source_y);
            let bl = elevation_at(source_x - s, source_y + s);
            let bottom = elevation_at(source_x, source_y + s);
            let br = elevation_at(source_x + s, source_y + s);
            if ![tl, top, tr, left, right, bl, bottom, br]
                .into_iter()
                .all(f32::is_finite)
            {
                continue;
            }

            let dz_dx = f64::from(tr + 2.0 * right + br - tl - 2.0 * left - bl) / denominator;
            let dz_dy = f64::from(bl + 2.0 * bottom + br - tl - 2.0 * top - tr) / denominator;
            field[local_y * field_width + local_x] =
                composite_lightness_delta(dz_dx, dz_dy, azimuth, tone_profile);
        }
    }
    let field_elapsed = sampling_started.elapsed();
    tracing::debug!(
        field_ms = field_elapsed.as_millis() as u64,
        "hillshade field stages"
    );

    let mut labels = vec![0; grid_width * grid_height];
    let mut tones = vec![f32::NAN; grid_width * grid_height];
    for local_y in 0..grid_height {
        for local_x in 0..grid_width {
            let index = local_y * grid_width + local_x;
            let value = field[index];
            if value.is_finite() {
                tones[index] = signed_code(value, tone_profile) as f32;
                labels[index] = quantize_lightness_delta(value, tone_profile);
            }
        }
    }
    ShadeGrid {
        labels,
        tones,
        width: grid_width,
        height: grid_height,
        origin: -GRID_BUFFER,
    }
}

fn meters_per_pixel(zoom: u8, tile_y: u32, pixel_y: i32, tile_size: usize) -> f64 {
    let world_pixels = (1_u64 << zoom) as f64 * tile_size as f64;
    let global_y = tile_y as f64 * tile_size as f64 + f64::from(pixel_y);
    let mercator_y = (global_y / world_pixels).clamp(0.0, 1.0);
    let latitude = (std::f64::consts::PI * (1.0 - 2.0 * mercator_y))
        .sinh()
        .atan();
    EARTH_CIRCUMFERENCE_METERS * latitude.cos() / world_pixels
}

/// Projects maplibre-gl's default (`standard`) hillshade composite onto signed
/// CIE L* distance from the reference background, then quantizes it with the
/// shared perceptual step from [`ToneProfile`].
/// CIE L* lightness of a relative luminance in `[0, 1]`.
fn lightness(luminance: f64) -> f64 {
    const EPSILON: f64 = 216.0 / 24389.0;
    if luminance > EPSILON {
        116.0 * luminance.cbrt() - 16.0
    } else {
        24389.0 / 27.0 * luminance
    }
}

fn normalized_srgb(srgb: [u8; 3]) -> [f64; 3] {
    srgb.map(|channel| f64::from(channel) / 255.0)
}

fn relative_luminance(srgb: [f64; 3]) -> f64 {
    let linear = srgb.map(|channel| {
        if channel <= 0.04045 {
            channel / 12.92
        } else {
            ((channel + 0.055) / 1.055).powf(2.4)
        }
    });
    0.2126 * linear[0] + 0.7152 * linear[1] + 0.0722 * linear[2]
}

fn fill_lightness(profile: ToneProfile, shadow: bool, opacity: f64) -> f64 {
    let fill = if shadow {
        profile.shadow_srgb
    } else {
        profile.highlight_srgb
    };
    let composite = std::array::from_fn(|channel| {
        profile.background_srgb[channel]
            + opacity * (fill[channel] - profile.background_srgb[channel])
    });
    lightness(relative_luminance(composite))
}

/// Solve the fill opacity through the same sRGB channel blend performed by
/// MapLibre. Interpolating relative luminance directly makes dark fills too
/// strong because the sRGB transfer function is nonlinear.
fn opacity_for_lightness(profile: ToneProfile, shadow: bool, target: f64) -> f64 {
    let mut low = 0.0;
    let mut high = 1.0;
    for _ in 0..48 {
        let middle = (low + high) * 0.5;
        let rendered = fill_lightness(profile, shadow, middle);
        if (shadow && rendered > target) || (!shadow && rendered < target) {
            low = middle;
        } else {
            high = middle;
        }
    }
    (low + high) * 0.5
}

pub fn opacity_stops(shadow: bool) -> Vec<(u8, f64)> {
    let profile = ToneProfile::new();
    let direction = if shadow { -1.0 } else { 1.0 };
    (1..=profile.levels(shadow))
        .map(|level| {
            let lightness_delta = profile.lightness_delta_for_code(shadow, f64::from(level));
            let band_lightness = profile.background_lightness + direction * lightness_delta;
            (
                level,
                opacity_for_lightness(profile, shadow, band_lightness),
            )
        })
        .collect()
}

#[cfg(test)]
fn quantize(dz_dx: f64, dz_dy: f64, azimuth: f64, profile: ToneProfile) -> i8 {
    quantize_lightness_delta(
        composite_lightness_delta(dz_dx, dz_dy, azimuth, profile),
        profile,
    )
}

fn composite_lightness_delta(dz_dx: f64, dz_dy: f64, azimuth: f64, profile: ToneProfile) -> f64 {
    let length = (dz_dx * dz_dx + dz_dy * dz_dy).sqrt();
    if length == 0.0 {
        return 0.0;
    }
    let aspect = if dz_dx != 0.0 {
        dz_dy.atan2(-dz_dx)
    } else {
        std::f64::consts::FRAC_PI_2 * if dz_dy > 0.0 { 1.0 } else { -1.0 }
    };
    let shade = (((aspect + azimuth) / std::f64::consts::PI + 0.5).rem_euclid(2.0) - 1.0).abs();
    let scaled_slope = (0.625 * length).atan();

    // maplibre standard composites two terms: an aspect-driven mix of the
    // shadow and highlight colors at `sin(scaledSlope)` alpha, over a
    // slope-only accent at `1 - cos(scaledSlope)` alpha. Evaluate that
    // composite's luminance over the reference background and keep only the
    // resulting signed shift: one scalar per cell instead of overlapping
    // component planes, which halves the boundary geometry per tile.
    let shade_alpha = scaled_slope.sin();
    let accent_alpha = 1.0 - scaled_slope.cos();
    let composite_alpha = accent_alpha * (1.0 - shade_alpha) + shade_alpha;
    // Match the shader's actual order of operations. Its color uniforms and
    // framebuffer blending operate on sRGB channel values; mixing endpoint
    // luminances here instead makes intermediate shadow/highlight colors far
    // too bright because the sRGB transfer function is nonlinear.
    let output_srgb = std::array::from_fn(|channel| {
        let shade_srgb = profile.shadow_srgb[channel]
            + shade * (profile.highlight_srgb[channel] - profile.shadow_srgb[channel]);
        let fragment_srgb = profile.accent_srgb[channel] * accent_alpha * (1.0 - shade_alpha)
            + shade_srgb * shade_alpha;
        fragment_srgb + profile.background_srgb[channel] * (1.0 - composite_alpha)
    });

    // One global CIE L* step serves both sides. Half a step around the
    // background is neutral; every emitted signed level then represents the
    // same perceptual distance regardless of class.
    lightness(relative_luminance(output_srgb)) - profile.background_lightness
}

/// The un-rounded signed position of a lightness shift on the level scale.
fn signed_code(lightness_delta: f64, profile: ToneProfile) -> f64 {
    let shadow = lightness_delta < 0.0;
    let code = profile.code_for_lightness_delta(shadow, lightness_delta.abs());
    if shadow { -code } else { code }
}

fn quantize_lightness_delta(lightness_delta: f64, profile: ToneProfile) -> i8 {
    let code = signed_code(lightness_delta, profile);
    if code.abs() < 0.5 {
        return 0;
    }
    let shadow = code < 0.0;
    let level = code
        .abs()
        .round()
        .clamp(1.0, f64::from(profile.levels(shadow))) as i8;
    if shadow { -level } else { level }
}

fn remove_speckles(labels: &mut [i8], width: usize, height: usize) {
    #[derive(Debug)]
    struct Component {
        label: i8,
        cells: Vec<usize>,
        touches_outer_edge: bool,
    }

    let source = labels.to_vec();
    let mut component_at = vec![usize::MAX; source.len()];
    let mut components = Vec::new();
    for start in 0..source.len() {
        if component_at[start] != usize::MAX {
            continue;
        }
        let id = components.len();
        let label = source[start];
        let mut queue = VecDeque::from([start]);
        let mut cells = Vec::new();
        let mut touches_outer_edge = false;
        component_at[start] = id;
        while let Some(index) = queue.pop_front() {
            cells.push(index);
            let x = index % width;
            let y = index / width;
            touches_outer_edge |= x == 0 || y == 0 || x + 1 == width || y + 1 == height;
            for neighbor in neighbors4(x, y, width, height).into_iter().flatten() {
                if component_at[neighbor] == usize::MAX && source[neighbor] == label {
                    component_at[neighbor] = id;
                    queue.push_back(neighbor);
                }
            }
        }
        components.push(Component {
            label,
            cells,
            touches_outer_edge,
        });
    }

    let stable = components
        .iter()
        .map(|component| component.cells.len() >= MIN_REGION_CELLS || component.touches_outer_edge)
        .collect::<Vec<_>>();
    let mut adjacency = vec![BTreeMap::<usize, usize>::new(); components.len()];
    for (id, component) in components.iter().enumerate() {
        for &index in &component.cells {
            let x = index % width;
            let y = index / width;
            for neighbor in neighbors4(x, y, width, height).into_iter().flatten() {
                let other = component_at[neighbor];
                if other != id {
                    *adjacency[id].entry(other).or_default() += 1;
                }
            }
        }
    }

    let mut resolved = components
        .iter()
        .enumerate()
        .map(|(id, component)| stable[id].then_some(component.label))
        .collect::<Vec<_>>();
    loop {
        let mut round = Vec::new();
        for (id, component) in components.iter().enumerate() {
            if resolved[id].is_some() {
                continue;
            }
            let replacement = adjacency[id]
                .iter()
                .filter_map(|(&other, &count)| resolved[other].map(|label| (label, count)))
                .max_by_key(|&(other_label, count)| {
                    (
                        count,
                        Reverse((i16::from(other_label) - i16::from(component.label)).abs()),
                        Reverse(other_label.unsigned_abs()),
                        Reverse(other_label),
                    )
                })
                .map(|(label, _)| label);
            if let Some(replacement) = replacement {
                round.push((id, replacement));
            }
        }
        if round.is_empty() {
            break;
        }
        for (id, replacement) in round {
            resolved[id] = Some(replacement);
        }
    }

    for (id, component) in components.iter().enumerate() {
        if stable[id] {
            continue;
        }
        if let Some(replacement) = resolved[id] {
            for &index in &component.cells {
                labels[index] = replacement;
            }
        }
    }
}

fn neighbors4(x: usize, y: usize, width: usize, height: usize) -> [Option<usize>; 4] {
    [
        (y > 0).then(|| (y - 1) * width + x),
        (x + 1 < width).then(|| y * width + x + 1),
        (y + 1 < height).then(|| (y + 1) * width + x),
        (x > 0).then(|| y * width + x - 1),
    ]
}

fn polygonize(
    labels: &[i8],
    tones: &[f32],
    width: usize,
    height: usize,
    origin: i32,
    tile_size: usize,
) -> BTreeMap<i8, Vec<MvtPolygon>> {
    let trace_started = std::time::Instant::now();
    let rings = trace_interpolated_shared_rings(
        labels,
        tones,
        width,
        height,
        origin,
        tile_size as i32,
        HILLSHADE_COORD_SCALE,
    );
    let trace_elapsed = trace_started.elapsed();
    let assembly_started = std::time::Instant::now();
    let mut result = BTreeMap::new();
    for (shade, rings) in rings {
        let mut outers: Vec<(Vec<MvtCoord>, Vec<Vec<MvtCoord>>)> = Vec::new();
        let mut holes = Vec::new();
        for ring in rings {
            if ring.len() < 4 {
                continue;
            }
            let ring: Vec<MvtCoord> = ring.into_iter().map(|(x, y)| MvtCoord { x, y }).collect();
            match signed_area(&ring) {
                0 => continue,
                area if area > 0 => outers.push((ring, Vec::new())),
                _ => holes.push(ring),
            }
        }

        for hole in holes {
            let point = hole[0];
            if let Some((_, inner)) = outers
                .iter_mut()
                .filter(|(outer, _)| point_in_ring(point, outer))
                .min_by_key(|(outer, _)| signed_area(outer).unsigned_abs())
            {
                inner.push(hole);
            }
        }

        let polygons = outers
            .into_iter()
            .map(|(outer, holes)| {
                MvtPolygon::new(
                    MvtLineString::new(outer),
                    holes.into_iter().map(MvtLineString::new).collect(),
                )
            })
            .collect::<Vec<_>>();
        if !polygons.is_empty() {
            result.insert(shade, polygons);
        }
    }
    tracing::debug!(
        trace_ms = trace_elapsed.as_millis() as u64,
        assembly_ms = assembly_started.elapsed().as_millis() as u64,
        "hillshade polygonize stages"
    );
    result
}

fn signed_area(ring: &[MvtCoord]) -> i64 {
    ring.iter()
        .zip(ring.iter().cycle().skip(1))
        .take(ring.len())
        .map(|(a, b)| i64::from(a.x) * i64::from(b.y) - i64::from(a.y) * i64::from(b.x))
        .sum()
}

fn point_in_ring(point: MvtCoord, ring: &[MvtCoord]) -> bool {
    let mut inside = false;
    for (a, b) in ring
        .iter()
        .zip(ring.iter().cycle().skip(1))
        .take(ring.len())
    {
        let crosses = (a.y > point.y) != (b.y > point.y)
            && f64::from(point.x)
                < f64::from(b.x - a.x) * f64::from(point.y - a.y) / f64::from(b.y - a.y)
                    + f64::from(a.x);
        if crosses {
            inside = !inside;
        }
    }
    inside
}

#[cfg(test)]
mod tests {
    use super::*;

    fn polygonize_labels(
        labels: Vec<i8>,
        width: usize,
        height: usize,
    ) -> BTreeMap<i8, Vec<MvtPolygon>> {
        let tones = labels
            .iter()
            .map(|label| f32::from(*label))
            .collect::<Vec<_>>();
        polygonize(&labels, &tones, width, height, 0, width)
    }

    #[test]
    fn flat_illumination_is_neutral() {
        let azimuth = f64::from(SUN_AZIMUTH_DEGREES.to_radians()) + std::f64::consts::PI;
        let profile = ToneProfile::new();
        // Flat terrain has no derivative: neutral.
        assert_eq!(quantize(0.0, 0.0, azimuth, profile), 0);
        // A face tilted toward the NW light has its gradient pointing SE
        // (screen y grows southward): net lightening.
        assert!(quantize(1.0, 1.0, azimuth, profile) > 0);
        // The opposite face darkens.
        assert!(quantize(-1.0, -1.0, azimuth, profile) < 0);
        // A slope perpendicular to the light still darkens: the accent term
        // (folded into the tone projection) is aspect-independent.
        assert!(quantize(1.0, -1.0, azimuth, profile) < 0);
        // Steep shadow terrain never escapes the level range.
        assert_eq!(
            quantize(-100.0, -100.0, azimuth, profile),
            -(profile.shadow_levels as i8)
        );
        // A very gentle slope falls under the sparsity floor.
        assert_eq!(quantize(-0.005, -0.005, azimuth, profile), 0);
    }

    #[test]
    fn tone_budget_follows_perceptual_span_and_compands_near_neutral() {
        let profile = ToneProfile::new();
        assert_eq!(
            profile.shadow_levels + profile.highlight_levels,
            TOTAL_TONE_LEVELS
        );
        assert!(profile.shadow_levels > profile.highlight_levels);
        assert!(
            (profile.lightness_delta_for_code(true, 1.0) - FIRST_SHADOW_LEVEL_DELTA_L).abs() < 1e-9
        );
        let between_shadow_levels = profile.lightness_delta_for_code(true, 1.1);
        assert_eq!(
            quantize_lightness_delta(-between_shadow_levels, profile),
            -1
        );
        let between_highlight_levels = profile.lightness_delta_for_code(false, 1.9);
        assert_eq!(
            quantize_lightness_delta(between_highlight_levels, profile),
            2
        );

        for shadow in [true, false] {
            let stops = opacity_stops(shadow);
            assert_eq!(stops.len(), profile.levels(shadow) as usize);
            assert!(
                stops
                    .iter()
                    .map(|(level, _)| *level)
                    .eq(1..=profile.levels(shadow))
            );
            assert!(stops.windows(2).all(|pair| pair[0].1 < pair[1].1));
            assert!(
                stops
                    .iter()
                    .all(|(_, opacity)| (0.0..=1.0).contains(opacity))
            );
            for (level, opacity) in stops {
                let rendered = fill_lightness(profile, shadow, opacity);
                let delta = (rendered - profile.background_lightness).abs();
                let expected = profile.lightness_delta_for_code(shadow, f64::from(level));
                assert!((delta - expected).abs() < 1e-9);
            }
            let deltas = (1..=profile.levels(shadow))
                .map(|level| profile.lightness_delta_for_code(shadow, f64::from(level)))
                .collect::<Vec<_>>();
            assert!(
                deltas
                    .windows(3)
                    .all(|values| values[1] - values[0] < values[2] - values[1])
            );
        }
    }

    #[test]
    fn shader_color_mix_is_evaluated_before_luminance() {
        let shadow = normalized_srgb(SHADOW_SRGB);
        let highlight = normalized_srgb(HIGHLIGHT_SRGB);
        let midpoint = std::array::from_fn(|channel| (shadow[channel] + highlight[channel]) / 2.0);

        // Mixing endpoint luminances would overstate the brightness produced
        // by MapLibre's channel-wise sRGB shader mix.
        assert!(
            relative_luminance(midpoint)
                < (relative_luminance(shadow) + relative_luminance(highlight)) / 2.0
        );
    }

    #[test]
    fn traces_one_square_and_preserves_a_hole() {
        let mut labels = vec![1; 5 * 5];
        labels[2 * 5 + 2] = 0;
        let polygons = polygonize_labels(labels, 5, 5);
        let polygons = &polygons[&1];
        assert_eq!(polygons.len(), 1);
        assert_eq!(polygons[0].interiors().len(), 1);
    }

    /// Collects every polygon edge of one shade as undirected segments.
    /// `geo_types::Polygon` stores rings closed (`first == last`), so
    /// consecutive pairs already cover the full boundary.
    fn ring_edges(
        ring: &fast_mvt::MvtLineString,
        edges: &mut std::collections::BTreeSet<(i32, i32, i32, i32)>,
    ) {
        for pair in ring.0.windows(2) {
            let (a, b) = (pair[0], pair[1]);
            if a == b {
                continue;
            }
            let (p, q) = if (a.x, a.y) <= (b.x, b.y) {
                (a, b)
            } else {
                (b, a)
            };
            edges.insert((p.x, p.y, q.x, q.y));
        }
    }

    fn edge_set(polygons: &[MvtPolygon]) -> std::collections::BTreeSet<(i32, i32, i32, i32)> {
        let mut edges = std::collections::BTreeSet::new();
        for polygon in polygons {
            ring_edges(polygon.exterior(), &mut edges);
            for hole in polygon.interiors() {
                ring_edges(hole, &mut edges);
            }
        }
        edges
    }

    #[test]
    fn staircase_collapses_and_bands_share_identical_boundaries() {
        // Two bands split by a diagonal staircase: x + y < 8 is shade 1.
        let mut labels = vec![0_i8; 64];
        for y in 0..8_usize {
            for x in 0..8_usize {
                labels[y * 8 + x] = if x + y < 8 { 1 } else { 2 };
            }
        }
        let polygons = polygonize_labels(labels, 8, 8);
        let band1 = edge_set(&polygons[&1]);
        let band2 = edge_set(&polygons[&2]);

        // The staircase collapsed: the shade-1 triangle needs only a handful of
        // vertices (the raw staircase boundary alone has ~15).
        let verts: usize = polygons[&1].iter().map(|p| p.exterior().0.len()).sum();
        assert!(verts <= 10, "staircase not simplified: {verts} vertices");

        // Every non-border edge is shared: emitted identically by both bands.
        let scale = HILLSHADE_COORD_SCALE;
        let max = 8 * scale;
        let on_border = |e: &(i32, i32, i32, i32)| {
            (e.0 == 0 && e.2 == 0)
                || (e.0 == max && e.2 == max)
                || (e.1 == 0 && e.3 == 0)
                || (e.1 == max && e.3 == max)
        };
        let interior1: Vec<_> = band1.iter().filter(|e| !on_border(e)).collect();
        assert!(!interior1.is_empty());
        for edge in &interior1 {
            assert!(band2.contains(edge), "sliver: {edge:?} missing from band 2");
        }
        for edge in band2.iter().filter(|e| !on_border(e)) {
            assert!(band1.contains(edge), "sliver: {edge:?} missing from band 1");
        }
    }

    #[test]
    fn interior_blob_boundary_matches_between_bands() {
        // A 2x2 blob of shade 1 inside shade 2: no junction anywhere, so the
        // blob outline and the surrounding hole must simplify identically.
        let mut labels = vec![2_i8; 36];
        for y in 2..4_usize {
            for x in 2..4_usize {
                labels[y * 6 + x] = 1;
            }
        }
        let polygons = polygonize_labels(labels, 6, 6);
        let blob = edge_set(&polygons[&1]);
        let mut hole = std::collections::BTreeSet::new();
        for polygon in &polygons[&2] {
            for ring in polygon.interiors() {
                ring_edges(ring, &mut hole);
            }
        }
        assert_eq!(blob, hole);
    }

    #[test]
    fn removes_single_cell_speckles() {
        let mut labels = vec![0; 9];
        labels[4] = 2;
        remove_speckles(&mut labels, 3, 3);
        assert!(labels.iter().all(|label| *label == 0));

        labels[0] = 2;
        remove_speckles(&mut labels, 3, 3);
        assert_eq!(labels[0], 2, "outer-edge regions must remain seam-stable");
    }

    #[test]
    fn boundary_regions_are_not_merged_differently_by_adjacent_tiles() {
        const WIDTH: usize = 10;
        const HEIGHT: usize = 10;
        let labels = |origin_x: i32| {
            (0..WIDTH * HEIGHT)
                .map(|index| {
                    let x = origin_x + (index % WIDTH) as i32;
                    let y = index / WIDTH;
                    if (3..=9).contains(&x) && y == 4 { 1 } else { 0 }
                })
                .collect::<Vec<_>>()
        };
        // Buffered tiles covering global x=-1..8 and x=7..16. The same region
        // has six visible cells on the left but only three on the right.
        let mut left = labels(-1);
        let mut right = labels(7);
        remove_speckles(&mut left, WIDTH, HEIGHT);
        remove_speckles(&mut right, WIDTH, HEIGHT);

        for global_x in [7, 8] {
            let left_x = (global_x + 1) as usize;
            let right_x = (global_x - 7) as usize;
            assert_eq!(
                left[4 * WIDTH + left_x],
                right[4 * WIDTH + right_x],
                "shared label differs at global x={global_x}"
            );
        }
    }

    #[test]
    fn adjacent_tiles_agree_on_shared_illumination() {
        // Two horizontally adjacent tiles over one continuous field must
        // quantize identical shade labels in their overlapping grid columns:
        // the deterministic-field property behind seamless hillshade edges.
        fn field(x: i32, y: i32) -> f32 {
            let (fx, fy) = (x as f32 * 0.07, y as f32 * 0.05);
            500.0 + 260.0 * (fx.sin() * fy.cos()) + 80.0 * ((2.1 * fx).cos() * (1.3 * fy).sin())
        }
        let size = 64_i32;
        let left = DemNeighborhood::synthetic(size as usize, field);
        let right = DemNeighborhood::synthetic(size as usize, move |x, y| field(x + size, y));
        let grid_left = illumination_labels(&left, 14, 6451);
        let grid_right = illumination_labels(&right, 14, 6451);

        // Grid columns overlap with a shift of GRID_SIZE.
        let mut compared = 0;
        for grid_x_left in [GRID_SIZE as i32 - 1, GRID_SIZE as i32] {
            let grid_x_right = grid_x_left - GRID_SIZE as i32;
            let column_left = (grid_x_left - grid_left.origin) as usize;
            let column_right = (grid_x_right - grid_right.origin) as usize;
            for row in 0..grid_left.height {
                assert_eq!(
                    grid_left.labels[row * grid_left.width + column_left],
                    grid_right.labels[row * grid_right.width + column_right],
                    "column {grid_x_left} row {row}"
                );
                let left_tone = grid_left.tones[row * grid_left.width + column_left];
                let right_tone = grid_right.tones[row * grid_right.width + column_right];
                assert!(
                    (left_tone - right_tone).abs() < 1e-5,
                    "continuous tone differs at column {grid_x_left} row {row}: {left_tone} vs {right_tone}"
                );
                compared += 1;
            }
        }
        assert!(compared > 0);
    }

    #[test]
    fn nodata_shade_bytes_are_neutral_not_black() {
        assert_eq!(shade_byte(f64::NAN), 128);
        assert_eq!(shade_byte(0.0), 128);
        assert_eq!(shade_byte(-1_000.0), 0);
        assert_eq!(shade_byte(1_000.0), 255);
    }
}

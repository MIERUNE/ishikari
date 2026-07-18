//! Terrarium DEM decoding and 3x3 neighborhood access.

use std::sync::Arc;

use anyhow::{Context, Result, bail};
use image::ImageFormat;

const MIN_VALID_ELEVATION: f32 = -12_000.0;
const MAX_VALID_ELEVATION: f32 = 9_000.0;
/// Maximum supported DEM source-tile edge (px). Mapterhorn Terrarium sources
/// are 256px or 512px; accepting larger inputs would multiply decode,
/// neighborhood, contour, and hillshade memory without a supported use case.
const MAX_DEM_TILE_DIM: u32 = 512;
const MAX_DEM_TILE_PIXELS: usize = MAX_DEM_TILE_DIM as usize * MAX_DEM_TILE_DIM as usize;
/// Maximum retained decoded bytes represented by a complete 3x3 neighborhood.
/// Decoder temporaries and generated-product buffers are separate, short-lived
/// allocations bounded by CPU concurrency.
pub const MAX_DEM_NEIGHBORHOOD_BYTES: usize =
    9 * (MAX_DEM_TILE_PIXELS * std::mem::size_of::<f32>() + std::mem::size_of::<DemTile>());

#[derive(Debug)]
pub struct DemTile {
    width: usize,
    height: usize,
    elevations: Vec<f32>,
}

impl DemTile {
    fn get(&self, x: usize, y: usize) -> f32 {
        self.elevations[y * self.width + x]
    }

    /// Approximate heap footprint, for cache weighing.
    pub fn byte_size(&self) -> usize {
        self.elevations.len() * std::mem::size_of::<f32>() + std::mem::size_of::<Self>()
    }
}

/// A center DEM tile plus its eight neighbors in row-major order. Tiles are
/// shared `Arc`s so decoded DEMs can live in a cross-product, cross-request
/// cache (neighboring derived tiles reuse six of the nine).
#[derive(Debug)]
pub struct DemNeighborhood {
    tiles: [Option<Arc<DemTile>>; 9],
    width: usize,
    height: usize,
}

impl DemNeighborhood {
    pub fn from_tiles(tiles: [Option<Arc<DemTile>>; 9]) -> Result<Self> {
        let center = tiles[4]
            .as_ref()
            .context("center Mapterhorn DEM tile is missing")?;
        let (width, height) = (center.width, center.height);
        if width < 3 || height < 3 {
            bail!("DEM tile is too small: {width}x{height}");
        }
        // Decode enforces this for external inputs; retain a defense-in-depth
        // invariant here for synthetic or future programmatic callers.
        if width != height {
            bail!("DEM tile is not square: {width}x{height}");
        }
        let mut retained_bytes = 0usize;
        for tile in tiles.iter().flatten() {
            if tile.width != width || tile.height != height {
                bail!(
                    "DEM neighborhood dimensions differ: expected {width}x{height}, got {}x{}",
                    tile.width,
                    tile.height
                );
            }
            let expected_pixels = tile
                .width
                .checked_mul(tile.height)
                .context("DEM tile dimensions overflow")?;
            if tile.elevations.len() != expected_pixels {
                bail!(
                    "DEM tile sample count differs: expected {expected_pixels}, got {}",
                    tile.elevations.len()
                );
            }
            retained_bytes = retained_bytes
                .checked_add(tile.byte_size())
                .context("DEM neighborhood byte size overflow")?;
        }
        if retained_bytes > MAX_DEM_NEIGHBORHOOD_BYTES {
            bail!(
                "DEM neighborhood exceeds the {}-byte decoded budget: {retained_bytes}",
                MAX_DEM_NEIGHBORHOOD_BYTES
            );
        }
        Ok(Self {
            tiles,
            width,
            height,
        })
    }

    pub fn width(&self) -> usize {
        self.width
    }

    pub fn height(&self) -> usize {
        self.height
    }

    /// Reads through a one-tile border. A missing sparse detail neighbor falls
    /// back to the center edge; the requested center tile itself is mandatory.
    pub(crate) fn get(&self, x: i32, y: i32) -> f32 {
        let width = self.width as i32;
        let height = self.height as i32;
        let (column, local_x) = tile_axis(x, width);
        let (row, local_y) = tile_axis(y, height);
        let index = ((row + 1) * 3 + column + 1) as usize;
        if let Some(tile) = &self.tiles[index] {
            return tile.get(local_x as usize, local_y as usize);
        }

        let center = self.tiles[4].as_ref().expect("center checked in decode");
        center.get(
            x.clamp(0, width - 1) as usize,
            y.clamp(0, height - 1) as usize,
        )
    }

    /// Converts pixel-center samples to a grid-point elevation by averaging the
    /// four adjacent samples, matching maplibre-contour's seam-safe input.
    pub(crate) fn grid_elevation(&self, x: i32, y: i32) -> f32 {
        let values = [
            self.get(x - 1, y - 1),
            self.get(x, y - 1),
            self.get(x - 1, y),
            self.get(x, y),
        ];
        let mut sum = 0.0;
        let mut count = 0;
        for value in values {
            if value.is_finite() {
                sum += value;
                count += 1;
            }
        }
        if count == 0 {
            f32::NAN
        } else {
            sum / count as f32
        }
    }
}

fn tile_axis(value: i32, size: i32) -> (i32, i32) {
    if value < 0 {
        (-1, value + size)
    } else if value >= size {
        (1, value - size)
    } else {
        (0, value)
    }
}

pub fn decode_terrarium(bytes: &[u8]) -> Result<DemTile> {
    // Cap decode dimensions so a crafted WebP cannot expand a small payload
    // beyond the supported 256/512px source contract.
    let mut reader = image::ImageReader::new(std::io::Cursor::new(bytes));
    reader.set_format(ImageFormat::WebP);
    let mut limits = image::Limits::default();
    limits.max_image_width = Some(MAX_DEM_TILE_DIM);
    limits.max_image_height = Some(MAX_DEM_TILE_DIM);
    // Defense in depth for decoder implementations that honor the non-strict
    // allocation limit. Strict dimensions remain the primary memory bound.
    limits.max_alloc = Some(16 * 1024 * 1024);
    reader.limits(limits);
    let image = reader.decode().context("decode Mapterhorn WebP")?;
    let (width, height) = (image.width(), image.height());
    if width != height {
        bail!("DEM tile is not square: {width}x{height}");
    }
    let pixels = (width as usize)
        .checked_mul(height as usize)
        .context("DEM tile dimensions overflow")?;
    if pixels > MAX_DEM_TILE_PIXELS {
        bail!("DEM tile exceeds the supported pixel budget: {width}x{height}");
    }
    let image = image.into_rgb8();
    let elevations = image
        .pixels()
        .map(|pixel| {
            let [r, g, b] = pixel.0;
            let elevation = f32::from(r) * 256.0 + f32::from(g) + f32::from(b) / 256.0 - 32_768.0;
            if (MIN_VALID_ELEVATION..=MAX_VALID_ELEVATION).contains(&elevation) {
                elevation
            } else {
                f32::NAN
            }
        })
        .collect();
    Ok(DemTile {
        width: width as usize,
        height: height as usize,
        elevations,
    })
}

#[cfg(test)]
impl DemNeighborhood {
    /// Builds a synthetic neighborhood from a global elevation function over
    /// center-tile pixel coordinates (neighbors continue the same field).
    pub(super) fn synthetic(size: usize, elevation: impl Fn(i32, i32) -> f32) -> Self {
        let tile = |col: i32, row: i32| DemTile {
            width: size,
            height: size,
            elevations: (0..size * size)
                .map(|i| {
                    let x = col * size as i32 + (i % size) as i32;
                    let y = row * size as i32 + (i / size) as i32;
                    elevation(x, y)
                })
                .collect(),
        };
        let mut tiles: [Option<Arc<DemTile>>; 9] = std::array::from_fn(|_| None);
        for row in -1_i32..=1 {
            for col in -1_i32..=1 {
                tiles[((row + 1) * 3 + col + 1) as usize] = Some(Arc::new(tile(col, row)));
            }
        }
        Self {
            tiles,
            width: size,
            height: size,
        }
    }
}

#[cfg(test)]
mod tests {
    use image::{DynamicImage, ImageBuffer, ImageFormat, Rgb};
    use std::io::Cursor;

    use super::*;

    fn webp_tile(rgb: [u8; 3], width: u32, height: u32) -> Vec<u8> {
        let image = DynamicImage::ImageRgb8(ImageBuffer::from_pixel(width, height, Rgb(rgb)));
        let mut bytes = Cursor::new(Vec::new());
        image.write_to(&mut bytes, ImageFormat::WebP).unwrap();
        bytes.into_inner()
    }

    #[test]
    fn decodes_terrarium_elevation() {
        // 128*256 + 10 + 128/256 - 32768 = 10.5m.
        let tile = decode_terrarium(&webp_tile([128, 10, 128], 3, 3)).unwrap();
        assert_eq!(tile.get(1, 1), 10.5);
    }

    #[test]
    fn reads_neighbor_and_falls_back_for_missing_neighbor() {
        let mut tiles: [Option<Arc<DemTile>>; 9] = std::array::from_fn(|_| None);
        tiles[4] = Some(Arc::new(
            decode_terrarium(&webp_tile([128, 0, 0], 3, 3)).unwrap(),
        ));
        tiles[5] = Some(Arc::new(
            decode_terrarium(&webp_tile([128, 1, 0], 3, 3)).unwrap(),
        ));
        let neighborhood = DemNeighborhood::from_tiles(tiles).unwrap();
        assert_eq!(neighborhood.get(3, 1), 1.0);
        assert_eq!(neighborhood.get(-1, 1), 0.0);
    }

    #[test]
    fn rejects_rectangular_dem_tiles_during_decode() {
        // Reject before the malformed tile can enter the decoded-DEM cache.
        for (width, height) in [(8, 3), (3, 8)] {
            let error = decode_terrarium(&webp_tile([128, 0, 0], width, height)).unwrap_err();
            assert!(error.to_string().contains("not square"), "{error:#}");
        }
    }

    #[test]
    fn decodes_the_maximum_supported_edge() {
        let tile = decode_terrarium(&webp_tile([128, 0, 0], MAX_DEM_TILE_DIM, MAX_DEM_TILE_DIM))
            .expect("maximum supported DEM");
        assert_eq!(tile.width, MAX_DEM_TILE_DIM as usize);
        assert_eq!(tile.elevations.len(), MAX_DEM_TILE_PIXELS);
    }

    #[test]
    fn rejects_dem_tiles_larger_than_the_supported_edge() {
        let oversized = MAX_DEM_TILE_DIM + 1;
        let error = decode_terrarium(&webp_tile([128, 0, 0], oversized, oversized)).unwrap_err();
        assert!(
            error.to_string().contains("decode Mapterhorn WebP"),
            "{error:#}"
        );
    }

    #[test]
    fn maximum_supported_neighborhood_fits_the_aggregate_budget() {
        let tile = Arc::new(DemTile {
            width: MAX_DEM_TILE_DIM as usize,
            height: MAX_DEM_TILE_DIM as usize,
            elevations: vec![0.0; MAX_DEM_TILE_PIXELS],
        });
        let tiles = std::array::from_fn(|_| Some(tile.clone()));
        let neighborhood = DemNeighborhood::from_tiles(tiles).expect("maximum neighborhood");
        assert_eq!(neighborhood.width(), MAX_DEM_TILE_DIM as usize);
    }
}

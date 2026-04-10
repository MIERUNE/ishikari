//! PMTiles format primitives, directory parsing, and low-level decoding helpers.

use std::io::Read;

use anyhow::{Context, Result, anyhow, bail};
use brotli::Decompressor;
use bytes::{Buf, Bytes};
use fast_hilbert::xy2h;
use flate2::read::GzDecoder;

const MAX_ZOOM: u8 = 31;
const MAX_TILE_ID: u64 = 6_148_914_691_236_517_204;

/// Tile payload bytes and the HTTP metadata needed to serve them.
pub struct TileData {
    pub bytes: Bytes,
    pub content_type: &'static str,
    pub content_encoding: Option<&'static str>,
}

/// Result of a tile lookup together with whether chunk cache satisfied it.
pub struct TileFetch {
    pub cache_hit: bool,
    pub tile: TileData,
}

/// XYZ tile coordinates validated against the PMTiles zoom range.
#[derive(Clone, Copy)]
pub struct TileCoord {
    z: u8,
    x: u32,
    y: u32,
}

impl TileCoord {
    /// Validates and constructs a tile coordinate.
    pub fn new(z: u8, x: u32, y: u32) -> Result<Self> {
        let extent = 1u64
            .checked_shl(u32::from(z))
            .context("invalid zoom level")?;
        if z > MAX_ZOOM || u64::from(x) >= extent || u64::from(y) >= extent {
            bail!("invalid tile coordinate z={z} x={x} y={y}");
        }
        Ok(Self { z, x, y })
    }
}

/// PMTiles Hilbert tile id.
#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub struct TileId(u64);

impl TileId {
    /// Validates and constructs a PMTiles tile id.
    pub fn new(value: u64) -> Result<Self> {
        if value > MAX_TILE_ID {
            bail!("invalid tile id {value}");
        }
        Ok(Self(value))
    }

    /// Returns the underlying integer tile id.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl From<TileCoord> for TileId {
    fn from(coord: TileCoord) -> Self {
        if coord.z == 0 {
            return Self(0);
        }
        let base = pyramid_size_before_zoom(coord.z);
        Self(base + xy2h(coord.x, coord.y, coord.z))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Compression {
    Unknown,
    None,
    Gzip,
    Brotli,
    Zstd,
}

impl Compression {
    /// Maps PMTiles compression to the HTTP Content-Encoding header value.
    pub fn content_encoding(self) -> Option<&'static str> {
        match self {
            Self::Gzip => Some("gzip"),
            Self::Brotli => Some("br"),
            Self::Zstd => Some("zstd"),
            Self::Unknown | Self::None => None,
        }
    }
}

impl TryFrom<u8> for Compression {
    type Error = anyhow::Error;

    /// Decodes a PMTiles compression enum from its on-disk integer value.
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::None),
            2 => Ok(Self::Gzip),
            3 => Ok(Self::Brotli),
            4 => Ok(Self::Zstd),
            _ => bail!("invalid PMTiles compression {value}"),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TileType {
    Unknown,
    Mvt,
    Png,
    Jpeg,
    Webp,
    Avif,
    Mlt,
}

impl TileType {
    /// Maps PMTiles tile types to the HTTP Content-Type header value.
    pub fn content_type(self) -> &'static str {
        match self {
            Self::Mvt => "application/vnd.mapbox-vector-tile",
            Self::Png => "image/png",
            Self::Jpeg => "image/jpeg",
            Self::Webp => "image/webp",
            Self::Avif => "image/avif",
            Self::Mlt => "application/vnd.maplibre-vector-tile",
            Self::Unknown => "application/octet-stream",
        }
    }

    /// Maps PMTiles tile types to the TileJSON format field.
    pub fn tilejson_format(self) -> Option<&'static str> {
        match self {
            Self::Mvt | Self::Mlt => Some("pbf"),
            Self::Png => Some("png"),
            Self::Jpeg => Some("jpg"),
            Self::Webp => Some("webp"),
            Self::Avif => Some("avif"),
            Self::Unknown => None,
        }
    }

    /// Maps PMTiles tile types to the TileJSON encoding field.
    pub fn tilejson_encoding(self) -> Option<&'static str> {
        match self {
            Self::Mvt => Some("mvt"),
            Self::Mlt => Some("mlt"),
            Self::Unknown | Self::Png | Self::Jpeg | Self::Webp | Self::Avif => None,
        }
    }
}

impl TryFrom<u8> for TileType {
    type Error = anyhow::Error;

    /// Decodes a PMTiles tile type from its on-disk integer value.
    fn try_from(value: u8) -> Result<Self> {
        match value {
            0 => Ok(Self::Unknown),
            1 => Ok(Self::Mvt),
            2 => Ok(Self::Png),
            3 => Ok(Self::Jpeg),
            4 => Ok(Self::Webp),
            5 => Ok(Self::Avif),
            6 => Ok(Self::Mlt),
            _ => bail!("invalid PMTiles tile type {value}"),
        }
    }
}

/// Parsed PMTiles archive header.
#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub version: u8,
    pub root_offset: u64,
    pub root_length: u64,
    pub metadata_offset: u64,
    pub metadata_length: u64,
    pub leaf_offset: u64,
    pub leaf_length: u64,
    pub data_offset: u64,
    pub data_length: u64,
    pub n_addressed_tiles: u64,
    pub n_tile_entries: u64,
    pub n_tile_contents: u64,
    pub clustered: bool,
    pub internal_compression: Compression,
    pub tile_compression: Compression,
    pub tile_type: TileType,
    pub min_zoom: u8,
    pub max_zoom: u8,
    pub min_longitude: f64,
    pub min_latitude: f64,
    pub max_longitude: f64,
    pub max_latitude: f64,
    pub center_zoom: u8,
    pub center_longitude: f64,
    pub center_latitude: f64,
}

impl Header {
    /// Parses the fixed-size PMTiles archive header.
    pub fn parse(mut bytes: Bytes) -> Result<Self> {
        let magic = bytes.split_to("PMTiles".len());
        if magic != "PMTiles".as_bytes() {
            bail!("invalid PMTiles magic");
        }

        let version = bytes.get_u8();
        let root_offset = bytes.get_u64_le();
        let root_length = bytes.get_u64_le();
        let metadata_offset = bytes.get_u64_le();
        let metadata_length = bytes.get_u64_le();
        let leaf_offset = bytes.get_u64_le();
        let leaf_length = bytes.get_u64_le();
        let data_offset = bytes.get_u64_le();
        let data_length = bytes.get_u64_le();
        let n_addressed_tiles = bytes.get_u64_le();
        let n_tile_entries = bytes.get_u64_le();
        let n_tile_contents = bytes.get_u64_le();
        let clustered = bytes.get_u8() != 0;
        let internal_compression = Compression::try_from(bytes.get_u8())?;
        let tile_compression = Compression::try_from(bytes.get_u8())?;
        let tile_type = TileType::try_from(bytes.get_u8())?;
        let min_zoom = bytes.get_u8();
        let max_zoom = bytes.get_u8();
        let min_longitude = read_coordinate_part(&mut bytes);
        let min_latitude = read_coordinate_part(&mut bytes);
        let max_longitude = read_coordinate_part(&mut bytes);
        let max_latitude = read_coordinate_part(&mut bytes);
        let center_zoom = bytes.get_u8();
        let center_longitude = read_coordinate_part(&mut bytes);
        let center_latitude = read_coordinate_part(&mut bytes);

        Ok(Self {
            version,
            root_offset,
            root_length,
            metadata_offset,
            metadata_length,
            leaf_offset,
            leaf_length,
            data_offset,
            data_length,
            n_addressed_tiles,
            n_tile_entries,
            n_tile_contents,
            clustered,
            internal_compression,
            tile_compression,
            tile_type,
            min_zoom,
            max_zoom,
            min_longitude,
            min_latitude,
            max_longitude,
            max_latitude,
            center_zoom,
            center_longitude,
            center_latitude,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Directory {
    pub entries: Vec<DirectoryEntry>,
}

impl Directory {
    /// Parses a PMTiles directory block after applying internal compression.
    pub fn parse(compression: Compression, bytes: Bytes) -> Result<Self> {
        let decompressed = decompress_bytes(compression, bytes)?;
        let mut buffer = decompressed.as_ref();
        let n_entries = usize::try_from(read_u64_varint(&mut buffer)?)
            .context("varint does not fit into usize")?;
        let mut entries = vec![DirectoryEntry::default(); n_entries];

        let mut next_tile_id = 0;
        for entry in &mut entries {
            next_tile_id += read_u64_varint(&mut buffer)?;
            entry.tile_id = next_tile_id;
        }
        for entry in &mut entries {
            entry.run_length = u32::try_from(read_u64_varint(&mut buffer)?)
                .context("varint does not fit into u32")?;
        }
        for entry in &mut entries {
            entry.length = u32::try_from(read_u64_varint(&mut buffer)?)
                .context("varint does not fit into u32")?;
        }

        let mut previous_entry: Option<&DirectoryEntry> = None;
        for entry in &mut entries {
            let offset = read_u64_varint(&mut buffer)?;
            entry.offset = if offset == 0 {
                let previous = previous_entry.context("invalid PMTiles directory entry")?;
                previous.offset + u64::from(previous.length)
            } else {
                offset - 1
            };
            previous_entry = Some(entry);
        }

        Ok(Self { entries })
    }

    /// Finds the directory entry that contains or references the requested tile id.
    pub fn find_tile_id(&self, tile_id: TileId) -> Option<(usize, &DirectoryEntry)> {
        match self
            .entries
            .binary_search_by(|entry| entry.tile_id.cmp(&tile_id.value()))
        {
            Ok(index) => self.entries.get(index).map(|entry| (index, entry)),
            Err(next_id) => {
                if next_id > 0 {
                    let previous = self.entries.get(next_id - 1)?;
                    if previous.is_leaf()
                        || (tile_id.value() - previous.tile_id) < u64::from(previous.run_length)
                    {
                        return Some((next_id - 1, previous));
                    }
                }
                None
            }
        }
    }

    /// Estimates the heap footprint of this directory for cache weighting.
    pub fn approx_byte_size(&self) -> usize {
        self.entries.capacity() * std::mem::size_of::<DirectoryEntry>()
    }
}

#[derive(Clone, Debug, Default)]
pub struct DirectoryEntry {
    pub tile_id: u64,
    pub offset: u64,
    pub length: u32,
    pub run_length: u32,
}

impl DirectoryEntry {
    /// Reports whether this entry points to a child leaf directory.
    pub fn is_leaf(&self) -> bool {
        self.run_length == 0
    }
}

/// Returns the number of Hilbert-addressed tiles before the given zoom level.
fn pyramid_size_before_zoom(z: u8) -> u64 {
    if z == 0 {
        return 0;
    }
    ((1_u64 << (u32::from(z) * 2)) - 1) / 3
}

/// Decompresses PMTiles payload bytes using the declared compression codec.
pub fn decompress_bytes(compression: Compression, bytes: Bytes) -> Result<Bytes> {
    match compression {
        Compression::None => Ok(bytes),
        Compression::Gzip => {
            let mut decoder = GzDecoder::new(bytes.as_ref());
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(Bytes::from(decompressed))
        }
        Compression::Brotli => {
            let mut decoder = Decompressor::new(bytes.as_ref(), 4096);
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed)?;
            Ok(Bytes::from(decompressed))
        }
        Compression::Zstd => Ok(Bytes::from(zstd::stream::decode_all(bytes.as_ref())?)),
        Compression::Unknown => Err(anyhow!("unsupported PMTiles compression")),
    }
}

/// Reads a fixed-point coordinate component from the PMTiles header.
fn read_coordinate_part(buf: &mut Bytes) -> f64 {
    f64::from(buf.get_i32_le()) / 10_000_000.0
}

/// Reads a base-128 varint from a PMTiles-encoded byte slice.
fn read_u64_varint(buf: &mut &[u8]) -> Result<u64> {
    let mut shift = 0;
    let mut value = 0u64;
    loop {
        let byte = *buf
            .first()
            .context("unexpected EOF while reading PMTiles varint")?;
        *buf = &buf[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
        if shift >= 64 {
            bail!("invalid PMTiles varint");
        }
    }
}

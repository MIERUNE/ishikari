//! Interned string types for shared identifiers.

use std::{fmt, ops::Deref};

use anyhow::{Result, bail};
use internment::ArcIntern;

/// Validated, interned tileset identifier.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TilesetId(ArcIntern<str>);

impl TilesetId {
    /// Creates a tileset id after validating it.
    pub fn try_new(value: &str) -> Result<Self> {
        validate_tileset_id(value)?;
        Ok(Self(ArcIntern::from(value)))
    }

    /// Creates a tileset id without validation (for internal/test use).
    pub fn new_unchecked(value: &str) -> Self {
        Self(ArcIntern::from(value))
    }

    /// Returns the tileset id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl AsRef<str> for TilesetId {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for TilesetId {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for TilesetId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<String> for TilesetId {
    type Error = anyhow::Error;

    fn try_from(value: String) -> Result<Self> {
        Self::try_new(&value)
    }
}

/// Validates a tileset identifier.
fn validate_tileset_id(tileset_id: &str) -> Result<()> {
    if tileset_id.is_empty() {
        bail!("tileset_id must not be empty");
    }
    if tileset_id
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Ok(());
    }
    bail!("tileset_id contains invalid characters");
}

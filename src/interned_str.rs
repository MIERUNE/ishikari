//! Interned string for shared identifiers

use std::{fmt, ops::Deref};

use internment::ArcIntern;

/// Shared, interned string value.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InternedStr(ArcIntern<str>);

/// Interned tileset identifier.
pub type TilesetId = InternedStr;

impl InternedStr {
    /// Interns a string for cheap cloning and cache-key reuse.
    pub fn new(value: &str) -> Self {
        Self(ArcIntern::from(value))
    }

    /// Returns the interned string as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns the byte length of the interned string.
    pub fn len(&self) -> usize {
        self.0.len()
    }
}

impl AsRef<str> for InternedStr {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl Deref for InternedStr {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.as_str()
    }
}

impl fmt::Display for InternedStr {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl From<&str> for InternedStr {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for InternedStr {
    fn from(value: String) -> Self {
        Self::new(&value)
    }
}

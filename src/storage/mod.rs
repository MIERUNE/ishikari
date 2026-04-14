//! Storage integrations for local chunked reads and peer forwarding.

mod chunked_store;
mod peer;
mod pmtiles;
mod resolver;
mod routing;

pub use resolver::{ResourceResolver, ResourceResolverConfig, TilesetError, TilesetInfo};

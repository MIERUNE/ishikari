//! Chunked byte-range planning, caching, and inflight fetch coordination.

mod cache;
mod coordinator;
mod fetcher;
mod store;

pub use fetcher::{BackendLatencyModel, ChunkFetchError};
pub(crate) use store::{ChunkReadSource, ChunkedStoreConfig};
pub use store::{ChunkedStore, validate_chunked_store_limits};

#[cfg(feature = "simulator-support")]
pub use coordinator::plan_chunk_fetch_ranges;
#[cfg(feature = "simulator-support")]
pub use fetcher::local_tileset_archive_paths;

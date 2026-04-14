//! Chunked byte-range planning, caching, and inflight fetch coordination.

mod cache;
mod coordinator;
mod fetcher;
mod store;

pub use fetcher::ChunkFetchError;
pub use store::ChunkedStore;

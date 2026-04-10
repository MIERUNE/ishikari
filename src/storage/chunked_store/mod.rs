//! Chunked byte-range planning, caching, and inflight fetch aggregation.

mod cache;
pub(crate) mod fetch_aggregator;
pub(crate) mod object_store;

pub use object_store::ChunkedStore;

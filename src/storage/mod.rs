//! Storage integrations for local chunked reads and peer forwarding.

pub mod chunked_store;
mod distributed;
mod peer;

pub use chunked_store::ChunkedStore;
pub use distributed::DistributedStorage;
pub(crate) use peer::{PeerBackend, PeerFetchError};

//! In-flight chunk fetch aggregation and waiter coordination.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::anyhow;
use object_store::Error as ObjectStoreError;
use thiserror::Error;
use tokio::{
    sync::{Mutex, oneshot},
    time,
};
use tracing::debug;

use crate::interned_str::InternedStr;

const FETCH_MERGE_WINDOW: Duration = Duration::from_millis(10);
const IMMEDIATE_CHUNK_INDEX: u64 = 0;

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    #[error("{0}")]
    Message(String),
}

impl From<ObjectStoreError> for ChunkFetchError {
    fn from(error: ObjectStoreError) -> Self {
        if matches!(error, ObjectStoreError::NotFound { .. }) {
            return Self::NotFound;
        }
        Self::Message(format!("failed to fetch chunk range from object store: {error}"))
    }
}

/// Backend capabilities required by the chunk fetch aggregator.
pub trait ChunkFetchBackend: Send + Sync + 'static {
    /// Returns the backend node identifier for logging.
    fn backend_node_id(&self) -> &str;

    /// Returns whether a chunk is already cached locally.
    fn chunk_present(&self, tileset_id: &InternedStr, chunk_index: u64) -> bool;

    /// Fetches a batch of chunks into the local chunk cache.
    fn fetch_chunk_batch(
        &self,
        tileset_id: &InternedStr,
        chunks: &BTreeSet<u64>,
        archive_len: u64,
        batch_age_ms: u64,
    ) -> impl std::future::Future<Output = std::result::Result<(), ChunkFetchError>> + Send;
}

/// Coordinates shared inflight chunk fetches.
#[derive(Clone, Default)]
pub struct FetchAggregator {
    states: Arc<Mutex<HashMap<InternedStr, TilesetFetchState>>>,
}

#[derive(Default)]
struct TilesetFetchState {
    pending_chunks: BTreeSet<u64>,
    inflight_chunks: BTreeSet<u64>,
    waiters: HashMap<u64, Vec<oneshot::Sender<Result<(), ChunkFetchError>>>>,
    task_running: bool,
    archive_len: u64,
    batch_started_at: Option<Instant>,
}

impl FetchAggregator {
    /// Ensures the required chunks are present locally, batching concurrent misses.
    pub async fn ensure_chunks<B>(
        &self,
        backend: B,
        tileset_id: &InternedStr,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<(), ChunkFetchError>
    where
        B: ChunkFetchBackend + Clone,
    {
        let mut receivers = Vec::new();

        {
            let mut states = self.states.lock().await;
            let state = states.entry(tileset_id.clone()).or_default();
            let created_new_batch = !state.task_running
                && state.pending_chunks.is_empty()
                && state.inflight_chunks.is_empty();
            state.archive_len = state.archive_len.max(archive_len);
            let mut newly_requested_chunks = BTreeSet::new();

            for &chunk_index in required_chunks {
                if backend.chunk_present(tileset_id, chunk_index) {
                    continue;
                }

                let (tx, rx) = oneshot::channel();
                state.waiters.entry(chunk_index).or_default().push(tx);
                if !state.inflight_chunks.contains(&chunk_index)
                    && state.pending_chunks.insert(chunk_index)
                {
                    newly_requested_chunks.insert(chunk_index);
                }
                receivers.push(rx);
            }

            if !newly_requested_chunks.is_empty() {
                if created_new_batch {
                    state.batch_started_at = Some(Instant::now());
                }
                debug!(
                    node_id = %backend.backend_node_id(),
                    tileset_id = %tileset_id,
                    requested_chunks = ?newly_requested_chunks,
                    pending_chunks = ?state.pending_chunks,
                    inflight_chunks = ?state.inflight_chunks,
                    "{}",
                    if created_new_batch {
                        "created chunk fetch batch"
                    } else {
                        "batched into pending chunk fetch batch"
                    }
                );
            }

            if !state.task_running && !state.pending_chunks.is_empty() {
                state.task_running = true;
                let flush_immediately =
                    created_new_batch && state.pending_chunks.contains(&IMMEDIATE_CHUNK_INDEX);
                let aggregator = self.clone();
                let tileset_id = tileset_id.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    aggregator.run(backend, tileset_id, flush_immediately).await;
                });
            }
        }

        for receiver in receivers {
            let result = receiver.await.map_err(|_| {
                ChunkFetchError::Message(anyhow!("chunk fetch waiter dropped").to_string())
            })?;
            result?;
        }

        Ok(())
    }

    /// Flushes a pending object batch into chunk fetches after the merge window.
    async fn run<B>(&self, backend: B, tileset_id: InternedStr, mut flush_immediately: bool)
    where
        B: ChunkFetchBackend + Clone,
    {
        loop {
            if flush_immediately {
                flush_immediately = false;
            } else {
                time::sleep(FETCH_MERGE_WINDOW).await;
            }

            let (chunks, archive_len, batch_age_ms) = {
                let mut states = self.states.lock().await;
                let Some(state) = states.get_mut(&tileset_id) else {
                    return;
                };
                if state.pending_chunks.is_empty() {
                    if state.inflight_chunks.is_empty() {
                        state.task_running = false;
                        states.remove(&tileset_id);
                        debug!(node_id = %backend.backend_node_id(), tileset_id = %tileset_id, "removed empty chunk fetch state");
                    }
                    return;
                }
                let chunks = std::mem::take(&mut state.pending_chunks);
                state.inflight_chunks.extend(chunks.iter().copied());
                let batch_age_ms = state
                    .batch_started_at
                    .map(|started_at| started_at.elapsed().as_millis() as u64)
                    .unwrap_or_default();
                (chunks, state.archive_len, batch_age_ms)
            };

            let result = backend
                .fetch_chunk_batch(&tileset_id, &chunks, archive_len, batch_age_ms)
                .await
                .map_err(|error| error.clone());

            let mut states = self.states.lock().await;
            let Some(state) = states.get_mut(&tileset_id) else {
                continue;
            };

            for &chunk_index in &chunks {
                state.inflight_chunks.remove(&chunk_index);
                if let Some(waiters) = state.waiters.remove(&chunk_index) {
                    for waiter in waiters {
                        let _ = waiter.send(result.clone());
                    }
                }
            }

            let waiter_count: usize = state.waiters.values().map(Vec::len).sum();
            debug!(
                node_id = %backend.backend_node_id(),
                tileset_id = %tileset_id,
                completed_chunks = ?chunks,
                fetch_succeeded = result.is_ok(),
                pending_chunks = ?state.pending_chunks,
                inflight_chunks = ?state.inflight_chunks,
                waiter_keys = state.waiters.len(),
                waiters = waiter_count,
                "completed chunk fetch batch"
            );

            if state.pending_chunks.is_empty() && state.inflight_chunks.is_empty() {
                state.task_running = false;
                state.batch_started_at = None;
                states.remove(&tileset_id);
                debug!(node_id = %backend.backend_node_id(), tileset_id = %tileset_id, "removed empty chunk fetch state");
                return;
            }

            if state.inflight_chunks.is_empty() {
                state.batch_started_at = Some(Instant::now());
            }
        }
    }
}

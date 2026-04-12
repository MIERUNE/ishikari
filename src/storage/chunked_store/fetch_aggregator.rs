//! In-flight chunk fetch aggregation and waiter coordination.

use std::{
    collections::{BTreeSet, HashMap},
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use thiserror::Error;
use tokio::{
    sync::{Mutex, oneshot},
    time,
};
use tracing::debug;

use crate::interned_str::TilesetId;

const FETCH_MERGE_WINDOW: Duration = Duration::from_millis(10);
const IMMEDIATE_CHUNK_INDEX: u64 = 0;
const MAX_CONCURRENT_FETCHES_PER_TILESET: usize = 32;

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    #[error("{0}")]
    Message(String),
}

/// Backend capabilities required by the chunk fetch aggregator.
pub trait ChunkFetchBackend: Send + Sync + 'static {
    /// Splits requested chunks into backend fetch groups.
    fn chunk_fetch_groups(&self, chunks: &BTreeSet<u64>) -> Vec<BTreeSet<u64>>;

    /// Fetches one chunk group into the local chunk cache.
    fn fetch_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunks: &BTreeSet<u64>,
        archive_len: u64,
    ) -> impl std::future::Future<Output = std::result::Result<(), ChunkFetchError>> + Send;
}

/// Coordinates shared inflight chunk fetches.
#[derive(Clone, Default)]
pub struct FetchAggregator {
    /// Per-tileset fetch state keyed by tileset id.
    tileset_states: Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
}

/// Inflight and pending fetch coordination state for a single tileset.
#[derive(Default)]
struct TilesetFetchState {
    /// Chunks queued for the next backend fetch batch.
    pending_chunks: BTreeSet<u64>,
    /// Chunks currently being fetched from the backend.
    inflight_chunks: BTreeSet<u64>,
    /// Per-chunk waiters that are released when the shared fetch completes.
    waiters: HashMap<u64, Vec<oneshot::Sender<Result<(), ChunkFetchError>>>>,
    /// Whether the per-tileset scheduler task is currently running.
    scheduler_running: bool,
    /// Number of backend fetches currently inflight for this tileset.
    inflight_batch_count: usize,
    archive_len: u64,
}

impl FetchAggregator {
    /// Fetches chunks for a tileset while coalescing concurrent requests.
    pub async fn fetch_chunks<B>(
        &self,
        backend: B,
        tileset_id: &TilesetId,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<(), ChunkFetchError>
    where
        B: ChunkFetchBackend + Clone,
    {
        let mut receivers = Vec::new();

        {
            let mut tileset_states = self.tileset_states.lock().await;
            let tileset_state = tileset_states.entry(tileset_id.clone()).or_default();
            let was_idle = !tileset_state.scheduler_running
                && tileset_state.pending_chunks.is_empty()
                && tileset_state.inflight_chunks.is_empty()
                && tileset_state.inflight_batch_count == 0;
            tileset_state.archive_len = tileset_state.archive_len.max(archive_len);

            for &chunk_index in required_chunks {
                let (tx, rx) = oneshot::channel();
                // Each caller waits on its own oneshot, but the actual backend fetch is shared.
                tileset_state
                    .waiters
                    .entry(chunk_index)
                    .or_default()
                    .push(tx);
                if !tileset_state.inflight_chunks.contains(&chunk_index) {
                    tileset_state.pending_chunks.insert(chunk_index);
                }
                receivers.push(rx);
            }

            if !tileset_state.scheduler_running && !tileset_state.pending_chunks.is_empty() {
                tileset_state.scheduler_running = true;
                let flush_immediately = was_idle
                    && tileset_state
                        .pending_chunks
                        .contains(&IMMEDIATE_CHUNK_INDEX);
                let aggregator = self.clone();
                let tileset_id = tileset_id.clone();
                let backend = backend.clone();
                // A single scheduler task wakes every merge window and starts new fetch batches
                // while this tileset has pending work and available inflight capacity.
                tokio::spawn(async move {
                    aggregator
                        .run_scheduler(backend, tileset_id, flush_immediately)
                        .await;
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

    /// Wakes every merge window and starts at most one new fetch batch per tick.
    async fn run_scheduler<B>(&self, backend: B, tileset_id: TilesetId, mut flush_immediately: bool)
    where
        B: ChunkFetchBackend + Clone,
    {
        loop {
            if flush_immediately {
                flush_immediately = false;
            } else {
                time::sleep(FETCH_MERGE_WINDOW).await;
            }

            let groups = {
                let mut tileset_states = self.tileset_states.lock().await;
                let Some(state) = tileset_states.get_mut(&tileset_id) else {
                    return;
                };
                if state.pending_chunks.is_empty() {
                    // No work is queued for the next tick, so the scheduler can stop. The
                    // tileset state remains alive while inflight batches are still running.
                    state.scheduler_running = false;
                    if state.inflight_batch_count == 0 {
                        tileset_states.remove(&tileset_id);
                        debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
                    }
                    return;
                }
                if state.inflight_batch_count >= MAX_CONCURRENT_FETCHES_PER_TILESET {
                    None
                } else {
                    let available_slots =
                        MAX_CONCURRENT_FETCHES_PER_TILESET - state.inflight_batch_count;
                    let selected_groups: Vec<_> = backend
                        .chunk_fetch_groups(&state.pending_chunks)
                        .into_iter()
                        .take(available_slots)
                        .collect();
                    if selected_groups.is_empty() {
                        None
                    } else {
                        for chunks in &selected_groups {
                            // Mark each scheduled group inflight while leaving unscheduled chunks
                            // in pending for a later merge window tick.
                            state.inflight_chunks.extend(chunks.iter().copied());
                            for &chunk_index in chunks {
                                state.pending_chunks.remove(&chunk_index);
                            }
                        }
                        state.inflight_batch_count += selected_groups.len();
                        Some((selected_groups, state.archive_len))
                    }
                }
            };

            let Some((groups, archive_len)) = groups else {
                continue;
            };

            for chunks in groups {
                let aggregator = self.clone();
                let tileset_id = tileset_id.clone();
                let backend = backend.clone();
                tokio::spawn(async move {
                    aggregator
                        .run_fetch_batch(backend, tileset_id, chunks, archive_len)
                        .await;
                });
            }
        }
    }

    /// Fetches one batch and releases all waiters for the covered chunks.
    async fn run_fetch_batch<B>(
        &self,
        backend: B,
        tileset_id: TilesetId,
        chunks: BTreeSet<u64>,
        archive_len: u64,
    ) where
        B: ChunkFetchBackend + Clone,
    {
        let result = backend
            .fetch_chunk_group(&tileset_id, &chunks, archive_len)
            .await;

        let mut tileset_states = self.tileset_states.lock().await;
        let Some(state) = tileset_states.get_mut(&tileset_id) else {
            return;
        };

        state.inflight_batch_count = state.inflight_batch_count.saturating_sub(1);
        for &chunk_index in &chunks {
            state.inflight_chunks.remove(&chunk_index);
            if let Some(waiters) = state.waiters.remove(&chunk_index) {
                // All callers waiting on this chunk observe the same fetch result.
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            }
        }

        let waiter_count: usize = state.waiters.values().map(Vec::len).sum();
        debug!(
            tileset_id = %tileset_id,
            completed_chunks = ?chunks,
            fetch_succeeded = result.is_ok(),
            pending_chunks = ?state.pending_chunks,
            inflight_chunks = ?state.inflight_chunks,
            inflight_batches = state.inflight_batch_count,
            waiter_keys = state.waiters.len(),
            waiters = waiter_count,
            "completed chunk fetch batch"
        );

        if !state.scheduler_running
            && state.pending_chunks.is_empty()
            && state.inflight_batch_count == 0
        {
            tileset_states.remove(&tileset_id);
            debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
        }
    }
}

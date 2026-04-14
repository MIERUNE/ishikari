//! In-flight chunk fetch coordination and waiter management.

use std::{
    collections::{BTreeSet, HashMap},
    ops::Range,
    sync::Arc,
    time::Duration,
};

use anyhow::anyhow;
use tokio::{
    sync::{Mutex, oneshot},
    time,
};
use tracing::debug;

use crate::interned::TilesetId;

use super::{
    fetcher::{ChunkFetchError, ChunkFetcher},
    store::ChunkedStore,
};

const FETCH_MERGE_WINDOW: Duration = Duration::from_millis(10);
const IMMEDIATE_CHUNK_INDEX: u64 = 0;
const MAX_CHUNK_GAP: u64 = 1;
const MAX_CONCURRENT_FETCHES_PER_TILESET: usize = 32;

/// Coordinates shared inflight chunk fetches.
#[derive(Clone)]
pub struct ChunkFetchCoordinator {
    fetcher: ChunkFetcher,
    max_fetch_chunks: u64,
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
    inflight_fetch_count: usize,
    archive_len: u64,
}

impl ChunkFetchCoordinator {
    pub fn new(fetcher: ChunkFetcher, max_fetch_chunks: u64) -> Self {
        Self {
            fetcher,
            max_fetch_chunks,
            tileset_states: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.fetcher.chunk_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.fetcher.received_bytes()
    }

    /// Fetches chunks for a tileset while coalescing concurrent requests.
    pub async fn fetch_chunks(
        &self,
        store: ChunkedStore,
        tileset_id: &TilesetId,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<(), ChunkFetchError> {
        let mut receivers = Vec::with_capacity(required_chunks.len());

        {
            let mut tileset_states = self.tileset_states.lock().await;
            let tileset_state = tileset_states.entry(tileset_id.clone()).or_default();
            let was_idle = !tileset_state.scheduler_running
                && tileset_state.pending_chunks.is_empty()
                && tileset_state.inflight_chunks.is_empty()
                && tileset_state.inflight_fetch_count == 0;
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
                let coordinator = self.clone();
                let tileset_id = tileset_id.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    coordinator
                        .run_scheduler(store, tileset_id, flush_immediately)
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

    async fn run_scheduler(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        mut flush_immediately: bool,
    ) {
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
                    state.scheduler_running = false;
                    if state.inflight_fetch_count == 0 {
                        tileset_states.remove(&tileset_id);
                        debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
                    }
                    return;
                }
                if state.inflight_fetch_count >= MAX_CONCURRENT_FETCHES_PER_TILESET {
                    None
                } else {
                    let available_slots =
                        MAX_CONCURRENT_FETCHES_PER_TILESET - state.inflight_fetch_count;
                    let selected_groups: Vec<_> = contiguous_chunk_ranges(
                        &state.pending_chunks,
                        self.max_fetch_chunks,
                        MAX_CHUNK_GAP,
                    )
                    .into_iter()
                    .take(available_slots)
                    .collect();
                    if selected_groups.is_empty() {
                        None
                    } else {
                        for chunk_range in &selected_groups {
                            state
                                .inflight_chunks
                                .extend(chunk_range.start..chunk_range.end);
                            for chunk_index in chunk_range.start..chunk_range.end {
                                state.pending_chunks.remove(&chunk_index);
                            }
                        }
                        state.inflight_fetch_count += selected_groups.len();
                        Some((selected_groups, state.archive_len))
                    }
                }
            };

            let Some((groups, archive_len)) = groups else {
                continue;
            };

            for chunk_range in groups {
                let coordinator = self.clone();
                let tileset_id = tileset_id.clone();
                let store = store.clone();
                tokio::spawn(async move {
                    coordinator
                        .run_fetch_chunk_group(store, tileset_id, chunk_range, archive_len)
                        .await;
                });
            }
        }
    }

    async fn run_fetch_chunk_group(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) {
        let result = self
            .fetcher
            .fetch_chunk_group(&tileset_id, chunk_range.clone(), archive_len)
            .await
            .and_then(|bytes| {
                store
                    .cache_chunk_group(&tileset_id, chunk_range.clone(), archive_len, bytes)
                    .map_err(|error| ChunkFetchError::Message(error.to_string()))
            });

        let mut tileset_states = self.tileset_states.lock().await;
        let Some(state) = tileset_states.get_mut(&tileset_id) else {
            return;
        };

        state.inflight_fetch_count = state.inflight_fetch_count.saturating_sub(1);
        for chunk_index in chunk_range.start..chunk_range.end {
            state.inflight_chunks.remove(&chunk_index);
            if let Some(waiters) = state.waiters.remove(&chunk_index) {
                for waiter in waiters {
                    let _ = waiter.send(result.clone());
                }
            }
        }

        let waiter_count: usize = state.waiters.values().map(Vec::len).sum();
        debug!(
            tileset_id = %tileset_id,
            start_chunk = chunk_range.start,
            end_chunk = chunk_range.end,
            fetch_succeeded = result.is_ok(),
            pending_chunks = ?state.pending_chunks,
            inflight_chunks = ?state.inflight_chunks,
            inflight_fetches = state.inflight_fetch_count,
            waiter_keys = state.waiters.len(),
            waiters = waiter_count,
            "completed chunk fetch group"
        );

        if !state.scheduler_running
            && state.pending_chunks.is_empty()
            && state.inflight_fetch_count == 0
        {
            tileset_states.remove(&tileset_id);
            debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
        }
    }
}

fn contiguous_chunk_ranges(
    chunks: &BTreeSet<u64>,
    max_fetch_chunks: u64,
    max_chunk_gap: u64,
) -> Vec<Range<u64>> {
    let mut ranges = Vec::new();
    let mut iter = chunks.iter().copied();
    let Some(mut start) = iter.next() else {
        return ranges;
    };
    let max_fetch_chunks = max_fetch_chunks.max(1);
    let mut end = start + 1;

    for chunk in iter {
        if chunk <= end + max_chunk_gap && chunk + 1 - start <= max_fetch_chunks {
            end = chunk + 1;
            continue;
        }
        ranges.push(start..end);
        start = chunk;
        end = chunk + 1;
    }
    ranges.push(start..end);
    ranges
}

//! In-flight chunk fetch coordination and waiter management.

use std::{
    collections::{BTreeSet, HashMap},
    ops::Range,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::anyhow;
use bytes::Bytes;
use tokio::{
    sync::{Mutex, Notify, oneshot},
    time::{self, Instant},
};
use tracing::debug;

use crate::{interned::TilesetId, metrics::NodeMetrics};

use super::{
    fetcher::{ChunkFetchError, ChunkFetcher},
    store::ChunkedStore,
};

const IMMEDIATE_CHUNK_INDEX: u64 = 0;
const MAX_CHUNK_GAP: u64 = 1;
const MAX_CONCURRENT_FETCHES_PER_TILESET: usize = 32;
/// Total time a single admitted request may spend waiting for its chunks
/// (queue + dispatch + backend read). Counts queue time, unlike the per-range
/// backend timeout that starts only after an execution permit is acquired, so a
/// request cannot wait behind a saturated backend without a deadline.
const REQUEST_FETCH_DEADLINE: Duration = Duration::from_secs(30);

/// Coordinates shared inflight chunk fetches.
#[derive(Clone)]
pub struct ChunkFetchCoordinator {
    fetcher: ChunkFetcher,
    metrics: NodeMetrics,
    max_fetch_chunks: u64,
    merge_window: Duration,
    /// Per-tileset fetch state keyed by tileset id.
    tileset_states: Arc<Mutex<HashMap<TilesetId, TilesetFetchState>>>,
    /// Count of admitted (in-flight) `fetch_chunks` requests, and the ceiling
    /// above which new requests are shed. Backend *execution* concurrency is
    /// bounded separately; this bounds admitted work — coordinator state,
    /// waiters, and detached tasks — so distinct cold tilesets cannot grow it
    /// without limit under a slow backend.
    admitted_fetches: Arc<AtomicUsize>,
    max_admitted_fetches: usize,
}

/// RAII reservation in the admitted-fetch counter. Reserving fails (a shed)
/// when the counter is already at its ceiling. Sender-side waiters retain it
/// across request cancellation, and it releases when the associated detached
/// work has actually delivered or discarded every result.
struct AdmittedFetchGuard {
    admitted: Arc<AtomicUsize>,
}

impl AdmittedFetchGuard {
    fn reserve(admitted: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = admitted.fetch_add(1, Ordering::AcqRel);
        if previous >= max {
            admitted.fetch_sub(1, Ordering::AcqRel);
            None
        } else {
            Some(Self {
                admitted: Arc::clone(admitted),
            })
        }
    }
}

impl Drop for AdmittedFetchGuard {
    fn drop(&mut self) {
        self.admitted.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Inflight and pending fetch coordination state for a single tileset.
#[derive(Default)]
struct TilesetFetchState {
    /// Chunks queued for the next backend fetch batch.
    pending_chunks: BTreeSet<u64>,
    /// Time when the current pending set first became non-empty.
    first_pending_at: Option<Instant>,
    /// Chunks currently being fetched from the backend.
    inflight_chunks: BTreeSet<u64>,
    /// Per-chunk waiters that are released when the shared fetch completes.
    waiters: HashMap<u64, Vec<ChunkWaiter>>,
    /// Whether the per-tileset scheduler task is currently running.
    scheduler_running: bool,
    /// Number of backend fetches currently inflight for this tileset.
    inflight_fetch_count: usize,
    /// Wakes the scheduler when an inflight fetch releases a per-tileset slot.
    capacity_available: Arc<Notify>,
    archive_len: u64,
}

/// How a newly requested chunk joined the fetch state, for the wait metric.
enum EnqueueOutcome {
    /// Chunk is already being fetched; the caller joins that inflight fetch.
    JoinedInflight,
    /// Chunk was newly queued for the next batch.
    Queued,
    /// Chunk was already queued by another caller; this caller joins it.
    JoinedPending,
}

struct EnqueuedChunk {
    chunk_index: u64,
    receiver: oneshot::Receiver<Result<Bytes, ChunkFetchError>>,
    outcome: EnqueueOutcome,
}

/// Sender-side waiter that retains the request's admission reservation until
/// the detached backend work actually releases it. If the HTTP future times
/// out or is cancelled, dropping its receiver must not make the slot reusable
/// while the scheduler/fetch task is still queued.
struct ChunkWaiter {
    sender: oneshot::Sender<Result<Bytes, ChunkFetchError>>,
    _admission: Arc<AdmittedFetchGuard>,
}

impl EnqueueOutcome {
    fn metric_label(&self) -> &'static str {
        match self {
            Self::JoinedInflight => "joined_inflight",
            Self::Queued => "queued",
            Self::JoinedPending => "joined_pending",
        }
    }
}

/// Outcome of a scheduler pass asking the state what to dispatch next.
enum DispatchDecision {
    /// Nothing pending; the scheduler stops (state set non-running).
    Idle,
    /// Pending work exists but the concurrency cap leaves no slot this pass.
    Throttled(Arc<Notify>),
    /// Dispatch these contiguous chunk ranges; carries dispatch metric inputs.
    Dispatch {
        groups: Vec<Range<u64>>,
        archive_len: u64,
        queue_delay: Duration,
        pending_at_dispatch: usize,
    },
}

impl TilesetFetchState {
    /// Whether this tileset has no scheduled, pending, or inflight work — the
    /// idle state a fresh fetch can flush immediately from.
    fn is_idle(&self) -> bool {
        !self.scheduler_running
            && self.pending_chunks.is_empty()
            && self.inflight_chunks.is_empty()
            && self.inflight_fetch_count == 0
    }

    /// Whether the state holds no scheduled or inflight work and can be dropped
    /// from the coordinator map.
    fn is_drainable(&self) -> bool {
        !self.scheduler_running && self.pending_chunks.is_empty() && self.inflight_fetch_count == 0
    }

    /// Registers a waiter per requested chunk, queuing chunks not already
    /// inflight or pending. Returns each chunk's receiver and how it joined, so
    /// the caller can await it and record the wait metric.
    fn enqueue_chunks(
        &mut self,
        required_chunks: &[u64],
        queued_at: Instant,
        admission: &Arc<AdmittedFetchGuard>,
    ) -> Vec<EnqueuedChunk> {
        let mut joined = Vec::with_capacity(required_chunks.len());
        for &chunk_index in required_chunks {
            let (tx, rx) = oneshot::channel();
            // Each caller waits on its own oneshot, but the backend fetch is shared.
            self.waiters
                .entry(chunk_index)
                .or_default()
                .push(ChunkWaiter {
                    sender: tx,
                    _admission: Arc::clone(admission),
                });
            let outcome = if self.inflight_chunks.contains(&chunk_index) {
                EnqueueOutcome::JoinedInflight
            } else if self.pending_chunks.insert(chunk_index) {
                if self.first_pending_at.is_none() {
                    self.first_pending_at = Some(queued_at);
                }
                EnqueueOutcome::Queued
            } else {
                EnqueueOutcome::JoinedPending
            };
            joined.push(EnqueuedChunk {
                chunk_index,
                receiver: rx,
                outcome,
            });
        }
        joined
    }

    /// Selects the next batch of contiguous chunk ranges to dispatch, moving
    /// them from pending to inflight and reserving fetch slots. Stops the
    /// scheduler when nothing is pending.
    fn select_dispatch_groups(&mut self, max_fetch_chunks: u64) -> DispatchDecision {
        if self.pending_chunks.is_empty() {
            self.scheduler_running = false;
            return DispatchDecision::Idle;
        }
        if self.inflight_fetch_count >= MAX_CONCURRENT_FETCHES_PER_TILESET {
            return DispatchDecision::Throttled(Arc::clone(&self.capacity_available));
        }
        let available_slots = MAX_CONCURRENT_FETCHES_PER_TILESET - self.inflight_fetch_count;
        let groups: Vec<Range<u64>> =
            contiguous_chunk_ranges(&self.pending_chunks, max_fetch_chunks, MAX_CHUNK_GAP)
                .into_iter()
                .take(available_slots)
                .collect();
        if groups.is_empty() {
            return DispatchDecision::Throttled(Arc::clone(&self.capacity_available));
        }
        // Snapshot metric inputs before the chunks leave the pending set.
        let pending_at_dispatch = self.pending_chunks.len();
        let queue_delay = self
            .first_pending_at
            .map(|instant| instant.elapsed())
            .unwrap_or_default();
        for chunk_range in &groups {
            self.inflight_chunks
                .extend(chunk_range.start..chunk_range.end);
            for chunk_index in chunk_range.start..chunk_range.end {
                self.pending_chunks.remove(&chunk_index);
            }
        }
        if self.pending_chunks.is_empty() {
            self.first_pending_at = None;
        }
        self.inflight_fetch_count += groups.len();
        DispatchDecision::Dispatch {
            groups,
            archive_len: self.archive_len,
            queue_delay,
            pending_at_dispatch,
        }
    }

    /// Releases a finished fetch group: frees its slot, clears the inflight
    /// chunks, and delivers `result` to every waiter. Returns the number of
    /// waiters released (for the group-waiters metric).
    fn complete_group(
        &mut self,
        chunk_range: Range<u64>,
        result: &Result<HashMap<u64, Bytes>, ChunkFetchError>,
    ) -> usize {
        let scheduler_needs_capacity = self.inflight_fetch_count
            >= MAX_CONCURRENT_FETCHES_PER_TILESET
            && !self.pending_chunks.is_empty();
        self.inflight_fetch_count = self.inflight_fetch_count.saturating_sub(1);
        if scheduler_needs_capacity {
            self.capacity_available.notify_one();
        }
        let mut released_waiters = 0;
        for chunk_index in chunk_range.start..chunk_range.end {
            self.inflight_chunks.remove(&chunk_index);
            if let Some(waiters) = self.waiters.remove(&chunk_index) {
                released_waiters += waiters.len();
                let chunk_result = match result {
                    Ok(chunks) => chunks.get(&chunk_index).cloned().ok_or_else(|| {
                        ChunkFetchError::Message(format!(
                            "fetched group omitted chunk {chunk_index}"
                        ))
                    }),
                    Err(error) => Err(error.clone()),
                };
                for waiter in waiters {
                    let _ = waiter.sender.send(chunk_result.clone());
                }
            }
        }
        released_waiters
    }
}

impl ChunkFetchCoordinator {
    pub fn new(
        fetcher: ChunkFetcher,
        max_fetch_chunks: u64,
        merge_window: Duration,
        max_admitted_fetches: usize,
        metrics: NodeMetrics,
    ) -> Self {
        metrics.set_chunk_fetch_merge_window(merge_window);
        Self {
            fetcher,
            metrics,
            max_fetch_chunks,
            merge_window,
            tileset_states: Arc::new(Mutex::new(HashMap::new())),
            admitted_fetches: Arc::new(AtomicUsize::new(0)),
            max_admitted_fetches: max_admitted_fetches.max(1),
        }
    }

    pub fn chunk_size(&self) -> u64 {
        self.fetcher.chunk_size()
    }

    pub fn received_bytes(&self) -> u64 {
        self.fetcher.received_bytes()
    }

    pub fn metrics(&self) -> &NodeMetrics {
        &self.metrics
    }

    /// Fetches chunks for a tileset while coalescing concurrent requests.
    pub async fn fetch_chunks(
        &self,
        store: ChunkedStore,
        tileset_id: &TilesetId,
        required_chunks: &[u64],
        archive_len: u64,
    ) -> std::result::Result<HashMap<u64, Bytes>, ChunkFetchError> {
        // Reserve an admitted-fetch slot before registering any coordinator
        // state, so a flood of distinct cold tilesets cannot grow the state
        // map, waiter set, and detached tasks without bound under a slow
        // backend. Overload is shed (503), not queued. The guard is held until
        // this request's chunks resolve and releases on any early return.
        let admission = Arc::new(
            AdmittedFetchGuard::reserve(&self.admitted_fetches, self.max_admitted_fetches)
                .ok_or_else(|| {
                    ChunkFetchError::Overload("backend fetch admission is saturated".into())
                })?,
        );

        let mut receivers = Vec::with_capacity(required_chunks.len());
        let queued_at = Instant::now();

        {
            let mut tileset_states = self.tileset_states.lock().await;
            let tileset_state = tileset_states.entry(tileset_id.clone()).or_default();
            let was_idle = tileset_state.is_idle();
            tileset_state.archive_len = tileset_state.archive_len.max(archive_len);

            for enqueued in tileset_state.enqueue_chunks(required_chunks, queued_at, &admission) {
                self.metrics
                    .record_chunk_fetch_wait(enqueued.outcome.metric_label());
                receivers.push((enqueued.chunk_index, enqueued.receiver));
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

        let mut chunks = HashMap::with_capacity(receivers.len());
        // Bound the total wait (queue + dispatch + backend read) from enqueue so
        // a request cannot block indefinitely behind a saturated backend.
        let collect = async {
            for (chunk_index, receiver) in receivers {
                let result = receiver.await.map_err(|_| {
                    ChunkFetchError::Message(anyhow!("chunk fetch waiter dropped").to_string())
                })?;
                chunks.insert(chunk_index, result?);
            }
            Ok(chunks)
        };
        match time::timeout(REQUEST_FETCH_DEADLINE, collect).await {
            Ok(result) => result,
            Err(_) => Err(ChunkFetchError::Timeout(format!(
                "backend chunk fetch exceeded the {}s request deadline",
                REQUEST_FETCH_DEADLINE.as_secs()
            ))),
        }
    }

    async fn run_scheduler(
        &self,
        store: ChunkedStore,
        tileset_id: TilesetId,
        mut flush_immediately: bool,
    ) {
        loop {
            let flushed_immediately = flush_immediately;
            if flush_immediately {
                flush_immediately = false;
            } else {
                time::sleep(self.merge_window).await;
            }

            let (dispatch, capacity_available) = {
                let mut tileset_states = self.tileset_states.lock().await;
                let Some(state) = tileset_states.get_mut(&tileset_id) else {
                    return;
                };
                match state.select_dispatch_groups(self.max_fetch_chunks) {
                    DispatchDecision::Idle => {
                        if state.is_drainable() {
                            tileset_states.remove(&tileset_id);
                            debug!(tileset_id = %tileset_id, "removed empty chunk fetch state");
                        }
                        return;
                    }
                    DispatchDecision::Throttled(capacity_available) => {
                        (None, Some(capacity_available))
                    }
                    DispatchDecision::Dispatch {
                        groups,
                        archive_len,
                        queue_delay,
                        pending_at_dispatch,
                    } => {
                        let flush_label = if flushed_immediately {
                            "immediate"
                        } else {
                            "window"
                        };
                        self.metrics.record_chunk_fetch_dispatch(
                            flush_label,
                            queue_delay,
                            pending_at_dispatch,
                        );
                        (Some((groups, archive_len)), None)
                    }
                }
            };

            if let Some(capacity_available) = capacity_available {
                capacity_available.notified().await;
                flush_immediately = true;
                continue;
            }

            let Some((groups, archive_len)) = dispatch else {
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

        let released_waiters = state.complete_group(chunk_range.clone(), &result);
        self.metrics.record_chunk_fetch_group_waiters(
            if result.is_ok() { "success" } else { "error" },
            released_waiters,
        );

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

        if state.is_drainable() {
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

/// Plans the backend ranges used by the production chunk coordinator.
#[cfg(feature = "simulator-support")]
pub fn plan_chunk_fetch_ranges(chunks: &BTreeSet<u64>, max_fetch_chunks: u64) -> Vec<Range<u64>> {
    contiguous_chunk_ranges(chunks, max_fetch_chunks, MAX_CHUNK_GAP)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::*;

    fn set(values: &[u64]) -> BTreeSet<u64> {
        values.iter().copied().collect()
    }

    fn test_admission() -> Arc<AdmittedFetchGuard> {
        Arc::new(
            AdmittedFetchGuard::reserve(&Arc::new(AtomicUsize::new(0)), 1).expect("test admission"),
        )
    }

    #[test]
    fn admitted_fetch_guard_sheds_above_the_ceiling_and_releases_on_drop() {
        let admitted = Arc::new(AtomicUsize::new(0));
        let first = AdmittedFetchGuard::reserve(&admitted, 2).expect("first admitted");
        let second = AdmittedFetchGuard::reserve(&admitted, 2).expect("second admitted");
        assert_eq!(admitted.load(Ordering::Acquire), 2);
        // At the ceiling: the next request is shed, and the failed reservation
        // does not leak a slot.
        assert!(AdmittedFetchGuard::reserve(&admitted, 2).is_none());
        assert_eq!(admitted.load(Ordering::Acquire), 2);
        drop(second);
        assert_eq!(admitted.load(Ordering::Acquire), 1);
        // A slot freed by drop is immediately reusable.
        let third = AdmittedFetchGuard::reserve(&admitted, 2).expect("slot reused after drop");
        assert_eq!(admitted.load(Ordering::Acquire), 2);
        drop(first);
        drop(third);
        assert_eq!(admitted.load(Ordering::Acquire), 0);
    }

    #[test]
    fn cancelled_receiver_keeps_admission_until_detached_work_finishes() {
        let admitted = Arc::new(AtomicUsize::new(0));
        let admission =
            Arc::new(AdmittedFetchGuard::reserve(&admitted, 1).expect("request is admitted"));
        let mut state = TilesetFetchState::default();
        let receivers = state.enqueue_chunks(&[7], Instant::now(), &admission);

        // Model an HTTP timeout/cancellation: both the request-side receiver
        // and its local guard disappear while the detached fetch remains.
        drop(receivers);
        drop(admission);
        assert_eq!(admitted.load(Ordering::Acquire), 1);
        assert!(AdmittedFetchGuard::reserve(&admitted, 1).is_none());

        let _ = state.select_dispatch_groups(1);
        state.complete_group(7..8, &Ok(HashMap::from([(7, Bytes::new())])));
        assert_eq!(admitted.load(Ordering::Acquire), 0);
        assert!(AdmittedFetchGuard::reserve(&admitted, 1).is_some());
    }

    #[test]
    fn enqueue_queues_new_chunks_then_joins_existing() {
        let mut state = TilesetFetchState::default();
        let now = Instant::now();

        let admission = test_admission();
        let queued = state.enqueue_chunks(&[5], now, &admission);
        assert!(matches!(queued[0].outcome, EnqueueOutcome::Queued));
        assert!(state.pending_chunks.contains(&5));
        assert!(state.first_pending_at.is_some());

        // A second waiter for the same still-pending chunk joins it.
        let joined = state.enqueue_chunks(&[5], now, &admission);
        assert!(matches!(joined[0].outcome, EnqueueOutcome::JoinedPending));

        // A chunk already inflight is joined, not re-queued.
        state.inflight_chunks.insert(9);
        let inflight = state.enqueue_chunks(&[9], now, &admission);
        assert!(matches!(
            inflight[0].outcome,
            EnqueueOutcome::JoinedInflight
        ));
        assert!(!state.pending_chunks.contains(&9));
    }

    #[test]
    fn select_dispatch_moves_pending_to_inflight() {
        let mut state = TilesetFetchState::default();
        state.enqueue_chunks(&[1, 2, 3], Instant::now(), &test_admission());

        let DispatchDecision::Dispatch { groups, .. } = state.select_dispatch_groups(4) else {
            panic!("expected a dispatch");
        };
        assert_eq!(groups, vec![1..4]);
        assert!(state.pending_chunks.is_empty());
        assert_eq!(state.inflight_chunks, set(&[1, 2, 3]));
        assert_eq!(state.inflight_fetch_count, 1);
        assert!(state.first_pending_at.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn queue_delay_follows_tokio_virtual_time() {
        let mut state = TilesetFetchState::default();
        state.enqueue_chunks(&[1], Instant::now(), &test_admission());

        time::advance(Duration::from_millis(10)).await;

        let DispatchDecision::Dispatch { queue_delay, .. } = state.select_dispatch_groups(4) else {
            panic!("expected a dispatch");
        };
        assert_eq!(queue_delay, Duration::from_millis(10));
    }

    #[test]
    fn select_dispatch_idle_stops_scheduler() {
        let mut state = TilesetFetchState {
            scheduler_running: true,
            ..Default::default()
        };
        assert!(matches!(
            state.select_dispatch_groups(4),
            DispatchDecision::Idle
        ));
        assert!(!state.scheduler_running);
        assert!(state.is_drainable());
    }

    #[test]
    fn complete_group_releases_waiters_and_drains() {
        let mut state = TilesetFetchState::default();
        let mut receivers: Vec<_> = state
            .enqueue_chunks(&[1, 2], Instant::now(), &test_admission())
            .into_iter()
            .map(|enqueued| enqueued.receiver)
            .collect();
        let _ = state.select_dispatch_groups(4);

        let chunks = HashMap::from([
            (1, Bytes::from_static(b"chunk one")),
            (2, Bytes::from_static(b"chunk two")),
        ]);
        let released = state.complete_group(1..3, &Ok(chunks));
        assert_eq!(released, 2);
        assert_eq!(state.inflight_fetch_count, 0);
        assert!(state.inflight_chunks.is_empty());
        assert_eq!(
            receivers[0].try_recv().expect("chunk 1").expect("ok"),
            "chunk one"
        );
        assert_eq!(
            receivers[1].try_recv().expect("chunk 2").expect("ok"),
            "chunk two"
        );
        assert!(state.is_drainable());
    }

    #[tokio::test]
    async fn completing_fetch_wakes_capacity_waiter() {
        let mut state = TilesetFetchState {
            scheduler_running: true,
            inflight_fetch_count: MAX_CONCURRENT_FETCHES_PER_TILESET,
            ..Default::default()
        };
        state.inflight_chunks.insert(1);
        state.enqueue_chunks(&[2], Instant::now(), &test_admission());

        let DispatchDecision::Throttled(capacity_available) = state.select_dispatch_groups(4)
        else {
            panic!("expected capacity throttling");
        };
        let notified = capacity_available.notified();

        state.complete_group(1..2, &Ok(HashMap::from([(1, Bytes::new())])));
        tokio::time::timeout(Duration::from_secs(1), notified)
            .await
            .expect("fetch completion must wake the scheduler");
    }

    #[test]
    fn groups_contiguous_chunks_into_one_backend_range() {
        assert_eq!(contiguous_chunk_ranges(&set(&[2, 3, 4]), 4, 1), vec![2..5]);
    }

    #[test]
    fn prefetches_across_small_chunk_gaps() {
        assert_eq!(contiguous_chunk_ranges(&set(&[2, 4]), 4, 1), vec![2..5]);
    }

    #[test]
    fn respects_max_fetch_chunks_even_when_gaps_are_mergeable() {
        assert_eq!(
            contiguous_chunk_ranges(&set(&[2, 4, 6]), 4, 1),
            vec![2..5, 6..7]
        );
    }

    #[test]
    fn splits_large_gaps() {
        assert_eq!(
            contiguous_chunk_ranges(&set(&[2, 5]), 4, 1),
            vec![2..3, 5..6]
        );
    }
}

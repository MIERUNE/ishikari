//! Prometheus-backed node metrics.
//!
//! Counters are incremented at the call sites; point-in-time gauges (cache
//! sizes, membership, drain state, cumulative backend bytes) are refreshed at
//! scrape time by the `/_internal/metrics` handler. Labels never contain
//! attacker-controlled values such as `tileset_id`; only bounded route
//! patterns and status codes are used.

use std::{sync::Arc, time::Duration};

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    IntGaugeVec, Opts, Registry, TextEncoder, core::Collector,
};

const BACKEND_OUTCOMES: &[&str] = &["success", "not_found", "error", "timeout"];

/// Cloneable handle to the node's Prometheus registry and metric families.
#[derive(Clone)]
pub struct NodeMetrics(Arc<Inner>);

macro_rules! define_metrics_inner {
    ($($field:ident: $metric:ty),+ $(,)?) => {
        struct Inner {
            registry: Registry,
            $($field: $metric,)+
        }

        impl Inner {
            fn register_collectors(&self) {
                $(self.registry
                    .register(Box::new(self.$field.clone()))
                    .expect("unique metric");)+
            }
        }
    };
}

define_metrics_inner!(
    egress_bytes: IntCounter,
    internal_bytes: IntCounter,
    http_requests: IntCounterVec,
    http_request_duration: HistogramVec,
    tiles_served: IntCounterVec,
    tile_cache: IntCounterVec,
    tile_negative_cache_hits: IntCounter,
    peer_forward: IntCounterVec,
    peer_fetch: IntCounterVec,
    peer_fetch_duplicate_inflight: IntCounterVec,
    internal_resource_requests: IntCounterVec,
    provider_resource_cache: IntCounterVec,
    mapterhorn_resolve: IntCounterVec,
    cache_bytes: IntGaugeVec,
    backend_fetch_bytes: IntCounter,
    backend_fetch_duration: HistogramVec,
    backend_fetch_size_bytes: HistogramVec,
    backend_fetch_chunks: HistogramVec,
    backend_fetch_queue_duration: Histogram,
    backend_fetch_concurrency: IntGaugeVec,
    chunk_size_bytes: IntGauge,
    max_fetch_chunks: IntGauge,
    chunk_fetch_merge_window_seconds: Gauge,
    chunk_fetch_queue_delay: HistogramVec,
    chunk_fetch_pending_chunks: HistogramVec,
    chunk_fetch_group_waiters: HistogramVec,
    chunk_cache: IntCounterVec,
    chunk_fetch_wait: IntCounterVec,
    cpu_work_admission: IntCounterVec,
    cpu_work_queue_duration: HistogramVec,
    cpu_work: IntGaugeVec,
    terrain_source_duration: HistogramVec,
    terrain_generation_duration: HistogramVec,
    terrain_source_tiles: HistogramVec,
    terrain_output_size_bytes: HistogramVec,
    membership_size: IntGaugeVec,
    drain_state: IntGauge,
);

macro_rules! define_node_metrics_snapshot {
    ($($field:ident),+ $(,)?) => {
        /// Point-in-time counters used by tests and the in-process simulator.
        #[derive(Debug, Clone, Copy, Default, Eq, PartialEq, serde::Serialize)]
        pub struct NodeMetricsSnapshot {
            $(pub $field: u64,)+
        }

        impl NodeMetricsSnapshot {
            /// Adds another point-in-time snapshot to this aggregate.
            pub fn merge(&mut self, other: &Self) {
                $(self.$field += other.$field;)+
            }
        }
    };
}

define_node_metrics_snapshot!(
    negative_cache_hits,
    peer_forward_successes,
    peer_forward_not_found,
    peer_forward_retryable,
    peer_forward_fatal,
    peer_forward_backoff_skips,
    peer_tile_fetches,
    peer_bootstrap_fetches,
    peer_leaf_fetches,
    peer_provider_fetches,
    peer_tile_duplicate_inflight,
    peer_bootstrap_duplicate_inflight,
    peer_leaf_duplicate_inflight,
    peer_provider_duplicate_inflight,
    internal_tile_requests,
    internal_bootstrap_requests,
    internal_leaf_requests,
    internal_provider_requests,
    backend_fetches,
    backend_fetch_successes,
    backend_fetch_not_found,
    backend_fetch_errors,
    backend_fetch_timeouts,
    backend_fetched_chunks,
    chunk_cache_hits,
    chunk_cache_misses,
    chunk_cache_post_fetch_hits,
    chunk_fetch_queued,
    chunk_fetch_joined_pending,
    chunk_fetch_joined_inflight,
    chunk_dispatch_immediate,
    chunk_dispatch_window,
    chunk_dispatch_pending_chunks,
    chunk_waiters_released,
);

/// One Prometheus histogram captured as mergeable cumulative buckets.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct HistogramSnapshot {
    pub count: u64,
    pub sum: f64,
    pub buckets: Vec<HistogramBucketSnapshot>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, serde::Serialize)]
pub struct HistogramBucketSnapshot {
    pub upper_bound: f64,
    pub cumulative_count: u64,
}

impl HistogramSnapshot {
    /// Adds another histogram with the same bucket layout.
    pub fn merge(&mut self, other: &Self) {
        if self.buckets.is_empty() {
            self.buckets = other.buckets.clone();
        } else {
            debug_assert_eq!(self.buckets.len(), other.buckets.len());
            for (target, source) in self.buckets.iter_mut().zip(&other.buckets) {
                debug_assert_eq!(target.upper_bound, source.upper_bound);
                target.cumulative_count += source.cumulative_count;
            }
        }
        self.count += other.count;
        self.sum += other.sum;
    }
}

/// Scheduler/backend histograms used by the simulator and structured tests.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize)]
pub struct NodeHistogramSnapshot {
    pub backend_fetch_duration_seconds: HistogramSnapshot,
    pub backend_fetch_queue_duration_seconds: HistogramSnapshot,
    pub backend_fetch_size_bytes: HistogramSnapshot,
    pub backend_fetch_chunks: HistogramSnapshot,
    pub queue_delay_immediate_seconds: HistogramSnapshot,
    pub queue_delay_window_seconds: HistogramSnapshot,
    pub pending_chunks_immediate: HistogramSnapshot,
    pub pending_chunks_window: HistogramSnapshot,
    pub group_waiters: HistogramSnapshot,
}

impl NodeHistogramSnapshot {
    pub fn merge(&mut self, other: &Self) {
        self.backend_fetch_duration_seconds
            .merge(&other.backend_fetch_duration_seconds);
        self.backend_fetch_queue_duration_seconds
            .merge(&other.backend_fetch_queue_duration_seconds);
        self.backend_fetch_size_bytes
            .merge(&other.backend_fetch_size_bytes);
        self.backend_fetch_chunks.merge(&other.backend_fetch_chunks);
        self.queue_delay_immediate_seconds
            .merge(&other.queue_delay_immediate_seconds);
        self.queue_delay_window_seconds
            .merge(&other.queue_delay_window_seconds);
        self.pending_chunks_immediate
            .merge(&other.pending_chunks_immediate);
        self.pending_chunks_window
            .merge(&other.pending_chunks_window);
        self.group_waiters.merge(&other.group_waiters);
    }
}

fn int_counter(name: &str, help: &str) -> IntCounter {
    IntCounter::new(name, help).expect("valid metric")
}

fn int_counter_vec(name: &str, help: &str, labels: &[&str]) -> IntCounterVec {
    IntCounterVec::new(Opts::new(name, help), labels).expect("valid metric")
}

fn int_gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::new(name, help).expect("valid metric")
}

fn int_gauge_vec(name: &str, help: &str, labels: &[&str]) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), labels).expect("valid metric")
}

fn gauge(name: &str, help: &str) -> Gauge {
    Gauge::new(name, help).expect("valid metric")
}

fn histogram(name: &str, help: &str, buckets: Vec<f64>) -> Histogram {
    Histogram::with_opts(HistogramOpts::new(name, help).buckets(buckets)).expect("valid metric")
}

fn histogram_vec(name: &str, help: &str, buckets: Vec<f64>, labels: &[&str]) -> HistogramVec {
    HistogramVec::new(HistogramOpts::new(name, help).buckets(buckets), labels)
        .expect("valid metric")
}

impl NodeMetrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let egress_bytes = int_counter(
            "ishikari_external_egress_bytes_total",
            "Bytes served to external clients",
        );
        let internal_bytes = int_counter(
            "ishikari_internal_egress_bytes_total",
            "Bytes served to peers over internal endpoints",
        );
        let http_requests = int_counter_vec(
            "ishikari_http_requests_total",
            "HTTP requests by route and status",
            &["endpoint", "status"],
        );
        let http_request_duration = histogram_vec(
            "ishikari_http_request_duration_seconds",
            "End-to-end HTTP request duration by route and status class",
            vec![
                0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ],
            &["endpoint", "status_class"],
        );
        let tiles_served = int_counter_vec(
            "ishikari_tiles_served_total",
            "External tile responses by where they were served from",
            &["source"],
        );
        let tile_cache = int_counter_vec(
            "ishikari_tile_cache_total",
            "Tile cache lookups and inserts by outcome",
            &["outcome"],
        );
        let tile_negative_cache_hits = int_counter(
            "ishikari_tile_negative_cache_hits_total",
            "Tile resolutions served by an existing negative L1 cache entry",
        );
        let peer_forward = int_counter_vec(
            "ishikari_peer_forward_total",
            "Peer forwarding attempts and backoff skips by outcome",
            &["outcome"],
        );
        let peer_fetch = int_counter_vec(
            "ishikari_peer_fetch_total",
            "Internal peer fetch attempts by resource and outcome",
            &["resource", "outcome"],
        );
        let peer_fetch_duplicate_inflight = int_counter_vec(
            "ishikari_peer_fetch_duplicate_inflight_total",
            "Peer fetches overlapping an identical in-flight peer/path request",
            &["resource"],
        );
        let internal_resource_requests = int_counter_vec(
            "ishikari_internal_resource_requests_total",
            "Internal resource requests served by resource and outcome",
            &["resource", "outcome"],
        );
        let provider_resource_cache = int_counter_vec(
            "ishikari_provider_resource_cache_total",
            "Provider resource cache activity by resource and outcome",
            &["resource", "outcome"],
        );
        let mapterhorn_resolve = int_counter_vec(
            "ishikari_mapterhorn_resolve_total",
            "Mapterhorn composite tile resolutions by outcome",
            &["outcome"],
        );
        let cache_bytes = int_gauge_vec(
            "ishikari_cache_bytes",
            "Weighted byte size of each cache",
            &["cache"],
        );
        let backend_fetch_bytes = int_counter(
            "ishikari_backend_fetch_bytes_total",
            "Cumulative bytes fetched from object storage / upstream",
        );
        let backend_fetch_duration = histogram_vec(
            "ishikari_backend_fetch_duration_seconds",
            "Duration of object-storage chunk group fetches by outcome",
            vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0],
            &["outcome"],
        );
        let backend_fetch_size_bytes = histogram_vec(
            "ishikari_backend_fetch_size_bytes",
            "Byte size of object-storage chunk group fetches by outcome",
            vec![
                16_384.0,
                65_536.0,
                262_144.0,
                1_048_576.0,
                2_097_152.0,
                4_194_304.0,
                8_388_608.0,
                16_777_216.0,
                33_554_432.0,
            ],
            &["outcome"],
        );
        let backend_fetch_chunks = histogram_vec(
            "ishikari_backend_fetch_chunks",
            "Number of fixed-size chunks covered by each object-storage fetch by outcome",
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0],
            &["outcome"],
        );
        let backend_fetch_queue_duration = histogram(
            "ishikari_backend_fetch_queue_duration_seconds",
            "Time an object-storage range fetch waits for the process-wide concurrency limit",
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        );
        let backend_fetch_concurrency = int_gauge_vec(
            "ishikari_backend_fetch_concurrency",
            "Process-wide object-storage range-fetch admission state",
            &["state"],
        );
        let chunk_size_bytes = int_gauge(
            "ishikari_chunk_size_bytes",
            "Configured backend chunk size in bytes",
        );
        let max_fetch_chunks = int_gauge(
            "ishikari_max_fetch_chunks",
            "Configured maximum chunks to fetch in one backend request",
        );
        let chunk_fetch_merge_window_seconds = gauge(
            "ishikari_chunk_fetch_merge_window_seconds",
            "Configured scheduler delay used to merge nearby chunk fetch requests",
        );
        let chunk_fetch_queue_delay = histogram_vec(
            "ishikari_chunk_fetch_queue_delay_seconds",
            "Time from the first queued missing chunk to backend fetch dispatch",
            vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
            &["flush"],
        );
        let chunk_fetch_pending_chunks = histogram_vec(
            "ishikari_chunk_fetch_pending_chunks",
            "Number of pending chunks visible when the scheduler dispatches backend fetches",
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0],
            &["flush"],
        );
        let chunk_fetch_group_waiters = histogram_vec(
            "ishikari_chunk_fetch_group_waiters",
            "Number of chunk waiters released by each completed backend fetch group",
            vec![1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 64.0, 128.0],
            &["outcome"],
        );
        let chunk_cache = int_counter_vec(
            "ishikari_chunk_cache_total",
            "Chunk cache lookups and post-fetch reads by outcome",
            &["outcome"],
        );
        let chunk_fetch_wait = int_counter_vec(
            "ishikari_chunk_fetch_wait_total",
            "Chunk wait registrations by whether they queued a new fetch or joined existing work",
            &["outcome"],
        );
        let cpu_work_admission = int_counter_vec(
            "ishikari_cpu_work_admission_total",
            "CPU-heavy work admission attempts by work kind and outcome",
            &["work", "outcome"],
        );
        let cpu_work_queue_duration = histogram_vec(
            "ishikari_cpu_work_queue_duration_seconds",
            "Time admitted CPU-heavy work waits for a blocking-work permit",
            vec![
                0.0001, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
            &["work"],
        );
        let cpu_work = int_gauge_vec(
            "ishikari_cpu_work",
            "CPU-heavy work admission and execution state by class (plus `all`)",
            &["class", "state"],
        );
        let terrain_source_duration = histogram_vec(
            "ishikari_terrain_source_duration_seconds",
            "Time to fetch and decode a derived terrain product's DEM neighborhood",
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ],
            &["product"],
        );
        let terrain_generation_duration = histogram_vec(
            "ishikari_terrain_generation_duration_seconds",
            "CPU time to generate and compress a derived terrain product",
            vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ],
            &["product"],
        );
        let terrain_source_tiles = histogram_vec(
            "ishikari_terrain_source_tiles",
            "Number of present DEM source tiles used by a derived terrain generation",
            vec![1.0, 4.0, 6.0, 8.0, 9.0],
            &["product"],
        );
        let terrain_output_size_bytes = histogram_vec(
            "ishikari_terrain_output_size_bytes",
            "Compressed byte size of generated terrain tile responses",
            vec![
                4_096.0,
                16_384.0,
                65_536.0,
                131_072.0,
                262_144.0,
                524_288.0,
                1_048_576.0,
                2_097_152.0,
            ],
            &["product"],
        );
        let membership_size = int_gauge_vec(
            "ishikari_membership_size",
            "Cluster member count by state",
            &["state"],
        );
        let drain_state = int_gauge(
            "ishikari_drain_state",
            "1 if this node is draining, otherwise 0",
        );

        let inner = Inner {
            registry,
            egress_bytes,
            internal_bytes,
            http_requests,
            http_request_duration,
            tiles_served,
            tile_cache,
            tile_negative_cache_hits,
            peer_forward,
            peer_fetch,
            peer_fetch_duplicate_inflight,
            internal_resource_requests,
            provider_resource_cache,
            mapterhorn_resolve,
            cache_bytes,
            backend_fetch_bytes,
            backend_fetch_duration,
            backend_fetch_size_bytes,
            backend_fetch_chunks,
            backend_fetch_queue_duration,
            backend_fetch_concurrency,
            chunk_size_bytes,
            max_fetch_chunks,
            chunk_fetch_merge_window_seconds,
            chunk_fetch_queue_delay,
            chunk_fetch_pending_chunks,
            chunk_fetch_group_waiters,
            chunk_cache,
            chunk_fetch_wait,
            cpu_work_admission,
            cpu_work_queue_duration,
            cpu_work,
            terrain_source_duration,
            terrain_generation_duration,
            terrain_source_tiles,
            terrain_output_size_bytes,
            membership_size,
            drain_state,
        };
        inner.register_collectors();
        Self(Arc::new(inner))
    }

    pub fn add_egress_bytes(&self, bytes: u64) {
        self.0.egress_bytes.inc_by(bytes);
    }

    pub fn add_internal_bytes(&self, bytes: u64) {
        self.0.internal_bytes.inc_by(bytes);
    }

    pub fn egress_bytes(&self) -> u64 {
        self.0.egress_bytes.get()
    }

    pub fn internal_bytes(&self) -> u64 {
        self.0.internal_bytes.get()
    }

    /// Records one completed HTTP request against a bounded route pattern.
    pub fn record_http(&self, endpoint: &str, status: u16, duration: Duration) {
        self.record_http_request(endpoint, status);
        self.record_http_duration(endpoint, status, duration);
    }

    /// Records an HTTP request count without adding a duration observation.
    pub fn record_http_request(&self, endpoint: &str, status: u16) {
        self.0
            .http_requests
            .with_label_values(&[endpoint, &status.to_string()])
            .inc();
    }

    /// Records an HTTP duration observation without incrementing request count.
    pub fn record_http_duration(&self, endpoint: &str, status: u16, duration: Duration) {
        self.0
            .http_request_duration
            .with_label_values(&[endpoint, status_class(status)])
            .observe(duration.as_secs_f64());
    }

    /// Records one external tile response by its served-from source.
    pub fn record_tile_served(&self, source: &str) {
        self.0.tiles_served.with_label_values(&[source]).inc();
    }

    /// Records one tile-cache event.
    ///
    /// `negative_hit` is an internal exact event emitted only for
    /// `TileSource::NegativeCache`. The existing aggregate `negative` outcome is
    /// recorded separately so its historical hit-plus-insert semantics remain
    /// compatible.
    pub fn record_tile_cache(&self, outcome: &str) {
        if outcome == "negative_hit" {
            self.0.tile_negative_cache_hits.inc();
        } else {
            self.0.tile_cache.with_label_values(&[outcome]).inc();
        }
    }

    /// Records one peer forwarding outcome or one routing skip due to backoff.
    pub fn record_peer_forward(&self, outcome: &str) {
        self.0.peer_forward.with_label_values(&[outcome]).inc();
    }

    /// Records one internal peer network attempt by bounded resource kind.
    pub fn record_peer_fetch(&self, resource: &str, outcome: &str) {
        self.0
            .peer_fetch
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records a peer fetch that overlaps an identical peer/path request.
    pub fn record_peer_fetch_duplicate_inflight(&self, resource: &str) {
        self.0
            .peer_fetch_duplicate_inflight
            .with_label_values(&[resource])
            .inc();
    }

    /// Records one internal resource request served by this node.
    pub fn record_internal_resource_request(&self, resource: &str, outcome: &str) {
        self.0
            .internal_resource_requests
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records provider-cache activity for the bounded style/glyph/sprite kinds.
    pub fn record_provider_resource_cache(&self, resource: &str, outcome: &str) {
        self.0
            .provider_resource_cache
            .with_label_values(&[resource, outcome])
            .inc();
    }

    /// Records one Mapterhorn composite resolution outcome: `base`, `detail`,
    /// `detail_negative` (archive absent), or `detail_error` (transient probe
    /// failure, not cached).
    pub fn record_mapterhorn(&self, outcome: &str) {
        self.0
            .mapterhorn_resolve
            .with_label_values(&[outcome])
            .inc();
    }

    /// Sets the weighted byte size gauge for a named cache.
    pub fn set_cache_bytes(&self, cache: &str, bytes: u64) {
        self.0
            .cache_bytes
            .with_label_values(&[cache])
            .set(bytes as i64);
    }

    /// Advances the backend-fetch counter to a cumulative total.
    ///
    /// The source value lives in the storage layer as a monotonic cumulative
    /// count; this folds it into a real Prometheus counter at scrape time. Both
    /// reset to 0 together on process restart, so `rate()` stays correct.
    pub fn sync_backend_fetch_bytes(&self, cumulative: u64) {
        let current = self.0.backend_fetch_bytes.get();
        if cumulative > current {
            self.0.backend_fetch_bytes.inc_by(cumulative - current);
        }
    }

    /// Records one object-store chunk group fetch.
    pub fn record_backend_fetch(&self, outcome: &str, duration: Duration, chunks: u64, bytes: u64) {
        self.0
            .backend_fetch_duration
            .with_label_values(&[outcome])
            .observe(duration.as_secs_f64());
        self.0
            .backend_fetch_size_bytes
            .with_label_values(&[outcome])
            .observe(bytes as f64);
        self.0
            .backend_fetch_chunks
            .with_label_values(&[outcome])
            .observe(chunks as f64);
    }

    /// Exposes the process-wide backend-fetch concurrency ceiling.
    pub fn set_backend_fetch_concurrency_limit(&self, limit: usize) {
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["active"])
            .set(0);
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["waiting"])
            .set(0);
        self.0
            .backend_fetch_concurrency
            .with_label_values(&["limit"])
            .set(limit as i64);
    }

    /// Adjusts the current backend-fetch admission state (`active` or `waiting`).
    pub fn adjust_backend_fetch_concurrency(&self, state: &str, delta: i64) {
        self.0
            .backend_fetch_concurrency
            .with_label_values(&[state])
            .add(delta);
    }

    /// Records time spent waiting for the process-wide backend-fetch permit.
    pub fn record_backend_fetch_queue(&self, duration: Duration) {
        self.0
            .backend_fetch_queue_duration
            .observe(duration.as_secs_f64());
    }

    /// Exposes backend chunking configuration for comparing deployments.
    pub fn set_chunk_config(&self, chunk_size_bytes: u64, max_fetch_chunks: u64) {
        self.0.chunk_size_bytes.set(chunk_size_bytes as i64);
        self.0.max_fetch_chunks.set(max_fetch_chunks as i64);
    }

    /// Exposes the configured merge window used by the chunk fetch scheduler.
    pub fn set_chunk_fetch_merge_window(&self, duration: Duration) {
        self.0
            .chunk_fetch_merge_window_seconds
            .set(duration.as_secs_f64());
    }

    /// Records one scheduler dispatch after coalescing pending chunk requests.
    pub fn record_chunk_fetch_dispatch(
        &self,
        flush: &str,
        queue_delay: Duration,
        pending_chunks: usize,
    ) {
        self.0
            .chunk_fetch_queue_delay
            .with_label_values(&[flush])
            .observe(queue_delay.as_secs_f64());
        self.0
            .chunk_fetch_pending_chunks
            .with_label_values(&[flush])
            .observe(pending_chunks as f64);
    }

    /// Records how many chunk waiters were satisfied by a completed backend group.
    pub fn record_chunk_fetch_group_waiters(&self, outcome: &str, waiters: usize) {
        self.0
            .chunk_fetch_group_waiters
            .with_label_values(&[outcome])
            .observe(waiters as f64);
    }

    /// Records one chunk cache lookup/read outcome.
    pub fn record_chunk_cache(&self, outcome: &str) {
        self.0.chunk_cache.with_label_values(&[outcome]).inc();
    }

    /// Records one required missing chunk's relationship to pending/inflight work.
    pub fn record_chunk_fetch_wait(&self, outcome: &str) {
        self.0.chunk_fetch_wait.with_label_values(&[outcome]).inc();
    }

    /// Records admission or shedding for one of the fixed CPU-work kinds.
    pub fn record_cpu_work_admission(&self, work: &str, outcome: &str) {
        self.0
            .cpu_work_admission
            .with_label_values(&[work, outcome])
            .inc();
    }

    /// Records how long admitted work waited for a CPU-work permit.
    pub fn record_cpu_work_queue_duration(&self, work: &str, duration: Duration) {
        self.0
            .cpu_work_queue_duration
            .with_label_values(&[work])
            .observe(duration.as_secs_f64());
    }

    /// Sets aggregate current and configured CPU-work admission/execution values.
    pub fn set_cpu_work(
        &self,
        class: &str,
        inflight: usize,
        running: usize,
        concurrency: usize,
        max: usize,
    ) {
        for (state, value) in [
            ("inflight", inflight),
            ("running", running),
            ("concurrency", concurrency),
            ("max_inflight", max),
        ] {
            self.0
                .cpu_work
                .with_label_values(&[class, state])
                .set(value as i64);
        }
    }

    /// Records the successful cold-generation cost for one fixed terrain product.
    pub fn record_terrain_generation(
        &self,
        product: &str,
        source_duration: Duration,
        generation_duration: Duration,
        source_tiles: usize,
        output_bytes: usize,
    ) {
        self.0
            .terrain_source_duration
            .with_label_values(&[product])
            .observe(source_duration.as_secs_f64());
        self.0
            .terrain_generation_duration
            .with_label_values(&[product])
            .observe(generation_duration.as_secs_f64());
        self.0
            .terrain_source_tiles
            .with_label_values(&[product])
            .observe(source_tiles as f64);
        self.0
            .terrain_output_size_bytes
            .with_label_values(&[product])
            .observe(output_bytes as f64);
    }

    /// Returns a structured snapshot without parsing Prometheus text output.
    pub fn snapshot(&self) -> NodeMetricsSnapshot {
        let counter =
            |metric: &IntCounterVec, label: &str| metric.with_label_values(&[label]).get();
        let labeled_counter_sum = |metric: &IntCounterVec, first: &str, second_values: &[&str]| {
            second_values
                .iter()
                .map(|second| metric.with_label_values(&[first, *second]).get())
                .sum()
        };
        let histogram_count = |metric: &HistogramVec, label: &str| {
            metric.with_label_values(&[label]).get_sample_count()
        };
        let histogram_sum = |metric: &HistogramVec, label: &str| {
            metric.with_label_values(&[label]).get_sample_sum().round() as u64
        };
        let provider_resources = ["style", "glyph", "sprite", "derived", "other"];
        let peer_outcomes = ["success", "not_found", "retryable", "fatal"];
        let internal_outcomes = ["success", "not_found", "retryable", "error"];
        let backend_fetch_successes = histogram_count(&self.0.backend_fetch_duration, "success");
        let backend_fetch_not_found = histogram_count(&self.0.backend_fetch_duration, "not_found");
        let backend_fetch_errors = histogram_count(&self.0.backend_fetch_duration, "error");
        let backend_fetch_timeouts = histogram_count(&self.0.backend_fetch_duration, "timeout");

        NodeMetricsSnapshot {
            negative_cache_hits: self.0.tile_negative_cache_hits.get(),
            peer_forward_successes: counter(&self.0.peer_forward, "success"),
            peer_forward_not_found: counter(&self.0.peer_forward, "not_found"),
            peer_forward_retryable: counter(&self.0.peer_forward, "retryable"),
            peer_forward_fatal: counter(&self.0.peer_forward, "fatal"),
            peer_forward_backoff_skips: counter(&self.0.peer_forward, "backoff"),
            peer_tile_fetches: labeled_counter_sum(&self.0.peer_fetch, "tile", &peer_outcomes),
            peer_bootstrap_fetches: labeled_counter_sum(
                &self.0.peer_fetch,
                "bootstrap",
                &peer_outcomes,
            ),
            peer_leaf_fetches: labeled_counter_sum(&self.0.peer_fetch, "leaf", &peer_outcomes),
            peer_provider_fetches: provider_resources
                .into_iter()
                .map(|resource| labeled_counter_sum(&self.0.peer_fetch, resource, &peer_outcomes))
                .sum(),
            peer_tile_duplicate_inflight: counter(&self.0.peer_fetch_duplicate_inflight, "tile"),
            peer_bootstrap_duplicate_inflight: counter(
                &self.0.peer_fetch_duplicate_inflight,
                "bootstrap",
            ),
            peer_leaf_duplicate_inflight: counter(&self.0.peer_fetch_duplicate_inflight, "leaf"),
            peer_provider_duplicate_inflight: provider_resources
                .into_iter()
                .map(|resource| counter(&self.0.peer_fetch_duplicate_inflight, resource))
                .sum(),
            internal_tile_requests: labeled_counter_sum(
                &self.0.internal_resource_requests,
                "tile",
                &internal_outcomes,
            ),
            internal_bootstrap_requests: labeled_counter_sum(
                &self.0.internal_resource_requests,
                "bootstrap",
                &internal_outcomes,
            ),
            internal_leaf_requests: labeled_counter_sum(
                &self.0.internal_resource_requests,
                "leaf",
                &internal_outcomes,
            ),
            internal_provider_requests: provider_resources
                .into_iter()
                .map(|resource| {
                    labeled_counter_sum(
                        &self.0.internal_resource_requests,
                        resource,
                        &internal_outcomes,
                    )
                })
                .sum(),
            backend_fetches: backend_fetch_successes
                + backend_fetch_not_found
                + backend_fetch_errors
                + backend_fetch_timeouts,
            backend_fetch_successes,
            backend_fetch_not_found,
            backend_fetch_errors,
            backend_fetch_timeouts,
            backend_fetched_chunks: histogram_sum(&self.0.backend_fetch_chunks, "success"),
            chunk_cache_hits: counter(&self.0.chunk_cache, "hit"),
            chunk_cache_misses: counter(&self.0.chunk_cache, "miss"),
            chunk_cache_post_fetch_hits: counter(&self.0.chunk_cache, "post_fetch_hit"),
            chunk_fetch_queued: counter(&self.0.chunk_fetch_wait, "queued"),
            chunk_fetch_joined_pending: counter(&self.0.chunk_fetch_wait, "joined_pending"),
            chunk_fetch_joined_inflight: counter(&self.0.chunk_fetch_wait, "joined_inflight"),
            chunk_dispatch_immediate: histogram_count(&self.0.chunk_fetch_queue_delay, "immediate"),
            chunk_dispatch_window: histogram_count(&self.0.chunk_fetch_queue_delay, "window"),
            chunk_dispatch_pending_chunks: ["immediate", "window"]
                .into_iter()
                .map(|flush| histogram_sum(&self.0.chunk_fetch_pending_chunks, flush))
                .sum(),
            chunk_waiters_released: ["success", "error"]
                .into_iter()
                .map(|outcome| histogram_sum(&self.0.chunk_fetch_group_waiters, outcome))
                .sum(),
        }
    }

    /// Returns mergeable backend/scheduler histogram buckets.
    pub fn histogram_snapshot(&self) -> NodeHistogramSnapshot {
        let labeled = |metric: &HistogramVec, label: &str| {
            histogram_snapshot(&metric.with_label_values(&[label]))
        };
        NodeHistogramSnapshot {
            backend_fetch_duration_seconds: merge_histograms(
                &self.0.backend_fetch_duration,
                BACKEND_OUTCOMES,
            ),
            backend_fetch_queue_duration_seconds: histogram_snapshot(
                &self.0.backend_fetch_queue_duration,
            ),
            backend_fetch_size_bytes: merge_histograms(
                &self.0.backend_fetch_size_bytes,
                BACKEND_OUTCOMES,
            ),
            backend_fetch_chunks: merge_histograms(&self.0.backend_fetch_chunks, BACKEND_OUTCOMES),
            queue_delay_immediate_seconds: labeled(&self.0.chunk_fetch_queue_delay, "immediate"),
            queue_delay_window_seconds: labeled(&self.0.chunk_fetch_queue_delay, "window"),
            pending_chunks_immediate: labeled(&self.0.chunk_fetch_pending_chunks, "immediate"),
            pending_chunks_window: labeled(&self.0.chunk_fetch_pending_chunks, "window"),
            group_waiters: merge_histograms(
                &self.0.chunk_fetch_group_waiters,
                &["success", "error"],
            ),
        }
    }

    /// Sets the live/dead membership gauges.
    pub fn set_membership(&self, live: i64, dead: i64) {
        self.0
            .membership_size
            .with_label_values(&["live"])
            .set(live);
        self.0
            .membership_size
            .with_label_values(&["dead"])
            .set(dead);
    }

    /// Sets the drain-state gauge.
    pub fn set_drain(&self, draining: bool) {
        self.0.drain_state.set(draining as i64);
    }

    /// Encodes the registry in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        let metric_families = self.0.registry.gather();
        let mut buffer = Vec::new();
        if TextEncoder::new()
            .encode(&metric_families, &mut buffer)
            .is_err()
        {
            return String::new();
        }
        String::from_utf8(buffer).unwrap_or_default()
    }
}

fn status_class(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

fn merge_histograms(metrics: &HistogramVec, values: &[&str]) -> HistogramSnapshot {
    let mut merged = HistogramSnapshot::default();
    for value in values {
        merged.merge(&histogram_snapshot(&metrics.with_label_values(&[*value])));
    }
    merged
}

fn histogram_snapshot(histogram: &Histogram) -> HistogramSnapshot {
    let families = histogram.collect();
    let Some(metric) = families
        .first()
        .and_then(|family| family.get_metric().first())
    else {
        return HistogramSnapshot::default();
    };
    let histogram = metric.get_histogram();
    HistogramSnapshot {
        count: histogram.get_sample_count(),
        sum: histogram.get_sample_sum(),
        buckets: histogram
            .get_bucket()
            .iter()
            .map(|bucket| HistogramBucketSnapshot {
                upper_bound: bucket.upper_bound(),
                cumulative_count: bucket.cumulative_count(),
            })
            .collect(),
    }
}

impl Default for NodeMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::NodeMetrics;

    #[test]
    fn exact_negative_cache_hit_metric_is_distinct_from_legacy_negative_events() {
        let metrics = NodeMetrics::new();
        assert!(
            metrics
                .encode()
                .contains("ishikari_tile_negative_cache_hits_total 0")
        );

        metrics.record_tile_cache("negative");
        assert_eq!(metrics.snapshot().negative_cache_hits, 0);

        metrics.record_tile_cache("negative_hit");
        let encoded = metrics.encode();
        assert_eq!(metrics.snapshot().negative_cache_hits, 1);
        assert!(encoded.contains("ishikari_tile_negative_cache_hits_total 1"));
        assert!(encoded.contains("ishikari_tile_cache_total{outcome=\"negative\"} 1"));
    }
}

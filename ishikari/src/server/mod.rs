//! HTTP app wiring and shared state.

use std::{
    future::Future,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};

use crate::{
    drain::{self, DrainController},
    membership::Membership,
    metrics::NodeMetrics,
    request_id, server,
    server::provider::ProviderConfig,
    server::tileset::mapterhorn::MapterhornResolver,
    server::upstream::ProviderFetchCache,
    storage::{ObjectStoreRegistry, ResourceResolver},
};
use anyhow::{Context, Result};
use axum::{
    Json, Router, ServiceExt,
    extract::{MatchedPath, Request, State},
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use tokio::net::TcpListener;
use tracing::Instrument;

pub(crate) type HttpError = (StatusCode, String);

pub struct TileRuntimeConfig {
    pub mapterhorn: Option<Arc<MapterhornResolver>>,
    pub cpu_work_concurrency: usize,
    /// Maximum admitted CPU-work units per work class (holding a permit or
    /// queued for one) before new work in that class is shed with 503.
    pub cpu_work_max_inflight: usize,
    pub derived_negative_ttl: Duration,
}

/// Weight charged for a negative cache entry (derived `Absent`/`Degraded`
/// placeholder keys, absent DEM markers). A nominal `1` would let millions of
/// negative entries blow far past a cache's byte capacity: each entry still
/// pays its key, enum, and moka bookkeeping on the heap.
const NEGATIVE_CACHE_ENTRY_WEIGHT: u32 = 128;
/// Terrain pipelines, MLT transcodes, and provider document transformations
/// each have an independent admission counter and ceiling.
const CPU_WORK_CLASS_COUNT: usize = 3;
/// GKE Autopilot caps Spot-pod termination grace at 25 seconds. Shutdown spends
/// two seconds announcing drain state before Axum stops accepting requests, so
/// every HTTP request must finish (or be cancelled) with headroom before the
/// platform sends SIGKILL.
const HTTP_REQUEST_DEADLINE: Duration = Duration::from_secs(20);

/// RAII reservation in the CPU-work admission counter. Reserving fails (a shed)
/// when the counter is already at its ceiling; the reservation is released on
/// drop — including when the awaiting future is cancelled before it acquires a
/// permit — so the count can never leak.
struct CpuWorkSlot {
    inflight: Arc<AtomicUsize>,
}

struct CacheMaintenanceGuard {
    running: Arc<AtomicBool>,
}

impl Drop for CacheMaintenanceGuard {
    fn drop(&mut self) {
        self.running.store(false, Ordering::Release);
    }
}

impl CpuWorkSlot {
    fn try_reserve(inflight: &Arc<AtomicUsize>, max: usize) -> Option<Self> {
        let previous = inflight.fetch_add(1, Ordering::Relaxed);
        if previous >= max {
            inflight.fetch_sub(1, Ordering::Relaxed);
            None
        } else {
            Some(Self {
                inflight: inflight.clone(),
            })
        }
    }
}

impl Drop for CpuWorkSlot {
    fn drop(&mut self) {
        self.inflight.fetch_sub(1, Ordering::Relaxed);
    }
}

/// Admission ticket for one unit of CPU-bound request work. Holds a per-class
/// execution permit and an in-flight slot; dropping it (e.g. at the end of the
/// `spawn_blocking` closure) releases them. Class shares isolate workloads while
/// the global permit enforces the configured pod-wide CPU ceiling.
pub(crate) struct CpuWorkPermit {
    _class_permit: tokio::sync::OwnedSemaphorePermit,
    _global_permit: tokio::sync::OwnedSemaphorePermit,
    _slot: CpuWorkSlot,
}

/// CPU execution ticket for a child stage of work whose parent request has
/// already passed admission. It waits for its class's execution permit and the
/// pod ceiling without reserving another in-flight slot, so sibling DEM decodes
/// cannot shed one another.
pub(crate) struct AdmittedCpuWorkPermit {
    _class_permit: tokio::sync::OwnedSemaphorePermit,
    _global_permit: tokio::sync::OwnedSemaphorePermit,
}

impl AdmittedCpuWorkPermit {
    async fn acquire(
        class_semaphore: Arc<tokio::sync::Semaphore>,
        global_semaphore: Arc<tokio::sync::Semaphore>,
    ) -> Result<Self, tokio::sync::AcquireError> {
        // Class before global everywhere, so the acquisition order is uniform
        // and cannot deadlock.
        let class_permit = class_semaphore.acquire_owned().await?;
        let global_permit = global_semaphore.acquire_owned().await?;
        Ok(Self {
            _class_permit: class_permit,
            _global_permit: global_permit,
        })
    }
}

/// Admission ticket for one derived-terrain pipeline, held from before the
/// neighborhood fetch through the end of generation so retained decoded-DEM
/// memory stays bounded (see [`AppState::admit_terrain_pipeline`]).
pub(crate) struct TerrainPipelinePermit {
    _permit: tokio::sync::OwnedSemaphorePermit,
    _slot: CpuWorkSlot,
}

struct DerivedTileExpiry {
    negative_ttl: Duration,
}

impl
    moka::Expiry<server::tileset::terrain::DerivedTileKey, server::tileset::terrain::DerivedOutcome>
    for DerivedTileExpiry
{
    fn expire_after_create(
        &self,
        _key: &server::tileset::terrain::DerivedTileKey,
        value: &server::tileset::terrain::DerivedOutcome,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        // Absences and refreshable edge-fallback tiles both re-resolve after
        // the short negative TTL; only clean positive tiles live until eviction.
        matches!(
            value,
            server::tileset::terrain::DerivedOutcome::Absent
                | server::tileset::terrain::DerivedOutcome::Degraded(_)
        )
        .then_some(self.negative_ttl)
    }
}

struct DecodedDemExpiry {
    negative_ttl: Duration,
}

fn cache_entry_weight<K, V>(key: &K, value: &V, heap_bytes: usize) -> u32 {
    std::mem::size_of_val(key)
        .saturating_add(std::mem::size_of_val(value))
        .saturating_add(heap_bytes)
        .max(NEGATIVE_CACHE_ENTRY_WEIGHT as usize)
        .min(u32::MAX as usize) as u32
}

fn mlt_cache_weight(key: &(crate::interned::TilesetId, u64), value: &bytes::Bytes) -> u32 {
    // The payload can be tiny (an empty MVT gzip is only a few dozen bytes),
    // but every entry still retains its key, Bytes handle, interned identifier,
    // and Moka bookkeeping. Charge the visible allocations and keep the same
    // conservative floor as the other byte-bounded tile caches.
    cache_entry_weight(key, value, key.0.as_str().len().saturating_add(value.len()))
}

fn derived_tile_cache_weight(
    key: &server::tileset::terrain::DerivedTileKey,
    value: &server::tileset::terrain::DerivedOutcome,
) -> u32 {
    let payload_bytes = match value {
        server::tileset::terrain::DerivedOutcome::Tile(tile)
        | server::tileset::terrain::DerivedOutcome::Degraded(tile) => tile.bytes.len(),
        server::tileset::terrain::DerivedOutcome::Absent => 0,
    };
    cache_entry_weight(key, value, payload_bytes)
}

fn decoded_dem_cache_weight(
    key: &(crate::interned::TilesetId, u64),
    value: &Option<Arc<server::tileset::terrain::dem::DemTile>>,
) -> u32 {
    let payload_bytes = value.as_ref().map_or(0, |tile| tile.byte_size());
    cache_entry_weight(key, value, payload_bytes)
}

impl
    moka::Expiry<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    > for DecodedDemExpiry
{
    fn expire_after_create(
        &self,
        _key: &(crate::interned::TilesetId, u64),
        value: &Option<Arc<server::tileset::terrain::dem::DemTile>>,
        _created_at: std::time::Instant,
    ) -> Option<Duration> {
        value.is_none().then_some(self.negative_ttl)
    }
}

#[derive(Clone)]
pub struct AppState {
    membership: Membership,
    pub(crate) metrics: NodeMetrics,
    resource_resolver: Arc<ResourceResolver>,
    drain: DrainController,
    provider: ProviderConfig,
    provider_fetch_cache: ProviderFetchCache,
    object_store_registry: Arc<ObjectStoreRegistry>,
    /// Per-pod cache of transcoded MLT tiles, keyed by (tileset, tile id).
    /// Populated lazily on first `.mlt` request; see `server::tileset::mlt`.
    mlt_cache: moka::future::Cache<(crate::interned::TilesetId, u64), bytes::Bytes>,
    /// Generated contour/hillshade MVTs. Async cache initialization single-flights
    /// the 3x3 source fetch and CPU generation for each derived tile.
    derived_tile_cache: moka::future::Cache<
        server::tileset::terrain::DerivedTileKey,
        server::tileset::terrain::DerivedOutcome,
    >,
    /// Decoded Terrarium DEM tiles, shared across derived products and
    /// neighboring derived tiles (each 3x3 window overlaps its neighbors in six
    /// of nine sources), so each source tile is WebP-decoded roughly once.
    dem_tile_cache: moka::future::Cache<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    >,
    /// Coalesces Moka maintenance when multiple metrics collectors scrape at
    /// once. Followers may report the previous eventually-consistent size.
    cache_maintenance_running: Arc<AtomicBool>,
    /// CPU-heavy request work is partitioned into per-class execution gates
    /// whose permit counts partition the configured pod concurrency, plus a
    /// pod-wide execution ceiling every class must also pass.
    ///
    /// For the usual `N >= 3` the class shares sum to `N`, so the class permits
    /// alone bound total CPU to `N` and the light classes' reserved shares mean
    /// a terrain flood can neither exceed the budget nor stall style/transcode.
    /// For a tiny pod (`N < 3`) the shares floor at one each and would sum above
    /// `N`; the pod ceiling then binds and enforces the true `N` (classes share
    /// it — full isolation is impossible below three permits, but the cgroup
    /// limit still holds). Each class also has its own admission backlog, so one
    /// class's flood cannot shed another.
    cpu_work_semaphore: Arc<tokio::sync::Semaphore>,
    terrain_work_semaphore: Arc<tokio::sync::Semaphore>,
    terrain_work_concurrency: usize,
    /// Bounds concurrent derived-terrain pipelines (fetch → CPU queue →
    /// generation), each of which can retain a decoded 3x3 DEM neighborhood.
    terrain_pipeline_semaphore: Arc<tokio::sync::Semaphore>,
    terrain_pipeline_inflight: Arc<AtomicUsize>,
    /// Class-local pool for millisecond-scale provider CPU work (style
    /// rewriting, provider JSON validation).
    provider_work_semaphore: Arc<tokio::sync::Semaphore>,
    provider_work_concurrency: usize,
    provider_work_inflight: Arc<AtomicUsize>,
    /// Class-local pool for MLT tile transcoding.
    transcode_work_semaphore: Arc<tokio::sync::Semaphore>,
    transcode_work_concurrency: usize,
    transcode_work_inflight: Arc<AtomicUsize>,
    /// Configured pod CPU ceiling (the size of `cpu_work_semaphore`).
    cpu_work_concurrency: usize,
    /// Per-class admitted-work ceiling. Each class has its own counter so one
    /// flood cannot consume another class's backlog budget.
    cpu_work_max_inflight: usize,
    derived_negative_ttl: Duration,
    /// Mapterhorn composite resolver, when a composite tileset is configured.
    mapterhorn: Option<Arc<MapterhornResolver>>,
}

impl AppState {
    pub fn new(
        membership: Membership,
        metrics: NodeMetrics,
        resource_resolver: Arc<ResourceResolver>,
        drain: DrainController,
        provider: ProviderConfig,
        object_store_registry: Arc<ObjectStoreRegistry>,
        tile_runtime: TileRuntimeConfig,
    ) -> Self {
        let TileRuntimeConfig {
            mapterhorn,
            cpu_work_concurrency,
            cpu_work_max_inflight,
            derived_negative_ttl,
        } = tile_runtime;
        // Partition the pod CPU budget across classes. The light, latency-
        // critical classes (provider, transcode) each reserve one permit;
        // terrain takes the rest. For `N >= 3` the shares sum to `N`, so the
        // class permits alone cap total CPU and reserve the light classes'
        // slots. For `N < 3` the shares floor at one each (sum > N); the pod
        // ceiling below then binds and enforces the true `N`.
        let total_cpu = cpu_work_concurrency.max(1);
        let provider_work_concurrency = 1;
        let transcode_work_concurrency = 1;
        let terrain_work_concurrency = total_cpu
            .saturating_sub(provider_work_concurrency + transcode_work_concurrency)
            .max(1);
        Self {
            membership,
            metrics,
            resource_resolver,
            drain,
            provider,
            provider_fetch_cache: ProviderFetchCache::new(),
            object_store_registry,
            mapterhorn,
            // Bounded, byte-weighted: first `.mlt` request transcodes, the rest
            // hit this cache. 64 MiB ≈ a few hundred warm MLT tiles per pod.
            mlt_cache: moka::future::Cache::builder()
                .max_capacity(64 * 1024 * 1024)
                .weigher(mlt_cache_weight)
                .build(),
            derived_tile_cache: moka::future::Cache::builder()
                .max_capacity(128 * 1024 * 1024)
                .weigher(derived_tile_cache_weight)
                .expire_after(DerivedTileExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            // 64 MiB ≈ 64 decoded 512px DEM tiles (f32) — an 8x8-source-tile
            // working set, plenty for a viewport of derived tiles.
            dem_tile_cache: moka::future::Cache::builder()
                .max_capacity(64 * 1024 * 1024)
                .weigher(decoded_dem_cache_weight)
                .expire_after(DecodedDemExpiry {
                    negative_ttl: derived_negative_ttl,
                })
                .build(),
            cache_maintenance_running: Arc::new(AtomicBool::new(false)),
            // Pod-wide execution ceiling = the configured `N`. Redundant when
            // the class shares sum to `N` (N >= 3); the binding cap when they
            // floor above `N` on a tiny pod.
            cpu_work_semaphore: Arc::new(tokio::sync::Semaphore::new(total_cpu)),
            terrain_work_semaphore: Arc::new(tokio::sync::Semaphore::new(terrain_work_concurrency)),
            terrain_work_concurrency,
            // Twice the terrain CPU share keeps generation fed (one pipeline
            // generating, one pre-fetching per CPU slot) while bounding retained
            // decoded-DEM memory to permits × one 3x3 neighborhood.
            terrain_pipeline_semaphore: Arc::new(tokio::sync::Semaphore::new(
                terrain_work_concurrency.saturating_mul(2),
            )),
            terrain_pipeline_inflight: Arc::new(AtomicUsize::new(0)),
            provider_work_semaphore: Arc::new(tokio::sync::Semaphore::new(
                provider_work_concurrency,
            )),
            provider_work_concurrency,
            provider_work_inflight: Arc::new(AtomicUsize::new(0)),
            transcode_work_semaphore: Arc::new(tokio::sync::Semaphore::new(
                transcode_work_concurrency,
            )),
            transcode_work_concurrency,
            transcode_work_inflight: Arc::new(AtomicUsize::new(0)),
            // The true pod ceiling is `total_cpu`, not the (possibly floored)
            // sum of class shares.
            cpu_work_concurrency: total_cpu,
            cpu_work_max_inflight: cpu_work_max_inflight.max(cpu_work_concurrency.max(1)),
            derived_negative_ttl,
        }
    }
}

impl AppState {
    /// Per-pod transcoded-MLT cache, keyed by `(tileset, tile id)`.
    pub(crate) fn mlt_cache(
        &self,
    ) -> &moka::future::Cache<(crate::interned::TilesetId, u64), bytes::Bytes> {
        &self.mlt_cache
    }

    /// The configured Mapterhorn composite resolver, if any.
    pub(crate) fn mapterhorn(&self) -> Option<&Arc<MapterhornResolver>> {
        self.mapterhorn.as_ref()
    }

    pub(crate) fn derived_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        server::tileset::terrain::DerivedTileKey,
        server::tileset::terrain::DerivedOutcome,
    > {
        &self.derived_tile_cache
    }

    /// Decoded-DEM cache backing derived terrain generation.
    pub(crate) fn dem_tile_cache(
        &self,
    ) -> &moka::future::Cache<
        (crate::interned::TilesetId, u64),
        Option<Arc<server::tileset::terrain::dem::DemTile>>,
    > {
        &self.dem_tile_cache
    }

    /// Admits one millisecond-scale provider CPU job (style rewriting or
    /// provider JSON validation). Its backlog and shed ceiling are isolated by
    /// class; execution also acquires the pod-wide CPU ceiling.
    pub(crate) async fn admit_provider_work(
        &self,
        work: &'static str,
    ) -> Result<CpuWorkPermit, HttpError> {
        self.admit_bounded_work(
            &self.provider_work_semaphore,
            &self.provider_work_inflight,
            work,
        )
        .await
    }

    /// Admits one MLT tile transcode. Its backlog and shed ceiling are isolated
    /// from terrain and provider work; execution shares the pod-wide ceiling.
    pub(crate) async fn admit_transcode_work(&self) -> Result<CpuWorkPermit, HttpError> {
        self.admit_bounded_work(
            &self.transcode_work_semaphore,
            &self.transcode_work_inflight,
            "mlt_transcode",
        )
        .await
    }

    /// Shed-then-queue admission: reserves an in-flight slot (shedding with
    /// `503` at the `cpu_work_max_inflight` ceiling), then waits for the class's
    /// own execution permit and the pod-wide execution ceiling.
    async fn admit_bounded_work(
        &self,
        semaphore: &Arc<tokio::sync::Semaphore>,
        inflight: &Arc<AtomicUsize>,
        work: &'static str,
    ) -> Result<CpuWorkPermit, HttpError> {
        let queue_started = std::time::Instant::now();
        let slot =
            CpuWorkSlot::try_reserve(inflight, self.cpu_work_max_inflight).ok_or_else(|| {
                self.metrics.record_cpu_work_admission(work, "shed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server overloaded".to_string(),
                )
            })?;
        let class_permit = semaphore.clone().acquire_owned().await.map_err(|_| {
            self.metrics.record_cpu_work_admission(work, "shutdown");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "cpu work is shutting down".to_string(),
            )
        })?;
        let global_permit = self
            .cpu_work_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                self.metrics.record_cpu_work_admission(work, "shutdown");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "cpu work is shutting down".to_string(),
                )
            })?;
        self.metrics.record_cpu_work_admission(work, "accepted");
        self.metrics
            .record_cpu_work_queue_duration(work, queue_started.elapsed());
        Ok(CpuWorkPermit {
            _class_permit: class_permit,
            _global_permit: global_permit,
            _slot: slot,
        })
    }

    /// Acquires CPU concurrency for a child stage of an already-admitted
    /// request. Terrain pipeline admission bounds the number of parents and
    /// retained neighborhoods, so child decodes and generation queue here
    /// without consuming or competing for additional `cpu_work_max_inflight`
    /// slots. Keep the returned permit through the blocking job.
    pub(crate) async fn acquire_admitted_cpu_work(
        &self,
        work: &'static str,
    ) -> Result<AdmittedCpuWorkPermit, HttpError> {
        let queue_started = std::time::Instant::now();
        let permit = AdmittedCpuWorkPermit::acquire(
            self.terrain_work_semaphore.clone(),
            self.cpu_work_semaphore.clone(),
        )
        .await
        .map_err(|_| {
            self.metrics.record_cpu_work_admission(work, "shutdown");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                "cpu work is shutting down".to_string(),
            )
        })?;
        self.metrics.record_cpu_work_admission(work, "accepted");
        self.metrics
            .record_cpu_work_queue_duration(work, queue_started.elapsed());
        Ok(permit)
    }

    /// Admits one derived-terrain pipeline (neighborhood fetch through
    /// generation). Unlike [`Self::admit_cpu_work`], this is acquired *before*
    /// the neighborhood fetch: with the supported 512px square DEM contract,
    /// each pipeline can retain at most about 9 MiB of decoded samples while it
    /// fetches and queues for CPU execution. The pipeline count therefore stays
    /// bounded independently of running CPU work; excess parents get 503.
    pub(crate) async fn admit_terrain_pipeline(&self) -> Result<TerrainPipelinePermit, HttpError> {
        let queue_started = std::time::Instant::now();
        let slot =
            CpuWorkSlot::try_reserve(&self.terrain_pipeline_inflight, self.cpu_work_max_inflight)
                .ok_or_else(|| {
                self.metrics
                    .record_cpu_work_admission("terrain_pipeline", "shed");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "server overloaded".to_string(),
                )
            })?;
        let permit = self
            .terrain_pipeline_semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| {
                self.metrics
                    .record_cpu_work_admission("terrain_pipeline", "shutdown");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    "terrain pipeline is shutting down".to_string(),
                )
            })?;
        self.metrics
            .record_cpu_work_admission("terrain_pipeline", "accepted");
        self.metrics
            .record_cpu_work_queue_duration("terrain_pipeline", queue_started.elapsed());
        Ok(TerrainPipelinePermit {
            _permit: permit,
            _slot: slot,
        })
    }

    pub(crate) fn derived_negative_ttl(&self) -> Duration {
        self.derived_negative_ttl
    }

    fn try_start_cache_maintenance(&self) -> Option<CacheMaintenanceGuard> {
        self.cache_maintenance_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .ok()
            .map(|_| CacheMaintenanceGuard {
                running: Arc::clone(&self.cache_maintenance_running),
            })
    }
}

/// Applies the shared middleware stack (drain gate, metrics, request-id) to a
/// router and binds the `AppState`.
fn with_common_layers(router: Router<AppState>, state: AppState) -> Router {
    router
        .fallback(not_found)
        .layer(middleware::from_fn_with_state(
            state.drain.clone(),
            reject_when_draining,
        ))
        // Keep this inside the metrics layer so deadline responses are observed
        // as 504s, and outside the handler/drain gate so it bounds the complete
        // peer-retry plus local-fallback request path.
        .layer(middleware::from_fn(enforce_request_deadline))
        .layer(middleware::from_fn_with_state(
            state.metrics.clone(),
            track_http_metrics,
        ))
        .layer(middleware::from_fn(propagate_request_id))
        .with_state(state)
}

async fn enforce_request_deadline(request: Request, next: Next) -> Response {
    match tokio::time::timeout(HTTP_REQUEST_DEADLINE, next.run(request)).await {
        Ok(response) => response,
        Err(_) => (
            StatusCode::GATEWAY_TIMEOUT,
            "request exceeded the server deadline",
        )
            .into_response(),
    }
}

/// Public-facing routes (served on the Gateway-fronted port): content plus the
/// top-level `/livez` `/readyz` health endpoints (k8s convention, matching the
/// sibling `biei` service). Metrics, `/_internal/*` and peer-to-peer forwarding
/// live only on the internal router so they are never reachable on the public
/// port.
fn public_router() -> Router<AppState> {
    Router::new()
        // Top-level health, mirrored as `/_internal/{healthz,readyz}` on the
        // internal port. Liveness is `/livez`, readiness is `/readyz`.
        .route("/livez", get(healthz))
        .route("/readyz", get(readyz))
        .route(
            "/tilesets/{tileset_id}",
            get(server::tileset::tilejson_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview",
            get(server::tileset::preview_handler),
        )
        .route(
            "/tilesets/{tileset_id}/preview.json",
            get(server::tileset::preview_style_handler),
        )
        .route(
            "/tilesets/{tileset_id}/{z}/{x}/{y}",
            get(server::tileset::tile_handler),
        )
        .route(
            "/tilesets/{tileset_id}/derived/{product}",
            get(server::tileset::derived_tilejson_handler),
        )
        .route(
            "/tilesets/{tileset_id}/derived/{product}/{z}/{x}/{y}",
            get(server::tileset::derived_tile_handler),
        )
        // Namespaced tileset keys ({namespace}/{tileset_id}). Static `preview`
        // / `preview.json` second segments take priority over the namespaced
        // TileJSON route, so they stay reachable as flat-tileset previews.
        .route(
            "/tilesets/{namespace}/{tileset_id}",
            get(server::tileset::namespaced_tilejson_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/preview",
            get(server::tileset::namespaced_preview_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/preview.json",
            get(server::tileset::namespaced_preview_style_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/{z}/{x}/{y}",
            get(server::tileset::namespaced_tile_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/derived/{product}",
            get(server::tileset::namespaced_derived_tilejson_handler),
        )
        .route(
            "/tilesets/{namespace}/{tileset_id}/derived/{product}/{z}/{x}/{y}",
            get(server::tileset::namespaced_derived_tile_handler),
        )
        .route("/styles/{*style_path}", get(server::style::style_handler))
        .route(
            "/fonts/{fontstack}/{range}",
            get(server::glyph::glyph_handler),
        )
}

/// Cluster-internal routes (served on a separate port that is NOT exposed
/// through the Gateway): operational endpoints and peer-to-peer forwarding.
/// All operational endpoints are namespaced under `/_internal/`
/// (`healthz`/`readyz`/`metrics`), matching the sibling `biei` service.
fn internal_router() -> Router<AppState> {
    Router::new()
        .route("/_internal/healthz", get(healthz))
        .route("/_internal/readyz", get(readyz))
        .route("/_internal/metrics", get(metrics_handler))
        .route("/_internal/cluster", get(cluster_handler))
        .route(
            "/_internal/tiles/{tileset_id}/{tile_id}",
            get(server::tileset::internal_tile_handler),
        )
        .route(
            "/_internal/derived/{tileset_id}/{product}/{z}/{x}/{y}",
            get(server::tileset::internal_derived_tile_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/bootstrap",
            get(server::internal::internal_bootstrap_handler),
        )
        .route(
            "/_internal/pmtiles/{tileset_id}/leaf/{offset}/{length}",
            get(server::internal::internal_leaf_handler),
        )
        .route(
            "/_internal/provider/styles/{*style_path}",
            get(server::style::internal_style_handler),
        )
        .route(
            "/_internal/provider/fonts/{fontstack}/{range}",
            get(server::glyph::internal_glyph_handler),
        )
}

/// Builds a `200 OK` response carrying `body` with the given content type and an
/// optional `Cache-Control`. Shared by the glyph / sprite / internal handlers so
/// the status/header boilerplate lives in one place.
pub(crate) fn bytes_response(
    body: impl Into<axum::body::Body>,
    content_type: &'static str,
    cache_control: Option<&'static str>,
) -> Response {
    let mut out = Response::new(body.into());
    *out.status_mut() = StatusCode::OK;
    out.headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    if let Some(cache_control) = cache_control {
        out.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static(cache_control),
        );
    }
    out
}

/// Marks a generated document whose absolute URLs depend on request origin
/// metadata supplied by the client or trusted reverse proxy.
pub(crate) fn apply_origin_vary(headers: &mut HeaderMap) {
    headers.insert(
        header::VARY,
        HeaderValue::from_static("Origin, X-Forwarded-Proto"),
    );
}

/// Serves the public router on `public_addr` (Gateway-fronted) and the internal
/// router on `internal_addr` (cluster-internal: metrics, peer forwarding). Both
/// shut down gracefully on the shared `shutdown` signal.
pub async fn run_http_server(
    state: AppState,
    public_addr: SocketAddr,
    internal_addr: SocketAddr,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let public = with_common_layers(public_router(), state.clone());
    let internal = with_common_layers(internal_router(), state);

    let public_listener = TcpListener::bind(public_addr)
        .await
        .with_context(|| format!("failed to bind public {public_addr}"))?;
    let internal_listener = TcpListener::bind(internal_addr)
        .await
        .with_context(|| format!("failed to bind internal {internal_addr}"))?;

    // Fan the single shutdown signal out to both servers.
    let (sd_tx, _) = tokio::sync::broadcast::channel::<()>(1);
    let mut rx_pub = sd_tx.subscribe();
    let mut rx_internal = sd_tx.subscribe();
    tokio::spawn(async move {
        shutdown.await;
        let _ = sd_tx.send(());
    });

    let public_srv = axum::serve(
        public_listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(public),
    )
    .with_graceful_shutdown(async move {
        let _ = rx_pub.recv().await;
    });
    let internal_srv = axum::serve(
        internal_listener,
        ServiceExt::<axum::http::Request<axum::body::Body>>::into_make_service(internal),
    )
    .with_graceful_shutdown(async move {
        let _ = rx_internal.recv().await;
    });

    // try_join! so an unexpected listener error surfaces immediately and the
    // other server is dropped, rather than blocking until both finish.
    tokio::try_join!(
        async { public_srv.await.context("public http server failed") },
        async { internal_srv.await.context("internal http server failed") },
    )?;
    Ok(())
}

pub(crate) fn get_origin(headers: &HeaderMap) -> String {
    let origin = headers
        .get(axum::http::header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty());
    let origin_parts = origin.and_then(split_origin);
    let scheme = headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .or_else(|| origin_parts.map(|(origin_scheme, _)| origin_scheme))
        // Reflect only real web schemes. A spoofed `X-Forwarded-Proto` such as
        // `https://attacker/x?` would otherwise be interpolated as the scheme and
        // point emitted glyph/sprite/tile URLs off-origin.
        .filter(|value| is_reflectable_scheme(value))
        .unwrap_or("http");
    let host = headers
        .get(axum::http::header::HOST)
        .and_then(|value| value.to_str().ok())
        .filter(|value| is_reflectable_host(value))
        .or_else(|| {
            origin_parts
                .map(|(_, origin_host)| origin_host)
                .filter(|value| is_reflectable_host(value))
        })
        .unwrap_or("127.0.0.1:8080");
    format!("{scheme}://{host}")
}

/// Whether a client-supplied `Host`/`Origin` host is safe to interpolate into
/// emitted URLs (TileJSON `tiles`, style `glyphs`/`sprite`/source URLs). A spoofed
/// `Host` is otherwise reflected verbatim — a header-injection / reflected-URL
/// vector — so restrict it to the characters a real authority can contain.
fn is_reflectable_host(host: &str) -> bool {
    !host.is_empty()
        && host.len() <= 255
        && host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b':' | b'_'))
}

/// Whether a client-supplied forwarded scheme is safe to reflect into emitted
/// URLs. Only `http`/`https`; anything else falls back to the default.
fn is_reflectable_scheme(scheme: &str) -> bool {
    scheme.eq_ignore_ascii_case("http") || scheme.eq_ignore_ascii_case("https")
}

/// Reports whether this node process is alive.
async fn healthz() -> StatusCode {
    StatusCode::OK
}

/// Reports whether this node is ready to receive traffic.
async fn readyz(State(state): State<AppState>) -> StatusCode {
    // Chitchat initialization completes before the HTTP server starts. A node
    // can serve from object storage even when it is the cluster's only member,
    // so readiness is gated only by the process-local drain state.
    if state.drain.is_draining() {
        StatusCode::SERVICE_UNAVAILABLE
    } else {
        StatusCode::OK
    }
}

/// Serves the default 404 response for unknown routes.
async fn not_found() -> (StatusCode, &'static str) {
    (StatusCode::NOT_FOUND, "not found\n")
}

/// Returns the current cluster membership snapshot.
async fn cluster_handler(State(state): State<AppState>) -> Json<crate::membership::ClusterView> {
    Json(state.membership.cluster_view().await)
}

/// Rejects new data and peer-forwarding requests with `503` while draining, so
/// callers fail over quickly. In-flight requests already past this layer finish
/// normally, and operational endpoints stay available.
async fn reject_when_draining(
    State(drain): State<DrainController>,
    request: Request,
    next: Next,
) -> Response {
    if drain.is_draining() && drain::is_drainable_path(request.uri().path()) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, "1")],
            "draining\n",
        )
            .into_response();
    }
    next.run(request).await
}

/// Accepts or generates an `X-Request-Id`, scopes it for the task, attaches it
/// to a tracing span, and echoes it on the response.
async fn propagate_request_id(request: Request, next: Next) -> Response {
    let incoming = request
        .headers()
        .get(request_id::HEADER)
        .and_then(|value| value.to_str().ok());
    let id = request_id::accept_or_generate(incoming);

    let header_value = HeaderValue::from_str(&id).ok();
    let span = tracing::info_span!("request", request_id = %id);
    let mut response = request_id::REQUEST_ID
        .scope(id, next.run(request).instrument(span))
        .await;

    if let Some(value) = header_value {
        response
            .headers_mut()
            .insert(HeaderName::from_static(request_id::HEADER), value);
    }
    response
}

/// Records each request against its matched route pattern and status code.
async fn track_http_metrics(
    State(metrics): State<NodeMetrics>,
    matched: Option<MatchedPath>,
    request: Request,
    next: Next,
) -> Response {
    let started = std::time::Instant::now();
    let internal_resource = crate::storage::internal_resource_kind(request.uri().path());
    let endpoint = matched
        .as_ref()
        .map_or("unknown", MatchedPath::as_str)
        .to_string();
    let response = next.run(request).await;
    // Exclude the scrape itself: its handler performs cache-gauge maintenance,
    // and recording that work in the exported histogram makes scrape latency
    // self-referential on the following scrape.
    if endpoint == "/_internal/metrics" {
        metrics.record_http_request(&endpoint, response.status().as_u16());
    } else {
        metrics.record_http(&endpoint, response.status().as_u16(), started.elapsed());
    }
    if let Some(resource) = internal_resource {
        let outcome = match response.status() {
            status if status.is_success() => "success",
            StatusCode::NOT_FOUND => "not_found",
            StatusCode::TOO_MANY_REQUESTS
            | StatusCode::SERVICE_UNAVAILABLE
            | StatusCode::GATEWAY_TIMEOUT => "retryable",
            _ => "error",
        };
        metrics.record_internal_resource_request(resource, outcome);
    }
    response
}

/// Serves the Prometheus exposition, refreshing point-in-time gauges first.
async fn metrics_handler(State(state): State<AppState>) -> impl IntoResponse {
    let view = state.membership.cluster_view().await;
    // Moka updates weighted size through deferred maintenance. Flush once for
    // concurrent scrapes and run the independent caches in parallel.
    if let Some(_guard) = state.try_start_cache_maintenance() {
        tokio::join!(
            state.mlt_cache.run_pending_tasks(),
            state.derived_tile_cache.run_pending_tasks(),
            state.dem_tile_cache.run_pending_tasks(),
        );
    }
    state
        .metrics
        .set_membership(view.live_ids.len() as i64, view.dead_ids.len() as i64);
    state.metrics.set_drain(state.drain.is_draining());
    // Report each CPU class from real counters, plus an `all` aggregate.
    // `running` is derived from live semaphore occupancy, `inflight` from the
    // class backlog counters — so a saturated pod shows up instead of the old
    // always-zero aggregate.
    let cpu_classes = [
        (
            "terrain",
            &state.terrain_pipeline_inflight,
            &state.terrain_work_semaphore,
            state.terrain_work_concurrency,
        ),
        (
            "provider",
            &state.provider_work_inflight,
            &state.provider_work_semaphore,
            state.provider_work_concurrency,
        ),
        (
            "transcode",
            &state.transcode_work_inflight,
            &state.transcode_work_semaphore,
            state.transcode_work_concurrency,
        ),
    ];
    let mut all_inflight = 0usize;
    for (class, inflight, semaphore, concurrency) in cpu_classes {
        let inflight = inflight.load(Ordering::Relaxed);
        let running = concurrency.saturating_sub(semaphore.available_permits());
        all_inflight = all_inflight.saturating_add(inflight);
        state.metrics.set_cpu_work(
            class,
            inflight,
            running,
            concurrency,
            state.cpu_work_max_inflight,
        );
    }
    // Aggregate `running` comes from the pod ceiling itself, not the per-class
    // sum: on a tiny pod the class shares sum above the true `N`, so the ceiling
    // is the honest total.
    state.metrics.set_cpu_work(
        "all",
        all_inflight,
        state
            .cpu_work_concurrency
            .saturating_sub(state.cpu_work_semaphore.available_permits()),
        state.cpu_work_concurrency,
        state
            .cpu_work_max_inflight
            .saturating_mul(CPU_WORK_CLASS_COUNT),
    );
    for (cache, bytes) in [
        ("tile", state.resource_resolver.tile_cache_weighted_size()),
        ("chunk", state.resource_resolver.chunk_cache_weighted_size()),
        ("provider", state.provider_fetch_cache.weighted_size()),
        ("mlt", state.mlt_cache.weighted_size()),
        ("derived", state.derived_tile_cache.weighted_size()),
        ("dem", state.dem_tile_cache.weighted_size()),
    ] {
        state.metrics.set_cache_bytes(cache, bytes);
    }
    state
        .metrics
        .sync_backend_fetch_bytes(state.resource_resolver.received_bytes());
    (
        [(header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        state.metrics.encode(),
    )
}

/// Splits an Origin header into scheme and host components.
fn split_origin(origin: &str) -> Option<(&str, &str)> {
    let (scheme, rest) = origin.split_once("://")?;
    let host = rest.split('/').next()?;
    if scheme.is_empty() || host.is_empty() {
        return None;
    }
    Some((scheme, host))
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use super::{
        AdmittedCpuWorkPermit, CpuWorkSlot, DecodedDemExpiry, DerivedTileExpiry,
        decoded_dem_cache_weight, derived_tile_cache_weight, enforce_request_deadline, get_origin,
        is_reflectable_host, mlt_cache_weight,
    };
    use axum::http::{HeaderValue, header};
    use moka::Expiry;

    use crate::server::tileset::terrain::DerivedOutcome;

    #[test]
    fn cpu_work_admission_sheds_at_ceiling_and_releases_on_drop() {
        let inflight = Arc::new(AtomicUsize::new(0));
        // Fill the two slots.
        let first = CpuWorkSlot::try_reserve(&inflight, 2).expect("first slot");
        let second = CpuWorkSlot::try_reserve(&inflight, 2).expect("second slot");
        // The third is shed while the counter is at its ceiling, and the failed
        // reservation must not leave the counter inflated.
        assert!(CpuWorkSlot::try_reserve(&inflight, 2).is_none());
        assert_eq!(inflight.load(std::sync::atomic::Ordering::Relaxed), 2);
        // Freeing one slot re-opens admission.
        drop(first);
        let third = CpuWorkSlot::try_reserve(&inflight, 2).expect("slot after release");
        drop(second);
        drop(third);
        assert_eq!(inflight.load(std::sync::atomic::Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn cpu_partition_isolates_classes_at_three_permits() {
        // N = 3: terrain share 1, provider share 1, global 3. A saturated
        // terrain class (holding its 1 class permit + 1 global) must not block
        // the provider class — the class shares reserve provider's slot.
        let terrain = Arc::new(tokio::sync::Semaphore::new(1));
        let provider = Arc::new(tokio::sync::Semaphore::new(1));
        let global = Arc::new(tokio::sync::Semaphore::new(3));
        let _terrain = AdmittedCpuWorkPermit::acquire(terrain, global.clone())
            .await
            .expect("terrain permit");

        tokio::time::timeout(
            Duration::from_millis(200),
            AdmittedCpuWorkPermit::acquire(provider, global),
        )
        .await
        .expect("provider must not wait on a saturated terrain class at N=3")
        .expect("provider permit");
    }

    #[tokio::test]
    async fn cpu_pod_ceiling_binds_below_three_permits() {
        // N = 1: class shares floor at 1 each (sum 3) but the pod ceiling is 1,
        // so total concurrent CPU work is 1 — the cgroup limit holds even though
        // full class isolation is impossible at a single permit.
        let terrain = Arc::new(tokio::sync::Semaphore::new(1));
        let provider = Arc::new(tokio::sync::Semaphore::new(1));
        let global = Arc::new(tokio::sync::Semaphore::new(1));
        let _terrain = AdmittedCpuWorkPermit::acquire(terrain, global.clone())
            .await
            .expect("terrain permit");

        let waiter = {
            let global = global.clone();
            tokio::spawn(async move { AdmittedCpuWorkPermit::acquire(provider, global).await })
        };
        tokio::task::yield_now().await;
        assert!(
            !waiter.is_finished(),
            "the pod ceiling of one must serialize CPU work"
        );
        waiter.abort();
    }

    #[test]
    fn degraded_and_absent_cache_weights_have_a_floor_and_charge_payload() {
        let key = crate::server::tileset::terrain::DerivedTileKey::for_test();
        let tiny = DerivedOutcome::Degraded(crate::pmtiles::TileData {
            bytes: bytes::Bytes::new(),
            content_type: "application/vnd.mapbox-vector-tile",
            content_encoding: None,
        });
        assert!(derived_tile_cache_weight(&key, &tiny) >= 128);
        assert!(derived_tile_cache_weight(&key, &DerivedOutcome::Absent) >= 128);

        let payload_len = 4096;
        let large = DerivedOutcome::Degraded(crate::pmtiles::TileData {
            bytes: bytes::Bytes::from(vec![0; payload_len]),
            content_type: "application/vnd.mapbox-vector-tile",
            content_encoding: None,
        });
        assert!(derived_tile_cache_weight(&key, &large) > payload_len as u32);

        let dem_key = (crate::interned::TilesetId::new_unchecked("terrain"), 1);
        assert!(decoded_dem_cache_weight(&dem_key, &None) >= 128);
    }

    #[tokio::test]
    async fn tiny_mlt_entries_charge_overhead_and_obey_capacity() {
        let tiny = bytes::Bytes::new();
        let short_key = (crate::interned::TilesetId::new_unchecked("a"), 1);
        assert_eq!(mlt_cache_weight(&short_key, &tiny), 128);

        let long_key = (
            crate::interned::TilesetId::new_unchecked(&"a".repeat(256)),
            2,
        );
        assert!(mlt_cache_weight(&long_key, &tiny) > 128);

        let cache = moka::future::Cache::builder()
            .max_capacity(256)
            .weigher(mlt_cache_weight)
            .build();
        for tile_id in 0..3 {
            cache
                .insert(
                    (crate::interned::TilesetId::new_unchecked("tiny"), tile_id),
                    tiny.clone(),
                )
                .await;
        }
        cache.run_pending_tasks().await;
        assert!(cache.weighted_size() <= 256);
        assert!(cache.entry_count() <= 2);
    }

    #[tokio::test(start_paused = true)]
    async fn server_deadline_cancels_a_stuck_request() {
        use axum::{Router, body::Body, http::Request, middleware, routing::get};
        use tower::ServiceExt;

        let router = Router::new()
            .route(
                "/stuck",
                get(|| async { std::future::pending::<&'static str>().await }),
            )
            .layer(middleware::from_fn(enforce_request_deadline));
        let response = router
            .oneshot(
                Request::builder()
                    .uri("/stuck")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::GATEWAY_TIMEOUT);
    }

    #[test]
    fn rejects_hosts_with_injection_chars() {
        assert!(is_reflectable_host("ishikari-demo.mierune.dev"));
        assert!(is_reflectable_host("127.0.0.1:8080"));
        assert!(!is_reflectable_host("evil.test/path"));
        assert!(!is_reflectable_host("evil.test foo"));
        assert!(!is_reflectable_host(""));
    }

    #[test]
    fn get_origin_does_not_reflect_a_spoofed_host() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example:8080"));
        assert_eq!(get_origin(&headers), "http://good.example:8080");

        // A `Host` carrying a path separator is dropped, not reflected verbatim.
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("a.test/evil"));
        assert_eq!(get_origin(&headers), "http://127.0.0.1:8080");
    }

    #[test]
    fn get_origin_rejects_spoofed_forwarded_scheme() {
        let mut headers = axum::http::HeaderMap::new();
        headers.insert(header::HOST, HeaderValue::from_static("good.example"));
        // A forwarded-proto that smuggles an authority is not reflected as the
        // scheme; it falls back to the default `http`.
        headers.insert(
            "x-forwarded-proto",
            HeaderValue::from_static("https://attacker.example/x?"),
        );
        assert_eq!(get_origin(&headers), "http://good.example");

        // A legitimate forwarded scheme is honored.
        headers.insert("x-forwarded-proto", HeaderValue::from_static("https"));
        assert_eq!(get_origin(&headers), "https://good.example");
    }

    #[test]
    fn only_negative_derived_results_expire() {
        let expiry = DerivedTileExpiry {
            negative_ttl: Duration::from_secs(45),
        };
        let key = crate::server::tileset::terrain::DerivedTileKey::for_test();
        assert_eq!(
            expiry.expire_after_create(&key, &DerivedOutcome::Absent, Instant::now(),),
            Some(Duration::from_secs(45))
        );
        assert_eq!(
            expiry.expire_after_create(
                &key,
                &DerivedOutcome::Tile(crate::pmtiles::TileData {
                    bytes: bytes::Bytes::new(),
                    content_type: "application/vnd.mapbox-vector-tile",
                    content_encoding: None,
                }),
                Instant::now(),
            ),
            None
        );
        // A tile generated after a transient failure or mutable in-world
        // neighbor absence re-resolves on the same short TTL as a center
        // absence, so the seam heals instead of persisting until eviction.
        assert_eq!(
            expiry.expire_after_create(
                &key,
                &DerivedOutcome::Degraded(crate::pmtiles::TileData {
                    bytes: bytes::Bytes::new(),
                    content_type: "application/vnd.mapbox-vector-tile",
                    content_encoding: None,
                }),
                Instant::now(),
            ),
            Some(Duration::from_secs(45))
        );
    }

    #[test]
    fn absent_decoded_dems_expire() {
        let expiry = DecodedDemExpiry {
            negative_ttl: Duration::from_secs(30),
        };
        let key = (crate::interned::TilesetId::new_unchecked("terrain"), 1);
        assert_eq!(
            expiry.expire_after_create(&key, &None, Instant::now()),
            Some(Duration::from_secs(30))
        );
    }
}

pub(crate) mod cache;
pub(crate) mod conditional;
#[cfg(test)]
mod contract_tests;
pub(crate) mod glyph;
pub mod internal;
pub mod provider;
mod provider_body;
mod provider_cache_policy;
pub(crate) mod sprite;
pub(crate) mod style;
pub mod tileset;
pub(crate) mod upstream;

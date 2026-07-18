# Ishikari Simulator Spec

A specification and current-status record for a deterministic, trace-driven
simulator that answers quantitative
questions about Ishikari's distributed cache behavior — hit rates, backend
egress, node-churn recovery, chunk batching efficiency, and load skew — under a
realistic workload derived from Japan's population distribution.

The simulator's primary purpose is to approximate a real deployment **without
provisioning the corresponding nodes, memory, object-store traffic, or
wall-clock runtime**. Exact reproduction is impossible; the goal is to account
for every important resource and decision with the cheapest faithful model:

- reuse production policy and algorithms where their behavior matters;
- represent cache capacity and payload size as logical weights when real bytes
  are unnecessary;
- advance queueing, network delay, and user think time on a virtual clock;
- calibrate stochastic cost models from cloud measurements; and
- use real-cluster tests only to fit and validate the model, not as the normal
  way to answer capacity questions.

This follows the biei-sim design principle: production policy should stay in
the main crate and be consumed through narrow seams. Real-cache mode executes
the production resolver directly. Modeled-cache mode instead implements a
metadata-only request flow while reusing production HRW routing, PMTiles access
planning, chunk-range planning, Moka eviction policies, and byte weights. That
approximation is calibrated against real-cache runs; it must not be described
as identical production execution. Any behavior that is neither executed from
production code nor represented by a measured model is an explicit limitation,
not an implicit zero-cost operation.

---

## 1. Motivation — questions the simulator must answer

Each scenario in §6 maps back to one of these. All are cheap to answer with a
simulator ("sweep 100 configs in a minute") and expensive to answer with a real
cluster ("one config per load-test run").

- **Q1 — Group size / candidate count trade-off.** How do
  `ISKR_ROUTER_TILE_GROUP_SIZE` (default 512) and `ISKR_ROUTER_TOP_K`
  (default 3) shape hit rate, peer-forward rate, and per-node load skew?
  Note: candidates beyond the first only matter on failure/drain, so the
  expected answer for load spread is "barely" — quantify it.
- **Q2 — Churn miss tsunami.** When a node joins or leaves (spot preemption,
  rolling deploy), what fraction of cached groups gets re-routed, how deep is
  the hit-rate dip, and how many requests does recovery take? The demo runs on
  Spot nodes, so this is a real production question.
- **Q3 — Cache sizing curves.** Hit rate and backend egress as a function of
  `tile_cache_max_bytes` × `chunk_cache_max_bytes` × node count × workload
  concentration. Produces the memory-sizing rationale we currently don't have.
- **Q4 — Chunk batching × Hilbert locality.** How far do
  `chunk_size_bytes` / `max_fetch_chunks` (with `MAX_CHUNK_GAP = 1` gap
  prefetch) cut backend GET count under a viewport workload, and where does
  read amplification (bytes fetched / bytes used) start to hurt? Under Phase 2,
  also sweep `chunk_fetch_merge_window_ms` and `backend_fetch_concurrency`, then
  inspect end-user latency and backend admission queue p95 rather than assuming
  every planned range starts immediately.
- **Q5 — Derived-tile generation queueing** (Phase 2 only). How does
  contour/hillshade generation (~100–500 ms/tile) behave under the
  pod-wide `ISKR_CPU_WORK_CONCURRENCY` execution ceiling and class-local
  `ISKR_CPU_WORK_MAX_INFLIGHT` admission limit with viewport request patterns?
  This is not modeled yet.

## 2. Approach — two phases

Execution phase and cache representation are independent concerns. `real`
cache mode is a small-scale reference oracle that exercises payload-bearing
production paths. `modeled` cache mode is the intended path for large node,
memory, and request-count sweeps: it preserves production placement, eviction,
weighting, and range-planning decisions while avoiding allocation of the
corresponding payload bytes.

**Phase 1A (this spec's default): deterministic trace-driven simulation.**
Requests are executed one at a time on a single-threaded Tokio runtime;
results are counted, not timed. This answers Q1–Q3: hit rates, egress, and
rebalance are functions of *request order*, not of latency.

**Phase 1B (Q4 only): deterministic viewport batches with paused Tokio time.**
All newly visible tiles from one viewport step are polled together, preserving
their deterministic `(dy, dx)` order. `tokio::time` starts paused so the real
configured fetch merge window (10 ms by default) advances without wall-clock
delay after every ready request has had a chance to enqueue its missing chunks.
This exercises the
production single-flight, gap merge, and `max_fetch_chunks` behavior in
real-cache mode without introducing a latency distribution. Modeled-cache mode
uses its metadata-only range planner instead.

**Phase 2: Tokio paused-time simulation** (biei-sim style) for questions where
latency and queueing matter. VUs run concurrently as closed users: one viewport
batch completes, then the VU sleeps for `1.2 +/- 0.5 s`. Imported sparse steps
expand into at most 1,000,000 transitions per user and 10,000,000 globally,
using checked arithmetic. Per-step deterministic think durations are summed into
one sleep before the next emitted batch, preserving its arrival timestamp
without scheduler-only wakeups. The runner uses the
real resolver, caches, peer transport, single-flight, merge window, and backend
fetch concurrency limit. Fixed backend and peer delays are controlled sweep
inputs; fitted lognormal backend profiles are implemented. Terrain generation
and shared CPU-admission queueing remain future work.

## 3. Workload model — population-driven viewport walk

The population-driven viewport model is implemented in Rust so one
deterministic trace can drive both simulator sweeps and real-cluster validation
runs.

### 3.1 Data

`ishikari-sim/data/census_2020_1km_population.geojson` (~18.5 MB): 2020
national census, 1 km (1/80°) mesh centroids with a `population` property.
Loaded at startup into a cumulative-weight array
(`Vec<(lng, lat, cum_weight)>`).
Zero/negative-population meshes are dropped.

### 3.2 Session state machine (per simulated user)

The workload contract uses these parameters and defaults:

| Parameter | Default | Meaning |
|---|---|---|
| `min_zoom` / `max_zoom` | 4 / 15 | zoom sampling range |
| `focus_zoom` | 13 | Gaussian zoom-weight center |
| `zoom_sigma` | 1.8 | Gaussian zoom-weight sigma |
| `session_reset_probability` | 0.07 | per-step chance to teleport & re-roll zoom |
| `zoom_walk_probability` | 0.0 | per non-reset step chance to replace pan with `z±1` at the same center |
| `move_step_tiles` | 1.0 | pan step, in tiles at current zoom |
| `viewport` | 3×3 | tiles fetched around the center tile |
| `users` | scenario-defined | number of concurrent session walkers |

Per step:

1. With probability `session_reset_probability` (or on first step): sample a
   mesh centroid from the population CDF by binary search, jitter uniformly
   within the 1/80° mesh cell, sample zoom `z` from the Gaussian weights over
   `[min_zoom, max_zoom]`. Reset the previous-viewport set.
2. Otherwise, with probability `zoom_walk_probability`: keep longitude/latitude
   fixed and move exactly one level to `z-1` or `z+1`. Interior directions are
   equiprobable; bounds reflect toward the only valid adjacent level. Zoom
   replaces pan for that step and requires `min_zoom < max_zoom`.
3. Otherwise: random-walk the position in Web Mercator unit space by
   `±move_step_tiles / 2^z` per axis.
4. Emit the 3×3 viewport around the center tile (x wrapped, y clamped),
   **deduplicated against the previous step's viewport set** — only newly
   visible tiles become requests, exactly like a map client.

Zoom-walk event and direction decisions are deterministic, domain-separated
functions of `(seed, user, step)`. Non-reset steps always consume the legacy pan
RNG draws even when zooming, so enabling the knob does not shift later reset
locations, reset zooms, or the underlying pan stream. Parent/child requests are
therefore introduced without confounding unrelated randomness. The immediately
previous viewport remains the only deduplication set; a changed zoom naturally
emits the new level's viewport.

### 3.3 Interleaving and traces

- Users are stepped round-robin. Phase 1A executes each step's new tiles in
  deterministic `(dy, dx)` order; Phase 1B polls that ordered list as one
  viewport batch. Think time does not exist in either mode.
- The generator is a pure function of `(census file, params, seed)`. A run can
  optionally dump the request sequence as a trace file
  (`jsonl: {step, user, ordinal, tileset, z, x, y}`) so that:
  - the exact same trace can be replayed across config sweeps
    (variance-free A/B), and
  - the trace can be exported for replay against a **real** cluster through the
    versioned `replay-http` runner for cross-validation (§8).
- Entry-node choice: each request is assigned an entry node, modeling the
  Gateway LB. Knob `entry_affinity = per_request | per_session`
  (default `per_request` uniform; Google Cloud Application Load Balancing
  performs backend selection per request, while `per_session` is a sensitivity
  model for connection affinity). Entry selection is derived from
  `(seed, request index or user, node count)` and may be materialized as
  `entry_node` when exporting a trace for direct-to-node cluster replay. This
  matters because a peer-
  forwarded tile is inserted into the entry node's L1 *as well as* living on
  the owner — replication that reduces effective cluster-wide cache capacity.
- Imported JSONL is bounded before allocation to 4 KiB of payload per line and
  256 MiB of raw input including blank lines and terminators. It is also limited
  to 2,000,000 requests, 10,000 users, and 64 requests per viewport batch.
  Structural validation accepts sparse monotonically increasing steps; only
  Phase 2 expands and bounds step transitions as described above.

## 4. System model (Phase 1)

### 4.1 What runs for real

In real-cache mode, the simulator instantiates **N real in-process node
instances** — each with its own production `ResourceResolver` (tile cache,
resource cache, `ChunkedStore` with chunk cache + fetch coordinator, PMTiles
`Reader`) and production `Membership`. It replaces internal HTTP and UDP with
in-process transports, but runs the real chitchat state machine over Tokio's
virtual clock. Everything that decides *what* to fetch, cache, group, forward,
or expose as a live peer is production code:

- `HrwRouter` (tile→group→candidates, XxHash64 HRW) — as is.
- `Reader` (bootstrap = header + root dir, leaf directory resolution,
  `z/x/y → tile_id` Hilbert mapping) — as is. Directory read amplification is
  therefore modeled exactly.
- `ChunkedStore` + `ChunkFetchCoordinator` (chunk cache, missing-chunk
  grouping with `MAX_CHUNK_GAP`, `max_fetch_chunks` cap, single-flight) — as is.
- `TileCache` / `ChunkCache` / `ResourceCache` (moka, byte-weighted, including
  negative entries) — as is.
- The full `route_tile` flow in `ResourceResolver` (L1 check → HRW → peer
  forward in score order → local fallback → cache insert on both sides) — as is.

Backing data is the real local archive (`data/japan.pmtiles`, 3.5 GB) via
`object_store::LocalFileSystem`, so tile sizes, directory layout, sparseness
(real 404s / negative caching), and Hilbert offsets are all real. No synthetic
archive model.

For capacity and node-count sweeps, `cache_mode=modeled` first builds a catalog
for the trace's unique tiles by resolving the real PMTiles bootstrap and leaf
directories without reading tile payloads. Each modeled node then keeps tiny
metadata values in Moka while assigning them the production logical byte
weights. Tile caches retain TinyLFU behavior; chunk caches retain LRU behavior
and the production 1 GiB cap. HRW placement and production chunk-range planning
are reused. This decouples resident memory from configured logical capacity.
`cache_mode=real` remains the calibration path for request-level fidelity.

### 4.2 Production seams

The current implementation uses three narrow boundaries:

1. **Peer transport.** `PeerBackend` constructs typed `/_internal/*` paths and
   calls `InternalTransport`. Production uses the reqwest implementation; the
   simulator resolves the target node in-process and calls the same resolver
   operations wrapped by the internal HTTP endpoints. Transport failures and
   fixed peer latency are injected at this boundary.
2. **Peer directory.** `PeerBackend` reads candidates through `PeerDirectory`.
   Production and real-cache simulation use `Membership`; every real-cache node
   owns an independent production chitchat instance. Modeled mode applies its
   cheaper active-node set immediately at event boundaries.
3. **Simulator support exports.** The `simulator-support` feature exposes the
   PMTiles access plan, chunk-range planner, routing primitives, and injected
   resolver constructors required by modeled and in-process runs. Normal
   Ishikari builds do not expose those simulator-only APIs.

No production `Clock` seam is needed. Phase 1A does not claim to model
cross-request merging. Phase 1B uses Tokio's paused clock to exercise the real
configured merge window without sleeping in wall-clock time. Production reads
it from `ISKR_CHUNK_FETCH_MERGE_WINDOW_MS`; simulator real-cache runs use
`ClusterConfig::chunk_fetch_merge_window_ms`. Zero removes the intentional wait
without disabling pending/inflight sharing or immediate bootstrap dispatch. The
shared production validator rejects values above 1000 ms in both modes.

### 4.3 Per-request flow

For each trace entry `(user, z, x, y)` in Phase 1A, or viewport batch in Phase
1B:

1. Pick the entry node per §3.3 and call its `ResourceResolver::route_tile`.
2. Production code does the rest. A thin `SimNode::serve_tile` adapter records
   the returned `TileSource`, its `cache_outcomes`, successful response bytes,
   and peer-transfer bytes. This mirrors the metric calls normally made by the
   HTTP handlers; `ResourceResolver::route_tile` does not increment those
   counters itself.
3. Phase 1A awaits completion before the next request. Phase 1B joins the
   ordered viewport futures before advancing to the next user step.

## 5. Metrics & output

Per run, per node, harvested from each node's Prometheus registry
(`NodeMetrics`) plus sim-side counters:

- Tiles served by source (`self_cache` / `peer_cache` / `self_backend` /
  `peer_backend` / `miss`) — headline **cache hit rate**, **L1 hit rate**, and
  **peer-forward rate**.
- Chunk cache hit / miss / post-fetch-hit; **backend GET count** and
  **backend bytes** (egress proxy); **read amplification** = backend bytes /
  tile bytes served.
- Peer-transfer bytes (east-west traffic).
- Gossip messages/bytes and unavailable-peer attempts caused by stale views.
- Peer forwarding outcomes and backoff skips from the production
  `NodeMetrics`, so failover attempts avoided during convergence are visible.
- Identical in-flight peer fetch overlap by resource kind, so additional
  response coalescing is considered only when measured fan-in justifies it.
- Per-node inbound and outbound internal-resource counts, classified by the
  shared production path classifier, for tile/index/provider hotspot analysis.
- Per-sample membership convergence: converged/stale node views, missing and
  extra peer references, min/max peer count, and virtual elapsed time.
- Cache occupancy (`tile_cache_weighted_size`, `chunk_cache_weighted_size`).
- **Load skew**: per-node share of local loads; report max/mean and CV.
- Backend scheduler distributions: fetch duration/size/chunks, queue delay,
  pending chunks at dispatch, and waiters released per fetch group. Structured
  histogram snapshots are merged without parsing Prometheus text.

Normal simulation reports use report schema v2 and output one self-contained
JSON document containing execution mode, tagged trace source (`generated` with
workload config, or `replay` with input path), cluster config, aggregate metrics,
and per-node summaries. `l1_cache_hits` counts positive entry-node L1 hits;
`negative_cache_hits` counts existing negative-entry hits separately. Generated
request traces are written separately as
JSONL and can be replayed in-process with either serial or viewport-batch
execution.

Churn replay emits configurable counter snapshots (default every 1,000
requests), plus paired pre/post-event snapshots. Samples include explicit
`l1_cache_hits` and `negative_cache_hits`; legacy `cache_hits` is retained as
their cumulative sum. They also include active cache occupancy, per-node request
counters, virtual elapsed time, and each node's agreement with the active
membership set, so hit-rate, backend-fetch, load-skew, gossip-convergence, and
recovery curves can be derived.

The sweep runner accepts sweep-spec schema v1, one replay
trace, entry-assignment seeds, a base cluster configuration, and Cartesian axes
for node count, candidate count, tile-group size, chunk size/range cap, tile and
chunk cache capacity, and entry-node peer caching. It builds one shared modeled
PMTiles catalog, executes each cell in a fresh modeled cluster, and writes one
report-schema-v2 run document per JSONL line to a sibling temporary file. The
destination is atomically replaced only after all cells succeed. Every line records
`sweep_spec_schema_version: 1`, the run index and count, effective configuration,
periodic samples, simulator version, and FNV-1a spec/trace fingerprints. Paths
are resolved relative to the spec. Duplicate axis values, invalid cluster
dimensions, unsupported schema versions, and grids larger than `max_runs` are
rejected before temporary output is created. `max_runs` may lower, but cannot
raise, the hard 10,000-run ceiling; the JSON sweep spec itself is limited to
1 MiB before parsing.

Every simulator file output rejects canonical, symlink, and hard-link identity
with its protected inputs. Generated traces, normal reports, HTTP replay
reports, sweep results, and visualizations are published by a same-directory
temporary-file rename, so validation or serialization failure cannot truncate
an input or a previously complete artifact.

Sweep-spec v1 is intentionally replay-only and modeled-cache-only. Sweep cells
run sequentially, but a cell can use serial or viewport-batch request execution.
Timed controls (`chunk_fetch_merge_window_ms`, backend latency/concurrency),
workload-generation axes, real-cache lifecycle, and sweep-level visualization
are not claimed by this runner. Modeled reports retain timed configuration for
provenance but do not treat it as an effective sweep axis.

## 6. Scenario catalog

Each scenario = a config grid × the workload of §3, run on the same seed(s).

- **S1 — Steady-state sizing (Q1, Q3).** First run a baseline plus one-factor
  sweeps, then run the full interactions only over the surviving Pareto
  candidates. Warm-up ends when bucketed hit rate and backend GET rate remain
  within a configured tolerance for several buckets (with a hard request cap);
  measurement then uses a fixed request count.
  Grid: `nodes ∈ {2,3,5,8}` × `tile_group_size ∈ {32,128,512,2048,8192}` ×
  `tile_cache ∈ {64M,256M,512M,1G}` × `chunk_cache ∈ {64M,256M,512M,1G}` ×
  `users ∈ {50, 500, 5000}` (workload concentration). Outputs: hit rate,
  egress, skew per cell. Decision informed: default group size, memory
  requests in the demo deployment.
- **S2 — Churn (Q2).** Warm cluster of N; at request index K: (a) add one
  empty node, (b) kill one node (spot preemption; its caches vanish),
  (c) rolling restart (serial leave→rejoin-empty for each node, drain
  semantics = node absent from peer list), (d) correlated majority loss
  (for example 10→3 at one viewport boundary). Measure: fraction of tile-groups
  re-routed (compare with analytic ~1/N), hit-rate dip depth, requests to
  recover within 1 pt of steady state, backend egress spike. Sweep
  `tile_group_size` — bigger groups mean fewer, bigger invalidations.
- **S3 — Chunk batching (Q4).** Modeled grid: `chunk_size_bytes ∈
  {256K,1M,4M}` × `max_fetch_chunks ∈ {1,4,8,16}` × zoom mix (`focus_zoom ∈
  {11,13,15}`). Real-cache/Phase 2 grid additionally varies
  `chunk_fetch_merge_window_ms ∈ {0,1,5,10,25}` and backend concurrency.
  Outputs: end-user latency, backend GETs per 1k tiles, read amplification,
  chunk-cache hit rate, backend queue p95, and waiter fan-in. Generate separate
  otherwise-identical traces for `zoom_walk_probability ∈ {0,0.05,0.1,0.2}` to
  see how cross-zoom requests break Hilbert locality; sweep-spec v1 is
  replay-only and intentionally does not vary workload-generation parameters.
- **S4 — Skew under population workload (Q1).** No failures: quantify how
  uneven HRW + tile groups + a Tokyo-heavy CDF make per-node load, and confirm
  `candidate_count` does *not* spread load (it's failover-only). Then repeat
  with one node marked failing (transport decorator) to see spillover onto
  candidate #2.
- **S5 — Cold start.** All caches empty, no churn: requests until hit rate
  plateaus. Informs deploy-time behavior and warm-up expectations.
- **S6 — Terrain generation queueing (Q5). Planned for Phase 2.**

## 7. Determinism

- Seeded `rand::rngs::StdRng`, one stream per user derived
  from `(seed, user_index)` so adding users doesn't perturb existing streams.
- Single-threaded `tokio::runtime::Builder::new_current_thread()`. Phase 1A is
  fully serialized. Phase 1B polls one deterministic ordered viewport batch at
  a time with Tokio time paused.
- Caveat: moka's TinyLFU maintenance is an internal detail we don't control;
  single-threaded access should be reproducible, but if bit-identical repeats
  fail, we accept "statistically stable" (fixed seed, N repeats, report
  spread) rather than swapping the production cache for a deterministic LRU —
  fidelity to moka's actual admission/eviction *is the point*.
- Phase 1A intentionally excludes cross-request coordinator merging. Q4
  results must come from Phase 1B. Bit-identical completion order is not a
  requirement for Phase 1B; fixed-seed repeats must produce statistically
  stable aggregate counters.

## 8. Validation against the real cluster

Before trusting sweep output, calibrate once. The `replay-http` subcommand reads
an existing trace and supports two explicit target contracts:

- repeated ordered `--node-url` values use each request's recorded `entry_node`
  as an exact index, preserving direct-node placement;
- one `--gateway-url` sends every request through the load balancer and ignores
  recorded entry assignments, measuring the real affinity distribution.

Serial mode preserves request order. `--viewport-batches` starts one validated
`(step,user)` batch concurrently and waits for it before the next. Requests send
`Cache-Control: no-cache`, follow no redirects, retry nothing, and stream and
discard up to 64 MiB of each response body. An oversized body is aborted and
reported as a failure rather than fully consumed. The report-schema-v2 document
contains status/body totals, bounded failure samples, client-observed latency
percentiles, target configuration, the body limits, and a trace fingerprint.

Optional ordered `--metrics-url` endpoints are scraped immediately before and
after replay. Counter deltas are derived per node before aggregation and fail on
a reset or disappearing series. The comparable output covers normalized tile
sources, external/internal/backend bytes, positive L1 hits, peer attempts,
backend fetch outcomes/chunks, and chunk-cache/coordinator counters. Every
execution mode reports the same L1 semantics: `l1_cache_hits` counts positive
tile-body hits only, and `negative_cache_hits` counts actual hits on an existing
negative entry. HTTP replay derives the latter from the always-exposed
`ishikari_tile_negative_cache_hits_total` counter; a target without that exact
report-v2 metric fails capture instead of silently producing zero. The legacy
`ishikari_tile_cache_total{outcome="negative"}` remains a hit-plus-insert event
counter and is not used for comparison. Production collapses all negative
result sources into `miss` in `tiles_served`, so source comparison must still
normalize simulator `negative_cache`, `self_miss`, `peer_miss`, and `miss`.
Client-observed Gateway latency is not equivalent to Axum's server histogram.
Metrics responses are retained up to 8 MiB and scraped sequentially, bounding
active capture memory to one body. Metrics are process-wide, so calibration
requires an otherwise idle deployment and stable pod identities/restart counts
outside the report.

Calibration procedure:

1. Run the local 3-node cluster with a fixed configuration and distinct public,
   internal, and gossip ports.
2. Replay an exported trace (§3.3) directly against the recorded entry nodes.
   Restart cold and replay it again through the Gateway.
3. Compare the replay report's Prometheus aggregate (and direct per-node values)
   with the in-process simulator report for the same trace and configuration.
4. Acceptance: Phase 1A hit rates within ~2 points; Phase 1B backend GET count
   within ~10%. Report direct-node and Gateway validation separately.

Initial local calibration (860 z13 requests, 3 nodes, 1 MiB tile cache and
4 MiB chunk cache per node) compares `cache_mode=real` with `modeled`:

- L1 hit rate: 11.74% real vs 11.51% modeled.
- Backend bytes: 126,877,696 real vs 132,120,576 modeled (+4.1%).
- Backend range GETs: 86 real vs 93 modeled (+8.1%).

This is inside the acceptance target for capacity sweeps. Cold bootstrap/leaf
cache lookup counters remain scheduling-sensitive, and modeled peer counters
cover tile forwarding but not every bootstrap/leaf control transfer. Use real
mode when those counters, rather than capacity/egress trends, are the subject.

## 9. Current Implementation

Layout: the repository is a Cargo workspace with sibling `ishikari` and
`ishikari-sim` crates, matching the biei/biei-sim layout. The production crate
exports the narrow injection seam only under its `simulator-support` feature;
the simulator owns `rand`, census parsing, trace generation, scenarios, and its
binary. Normal `ishikari` builds therefore carry no simulator dependencies.

Implemented: the transport/directory seam, workload generator, Phase 1A
in-process cluster, structured metrics report, Phase 1B viewport runner,
validated in-process JSONL replay, scripted node add/remove replay, versioned
modeled-cache sweep orchestration, and external direct-node/Gateway HTTP replay
with optional Prometheus calibration deltas. Metadata-only per-node modeled
tile/chunk caches and PMTiles access catalogs are also implemented for large
logical-capacity sweeps. A
self-contained HTML report visualizer renders request-indexed churn charts,
interval rates, peer failover/backoff activity, cache occupancy, and active-node
load.
Structured production scheduler histograms and aggregate node request-load
skew are included in current JSON reports.

Remaining implementation work is tracked once in [TODO.md](TODO.md).

## 10. Phase 2 latency and queueing

`--phase2` replays each trace user in its own Tokio task. Tiles in one viewport
use concurrent batch semantics, and the next viewport starts
after batch completion plus deterministic think-time jitter. Because Tokio time
is paused, long virtual runs complete without wall-clock sleeps.

The first implementation models:

- fixed or fitted lognormal latency per object-storage range GET via the
  production fetcher seam, with a transfer-time term proportional to range size;
- fixed in-process peer latency;
- production request batching, single-flight, and the per-tileset 32-fetch cap;
- request timeout cancellation;
- overall/per-source p50, p90, p95, p99, max, throughput, and per-node peak
  in-flight requests.

Representative measurements:

- Phase 1, 3 nodes, 64 MiB caches: 1.74 GB backend bytes and 1,288 range GETs.
- Phase 1, 3 nodes, 512 MiB caches: 0.96 GB and 705 batched range GETs.
- Phase 2, 100 ms backend latency and normal think time: p95 116 ms, p99 215 ms.
- Phase 2, 200 VUs, 300 ms backend latency and no think time: p95 617 ms,
  p99 925 ms, max 2.465 s; peak in-flight requests reached about 600 per node.
- Phase 2, 50 VUs, 3 nodes, 26,018 requests, and 100 ms backend latency exposed
  cold index fan-in: Reader single-flight reduced peer bootstrap fetches from
  286 to 2 and leaf fetches from 654 to 109. Identical overlap fell from
  284/545 to zero; p95/p99 remained 112/219 ms. Peer tile overlap was only
  25 of 10,898 fetches (0.23%), so tile response coalescing remains unwarranted.
- GKE `asia-northeast1` production metrics (2026-07-13): 343 successful range
  fetches averaged 94.5 ms at 1.78 MiB; 67.1% completed within 100 ms, 92.4%
  within 200 ms, and all within 500 ms. A same-region controlled range probe
  estimated approximately 6 ms/MiB of transfer-time slope.
- The fitted profile (`median=55 ms`, lognormal `sigma=0.9`, `6 ms/MiB`) on the
  32,129-request trace produced p50 2 ms, p95 94 ms, p99 224 ms, and max
  1.35 s. Cache hit rate was 47.6%, with 685 backend range fetches.

The high-load result demonstrates queueing above the nominal fetch latency;
it also produces production `joined_pending`/`joined_inflight` counters. The
versioned profile at
`ishikari-sim/data/gcs-asia-northeast1-2026-07-13.json` records both the raw
production histogram summary and controlled range-probe summary used for the
fit. Real-cache mode includes chitchat propagation and failure detection with a
configurable virtual per-hop delay. The latency model still does not include
terrain CPU semaphore time, gossip packet loss/partitions, HTTP parsing, public
Gateway/CDN latency, or kernel/network queues. Those should be added only with
measured inputs.

## 11. Non-goals (Phase 1)

- No stochastic gossip packet loss or partition model. Real-cache mode runs
  production chitchat with deterministic virtual hop latency; modeled mode
  intentionally changes membership instantaneously.
- No glyph/sprite/style provider traffic, no Mapterhorn composite resolution,
  no MLT transcoding, no terrain generation.
- No HTTP/serialization overhead; peer transfers are counted by tile byte
  length.
- No client cache model beyond viewport dedup. Browser HTTP caches are outside
  the simulator's current workload contract.

## Open questions

- `entry_affinity` default: confirm how the GKE Gateway actually balances
  HTTP/2 streams (per-request vs per-connection) and match it.
- Should the trace include tileset diversity (e.g. `japan` + a second
  tileset) to exercise per-tileset chunk coordinators and cache competition?
  Phase 1 starts single-tileset.

- Whether S2's "requests to recover" should also be reported in wall-clock
  terms using a simple requests/sec assumption, for communicating results.

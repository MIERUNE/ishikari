# Ishikari Simulator

Usage guide for the `ishikari-sim` crate. Commands below assume the workspace root as the current directory.

`ishikari-sim` generates deterministic population-weighted viewport traces and
estimates how a deployment behaves without allocating the equivalent cluster,
cache memory, object-store traffic, or wall-clock time. It reuses Ishikari's
production HRW, PMTiles range planning, request batching, and cache policy, then
combines them with logical byte capacity, virtual time, and cloud-calibrated
latency models:

## Quick start

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --tileset japan \
  --tileset-sources data \
  --nodes 3 \
  --users 50 \
  --steps 1000 \
  --viewport-batches \
  --output trace.jsonl \
  --report report.json
```

Add `--zoom-walk-probability 0.1` when generating a trace to replace 10% of
non-reset pan steps with a one-level `z±1` transition at the same geographic
center. The default is `0`, preserving the pan/reset-only workload. Generate a
separate trace for each probability before running replay-only sweeps so every
cache configuration in one sweep still receives exactly the same requests.

Without `--viewport-batches`, requests run serially for deterministic cache and
placement studies. With it, each viewport is polled concurrently under paused
Tokio time, exercising the configured production chunk merge window (10 ms by
default) without adding wall-clock delay. Use
`--chunk-fetch-merge-window-ms 0` for the no-delay baseline; the value is
recorded in `cluster.chunk_fetch_merge_window_ms`. Production and simulator
reject values above 1000 ms so the batching delay stays below request deadlines.

## Trace replay

Replay the exact same trace against another cache or batching configuration.
Imported JSONL is bounded to 4 KiB per line, 256 MiB of raw input (including
blank lines), 2,000,000 requests, 10,000 users, and 64 requests per viewport
batch. Sparse step numbers are valid outside timed Phase 2 replay:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --chunk-size-bytes 262144 \
  --max-fetch-chunks 8 \
  --report replay-report.json
```

The simulator can compare the production entry-node hot-cache policy with
owner-only positive tile caching using `--peer-tile-cache entry` (default) or
`--peer-tile-cache owner-only`. Both modes execute the production resolver;
the selected policy is recorded in the report as `cluster.cache_peer_tiles`.

## Parameter sweeps

Run replay-only modeled-cache parameter sweeps from a versioned JSON spec:

```json
{
  "schema_version": 1,
  "trace": "trace.jsonl",
  "viewport_batches": true,
  "entry_seeds": [1, 2, 3],
  "base_cluster": {
    "tileset_sources": "data"
  },
  "grid": {
    "node_count": [2, 3, 5],
    "tile_group_size": [128, 512, 2048],
    "tile_cache_max_bytes": [67108864, 268435456],
    "chunk_cache_max_bytes": [67108864, 268435456],
    "cache_peer_tiles": [true, false]
  }
}
```

Paths are relative to the sweep spec. The input grammar remains sweep-spec
schema v1. The runner builds the PMTiles catalog once, expands the Cartesian
grid in a stable order, creates a fresh modeled cluster per run, and writes one
self-contained report-schema-v2 document per JSONL line to a sibling temporary
file. The completed JSONL is atomically published. Each line records
`sweep_spec_schema_version: 1` and includes effective configuration,
aggregate/per-node results, churn-style periodic samples, and FNV-1a
fingerprints of the spec and trace:

`max_runs` can lower the per-spec run limit, but cannot raise the simulator's
hard 10,000-run ceiling. Sweep-spec input is also capped at 1 MiB before JSON
parsing, so Cartesian expansion and spec ingestion remain memory-bounded.

```bash
cargo run -p ishikari-sim --release -- \
  sweep sweep.json \
  --output sweep-results.jsonl
```

Sweep-spec version 1 exposes only modeled-cache parameters that affect
request-order and capacity results. Sweep cells run sequentially; each cell can
still use serial or viewport-batch request execution. Timed controls such as
merge-window duration and backend concurrency remain real-cache/Phase 2
experiments; modeled reports record those settings but do not execute their
timing behavior.

## HTTP calibration

Replay the same trace over real HTTP for simulator calibration. Repeated
`--node-url` values are ordered: trace `entry_node: 0` selects the first URL,
`entry_node: 1` the second, and so on. When metrics URLs are supplied, the runner
scrapes each node before and after replay and reports restart-checked deltas for
tile sources, client/peer/backend bytes, backend fetches, and chunk-cache work.
Positive L1 hits use the existing hit outcome; negative L1 hits use the exact
`ishikari_tile_negative_cache_hits_total` counter. Targets that do not expose
that report-v2 counter fail metrics capture instead of reporting a false zero:

```bash
# Start `bash demo.sh` in another terminal, then run:
cargo run -p ishikari-sim --release -- replay-http trace.jsonl \
  --node-url http://[::1]:8080 \
  --node-url http://[::1]:8081 \
  --node-url http://[::1]:8082 \
  --metrics-url http://[::1]:9090/_internal/metrics \
  --metrics-url http://[::1]:9091/_internal/metrics \
  --metrics-url http://[::1]:9092/_internal/metrics \
  --viewport-batches \
  --output direct-http-report.json
```

Gateway mode deliberately ignores recorded entry-node assignments while still
aggregating per-pod internal metrics:

```bash
cargo run -p ishikari-sim --release -- replay-http trace.jsonl \
  --gateway-url https://ishikari.example.com \
  --metrics-url http://127.0.0.1:9090/_internal/metrics \
  --metrics-url http://127.0.0.1:9091/_internal/metrics \
  --metrics-url http://127.0.0.1:9092/_internal/metrics \
  --viewport-batches \
  --output gateway-http-report.json
```

HTTP replay sends `Cache-Control: no-cache`, follows no redirects, performs no
retries, and streams and discards response bodies up to 64 MiB per response.
Larger bodies are aborted and reported as failures rather than fully consumed.
Prometheus responses are retained up to 8 MiB each and scraped sequentially, so
only one metrics body is active at a time. The runner writes bounded failure
samples plus client-observed latency percentiles. `200` and `404` are normal
outcomes; any transport error, other status, counter reset, or post-replay
metrics failure makes the command exit nonzero after preserving the report. An
initial metrics scrape is a preflight check and fails before replay or report
creation. Run calibration on an otherwise idle deployment because the
Prometheus counters are process-wide. The public target and internal metrics
endpoints are intentionally separate.

## Report schema

Normal simulation and HTTP replay reports use schema v2. Reports identify their
trace source as `generated` or `replay` and include the full cluster
configuration and aggregate/per-node metrics. `l1_cache_hits` is positive-only;
`negative_cache_hits` is separate.

## Churn replay

Replay node additions and removals with a churn plan. Events are applied at
request boundaries in serial mode and at the next completed viewport boundary
with `--viewport-batches`; the report records both requested and actual request
indices:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --churn-plan ishikari-sim/data/churn-example.json \
  --churn-sample-every-requests 1000 \
  --report churn-report.json
```

New nodes join with empty tile and chunk caches. Removed nodes leave the ingress
set and in-process transport immediately; in real mode, stale chitchat views may
still select them briefly and exercise the production peer fallback path. Their
cumulative requests, backend bytes, and metrics remain in the final report with
`active: false`. Churn samples make cache-hit loss, peer redistribution, and
backend refetches visible over time. They expose `l1_cache_hits` and
`negative_cache_hits` separately; legacy `cache_hits` remains their cumulative
sum. Each event has `pre_event` and `post_event` samples at the same request
index; samples also include active cache occupancy and per-node request
counters.
To make added nodes eligible for ingress, churn replay deterministically
reassigns requests over the current active set using `--entry-affinity`; it does
not reuse the trace's fixed node indices. In `real` cache mode every simulated
node runs Ishikari's production chitchat membership over an in-memory transport
and Tokio's virtual clock. Node-local peer views therefore converge after
churn, including the production failure detector and peer-list TTL. The
metadata-only `modeled` mode keeps membership changes instantaneous so large
node/capacity sweeps remain cheap.

## Visualization

Generate a self-contained visualization from any simulation report:

```bash
cargo run -p ishikari-sim -- visualize \
  churn-report.json \
  --output churn-report.html
```

Churn reports provide request-indexed trend charts with churn event markers,
interval cache/peer rates, peer failover and backoff activity, backend fetch
rate and transfer volume per 1,000 requests, active cache occupancy, and final
node load.
The HTML embeds the report and has no server or external asset dependency.

## Report semantics

Tile source labels distinguish both placement and backend involvement.
`self_cache` covers entry-node L1 hits and local resolutions completed entirely
from PMTiles/index and chunk caches. `peer_cache` is the equivalent response
from an HRW peer. `self_backend` and `peer_backend` mean that tile resolution
waited for at least one object-storage chunk fetch, including joining pending or
inflight work. `miss` includes positive lookup misses and negative-cache hits.
The reported `cache_hit_rate` is `(self_cache + peer_cache) / requests`, so it
includes positive L1 hits and PMTiles resolutions completed from chunk caches.
`l1_cache_hit_rate` remains available separately in the JSON report.
`Client egress` is the successful tile payload sent to end users; `Peer
transfer` is internal east-west traffic.

## Churn scenarios

For a majority-loss scenario, start with 10 nodes and remove seven at the same
viewport boundary:

```bash
cargo run -p ishikari-sim --release -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 10 \
  --churn-plan ishikari-sim/data/churn-majority-failure-example.json \
  --report majority-failure-report.json
```

This validates HRW redistribution and cold-cache recovery on the three
surviving nodes. Use `--cache-mode real` to include node-local chitchat
convergence, or `--cache-mode modeled` to isolate placement and logical cache
recovery with instantaneous membership. Gossip packet loss remains a separate
failure-injection model.
Use `churn-steady-state-example.json` for an event-free baseline with the same
dynamic ingress assignment; a regular replay preserves the trace's original
entry-node indices and is not comparable when changing the node count.
`churn-mixed-example.json` provides a longer-running deterministic sequence of
staggered additions, removals, temporary contraction, and removal of a node
that joined during the run.

## Modeled capacity sweeps

For large cache-capacity and node-count sweeps, use metadata-only modeled
caches. The catalog reads PMTiles directories once, but tile and chunk cache
entries retain only logical byte weights rather than payloads:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --cache-mode modeled \
  --viewport-batches \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 8 \
  --tile-cache-max-bytes 68719476736 \
  --chunk-cache-max-bytes 1073741824 \
  --report modeled-report.json
```

`real` remains the default reference mode and executes production resolvers
with real payload caches; it is useful for checking model fidelity on small
runs, not for representing production-scale memory. `modeled` is the scalable
capacity-study mode. It currently accepts one local PMTiles root and reuses
production HRW placement, Moka TinyLFU/LRU policy, byte weights, and chunk range
planning without retaining tile payloads. The production 1 GiB per-node
chunk-cache cap also applies in modeled mode.

## Phase 2 latency and queueing

For latency and queueing experiments, replay a trace with concurrent virtual
users under Tokio's paused clock. This runs the production resolver, caches,
single-flight, configured merge window, and 32 concurrent range-fetch limit while
adding deterministic backend and peer latency. The repository includes a GCS
profile measured from the demo cluster in `asia-northeast1`:

```bash
cargo run -p ishikari-sim -- \
  --simulate \
  --phase2 \
  --input-trace trace.jsonl \
  --tileset-sources data \
  --nodes 3 \
  --backend-latency-profile ishikari-sim/data/gcs-asia-northeast1-2026-07-13.json \
  --peer-latency-ms 1 \
  --report timed-report.json
```

The timed report includes throughput, request latency percentiles overall and
by source, timeouts, and peak in-flight requests per node. The common result
also reports backend fetch size/duration, batching queue delay, pending chunks,
group waiters, and node request-load skew (max/mean and coefficient of
variation). Each virtual user waits for its viewport batch, then sleeps for
`1200 +/- 500 ms` by default, matching the closed-user workload model. Sparse
steps expand to at most 1,000,000 deterministic think-time transitions per user
and 10,000,000 globally; their per-step durations are summed into one sleep
before the next emitted batch. The measured profile uses a deterministic
lognormal range-fetch latency plus a
per-MiB transfer term. Fixed controlled sweeps remain available through
`--artificial-backend-delay-ms`; sigma and the transfer slope can also be
supplied directly.

## Documents

- [Design specification](SPEC.md)
- [Open work](TODO.md)

# Ishikari Simulator TODO

Open simulator-specific experiments, validation, and modeling work.

## Distributed cache evaluation

- Use `ishikari-sim` to compare the current entry-node L1 insertion policy with
  owner-only insertion across realistic node counts, cache capacities, request
  skew, and churn. Decide whether the tile cache is intentionally a replicated
  hot tier or part of the owned aggregate capacity before changing production
  behavior.

  Initial modeled result (2026-07-14): 10 nodes, 159,584 requests, and 64 MiB
  tile cache per node. With the normal 512 MiB chunk cache, both policies made
  1,526 backend fetches and read 1.93 GB; entry caching reduced peer requests
  from 143,788 to 122,533. With a deliberately constrained 1 MiB chunk cache,
  owner-only reduced backend fetches from 26,571 to 15,889 and backend bytes
  from 33.86 GB to 19.62 GB, at the cost of more peer traffic. This confirms the
  policy depends on the tile/chunk cache ratio. Keep entry caching as the
  production default until production-sized capacity and churn sweeps justify a
  change. The simulator exposes both through `--peer-tile-cache`.
- Use per-node `ishikari_internal_resource_requests_total` for owner-side load
  and `ishikari_peer_fetch_total` for sender-side attempts, filtered to
  `resource="bootstrap"|"leaf"`, to measure whether group-zero ownership
  creates a material hotspot. Shard leaf ownership by byte-offset key only if
  concentration is significant.

  Initial real-resolver result (2026-07-15): a 3-node, 26,018-request replay
  sent all 2 bootstrap and 117 leaf requests to one owner, as designed. Those
  119 index requests were only 1.1% of the 10,873 internal tile requests, so
  the measured concentration does not justify sharding leaf ownership. The
  simulator report now includes per-node inbound and outbound counts; repeat
  with multi-tileset production traces before reconsidering.
- Benchmark the configurable chunk merge window against isolated and viewport
  workloads, including the 0 ms no-delay baseline and 10 ms default. Compare
  end-user latency, backend operation count, fetched bytes, and waiter fan-in;
  prefer an adaptive rule only if it improves the measured Pareto frontier.

## Calibration and model coverage

- Run controlled cold-cluster `replay-http` calibrations for direct-node and
  Gateway targets, compare their Prometheus deltas with the in-process simulator,
  and record the measured hit-rate/backend-GET error against the acceptance
  bounds in [`SPEC.md`](SPEC.md).
- Model terrain generation and the shared CPU-admission queue in Phase 2.
- Add gossip packet-loss or partition injection only after selecting measured
  failure inputs.

## Input hardening

- Harden simulator input parsing with `#[serde(deny_unknown_fields)]` on
  versioned sweep/spec input structures so a typo'd axis fails instead of
  silently running a different experiment.

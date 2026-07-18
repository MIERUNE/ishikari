# Ishikari TODO

System positioning, non-goals, guardrails, and refactor direction are documented
in `../specs/ishikari-spec.md`.

## Active Work

### Distributed Cache Evaluation

Simulator-backed cache-policy, ownership, and batching experiments are tracked
with their evidence in [`ishikari-sim/TODO.md`](../ishikari-sim/TODO.md).
Production defaults change only after those experiments justify the trade-off.

### Derived Terrain Products (experimental)

Contour and hillshade generation is an optional, bounded extension to the core
PMTiles delivery path. Keep it measurable and avoid letting it complicate the
stored-tile fast path. The current product and algorithm contract is documented
in `../specs/isoline-and-hillshade-spec.md`.

- Build a repeatable Pareto benchmark over representative terrain fixtures and
  zooms. Compare vector MVT/MLT, quantized lossless WebP, and continuous lossy
  raster rendered through `color-relief` against MapLibre's raster hillshade.
  Record compressed bytes, feature/ring/vertex counts, generation and decode
  time, render time, SSIM or equivalent structural error, and perceptual color
  error (OKLab Delta E). Use the results to choose defaults rather than treating
  the current tone count or representation as final.
- Verify the raster `color-relief` path in both the supported MapLibre GL JS
  version and Biei's concrete MapLibre Native build, including transparent
  neutral stops, texture filtering, and overzoom behavior.
- Constrain shared-arc simplification before increasing its tolerance. Candidate
  replacements must not introduce intersections or self-intersections, reverse
  ring/face orientation, or collapse narrow shade faces. Add focused fixtures
  for close parallel bands and junction-heavy terrain.
- Consider request-coalesced 2x2/4x4 metatile generation only if benchmarks show
  a material geometry-fragmentation or CPU benefit. The current one-cell halo is
  sufficient for the one-cell speckle rule; always generating a 4x4 metatile for
  one requested tile would overcompute 15 outputs. A metatile implementation
  should batch nearby cold requests, build shared topology once, split the
  children, and populate the existing derived cache in one pass.

### Demo and Acceptance Checks

- Keep Gateway-routed smoke checks current for TileJSON, tile bytes, style JSON,
  glyphs, sprites, health, and internal-path non-exposure.
- Keep Biei as one consumer smoke, not as an Ishikari-specific API contract.
- Add cold/warm latency checks when performance tuning resumes.
- Add a local multi-node dev-cluster script only if single-process tests stop
  catching cluster regressions.
- Keep the router-level HTTP contract tests (`server/contract_tests.rs`) current.
  They cover stored MVT and negotiated MLT tile responses through a generated
  single-tile PMTiles fixture; namespaced styles; provider cache metadata (public
  `Cache-Control` / `Age`, default and upstream-derived, repeated field lines,
  compressed style bodies, and internal `x-ishikari-provider-*` headers); glyph
  and sprite defaults; client conditional requests including derived TileJSON
  ETags; conditional origin revalidation that extends stale provider entries on
  `304`; and public-router internal-path non-exposure over a real local HTTP
  upstream.

## Optional Hardening

- Measure whether terrain DEM backend reads can crowd the process-wide
  `backend_fetch_concurrency` pool under a derived-tile flood
  (`ishikari_backend_fetch_queue_duration_seconds` split by workload replay):
  the per-tileset coordinator cap currently equals the default global limit, so
  one hot archive can transiently monopolize backend permits and delay cold
  stored-tile fetches for other tilesets. If material, add per-class or
  per-tileset backend fairness; CPU-class isolation already prevents shedding.

- Cache the rewritten style representation keyed by upstream body digest,
  effective origin, and requested encoding. Rewriting now runs bounded on the
  blocking pool, but it still decodes, parses, rewrites, and serializes up to
  2 MiB of JSON per request even when the upstream body is cached.

- Add a style catalog admin/update endpoint only if dynamic style registration
  becomes necessary.
- Define an explicit cache-invalidation contract before supporting mutable or
  unversioned PMTiles archives; then revisit tile negative-cache TTLs.
- Evaluate framed internal APIs, per-hop/end-to-end timeout budgets, and
  OpenTelemetry only if measurements show the current HTTP + request-id +
  Prometheus model is insufficient.
- Measure dead-node state growth under Spot churn before shortening membership
  retention beyond its current failure-detection grace period.
- Persist a monotonic membership incarnation only if wall-clock rollback becomes
  an operational concern.

## Open Questions

- How should style/version invalidation work before content-addressed IDs exist?
- Should Ishikari proxy external style assets, or require assets to be mirrored into the configured data backend?
- Which default cache TTLs are acceptable for mutable MIERUNE deployments?
- If fixed per-hop timeouts are insufficient, what internal end-to-end timeout budget should bound peer-forwarded fetches, and should it vary by resource kind?

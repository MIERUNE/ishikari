# Ishikari

A distributed PMTiles cache proxy for efficient, low-cost, large-scale serving from object storage.

> [!WARNING]
> This is an experimental, proof-of-concept project. The behavior, API, and configuration are not stable.

Ishikari focuses on large-scale PMTiles serving workloads:

- **Backend request batching** - reduces object storage requests, traffic, and latency.
- **Distributed cache** - uses gossip membership, locality-aware routing, and caching tuned for Hilbert-sorted PMTiles archives.
- **Optional derived terrain products** - generates hillshade and contour tiles
  from raster DEM sources such as Mapterhorn while preserving the ordinary
  PMTiles delivery path for source data.

CPU-heavy DEM decode, terrain generation, and MLT transcoding share one bounded
worker budget. `ISKR_CPU_WORK_CONCURRENCY` defaults to the process's available
parallelism and can be set explicitly per deployment.

LICENSE: MIT OR Apache-2.0


## Demo

```bash
# Serve from a local PMTiles file with an artificial backend delay.
mkdir data
pmtiles extract https://build.protomaps.com/20260206.pmtiles --bbox=122,24,155,46 data/japan.pmtiles
ISKR_ARTIFICIAL_BACKEND_DELAY_MS=50 bash demo.sh
open http://localhost:8080/tilesets/japan/preview
```

```bash
# Serve from a remote HTTP server (slow).
ISKR_TILESET_SOURCES=https://demo-bucket.protomaps.com/ bash demo.sh
open http://localhost:8080/tilesets/v4/preview
```

## Style, glyph, and sprite proxy

Ishikari can proxy MapLibre style JSON, glyph PBFs, and sprite assets from upstream templates:

```bash
ISKR_STYLE_TEMPLATES='carto=https://basemaps.cartocdn.com/{style_id}/style.json;default=https://styles.example/{style_id}/style.json' \
ISKR_GLYPH_URL_TEMPLATE='https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf' \
ISKR_SPRITE_TEMPLATES='carto=https://basemaps.cartocdn.com/{style_id}/sprite' \
cargo run -- --tileset-sources data
```

The style endpoint rewrites provider-relative `/{tileset_key}` sources to
Ishikari TileJSON URLs and points `glyphs` and `sprite` back to Ishikari.
Style, glyph, and sprite upstream fetches use bounded in-process caching and
single-flight coordination to absorb cold concurrent renders. Stale provider
entries revalidate conditionally, so an unchanged HTTP or object-store origin
can refresh freshness without sending the body again.

`ISKR_TILESET_SOURCES` (the PMTiles tile source) accepts the same `namespace=url;…;default=url`
form, so tilesets can be backed by multiple object-store roots. A namespaced key
is served from the matching root with the namespace stripped
(`regional/streets` → `{regional-root}/streets.pmtiles`); any other key falls to
the default root with its full path (`analysis/hrnowc` →
`{default-root}/analysis/hrnowc.pmtiles`). A single bare `ISKR_TILESET_SOURCES` stays the
default root.

## Composite Mapterhorn tileset

Set `ISKR_MAPTERHORN_TILESET` to a logical tileset such as
`mapterhorn/planet` and `ISKR_MAPTERHORN_MAXZOOM` to the advertised detail
zoom to expose Mapterhorn's base and detail archives as one tileset. Requests at
z0–12 use the logical base archive. Requests at z13+ resolve to the z6 ancestor
detail archive in the same namespace (`mapterhorn/6-{x6}-{y6}.pmtiles`).

Detail presence is probed on first use, single-flighted, and cached. Missing
detail coverage returns 404; Ishikari does not substitute an overzoomed z12
tile. Source reads still use normal HRW routing, chunk caching, range batching,
and negative caching.

Generated contour and hillshade outputs use the same tile-group HRW placement.
The owner single-flights generation, caches the result, and performs optional
MLT transcoding; another node generates locally only when the owner is
unavailable.

## MLT output

PMTiles containing native MLT tiles are served as stored. Stored MVT tiles can
also be transcoded on demand by using the `.mlt` path suffix or
`Accept: application/vnd.maplibre-tile`; ordinary requests remain as stored.
Transcodes are single-flighted into a bounded per-pod cache and run on the
blocking pool behind the shared `ISKR_CPU_WORK_CONCURRENCY` budget. Transcoded
outputs are not forwarded between peers.

## Observability

Prometheus metrics are exposed only on the internal listener at
`/_internal/metrics`. In addition to bounded route/status counters, Ishikari
reports end-to-end HTTP latency by route and status class, object-store range
fetch duration, size, admission queue delay, and concurrency saturation; chunk
batching and waiter fan-in; weighted cache bytes; and peer-routing outcomes.
`ISKR_BACKEND_FETCH_CONCURRENCY` bounds range fetches across all tilesets in a
process and defaults to 32. `ISKR_CHUNK_FETCH_MERGE_WINDOW_MS` controls how long
nearby missing chunks are collected before dispatch (10 ms by default; 0 removes
the intentional wait while preserving pending/inflight sharing). CPU-heavy DEM
decode, terrain generation, and MLT
transcoding expose admission, queue delay, current saturation, and shed counts.
Derived terrain cold-generation metrics separate source fetch/decode time from
product generation time and record compressed output size per fixed product.

## Simulator

See the [`ishikari-sim` documentation](ishikari-sim/README.md).

## Development documents

- [Design contract and guardrails](specs/ishikari-spec.md)
- [Open work and decisions](issues/ishikari-todo.md)
- [Derived isoline and hillshade specification](specs/isoline-and-hillshade-spec.md)

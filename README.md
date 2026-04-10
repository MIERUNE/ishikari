# ishikari

> [!NOTE]
> This project is in early stages of development.

A distributed cache proxy for serving tiles from [PMTiles](https://github.com/protomaps/PMTiles) archives in object storage.

Designed for large-scale tile serving deployments:

- **Backend request batching** - for workloads where object storage cost, traffic, and latency must be optimized.
- **Distributed cache** - uses locality-aware routing and caching for clustered PMTiles archives.

## TODO

- Features:
  - Serving style-related resources
  - Cache-control headers
  - Fast failover
  - Metrics
    - OpenTelemetry
  - Evaluate gRPC for internal communication
- Development:
  - Testing
  - Benchmarking
  - Kubernetes deployment

## Demo

```bash
# Serve from a local PMTiles file with an artificial backend delay.
mkdir data
pmtiles extract https://build.protomaps.com/20260206.pmtiles --bbox=122,24,155,46 data/japan.pmtiles
BACKEND_FETCH_DELAY_MS=50 bash demo.sh
open http://localhost:8080/tilesets/japan/preview
```

```bash
# Serve from a remote HTTP server.
DATA_URL=https://demo-bucket.protomaps.com/ bash demo.sh
open http://localhost:8080/tilesets/v4/preview
```

# Ishikari k8s demo

Runs Ishikari as a tile/style provider. Public demo traffic is routed by the
shared Gateway.

This is a demo, not a production deployment.

## Layout

| Path | Purpose |
|---|---|
| `Dockerfile` | Release Ishikari image. |
| `k8s/base/` | Deployment, ClusterIP HTTP Service, and headless gossip Service. |
| `k8s/platform/` | Shared `map-demo` namespace and Gateway. Apply once per cluster. |
| `k8s/overlays/gke/` | GKE overlay with provider config, Artifact Registry image, and HTTPRoute. |

## GKE

The current GKE demo runs on amd64 Spot nodes in `asia-northeast1`. Autopilot
Arm workloads are not available in this region, so keep the pushed image amd64
unless you move the demo to an Arm-supported region.

Build and push the demo image:

```sh
gcloud builds submit --config demo-deploy/cloudbuild.yaml .
```

The build imports and refreshes the BuildKit cache stored in Artifact Registry
as `ishikari-buildcache:latest`; the first build starts with an empty cache.

Create or bind the GCS reader identity once:

```sh
PROJECT_ID=mappf-experiment
BUCKET=mappf-experiment-demo
GSA=ishikari-gcs-reader@${PROJECT_ID}.iam.gserviceaccount.com
KSA=ishikari
NAMESPACE=map-demo

gcloud iam service-accounts create ishikari-gcs-reader \
  --project "${PROJECT_ID}" \
  --display-name "Ishikari demo GCS reader"

gcloud storage buckets add-iam-policy-binding "gs://${BUCKET}" \
  --member "serviceAccount:${GSA}" \
  --role roles/storage.objectViewer

gcloud iam service-accounts add-iam-policy-binding "${GSA}" \
  --project "${PROJECT_ID}" \
  --role roles/iam.workloadIdentityUser \
  --member "serviceAccount:${PROJECT_ID}.svc.id.goog[${NAMESPACE}/${KSA}]"
```

The checked-in Gateway listens on HTTPS and uses Google-managed Certificate
Manager certificates through the `mappf-demo-cert-map` certificate map. Create
the Certificate Manager resources once before applying the Gateway. This
repository only owns the Ishikari hostname; other applications that attach to
the shared Gateway should create their own certificate map entries separately:

```sh
gcloud services enable certificatemanager.googleapis.com

gcloud certificate-manager dns-authorizations create mappf-ishikari-demo \
  --domain=ishikari-demo.mierune.dev \
  --location=global

gcloud certificate-manager certificates create mappf-ishikari-demo-cert \
  --domains=ishikari-demo.mierune.dev \
  --dns-authorizations=mappf-ishikari-demo \
  --location=global

gcloud certificate-manager maps create mappf-demo-cert-map --location=global
gcloud certificate-manager maps entries create ishikari-demo \
  --map=mappf-demo-cert-map \
  --hostname=ishikari-demo.mierune.dev \
  --certificates=mappf-ishikari-demo-cert \
  --location=global
```

Add the DNS authorization CNAME records in Cloudflare:

| Name | Type | Target |
|---|---|---|
| `_acme-challenge.ishikari-demo.mierune.dev` | `CNAME` | `bd6f2292-a3a6-43c4-90c4-d3505e35eb8b.14.authorize.certificatemanager.goog` |

Set Cloudflare SSL/TLS mode to `Full` so Cloudflare connects to the Gateway over
HTTPS. The demo host records can stay proxied because certificate authorization
uses the CNAME records above, not load-balancer IP visibility.
Point `ishikari-demo.mierune.dev` at the reserved `mappf-demo-ingress` address.

Then apply the shared Gateway and Ishikari:

```sh
kubectl apply -k demo-deploy/k8s/platform
kubectl apply -k demo-deploy/k8s/overlays/gke
kubectl -n map-demo rollout status deploy/ishikari
```

The GKE overlay is configured for the demo bucket:

- PMTiles root: `ISKR_TILESET_SOURCES`
- styles: `ISKR_STYLE_TEMPLATES`
- glyphs: `ISKR_GLYPH_URL_TEMPLATE`
- sprites: `ISKR_SPRITE_TEMPLATES`

For another data source, patch those env vars in
`k8s/overlays/gke/patch-deployment.yaml`.

## Routes

The Gateway routes a catch-all `/` to Ishikari's public listener (`:8080`),
which serves the provider prefixes (`/tilesets/*`, `/styles/*`, `/fonts/*`) plus
top-level `/livez` `/readyz`. `/_internal/*`, `/metrics` and peer-to-peer
forwarding live on a separate cluster-internal port (`:9090`) that the Service
does not expose and the Gateway does not route, so nothing internal is reachable
publicly. The shared Gateway listens on HTTPS only.

**Trust boundary:** the base NetworkPolicy keeps public HTTP (`:8080`) reachable
while restricting peer HTTP (`:9090`) and gossip (`:7946/UDP`) to Ishikari pods
in the same namespace. The GKE overlay separately permits Managed Prometheus to
scrape `:9090`. Gossip and peer HTTP are not application-authenticated, so keep
the NetworkPolicy enabled and treat pods carrying `app: ishikari` as trusted.

Replicas use a hard hostname spread and a preferred zone spread, and the PDB
allows at most one voluntary disruption. The server's 20-second end-to-end HTTP
deadline plus the two-second drain propagation delay fits GKE Autopilot's
25-second maximum grace period for Spot pods.

## Checks

```sh
# In-cluster checks.
kubectl -n map-demo port-forward svc/ishikari 8080:8080
curl 'http://localhost:8080/tilesets/<tileset_id>'

# Metrics / _internal live on the internal port (9090), not the public 8080.
kubectl -n map-demo port-forward deploy/ishikari 9090:9090
curl -s localhost:9090/_internal/metrics

# Gateway checks.
curl 'https://ishikari-demo.mierune.dev/tilesets/<tileset_id>'
curl 'https://ishikari-demo.mierune.dev/styles/<style_id>/style.json'

# Certificate status.
gcloud certificate-manager certificates list \
  --location=global \
  --filter='name~mappf' \
  --format='table(name,managed.state,managed.authorizationAttemptInfo[].domain,managed.authorizationAttemptInfo[].state)'

# Must not route through the Gateway.
curl -i 'https://ishikari-demo.mierune.dev/_internal/metrics'
```

Acceptance: Ishikari serves public provider routes through the Gateway, and
deleting one Ishikari pod does not break the demo after readiness/drain settle.

//! Router-level HTTP contract tests.
//!
//! These exercise the fully assembled public and internal routers (including
//! the shared middleware stack) over a single-node membership with a real
//! local HTTP upstream, so header wiring — public `Cache-Control`/`Age`
//! (default and upstream-derived), internal `x-ishikari-provider-*` metadata,
//! and internal-path non-exposure — is asserted at the same altitude the
//! Gateway sees it.

use std::{
    future::IntoFuture,
    io::{Read, Write},
    net::SocketAddr,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use axum::{
    Router,
    body::Body,
    http::{HeaderMap, Request, StatusCode, header},
    response::IntoResponse,
    routing::get,
};
use bytes::Bytes;
use tower::util::ServiceExt;

use super::{
    AppState, TileRuntimeConfig, cache, internal_router, provider::ProviderConfig, public_router,
    with_common_layers,
};
use crate::{
    drain::DrainController,
    membership::{Membership, MembershipConfig},
    metrics::NodeMetrics,
    storage::{
        ObjectStoreRegistry, PROVIDER_AGE_HEADER, PROVIDER_CACHE_CONTROL_HEADER,
        PROVIDER_ETAG_HEADER, PROVIDER_LAST_MODIFIED_HEADER, ResourceResolver,
        ResourceResolverConfig,
    },
};

/// The style fixture's upstream policy and the normalized form Ishikari must
/// emit for it (shared `s-maxage` filled from `max-age`).
const STYLE_NORMALIZED_CACHE_CONTROL: &str =
    "public, max-age=123, s-maxage=123, stale-while-revalidate=60";
/// Upstream validators on the glyph fixture; glyph bytes are served verbatim,
/// so these must pass through to the public response unchanged.
const GLYPH_UPSTREAM_ETAG: &str = "\"glyph-v1\"";
const GLYPH_UPSTREAM_LAST_MODIFIED: &str = "Sat, 01 Feb 2025 10:00:00 GMT";
const PMTILES_HEADER_SIZE: usize = 127;
const PMTILES_FIXTURE_SIZE: usize = 16 * 1024;

/// Writes a minimal PMTiles v3 archive containing one gzip-compressed empty MVT
/// at z0/0/0. Building it in the harness keeps the router contract test
/// self-contained and avoids checking in an opaque binary fixture.
fn write_mvt_pmtiles_fixture(path: &std::path::Path) {
    fn write_varint(mut value: u64, output: &mut Vec<u8>) {
        while value >= 0x80 {
            output.push((value as u8) | 0x80);
            value >>= 7;
        }
        output.push(value as u8);
    }

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(&[]).expect("encode empty MVT");
    let tile = encoder.finish().expect("finish empty MVT");

    // One root entry, encoded column-wise: count, tile-id delta, run length,
    // byte length, and offset+1. It covers tile id 0 and points at data offset 0.
    let mut root = Vec::new();
    for value in [1, 0, 1, tile.len() as u64, 1] {
        write_varint(value, &mut root);
    }

    let root_offset = PMTILES_HEADER_SIZE as u64;
    let data_offset = root_offset + root.len() as u64;
    let mut archive = vec![0; PMTILES_FIXTURE_SIZE];
    archive[..7].copy_from_slice(b"PMTiles");
    archive[7] = 3;
    archive[8..16].copy_from_slice(&root_offset.to_le_bytes());
    archive[16..24].copy_from_slice(&(root.len() as u64).to_le_bytes());
    archive[24..32].copy_from_slice(&data_offset.to_le_bytes());
    archive[32..40].copy_from_slice(&0_u64.to_le_bytes());
    archive[40..48].copy_from_slice(&data_offset.to_le_bytes());
    archive[48..56].copy_from_slice(&0_u64.to_le_bytes());
    archive[56..64].copy_from_slice(&data_offset.to_le_bytes());
    archive[64..72].copy_from_slice(&(tile.len() as u64).to_le_bytes());
    for range in [72..80, 80..88, 88..96] {
        archive[range].copy_from_slice(&1_u64.to_le_bytes());
    }
    archive[96] = 1; // clustered
    archive[97] = 1; // internal compression: none
    archive[98] = 2; // tile compression: gzip
    archive[99] = 1; // tile type: MVT
    archive[100] = 0; // min zoom
    archive[101] = 0; // max zoom
    archive[PMTILES_HEADER_SIZE..PMTILES_HEADER_SIZE + root.len()].copy_from_slice(&root);
    archive[data_offset as usize..data_offset as usize + tile.len()].copy_from_slice(&tile);

    std::fs::write(path, archive).expect("write PMTiles fixture");
}

/// Serves the provider fixtures over real HTTP. The style carries an explicit
/// upstream `Cache-Control`; glyphs and sprites carry none, so they must fall
/// back to the resource defaults.
async fn spawn_upstream() -> (
    SocketAddr,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
) {
    let uncached_requests = Arc::new(AtomicUsize::new(0));
    let invalid_requests = Arc::new(AtomicUsize::new(0));
    let aged_requests = Arc::new(AtomicUsize::new(0));
    let revalidated_requests = Arc::new(AtomicUsize::new(0));
    let router = Router::new()
        .route(
            "/styles/base/style.json",
            get(|| async {
                let mut headers = HeaderMap::new();
                headers.insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
                // Repeated fields must be combined before policy parsing.
                headers.append(
                    header::CACHE_CONTROL,
                    "public, max-age=123".parse().unwrap(),
                );
                headers.append(
                    header::CACHE_CONTROL,
                    "stale-while-revalidate=60".parse().unwrap(),
                );
                headers.insert(header::AGE, "10".parse().unwrap());
                headers.insert(header::CONTENT_ENCODING, "gzip".parse().unwrap());
                let mut encoder =
                    flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
                encoder
                    .write_all(br#"{"version":8,"sources":{},"layers":[]}"#)
                    .unwrap();
                (headers, encoder.finish().unwrap())
            }),
        )
        .route(
            "/styles/revalidated/style.json",
            get({
                let requests = Arc::clone(&revalidated_requests);
                move |headers: HeaderMap| {
                    let requests = Arc::clone(&requests);
                    async move {
                        let request_index = requests.fetch_add(1, Ordering::Relaxed);
                        if request_index == 0 {
                            return (
                                StatusCode::OK,
                                [
                                    (header::CONTENT_TYPE, "application/json"),
                                    (
                                        header::CACHE_CONTROL,
                                        "public, max-age=0, stale-while-revalidate=60",
                                    ),
                                    (header::ETAG, "\"revalidated-v1\""),
                                ],
                                r#"{"version":8,"name":"revalidated","sources":{},"layers":[]}"#,
                            )
                                .into_response();
                        }
                        if headers
                            .get(header::IF_NONE_MATCH)
                            .is_some_and(|value| value == "\"revalidated-v1\"")
                        {
                            return (
                                StatusCode::NOT_MODIFIED,
                                [
                                    (
                                        header::CACHE_CONTROL,
                                        "public, max-age=120, stale-while-revalidate=60",
                                    ),
                                    (header::ETAG, "\"revalidated-v1\""),
                                ],
                            )
                                .into_response();
                        }
                        StatusCode::PRECONDITION_FAILED.into_response()
                    }
                }
            }),
        )
        .route(
            "/regional/base/style.json",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "application/json")],
                    r#"{"version":8,"name":"regional","sources":{},"layers":[]}"#,
                )
            }),
        )
        .route(
            "/styles/uncached/style.json",
            get({
                let requests = Arc::clone(&uncached_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        tokio::time::sleep(Duration::from_millis(25)).await;
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::CACHE_CONTROL, "no-store"),
                            ],
                            r#"{"version":8,"sources":{},"layers":[]}"#,
                        )
                    }
                }
            }),
        )
        .route(
            "/styles/invalid/style.json",
            get({
                let requests = Arc::clone(&invalid_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::CACHE_CONTROL, "public, max-age=3600"),
                            ],
                            "not-json",
                        )
                    }
                }
            }),
        )
        .route(
            // Carries a transported `Age` far larger than the style default
            // TTL but no `Cache-Control`: it must still be cached under the
            // default policy (age is charged only against explicit upstream
            // freshness), so a second request is served without a re-fetch.
            "/styles/aged/style.json",
            get({
                let requests = Arc::clone(&aged_requests);
                move || {
                    let requests = Arc::clone(&requests);
                    async move {
                        requests.fetch_add(1, Ordering::Relaxed);
                        (
                            [
                                (header::CONTENT_TYPE, "application/json"),
                                (header::AGE, "100000"),
                            ],
                            r#"{"version":8,"sources":{},"layers":[]}"#,
                        )
                    }
                }
            }),
        )
        .route(
            // A 2xx status that carries no complete representation must never
            // be cached or served as a style.
            "/styles/nocontent/style.json",
            get(|| async { StatusCode::NO_CONTENT }),
        )
        .route(
            "/styles/partial/style.json",
            get(|| async { (StatusCode::PARTIAL_CONTENT, "partial") }),
        )
        .route(
            "/styles/base/sprite.json",
            get(|| async { ([(header::CONTENT_TYPE, "application/json")], "{}") }),
        )
        .route(
            "/styles/base/sprite.png",
            get(|| async {
                (
                    [(header::CONTENT_TYPE, "image/png")],
                    &b"\x89PNG\r\n\x1a\n"[..],
                )
            }),
        )
        .route(
            "/fonts/TestFont/0-255.pbf",
            get(|| async {
                (
                    [
                        (header::CONTENT_TYPE, "application/x-protobuf"),
                        (header::ETAG, GLYPH_UPSTREAM_ETAG),
                        (header::LAST_MODIFIED, GLYPH_UPSTREAM_LAST_MODIFIED),
                    ],
                    &b"glyph-bytes"[..],
                )
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind upstream");
    let addr = listener.local_addr().expect("upstream addr");
    tokio::spawn(axum::serve(listener, router).into_future());
    (
        addr,
        uncached_requests,
        invalid_requests,
        aged_requests,
        revalidated_requests,
    )
}

struct Harness {
    public: Router,
    internal: Router,
    membership: Membership,
    tiles_dir: PathBuf,
    uncached_upstream_requests: Arc<AtomicUsize>,
    invalid_upstream_requests: Arc<AtomicUsize>,
    aged_upstream_requests: Arc<AtomicUsize>,
    revalidated_upstream_requests: Arc<AtomicUsize>,
    state: AppState,
}

impl Harness {
    async fn get(&self, router: &Router, path: &str) -> (StatusCode, HeaderMap, Bytes) {
        self.get_with(router, path, &[]).await
    }

    async fn get_with(
        &self,
        router: &Router,
        path: &str,
        request_headers: &[(header::HeaderName, &str)],
    ) -> (StatusCode, HeaderMap, Bytes) {
        let mut request = Request::builder().uri(path);
        for (name, value) in request_headers {
            request = request.header(name, *value);
        }
        let response = router
            .clone()
            .oneshot(request.body(Body::empty()).expect("request"))
            .await
            .expect("router response");
        let status = response.status();
        let headers = response.headers().clone();
        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .expect("response body");
        (status, headers, body)
    }

    fn cleanup(self) {
        let _ = self.membership.shutdown();
        let _ = std::fs::remove_dir_all(&self.tiles_dir);
    }
}

async fn harness(label: &str) -> Harness {
    let (
        upstream,
        uncached_upstream_requests,
        invalid_upstream_requests,
        aged_upstream_requests,
        revalidated_upstream_requests,
    ) = spawn_upstream().await;

    let suffix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let tiles_dir = std::env::temp_dir().join(format!(
        "ishikari-contract-{label}-{}-{suffix}",
        std::process::id()
    ));
    std::fs::create_dir_all(&tiles_dir).expect("tiles dir");
    write_mvt_pmtiles_fixture(&tiles_dir.join("fixture.pmtiles"));

    // Single node, no seeds: HRW routing always resolves to the local node, so
    // provider fetches take the local upstream path deterministically.
    let membership = Membership::spawn(MembershipConfig {
        node_id: "contract-node".to_string(),
        listen_addr: "127.0.0.1:0".parse().expect("addr"),
        advertise_addr: "127.0.0.1:0".parse().expect("addr"),
        http_advertise_addr: "127.0.0.1:0".parse().expect("addr"),
        http_port: 0,
        seed_nodes: Vec::new(),
        gossip_interval: Duration::from_millis(200),
    })
    .await
    .expect("membership");

    let registry = Arc::new(ObjectStoreRegistry::new());
    let metrics = NodeMetrics::new();
    let resolver = ResourceResolver::new(ResourceResolverConfig {
        self_node_id: "contract-node".to_string(),
        membership: membership.clone(),
        tileset_sources: tiles_dir.to_string_lossy().into_owned(),
        candidate_count: 1,
        tile_group_size: 512,
        chunk_size_bytes: PMTILES_FIXTURE_SIZE as u64,
        max_fetch_chunks: 4,
        chunk_fetch_merge_window: Duration::from_millis(10),
        backend_fetch_concurrency: 4,
        artificial_backend_delay_ms: 0,
        tile_cache_max_bytes: 1024 * 1024,
        chunk_cache_max_bytes: 1024 * 1024,
        tile_negative_ttl: Duration::from_secs(60),
        object_store_registry: Arc::clone(&registry),
        metrics: metrics.clone(),
    })
    .await
    .expect("resolver");

    let provider = ProviderConfig::new(
        Some(format!(
            "regional=http://{upstream}/regional/{{style_id}}/style.json;default=http://{upstream}/styles/{{style_id}}/style.json"
        )),
        Some(format!(
            "http://{upstream}/fonts/{{fontstack}}/{{range}}.pbf"
        )),
        Some(format!("http://{upstream}/styles/{{style_id}}/sprite")),
    )
    .expect("provider config");

    let state = AppState::new(
        membership.clone(),
        metrics,
        Arc::new(resolver),
        DrainController::new(),
        provider,
        registry,
        TileRuntimeConfig {
            // >= 3 so the class shares partition exactly (terrain 1, provider 1,
            // transcode 1) and the CPU-work isolation tests exercise the real
            // production regime rather than the sub-three shared-ceiling floor.
            mapterhorn: None,
            cpu_work_concurrency: 3,
            cpu_work_max_inflight: 4,
            derived_negative_ttl: Duration::from_secs(60),
        },
    );

    Harness {
        public: with_common_layers(public_router(), state.clone()),
        internal: with_common_layers(internal_router(), state.clone()),
        state,
        membership,
        tiles_dir,
        uncached_upstream_requests,
        invalid_upstream_requests,
        aged_upstream_requests,
        revalidated_upstream_requests,
    }
}

#[tokio::test]
async fn tiles_and_transcodes_run_while_terrain_saturates_its_cpu_share() {
    let harness = harness("tile-isolation").await;

    // Saturate the terrain CPU class as a stuck generation flood would. Terrain
    // gets its own execution share; stored tiles need no CPU permit, and MLT
    // transcode has a reserved share terrain cannot take — so both proceed
    // *without* the terrain permit ever being released.
    let _heavy = saturate_terrain_cpu(&harness.state).await;

    let (status, _, _) = harness
        .get(&harness.public, "/tilesets/fixture/0/0/0")
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "stored tiles must not touch CPU pools"
    );

    let (status, headers, _) = tokio::time::timeout(
        Duration::from_secs(2),
        harness.get(&harness.public, "/tilesets/fixture/0/0/0.mlt"),
    )
    .await
    .expect("transcode must not wait on the terrain class");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "application/vnd.maplibre-tile"
    );

    harness.cleanup();
}

#[tokio::test]
async fn styles_run_while_terrain_saturates_its_cpu_share() {
    let harness = harness("style-isolation").await;

    // A terrain flood holds every terrain execution permit. Provider work has
    // its own reserved share, so style rewriting runs concurrently — not after
    // the terrain permit releases.
    let _heavy = saturate_terrain_cpu(&harness.state).await;

    let (status, headers, _) = tokio::time::timeout(
        Duration::from_secs(2),
        harness.get(&harness.public, "/styles/base/style.json"),
    )
    .await
    .expect("style must not wait on the terrain class");
    assert_eq!(status, StatusCode::OK);
    assert!(headers.contains_key(header::ETAG));

    harness.cleanup();
}

/// Holds every terrain-class CPU permit, standing in for a terrain generation
/// flood. Kept alive by the returned guard for the duration of the assertions.
async fn saturate_terrain_cpu(state: &AppState) -> Vec<super::AdmittedCpuWorkPermit> {
    let mut held = Vec::new();
    // Acquire well past any realistic share; each `acquire` returns immediately
    // while permits remain and the loop stops once the class is fully occupied.
    for _ in 0..64 {
        match tokio::time::timeout(
            Duration::from_millis(50),
            state.acquire_admitted_cpu_work("test_saturation"),
        )
        .await
        {
            Ok(Ok(permit)) => held.push(permit),
            _ => break,
        }
    }
    assert!(!held.is_empty(), "expected at least one terrain permit");
    held
}

#[tokio::test]
async fn concurrent_uncacheable_requests_share_the_leader_body() {
    let harness = harness("uncacheable-singleflight").await;
    let path = "/styles/uncached/style.json";
    let (first, second, third) = tokio::join!(
        harness.get(&harness.public, path),
        harness.get(&harness.public, path),
        harness.get(&harness.public, path),
    );
    for (status, headers, _) in [first, second, third] {
        assert_eq!(status, StatusCode::OK);
        assert_eq!(headers[header::CACHE_CONTROL], "no-store");
    }
    assert_eq!(
        harness.uncached_upstream_requests.load(Ordering::Relaxed),
        1,
        "followers must reuse the uncacheable leader representation"
    );

    // It remains uncacheable for a later, independent request.
    assert_eq!(harness.get(&harness.public, path).await.0, StatusCode::OK);
    assert_eq!(
        harness.uncached_upstream_requests.load(Ordering::Relaxed),
        2
    );
    harness.cleanup();
}

#[tokio::test]
async fn transported_age_without_explicit_freshness_still_caches() {
    let harness = harness("aged-default").await;
    let path = "/styles/aged/style.json";

    let (status, _, _) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    // A second request is served from the provider cache: the transported
    // `Age: 100000` must not be charged against the invented default TTL, which
    // would otherwise evict on insert and re-fetch on every request.
    let (status, _, _) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        harness.aged_upstream_requests.load(Ordering::Relaxed),
        1,
        "default-TTL responses must cache regardless of transported Age"
    );
    harness.cleanup();
}

#[tokio::test]
async fn stale_provider_revalidation_reuses_bytes_on_origin_304() {
    let harness = harness("provider-revalidation").await;
    let path = "/styles/revalidated/style.json";

    let (status, initial_headers, initial_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        initial_headers[header::CACHE_CONTROL],
        "public, max-age=0, s-maxage=0, stale-while-revalidate=60"
    );
    assert_eq!(
        harness
            .revalidated_upstream_requests
            .load(Ordering::Relaxed),
        1
    );

    // The immediately stale body is served without waiting while one background
    // request revalidates it with the origin ETag.
    let (status, _, stale_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(stale_body, initial_body);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let (_, _, metrics) = harness
                .get(&harness.internal, "/_internal/metrics")
                .await;
            if String::from_utf8_lossy(&metrics).contains(
                "ishikari_provider_resource_cache_total{outcome=\"revalidated\",resource=\"style\"} 1",
            ) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("background conditional revalidation");

    // The 304 changed freshness to 120 seconds without sending another body.
    // A third request must use that refreshed entry rather than hit the origin.
    let (status, refreshed_headers, refreshed_body) = harness.get(&harness.public, path).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(refreshed_body, initial_body);
    assert_eq!(
        refreshed_headers[header::CACHE_CONTROL],
        "public, max-age=120, s-maxage=120, stale-while-revalidate=60"
    );
    assert_eq!(
        harness
            .revalidated_upstream_requests
            .load(Ordering::Relaxed),
        2
    );

    let (status, _, metrics) = harness.get(&harness.internal, "/_internal/metrics").await;
    assert_eq!(status, StatusCode::OK);
    assert!(String::from_utf8_lossy(&metrics).contains(
        "ishikari_provider_resource_cache_total{outcome=\"revalidated\",resource=\"style\"} 1"
    ));
    harness.cleanup();
}

#[tokio::test]
async fn non_200_success_statuses_are_rejected_as_upstream_errors() {
    let harness = harness("non-200-style").await;
    for path in ["/styles/nocontent/style.json", "/styles/partial/style.json"] {
        let (status, _, _) = harness.get(&harness.public, path).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "path: {path}");
    }
    harness.cleanup();
}

#[tokio::test]
async fn invalid_style_json_never_enters_the_provider_cache() {
    let harness = harness("invalid-style").await;
    let path = "/styles/invalid/style.json";
    assert_eq!(
        harness.get(&harness.public, path).await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        harness.get(&harness.public, path).await.0,
        StatusCode::BAD_GATEWAY
    );
    assert_eq!(
        harness.invalid_upstream_requests.load(Ordering::Relaxed),
        2,
        "invalid successful origin responses must not be cached"
    );

    let (status, _, metrics) = harness.get(&harness.internal, "/_internal/metrics").await;
    assert_eq!(status, StatusCode::OK);
    let metrics = String::from_utf8_lossy(&metrics);
    let validation = metrics
        .lines()
        .find(|line| {
            line.starts_with("ishikari_cpu_work_admission_total{")
                && line.contains("work=\"provider_json_validation\"")
                && line.contains("outcome=\"accepted\"")
        })
        .expect("provider JSON validation admission metric");
    assert!(validation.ends_with(" 2"), "{validation}");
    assert!(
        !metrics.contains("work=\"style_rewrite\""),
        "invalid JSON must fail before style rewriting: {metrics}"
    );
    harness.cleanup();
}

#[tokio::test]
async fn public_provider_responses_carry_cache_policy_and_age() {
    let harness = harness("public").await;

    // Style: upstream policy is honored and normalized, body is rewritten JSON.
    let (status, headers, body) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[header::AGE], "10");
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");
    assert!(headers.get(header::CONTENT_ENCODING).is_none());
    assert!(
        headers.get(PROVIDER_CACHE_CONTROL_HEADER).is_none(),
        "internal metadata must not leak on the public port"
    );
    let style: serde_json::Value = serde_json::from_slice(&body).expect("style JSON");
    assert_eq!(style["version"], 8);

    // A repeat request is served from the provider cache with the same policy.
    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert!(headers.contains_key(header::AGE));

    // Glyphs and sprites have no upstream policy: the asset default applies.
    // Glyph bytes are verbatim, so upstream validators pass through unchanged.
    let (status, headers, body) = harness
        .get(&harness.public, "/fonts/TestFont/0-255.pbf")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::AGE], "0");
    assert_eq!(headers[header::ETAG], GLYPH_UPSTREAM_ETAG);
    assert_eq!(headers[header::LAST_MODIFIED], GLYPH_UPSTREAM_LAST_MODIFIED);
    assert_eq!(body.as_ref(), b"glyph-bytes");

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/sprite.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::CONTENT_TYPE], "application/json");

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/sprite.png")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert_eq!(headers[header::CONTENT_TYPE], "image/png");

    harness.cleanup();
}

#[tokio::test]
async fn internal_provider_responses_carry_typed_metadata_not_public_headers() {
    let harness = harness("internal").await;

    let (status, headers, _) = harness
        .get(
            &harness.internal,
            "/_internal/provider/styles/base/style.json",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        headers[PROVIDER_CACHE_CONTROL_HEADER],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[PROVIDER_AGE_HEADER], "10");
    assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
    assert!(
        headers.get(header::CACHE_CONTROL).is_none(),
        "internal responses must not carry public caching headers"
    );

    let (status, headers, body) = harness
        .get(
            &harness.internal,
            "/_internal/provider/fonts/TestFont/0-255.pbf",
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[PROVIDER_CACHE_CONTROL_HEADER], cache::GLYPH_SPRITE);
    assert!(headers.contains_key(PROVIDER_AGE_HEADER));
    assert_eq!(headers[PROVIDER_ETAG_HEADER], GLYPH_UPSTREAM_ETAG);
    assert_eq!(
        headers[PROVIDER_LAST_MODIFIED_HEADER],
        GLYPH_UPSTREAM_LAST_MODIFIED
    );
    assert!(headers.get(header::CACHE_CONTROL).is_none());
    assert_eq!(body.as_ref(), b"glyph-bytes");

    harness.cleanup();
}

#[tokio::test]
async fn conditional_requests_return_304_with_cache_metadata() {
    let harness = harness("conditional").await;

    // Matching If-None-Match on a verbatim resource: 304, no body, but the
    // full cache metadata so downstream caches can refresh their entry.
    let (status, headers, body) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_NONE_MATCH, GLYPH_UPSTREAM_ETAG)],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(headers[header::CACHE_CONTROL], cache::GLYPH_SPRITE);
    assert!(headers.contains_key(header::AGE));
    assert_eq!(headers[header::ETAG], GLYPH_UPSTREAM_ETAG);

    // Non-matching validator: full 200 body.
    let (status, _, body) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_NONE_MATCH, "\"stale\"")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body.as_ref(), b"glyph-bytes");

    // If-Modified-Since at the upstream Last-Modified date matches.
    let (status, _, _) = harness
        .get_with(
            &harness.public,
            "/fonts/TestFont/0-255.pbf",
            &[(header::IF_MODIFIED_SINCE, GLYPH_UPSTREAM_LAST_MODIFIED)],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);

    // `*` matches any existing representation, even when the origin supplied
    // no entity tag.
    let (status, headers, _) = harness
        .get_with(
            &harness.public,
            "/styles/base/sprite.json",
            &[(header::IF_NONE_MATCH, "*")],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(headers.get(header::ETAG).is_none());

    // Style: the derived ETag identifies the rewritten body, is stable across
    // requests, and round-trips through If-None-Match.
    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    let style_etag = headers[header::ETAG].to_str().expect("etag").to_owned();
    assert_ne!(style_etag, GLYPH_UPSTREAM_ETAG);
    assert!(
        headers.get(header::LAST_MODIFIED).is_none(),
        "derived body must not reuse the upstream Last-Modified"
    );

    let (status, headers, _) = harness
        .get(&harness.public, "/styles/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::ETAG], style_etag.as_str());

    let (status, headers, body) = harness
        .get_with(
            &harness.public,
            "/styles/base/style.json",
            &[(header::IF_NONE_MATCH, style_etag.as_str())],
        )
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(
        headers[header::CACHE_CONTROL],
        STYLE_NORMALIZED_CACHE_CONTROL
    );
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");

    harness.cleanup();
}

#[tokio::test]
async fn rewritten_styles_vary_with_the_effective_origin() {
    let harness = harness("style-origin").await;
    let path = "/styles/base/style.json";
    let forwarded_proto = header::HeaderName::from_static("x-forwarded-proto");

    let (status, first_headers, first_body) = harness
        .get_with(
            &harness.public,
            path,
            &[
                (header::HOST, "one.example"),
                (forwarded_proto.clone(), "https"),
            ],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(first_headers[header::VARY], "Origin, X-Forwarded-Proto");
    let first: serde_json::Value = serde_json::from_slice(&first_body).expect("style JSON");
    assert_eq!(
        first["glyphs"],
        "https://one.example/fonts/{fontstack}/{range}.pbf"
    );

    let (status, second_headers, second_body) = harness
        .get_with(
            &harness.public,
            path,
            &[(header::HOST, "two.example"), (forwarded_proto, "https")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    let second: serde_json::Value = serde_json::from_slice(&second_body).expect("style JSON");
    assert_eq!(
        second["glyphs"],
        "https://two.example/fonts/{fontstack}/{range}.pbf"
    );
    assert_ne!(first_body, second_body);
    assert_ne!(first_headers[header::ETAG], second_headers[header::ETAG]);

    harness.cleanup();
}

#[tokio::test]
async fn namespaced_style_uses_the_matching_upstream_template() {
    let harness = harness("namespaced-style").await;

    let (status, headers, body) = harness
        .get(&harness.public, "/styles/regional/base/style.json")
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers[header::CACHE_CONTROL], cache::STYLE);
    let style: serde_json::Value = serde_json::from_slice(&body).expect("style JSON");
    assert_eq!(style["name"], "regional");

    harness.cleanup();
}

#[tokio::test]
async fn tile_routes_serve_stored_mvt_and_negotiate_mlt() {
    let harness = harness("tile-negotiation").await;
    let path = "/tilesets/fixture/0/0/0";

    let (status, headers, stored) = harness.get(&harness.public, path).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&stored)
    );
    assert_eq!(
        headers[header::CONTENT_TYPE],
        "application/vnd.mapbox-vector-tile"
    );
    assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILE);
    assert_eq!(headers[header::VARY], "Accept");
    let mut decoded = Vec::new();
    flate2::read::GzDecoder::new(stored.as_ref())
        .read_to_end(&mut decoded)
        .expect("decode stored MVT");
    assert!(decoded.is_empty());

    for (path, request_headers) in [
        ("/tilesets/fixture/0/0/0.mlt", Vec::new()),
        (
            path,
            vec![(header::ACCEPT, "application/vnd.maplibre-tile")],
        ),
    ] {
        let (status, headers, mlt) = harness
            .get_with(&harness.public, path, &request_headers)
            .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            headers[header::CONTENT_TYPE],
            "application/vnd.maplibre-tile"
        );
        assert_eq!(headers[header::CONTENT_ENCODING], "gzip");
        assert_eq!(headers[header::CACHE_CONTROL], cache::TILE);
        assert_eq!(headers[header::VARY], "Accept");
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(mlt.as_ref())
            .read_to_end(&mut decoded)
            .expect("decode negotiated MLT");
        assert!(decoded.is_empty());
    }

    harness.cleanup();
}

#[tokio::test]
async fn tilejson_emits_a_derived_etag_and_answers_conditional_requests() {
    let harness = harness("tilejson-conditional").await;
    let path = "/tilesets/fixture";

    let (status, headers, body) = harness.get(&harness.public, path).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "body: {}",
        String::from_utf8_lossy(&body)
    );
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILEJSON);
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");
    // TileJSON is a derived representation: strong ETag, no Last-Modified.
    let etag = headers[header::ETAG].to_str().expect("etag").to_owned();
    assert!(
        etag.starts_with('"'),
        "expected a strong ETag, got {etag:?}"
    );
    assert!(headers.get(header::LAST_MODIFIED).is_none());
    let document: serde_json::Value = serde_json::from_slice(&body).expect("tilejson JSON");
    assert_eq!(document["tilejson"], "3.0.0");

    // The ETag is stable across requests with the same origin.
    let (_, headers, _) = harness.get(&harness.public, path).await;
    assert_eq!(headers[header::ETAG].to_str().unwrap(), etag);

    // A matching If-None-Match yields 304 with cache metadata, no body.
    let (status, headers, body) = harness
        .get_with(&harness.public, path, &[(header::IF_NONE_MATCH, &etag)])
        .await;
    assert_eq!(status, StatusCode::NOT_MODIFIED);
    assert!(body.is_empty());
    assert_eq!(headers[header::CACHE_CONTROL], cache::TILEJSON);
    assert_eq!(headers[header::ETAG].to_str().unwrap(), etag);
    assert_eq!(headers[header::VARY], "Origin, X-Forwarded-Proto");

    // A non-matching validator serves the full document.
    let (status, _, body) = harness
        .get_with(
            &harness.public,
            path,
            &[(header::IF_NONE_MATCH, "\"stale\"")],
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body.is_empty());

    harness.cleanup();
}

#[tokio::test]
async fn internal_paths_are_not_exposed_on_the_public_router() {
    let harness = harness("exposure").await;

    // Sanity: the public router itself works.
    let (status, _, _) = harness.get(&harness.public, "/livez").await;
    assert_eq!(status, StatusCode::OK);

    for path in [
        "/_internal/metrics",
        "/_internal/healthz",
        "/_internal/cluster",
        "/_internal/provider/styles/base/style.json",
        "/_internal/provider/fonts/TestFont/0-255.pbf",
        "/_internal/tiles/demo/0",
    ] {
        let (status, _, _) = harness.get(&harness.public, path).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{path} must 404 publicly");
    }

    harness.cleanup();
}

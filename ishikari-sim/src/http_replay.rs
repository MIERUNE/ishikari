use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    fs::File,
    io::BufReader,
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail, ensure};
use ishikari::{
    pmtiles::{TileCoord, TileId},
    storage::TilesetId,
};
use reqwest::{Client, Response, StatusCode, Url, header};
use serde::Serialize;
use tokio::task::JoinSet;

use crate::{
    TraceEntry, read_trace_with_digest, report::calculate_derived_rates, trace::format_fnv1a64,
    viewport_batch_ranges,
};

const HTTP_REPLAY_SCHEMA_VERSION: u32 = 2;
const MAX_FAILURE_SAMPLES: usize = 20;
const MAX_REPLAY_BODY_BYTES: u64 = 64 * 1024 * 1024;
const MAX_METRICS_BODY_BYTES: u64 = 8 * 1024 * 1024;
const METRICS_SCRAPE_CONCURRENCY: u8 = 1;
// `u64::MAX as f64` rounds up to this value. Use it as an exclusive bound so
// the float-to-integer cast cannot silently saturate an out-of-range sample.
const U64_UPPER_BOUND_EXCLUSIVE: f64 = 18_446_744_073_709_551_616.0;

#[derive(Clone, Debug)]
pub enum HttpReplayTarget {
    DirectNodes { node_urls: Vec<Url> },
    Gateway { gateway_url: Url },
}

#[derive(Clone, Copy, Debug, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpExecutionMode {
    Serial,
    ViewportBatches,
}

#[derive(Clone, Debug)]
pub struct HttpReplayConfig {
    pub trace_path: PathBuf,
    pub target: HttpReplayTarget,
    pub mode: HttpExecutionMode,
    pub metrics_urls: Vec<Url>,
    pub request_timeout: Duration,
}

#[derive(Debug, Serialize)]
pub struct HttpReplayReport {
    schema_version: u32,
    kind: &'static str,
    runner_version: &'static str,
    trace: TraceFingerprint,
    execution: HttpExecutionReport,
    target: HttpTargetReport,
    result: HttpReplayResult,
    prometheus: PrometheusCapture,
}

impl HttpReplayReport {
    pub fn is_success(&self) -> bool {
        self.result.transport_errors == 0
            && self.result.unexpected_statuses == 0
            && !matches!(self.prometheus, PrometheusCapture::Failed { .. })
    }
}

#[derive(Debug, Serialize)]
struct TraceFingerprint {
    path: PathBuf,
    requests: usize,
    bytes: u64,
    fnv1a64: String,
}

#[derive(Debug, Serialize)]
struct HttpExecutionReport {
    mode: HttpExecutionMode,
    request_timeout_ms: u128,
    redirects: bool,
    retries: u8,
    cache_control_no_cache: bool,
    max_response_body_bytes: u64,
    max_metrics_body_bytes: u64,
    metrics_scrape_concurrency: u8,
}

#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum HttpTargetReport {
    DirectNodes { node_urls: Vec<String> },
    Gateway { gateway_url: String },
}

#[derive(Debug, Serialize)]
struct HttpReplayResult {
    attempted: usize,
    responses: usize,
    transport_errors: usize,
    unexpected_statuses: usize,
    status_counts: BTreeMap<u16, u64>,
    response_body_bytes: u64,
    elapsed_ms: f64,
    throughput_rps: f64,
    latency_ms: HttpLatencySummary,
    failure_samples: Vec<HttpFailureSample>,
}

#[derive(Debug, Default, Serialize)]
struct HttpLatencySummary {
    count: usize,
    mean: f64,
    p50: f64,
    p90: f64,
    p95: f64,
    p99: f64,
    max: f64,
}

#[derive(Debug, Serialize)]
struct HttpFailureSample {
    trace_index: usize,
    step: u64,
    user: usize,
    ordinal: usize,
    url: String,
    category: &'static str,
    detail: String,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum PrometheusCapture {
    Disabled,
    Complete {
        nodes: Vec<PrometheusNodeReport>,
        aggregate: Box<ComparableMetrics>,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Serialize)]
struct PrometheusNodeReport {
    target_index: usize,
    metrics_url: String,
    result: ComparableMetrics,
}

#[derive(Clone, Debug, Default, Serialize)]
struct ComparableMetrics {
    requests: u64,
    found: u64,
    not_found: u64,
    served_bytes: u64,
    by_source: BTreeMap<String, u64>,
    peer_requests: u64,
    peer_bytes: u64,
    backend_bytes: u64,
    l1_cache_hits: u64,
    negative_cache_hits: u64,
    l1_cache_hit_rate: f64,
    cache_hit_rate: f64,
    peer_forward_rate: f64,
    read_amplification: f64,
    backend_fetches: u64,
    backend_fetch_outcomes: BTreeMap<String, u64>,
    backend_fetched_chunks: u64,
    chunk_cache: BTreeMap<String, u64>,
    chunk_fetch_wait: BTreeMap<String, u64>,
}

impl ComparableMetrics {
    fn from_delta(before: &MetricSnapshot, after: &MetricSnapshot) -> Result<Self> {
        let mut result = Self::default();
        for source in [
            "self_cache",
            "self_backend",
            "peer_cache",
            "peer_backend",
            "miss",
        ] {
            result.by_source.insert(
                source.to_string(),
                counter_delta(
                    before,
                    after,
                    "ishikari_tiles_served_total",
                    &[("source", source)],
                )?,
            );
        }
        result.requests = result.by_source.values().try_fold(0_u64, |total, value| {
            checked_metric_add(total, *value, "requests")
        })?;
        result.not_found = result.by_source.get("miss").copied().unwrap_or_default();
        result.found = result.requests.saturating_sub(result.not_found);
        result.served_bytes =
            counter_delta(before, after, "ishikari_external_egress_bytes_total", &[])?;
        result.peer_bytes =
            counter_delta(before, after, "ishikari_internal_egress_bytes_total", &[])?;
        result.backend_bytes =
            counter_delta(before, after, "ishikari_backend_fetch_bytes_total", &[])?;
        result.l1_cache_hits = counter_delta(
            before,
            after,
            "ishikari_tile_cache_total",
            &[("outcome", "hit")],
        )?;
        result.negative_cache_hits = required_counter_delta(
            before,
            after,
            "ishikari_tile_negative_cache_hits_total",
            &[],
        )?;
        result.peer_requests =
            sum_counter_family_delta(before, after, "ishikari_peer_fetch_total")?;

        for outcome in ["success", "not_found", "error", "timeout"] {
            let count = counter_delta(
                before,
                after,
                "ishikari_backend_fetch_duration_seconds_count",
                &[("outcome", outcome)],
            )?;
            result.backend_fetches =
                checked_metric_add(result.backend_fetches, count, "backend_fetches")?;
            result
                .backend_fetch_outcomes
                .insert(outcome.to_string(), count);
        }
        result.backend_fetched_chunks = counter_delta(
            before,
            after,
            "ishikari_backend_fetch_chunks_sum",
            &[("outcome", "success")],
        )?;
        for outcome in ["hit", "miss", "post_fetch_hit"] {
            result.chunk_cache.insert(
                outcome.to_string(),
                counter_delta(
                    before,
                    after,
                    "ishikari_chunk_cache_total",
                    &[("outcome", outcome)],
                )?,
            );
        }
        for outcome in ["queued", "joined_pending", "joined_inflight"] {
            result.chunk_fetch_wait.insert(
                outcome.to_string(),
                counter_delta(
                    before,
                    after,
                    "ishikari_chunk_fetch_wait_total",
                    &[("outcome", outcome)],
                )?,
            );
        }
        result.finalize_rates();
        Ok(result)
    }

    fn add_assign(&mut self, other: &Self) -> Result<()> {
        macro_rules! add_fields {
            ($($field:ident),+ $(,)?) => {
                $(self.$field = checked_metric_add(
                    self.$field,
                    other.$field,
                    stringify!($field),
                )?;)+
            };
        }
        add_fields!(
            requests,
            found,
            not_found,
            served_bytes,
            peer_requests,
            peer_bytes,
            backend_bytes,
            l1_cache_hits,
            negative_cache_hits,
            backend_fetches,
            backend_fetched_chunks,
        );
        add_map("by_source", &mut self.by_source, &other.by_source)?;
        add_map(
            "backend_fetch_outcomes",
            &mut self.backend_fetch_outcomes,
            &other.backend_fetch_outcomes,
        )?;
        add_map("chunk_cache", &mut self.chunk_cache, &other.chunk_cache)?;
        add_map(
            "chunk_fetch_wait",
            &mut self.chunk_fetch_wait,
            &other.chunk_fetch_wait,
        )?;
        Ok(())
    }

    fn finalize_rates(&mut self) {
        let (request_rates, read_amplification) = calculate_derived_rates(
            self.requests,
            self.served_bytes,
            self.backend_bytes,
            self.l1_cache_hits,
            &self.by_source,
        );
        if let Some((l1, cache, peer)) = request_rates {
            self.l1_cache_hit_rate = l1;
            self.cache_hit_rate = cache;
            self.peer_forward_rate = peer;
        }
        if let Some(read_amplification) = read_amplification {
            self.read_amplification = read_amplification;
        }
    }
}

fn checked_metric_add(left: u64, right: u64, field: &str) -> Result<u64> {
    left.checked_add(right)
        .with_context(|| format!("Prometheus counter aggregate overflow for {field}"))
}

fn add_map(
    family: &str,
    target: &mut BTreeMap<String, u64>,
    other: &BTreeMap<String, u64>,
) -> Result<()> {
    for (key, value) in other {
        let target_value = target.entry(key.clone()).or_default();
        *target_value = checked_metric_add(*target_value, *value, &format!("{family}[{key}]"))?;
    }
    Ok(())
}

struct PlannedHttpRequest {
    trace_index: usize,
    step: u64,
    user: usize,
    ordinal: usize,
    url: Url,
}

struct HttpRequestOutcome {
    plan: PlannedHttpRequest,
    latency: Duration,
    status: Option<StatusCode>,
    body_bytes: u64,
    error_category: Option<&'static str>,
    error_detail: Option<String>,
}

struct HttpOutcomeAccumulator {
    attempted: usize,
    responses: usize,
    transport_errors: usize,
    unexpected_statuses: usize,
    status_counts: BTreeMap<u16, u64>,
    response_body_bytes: u64,
    latencies: Vec<Duration>,
    failure_samples: Vec<HttpFailureSample>,
}

impl HttpOutcomeAccumulator {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            attempted: 0,
            responses: 0,
            transport_errors: 0,
            unexpected_statuses: 0,
            status_counts: BTreeMap::new(),
            response_body_bytes: 0,
            // Latencies are the only per-request replay result retained. Plans
            // and outcomes are consumed one at a time (or one viewport batch at
            // a time), avoiding two additional trace-sized allocations.
            latencies: Vec::with_capacity(capacity),
            failure_samples: Vec::new(),
        }
    }

    fn record(&mut self, outcome: HttpRequestOutcome) {
        self.attempted += 1;
        self.latencies.push(outcome.latency);
        if let Some(status) = outcome.status {
            self.responses += 1;
            *self.status_counts.entry(status.as_u16()).or_default() += 1;
            self.response_body_bytes = self.response_body_bytes.saturating_add(outcome.body_bytes);
            if !matches!(status, StatusCode::OK | StatusCode::NOT_FOUND) {
                self.unexpected_statuses += 1;
                push_failure(
                    &mut self.failure_samples,
                    &outcome,
                    "status",
                    status.to_string(),
                );
            }
        }
        if let Some(category) = outcome.error_category {
            self.transport_errors += 1;
            push_failure(
                &mut self.failure_samples,
                &outcome,
                category,
                outcome
                    .error_detail
                    .clone()
                    .unwrap_or_else(|| "unknown error".to_string()),
            );
        }
    }

    fn finish(self, elapsed: Duration) -> HttpReplayResult {
        HttpReplayResult {
            attempted: self.attempted,
            responses: self.responses,
            transport_errors: self.transport_errors,
            unexpected_statuses: self.unexpected_statuses,
            status_counts: self.status_counts,
            response_body_bytes: self.response_body_bytes,
            elapsed_ms: elapsed.as_secs_f64() * 1_000.0,
            throughput_rps: if elapsed.is_zero() {
                0.0
            } else {
                self.attempted as f64 / elapsed.as_secs_f64()
            },
            latency_ms: summarize_latencies(self.latencies),
            failure_samples: self.failure_samples,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SeriesKey {
    name: String,
    labels: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Default)]
struct MetricSnapshot {
    samples: BTreeMap<SeriesKey, f64>,
}

/// Replays one existing trace against public Ishikari HTTP endpoints and
/// optionally captures per-node Prometheus counter deltas for calibration.
pub async fn run_http_replay(config: HttpReplayConfig) -> Result<HttpReplayReport> {
    validate_config(&config)?;
    let trace_file = File::open(&config.trace_path)
        .with_context(|| format!("open HTTP replay trace {}", config.trace_path.display()))?;
    // Fingerprint during the parse: reopening the file to hash it separately
    // would let a concurrent replacement describe different bytes than were
    // executed.
    let (entries, digest) = read_trace_with_digest(BufReader::new(trace_file))?;
    ensure!(!entries.is_empty(), "HTTP replay trace must not be empty");
    // Reject a malformed late entry before issuing any live request, but do
    // not retain a second trace-sized vector of URL plans.
    validate_trace_requests(&entries, &config.target)?;
    let trace = TraceFingerprint {
        path: config.trace_path.clone(),
        requests: entries.len(),
        bytes: digest.bytes,
        fnv1a64: format_fnv1a64(digest.fnv1a64),
    };

    let client = Client::builder()
        .timeout(config.request_timeout)
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("build HTTP replay client")?;
    let before_metrics = if config.metrics_urls.is_empty() {
        None
    } else {
        Some(scrape_metrics(&client, &config.metrics_urls).await?)
    };

    let started_at = Instant::now();
    let outcomes = execute_requests(&client, &entries, &config.target, config.mode).await?;
    let elapsed = started_at.elapsed();
    let result = outcomes.finish(elapsed);

    let prometheus = match before_metrics {
        None => PrometheusCapture::Disabled,
        Some(before) => match scrape_metrics(&client, &config.metrics_urls).await {
            Ok(after) => match build_prometheus_report(&config.metrics_urls, &before, &after) {
                Ok((nodes, aggregate)) => PrometheusCapture::Complete {
                    nodes,
                    aggregate: Box::new(aggregate),
                },
                Err(error) => PrometheusCapture::Failed {
                    error: format!("derive Prometheus deltas: {error:#}"),
                },
            },
            Err(error) => PrometheusCapture::Failed {
                error: format!("post-replay Prometheus scrape: {error:#}"),
            },
        },
    };

    Ok(HttpReplayReport {
        schema_version: HTTP_REPLAY_SCHEMA_VERSION,
        kind: "ishikari_http_replay",
        runner_version: env!("CARGO_PKG_VERSION"),
        trace,
        execution: HttpExecutionReport {
            mode: config.mode,
            request_timeout_ms: config.request_timeout.as_millis(),
            redirects: false,
            retries: 0,
            cache_control_no_cache: true,
            max_response_body_bytes: MAX_REPLAY_BODY_BYTES,
            max_metrics_body_bytes: MAX_METRICS_BODY_BYTES,
            metrics_scrape_concurrency: METRICS_SCRAPE_CONCURRENCY,
        },
        target: target_report(&config.target),
        result,
        prometheus,
    })
}

fn validate_config(config: &HttpReplayConfig) -> Result<()> {
    ensure!(
        !config.request_timeout.is_zero(),
        "HTTP replay request timeout must be positive"
    );
    let public_urls = match &config.target {
        HttpReplayTarget::DirectNodes { node_urls } => {
            ensure!(!node_urls.is_empty(), "direct replay requires node URLs");
            if !config.metrics_urls.is_empty() {
                ensure!(
                    config.metrics_urls.len() == node_urls.len(),
                    "direct replay requires one metrics URL per node URL"
                );
            }
            node_urls
        }
        HttpReplayTarget::Gateway { gateway_url } => std::slice::from_ref(gateway_url),
    };
    validate_unique_urls("public target", public_urls)?;
    validate_unique_urls("metrics", &config.metrics_urls)?;
    for url in public_urls {
        validate_public_base_url(url)?;
    }
    for url in &config.metrics_urls {
        validate_http_url(url, false)?;
    }
    Ok(())
}

fn validate_unique_urls(kind: &str, urls: &[Url]) -> Result<()> {
    let mut seen = HashSet::with_capacity(urls.len());
    ensure!(
        urls.iter().all(|url| seen.insert(url.as_str())),
        "duplicate {kind} URL"
    );
    Ok(())
}

fn validate_public_base_url(url: &Url) -> Result<()> {
    validate_http_url(url, true)
}

fn validate_http_url(url: &Url, require_root_path: bool) -> Result<()> {
    ensure!(
        matches!(url.scheme(), "http" | "https"),
        "URL scheme must be http or https: {url}"
    );
    ensure!(
        url.username().is_empty() && url.password().is_none(),
        "URL must not contain credentials"
    );
    ensure!(
        url.query().is_none() && url.fragment().is_none(),
        "URL must not contain a query or fragment: {url}"
    );
    if require_root_path {
        ensure!(
            url.path() == "/",
            "public target URL must have a root path: {url}"
        );
    }
    Ok(())
}

fn validate_trace_requests(entries: &[TraceEntry], target: &HttpReplayTarget) -> Result<()> {
    for (trace_index, entry) in entries.iter().enumerate() {
        plan_request(trace_index, entry, target)?;
    }
    Ok(())
}

fn plan_request(
    trace_index: usize,
    entry: &TraceEntry,
    target: &HttpReplayTarget,
) -> Result<PlannedHttpRequest> {
    let tileset = TilesetId::try_new(&entry.tileset)
        .with_context(|| format!("trace request {trace_index} has invalid tileset"))?;
    let coordinate = TileCoord::new(entry.z, entry.x, entry.y)
        .with_context(|| format!("trace request {trace_index} has invalid coordinate"))?;
    let _ = TileId::from(coordinate);
    let base = match target {
        HttpReplayTarget::Gateway { gateway_url } => gateway_url,
        HttpReplayTarget::DirectNodes { node_urls } => {
            let node = entry
                .entry_node
                .with_context(|| format!("trace request {trace_index} has no direct entry_node"))?;
            node_urls.get(node).with_context(|| {
                format!(
                    "trace request {trace_index} entry_node {node} exceeds {} direct targets",
                    node_urls.len()
                )
            })?
        }
    };
    let path = format!("tilesets/{tileset}/{}/{}/{}", entry.z, entry.x, entry.y);
    let url = base
        .join(&path)
        .with_context(|| format!("build tile URL for trace request {trace_index}"))?;
    Ok(PlannedHttpRequest {
        trace_index,
        step: entry.step,
        user: entry.user,
        ordinal: entry.ordinal,
        url,
    })
}

async fn execute_requests(
    client: &Client,
    entries: &[TraceEntry],
    target: &HttpReplayTarget,
    mode: HttpExecutionMode,
) -> Result<HttpOutcomeAccumulator> {
    let mut outcomes = HttpOutcomeAccumulator::with_capacity(entries.len());
    match mode {
        HttpExecutionMode::Serial => {
            for (trace_index, entry) in entries.iter().enumerate() {
                let plan = plan_request(trace_index, entry, target)?;
                outcomes.record(execute_request(client.clone(), plan).await);
            }
        }
        HttpExecutionMode::ViewportBatches => {
            for range in viewport_batch_ranges(entries)? {
                let mut tasks = JoinSet::new();
                for trace_index in range {
                    let plan = plan_request(trace_index, &entries[trace_index], target)?;
                    tasks.spawn(execute_request(client.clone(), plan));
                }
                while let Some(outcome) = tasks.join_next().await {
                    outcomes.record(outcome.context("HTTP replay request task failed")?);
                }
            }
        }
    }
    Ok(outcomes)
}

async fn execute_request(client: Client, plan: PlannedHttpRequest) -> HttpRequestOutcome {
    let started_at = Instant::now();
    let response = client
        .get(plan.url.clone())
        .header(header::CACHE_CONTROL, "no-cache")
        .send()
        .await;
    let response = match response {
        Ok(response) => response,
        Err(error) => {
            return HttpRequestOutcome {
                plan,
                latency: started_at.elapsed(),
                status: None,
                body_bytes: 0,
                error_category: Some(request_error_category(&error)),
                error_detail: Some(error.to_string()),
            };
        }
    };
    let status = response.status();
    // The replay only needs the byte count: stream and discard chunks instead
    // of buffering, and cap the total so a misconfigured target returning a
    // huge body cannot OOM the replay process under viewport concurrency.
    let mut response = response;
    let mut body_bytes = 0u64;
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                body_bytes = body_bytes.saturating_add(chunk.len() as u64);
                if body_bytes > MAX_REPLAY_BODY_BYTES {
                    return HttpRequestOutcome {
                        plan,
                        latency: started_at.elapsed(),
                        status: Some(status),
                        body_bytes,
                        error_category: Some("oversized_body"),
                        error_detail: Some(format!(
                            "response exceeded {MAX_REPLAY_BODY_BYTES} bytes"
                        )),
                    };
                }
            }
            Ok(None) => {
                return HttpRequestOutcome {
                    plan,
                    latency: started_at.elapsed(),
                    status: Some(status),
                    body_bytes,
                    error_category: None,
                    error_detail: None,
                };
            }
            Err(error) => {
                return HttpRequestOutcome {
                    plan,
                    latency: started_at.elapsed(),
                    status: Some(status),
                    body_bytes,
                    error_category: Some("body"),
                    error_detail: Some(error.to_string()),
                };
            }
        }
    }
}

fn request_error_category(error: &reqwest::Error) -> &'static str {
    if error.is_timeout() {
        "timeout"
    } else if error.is_connect() {
        "connect"
    } else if error.is_body() {
        "body"
    } else {
        "request"
    }
}

fn push_failure(
    failures: &mut Vec<HttpFailureSample>,
    outcome: &HttpRequestOutcome,
    category: &'static str,
    detail: String,
) {
    if failures.len() >= MAX_FAILURE_SAMPLES {
        return;
    }
    failures.push(HttpFailureSample {
        trace_index: outcome.plan.trace_index,
        step: outcome.plan.step,
        user: outcome.plan.user,
        ordinal: outcome.plan.ordinal,
        url: outcome.plan.url.to_string(),
        category,
        detail,
    });
}

fn summarize_latencies(mut values: Vec<Duration>) -> HttpLatencySummary {
    if values.is_empty() {
        return HttpLatencySummary::default();
    }
    values.sort_unstable();
    let sum = values.iter().map(Duration::as_secs_f64).sum::<f64>() * 1_000.0;
    HttpLatencySummary {
        count: values.len(),
        mean: sum / values.len() as f64,
        p50: percentile_ms(&values, 0.50),
        p90: percentile_ms(&values, 0.90),
        p95: percentile_ms(&values, 0.95),
        p99: percentile_ms(&values, 0.99),
        max: values
            .last()
            .map_or(0.0, |value| value.as_secs_f64() * 1_000.0),
    }
}

fn percentile_ms(values: &[Duration], quantile: f64) -> f64 {
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    values[index].as_secs_f64() * 1_000.0
}

async fn scrape_metrics(client: &Client, urls: &[Url]) -> Result<Vec<MetricSnapshot>> {
    scrape_metrics_with_limit(client, urls, MAX_METRICS_BODY_BYTES).await
}

async fn scrape_metrics_with_limit(
    client: &Client,
    urls: &[Url],
    max_body_bytes: u64,
) -> Result<Vec<MetricSnapshot>> {
    // Scrape sequentially so peak retained metrics memory is bounded by one
    // endpoint response rather than `urls.len() * max_body_bytes`.
    let mut snapshots = Vec::with_capacity(urls.len());
    for url in urls {
        let response = client
            .get(url.clone())
            .send()
            .await
            .with_context(|| format!("scrape metrics {url}"))?;
        ensure!(
            response.status() == StatusCode::OK,
            "metrics endpoint {url} returned {}",
            response.status()
        );
        let body = read_bounded_response(response, max_body_bytes, "metrics endpoint")
            .await
            .with_context(|| format!("read metrics body {url}"))?;
        let body = std::str::from_utf8(&body)
            .with_context(|| format!("metrics body {url} is not UTF-8"))?;
        snapshots.push(parse_metrics(body)?);
    }
    Ok(snapshots)
}

async fn read_bounded_response(
    mut response: Response,
    max_body_bytes: u64,
    kind: &str,
) -> Result<Vec<u8>> {
    if let Some(content_length) = response.content_length() {
        ensure!(
            content_length <= max_body_bytes,
            "{kind} response declares {content_length} bytes, exceeding {max_body_bytes}"
        );
    }
    let initial_capacity = response
        .content_length()
        .unwrap_or_default()
        .min(max_body_bytes) as usize;
    let mut body = Vec::with_capacity(initial_capacity);
    let mut body_bytes = 0_u64;
    while let Some(chunk) = response
        .chunk()
        .await
        .with_context(|| format!("stream {kind} response"))?
    {
        body_bytes = body_bytes
            .checked_add(chunk.len() as u64)
            .context("response byte count overflow")?;
        ensure!(
            body_bytes <= max_body_bytes,
            "{kind} response exceeded {max_body_bytes} bytes"
        );
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn build_prometheus_report(
    urls: &[Url],
    before: &[MetricSnapshot],
    after: &[MetricSnapshot],
) -> Result<(Vec<PrometheusNodeReport>, ComparableMetrics)> {
    ensure!(
        before.len() == after.len() && before.len() == urls.len(),
        "Prometheus scrape cardinality changed"
    );
    let mut nodes = Vec::with_capacity(urls.len());
    let mut aggregate = ComparableMetrics::default();
    for (index, ((before, after), url)) in before.iter().zip(after).zip(urls).enumerate() {
        let result = ComparableMetrics::from_delta(before, after)
            .with_context(|| format!("metrics target {index} ({url})"))?;
        aggregate.add_assign(&result)?;
        nodes.push(PrometheusNodeReport {
            target_index: index,
            metrics_url: url.to_string(),
            result,
        });
    }
    aggregate.finalize_rates();
    Ok((nodes, aggregate))
}

fn parse_metrics(input: &str) -> Result<MetricSnapshot> {
    let mut snapshot = MetricSnapshot::default();
    for (line_index, line) in input.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (series, value) = split_sample(line)
            .with_context(|| format!("parse Prometheus line {}", line_index + 1))?;
        if !series.name.starts_with("ishikari_") {
            continue;
        }
        ensure!(
            snapshot.samples.insert(series, value).is_none(),
            "duplicate Prometheus series on line {}",
            line_index + 1
        );
    }
    Ok(snapshot)
}

fn split_sample(line: &str) -> Result<(SeriesKey, f64)> {
    let mut quoted = false;
    let mut escaped = false;
    let split = line
        .char_indices()
        .find_map(|(index, character)| {
            if escaped {
                escaped = false;
                return None;
            }
            if quoted && character == '\\' {
                escaped = true;
                return None;
            }
            if character == '"' {
                quoted = !quoted;
                return None;
            }
            (!quoted && character.is_ascii_whitespace()).then_some(index)
        })
        .context("Prometheus sample has no value")?;
    let series = &line[..split];
    let value = line[split..]
        .split_ascii_whitespace()
        .next()
        .context("Prometheus sample has no value")?
        .parse::<f64>()
        .context("Prometheus sample value is invalid")?;
    let (name, labels) = match series.find('{') {
        Some(open) => {
            ensure!(series.ends_with('}'), "Prometheus labels are not closed");
            (
                &series[..open],
                parse_labels(&series[open + 1..series.len() - 1])?,
            )
        }
        None => (series, BTreeMap::new()),
    };
    ensure!(!name.is_empty(), "Prometheus metric name is empty");
    Ok((
        SeriesKey {
            name: name.to_string(),
            labels,
        },
        value,
    ))
}

fn parse_labels(input: &str) -> Result<BTreeMap<String, String>> {
    let bytes = input.as_bytes();
    let mut labels = BTreeMap::new();
    let mut index = 0;
    while index < bytes.len() {
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        let key_start = index;
        while index < bytes.len() && bytes[index] != b'=' {
            index += 1;
        }
        ensure!(
            index > key_start && index < bytes.len(),
            "invalid Prometheus label key"
        );
        let key = input[key_start..index].trim();
        index += 1;
        ensure!(
            index < bytes.len() && bytes[index] == b'"',
            "label {key} is not quoted"
        );
        index += 1;
        let mut value = String::new();
        let mut closed = false;
        while index < bytes.len() {
            match bytes[index] {
                b'"' => {
                    index += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    index += 1;
                    ensure!(index < bytes.len(), "unterminated label escape");
                    value.push(match bytes[index] {
                        b'n' => '\n',
                        b'\\' => '\\',
                        b'"' => '"',
                        other => other as char,
                    });
                    index += 1;
                }
                byte => {
                    value.push(byte as char);
                    index += 1;
                }
            }
        }
        ensure!(closed, "label {key} is not closed");
        ensure!(
            labels.insert(key.to_string(), value).is_none(),
            "duplicate Prometheus label {key}"
        );
        while index < bytes.len() && bytes[index].is_ascii_whitespace() {
            index += 1;
        }
        if index < bytes.len() {
            ensure!(
                bytes[index] == b',',
                "expected comma between Prometheus labels"
            );
            index += 1;
        }
    }
    Ok(labels)
}

fn required_counter_delta(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<u64> {
    let key = counter_key(name, labels);
    ensure!(
        before.samples.contains_key(&key) && after.samples.contains_key(&key),
        "required counter is unsupported or missing: {}{:?}",
        key.name,
        key.labels
    );
    counter_delta_for_key(before, after, &key)
}

fn counter_delta(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    name: &str,
    labels: &[(&str, &str)],
) -> Result<u64> {
    let key = counter_key(name, labels);
    counter_delta_for_key(before, after, &key)
}

fn counter_key(name: &str, labels: &[(&str, &str)]) -> SeriesKey {
    let labels = labels
        .iter()
        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
        .collect();
    SeriesKey {
        name: name.to_string(),
        labels,
    }
}

fn sum_counter_family_delta(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    name: &str,
) -> Result<u64> {
    let keys = before
        .samples
        .keys()
        .chain(after.samples.keys())
        .filter(|key| key.name == name)
        .cloned()
        .collect::<BTreeSet<_>>();
    keys.iter().try_fold(0_u64, |total, key| {
        checked_metric_add(total, counter_delta_for_key(before, after, key)?, name)
    })
}

fn counter_delta_for_key(
    before: &MetricSnapshot,
    after: &MetricSnapshot,
    key: &SeriesKey,
) -> Result<u64> {
    let before_value = before.samples.get(key).copied().unwrap_or(0.0);
    let Some(after_value) = after.samples.get(key).copied() else {
        if before.samples.contains_key(key) {
            bail!("counter series disappeared: {}{:?}", key.name, key.labels);
        }
        return Ok(0);
    };
    ensure!(
        before_value.is_finite() && after_value.is_finite(),
        "counter is not finite: {}{:?}",
        key.name,
        key.labels
    );
    ensure!(
        after_value >= before_value,
        "counter reset: {}{:?} before={before_value} after={after_value}",
        key.name,
        key.labels
    );
    let delta = after_value - before_value;
    let rounded = delta.round();
    ensure!(
        (delta - rounded).abs() < 1e-6,
        "counter delta is not an integer: {}{:?} delta={delta}",
        key.name,
        key.labels
    );
    ensure!(
        (0.0..U64_UPPER_BOUND_EXCLUSIVE).contains(&rounded),
        "counter delta is outside the u64 range: {}{:?} delta={delta}",
        key.name,
        key.labels
    );
    Ok(rounded as u64)
}

fn target_report(target: &HttpReplayTarget) -> HttpTargetReport {
    match target {
        HttpReplayTarget::DirectNodes { node_urls } => HttpTargetReport::DirectNodes {
            node_urls: node_urls.iter().map(ToString::to_string).collect(),
        },
        HttpReplayTarget::Gateway { gateway_url } => HttpTargetReport::Gateway {
            gateway_url: gateway_url.to_string(),
        },
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use axum::{Router, routing::get};

    use super::*;

    fn entry(index: usize, entry_node: Option<usize>) -> TraceEntry {
        TraceEntry {
            step: 0,
            user: 0,
            ordinal: index,
            tileset: "japan".to_string(),
            z: 0,
            x: 0,
            y: 0,
            entry_node,
        }
    }

    #[test]
    fn direct_replay_requires_valid_entry_node_and_gateway_ignores_it() {
        let direct = HttpReplayTarget::DirectNodes {
            node_urls: vec![Url::parse("http://node-0.example/").unwrap()],
        };
        assert!(plan_request(0, &entry(0, None), &direct).is_err());
        assert!(plan_request(0, &entry(0, Some(1)), &direct).is_err());

        let gateway = HttpReplayTarget::Gateway {
            gateway_url: Url::parse("https://gateway.example/").unwrap(),
        };
        let plan = plan_request(0, &entry(0, None), &gateway).expect("gateway plan");
        assert_eq!(
            plan.url.as_str(),
            "https://gateway.example/tilesets/japan/0/0/0"
        );
    }

    #[test]
    fn prometheus_delta_maps_comparable_fields_and_rejects_resets() {
        let before = parse_metrics(
            r#"
ishikari_tiles_served_total{source="self_cache"} 2
ishikari_tile_negative_cache_hits_total 4
ishikari_external_egress_bytes_total 100
ishikari_backend_fetch_bytes_total 50
ishikari_backend_fetch_duration_seconds_count{outcome="success"} 1
ishikari_backend_fetch_chunks_sum{outcome="success"} 2
ishikari_peer_fetch_total{resource="tile",outcome="success"} 3
"#,
        )
        .unwrap();
        let after = parse_metrics(
            r#"
ishikari_tiles_served_total{source="self_cache"} 5
ishikari_tiles_served_total{source="peer_cache"} 1
ishikari_tile_negative_cache_hits_total 7
ishikari_external_egress_bytes_total 220
ishikari_backend_fetch_bytes_total 90
ishikari_backend_fetch_duration_seconds_count{outcome="success"} 3
ishikari_backend_fetch_chunks_sum{outcome="success"} 7
ishikari_peer_fetch_total{outcome="success",resource="tile"} 5
"#,
        )
        .unwrap();
        let metrics = ComparableMetrics::from_delta(&before, &after).unwrap();
        assert_eq!(metrics.requests, 4);
        assert_eq!(metrics.served_bytes, 120);
        assert_eq!(metrics.backend_bytes, 40);
        assert_eq!(metrics.backend_fetches, 2);
        assert_eq!(metrics.backend_fetched_chunks, 5);
        assert_eq!(metrics.peer_requests, 2);
        assert_eq!(metrics.negative_cache_hits, 3);
        assert_eq!(metrics.cache_hit_rate, 1.0);

        let reset = parse_metrics("ishikari_external_egress_bytes_total 99\n").unwrap();
        let error = counter_delta(&before, &reset, "ishikari_external_egress_bytes_total", &[])
            .expect_err("counter reset must fail");
        assert!(error.to_string().contains("counter reset"));
    }

    #[test]
    fn prometheus_delta_rejects_targets_without_exact_negative_hit_metric() {
        let snapshot = parse_metrics("ishikari_external_egress_bytes_total 0\n").unwrap();
        let error = ComparableMetrics::from_delta(&snapshot, &snapshot)
            .expect_err("legacy target must not silently report zero negative hits");
        assert!(
            error
                .to_string()
                .contains("required counter is unsupported")
        );
    }

    #[test]
    fn prometheus_delta_rejects_counter_aggregate_overflow() {
        let before = parse_metrics(
            "ishikari_peer_fetch_total{resource=\"tile\"} 0\nishikari_peer_fetch_total{resource=\"provider\"} 0\n",
        )
        .unwrap();
        // Both values are exactly representable f64 integers and individually
        // fit in u64, but their family aggregate does not.
        let after = parse_metrics(
            "ishikari_peer_fetch_total{resource=\"tile\"} 9223372036854775808\nishikari_peer_fetch_total{resource=\"provider\"} 9223372036854775808\n",
        )
        .unwrap();
        let error = sum_counter_family_delta(&before, &after, "ishikari_peer_fetch_total")
            .expect_err("overflowing family total must fail");
        assert!(error.to_string().contains("aggregate overflow"));

        let out_of_range =
            parse_metrics("ishikari_peer_fetch_total{resource=\"tile\"} 18446744073709551616\n")
                .unwrap();
        let error = sum_counter_family_delta(
            &MetricSnapshot::default(),
            &out_of_range,
            "ishikari_peer_fetch_total",
        )
        .expect_err("out-of-range individual counter must fail");
        assert!(error.to_string().contains("outside the u64 range"));
    }

    #[tokio::test]
    async fn metrics_scrape_rejects_oversized_responses() {
        let router = Router::new().route("/metrics", get(|| async { "x".repeat(17) }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        let url = Url::parse(&format!("http://{address}/metrics")).unwrap();
        let error = scrape_metrics_with_limit(&Client::new(), &[url], 16)
            .await
            .expect_err("oversized metrics body must fail");
        assert!(error.to_string().contains("read metrics body"));
        assert!(format!("{error:#}").contains("exceeding 16"));
    }

    #[tokio::test]
    async fn gateway_replay_executes_a_trace_and_reports_http_results() {
        let requests = Arc::new(AtomicUsize::new(0));
        let router = Router::new()
            .route(
                "/tilesets/japan/0/0/0",
                get({
                    let requests = Arc::clone(&requests);
                    move || {
                        let requests = Arc::clone(&requests);
                        async move {
                            requests.fetch_add(1, Ordering::Relaxed);
                            "tile"
                        }
                    }
                }),
            )
            .route(
                "/metrics",
                get({
                    let requests = Arc::clone(&requests);
                    move || {
                        let requests = Arc::clone(&requests);
                        async move {
                            let count = requests.load(Ordering::Relaxed);
                            format!(
                                "ishikari_tiles_served_total{{source=\"self_cache\"}} {count}\nishikari_tile_negative_cache_hits_total 0\nishikari_external_egress_bytes_total {}\n",
                                count * 4
                            )
                        }
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });

        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let trace_path = std::env::temp_dir().join(format!(
            "ishikari-http-replay-{}-{suffix}.jsonl",
            std::process::id()
        ));
        let trace = [entry(0, Some(99)), entry(1, None)]
            .into_iter()
            .map(|entry| serde_json::to_string(&entry).unwrap())
            .collect::<Vec<_>>()
            .join("\n");
        std::fs::write(&trace_path, format!("{trace}\n")).unwrap();

        let report = run_http_replay(HttpReplayConfig {
            trace_path: trace_path.clone(),
            target: HttpReplayTarget::Gateway {
                gateway_url: Url::parse(&format!("http://{address}/")).unwrap(),
            },
            mode: HttpExecutionMode::ViewportBatches,
            metrics_urls: vec![Url::parse(&format!("http://{address}/metrics")).unwrap()],
            request_timeout: Duration::from_secs(5),
        })
        .await
        .expect("HTTP replay");

        assert!(report.is_success());
        let serialized = serde_json::to_value(&report).expect("serialize HTTP report");
        assert_eq!(serialized["schema_version"], 2);
        assert_eq!(
            serialized["execution"]["max_metrics_body_bytes"],
            MAX_METRICS_BODY_BYTES
        );
        assert_eq!(report.result.responses, 2);
        assert_eq!(report.result.status_counts.get(&200), Some(&2));
        assert_eq!(requests.load(Ordering::Relaxed), 2);
        let PrometheusCapture::Complete { nodes, aggregate } = report.prometheus else {
            panic!("expected complete Prometheus capture");
        };
        assert_eq!(nodes.len(), 1);
        assert_eq!(aggregate.requests, 2);
        assert_eq!(aggregate.served_bytes, 8);
        assert_eq!(aggregate.cache_hit_rate, 1.0);
        let _ = std::fs::remove_file(trace_path);
    }
}

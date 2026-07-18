//! Object-store fetch implementation for chunked reads.

use std::{
    collections::HashSet,
    ops::Range,
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use object_store::{
    Error as ObjectStoreError, ObjectStore, ObjectStoreExt, path::Path as ObjectPath,
};
use thiserror::Error;
use tokio::sync::Semaphore;
use tracing::debug;
use url::Url;

use crate::{interned::TilesetId, metrics::NodeMetrics, storage::ObjectStoreRegistry};

const BACKEND_FETCH_TIMEOUT: Duration = Duration::from_secs(10);
const MIB_BYTES: f64 = (1024 * 1024) as f64;

/// Deterministic latency injected before an object-store range fetch.
///
/// Production uses [`Self::fixed`]. The simulator can use [`Self::lognormal`]
/// to replay an empirical time-to-first-byte distribution plus a transfer-time
/// term proportional to the requested range size.
#[derive(Clone, Copy, Debug)]
pub struct BackendLatencyModel {
    median_ms: f64,
    lognormal_sigma: f64,
    transfer_ms_per_mib: f64,
    seed: u64,
}

impl BackendLatencyModel {
    pub fn fixed(delay_ms: u64) -> Self {
        Self {
            median_ms: delay_ms as f64,
            lognormal_sigma: 0.0,
            transfer_ms_per_mib: 0.0,
            seed: 0,
        }
    }

    #[cfg_attr(not(any(test, feature = "simulator-support")), allow(dead_code))]
    pub fn lognormal(
        median_ms: f64,
        lognormal_sigma: f64,
        transfer_ms_per_mib: f64,
        seed: u64,
    ) -> Result<Self> {
        if !median_ms.is_finite() || median_ms < 0.0 {
            bail!("backend latency median must be finite and non-negative");
        }
        if !lognormal_sigma.is_finite() || lognormal_sigma < 0.0 {
            bail!("backend latency sigma must be finite and non-negative");
        }
        if !transfer_ms_per_mib.is_finite() || transfer_ms_per_mib < 0.0 {
            bail!("backend transfer latency must be finite and non-negative");
        }
        Ok(Self {
            median_ms,
            lognormal_sigma,
            transfer_ms_per_mib,
            seed,
        })
    }

    fn delay(self, sequence: u64, range_start: u64, range_bytes: u64) -> Duration {
        let base_ms = if self.lognormal_sigma == 0.0 {
            self.median_ms
        } else {
            let first = uniform_open(splitmix64(
                self.seed ^ sequence.rotate_left(17) ^ range_start.rotate_left(31),
            ));
            let second = uniform_open(splitmix64(
                self.seed.rotate_left(29) ^ sequence ^ range_start.rotate_left(43),
            ));
            let standard_normal =
                (-2.0 * first.ln()).sqrt() * (std::f64::consts::TAU * second).cos();
            self.median_ms * (self.lognormal_sigma * standard_normal).exp()
        };
        let transfer_ms = range_bytes as f64 / MIB_BYTES * self.transfer_ms_per_mib;
        Duration::from_secs_f64(((base_ms + transfer_ms).max(0.0) / 1_000.0).min(86_400.0))
    }
}

fn uniform_open(value: u64) -> f64 {
    ((value >> 11) as f64 + 0.5) / (1_u64 << 53) as f64
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

/// Errors produced while fetching raw backend chunks.
#[derive(Clone, Debug, Error)]
pub enum ChunkFetchError {
    #[error("object not found")]
    NotFound,
    /// The backend read exceeded `PROVIDER_FETCH_TIMEOUT` / the range-read
    /// timeout. Kept as a typed variant so callers classify timeouts without
    /// matching on the message string.
    #[error("{0}")]
    Timeout(String),
    /// The process-wide admitted-fetch ceiling is full, so this request is shed
    /// rather than queued unboundedly. Typed so the service layer maps it to a
    /// 503 without matching on the message string.
    #[error("{0}")]
    Overload(String),
    #[error("{0}")]
    Message(String),
}

/// One object-store root that backs some set of tilesets.
struct TilesetSource {
    object_store: Arc<dyn ObjectStore>,
    base_path: ObjectPath,
}

/// Resolves a tileset key to the object-store root that backs it.
///
/// `TILESET_SOURCES` accepts the same `namespace=url;…;default=url` form as the style
/// and sprite templates (a bare URL is the default root). A namespaced tileset
/// key whose first segment matches a configured namespace is served from that
/// root with the namespace stripped (`regional/streets` → `{root}/streets.pmtiles`);
/// any other key falls to the default root with its full path preserved
/// (`analysis/hrnowc` → `{default}/analysis/hrnowc.pmtiles`).
#[derive(Clone)]
struct TilesetSources {
    namespaces: Vec<(String, Arc<TilesetSource>)>,
    default: Option<Arc<TilesetSource>>,
}

impl TilesetSources {
    fn parse(spec: &str, registry: &ObjectStoreRegistry) -> Result<Self> {
        let mut namespaces = Vec::new();
        let mut default = None;
        for (namespace, url) in parse_source_entries(spec)? {
            let source = Arc::new(build_source(&url, registry)?);
            match namespace {
                None => default = Some(source),
                Some(name) => namespaces.push((name, source)),
            }
        }
        Ok(Self {
            namespaces,
            default,
        })
    }

    /// Returns the backing store and object path for a tileset key, or `None`
    /// when no namespace matches and no default root is configured.
    fn resolve(&self, tileset_id: &str) -> Option<(Arc<dyn ObjectStore>, ObjectPath)> {
        let names: Vec<&str> = self
            .namespaces
            .iter()
            .map(|(name, _)| name.as_str())
            .collect();
        let (selected, relative) = select_namespace(tileset_id, &names);
        let source = match selected {
            Some(index) => &self.namespaces[index].1,
            None => self.default.as_ref()?,
        };
        Some((
            source.object_store.clone(),
            object_path_under(&source.base_path, relative),
        ))
    }
}

#[derive(Clone)]
pub struct ChunkFetcher {
    sources: TilesetSources,
    chunk_size: u64,
    backend_latency: BackendLatencyModel,
    backend_fetch_permits: Arc<Semaphore>,
    fetch_sequence: Arc<AtomicU64>,
    received_bytes: Arc<AtomicU64>,
    metrics: NodeMetrics,
}

impl ChunkFetcher {
    pub fn new(
        tileset_sources: String,
        chunk_size: u64,
        backend_fetch_concurrency: usize,
        backend_latency: BackendLatencyModel,
        registry: &ObjectStoreRegistry,
        metrics: NodeMetrics,
    ) -> Result<Self> {
        let sources = TilesetSources::parse(&tileset_sources, registry)?;
        let backend_fetch_concurrency = backend_fetch_concurrency.max(1);
        metrics.set_backend_fetch_concurrency_limit(backend_fetch_concurrency);
        Ok(Self {
            sources,
            chunk_size,
            backend_latency,
            backend_fetch_permits: Arc::new(Semaphore::new(backend_fetch_concurrency)),
            fetch_sequence: Arc::new(AtomicU64::new(0)),
            received_bytes: Arc::new(AtomicU64::new(0)),
            metrics,
        })
    }

    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    pub fn received_bytes(&self) -> u64 {
        self.received_bytes.load(Ordering::Relaxed)
    }

    pub async fn fetch_chunk_group(
        &self,
        tileset_id: &TilesetId,
        chunk_range: Range<u64>,
        archive_len: u64,
    ) -> std::result::Result<Bytes, ChunkFetchError> {
        if chunk_range.start >= chunk_range.end {
            return Ok(Bytes::new());
        }

        let (object_store, path) = self.sources.resolve(tileset_id.as_str()).ok_or_else(|| {
            ChunkFetchError::Message(format!(
                "no data source configured for tileset {tileset_id}"
            ))
        })?;
        let start_chunk = chunk_range.start;
        let end_chunk = chunk_range.end;
        // Chunk indices derive from PMTiles offsets read off the backend; guard the
        // span math so a corrupt archive yields a clean error rather than a wrapped
        // (and possibly reversed) range.
        let (Some(range_start), Some(range_end)) = (
            start_chunk.checked_mul(self.chunk_size),
            end_chunk.checked_mul(self.chunk_size),
        ) else {
            return Err(ChunkFetchError::Message(format!(
                "chunk range overflow: start_chunk={start_chunk} end_chunk={end_chunk} chunk_size={}",
                self.chunk_size
            )));
        };
        let range_end = range_end.min(archive_len);
        if range_start > range_end {
            return Err(ChunkFetchError::Message(format!(
                "chunk range start {range_start} exceeds end {range_end} (archive_len={archive_len})"
            )));
        }
        let prefetched_chunks = end_chunk - start_chunk;
        let prefetched_bytes = range_end - range_start;
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk,
            prefetched_chunks = prefetched_chunks,
            prefetched_bytes = prefetched_bytes,
            "fetching backend chunks"
        );

        // The per-tileset coordinator bounds one archive, while this semaphore
        // bounds the process as a whole. Distinct missing ids therefore cannot
        // multiply object-store concurrency without limit. The metric guard is
        // cancellation-safe while a request waits for admission.
        let queue_started_at = tokio::time::Instant::now();
        let waiting = BackendFetchGaugeGuard::new(self.metrics.clone(), "waiting");
        let permit = self
            .backend_fetch_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| ChunkFetchError::Message("backend fetch admission closed".into()))?;
        drop(waiting);
        self.metrics
            .record_backend_fetch_queue(queue_started_at.elapsed());
        let _active = BackendFetchGaugeGuard::new(self.metrics.clone(), "active");
        let _permit = permit;

        // Tokio's clock is wall-clock backed in production and virtual under
        // the simulator's paused-time runtime, so this metric remains useful in
        // both environments. Queue time is recorded separately above.
        let fetch_started_at = tokio::time::Instant::now();

        let sequence = self.fetch_sequence.fetch_add(1, Ordering::Relaxed);
        let backend_delay = self
            .backend_latency
            .delay(sequence, range_start, prefetched_bytes);
        // The deadline covers injected/observed latency and the object-store
        // operation together. Keeping the artificial delay outside this timeout
        // allowed detached scheduler tasks to retain backend permits and request
        // admission long after their callers had reached the outer deadline.
        let fetch_result = tokio::time::timeout(BACKEND_FETCH_TIMEOUT, async {
            if !backend_delay.is_zero() {
                tokio::time::sleep(backend_delay).await;
            }
            object_store.get_range(&path, range_start..range_end).await
        })
        .await;
        let bytes = match fetch_result {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                let outcome = backend_fetch_outcome(&error);
                self.metrics.record_backend_fetch(
                    outcome,
                    fetch_started_at.elapsed(),
                    prefetched_chunks,
                    prefetched_bytes,
                );
                return Err(ChunkFetchError::from(error));
            }
            Err(error) => {
                self.metrics.record_backend_fetch(
                    "timeout",
                    fetch_started_at.elapsed(),
                    prefetched_chunks,
                    prefetched_bytes,
                );
                return Err(ChunkFetchError::Timeout(format!(
                    "timed out fetching chunk range from object store: path={path} range={range_start}..{range_end}: {error}"
                )));
            }
        };
        let expected_len = (range_end - range_start) as usize;
        if bytes.len() != expected_len {
            self.metrics.record_backend_fetch(
                "error",
                fetch_started_at.elapsed(),
                prefetched_chunks,
                prefetched_bytes,
            );
            return Err(ChunkFetchError::Message(format!(
                "short range read from object store: path={path} range={range_start}..{range_end} expected_bytes={expected_len} actual_bytes={}",
                bytes.len()
            )));
        }
        self.received_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.metrics.record_backend_fetch(
            "success",
            fetch_started_at.elapsed(),
            prefetched_chunks,
            bytes.len() as u64,
        );
        debug!(
            tileset_id = %tileset_id,
            start_chunk = start_chunk,
            end_chunk = end_chunk - 1,
            backend_fetched_bytes = bytes.len(),
            duration_ms = fetch_started_at.elapsed().as_millis() as u64,
            "fetched backend chunk bytes"
        );

        Ok(bytes)
    }
}

struct BackendFetchGaugeGuard {
    metrics: NodeMetrics,
    state: &'static str,
}

impl BackendFetchGaugeGuard {
    fn new(metrics: NodeMetrics, state: &'static str) -> Self {
        metrics.adjust_backend_fetch_concurrency(state, 1);
        Self { metrics, state }
    }
}

impl Drop for BackendFetchGaugeGuard {
    fn drop(&mut self) {
        self.metrics
            .adjust_backend_fetch_concurrency(self.state, -1);
    }
}

fn backend_fetch_outcome(error: &ObjectStoreError) -> &'static str {
    if matches!(error, ObjectStoreError::NotFound { .. }) {
        "not_found"
    } else {
        "error"
    }
}

impl From<ObjectStoreError> for ChunkFetchError {
    fn from(error: ObjectStoreError) -> Self {
        if matches!(error, ObjectStoreError::NotFound { .. }) {
            return Self::NotFound;
        }
        // Do not retain the source: object-store errors can contain generated
        // signed request URLs, which would later be emitted by tileset logs.
        Self::Message("failed to fetch chunk range from object store".to_string())
    }
}

/// Parses a `TILESET_SOURCES` spec into `(namespace, url)` entries without building any
/// object stores. `None` namespace is the default root.
fn parse_source_entries(spec: &str) -> Result<Vec<(Option<String>, String)>> {
    let mut entries = Vec::new();
    let mut seen_default = false;
    let mut seen_namespaces = HashSet::new();

    for entry in spec.split(';') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (namespace, url) = match entry.split_once('=') {
            Some((key, value)) if is_namespace_key(key.trim()) => {
                let key = key.trim();
                if key == "default" {
                    (None, value.trim())
                } else {
                    (Some(key.to_string()), value.trim())
                }
            }
            // A bare URL (or anything with no namespace key before `=`, such as a
            // URL with a query string) is the default root.
            _ => (None, entry),
        };
        match &namespace {
            None => {
                if seen_default {
                    bail!("TILESET_SOURCES has multiple default sources");
                }
                seen_default = true;
            }
            Some(name) => {
                if !seen_namespaces.insert(name.clone()) {
                    bail!("TILESET_SOURCES has duplicate namespace {name:?}");
                }
            }
        }
        entries.push((namespace, url.to_string()));
    }

    if entries.is_empty() {
        bail!("TILESET_SOURCES must define at least one source");
    }
    Ok(entries)
}

/// Selects the namespace source for a tileset key. Returns the matched namespace
/// index (or `None` for the default root) and the key relative to that root: the
/// namespace is stripped on a match, otherwise the whole key is used.
fn select_namespace<'a>(tileset_id: &'a str, namespace_names: &[&str]) -> (Option<usize>, &'a str) {
    if let Some((namespace, rest)) = tileset_id.split_once('/')
        && let Some(index) = namespace_names.iter().position(|name| *name == namespace)
    {
        return (Some(index), rest);
    }
    (None, tileset_id)
}

/// Builds an object path under `base` from a `/`-delimited key, appending the
/// `.pmtiles` extension to the final segment. object_store's `Path::join`
/// encodes its whole argument as one segment, so each segment is joined
/// separately.
fn object_path_under(base: &ObjectPath, relative_key: &str) -> ObjectPath {
    let mut path = base.clone();
    let mut parts = relative_key.split('/').peekable();
    while let Some(part) = parts.next() {
        if parts.peek().is_some() {
            path = path.join(part);
        } else {
            path = path.join(format!("{part}.pmtiles"));
        }
    }
    path
}

fn build_source(source_url: &str, registry: &ObjectStoreRegistry) -> Result<TilesetSource> {
    let url = normalize_source_url(source_url)?;
    // The registry dedups stores by bucket/host, so multiple namespaces (or the
    // provider layer) backed by the same bucket share one store and pool.
    let (object_store, base_path) = registry.resolve(&url)?;
    Ok(TilesetSource {
        object_store,
        base_path,
    })
}

fn is_namespace_key(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn normalize_source_url(source_url: &str) -> Result<Url> {
    if let Ok(url) = Url::parse(source_url) {
        return Ok(url);
    }

    let path = std::fs::canonicalize(PathBuf::from(source_url))
        .with_context(|| format!("failed to resolve local data path {source_url}"))?;
    Url::from_directory_path(path)
        .map_err(|_| anyhow!("failed to convert local path to file:// URL"))
}

/// Resolves the exact local PMTiles archives used by a set of tileset ids.
///
/// This intentionally shares production's source parser and namespace
/// selection instead of scanning directories. Simulator output guards can
/// therefore protect nested archives, symlinked paths, and percent-encoded
/// `file://` roots exactly as the production reader resolves them. Remote
/// sources contribute no local path.
#[cfg(feature = "simulator-support")]
pub fn local_tileset_archive_paths<'a>(
    spec: &str,
    tileset_ids: impl IntoIterator<Item = &'a str>,
) -> Result<Vec<PathBuf>> {
    let mut namespaces = Vec::new();
    let mut default = None;
    for (namespace, source) in parse_source_entries(spec)? {
        let local_root = local_source_root(&source)?;
        match namespace {
            Some(namespace) => namespaces.push((namespace, local_root)),
            None => default = Some(local_root),
        }
    }

    let namespace_names = namespaces
        .iter()
        .map(|(namespace, _)| namespace.as_str())
        .collect::<Vec<_>>();
    let mut seen = HashSet::new();
    let mut archives = Vec::new();
    for tileset_id in tileset_ids {
        let tileset_id = TilesetId::try_new(tileset_id)
            .with_context(|| format!("invalid tileset id {tileset_id:?}"))?;
        let (selected, relative) = select_namespace(tileset_id.as_str(), &namespace_names);
        let root = match selected {
            Some(index) => namespaces[index].1.as_ref(),
            None => default.as_ref().and_then(Option::as_ref),
        };
        let Some(root) = root else {
            continue;
        };
        let archive = root.join(format!("{relative}.pmtiles"));
        if seen.insert(archive.clone()) {
            archives.push(archive);
        }
    }
    Ok(archives)
}

#[cfg(feature = "simulator-support")]
fn local_source_root(source: &str) -> Result<Option<PathBuf>> {
    let mut url = normalize_source_url(source)?;
    if url.scheme() != "file" {
        return Ok(None);
    }
    // Object-store selection depends on the URL path; query/fragment metadata
    // does not name a different local filesystem directory.
    url.set_query(None);
    url.set_fragment(None);
    url.to_file_path()
        .map(Some)
        .map_err(|_| anyhow!("invalid local source URL"))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    #[cfg(feature = "simulator-support")]
    use super::local_tileset_archive_paths;
    use super::{
        BACKEND_FETCH_TIMEOUT, BackendLatencyModel, ChunkFetchError, ChunkFetcher,
        object_path_under, parse_source_entries, select_namespace,
    };
    use crate::{interned::TilesetId, metrics::NodeMetrics, storage::ObjectStoreRegistry};
    use object_store::{Error as ObjectStoreError, path::Path as ObjectPath};
    #[cfg(feature = "simulator-support")]
    use url::Url;

    #[test]
    fn chunk_fetch_errors_never_retain_object_store_sources() {
        let secret = "SECRETTOKEN";
        let source = std::io::Error::other(format!(
            "GET https://bucket.example/archive?X-Signature={secret}"
        ));
        let error = ChunkFetchError::from(ObjectStoreError::Generic {
            store: "synthetic",
            source: Box::new(source),
        });
        let message = error.to_string();

        assert_eq!(message, "failed to fetch chunk range from object store");
        assert!(!message.contains(secret), "{message}");
        assert!(!message.contains("bucket.example"), "{message}");
    }

    #[test]
    fn parses_default_and_namespaced_entries() {
        let entries = parse_source_entries("carto=gs://a;regional=gs://b;default=gs://c").unwrap();
        assert_eq!(
            entries,
            vec![
                (Some("carto".to_string()), "gs://a".to_string()),
                (Some("regional".to_string()), "gs://b".to_string()),
                (None, "gs://c".to_string()),
            ]
        );
    }

    #[test]
    fn bare_url_is_the_default_source() {
        assert_eq!(
            parse_source_entries("gs://bucket/prefix").unwrap(),
            vec![(None, "gs://bucket/prefix".to_string())]
        );
        // A URL with a query string has no namespace key before `=`.
        assert_eq!(
            parse_source_entries("https://h/p?t=1").unwrap(),
            vec![(None, "https://h/p?t=1".to_string())]
        );
    }

    #[test]
    fn rejects_duplicate_and_empty_specs() {
        assert!(parse_source_entries("carto=gs://a;carto=gs://b").is_err());
        assert!(parse_source_entries("gs://a;default=gs://b").is_err());
        assert!(parse_source_entries("   ").is_err());
    }

    #[test]
    fn namespace_match_strips_prefix_else_default_keeps_whole_key() {
        let names = ["carto", "regional"];
        assert_eq!(
            select_namespace("carto/voyager", &names),
            (Some(0), "voyager")
        );
        assert_eq!(
            select_namespace("regional/streets", &names),
            (Some(1), "streets")
        );
        // No namespace match -> default root with the full key (nested path).
        assert_eq!(
            select_namespace("analysis/hrnowc", &names),
            (None, "analysis/hrnowc")
        );
        assert_eq!(select_namespace("japan", &names), (None, "japan"));
    }

    #[test]
    fn builds_nested_object_paths_with_extension() {
        let base = ObjectPath::from("prefix");
        assert_eq!(
            object_path_under(&base, "voyager").as_ref(),
            "prefix/voyager.pmtiles"
        );
        assert_eq!(
            object_path_under(&base, "analysis/hrnowc").as_ref(),
            "prefix/analysis/hrnowc.pmtiles"
        );
        assert_eq!(
            object_path_under(&ObjectPath::default(), "japan").as_ref(),
            "japan.pmtiles"
        );
    }

    #[cfg(feature = "simulator-support")]
    #[test]
    fn exact_local_archive_paths_share_production_routing_and_decode_file_urls() {
        let root = std::env::temp_dir().join(format!(
            "ishikari local sources {} {}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir_all(&root).expect("create local source root");
        let root_url = Url::from_directory_path(&root).expect("file URL");
        assert!(root_url.as_str().contains("%20"));

        let spec = format!("regional={root_url};default=gs://remote/default");
        let paths = local_tileset_archive_paths(
            &spec,
            ["regional/streets", "regional/streets", "other/nested"],
        )
        .expect("resolve exact local archives");
        assert_eq!(paths, [root.join("streets.pmtiles")]);

        let default_spec = root_url.to_string();
        let paths = local_tileset_archive_paths(&default_spec, ["demo/streets"])
            .expect("resolve nested default archive");
        assert_eq!(paths, [root.join("demo/streets.pmtiles")]);
        std::fs::remove_dir_all(root).expect("remove local source root");
    }

    #[test]
    fn fixed_backend_latency_is_constant() {
        let model = BackendLatencyModel::fixed(125);

        assert_eq!(model.delay(0, 0, 1), std::time::Duration::from_millis(125));
        assert_eq!(
            model.delay(99, 4_000_000, 4 * 1024 * 1024),
            std::time::Duration::from_millis(125)
        );
    }

    #[test]
    fn backend_latency_model_is_deterministic_and_size_aware() {
        let model = BackendLatencyModel::lognormal(55.0, 0.9, 6.0, 7).unwrap();

        let first = model.delay(11, 2_000_000, 1024 * 1024);
        assert_eq!(first, model.delay(11, 2_000_000, 1024 * 1024));
        assert_eq!(
            model.delay(11, 2_000_000, 2 * 1024 * 1024) - first,
            std::time::Duration::from_millis(6)
        );
    }

    #[test]
    fn backend_latency_distribution_matches_configured_shape() {
        let model = BackendLatencyModel::lognormal(55.0, 0.9, 0.0, 23).unwrap();
        let mut samples: Vec<_> = (0..10_001)
            .map(|sequence| model.delay(sequence, sequence * 1024, 1024 * 1024))
            .collect();
        samples.sort_unstable();
        let median_ms = samples[samples.len() / 2].as_secs_f64() * 1_000.0;
        let mean_ms = samples.iter().map(|value| value.as_secs_f64()).sum::<f64>() * 1_000.0
            / samples.len() as f64;

        assert!((52.0..58.0).contains(&median_ms), "median={median_ms}");
        assert!((78.0..88.0).contains(&mean_ms), "mean={mean_ms}");
    }

    #[tokio::test(start_paused = true)]
    async fn backend_duration_metric_follows_tokio_virtual_time() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-virtual-time-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("fixture.pmtiles"), b"abcdefgh").unwrap();

        let metrics = NodeMetrics::new();
        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            32,
            BackendLatencyModel::fixed(100),
            &ObjectStoreRegistry::new(),
            metrics.clone(),
        )
        .unwrap();
        let bytes = fetcher
            .fetch_chunk_group(&TilesetId::try_new("fixture").unwrap(), 0..1, 8)
            .await
            .unwrap();

        assert_eq!(bytes.as_ref(), b"abcd");
        let duration = metrics.histogram_snapshot().backend_fetch_duration_seconds;
        assert_eq!(duration.count, 1);
        assert!((duration.sum - 0.1).abs() < 1e-9, "sum={}", duration.sum);

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn backend_timeout_includes_artificial_latency_and_releases_permit() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-timeout-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("fixture.pmtiles"), b"abcdefgh").unwrap();

        let metrics = NodeMetrics::new();
        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            1,
            BackendLatencyModel::fixed(
                u64::try_from((BACKEND_FETCH_TIMEOUT + Duration::from_secs(1)).as_millis())
                    .unwrap(),
            ),
            &ObjectStoreRegistry::new(),
            metrics.clone(),
        )
        .unwrap();
        let task = tokio::spawn(async move {
            fetcher
                .fetch_chunk_group(&TilesetId::try_new("fixture").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;
        tokio::time::advance(BACKEND_FETCH_TIMEOUT).await;
        tokio::task::yield_now().await;

        assert!(matches!(
            task.await.unwrap(),
            Err(ChunkFetchError::Timeout(_))
        ));
        let encoded = metrics.encode();
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 0"));
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 0"));

        std::fs::remove_dir_all(directory).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn process_wide_limit_serializes_distinct_tilesets() {
        let suffix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let directory = std::env::temp_dir().join(format!(
            "ishikari-fetcher-concurrency-{}-{suffix}",
            std::process::id()
        ));
        std::fs::create_dir(&directory).unwrap();
        std::fs::write(directory.join("first.pmtiles"), b"abcdefgh").unwrap();
        std::fs::write(directory.join("second.pmtiles"), b"ijklmnop").unwrap();

        let metrics = NodeMetrics::new();
        let fetcher = ChunkFetcher::new(
            directory.to_string_lossy().into_owned(),
            4,
            1,
            BackendLatencyModel::fixed(100),
            &ObjectStoreRegistry::new(),
            metrics.clone(),
        )
        .unwrap();
        let first_fetcher = fetcher.clone();
        let first = tokio::spawn(async move {
            first_fetcher
                .fetch_chunk_group(&TilesetId::try_new("first").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;
        let second = tokio::spawn(async move {
            fetcher
                .fetch_chunk_group(&TilesetId::try_new("second").unwrap(), 0..1, 8)
                .await
        });
        tokio::task::yield_now().await;

        let saturated = metrics.encode();
        assert!(saturated.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 1"));
        assert!(saturated.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 1"));

        tokio::time::advance(Duration::from_millis(100)).await;
        tokio::task::yield_now().await;
        tokio::time::advance(Duration::from_millis(100)).await;
        assert_eq!(first.await.unwrap().unwrap().as_ref(), b"abcd");
        assert_eq!(second.await.unwrap().unwrap().as_ref(), b"ijkl");

        let encoded = metrics.encode();
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"active\"} 0"));
        assert!(encoded.contains("ishikari_backend_fetch_concurrency{state=\"waiting\"} 0"));
        let queue = metrics
            .histogram_snapshot()
            .backend_fetch_queue_duration_seconds;
        assert_eq!(queue.count, 2);
        assert!(queue.sum >= 0.1, "queue sum={}", queue.sum);

        std::fs::remove_dir_all(directory).unwrap();
    }
}

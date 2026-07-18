use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, ensure};
use serde::Serialize;
use tokio::{task::JoinSet, time::Instant};

use crate::{
    TraceEntry,
    cluster::{PreparedRequest, ServedRequest, SimCluster, execute_request, source_name},
    viewport_batch_ranges,
};

const MAX_TIMED_STEP_WORK_PER_USER: u64 = 1_000_000;
const MAX_TIMED_STEP_WORK_TOTAL: u64 = 10_000_000;

#[derive(Debug, Clone, Serialize)]
pub struct TimedConfig {
    pub think_time_ms: u64,
    pub think_jitter_ms: u64,
    pub request_overhead_ms: u64,
    pub request_timeout_ms: u64,
    pub seed: u64,
}

impl Default for TimedConfig {
    fn default() -> Self {
        Self {
            think_time_ms: 1_200,
            think_jitter_ms: 500,
            request_overhead_ms: 1,
            request_timeout_ms: 10_000,
            seed: 1,
        }
    }
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct LatencySummary {
    pub requests: usize,
    pub p50_ms: f64,
    pub p90_ms: f64,
    pub p95_ms: f64,
    pub p99_ms: f64,
    pub max_ms: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct TimedReport {
    pub config: TimedConfig,
    pub requests: usize,
    pub completed: usize,
    pub timed_out: usize,
    pub elapsed_ms: f64,
    pub throughput_rps: f64,
    pub latency: LatencySummary,
    pub latency_by_source: BTreeMap<String, LatencySummary>,
    pub node_peak_inflight: Vec<usize>,
}

struct PreparedBatch {
    step: u64,
    think_delay_before: Duration,
    requests: Vec<PreparedRequest>,
}

struct RequestRecord {
    latency: Duration,
    result: RequestResult,
}

enum RequestResult {
    Completed(ServedRequest),
    TimedOut,
}

struct UserResult {
    records: Vec<RequestRecord>,
    completed_at: Instant,
}

struct InflightTracker {
    current: Vec<AtomicUsize>,
    peak: Vec<AtomicUsize>,
}

impl InflightTracker {
    fn new(nodes: usize) -> Self {
        Self {
            current: (0..nodes).map(|_| AtomicUsize::new(0)).collect(),
            peak: (0..nodes).map(|_| AtomicUsize::new(0)).collect(),
        }
    }

    fn enter(&self, node: usize) {
        let current = self.current[node].fetch_add(1, Ordering::Relaxed) + 1;
        self.peak[node].fetch_max(current, Ordering::Relaxed);
    }

    fn leave(&self, node: usize) {
        self.current[node].fetch_sub(1, Ordering::Relaxed);
    }

    fn peaks(&self) -> Vec<usize> {
        self.peak
            .iter()
            .map(|value| value.load(Ordering::Relaxed))
            .collect()
    }
}

pub async fn run_timed_trace(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    config: TimedConfig,
) -> Result<TimedReport> {
    ensure!(
        config.request_timeout_ms > 0,
        "request timeout must be positive"
    );

    let batches = prepare_user_batches(cluster, entries, &config)?;
    let tracker = Arc::new(InflightTracker::new(cluster.node_count()));
    let started_at = Instant::now();
    let mut users = JoinSet::new();

    for batches in batches.into_values() {
        let tracker = tracker.clone();
        let config = config.clone();
        users.spawn(run_user(batches, config, tracker));
    }

    let mut records = Vec::with_capacity(entries.len());
    let mut completed_at = started_at;
    while let Some(result) = users.join_next().await {
        let user = result.context("timed user task failed")??;
        completed_at = completed_at.max(user.completed_at);
        records.extend(user.records);
    }

    let mut completed = 0;
    let mut timed_out = 0;
    let mut latencies = Vec::with_capacity(records.len());
    let mut by_source: BTreeMap<String, Vec<Duration>> = BTreeMap::new();
    for record in records {
        latencies.push(record.latency);
        match record.result {
            RequestResult::Completed(served) => {
                completed += 1;
                by_source
                    .entry(source_name(served.source).to_string())
                    .or_default()
                    .push(record.latency);
                cluster.record(served);
            }
            RequestResult::TimedOut => timed_out += 1,
        }
    }

    let elapsed = completed_at.saturating_duration_since(started_at);
    let throughput_rps = if elapsed.is_zero() {
        0.0
    } else {
        completed as f64 / elapsed.as_secs_f64()
    };
    let latency_by_source = by_source
        .into_iter()
        .map(|(source, values)| (source, summarize(values)))
        .collect();

    Ok(TimedReport {
        config,
        requests: entries.len(),
        completed,
        timed_out,
        elapsed_ms: duration_ms(elapsed),
        throughput_rps,
        latency: summarize(latencies),
        latency_by_source,
        node_peak_inflight: tracker.peaks(),
    })
}

fn prepare_user_batches(
    cluster: &SimCluster,
    entries: &[TraceEntry],
    config: &TimedConfig,
) -> Result<BTreeMap<usize, Vec<PreparedBatch>>> {
    let mut users: BTreeMap<usize, Vec<PreparedBatch>> = BTreeMap::new();
    for range in viewport_batch_ranges(entries)? {
        let batch = &entries[range];
        let first = batch.first().context("viewport batch is empty")?;
        let requests = batch
            .iter()
            .map(|entry| cluster.prepare(entry))
            .collect::<Result<Vec<_>>>()?;
        users.entry(first.user).or_default().push(PreparedBatch {
            step: first.step,
            think_delay_before: Duration::ZERO,
            requests,
        });
    }
    let mut global_step_work = 0_u64;
    for (&user, batches) in &mut users {
        batches.sort_by_key(|batch| batch.step);
        prepare_think_delays(user, batches, config, &mut global_step_work)?;
    }
    Ok(users)
}

fn prepare_think_delays(
    user: usize,
    batches: &mut [PreparedBatch],
    config: &TimedConfig,
    global_step_work: &mut u64,
) -> Result<()> {
    let mut previous_step = None;
    let mut user_step_work = 0_u64;
    for batch in batches {
        let Some(step) = previous_step else {
            previous_step = Some(batch.step);
            continue;
        };
        let transition_work = batch
            .step
            .checked_sub(step)
            .context("timed trace step transition underflow")?;
        user_step_work = user_step_work
            .checked_add(transition_work)
            .context("timed per-user step work overflow")?;
        ensure!(
            user_step_work <= MAX_TIMED_STEP_WORK_PER_USER,
            "timed trace user {user} expands to {user_step_work} step transitions, exceeding {MAX_TIMED_STEP_WORK_PER_USER}"
        );
        *global_step_work = global_step_work
            .checked_add(transition_work)
            .context("timed global step work overflow")?;
        ensure!(
            *global_step_work <= MAX_TIMED_STEP_WORK_TOTAL,
            "timed trace expands to {} step transitions, exceeding {MAX_TIMED_STEP_WORK_TOTAL}",
            *global_step_work
        );

        batch.think_delay_before =
            (step..batch.step).try_fold(Duration::ZERO, |total, iteration| {
                total
                    .checked_add(think_time(config, user, iteration))
                    .context("timed think duration overflow")
            })?;
        previous_step = Some(batch.step);
    }
    Ok(())
}

async fn run_user(
    batches: Vec<PreparedBatch>,
    config: TimedConfig,
    tracker: Arc<InflightTracker>,
) -> Result<UserResult> {
    let mut records = Vec::new();

    for batch in batches {
        if !batch.think_delay_before.is_zero() {
            tokio::time::sleep(batch.think_delay_before).await;
        }

        let arrived_at = Instant::now();
        let mut requests = JoinSet::new();
        for request in batch.requests {
            let tracker = tracker.clone();
            let overhead = Duration::from_millis(config.request_overhead_ms);
            let timeout = Duration::from_millis(config.request_timeout_ms);
            requests.spawn(async move {
                let node = request.node_index;
                tracker.enter(node);
                let result = tokio::time::timeout(timeout, async move {
                    if !overhead.is_zero() {
                        tokio::time::sleep(overhead).await;
                    }
                    execute_request(request).await
                })
                .await;
                tracker.leave(node);
                let result = match result {
                    Ok(served) => RequestResult::Completed(served?),
                    Err(_) => RequestResult::TimedOut,
                };
                Ok::<_, anyhow::Error>(RequestRecord {
                    latency: Instant::now().saturating_duration_since(arrived_at),
                    result,
                })
            });
        }
        while let Some(result) = requests.join_next().await {
            records.push(result.context("timed request task failed")??);
        }
    }

    Ok(UserResult {
        records,
        completed_at: Instant::now(),
    })
}

fn think_time(config: &TimedConfig, user: usize, iteration: u64) -> Duration {
    let unit = uniform_unit(splitmix64(
        config.seed ^ (user as u64).rotate_left(17) ^ iteration.rotate_left(31),
    ));
    let signed = unit * 2.0 - 1.0;
    let millis = config.think_time_ms as f64 + signed * config.think_jitter_ms as f64;
    Duration::from_secs_f64((millis.max(0.0)) / 1_000.0)
}

fn uniform_unit(value: u64) -> f64 {
    (value >> 11) as f64 * (1.0 / ((1_u64 << 53) as f64))
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn summarize(mut latencies: Vec<Duration>) -> LatencySummary {
    latencies.sort_unstable();
    LatencySummary {
        requests: latencies.len(),
        p50_ms: percentile(&latencies, 0.50),
        p90_ms: percentile(&latencies, 0.90),
        p95_ms: percentile(&latencies, 0.95),
        p99_ms: percentile(&latencies, 0.99),
        max_ms: latencies.last().copied().map_or(0.0, duration_ms),
    }
}

fn percentile(values: &[Duration], quantile: f64) -> f64 {
    if values.is_empty() {
        return 0.0;
    }
    let index = ((values.len() - 1) as f64 * quantile).round() as usize;
    duration_ms(values[index])
}

fn duration_ms(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{
        MAX_TIMED_STEP_WORK_PER_USER, MAX_TIMED_STEP_WORK_TOTAL, PreparedBatch, TimedConfig,
        percentile, prepare_think_delays, run_timed_trace, summarize, think_time,
    };
    use crate::{ClusterConfig, SimCluster, TraceEntry};

    #[test]
    fn latency_summary_uses_nearest_rank_positions() {
        let values = (1..=100).map(Duration::from_millis).collect();
        let summary = summarize(values);

        assert_eq!(summary.p50_ms, 51.0);
        assert_eq!(summary.p90_ms, 90.0);
        assert_eq!(summary.p99_ms, 99.0);
        assert_eq!(summary.max_ms, 100.0);
        assert_eq!(percentile(&[], 0.5), 0.0);
    }

    #[test]
    fn think_time_is_deterministic_and_bounded() {
        let config = TimedConfig::default();
        let first = think_time(&config, 7, 11);

        assert_eq!(first, think_time(&config, 7, 11));
        assert!(first >= Duration::from_millis(700));
        assert!(first <= Duration::from_millis(1_700));
    }

    #[test]
    fn cumulative_step_work_is_bounded_and_think_sleeps_are_aggregated() {
        let config = TimedConfig::default();
        let mut batches = vec![
            PreparedBatch {
                step: 10,
                think_delay_before: Duration::ZERO,
                requests: Vec::new(),
            },
            PreparedBatch {
                step: 13,
                think_delay_before: Duration::ZERO,
                requests: Vec::new(),
            },
        ];
        let expected = (10..13).fold(Duration::ZERO, |total, iteration| {
            total + think_time(&config, 7, iteration)
        });
        let mut global = 0;
        prepare_think_delays(7, &mut batches, &config, &mut global).expect("bounded work");
        assert_eq!(global, 3);
        assert_eq!(batches[1].think_delay_before, expected);

        batches[1].step = 10 + MAX_TIMED_STEP_WORK_PER_USER + 1;
        let error = prepare_think_delays(7, &mut batches, &config, &mut 0)
            .expect_err("per-user work must be bounded");
        assert!(error.to_string().contains("user 7"));

        batches[1].step = 11;
        let mut global = MAX_TIMED_STEP_WORK_TOTAL;
        let error = prepare_think_delays(7, &mut batches, &config, &mut global)
            .expect_err("global work must be bounded");
        assert!(error.to_string().contains("timed trace expands"));

        let mut global = u64::MAX;
        let error = prepare_think_delays(7, &mut batches, &config, &mut global)
            .expect_err("global work overflow must fail");
        assert!(error.to_string().contains("overflow"));
    }

    #[tokio::test(start_paused = true)]
    async fn timed_runner_starts_users_concurrently_and_enforces_timeout() {
        let mut cluster = SimCluster::new(ClusterConfig {
            node_count: 1,
            tileset_sources: env!("CARGO_MANIFEST_DIR").to_string(),
            ..ClusterConfig::default()
        })
        .await
        .expect("cluster");
        let entries: Vec<_> = (0..2)
            .map(|user| TraceEntry {
                step: 0,
                user,
                ordinal: 0,
                tileset: "missing".to_string(),
                z: 0,
                x: 0,
                y: 0,
                entry_node: Some(0),
            })
            .collect();

        let report = run_timed_trace(
            &mut cluster,
            &entries,
            TimedConfig {
                think_time_ms: 0,
                think_jitter_ms: 0,
                request_overhead_ms: 20,
                request_timeout_ms: 5,
                seed: 1,
            },
        )
        .await
        .expect("timed run");

        assert_eq!(report.requests, 2);
        assert_eq!(report.completed, 0);
        assert_eq!(report.timed_out, 2);
        assert_eq!(report.latency.p50_ms, 5.0);
        assert_eq!(report.node_peak_inflight, [2]);
    }
}

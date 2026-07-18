use std::{fs::File, io::BufReader, path::PathBuf, sync::Arc};

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use ishikari_sim::{
    AtomicOutputFile, BackendLatencyConfig, BackendLatencyProfile, ChurnConfig, ChurnPlan,
    ChurnReport, ClusterConfig, EntryAffinity, HttpExecutionMode, HttpReplayConfig,
    HttpReplayTarget, ModeledCluster, PopulationCdf, SimCluster, TileCatalog, TimedConfig,
    TimedReport, TraceEntry, Workload, WorkloadConfig, ensure_output_distinct,
    local_source_archives_for_tilesets, read_trace, run_churn_trace, run_http_replay,
    run_modeled_churn_trace, run_sweep, run_timed_trace, viewport_batch_ranges, write_atomic,
    write_trace_entry, write_visualization,
};
use reqwest::Url;
use serde::Serialize;

const REPORT_SCHEMA_VERSION: u32 = 2;

#[derive(Serialize)]
struct RunReport {
    schema_version: u32,
    execution_mode: &'static str,
    cache_mode: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    catalog_tiles: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timing: Option<TimedReport>,
    #[serde(skip_serializing_if = "Option::is_none")]
    backend_latency_profile: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    churn_plan: Option<PathBuf>,
    #[serde(skip_serializing_if = "Option::is_none")]
    churn: Option<ChurnReport>,
    trace: TraceSource,
    cluster: ClusterConfig,
    result: ishikari_sim::SimReport,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TraceSource {
    Generated {
        census: PathBuf,
        steps: u64,
        workload: WorkloadConfig,
        output: PathBuf,
    },
    Replay {
        input: PathBuf,
        requests: usize,
    },
}

#[derive(Parser)]
#[command(about = "Generate deterministic traces and run an in-process Ishikari cluster")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
    #[command(flatten)]
    simulation: SimulationArgs,
}

#[derive(Subcommand)]
enum Command {
    /// Generate a self-contained HTML visualization from a simulation report.
    Visualize(VisualizeArgs),
    /// Run a versioned, replay-only modeled-cache parameter sweep.
    Sweep(SweepArgs),
    /// Replay a trace through direct nodes or a Gateway and capture calibration metrics.
    ReplayHttp(ReplayHttpArgs),
}

#[derive(clap::Args)]
struct VisualizeArgs {
    /// Simulation report JSON generated with --report.
    input: PathBuf,
    /// Destination HTML path; defaults to the input path with an .html extension.
    #[arg(short, long)]
    output: Option<PathBuf>,
}

#[derive(clap::Args)]
struct SweepArgs {
    /// Versioned JSON sweep specification.
    spec: PathBuf,
    /// Destination JSONL file containing one self-contained document per run.
    #[arg(short, long)]
    output: PathBuf,
}

#[derive(clap::Args)]
struct ReplayHttpArgs {
    /// Existing simulator JSONL trace to replay.
    trace: PathBuf,
    /// Ordered direct-node public URL; entry_node indexes this repeated option.
    #[arg(long = "node-url", conflicts_with = "gateway_url")]
    node_urls: Vec<Url>,
    /// Single public Gateway URL; recorded entry_node values are ignored.
    #[arg(long, conflicts_with = "node_urls")]
    gateway_url: Option<Url>,
    /// Per-node internal Prometheus endpoint scraped before and after replay.
    #[arg(long = "metrics-url")]
    metrics_urls: Vec<Url>,
    /// Execute each viewport batch concurrently while preserving batch boundaries.
    #[arg(long)]
    viewport_batches: bool,
    /// Per-request timeout in milliseconds. Requests are never retried.
    #[arg(long, default_value_t = 30_000)]
    request_timeout_ms: u64,
    /// Destination versioned JSON report.
    #[arg(short, long)]
    output: PathBuf,
}

impl ReplayHttpArgs {
    fn into_config(self) -> Result<(HttpReplayConfig, PathBuf)> {
        let target = match (self.gateway_url, self.node_urls.is_empty()) {
            (Some(gateway_url), true) => HttpReplayTarget::Gateway { gateway_url },
            (None, false) => HttpReplayTarget::DirectNodes {
                node_urls: self.node_urls,
            },
            (Some(_), false) => bail!("--gateway-url conflicts with --node-url"),
            (None, true) => bail!("provide --gateway-url or at least one --node-url"),
        };
        ensure_output_distinct(&self.output, [self.trace.as_path()])
            .context("validate HTTP replay output path")?;
        Ok((
            HttpReplayConfig {
                trace_path: self.trace,
                target,
                mode: if self.viewport_batches {
                    HttpExecutionMode::ViewportBatches
                } else {
                    HttpExecutionMode::Serial
                },
                metrics_urls: self.metrics_urls,
                request_timeout: std::time::Duration::from_millis(self.request_timeout_ms),
            },
            self.output,
        ))
    }
}

#[derive(clap::Args)]
struct SimulationArgs {
    #[arg(
        long,
        default_value = "ishikari-sim/data/census_2020_1km_population.geojson"
    )]
    census: PathBuf,
    /// Write a generated JSONL trace to this path.
    #[arg(long)]
    output: Option<PathBuf>,
    /// Replay an existing JSONL trace instead of generating one.
    #[arg(long, conflicts_with = "output", requires = "simulate")]
    input_trace: Option<PathBuf>,
    #[arg(long, default_value = "mierune/omt")]
    tileset: String,
    #[arg(long, default_value_t = 50)]
    users: usize,
    #[arg(long, default_value_t = 1_000)]
    steps: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value_t = 4)]
    min_zoom: u8,
    #[arg(long, default_value_t = 15)]
    max_zoom: u8,
    #[arg(long, default_value_t = 13.0)]
    focus_zoom: f64,
    #[arg(long, default_value_t = 1.8)]
    zoom_sigma: f64,
    #[arg(long, default_value_t = 0.07)]
    session_reset_probability: f64,
    /// Per non-reset step, replace panning with a one-level zoom at the same center.
    #[arg(long, default_value_t = 0.0)]
    zoom_walk_probability: f64,
    #[arg(long, default_value_t = 1.0)]
    move_step_tiles: f64,
    #[arg(long, default_value_t = 3)]
    nodes: usize,
    #[arg(long, value_enum, default_value_t = AffinityArg::PerRequest)]
    entry_affinity: AffinityArg,
    /// Execute the generated or input trace through an in-process Ishikari cluster.
    #[arg(long)]
    simulate: bool,
    /// Use real payload caches or metadata-only logical-capacity caches.
    #[arg(long, value_enum, default_value_t = CacheModeArg::Real)]
    cache_mode: CacheModeArg,
    /// Poll each viewport's new tiles concurrently under paused Tokio time.
    #[arg(long, requires = "simulate")]
    viewport_batches: bool,
    /// Replay VUs concurrently with virtual think time and collect latency percentiles.
    #[arg(long, requires = "simulate", conflicts_with = "viewport_batches")]
    phase2: bool,
    /// Apply node add/remove events while replaying a trace.
    #[arg(
        long,
        requires_all = ["simulate", "input_trace"],
        conflicts_with = "phase2"
    )]
    churn_plan: Option<PathBuf>,
    /// Cumulative churn metrics sampling interval.
    #[arg(long, default_value_t = 1_000, requires = "churn_plan")]
    churn_sample_every_requests: u64,
    #[arg(long, default_value = "data")]
    tileset_sources: String,
    #[arg(long, default_value_t = 3)]
    candidate_count: usize,
    #[arg(long, default_value_t = 512)]
    tile_group_size: u64,
    #[arg(long, default_value_t = 1024 * 1024)]
    chunk_size_bytes: u64,
    #[arg(long, default_value_t = 4)]
    max_fetch_chunks: u64,
    /// Scheduler delay used by each real-cache node to merge nearby chunk fetches.
    /// Zero removes the intentional delay. Modeled-cache execution only records it.
    #[arg(long, default_value_t = 10)]
    chunk_fetch_merge_window_ms: u64,
    /// Process-wide object-storage range-fetch limit per simulated node.
    #[arg(long, default_value_t = 32)]
    backend_fetch_concurrency: usize,
    /// Fixed range-fetch delay, or the median when sigma is non-zero.
    #[arg(long, default_value_t = 0)]
    artificial_backend_delay_ms: u64,
    /// Lognormal sigma for the artificial range-fetch delay.
    #[arg(long, default_value_t = 0.0)]
    artificial_backend_delay_sigma: f64,
    /// Transfer-time slope added per MiB in each artificial range fetch.
    #[arg(long, default_value_t = 0.0)]
    artificial_backend_transfer_ms_per_mib: f64,
    /// Load an empirical object-storage latency model for Phase 2.
    #[arg(long, requires = "phase2")]
    backend_latency_profile: Option<PathBuf>,
    /// Simulated latency added to an in-process peer request.
    #[arg(long, default_value_t = 0)]
    peer_latency_ms: u64,
    /// Production chitchat gossip interval used by real-cache simulation.
    #[arg(long, default_value_t = 200)]
    gossip_interval_ms: u64,
    /// Virtual one-way latency added to each in-memory gossip message.
    #[arg(long, default_value_t = 1)]
    gossip_hop_latency_ms: u64,
    #[arg(long, default_value_t = 1_200)]
    think_time_ms: u64,
    #[arg(long, default_value_t = 500)]
    think_jitter_ms: u64,
    #[arg(long, default_value_t = 1)]
    request_overhead_ms: u64,
    #[arg(long, default_value_t = 10_000)]
    request_timeout_ms: u64,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    tile_cache_max_bytes: u64,
    /// Cache successful peer responses at the entry node, or only at the HRW owner.
    #[arg(long, value_enum, default_value_t = PeerTileCacheArg::Entry)]
    peer_tile_cache: PeerTileCacheArg,
    #[arg(long, default_value_t = 512 * 1024 * 1024)]
    chunk_cache_max_bytes: u64,
    #[arg(long, requires = "simulate")]
    report: Option<PathBuf>,
}

impl SimulationArgs {
    fn validate(&self) -> Result<()> {
        if self.output.is_none() && self.input_trace.is_none() {
            bail!("--output is required when generating a trace");
        }
        if self.cache_mode == CacheModeArg::Modeled && !self.simulate {
            bail!("--cache-mode modeled requires --simulate");
        }
        if self.phase2 && (self.cache_mode != CacheModeArg::Real || self.input_trace.is_none()) {
            bail!("--phase2 requires --cache-mode real and --input-trace");
        }
        if self.churn_plan.is_some() && self.churn_sample_every_requests == 0 {
            bail!("--churn-sample-every-requests must be greater than zero");
        }
        // A generated workload has one known tileset, so protect its exact
        // production-resolved local archive before publishing the trace.
        // Replay traces are handled after their entries have been parsed.
        let source_archives = if self.simulate && self.input_trace.is_none() {
            local_source_archives_for_tilesets(&self.tileset_sources, [self.tileset.as_str()])?
        } else {
            Vec::new()
        };
        if let Some(output) = &self.output {
            let mut protected = vec![self.census.as_path()];
            protected.extend(source_archives.iter().map(PathBuf::as_path));
            ensure_output_distinct(output, protected)
                .context("validate generated trace output path")?;
        }
        if let Some(report) = &self.report {
            let mut protected = Vec::new();
            if let Some(input_trace) = &self.input_trace {
                protected.push(input_trace.as_path());
            } else {
                protected.push(self.census.as_path());
            }
            if let Some(output) = &self.output {
                protected.push(output.as_path());
            }
            if let Some(churn_plan) = &self.churn_plan {
                protected.push(churn_plan.as_path());
            }
            if let Some(profile) = &self.backend_latency_profile {
                protected.push(profile.as_path());
            }
            protected.extend(source_archives.iter().map(PathBuf::as_path));
            ensure_output_distinct(report, protected)
                .context("validate simulation report output path")?;
        }
        Ok(())
    }

    fn validate_replay_archive_output(&self, entries: &[TraceEntry]) -> Result<()> {
        let Some(report) = &self.report else {
            return Ok(());
        };
        let archives = local_source_archives_for_tilesets(
            &self.tileset_sources,
            entries.iter().map(|entry| entry.tileset.as_str()),
        )?;
        ensure_output_distinct(report, archives.iter().map(PathBuf::as_path))
            .context("validate report against replay PMTiles inputs")
    }

    fn uses_paused_time(&self) -> bool {
        self.simulate && self.cache_mode == CacheModeArg::Real
    }

    fn cluster_config(&self) -> Result<ClusterConfig> {
        let profile = self
            .backend_latency_profile
            .as_ref()
            .map(|path| BackendLatencyProfile::from_path(path))
            .transpose()?;
        let backend_latency = match profile {
            Some(profile) => BackendLatencyConfig {
                seed: self.seed,
                ..profile.model
            },
            None => BackendLatencyConfig {
                median_ms: self.artificial_backend_delay_ms,
                lognormal_sigma: self.artificial_backend_delay_sigma,
                transfer_ms_per_mib: self.artificial_backend_transfer_ms_per_mib,
                seed: self.seed,
            },
        };
        Ok(ClusterConfig {
            node_count: self.nodes,
            tileset_sources: self.tileset_sources.clone(),
            candidate_count: self.candidate_count,
            tile_group_size: self.tile_group_size,
            chunk_size_bytes: self.chunk_size_bytes,
            max_fetch_chunks: self.max_fetch_chunks,
            chunk_fetch_merge_window_ms: self.chunk_fetch_merge_window_ms,
            backend_fetch_concurrency: self.backend_fetch_concurrency,
            backend_latency,
            peer_latency_ms: self.peer_latency_ms,
            gossip_interval_ms: self.gossip_interval_ms,
            gossip_hop_latency_ms: self.gossip_hop_latency_ms,
            tile_cache_max_bytes: self.tile_cache_max_bytes,
            chunk_cache_max_bytes: self.chunk_cache_max_bytes,
            cache_peer_tiles: self.peer_tile_cache == PeerTileCacheArg::Entry,
        })
    }

    fn timed_config(&self) -> TimedConfig {
        TimedConfig {
            think_time_ms: self.think_time_ms,
            think_jitter_ms: self.think_jitter_ms,
            request_overhead_ms: self.request_overhead_ms,
            request_timeout_ms: self.request_timeout_ms,
            seed: self.seed,
        }
    }

    fn churn_config(&self) -> ChurnConfig {
        ChurnConfig {
            seed: self.seed,
            entry_affinity: match self.entry_affinity {
                AffinityArg::PerRequest => EntryAffinity::PerRequest,
                AffinityArg::PerSession => EntryAffinity::PerSession,
            },
            sample_every_requests: self.churn_sample_every_requests,
        }
    }

    fn execution_mode(&self) -> &'static str {
        if self.phase2 {
            "phase2"
        } else if self.churn_plan.is_some() {
            "churn"
        } else if self.viewport_batches {
            "viewport_batches"
        } else {
            "serial"
        }
    }
}

#[derive(Clone, Copy, ValueEnum)]
enum AffinityArg {
    PerRequest,
    PerSession,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum CacheModeArg {
    Real,
    Modeled,
}

#[derive(Clone, Copy, Eq, PartialEq, ValueEnum)]
enum PeerTileCacheArg {
    Entry,
    OwnerOnly,
}

impl CacheModeArg {
    fn label(self) -> &'static str {
        match self {
            Self::Real => "real",
            Self::Modeled => "modeled",
        }
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    if let Some(command) = cli.command {
        match command {
            Command::Visualize(args) => {
                let output = args
                    .output
                    .unwrap_or_else(|| args.input.with_extension("html"));
                write_visualization(&args.input, &output)?;
                eprintln!("wrote visualization to {}", output.display());
            }
            Command::Sweep(args) => {
                run_sweep(&args.spec, &args.output).await?;
                eprintln!("wrote sweep results to {}", args.output.display());
            }
            Command::ReplayHttp(args) => {
                let (config, output) = args.into_config()?;
                let report = run_http_replay(config).await?;
                let successful = report.is_success();
                write_atomic(&output, |writer| {
                    serde_json::to_writer_pretty(writer, &report)
                        .context("serialize HTTP replay report")
                })
                .with_context(|| format!("write HTTP replay report {}", output.display()))?;
                eprintln!("wrote HTTP replay report to {}", output.display());
                if !successful {
                    bail!("HTTP replay completed with failures; inspect the report");
                }
            }
        }
        return Ok(());
    }
    let args = cli.simulation;
    args.validate()?;
    if args.uses_paused_time() {
        tokio::time::pause();
    }
    let cluster_config = args.cluster_config()?;
    let mut cluster = if args.simulate && args.cache_mode == CacheModeArg::Real {
        Some(SimCluster::new(cluster_config.clone()).await?)
    } else {
        None
    };
    let mut modeled_result = None;
    let mut catalog_tiles = None;
    let mut timing = None;
    let mut churn = None;
    let trace_source = if let Some(input) = &args.input_trace {
        let file =
            File::open(input).with_context(|| format!("open trace file {}", input.display()))?;
        let entries = read_trace(BufReader::new(file))?;
        args.validate_replay_archive_output(&entries)?;
        match args.cache_mode {
            CacheModeArg::Real => {
                let cluster = cluster.as_mut().expect("real simulation cluster");
                if args.phase2 {
                    timing = Some(run_timed_trace(cluster, &entries, args.timed_config()).await?);
                } else if let Some(path) = &args.churn_plan {
                    let plan = ChurnPlan::from_path(path)?;
                    churn = Some(
                        run_churn_trace(
                            cluster,
                            &entries,
                            args.viewport_batches,
                            &plan,
                            args.churn_config(),
                        )
                        .await?,
                    );
                } else {
                    replay_trace(cluster, &entries, args.viewport_batches).await?;
                }
            }
            CacheModeArg::Modeled => {
                let (result, tile_count, churn_report) =
                    run_modeled_simulation(&args, &cluster_config, &entries).await?;
                catalog_tiles = Some(tile_count);
                modeled_result = Some(result);
                churn = churn_report;
            }
        }
        eprintln!(
            "replayed {} requests from {}",
            entries.len(),
            input.display()
        );
        TraceSource::Replay {
            input: input.clone(),
            requests: entries.len(),
        }
    } else {
        let source = generate_trace(&args, cluster.as_mut()).await?;
        if args.cache_mode == CacheModeArg::Modeled {
            let output = args
                .output
                .as_ref()
                .expect("validated generated trace output");
            let file = File::open(output)
                .with_context(|| format!("open generated trace file {}", output.display()))?;
            let entries = read_trace(BufReader::new(file))?;
            let (result, tile_count, churn_report) =
                run_modeled_simulation(&args, &cluster_config, &entries).await?;
            catalog_tiles = Some(tile_count);
            modeled_result = Some(result);
            churn = churn_report;
            eprintln!("simulated {} generated requests", entries.len());
        }
        source
    };

    let result = modeled_result.or_else(|| cluster.map(SimCluster::report));
    if let Some(result) = result {
        let report = RunReport {
            schema_version: REPORT_SCHEMA_VERSION,
            execution_mode: args.execution_mode(),
            cache_mode: args.cache_mode.label(),
            catalog_tiles,
            timing,
            backend_latency_profile: args.backend_latency_profile,
            churn_plan: args.churn_plan,
            churn,
            trace: trace_source,
            cluster: cluster_config,
            result,
        };
        if let Some(path) = args.report {
            write_atomic(&path, |writer| {
                serde_json::to_writer_pretty(writer, &report).context("serialize simulation report")
            })
            .with_context(|| format!("write report file {}", path.display()))?;
        } else {
            eprintln!("{}", serde_json::to_string_pretty(&report)?);
        }
    }
    Ok(())
}

async fn run_modeled_simulation(
    args: &SimulationArgs,
    cluster_config: &ClusterConfig,
    entries: &[TraceEntry],
) -> Result<(ishikari_sim::SimReport, usize, Option<ChurnReport>)> {
    let catalog = TileCatalog::build(&args.tileset_sources, entries).await?;
    let tile_count = catalog.len();
    let mut modeled = ModeledCluster::new(cluster_config.clone(), catalog)?;
    let churn = if let Some(path) = &args.churn_plan {
        let plan = ChurnPlan::from_path(path)?;
        Some(run_modeled_churn_trace(
            &mut modeled,
            entries,
            args.viewport_batches,
            &plan,
            args.churn_config(),
        )?)
    } else {
        replay_modeled_trace(&mut modeled, entries, args.viewport_batches)?;
        None
    };
    Ok((modeled.report(), tile_count, churn))
}

async fn generate_trace(
    args: &SimulationArgs,
    mut cluster: Option<&mut SimCluster>,
) -> Result<TraceSource> {
    let census = File::open(&args.census)
        .with_context(|| format!("open census file {}", args.census.display()))?;
    let population = Arc::new(PopulationCdf::from_reader(BufReader::new(census))?);
    let workload_config = WorkloadConfig {
        tileset: args.tileset.clone(),
        users: args.users,
        seed: args.seed,
        min_zoom: args.min_zoom,
        max_zoom: args.max_zoom,
        focus_zoom: args.focus_zoom,
        zoom_sigma: args.zoom_sigma,
        session_reset_probability: args.session_reset_probability,
        zoom_walk_probability: args.zoom_walk_probability,
        move_step_tiles: args.move_step_tiles,
        node_count: args.nodes,
        entry_affinity: match args.entry_affinity {
            AffinityArg::PerRequest => EntryAffinity::PerRequest,
            AffinityArg::PerSession => EntryAffinity::PerSession,
        },
    };
    let output_path = args
        .output
        .as_ref()
        .context("--output is required when generating a trace")?;
    let mut output = AtomicOutputFile::create(output_path)
        .with_context(|| format!("create trace file {}", output_path.display()))?;
    let mut workload = Workload::new(workload_config.clone(), population.clone())?;
    let mut request_count = 0_u64;

    for step in 0..args.steps {
        for user in 0..workload_config.users {
            let entries = workload.step(step, user)?;
            if let Some(cluster) = cluster.as_deref_mut() {
                serve_entries(cluster, &entries, args.viewport_batches).await?;
            }
            for entry in &entries {
                write_trace_entry(output.writer(), entry)?;
                request_count += 1;
            }
        }
    }
    output.finish().context("publish trace")?;
    eprintln!(
        "wrote {request_count} requests from {} population points (weight {:.0}) to {}",
        population.point_count(),
        population.total_weight(),
        output_path.display()
    );

    Ok(TraceSource::Generated {
        census: args.census.clone(),
        steps: args.steps,
        workload: workload_config,
        output: output_path.clone(),
    })
}

async fn replay_trace(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            cluster.serve_viewport(&entries[range]).await?;
        }
    } else {
        for entry in entries {
            cluster.serve(entry).await?;
        }
    }
    Ok(())
}

async fn serve_entries(
    cluster: &mut SimCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        cluster.serve_viewport(entries).await
    } else {
        for entry in entries {
            cluster.serve(entry).await?;
        }
        Ok(())
    }
}

fn replay_modeled_trace(
    cluster: &mut ModeledCluster,
    entries: &[TraceEntry],
    viewport_batches: bool,
) -> Result<()> {
    if viewport_batches {
        for range in viewport_batch_ranges(entries)? {
            cluster.serve_viewport(&entries[range])?;
        }
    } else {
        for entry in entries {
            cluster.serve(entry)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normal_run_report_serializes_schema_v2() {
        let report = RunReport {
            schema_version: REPORT_SCHEMA_VERSION,
            execution_mode: "serial",
            cache_mode: "modeled",
            catalog_tiles: Some(0),
            timing: None,
            backend_latency_profile: None,
            churn_plan: None,
            churn: None,
            trace: TraceSource::Replay {
                input: PathBuf::from("trace.jsonl"),
                requests: 0,
            },
            cluster: ClusterConfig::default(),
            result: ishikari_sim::SimReport::default(),
        };

        let value = serde_json::to_value(report).expect("serialize normal report");
        assert_eq!(value["schema_version"], 2);
    }
}

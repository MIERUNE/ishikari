//! Deterministic workloads and in-process simulation for Ishikari.

mod churn;
mod cluster;
mod http_replay;
mod latency;
mod membership;
mod modeled;
mod output;
mod report;
mod sweep;
mod timed;
mod trace;
mod visualization;
mod workload;

pub use churn::{
    AppliedChurnEvent, ChurnConfig, ChurnPlan, ChurnReport, ChurnSample, run_churn_trace,
    run_modeled_churn_trace,
};
pub use cluster::{ClusterConfig, SimCluster};
pub use http_replay::{
    HttpExecutionMode, HttpReplayConfig, HttpReplayReport, HttpReplayTarget, run_http_replay,
};
pub use latency::{BackendLatencyConfig, BackendLatencyProfile};
pub use modeled::{ModeledCluster, TileCatalog};
#[doc(hidden)]
pub use output::{
    AtomicOutputFile, ensure_output_distinct, local_source_archives,
    local_source_archives_for_tilesets, write_atomic,
};
pub use report::{ClusterObservation, SimReport};
pub use sweep::run_sweep;
pub use timed::{LatencySummary, TimedConfig, TimedReport, run_timed_trace};
pub use trace::{read_trace, read_trace_with_digest, viewport_batch_ranges, write_trace_entry};
pub use visualization::{render_visualization, write_visualization};
pub use workload::{EntryAffinity, PopulationCdf, TraceEntry, Workload, WorkloadConfig};

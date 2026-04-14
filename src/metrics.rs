//! Temporary metrics module.

use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};

/// Cumulative byte counters for egress and internal traffic.
#[derive(Clone)]
pub struct NodeMetrics(Arc<Inner>);

struct Inner {
    egress_bytes: AtomicU64,
    internal_bytes: AtomicU64,
}

impl NodeMetrics {
    pub fn new() -> Self {
        Self(Arc::new(Inner {
            egress_bytes: AtomicU64::new(0),
            internal_bytes: AtomicU64::new(0),
        }))
    }

    pub fn add_egress_bytes(&self, bytes: u64) {
        self.0.egress_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn add_internal_bytes(&self, bytes: u64) {
        self.0.internal_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn egress_bytes(&self) -> u64 {
        self.0.egress_bytes.load(Ordering::Relaxed)
    }

    pub fn internal_bytes(&self) -> u64 {
        self.0.internal_bytes.load(Ordering::Relaxed)
    }
}

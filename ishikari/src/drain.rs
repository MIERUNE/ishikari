//! Local drain state for graceful shutdown.
//!
//! On shutdown the node flips this flag, publishes `draining` to membership, and
//! starts rejecting peer-forwarding requests with `503` so sibling nodes fail
//! over quickly, while in-flight requests are allowed to finish. Public /
//! load-balancer-facing requests keep being served until the LB pulls the pod.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Cloneable handle to the node's local drain flag.
#[derive(Clone, Default)]
pub struct DrainController(Arc<AtomicBool>);

impl DrainController {
    pub fn new() -> Self {
        Self::default()
    }

    /// Marks the node as draining. Idempotent.
    pub fn begin(&self) {
        self.0.store(true, Ordering::SeqCst);
    }

    /// Returns whether the node is draining.
    pub fn is_draining(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

/// Returns whether a request path is peer-to-peer forwarding *into* this node,
/// which is rejected with `503` the moment draining begins so sibling nodes fail
/// over to a healthy replica immediately.
///
/// Public / load-balancer-facing paths (`/tilesets`, `/styles`, `/fonts`) are
/// intentionally NOT drainable. GKE Gateway removes Terminating pods from the NEG
/// on its own schedule; rejecting public requests at SIGTERM would surface as
/// client-visible `503`s while the LB is still sending traffic. Operational
/// endpoints (`/_internal/healthz`, `/_internal/readyz`, `/_internal/metrics`,
/// `/_internal/cluster`) stay available throughout.
pub fn is_drainable_path(path: &str) -> bool {
    path.starts_with("/_internal/tiles/")
        || path.starts_with("/_internal/derived/")
        || path.starts_with("/_internal/pmtiles/")
        || path.starts_with("/_internal/provider/")
}

#[cfg(test)]
mod tests {
    use super::is_drainable_path;

    #[test]
    fn only_peer_forwarding_paths_drain() {
        // Peer-to-peer forwarding into this node: rejected on drain for fast failover.
        assert!(is_drainable_path("/_internal/tiles/demo/streets/0/0/0"));
        assert!(is_drainable_path(
            "/_internal/derived/mapterhorn/planet/hillshade/8/226/100"
        ));
        assert!(is_drainable_path(
            "/_internal/pmtiles/demo/streets/bootstrap"
        ));
        assert!(is_drainable_path(
            "/_internal/provider/styles/carto/voyager/style.json"
        ));
        assert!(is_drainable_path(
            "/_internal/provider/fonts/Noto%20Sans/0-255.pbf"
        ));
        // Public / LB-facing: keep serving until the LB pulls the pod.
        assert!(!is_drainable_path("/tilesets/demo/streets"));
        assert!(!is_drainable_path(
            "/styles/carto/voyager-gl-style/style.json"
        ));
        assert!(!is_drainable_path("/fonts/Open%20Sans%20Regular/0-255.pbf"));
        // Operational endpoints: always available.
        assert!(!is_drainable_path("/_internal/healthz"));
        assert!(!is_drainable_path("/_internal/readyz"));
        assert!(!is_drainable_path("/_internal/metrics"));
    }
}

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Default)]
pub struct IceCandidateTracker {
    host_count: AtomicUsize,
    srflx_count: AtomicUsize,
    prflx_count: AtomicUsize,
    relay_count: AtomicUsize,
}

impl IceCandidateTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_candidate(&self, candidate: &str) {
        if candidate.contains("typ host") {
            self.host_count.fetch_add(1, Ordering::Relaxed);
        } else if candidate.contains("typ srflx") {
            self.srflx_count.fetch_add(1, Ordering::Relaxed);
        } else if candidate.contains("typ prflx") {
            self.prflx_count.fetch_add(1, Ordering::Relaxed);
        } else if candidate.contains("typ relay") {
            self.relay_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn has_sufficient_candidates(&self) -> bool {
        let host = self.host_count.load(Ordering::Relaxed);
        let srflx = self.srflx_count.load(Ordering::Relaxed);
        let relay = self.relay_count.load(Ordering::Relaxed);
        (host >= 1 && srflx >= 1) || relay >= 1
    }

    pub fn summary(&self) -> String {
        format!(
            "host={}, srflx={}, prflx={}, relay={}",
            self.host_count.load(Ordering::Relaxed),
            self.srflx_count.load(Ordering::Relaxed),
            self.prflx_count.load(Ordering::Relaxed),
            self.relay_count.load(Ordering::Relaxed)
        )
    }

    pub fn has_relay(&self) -> bool {
        self.relay_count.load(Ordering::Relaxed) >= 1
    }
}

//! Observability counters (TSI-2139).
//!
//! Zero-cost, lock-free metrics used to establish a quantitative baseline
//! before the DDD refactor. All counters use `std::sync::atomic` so they
//! can be read from `.stats` without blocking on any of the download locks.
//!
//! This module is purely additive: it does not change any runtime behaviour,
//! only records what already happens.

use std::sync::atomic::{AtomicU64, Ordering};

/// Immutable snapshot of all counters, for `.stats` rendering.
#[derive(Debug, Clone, Default)]
pub struct MetricsSnapshot {
    // ── Three-tier cache hit/miss ─────────────────────────────────────
    /// L1 (memory) cache: whole-file reads served from `torrent_data_cache`.
    pub l1_hits: u64,
    pub l1_misses: u64,
    /// TSI-2274: number of entries currently in the L1 range cache.
    pub l1_entries: u64,
    /// L2 (disk piece) cache: reads whose pieces were all present on disk.
    pub l2_hits: u64,
    pub l2_misses: u64,
    /// L3 (metadata) cache: `TorrentInfo` parse hits/misses.
    pub l3_hits: u64,
    pub l3_misses: u64,
    // ── Deferred reads ────────────────────────────────────────────────
    pub deferred_reads: u64,
    // ── Pending reads table depth ─────────────────────────────────────
    pub pending_reads_current: u64,
    pub pending_reads_peak: u64,
    // ── Piece-wait poll hit rate ──────────────────────────────────────
    pub poll_checks: u64,
    pub poll_hits: u64,
    // ── Download queue depth ──────────────────────────────────────────
    pub download_queue_current: u64,
    pub download_queue_peak: u64,
    // ── Worker thread occupancy ───────────────────────────────────────
    pub workers_active: u64,
    pub workers_peak: u64,
    // ── Lock wait time ────────────────────────────────────────────────
    pub lock_acquires: u64,
    pub lock_wait_nanos: u64,
}

/// Shared, atomically-updated observability counters.
pub struct Metrics {
    // L1 memory cache
    l1_hits: AtomicU64,
    l1_misses: AtomicU64,
    /// TSI-2274: current L1 range cache entry count (gauge, not counter).
    l1_entries: AtomicU64,
    l2_hits: AtomicU64,
    l2_misses: AtomicU64,
    // L3 metadata cache
    l3_hits: AtomicU64,
    l3_misses: AtomicU64,
    // Deferred reads
    deferred_reads: AtomicU64,
    // Pending reads table depth
    pending_reads_current: AtomicU64,
    pending_reads_peak: AtomicU64,
    // Piece-wait poll hit rate
    poll_checks: AtomicU64,
    poll_hits: AtomicU64,
    // Download queue depth
    download_queue_current: AtomicU64,
    download_queue_peak: AtomicU64,
    // Worker thread occupancy
    workers_active: AtomicU64,
    workers_peak: AtomicU64,
    // Lock wait time
    lock_acquires: AtomicU64,
    lock_wait_nanos: AtomicU64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            l1_hits: AtomicU64::new(0),
            l1_misses: AtomicU64::new(0),
            l1_entries: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            l3_hits: AtomicU64::new(0),
            l3_misses: AtomicU64::new(0),
            deferred_reads: AtomicU64::new(0),
            pending_reads_current: AtomicU64::new(0),
            pending_reads_peak: AtomicU64::new(0),
            poll_checks: AtomicU64::new(0),
            poll_hits: AtomicU64::new(0),
            download_queue_current: AtomicU64::new(0),
            download_queue_peak: AtomicU64::new(0),
            workers_active: AtomicU64::new(0),
            workers_peak: AtomicU64::new(0),
            lock_acquires: AtomicU64::new(0),
            lock_wait_nanos: AtomicU64::new(0),
        }
    }

    // ── Three-tier cache ───────────────────────────────────────────────

    pub fn l1_hit(&self) {
        self.l1_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn l1_miss(&self) {
        self.l1_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// TSI-2274: set the current L1 range cache entry count (gauge).
    pub fn set_l1_entries(&self, n: u64) {
        self.l1_entries.store(n, Ordering::Relaxed);
    }

    pub fn l2_hit(&self) {
        self.l2_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn l2_miss(&self) {
        self.l2_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub fn l3_hit(&self) {
        self.l3_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub fn l3_miss(&self) {
        self.l3_misses.fetch_add(1, Ordering::Relaxed);
    }

    // ── Deferred reads ────────────────────────────────────────────────

    pub fn deferred_read(&self) {
        self.deferred_reads.fetch_add(1, Ordering::Relaxed);
    }

    // ── Pending reads table ───────────────────────────────────────────

    /// Record insertion of a pending read (FUSE deferred-read ticket).
    pub fn pending_reads_inc(&self) {
        let cur = self.pending_reads_current.fetch_add(1, Ordering::Relaxed) + 1;
        self.pending_reads_peak.fetch_max(cur, Ordering::Relaxed);
    }
    /// Record removal of a pending read (FUSE deferred-read ticket).
    ///
    /// Saturating: a double-resolution race (e.g. the deadline checker expiring
    /// a ticket that a worker also resolves) can decrement past zero, which
    /// previously wrapped the u64 gauge and surfaced as a huge value in
    /// `.stats`. Clamp at zero instead.
    pub fn pending_reads_dec(&self) {
        self.pending_reads_dec_n(1);
    }

    /// Record removal of `n` pending reads (FUSE deferred-read tickets) at
    /// once.  Used when a coalesced read group fans out its result to `n`
    /// waiters (TSI-2896).  Saturating: a double-resolution race can decrement
    /// past zero; clamp at zero instead of wrapping the u64 gauge.
    pub fn pending_reads_dec_n(&self, n: usize) {
        let _ =
            self.pending_reads_current
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |cur| {
                    Some(cur.saturating_sub(n as u64))
                });
    }

    // ── Poll hit rate ─────────────────────────────────────────────────

    /// Record one piece-wait poll; `ready` is true when the poll observed
    /// the piece already available (have_piece or cache hit).
    pub fn record_poll(&self, ready: bool) {
        self.poll_checks.fetch_add(1, Ordering::Relaxed);
        if ready {
            self.poll_hits.fetch_add(1, Ordering::Relaxed);
        }
    }

    // ── Download queue / worker gauges ────────────────────────────────

    /// RAII guard that tracks "current + peak" for the download queue.
    pub fn download_queue_guard(&self) -> GaugeGuard<'_> {
        GaugeGuard::new(&self.download_queue_current, &self.download_queue_peak)
    }

    /// RAII guard that tracks "current + peak" worker occupancy.
    pub fn worker_guard(&self) -> GaugeGuard<'_> {
        GaugeGuard::new(&self.workers_active, &self.workers_peak)
    }

    // ── Lock wait ─────────────────────────────────────────────────────

    /// Record the time spent waiting to acquire a contended lock.
    pub fn record_lock_wait(&self, wait: std::time::Duration) {
        self.lock_wait_nanos
            .fetch_add(wait.as_nanos() as u64, Ordering::Relaxed);
        self.lock_acquires.fetch_add(1, Ordering::Relaxed);
    }

    // ── Snapshot ──────────────────────────────────────────────────────

    /// Take an immutable snapshot of all counters.
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            l1_hits: self.l1_hits.load(Ordering::Relaxed),
            l1_misses: self.l1_misses.load(Ordering::Relaxed),
            l1_entries: self.l1_entries.load(Ordering::Relaxed),
            l2_hits: self.l2_hits.load(Ordering::Relaxed),
            l2_misses: self.l2_misses.load(Ordering::Relaxed),
            l3_hits: self.l3_hits.load(Ordering::Relaxed),
            l3_misses: self.l3_misses.load(Ordering::Relaxed),
            deferred_reads: self.deferred_reads.load(Ordering::Relaxed),
            pending_reads_current: self.pending_reads_current.load(Ordering::Relaxed),
            pending_reads_peak: self.pending_reads_peak.load(Ordering::Relaxed),
            poll_checks: self.poll_checks.load(Ordering::Relaxed),
            poll_hits: self.poll_hits.load(Ordering::Relaxed),
            download_queue_current: self.download_queue_current.load(Ordering::Relaxed),
            download_queue_peak: self.download_queue_peak.load(Ordering::Relaxed),
            workers_active: self.workers_active.load(Ordering::Relaxed),
            workers_peak: self.workers_peak.load(Ordering::Relaxed),
            lock_acquires: self.lock_acquires.load(Ordering::Relaxed),
            lock_wait_nanos: self.lock_wait_nanos.load(Ordering::Relaxed),
        }
    }
}

/// RAII guard for a `current` + `peak` gauge. Increments `current` (and
/// updates `peak`) on creation, decrements `current` on drop.
pub struct GaugeGuard<'a> {
    current: &'a AtomicU64,
}

impl<'a> GaugeGuard<'a> {
    fn new(current: &'a AtomicU64, peak: &'a AtomicU64) -> Self {
        let cur = current.fetch_add(1, Ordering::Relaxed) + 1;
        peak.fetch_max(cur, Ordering::Relaxed);
        Self { current }
    }
}

impl<'a> Drop for GaugeGuard<'a> {
    fn drop(&mut self) {
        self.current.fetch_sub(1, Ordering::Relaxed);
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counters_start_at_zero() {
        let m = Metrics::new();
        let s = m.snapshot();
        assert_eq!(s.l1_hits, 0);
        assert_eq!(s.l1_misses, 0);
        assert_eq!(s.deferred_reads, 0);
        assert_eq!(s.lock_acquires, 0);
    }

    #[test]
    fn hit_miss_counters_accumulate() {
        let m = Metrics::new();
        m.l1_hit();
        m.l1_hit();
        m.l1_miss();
        m.l2_hit();
        m.l2_miss();
        m.l3_miss();
        let s = m.snapshot();
        assert_eq!(s.l1_hits, 2);
        assert_eq!(s.l1_misses, 1);
        assert_eq!(s.l2_hits, 1);
        assert_eq!(s.l2_misses, 1);
        assert_eq!(s.l3_misses, 1);
        assert_eq!(s.l3_hits, 0);
    }

    #[test]
    fn deferred_counter_accumulates() {
        let m = Metrics::new();
        m.deferred_read();
        m.deferred_read();
        assert_eq!(m.snapshot().deferred_reads, 2);
    }

    #[test]
    fn poll_records_check_and_hit() {
        let m = Metrics::new();
        m.record_poll(false);
        m.record_poll(true);
        m.record_poll(true);
        let s = m.snapshot();
        assert_eq!(s.poll_checks, 3);
        assert_eq!(s.poll_hits, 2);
    }

    #[test]
    fn gauge_guard_tracks_current_and_peak() {
        let m = Metrics::new();
        {
            let _g1 = m.download_queue_guard();
            {
                let _g2 = m.download_queue_guard();
                assert_eq!(m.snapshot().download_queue_current, 2);
            }
            assert_eq!(m.snapshot().download_queue_current, 1);
        }
        let s = m.snapshot();
        assert_eq!(s.download_queue_current, 0);
        assert_eq!(s.download_queue_peak, 2);
    }

    #[test]
    fn pending_reads_tracks_peak() {
        let m = Metrics::new();
        m.pending_reads_inc();
        m.pending_reads_inc();
        m.pending_reads_dec();
        let s = m.snapshot();
        assert_eq!(s.pending_reads_current, 1);
        assert_eq!(s.pending_reads_peak, 2);
    }

    #[test]
    fn pending_reads_dec_saturates_at_zero() {
        let m = Metrics::new();
        // A double-resolution race would previously wrap the u64 gauge.
        m.pending_reads_dec();
        m.pending_reads_dec();
        assert_eq!(m.snapshot().pending_reads_current, 0);
        // A subsequent legitimate increment still works after clamping.
        m.pending_reads_inc();
        assert_eq!(m.snapshot().pending_reads_current, 1);
    }

    #[test]
    fn lock_wait_records_total_and_count() {
        let m = Metrics::new();
        m.record_lock_wait(std::time::Duration::from_micros(150));
        m.record_lock_wait(std::time::Duration::from_micros(250));
        let s = m.snapshot();
        assert_eq!(s.lock_acquires, 2);
        assert_eq!(s.lock_wait_nanos, 400_000);
    }
}

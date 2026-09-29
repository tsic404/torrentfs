//! Recent read-failure log — the data behind the per-torrent `.read-errors`
//! diagnostics file.
//!
//! A read that exhausts its piece-wait window is classified (see
//! `engine::classify_read_stall`) and answered with a bare `ENODATA`, so the
//! reason only reached the daemon's own stderr, where the reading client
//! cannot see it.  The log keeps the last [`READ_FAILURE_CAPACITY`] failures
//! per info_hash together with the swarm state observed at the moment of
//! failure, so the operator can read them back from inside the mount
//! (`data/<name>/.read-errors`).
//!
//! Written by the engine thread at failure time, read by the FUSE dispatch
//! thread.  Sharing one `Mutex` (rather than publishing into the periodic
//! `DownloadSnapshot`) keeps a fresh failure immediately visible: the failed
//! read has already returned `ENODATA` when the operator cats the file, so a
//! snapshot that lags by up to a tick would render an empty log right when it
//! matters.

use std::collections::{HashMap, VecDeque};
use std::time::SystemTime;

/// Most recent failures kept per info_hash; older records are evicted.
pub const READ_FAILURE_CAPACITY: usize = 8;

/// What a read's piece-wait window actually ran out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadStallCause {
    /// No seeder is connected and no cache-side evidence explains the stall,
    /// so nothing in the swarm can serve the read.
    NoSeeder,
    /// The read is waiting on data the on-disk cache does not hold: the piece
    /// it waits on was there and is gone, or its range cannot fit the cache at
    /// all.  Either way the read cannot be served from the cache and depends
    /// on a re-download.
    CacheStall,
    /// A seeder is connected and the cache holds the read, but the pieces did
    /// not arrive inside the window.
    SlowSwarm,
}

impl ReadStallCause {
    /// Stable identifier rendered in `.read-errors`, kept in the same spelling
    /// as the stderr hint the classifier logs.
    pub fn as_str(self) -> &'static str {
        match self {
            ReadStallCause::NoSeeder => "NoSeeder",
            ReadStallCause::CacheStall => "CacheStall",
            ReadStallCause::SlowSwarm => "SlowSwarm",
        }
    }

    /// The one thing that can unblock the next read of this torrent.  The
    /// message text already carries the evidence; this is the action distilled
    /// out of it, so a script can act without parsing prose.
    pub fn suggested_action(self) -> &'static str {
        match self {
            ReadStallCause::NoSeeder => {
                "wait for a seeder to connect (check tracker health), then retry the read"
            }
            ReadStallCause::CacheStall => {
                "raise [cache] cache_size to at least the size of the file being read"
            }
            ReadStallCause::SlowSwarm => {
                "retry the read; a seeder is connected but delivered nothing inside the \
                 window (raise [timeouts] read_timeout_secs to wait longer)"
            }
        }
    }
}

/// One failed read, with the torrent's swarm state at the moment it was
/// recorded.
#[derive(Debug, Clone)]
pub struct ReadFailure {
    pub cause: ReadStallCause,
    /// The same reason text the failed read returned to the caller.
    pub message: String,
    /// Wall-clock instant the read failed.
    pub at: SystemTime,
    pub num_peers: i32,
    pub num_seeds: i32,
    /// Swarm progress in percent (`status().progress * 100.0`), as reported to
    /// the reader at failure time.
    pub progress: f64,
}

/// Per-info_hash ring of recent read failures.
#[derive(Default)]
pub struct ReadFailureLog {
    by_info_hash: HashMap<String, VecDeque<ReadFailure>>,
}

impl ReadFailureLog {
    /// Record `failure` as the newest failure of `info_hash`, evicting the
    /// oldest record once the ring is full.
    pub fn record(&mut self, info_hash: &str, failure: ReadFailure) {
        let ring = self.by_info_hash.entry(info_hash.to_string()).or_default();
        if ring.len() == READ_FAILURE_CAPACITY {
            ring.pop_front();
        }
        ring.push_back(failure);
    }

    /// Failures recorded for `info_hash`, newest first.  Empty when that
    /// torrent never failed a read.
    pub fn recent(&self, info_hash: &str) -> Vec<ReadFailure> {
        self.by_info_hash
            .get(info_hash)
            .map(|ring| ring.iter().rev().cloned().collect())
            .unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::{ReadFailure, ReadFailureLog, ReadStallCause, READ_FAILURE_CAPACITY};
    use std::time::SystemTime;

    fn failure(message: &str) -> ReadFailure {
        ReadFailure {
            cause: ReadStallCause::NoSeeder,
            message: message.to_string(),
            at: SystemTime::UNIX_EPOCH,
            num_peers: 0,
            num_seeds: 0,
            progress: 0.0,
        }
    }

    /// The point of the ring: a torrent that fails its reads repeatedly must
    /// keep the newest [`READ_FAILURE_CAPACITY`] records and drop the oldest,
    /// not grow without bound and not drop the newest.
    #[test]
    fn ring_keeps_newest_records_and_evicts_oldest() {
        let mut log = ReadFailureLog::default();
        let total = READ_FAILURE_CAPACITY + 3;
        for i in 0..total {
            log.record("hash-a", failure(&format!("failure-{i}")));
        }

        let recent = log.recent("hash-a");
        assert_eq!(recent.len(), READ_FAILURE_CAPACITY);
        // Newest first: the last failure recorded is reported first.
        assert_eq!(recent[0].message, format!("failure-{}", total - 1));
        // Exactly the first three are gone.
        assert_eq!(recent.last().unwrap().message, "failure-3");
    }

    /// Records are keyed by info_hash: one torrent's failures must not leak
    /// into another's `.read-errors`, and a torrent that never failed reports
    /// nothing.
    #[test]
    fn logs_are_per_info_hash() {
        let mut log = ReadFailureLog::default();
        log.record("hash-a", failure("a-1"));
        log.record("hash-b", failure("b-1"));

        assert_eq!(log.recent("hash-a")[0].message, "a-1");
        assert_eq!(log.recent("hash-b")[0].message, "b-1");
        assert!(log.recent("hash-c").is_empty());
    }

    /// Each cause renders a stable identifier and a distinct action: the
    /// operator has to be able to tell the three stalls apart in the file and
    /// the rendered action must point at the cause's own remedy.
    #[test]
    fn causes_render_distinct_identifiers_and_actions() {
        let causes = [
            ReadStallCause::NoSeeder,
            ReadStallCause::CacheStall,
            ReadStallCause::SlowSwarm,
        ];
        for (i, cause) in causes.iter().enumerate() {
            for other in &causes[i + 1..] {
                assert_ne!(cause.as_str(), other.as_str());
                assert_ne!(cause.suggested_action(), other.suggested_action());
            }
        }
    }
}

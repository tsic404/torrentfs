use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Timeouts
// ============================================================

/// Default FUSE read timeout (seconds) used when `read_timeout_secs` is unset
/// or non-positive. Raised from 30s to 60s so a cold piece (e.g. piece 0 of a
/// large uncached file) has time to arrive from a healthy-but-slow swarm
/// before the read surfaces ENODATA.
pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 60;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TimeoutsConfig {
    pub peer_timeout: Option<i64>,
    pub urlseed_timeout: Option<i64>,
    pub urlseed_pipeline_size: Option<i64>,
    pub stop_tracker_timeout: Option<i64>,
    pub tracker_completion_timeout: Option<i64>,
    pub tracker_receive_timeout: Option<i64>,
    pub inactivity_timeout: Option<i64>,
    /// Timeout in seconds for waiting on torrent state transitions and piece
    /// downloads during FUSE read operations. Defaults to
    /// [`DEFAULT_READ_TIMEOUT_SECS`] when unset or non-positive.
    /// This is a torrentfs-level timeout, not passed to libtorrent.
    pub read_timeout_secs: Option<i64>,
}

impl TimeoutsConfig {
    /// Resolved read timeout: the configured value when positive, otherwise
    /// [`DEFAULT_READ_TIMEOUT_SECS`].
    pub fn resolved_read_timeout_secs(&self) -> u64 {
        self.read_timeout_secs
            .filter(|&v| v > 0)
            .map(|v| v as u64)
            .unwrap_or(DEFAULT_READ_TIMEOUT_SECS)
    }
}

impl WriteJson for TimeoutsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, peer_timeout);
        json_field_int!(map, self, urlseed_timeout);
        json_field_int!(map, self, urlseed_pipeline_size);
        json_field_int!(map, self, stop_tracker_timeout);
        json_field_int!(map, self, tracker_completion_timeout);
        json_field_int!(map, self, tracker_receive_timeout);
        json_field_int!(map, self, inactivity_timeout);
    }
}

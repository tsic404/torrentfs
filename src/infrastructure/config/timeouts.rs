use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Timeouts
// ============================================================

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
    /// Timeout in seconds for waiting on torrent state transitions and piece downloads
    /// during FUSE read operations. Defaults to 30s if not set.
    /// This is a torrentfs-level timeout, not passed to libtorrent.
    pub read_timeout_secs: Option<i64>,
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

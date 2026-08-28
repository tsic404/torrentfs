use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Tracker
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TrackerConfig {
    pub announce_to_all_trackers: Option<bool>,
    pub announce_to_all_tiers: Option<bool>,
    pub prefer_udp_trackers: Option<bool>,
    pub tracker_backoff: Option<i64>,
    pub tracker_maximum_response_length: Option<i64>,
    pub min_announce_interval: Option<i64>,
    pub udp_tracker_token_expiry: Option<i64>,
}

impl WriteJson for TrackerConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_bool!(map, self, announce_to_all_trackers);
        json_field_bool!(map, self, announce_to_all_tiers);
        json_field_bool!(map, self, prefer_udp_trackers);
        json_field_int!(map, self, tracker_backoff);
        json_field_int!(map, self, tracker_maximum_response_length);
        json_field_int!(map, self, min_announce_interval);
        json_field_int!(map, self, udp_tracker_token_expiry);
    }
}

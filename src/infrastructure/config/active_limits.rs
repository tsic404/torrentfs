use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Active Limits
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ActiveLimitsConfig {
    pub active_downloads: Option<i64>,
    pub active_seeds: Option<i64>,
    pub active_checking: Option<i64>,
    pub active_limit: Option<i64>,
    pub active_tracker_limit: Option<i64>,
    pub active_lsd_limit: Option<i64>,
    pub active_dht_limit: Option<i64>,
}

impl WriteJson for ActiveLimitsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, active_downloads);
        json_field_int!(map, self, active_seeds);
        json_field_int!(map, self, active_checking);
        json_field_int!(map, self, active_limit);
        json_field_int!(map, self, active_tracker_limit);
        json_field_int!(map, self, active_lsd_limit);
        json_field_int!(map, self, active_dht_limit);
    }
}

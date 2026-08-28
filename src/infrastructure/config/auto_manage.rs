use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Auto Manage
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AutoManageConfig {
    pub auto_manage_interval: Option<i64>,
    pub auto_manage_startup: Option<i64>,
    pub auto_manage_prefer_seeds: Option<bool>,
    pub dont_count_slow_torrents: Option<bool>,
    pub share_ratio_limit: Option<f64>,
    pub seed_time_ratio_limit: Option<f64>,
    pub seed_time_limit: Option<i64>,
}

impl WriteJson for AutoManageConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, auto_manage_interval);
        json_field_int!(map, self, auto_manage_startup);
        json_field_bool!(map, self, auto_manage_prefer_seeds);
        json_field_bool!(map, self, dont_count_slow_torrents);
        if let Some(val) = self.share_ratio_limit {
            map.insert(
                "share_ratio_limit".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        if let Some(val) = self.seed_time_ratio_limit {
            map.insert(
                "seed_time_ratio_limit".to_string(),
                serde_json::Value::Number(
                    serde_json::Number::from_f64(val).unwrap_or(serde_json::Number::from(0)),
                ),
            );
        }
        json_field_int!(map, self, seed_time_limit);
    }
}

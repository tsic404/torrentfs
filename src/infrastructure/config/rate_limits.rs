use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Rate Limits
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RateLimitsConfig {
    pub download_rate_limit: Option<i64>,
    pub upload_rate_limit: Option<i64>,
    pub rate_limit_utp: Option<bool>,
    pub rate_limit_ip_overhead: Option<bool>,
}

impl WriteJson for RateLimitsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, download_rate_limit);
        json_field_int!(map, self, upload_rate_limit);
        json_field_bool!(map, self, rate_limit_utp);
        json_field_bool!(map, self, rate_limit_ip_overhead);
    }
}

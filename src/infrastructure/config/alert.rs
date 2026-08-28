use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Alert
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AlertConfig {
    pub alert_mask: Option<i64>,
    pub alert_queue_size: Option<i64>,
}

impl WriteJson for AlertConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, alert_mask);
        json_field_int!(map, self, alert_queue_size);
    }
}

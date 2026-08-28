use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// DHT
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DhtConfig {
    pub enabled: Option<bool>,
    pub max_dht_items: Option<i64>,
    pub dht_announce_interval: Option<i64>,
    pub max_active_dht_limit: Option<i64>,
}

impl WriteJson for DhtConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(val) = self.enabled {
            map.insert("enable_dht".to_string(), serde_json::Value::Bool(val));
        }
        json_field_int!(map, self, max_dht_items);
        json_field_int!(map, self, dht_announce_interval);
        json_field_int!(map, self, max_active_dht_limit);
    }
}

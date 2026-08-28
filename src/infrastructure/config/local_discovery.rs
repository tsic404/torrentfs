use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;

// ============================================================
// Local Discovery
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct LocalDiscoveryConfig {
    pub lsd_enabled: Option<bool>,
    pub upnp_enabled: Option<bool>,
    pub natpmp_enabled: Option<bool>,
}

impl WriteJson for LocalDiscoveryConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        if let Some(val) = self.lsd_enabled {
            map.insert("enable_lsd".to_string(), serde_json::Value::Bool(val));
        }
        if let Some(val) = self.upnp_enabled {
            map.insert("enable_upnp".to_string(), serde_json::Value::Bool(val));
        }
        if let Some(val) = self.natpmp_enabled {
            map.insert("enable_natpmp".to_string(), serde_json::Value::Bool(val));
        }
    }
}

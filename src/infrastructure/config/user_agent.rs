use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_str;

// ============================================================
// User Agent
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct UserAgentConfig {
    pub user_agent: Option<String>,
    pub peer_fingerprint: Option<String>,
    pub always_send_user_agent: Option<bool>,
}

impl WriteJson for UserAgentConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, user_agent);
        json_field_str!(map, self, peer_fingerprint);
        json_field_bool!(map, self, always_send_user_agent);
    }
}

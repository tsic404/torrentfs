use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Encryption
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct EncryptionConfig {
    pub encryption_policy: Option<i64>,
    pub allowed_encryption_level: Option<i64>,
}

impl WriteJson for EncryptionConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, encryption_policy);
        json_field_int!(map, self, allowed_encryption_level);
    }
}

use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Algorithms
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct AlgorithmsConfig {
    pub choking_algorithm: Option<i64>,
    pub seed_choking_algorithm: Option<i64>,
    pub mixed_mode_algorithm: Option<i64>,
    pub suggest_mode: Option<i64>,
}

impl WriteJson for AlgorithmsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, choking_algorithm);
        json_field_int!(map, self, seed_choking_algorithm);
        json_field_int!(map, self, mixed_mode_algorithm);
        json_field_int!(map, self, suggest_mode);
    }
}

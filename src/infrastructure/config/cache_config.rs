use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Cache
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    pub cache_size: Option<i64>,
    pub cache_expiry: Option<i64>,
    pub use_read_cache: Option<bool>,
    pub use_disk_cache_pool: Option<bool>,
    pub volatile_read_cache: Option<bool>,
    pub guided_read_cache: Option<bool>,
    pub default_cache_min_age: Option<i64>,
}

impl WriteJson for CacheConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, cache_size);
        json_field_int!(map, self, cache_expiry);
        json_field_bool!(map, self, use_read_cache);
        json_field_bool!(map, self, use_disk_cache_pool);
        json_field_bool!(map, self, volatile_read_cache);
        json_field_bool!(map, self, guided_read_cache);
        json_field_int!(map, self, default_cache_min_age);
    }
}

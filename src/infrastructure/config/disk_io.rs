use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Disk I/O
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct DiskIoConfig {
    pub disk_io_write_mode: Option<i64>,
    pub disk_io_read_mode: Option<i64>,
    pub file_pool_size: Option<i64>,
    pub max_queued_disk_bytes: Option<i64>,
    pub max_queued_disk_bytes_low_watermark: Option<i64>,
    pub use_disk_read_ahead: Option<bool>,
    pub lock_disk_cache: Option<bool>,
    pub no_atime_storage: Option<bool>,
    pub low_prio_disk: Option<bool>,
}

impl WriteJson for DiskIoConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, disk_io_write_mode);
        json_field_int!(map, self, disk_io_read_mode);
        json_field_int!(map, self, file_pool_size);
        json_field_int!(map, self, max_queued_disk_bytes);
        json_field_int!(map, self, max_queued_disk_bytes_low_watermark);
        json_field_bool!(map, self, use_disk_read_ahead);
        json_field_bool!(map, self, lock_disk_cache);
        json_field_bool!(map, self, no_atime_storage);
        json_field_bool!(map, self, low_prio_disk);
    }
}

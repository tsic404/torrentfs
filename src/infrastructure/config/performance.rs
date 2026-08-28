use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Performance
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PerformanceConfig {
    pub aio_threads: Option<i64>,
    pub network_threads: Option<i64>,
    pub checking_mem_usage: Option<i64>,
    pub tick_interval: Option<i64>,
    pub send_buffer_watermark: Option<i64>,
    pub send_buffer_watermark_factor: Option<i64>,
    pub send_buffer_low_watermark: Option<i64>,
    pub recv_socket_buffer_size: Option<i64>,
    pub send_socket_buffer_size: Option<i64>,
    pub optimistic_disk_retry: Option<i64>,
}

impl WriteJson for PerformanceConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, aio_threads);
        json_field_int!(map, self, network_threads);
        json_field_int!(map, self, checking_mem_usage);
        json_field_int!(map, self, tick_interval);
        json_field_int!(map, self, send_buffer_watermark);
        json_field_int!(map, self, send_buffer_watermark_factor);
        json_field_int!(map, self, send_buffer_low_watermark);
        json_field_int!(map, self, recv_socket_buffer_size);
        json_field_int!(map, self, send_socket_buffer_size);
        json_field_int!(map, self, optimistic_disk_retry);
    }
}

use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;
use crate::json_field_str;

// ============================================================
// Connections
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConnectionsConfig {
    pub listen_interfaces: Option<String>,
    pub outgoing_interfaces: Option<String>,
    pub max_connections: Option<i64>,
    pub max_uploads: Option<i64>,
    pub listen_queue_size: Option<i64>,
    pub connection_speed: Option<i64>,
    pub smooth_connects: Option<bool>,
    pub allow_multiple_connections_per_ip: Option<bool>,
    pub max_peerlist_size: Option<i64>,
    pub max_paused_peerlist_size: Option<i64>,
    pub min_reconnect_time: Option<i64>,
    pub peer_connect_timeout: Option<i64>,
}

impl ConnectionsConfig {
    /// Validate the numeric fields that are semantically bounded:
    ///
    /// - `max_connections` / `max_uploads`: must be positive and fit the
    ///   libtorrent FFI boundary (`i32`), which truncates via
    ///   `static_cast<int>` in `libtorrent_wrapper.cpp`.
    /// - `max_uploads` is never applied by the wrapper (silently ignored for
    ///   libtorrent 2.0), but validating it keeps the TOML contract honest.
    /// - Remaining connections fields (`listen_queue_size`,
    ///   `connection_speed`, `max_peerlist_size`, `min_reconnect_time`,
    ///   `peer_connect_timeout`) are intentionally left to libtorrent
    ///   clamp/default semantics.
    pub(crate) fn validate(&self) -> Result<(), String> {
        check_positive_i32("max_connections", self.max_connections)?;
        check_positive_i32("max_uploads", self.max_uploads)?;
        Ok(())
    }
}

/// A connections count must be `1..=i32::MAX`: zero/negative is invalid and
/// anything above `i32::MAX` would be truncated at the FFI boundary.
fn check_positive_i32(name: &str, value: Option<i64>) -> Result<(), String> {
    match value {
        Some(v) if v < 1 => Err(format!("[connections] {} must be >= 1, got {}", name, v)),
        Some(v) if v > i32::MAX as i64 => Err(format!(
            "[connections] {} must be <= {}, got {}",
            name,
            i32::MAX,
            v
        )),
        _ => Ok(()),
    }
}

impl WriteJson for ConnectionsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, listen_interfaces);
        json_field_str!(map, self, outgoing_interfaces);
        json_field_int!(map, self, max_connections);
        json_field_int!(map, self, max_uploads);
        json_field_int!(map, self, listen_queue_size);
        json_field_int!(map, self, connection_speed);
        json_field_bool!(map, self, smooth_connects);
        json_field_bool!(map, self, allow_multiple_connections_per_ip);
        json_field_int!(map, self, max_peerlist_size);
        json_field_int!(map, self, max_paused_peerlist_size);
        json_field_int!(map, self, min_reconnect_time);
        json_field_int!(map, self, peer_connect_timeout);
    }
}

use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;
use crate::json_field_str;

// ============================================================
// Proxy
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ProxyConfig {
    pub host: Option<String>,
    pub port: Option<i64>,
    /// libtorrent `proxy_type` is 0..=7 (`none`/`socks4`/`socks5`/`socks5_pw`/
    /// `http`/`http_pw`/`i2p_proxy`/`crypto`) but the Rust config models it as
    /// a free-form string (TOML `type = "socks5"`) and `apply_str_setting`
    /// in the C wrapper does not map it — the value is silently dropped there.
    /// No enum-domain validation is applied here on purpose (TSI-2496).
    #[serde(rename = "type", alias = "proxy_type")]
    pub proxy_type: Option<String>,
    pub proxy_hostnames: Option<bool>,
    pub proxy_peer_connections: Option<bool>,
    pub proxy_tracker_connections: Option<bool>,
    pub anonymous_mode: Option<bool>,
    pub force_proxy: Option<bool>,
}

impl WriteJson for ProxyConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_str!(map, self, host);
        json_field_int!(map, self, port);
        if let Some(ref val) = self.proxy_type {
            if !val.is_empty() {
                map.insert(
                    "proxy_type".to_string(),
                    serde_json::Value::String(val.clone()),
                );
            }
        }
        json_field_bool!(map, self, proxy_hostnames);
        json_field_bool!(map, self, proxy_peer_connections);
        json_field_bool!(map, self, proxy_tracker_connections);
        json_field_bool!(map, self, anonymous_mode);
        json_field_bool!(map, self, force_proxy);
    }
}

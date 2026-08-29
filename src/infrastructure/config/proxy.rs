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
    /// libtorrent 2.1.1 `proxy_type_t` is 0..=6 (`none`/`socks4`/`socks5`/
    /// `socks5_pw`/`http`/`http_pw`/`i2p_proxy`); `none` is expressed on the
    /// Rust side as `None`. The config models the kind as a free-form string
    /// (TOML `type = "socks5"`) and `apply_str_setting` in the C wrapper
    /// does not map `proxy_type`, so it is silently dropped there.
    /// Enum-domain validation is applied in `ProxyConfig::validate()`.
    #[serde(rename = "type", alias = "proxy_type")]
    pub proxy_type: Option<String>,
    pub proxy_hostnames: Option<bool>,
    pub proxy_peer_connections: Option<bool>,
    pub proxy_tracker_connections: Option<bool>,
    pub anonymous_mode: Option<bool>,
    pub force_proxy: Option<bool>,
}

impl ProxyConfig {
    /// Validate `proxy_type` (canonical `type` / alias `proxy_type`).
    ///
    /// The field is optional, but once present — including as an empty
    /// string — it MUST name a real libtorrent proxy kind. Empty or
    /// unknown values are rejected here rather than being silently
    /// dropped in `write_json` and falling back to libtorrent's default,
    /// which the user would never notice.
    pub(crate) fn validate(&self) -> Result<(), String> {
        const LEGAL_PROXY_TYPES: [&str; 6] = [
            "socks4",
            "socks5",
            "socks5_pw",
            "http",
            "http_pw",
            "i2p_proxy",
        ];
        match self.proxy_type.as_deref() {
            Some(val) if LEGAL_PROXY_TYPES.contains(&val) => Ok(()),
            Some(val) => Err(format!(
                "[proxy] proxy_type must be one of {}, got {:?}",
                LEGAL_PROXY_TYPES.join(", "),
                val
            )),
            None => Ok(()),
        }
    }
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

use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;

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
    /// converts it to the `proxy_type` int_types setting (TSI-2529).
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
    ///
    /// The accepted domain also tracks the wrapper's compile-time capability:
    /// `i2p_proxy` is only legal when libtorrent was built with
    /// `TORRENT_USE_I2P=1`. On I2P-disabled builds the C++ wrapper's
    /// `apply_str_setting` has no `i2p_proxy` branch and silently ignores the
    /// value, so rejecting it here turns that silent drop into a config-time
    /// error (TSI-2547).
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.validate_with(crate::infrastructure::config::i2p_enabled())
    }

    /// Capability-aware validation, factored out so both I2P build variants
    /// are unit-testable without relinking against a different libtorrent.
    fn validate_with(&self, i2p_enabled: bool) -> Result<(), String> {
        const LEGAL_PROXY_TYPES: [&str; 6] = [
            "socks4",
            "socks5",
            "socks5_pw",
            "http",
            "http_pw",
            "i2p_proxy",
        ];
        match self.proxy_type.as_deref() {
            Some("i2p_proxy") if !i2p_enabled => Err(
                "i2p_proxy requires a libtorrent build with I2P support (TORRENT_USE_I2P=1); \
                 this build has I2P compiled out"
                    .to_string(),
            ),
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
        // `host`/`port` are the user-facing TOML keys; libtorrent's real
        // settings_pack names are `proxy_hostname`/`proxy_port`. The wrapper
        // only recognizes the latter — emitting `host`/`port` made both values
        // silently dropped (TSI-2538). The other fields already use their
        // libtorrent names verbatim, so only these two need explicit keys.
        if let Some(val) = &self.host {
            if !val.is_empty() {
                map.insert(
                    "proxy_hostname".to_string(),
                    serde_json::Value::String(val.clone()),
                );
            }
        }
        if let Some(val) = self.port {
            map.insert(
                "proxy_port".to_string(),
                serde_json::Value::Number(serde_json::Number::from(val)),
            );
        }
        if let Some(val) = &self.proxy_type {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(proxy_type: &str) -> ProxyConfig {
        ProxyConfig {
            proxy_type: Some(proxy_type.to_string()),
            ..ProxyConfig::default()
        }
    }

    #[test]
    fn validate_accepts_i2p_proxy_when_enabled() {
        assert!(config("i2p_proxy").validate_with(true).is_ok());
    }

    #[test]
    fn validate_acceptance_matches_linked_libtorrent_capability() {
        // The live probe round-trips through the C++ wrapper's TORRENT_USE_I2P
        // so validation and apply_str_setting can never drift on any build.
        let enabled = crate::infrastructure::config::i2p_enabled();
        assert_eq!(config("i2p_proxy").validate().is_ok(), enabled);
    }

    #[test]
    fn validate_rejects_i2p_proxy_when_disabled() {
        let err = config("i2p_proxy").validate_with(false).unwrap_err();
        assert!(
            err.contains("I2P") || err.contains("i2p"),
            "unexpected error: {}",
            err
        );
    }

    #[test]
    fn validate_accepts_non_i2p_types_regardless_of_capability() {
        for t in ["socks4", "socks5", "socks5_pw", "http", "http_pw"] {
            assert!(config(t).validate_with(false).is_ok(), "{t}");
            assert!(config(t).validate_with(true).is_ok(), "{t}");
        }
    }

    #[test]
    fn validate_rejects_unknown_type() {
        let err = config("bogus").validate_with(true).unwrap_err();
        assert!(err.contains("bogus"), "unexpected error: {}", err);
    }

    #[test]
    fn validate_accepts_none() {
        assert!(ProxyConfig::default().validate_with(false).is_ok());
        assert!(ProxyConfig::default().validate_with(true).is_ok());
    }
}

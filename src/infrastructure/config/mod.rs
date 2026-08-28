use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::error::{TorrentError, TorrentResult};

// ---- config sub-modules ----
pub mod active_limits;
pub mod alert;
pub mod algorithms;
pub mod auto_manage;
pub mod cache_config;
pub mod concurrency;
pub mod connections;
pub mod dht;
pub mod disk_io;
pub mod encryption;
pub mod local_discovery;
pub mod misc;
pub mod performance;
pub mod piece_priority;
pub mod pieces;
pub mod proxy;
pub mod rate_limits;
pub mod timeouts;
pub mod tracker;
pub mod user_agent;

pub use active_limits::ActiveLimitsConfig;
pub use alert::AlertConfig;
pub use algorithms::AlgorithmsConfig;
pub use auto_manage::AutoManageConfig;
pub use cache_config::CacheConfig;
pub use concurrency::ConcurrencyConfig;
pub use connections::ConnectionsConfig;
pub use dht::DhtConfig;
pub use disk_io::DiskIoConfig;
pub use encryption::EncryptionConfig;
pub use local_discovery::LocalDiscoveryConfig;
pub use misc::MiscConfig;
pub use performance::PerformanceConfig;
pub use piece_priority::PiecePriorityToml;
pub use pieces::PiecesConfig;
pub use proxy::ProxyConfig;
pub use rate_limits::RateLimitsConfig;
pub use timeouts::TimeoutsConfig;
pub use tracker::TrackerConfig;
pub use user_agent::UserAgentConfig;

/// Top-level TOML configuration for torrentfs.
/// All fields are optional — missing values use libtorrent defaults.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TorrentfsConfig {
    #[serde(default)]
    pub connections: ConnectionsConfig,

    #[serde(default)]
    pub dht: DhtConfig,

    #[serde(default)]
    pub local_discovery: LocalDiscoveryConfig,

    #[serde(default)]
    pub rate_limits: RateLimitsConfig,

    #[serde(default)]
    pub disk_io: DiskIoConfig,

    #[serde(default)]
    pub cache: CacheConfig,

    #[serde(default)]
    pub concurrency: ConcurrencyConfig,

    #[serde(default)]
    pub pieces: PiecesConfig,

    #[serde(default)]
    pub timeouts: TimeoutsConfig,

    #[serde(default)]
    pub tracker: TrackerConfig,

    #[serde(default)]
    pub algorithms: AlgorithmsConfig,

    #[serde(default)]
    pub active_limits: ActiveLimitsConfig,

    #[serde(default)]
    pub auto_manage: AutoManageConfig,

    #[serde(default)]
    pub encryption: EncryptionConfig,

    #[serde(default)]
    pub proxy: ProxyConfig,

    #[serde(default)]
    pub user_agent: UserAgentConfig,

    #[serde(default)]
    pub alert: AlertConfig,

    #[serde(default)]
    pub performance: PerformanceConfig,

    #[serde(default)]
    pub misc: MiscConfig,
    #[serde(default)]
    pub piece_priority: PiecePriorityToml,
}

impl TorrentfsConfig {
    /// Load configuration from a TOML file.
    pub fn from_file(path: &Path) -> TorrentResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            TorrentError::ParseError(format!("Failed to read config file {:?}: {}", path, e))
        })?;
        let config: TorrentfsConfig = toml::from_str(&content).map_err(|e| {
            TorrentError::ParseError(format!("Invalid config TOML in {:?}: {}", path, e))
        })?;
        Ok(config)
    }

    /// Default configuration (all libtorrent defaults).
    pub fn default_config() -> Self {
        Self::default()
    }

    /// Serialize non-default settings to a flat JSON string for the C FFI layer.
    /// The JSON keys must match libtorrent settings_pack names exactly.
    pub fn to_settings_json(&self) -> String {
        let mut map = serde_json::Map::new();

        self.connections.write_json(&mut map);
        self.dht.write_json(&mut map);
        self.local_discovery.write_json(&mut map);
        self.rate_limits.write_json(&mut map);
        self.disk_io.write_json(&mut map);
        self.cache.write_json(&mut map);
        self.pieces.write_json(&mut map);
        self.timeouts.write_json(&mut map);
        self.tracker.write_json(&mut map);
        self.algorithms.write_json(&mut map);
        self.active_limits.write_json(&mut map);
        self.auto_manage.write_json(&mut map);
        self.encryption.write_json(&mut map);
        self.proxy.write_json(&mut map);
        self.user_agent.write_json(&mut map);
        self.alert.write_json(&mut map);
        self.performance.write_json(&mut map);
        self.misc.write_json(&mut map);

        serde_json::to_string(&map).unwrap_or_else(|_| "{}".to_string())
    }
}

impl Default for TorrentfsConfig {
    fn default() -> Self {
        TorrentfsConfig {
            connections: ConnectionsConfig::default(),
            dht: DhtConfig::default(),
            local_discovery: LocalDiscoveryConfig::default(),
            rate_limits: RateLimitsConfig::default(),
            disk_io: DiskIoConfig::default(),
            cache: CacheConfig::default(),
            concurrency: ConcurrencyConfig::default(),
            pieces: PiecesConfig::default(),
            timeouts: TimeoutsConfig::default(),
            tracker: TrackerConfig::default(),
            algorithms: AlgorithmsConfig::default(),
            active_limits: ActiveLimitsConfig::default(),
            auto_manage: AutoManageConfig::default(),
            encryption: EncryptionConfig::default(),
            proxy: ProxyConfig::default(),
            user_agent: UserAgentConfig::default(),
            alert: AlertConfig::default(),
            performance: PerformanceConfig::default(),
            misc: MiscConfig::default(),
            piece_priority: PiecePriorityToml::default(),
        }
    }
}

// Helper trait and macros for writing config sections to JSON.
// These are exported so sub-modules can use them via `crate::infrastructure::config::WriteJson` etc.
#[macro_export]
macro_rules! json_field_str {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(ref val) = $self.$field {
            if !val.is_empty() {
                $map.insert(
                    stringify!($field).to_string(),
                    serde_json::Value::String(val.clone()),
                );
            }
        }
    };
}

#[macro_export]
macro_rules! json_field_int {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(val) = $self.$field {
            $map.insert(
                stringify!($field).to_string(),
                serde_json::Value::Number(serde_json::Number::from(val)),
            );
        }
    };
}

#[macro_export]
macro_rules! json_field_bool {
    ($map:expr, $self:expr, $field:ident) => {
        if let Some(val) = $self.$field {
            $map.insert(stringify!($field).to_string(), serde_json::Value::Bool(val));
        }
    };
}

pub(crate) trait WriteJson {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile;

    #[test]
    fn test_default_config_is_empty_json() {
        let config = TorrentfsConfig::default();
        let json = config.to_settings_json();
        assert_eq!(json, "{}");
    }

    #[test]
    fn test_parse_minimal_config() {
        let toml_str = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"

[dht]
enabled = true
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(
            config.connections.listen_interfaces,
            Some("0.0.0.0:6881".to_string())
        );
        assert_eq!(config.dht.enabled, Some(true));
        // Unspecified fields should be None
        assert_eq!(config.connections.max_connections, None);
    }

    #[test]
    fn test_settings_json_with_values() {
        let toml_str = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 200

[dht]
enabled = true
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        let json = config.to_settings_json();
        assert!(json.contains("listen_interfaces"));
        assert!(json.contains("0.0.0.0:6881"));
        assert!(json.contains("max_connections"));
        assert!(json.contains("200"));
        assert!(json.contains("enable_dht"));
        assert!(json.contains("true"));
    }

    #[test]
    fn test_config_from_file() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("config.toml");
        std::fs::write(
            &config_path,
            r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 200

[timeouts]
read_timeout_secs = 60

[local_discovery]
lsd_enabled = false
"#,
        )
        .unwrap();

        let config = TorrentfsConfig::from_file(&config_path).unwrap();
        assert_eq!(
            config.connections.listen_interfaces,
            Some("0.0.0.0:6881".to_string())
        );
        assert_eq!(config.connections.max_connections, Some(200));
        assert_eq!(config.timeouts.read_timeout_secs, Some(60));
        assert_eq!(config.local_discovery.lsd_enabled, Some(false));

        // Verify JSON output includes the settings
        let json = config.to_settings_json();
        assert!(json.contains("listen_interfaces"));
        assert!(json.contains("max_connections"));
        assert!(json.contains("enable_lsd")); // false → still written as key
    }

    #[test]
    fn test_config_from_file_nonexistent() {
        let result = TorrentfsConfig::from_file(std::path::Path::new("/nonexistent/config.toml"));
        assert!(result.is_err());
        match result {
            Err(TorrentError::ParseError(_)) => {} // ParseError wraps IO errors for config
            Err(e) => panic!("Expected ParseError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_config_from_file_invalid_toml() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("invalid.toml");
        std::fs::write(&config_path, "this is not valid toml {{{").unwrap();

        let result = TorrentfsConfig::from_file(&config_path);
        assert!(result.is_err());
        match result {
            Err(TorrentError::ParseError(_)) => {}
            Err(e) => panic!("Expected ParseError, got {:?}", e),
            Ok(_) => panic!("Expected error"),
        }
    }

    #[test]
    fn test_read_timeout_config() {
        // Default: read_timeout_secs not set → defaults to 30
        let default_config = TorrentfsConfig::default_config();
        let timeout = default_config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);
        assert_eq!(timeout, 30);

        // Custom timeout
        let toml_str = r#"
[timeouts]
read_timeout_secs = 10
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.timeouts.read_timeout_secs, Some(10));
    }
    #[test]
    fn test_config_rejects_unknown_section() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("unknown_section.toml");
        std::fs::write(&config_path, "[bogus_section]\nfoo = 1\n").unwrap();

        let result = TorrentfsConfig::from_file(&config_path);
        assert!(result.is_err(), "unknown top-level section must fail");
        match result {
            Err(TorrentError::ParseError(msg)) => {
                assert!(
                    msg.contains("unknown field") || msg.contains("bogus_section"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("Expected ParseError, got {:?}", other),
        }
    }

    #[test]
    fn test_config_rejects_unknown_key_in_section() {
        let dir = tempfile::TempDir::new().unwrap();
        let config_path = dir.path().join("unknown_key.toml");
        std::fs::write(&config_path, "[cache]\nmax_size = \"not-a-size\"\n").unwrap();

        let result = TorrentfsConfig::from_file(&config_path);
        assert!(result.is_err(), "unknown key in known section must fail");
        match result {
            Err(TorrentError::ParseError(msg)) => {
                assert!(
                    msg.contains("unknown field") && msg.contains("max_size"),
                    "unexpected error: {}",
                    msg
                );
            }
            other => panic!("Expected ParseError, got {:?}", other),
        }
    }

    #[test]
    fn test_piece_priority_config_section() {
        let toml_str = r#"
[piece_priority]
access_window_mb = 2048
rest_priority = 0
backward_priority = 1
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.piece_priority.access_window_mb, Some(2048));
        assert_eq!(config.piece_priority.rest_priority, Some(0));
        assert_eq!(config.piece_priority.backward_priority, Some(1));
        // Unspecified fields remain None (so they fall back to defaults).
        assert_eq!(config.piece_priority.current_priority, None);
        assert_eq!(config.piece_priority.window_edge_priority, None);
    }
}

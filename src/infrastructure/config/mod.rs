use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::config::enums::EnumValidator;

/// Default on-disk state locations when `--db` / `--cache` are not given.
///
/// Both live under the XDG data directory (`$XDG_DATA_HOME`, else
/// `~/.local/share`), never under a FUSE mountpoint: a path inside the
/// mountpoint is resolved in the mounted namespace instead of reaching the
/// underlying disk — under `<mount>/data/` it hits the read-only projection
/// (`EROFS`), elsewhere it resolves inside the synthetic FUSE root
/// (`EACCES`/`ENOENT`) — so the state can never be persisted there (C-12,
/// C-2.2.4).
pub fn default_state_paths() -> (PathBuf, PathBuf) {
    let base = state_base_dir();
    (
        base.join("torrentfs/db/metadata.db"),
        base.join("torrentfs/cache"),
    )
}

/// Shared base the state falls back to when the XDG data directory is
/// unavailable.  Single source for both `default_state_paths` and the
/// ownership check below, so the two cannot drift apart.
const TMP_STATE_BASE: &str = "/tmp";

fn state_base_dir() -> PathBuf {
    dirs::data_local_dir().unwrap_or_else(|| PathBuf::from(TMP_STATE_BASE))
}

/// `<base>/torrentfs`, the root the default state lives under.
fn tmp_state_root() -> PathBuf {
    Path::new(TMP_STATE_BASE).join("torrentfs")
}

#[cfg(unix)]
fn current_euid() -> u32 {
    // `geteuid` has no failure mode and needs no allocation.
    unsafe { libc::geteuid() }
}

/// Refuse a fallback root in the shared `/tmp` that the current user does not
/// own, and refuse one that is a symbolic link: following a link would adopt
/// the directory behind it and let its owner redirect (or watch) every write
/// the daemon makes.
fn check_tmp_fallback_root(root: &Path) -> TorrentResult<()> {
    let metadata = match fs::symlink_metadata(root) {
        Ok(metadata) => metadata,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => {
            return Err(TorrentError::IoError(format!(
                "Failed to inspect state directory {}: {}",
                root.display(),
                e
            )))
        }
    };

    if metadata.file_type().is_symlink() {
        return Err(TorrentError::IoError(format!(
            "Refusing to use state directory {}: it is a symbolic link",
            root.display()
        )));
    }

    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        let owner = metadata.uid();
        if owner != current_euid() {
            return Err(TorrentError::IoError(format!(
                "Refusing to use state directory {}: owned by uid {}, running as uid {}",
                root.display(),
                owner,
                current_euid()
            )));
        }
    }

    Ok(())
}

/// Create `dir`, and every missing ancestor up to the first existing one, with
/// owner-only permissions (`0700`).
///
/// The state tree holds the piece cache and the SQLite metadata: a
/// world-readable root lets another local user read `cache_metadata.txt`,
/// which is the file path resolution has to defend against (C-5.4.6).  Only
/// directories this call creates are tightened, so an existing parent the user
/// prepared (a mounted volume, `~/.local/share`) keeps its mode.
///
/// When the XDG data directory is unavailable the state root falls back to
/// `/tmp/torrentfs`; a root that already exists there must belong to the
/// current user and must not be a symbolic link (see
/// [`check_tmp_fallback_root`]).
pub fn create_state_dir(dir: &Path) -> TorrentResult<()> {
    // `Path::parent` of a bare file name is `""`; there is nothing to create
    // for the current directory and no way to chmod it.
    if dir.as_os_str().is_empty() {
        return Ok(());
    }

    let fallback_root = tmp_state_root();
    if dir.starts_with(&fallback_root) {
        check_tmp_fallback_root(&fallback_root)?;
    }

    let mut missing = Vec::new();
    let mut candidate = dir;
    while !candidate.exists() {
        missing.push(candidate.to_path_buf());
        match candidate.parent() {
            // `""` is the current directory, not a parent to create: a bare
            // relative path (`--cache cache-data`) stops here.
            Some(parent) if !parent.as_os_str().is_empty() => candidate = parent,
            _ => break,
        }
    }

    fs::create_dir_all(dir).map_err(|e| {
        TorrentError::IoError(format!(
            "Failed to create state directory {}: {}",
            dir.display(),
            e
        ))
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        for created in &missing {
            fs::set_permissions(created, fs::Permissions::from_mode(0o700)).map_err(|e| {
                TorrentError::IoError(format!(
                    "Failed to restrict permissions on state directory {}: {}",
                    created.display(),
                    e
                ))
            })?;
        }
    }

    Ok(())
}

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
pub mod enums;
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
pub use concurrency::{ConcurrencyConfig, MAX_DOWNLOAD_QUEUE_DEPTH, MAX_DOWNLOAD_WORKERS};
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
pub use timeouts::{
    TimeoutsConfig, DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS, DEFAULT_PEER_DISCOVERY_WAIT_SECS,
    DEFAULT_READ_TIMEOUT_SECS, MAX_LOCAL_TIMEOUT_SECS,
};
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
    /// Validate numeric value ranges for the fields whose domain is bounded:
    /// negative counts, negative rate limits, values truncated at the i32 FFI
    /// boundary, the torrentfs-level timeouts, and the concurrency limits.
    /// All other numeric fields are intentionally left to libtorrent
    /// clamp/default semantics (see each section's `validate` doc for
    /// rationale).
    fn validate(&self) -> TorrentResult<()> {
        let checks = [
            self.connections.validate(),
            self.rate_limits.validate(),
            self.timeouts.validate(),
            self.concurrency.validate(),
            self.proxy.validate(),
        ];
        match checks.into_iter().find_map(Result::err) {
            Some(msg) => Err(TorrentError::ParseError(format!(
                "Invalid config value: {}",
                msg
            ))),
            None => Ok(()),
        }
    }

    /// Load configuration from a TOML file, then validate numeric and enum
    /// value domains.
    pub fn from_file(path: &Path) -> TorrentResult<Self> {
        let content = std::fs::read_to_string(path).map_err(|e| {
            TorrentError::ParseError(format!("Failed to read config file {:?}: {}", path, e))
        })?;
        let config: TorrentfsConfig = toml::from_str(&content).map_err(|e| {
            TorrentError::ParseError(format!("Invalid config TOML in {:?}: {}", path, e))
        })?;
        config.validate()?;
        config.validate_enum_ranges()?;
        Ok(config)
    }

    /// Validate the value domain of libtorrent-backed enum fields.
    ///
    /// Numeric range validation lives in `validate()`; `from_file`
    /// calls both so neither check can silently drop the other.
    pub fn validate_enum_ranges(&self) -> TorrentResult<()> {
        let errs = [
            EnumValidator::choking_algorithm(self.algorithms.choking_algorithm),
            EnumValidator::seed_choking_algorithm(self.algorithms.seed_choking_algorithm),
            EnumValidator::mixed_mode_algorithm(self.algorithms.mixed_mode_algorithm),
            EnumValidator::suggest_mode(self.algorithms.suggest_mode),
            EnumValidator::encryption_policy(self.encryption.encryption_policy),
            EnumValidator::allowed_encryption_level(self.encryption.allowed_encryption_level),
        ];

        match errs.into_iter().flatten().next() {
            Some(e) => Err(TorrentError::ConfigError(e.to_string())),
            None => Ok(()),
        }
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

/// Whether the linked libtorrent build compiled I2P support in
/// (`TORRENT_USE_I2P=1`). Mirrors the C++ wrapper capability so config
/// validation can reject `i2p_proxy` on I2P-disabled builds instead of
/// letting `apply_str_setting` silently drop it.
pub(crate) fn i2p_enabled() -> bool {
    unsafe { libtorrent_sys::lt_torrent_i2p_enabled() != 0 }
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
peer_wait_cap_secs = 30
no_seeder_read_timeout_secs = 20

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
        assert_eq!(config.timeouts.resolved_peer_wait_cap_secs(), Some(30));
        assert_eq!(config.timeouts.resolved_no_seeder_read_timeout_secs(), 20);
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
        // Default: read_timeout_secs unset → resolves to DEFAULT_READ_TIMEOUT_SECS (60).
        let default_config = TorrentfsConfig::default_config();
        assert_eq!(
            default_config.timeouts.resolved_read_timeout_secs(),
            DEFAULT_READ_TIMEOUT_SECS
        );

        // Non-positive value falls back to the default.
        let non_positive: TorrentfsConfig =
            toml::from_str("[timeouts]\nread_timeout_secs = 0\n").unwrap();
        assert_eq!(
            non_positive.timeouts.resolved_read_timeout_secs(),
            DEFAULT_READ_TIMEOUT_SECS
        );

        // Custom timeout
        let toml_str = r#"
[timeouts]
read_timeout_secs = 10
"#;
        let config: TorrentfsConfig = toml::from_str(toml_str).unwrap();
        assert_eq!(config.timeouts.read_timeout_secs, Some(10));
        assert_eq!(config.timeouts.resolved_read_timeout_secs(), 10);
    }

    #[test]
    fn test_peer_discovery_wait_config() {
        // Default: unset → the shipped discovery window, long enough to cover
        // a cold mount's first announce.
        let default_config = TorrentfsConfig::default_config();
        assert_eq!(
            default_config.timeouts.resolved_peer_discovery_wait_secs(),
            DEFAULT_PEER_DISCOVERY_WAIT_SECS
        );

        // Non-positive value falls back to the default, like read_timeout_secs.
        let non_positive: TorrentfsConfig =
            toml::from_str("[timeouts]\npeer_discovery_wait_secs = 0\n").unwrap();
        assert_eq!(
            non_positive.timeouts.resolved_peer_discovery_wait_secs(),
            DEFAULT_PEER_DISCOVERY_WAIT_SECS
        );

        // Custom window
        let config: TorrentfsConfig =
            toml::from_str("[timeouts]\npeer_discovery_wait_secs = 90\n").unwrap();
        assert_eq!(config.timeouts.peer_discovery_wait_secs, Some(90));
        assert_eq!(config.timeouts.resolved_peer_discovery_wait_secs(), 90);
    }

    #[test]
    fn test_peer_wait_cap_config() {
        // Default: unset → no extra cap, so the discovery window stays the one
        // the read timeout and `peer_discovery_wait_secs` agree on.
        let default_config = TorrentfsConfig::default_config();
        assert_eq!(default_config.timeouts.resolved_peer_wait_cap_secs(), None);

        // Non-positive means the same as unset — a cap of zero seconds would
        // fail every cold read on its first poll.
        let non_positive: TorrentfsConfig =
            toml::from_str("[timeouts]\npeer_wait_cap_secs = 0\n").unwrap();
        assert_eq!(non_positive.timeouts.resolved_peer_wait_cap_secs(), None);

        // Custom cap
        let config: TorrentfsConfig =
            toml::from_str("[timeouts]\npeer_wait_cap_secs = 5\n").unwrap();
        assert_eq!(config.timeouts.peer_wait_cap_secs, Some(5));
        assert_eq!(config.timeouts.resolved_peer_wait_cap_secs(), Some(5));
    }

    #[test]
    fn test_no_seeder_read_timeout_config() {
        // Default: unset → the shipped no-seeder piece-wait window.
        let default_config = TorrentfsConfig::default_config();
        assert_eq!(
            default_config
                .timeouts
                .resolved_no_seeder_read_timeout_secs(),
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
        );

        // Non-positive value falls back to the default, like read_timeout_secs.
        let non_positive: TorrentfsConfig =
            toml::from_str("[timeouts]\nno_seeder_read_timeout_secs = 0\n").unwrap();
        assert_eq!(
            non_positive.timeouts.resolved_no_seeder_read_timeout_secs(),
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
        );

        // Custom window
        let config: TorrentfsConfig =
            toml::from_str("[timeouts]\nno_seeder_read_timeout_secs = 60\n").unwrap();
        assert_eq!(config.timeouts.no_seeder_read_timeout_secs, Some(60));
        assert_eq!(config.timeouts.resolved_no_seeder_read_timeout_secs(), 60);
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
        // `backward_priority` is accepted (deprecated, ignored at runtime) so
        // existing config files keep parsing.
        assert_eq!(config.piece_priority.backward_priority, Some(1));
        // Unspecified fields remain None (so they fall back to defaults).
        assert_eq!(config.piece_priority.current_priority, None);
        assert_eq!(config.piece_priority.window_edge_priority, None);
    }

    #[cfg(unix)]
    #[test]
    fn test_tmp_fallback_root_rejects_a_symlink() {
        let root = tempfile::TempDir::new().unwrap();
        let target = root.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = root.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();

        assert!(
            check_tmp_fallback_root(&link).is_err(),
            "a symlinked state root would let its owner redirect every write"
        );
        assert!(
            check_tmp_fallback_root(&target).is_ok(),
            "a real directory owned by the current user is accepted"
        );
        assert!(
            check_tmp_fallback_root(&root.path().join("missing")).is_ok(),
            "an absent root is created later and needs no check"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_create_state_dir_accepts_a_relative_path() {
        use std::os::unix::fs::PermissionsExt;
        use std::sync::Mutex;

        // `--cache cache-data` resolves against the process working directory.
        // Its parent is `""`, the current directory, which must not be pushed
        // as a directory to create or chmod (that used to fail with ENOENT
        // after the directory had already been created).
        static CWD_LOCK: Mutex<()> = Mutex::new(());

        struct RestoreCwd(std::path::PathBuf);
        impl Drop for RestoreCwd {
            fn drop(&mut self) {
                let _ = std::env::set_current_dir(&self.0);
            }
        }

        let _guard = CWD_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let root = tempfile::TempDir::new().unwrap();
        let _restore = RestoreCwd(std::env::current_dir().unwrap());
        std::env::set_current_dir(root.path()).unwrap();

        for relative in ["cache-data", "nested/child"] {
            create_state_dir(Path::new(relative)).expect("create relative state dir");
            let created = root.path().join(relative);
            assert!(created.is_dir(), "{relative} was not created");
            let mode = fs::metadata(&created).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{relative} has mode {mode:o}");
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_create_state_dir_is_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::TempDir::new().unwrap();
        let nested = root.path().join("torrentfs/db");
        create_state_dir(&nested).expect("create nested state dir");

        // Every directory this call created is owner-only: the tree holds the
        // cache metadata and the SQLite state.
        for path in [
            root.path().join("torrentfs"),
            root.path().join("torrentfs/db"),
        ] {
            let mode = fs::metadata(&path).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o700, "{} has mode {:o}", path.display(), mode);
        }
    }

    #[cfg(unix)]
    #[test]
    fn test_create_state_dir_leaves_existing_mode_alone() {
        use std::os::unix::fs::PermissionsExt;

        let root = tempfile::TempDir::new().unwrap();
        let existing = root.path().join("share");
        fs::create_dir(&existing).unwrap();
        fs::set_permissions(&existing, fs::Permissions::from_mode(0o755)).unwrap();

        create_state_dir(&existing).expect("adopt existing dir");

        // A directory the caller prepared keeps its mode; only newly created
        // levels are tightened.
        let mode = fs::metadata(&existing).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o755);
    }

    fn load_config(toml_str: &str) -> TorrentResult<TorrentfsConfig> {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("config.toml");
        std::fs::write(&path, toml_str).unwrap();
        TorrentfsConfig::from_file(&path)
    }

    #[test]
    fn test_local_timeout_keys_reject_values_above_one_day() {
        for key in [
            "read_timeout_secs",
            "peer_discovery_wait_secs",
            "peer_wait_cap_secs",
            "no_seeder_read_timeout_secs",
        ] {
            let over = format!("[timeouts]\n{} = {}\n", key, MAX_LOCAL_TIMEOUT_SECS + 1);
            let err = load_config(&over).expect_err("over-limit timeout must be rejected");
            let msg = err.to_string();
            assert!(
                msg.contains(key) && msg.contains(&MAX_LOCAL_TIMEOUT_SECS.to_string()),
                "error must name the key and its range, got: {}",
                msg
            );
        }
    }

    #[test]
    fn test_local_timeout_keys_accept_the_boundary_value() {
        let at_limit = format!(
            "[timeouts]\nread_timeout_secs = {}\n",
            MAX_LOCAL_TIMEOUT_SECS
        );
        let config = load_config(&at_limit).expect("the boundary value must be accepted");
        assert_eq!(
            config.timeouts.resolved_read_timeout_secs(),
            MAX_LOCAL_TIMEOUT_SECS
        );
    }

    #[test]
    fn test_local_timeout_keys_keep_the_non_positive_fallback() {
        // The upper bound must not turn the documented "non-positive falls
        // back to the default" rule into a rejection.
        let config = load_config("[timeouts]\nread_timeout_secs = -5\n")
            .expect("a non-positive timeout still falls back");
        assert_eq!(
            config.timeouts.resolved_read_timeout_secs(),
            DEFAULT_READ_TIMEOUT_SECS
        );
    }

    #[test]
    fn test_concurrency_keys_reject_values_above_their_ceilings() {
        let workers = format!(
            "[concurrency]\ndownload_workers = {}\n",
            MAX_DOWNLOAD_WORKERS + 1
        );
        let err = load_config(&workers).expect_err("over-limit worker count must be rejected");
        assert!(
            err.to_string().contains("download_workers"),
            "error must name the key, got: {}",
            err
        );

        let depth = format!(
            "[concurrency]\ndownload_queue_depth = {}\n",
            MAX_DOWNLOAD_QUEUE_DEPTH + 1
        );
        let err = load_config(&depth).expect_err("over-limit queue depth must be rejected");
        assert!(
            err.to_string().contains("download_queue_depth"),
            "error must name the key, got: {}",
            err
        );
    }

    #[test]
    fn test_concurrency_keys_accept_their_ceilings_and_zero() {
        let at_ceiling = format!(
            "[concurrency]\ndownload_workers = {}\ndownload_queue_depth = {}\n",
            MAX_DOWNLOAD_WORKERS, MAX_DOWNLOAD_QUEUE_DEPTH
        );
        load_config(&at_ceiling).expect("the ceiling values must be accepted");

        // Zero keeps its documented "use the default" meaning.
        let zeros = load_config("[concurrency]\ndownload_workers = 0\ndownload_queue_depth = 0\n")
            .expect("zero must still mean the default");
        assert_eq!(zeros.concurrency.download_workers, Some(0));
    }
}

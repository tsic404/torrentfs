//! Tests for the --config parameter (TSI-1949, scenario 10).
//!
//! Validates the full config loading pipeline:
//! 1. TOML parsing succeeds with all sections
//! 2. Non-default values are correctly parsed
//! 3. JSON serialization for libtorrent FFI works
//! 4. Config file missing/broken yields appropriate errors

use std::io::Write;
use torrentfs::TorrentfsConfig;

/// Helper: write a TOML string to a temp file and load it.
fn load_config_from_str(toml_content: &str) -> Result<TorrentfsConfig, String> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    write!(file, "{}", toml_content).map_err(|e| e.to_string())?;
    TorrentfsConfig::from_file(file.path()).map_err(|e| e.to_string())
}

#[test]
fn test_load_default_config_is_all_none() {
    let cfg = TorrentfsConfig::default_config();
    // All fields should be None (default)
    assert!(cfg.connections.listen_interfaces.is_none());
    assert!(cfg.connections.max_connections.is_none());
    assert!(cfg.dht.enabled.is_none());
    assert!(cfg.cache.cache_size.is_none());
    assert!(cfg.timeouts.read_timeout_secs.is_none());
}

#[test]
fn test_load_config_with_connections() {
    let toml = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 200
allow_multiple_connections_per_ip = true
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(
        cfg.connections.listen_interfaces,
        Some("0.0.0.0:6881".to_string())
    );
    assert_eq!(cfg.connections.max_connections, Some(200));
    assert_eq!(
        cfg.connections.allow_multiple_connections_per_ip,
        Some(true)
    );
}

#[test]
fn test_load_config_with_dht_disabled() {
    let toml = r#"
[dht]
enabled = false
max_dht_items = 500
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.dht.enabled, Some(false));
    assert_eq!(cfg.dht.max_dht_items, Some(500));
}

#[test]
fn test_load_config_with_rate_limits() {
    let toml = r#"
[rate_limits]
download_rate_limit = 1048576
upload_rate_limit = 524288
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.rate_limits.download_rate_limit, Some(1048576));
    assert_eq!(cfg.rate_limits.upload_rate_limit, Some(524288));
}

#[test]
fn test_load_config_with_cache() {
    let toml = r#"
[cache]
cache_size = 67108864
cache_expiry = 3600
use_read_cache = true
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.cache.cache_size, Some(67108864));
    assert_eq!(cfg.cache.cache_expiry, Some(3600));
    assert_eq!(cfg.cache.use_read_cache, Some(true));
}

#[test]
fn test_load_config_with_timeouts() {
    let toml = r#"
[timeouts]
read_timeout_secs = 60
peer_timeout = 120
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.timeouts.read_timeout_secs, Some(60));
    assert_eq!(cfg.timeouts.peer_timeout, Some(120));
}

#[test]
fn test_load_config_with_disk_io() {
    let toml = r#"
[disk_io]
disk_io_write_mode = 1
disk_io_read_mode = 1
file_pool_size = 40
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.disk_io.disk_io_write_mode, Some(1));
    assert_eq!(cfg.disk_io.disk_io_read_mode, Some(1));
    assert_eq!(cfg.disk_io.file_pool_size, Some(40));
}

#[test]
fn test_load_config_with_proxy_type() {
    let toml = r#"
[proxy]
type = "socks5"
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load proxy config");
    assert_eq!(cfg.proxy.proxy_type, Some("socks5".to_string()));
}

#[test]
fn test_load_config_with_proxy_type_alias() {
    let toml = r#"
[proxy]
proxy_type = "socks5"
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load proxy alias config");
    assert_eq!(cfg.proxy.proxy_type, Some("socks5".to_string()));
}

#[test]
fn test_proxy_type_serializes_to_settings_pack_key() {
    let toml = r#"
[proxy]
type = "socks5"
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load proxy config");
    let json = cfg.to_settings_json();
    assert!(
        json.contains("\"proxy_type\":\"socks5\""),
        "settings JSON must use libtorrent key proxy_type, got: {}",
        json
    );
}

#[test]
fn test_load_config_with_multiple_sections() {
    let toml = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 100
allow_multiple_connections_per_ip = true

[dht]
enabled = false

[local_discovery]
lsd_enabled = true
upnp_enabled = false
natpmp_enabled = false

[rate_limits]
download_rate_limit = 1048576
upload_rate_limit = 524288

[cache]
cache_size = 67108864

[timeouts]
read_timeout_secs = 60

[disk_io]
disk_io_write_mode = 1
disk_io_read_mode = 1
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load multi-section config");

    // Verify connections
    assert_eq!(
        cfg.connections.listen_interfaces,
        Some("0.0.0.0:6881".to_string())
    );
    assert_eq!(cfg.connections.max_connections, Some(100));
    assert_eq!(
        cfg.connections.allow_multiple_connections_per_ip,
        Some(true)
    );

    // Verify DHT
    assert_eq!(cfg.dht.enabled, Some(false));

    // Verify local discovery
    assert_eq!(cfg.local_discovery.lsd_enabled, Some(true));
    assert_eq!(cfg.local_discovery.upnp_enabled, Some(false));
    assert_eq!(cfg.local_discovery.natpmp_enabled, Some(false));

    // Verify rate limits
    assert_eq!(cfg.rate_limits.download_rate_limit, Some(1048576));
    assert_eq!(cfg.rate_limits.upload_rate_limit, Some(524288));

    // Verify cache
    assert_eq!(cfg.cache.cache_size, Some(67108864));

    // Verify timeouts
    assert_eq!(cfg.timeouts.read_timeout_secs, Some(60));

    // Verify disk IO
    assert_eq!(cfg.disk_io.disk_io_write_mode, Some(1));
    assert_eq!(cfg.disk_io.disk_io_read_mode, Some(1));
}

#[test]
fn test_load_config_empty_file_defaults() {
    let toml = "";
    let cfg = load_config_from_str(toml).expect("Empty config should load with defaults");
    // Empty TOML should produce all-None config (same as default)
    assert!(cfg.connections.max_connections.is_none());
    assert!(cfg.dht.enabled.is_none());
    assert!(cfg.cache.cache_size.is_none());
}

#[test]
fn test_load_config_invalid_toml_returns_error() {
    let toml = "this is not valid toml {{{";
    let result = load_config_from_str(toml);
    assert!(result.is_err(), "Invalid TOML should return error");
}

#[test]
fn test_load_config_nonexistent_file_returns_error() {
    let result = TorrentfsConfig::from_file(std::path::Path::new("/nonexistent/config/path.toml"));
    assert!(result.is_err(), "Nonexistent file should return error");
}

/// TSI-2394: the CLI must expose `--config-check` so the container entrypoint
/// can fail fast on an invalid --config instead of reaching the FUSE mount
/// stage. Exit code contract: 0 = valid, non-zero = invalid/missing.
#[test]
fn test_config_check_flag_rejects_invalid_toml() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("bad.toml");
    std::fs::write(&config_path, "this is not valid toml {{{").unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "invalid TOML must exit non-zero via --config-check"
    );
}

#[test]
fn test_config_check_flag_accepts_valid_toml() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("good.toml");
    std::fs::write(&config_path, "[timeouts]\nread_timeout_secs = 60\n").unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        out.status.success(),
        "valid TOML must exit 0 via --config-check: {}",
        out.status
    );
}

#[test]
fn test_config_check_flag_requires_config_option() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .arg("--config-check")
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "--config-check without --config must be a usage error"
    );
}

/// TSI-2490: `--config-check` must reject unknown keys and sections instead
/// of silently ignoring them (rc=0 with "config is valid").
#[test]
fn test_config_check_flag_rejects_unknown_section() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("unknown-section.toml");
    std::fs::write(&config_path, "[bogus_section]\nfoo = 1\n").unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "unknown section must exit non-zero via --config-check"
    );
}

#[test]
fn test_config_check_flag_rejects_unknown_key() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("unknown-key.toml");
    std::fs::write(&config_path, "[cache]\nmax_size = \"not-a-size\"\n").unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "unknown key in known section must exit non-zero via --config-check"
    );
}

#[test]
fn test_config_check_flag_rejects_invalid_value_type() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("bad-value.toml");
    std::fs::write(&config_path, "[cache]\ncache_size = \"not-a-size\"\n").unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "invalid value type must exit non-zero via --config-check"
    );
}

/// TSI-2494: `--config-check` must reject integer config fields whose value is
/// unambiguously invalid (negative counts, negative rate limits, values that
/// would truncate at the i32 FFI boundary), while preserving legal sentinel
/// values such as `rate_limit = 0` ("unlimited").
fn run_config_check(config_toml: &str) -> std::process::Output {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, config_toml).unwrap();

    std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs")
}

fn assert_rejected(out: &std::process::Output, case: &str) {
    assert!(
        !out.status.success(),
        "{} must exit non-zero via --config-check",
        case
    );
    // tracing's default subscriber writes to stdout, not stderr.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        stdout.contains("Invalid config value") || stdout.contains("Invalid config TOML"),
        "{}: output should explain the config error, got stdout={} stderr={}",
        case,
        stdout,
        stderr
    );
}

fn assert_accepted(out: &std::process::Output, case: &str) {
    assert!(
        out.status.success(),
        "{} must exit 0 via --config-check: {}",
        case,
        out.status
    );
}

#[test]
fn test_config_check_rejects_negative_max_connections() {
    assert_rejected(
        &run_config_check("[connections]\nmax_connections = -5\n"),
        "negative max_connections",
    );
}

#[test]
fn test_config_check_rejects_zero_max_connections() {
    assert_rejected(
        &run_config_check("[connections]\nmax_connections = 0\n"),
        "zero max_connections",
    );
}

#[test]
fn test_config_check_rejects_over_i32_max_connections() {
    // 2147483648 = i32::MAX + 1: parses as i64 but would truncate at the FFI
    // boundary, so `validate` must reject it (distinct from the serde i64
    // overflow path exercised below).
    assert_rejected(
        &run_config_check("[connections]\nmax_connections = 2147483648\n"),
        "max_connections over i32::MAX",
    );
}

// 9223372036854775808 = i64::MAX + 1: rejected by serde's i64 deserializer
// before `validate` ever runs. Kept to pin the over-i64 exit contract.
#[test]
fn test_config_check_rejects_over_i64_max_connections() {
    assert_rejected(
        &run_config_check("[connections]\nmax_connections = 9223372036854775808\n"),
        "max_connections over i64::MAX",
    );
}

#[test]
fn test_config_check_rejects_negative_download_rate_limit() {
    assert_rejected(
        &run_config_check("[rate_limits]\ndownload_rate_limit = -1\n"),
        "negative download_rate_limit",
    );
}

#[test]
fn test_config_check_rejects_over_i32_download_rate_limit() {
    assert_rejected(
        &run_config_check("[rate_limits]\ndownload_rate_limit = 2147483648\n"),
        "download_rate_limit over i32::MAX",
    );
}

#[test]
fn test_config_check_rejects_over_i32_upload_rate_limit() {
    assert_rejected(
        &run_config_check("[rate_limits]\nupload_rate_limit = 2147483648\n"),
        "upload_rate_limit over i32::MAX",
    );
}

#[test]
fn test_config_check_accepts_zero_download_rate_limit() {
    // libtorrent semantics: 0 = unlimited (settings_pack.hpp).
    assert_accepted(
        &run_config_check("[rate_limits]\ndownload_rate_limit = 0\n"),
        "download_rate_limit = 0 (unlimited)",
    );
}

#[test]
fn test_config_check_accepts_zero_upload_rate_limit() {
    assert_accepted(
        &run_config_check("[rate_limits]\nupload_rate_limit = 0\n"),
        "upload_rate_limit = 0 (unlimited)",
    );
}

#[test]
fn test_config_check_accepts_one_max_connections() {
    assert_accepted(
        &run_config_check("[connections]\nmax_connections = 1\n"),
        "max_connections = 1",
    );
}

#[test]
fn test_config_check_accepts_i32_max_download_rate_limit() {
    assert_accepted(
        &run_config_check("[rate_limits]\ndownload_rate_limit = 2147483647\n"),
        "download_rate_limit = i32::MAX",
    );
}

#[test]
fn test_config_check_accepts_i32_max_connections() {
    assert_accepted(
        &run_config_check("[connections]\nmax_connections = 2147483647\n"),
        "max_connections = i32::MAX",
    );
}

#[test]
fn test_config_to_settings_json() {
    let toml = r#"
[connections]
listen_interfaces = "0.0.0.0:6881"
max_connections = 100

[dht]
enabled = false
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");

    let json = cfg.to_settings_json();
    assert!(json.contains("listen_interfaces"));
    assert!(json.contains("0.0.0.0:6881"));
    assert!(json.contains("max_connections"));
    assert!(json.contains("100"));
    assert!(json.contains("enable_dht"));
    assert!(json.contains("false"));
}

#[test]
fn test_config_to_settings_json_default_is_empty() {
    let cfg = TorrentfsConfig::default_config();
    let json = cfg.to_settings_json();
    // Default config with all None should produce empty JSON object
    assert_eq!(json, "{}");
}

/// Regression test for TSI-2297: the config `cache.cache_size` value is
/// plumbed through to the CacheManager (previously hardcoded 1 GiB).
#[test]
fn test_engine_passes_cache_size_to_cache_manager() {
    use torrentfs::download::DownloadEngine;

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    config.cache.cache_size = Some(67_108_864); // 64 MiB
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let cm = engine.cache_manager();
    let cm = cm.lock().unwrap();
    assert_eq!(cm.max_cache_size(), 67_108_864);
    drop(cm);
    engine.shutdown();
}

/// Default config (cache_size unset) falls back to 1 GiB.
#[test]
fn test_engine_cache_size_defaults_to_1gib() {
    use torrentfs::download::DownloadEngine;

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let cm = engine.cache_manager();
    let cm = cm.lock().unwrap();
    assert_eq!(cm.max_cache_size(), 1024 * 1024 * 1024);
    drop(cm);
    engine.shutdown();
}

/// Non-positive cache_size falls back to 1 GiB (guard against misconfig).
#[test]
fn test_engine_cache_size_non_positive_falls_back() {
    use torrentfs::download::DownloadEngine;

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    config.cache.cache_size = Some(0);
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let cm = engine.cache_manager();
    let cm = cm.lock().unwrap();
    assert_eq!(cm.max_cache_size(), 1024 * 1024 * 1024);
    drop(cm);
    engine.shutdown();
}

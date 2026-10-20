//! Design acceptance case TC-config-01: starting without a config file uses
//! all defaults (`docs/03-acceptance.md` §G AC-60).
//!
//! Migrated from `tests/config_test.rs` (default/fallback assertions) and the
//! engine cache-size defaults.  The observable defaults: every config field is
//! unset (`None`), the offline `--config-check` accepts a default run, and the
//! engine falls back to the shipped defaults (1 GiB cache, 60s read timeout,
//! 30s discovery window, 15s no-seeder wait, no discovery cap, 256 download
//! queue).

use crate::common::local_test_config;
use std::io::Write;

pub const TC_ID: &str = "TC-config-01";

/// The default config carries `None` for every user-settable field — the
/// "upstream defaults + torrentfs defaults" baseline.
#[test]
fn tc_config_01_default_config_is_all_none() {
    let cfg = torrentfs::TorrentfsConfig::default_config();
    assert!(cfg.connections.listen_interfaces.is_none());
    assert!(cfg.connections.max_connections.is_none());
    assert!(cfg.dht.enabled.is_none());
    assert!(cfg.cache.cache_size.is_none());
    assert!(cfg.timeouts.read_timeout_secs.is_none());
}

/// `--config-check` with a default config file (all values unset) exits 0:
/// the defaults are legal.
#[test]
fn tc_config_01_default_config_passes_config_check() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("default.toml");
    let mut file = std::fs::File::create(&config_path).unwrap();
    writeln!(file, "# empty default config").unwrap();
    drop(file);

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(&config_path)
        .output()
        .expect("failed to run torrentfs");
    assert!(
        out.status.success(),
        "default config must pass --config-check: {}",
        String::from_utf8_lossy(&out.stdout)
    );
}

/// Unset cache_size falls back to the shipped 1 GiB default.
#[test]
fn tc_config_01_engine_cache_size_defaults_to_1gib() {
    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let cm = engine.cache_manager();
    let cm = cm.lock().unwrap();
    assert_eq!(cm.max_cache_size(), 1024 * 1024 * 1024);
    drop(cm);
    engine.shutdown();
}

/// Non-positive cache_size falls back to 1 GiB (guard against misconfig).
#[test]
fn tc_config_01_engine_cache_size_non_positive_falls_back() {
    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    config.cache.cache_size = Some(0);
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let cm = engine.cache_manager();
    let cm = cm.lock().unwrap();
    assert_eq!(cm.max_cache_size(), 1024 * 1024 * 1024);
    drop(cm);
    engine.shutdown();
}

/// The local test config (used by every engine test here) applies torrentfs
/// defaults for the read-wait semantics: 30s read timeout, no-seeder wait 15s.
/// Exercised through the engine's own resolution so the defaults stay pinned.
#[test]
fn tc_config_01_wait_timeouts_resolve_from_default_config() {
    let mut cfg = local_test_config();
    cfg.timeouts.read_timeout_secs = None;
    cfg.timeouts.peer_discovery_wait_secs = None;
    cfg.timeouts.peer_wait_cap_secs = None;
    cfg.timeouts.no_seeder_read_timeout_secs = None;
    assert_eq!(cfg.timeouts.resolved_read_timeout_secs(), 60);
    assert_eq!(cfg.timeouts.resolved_peer_discovery_wait_secs(), 30);
    assert_eq!(cfg.timeouts.resolved_peer_wait_cap_secs(), None);
    assert_eq!(cfg.timeouts.resolved_no_seeder_read_timeout_secs(), 15);
}

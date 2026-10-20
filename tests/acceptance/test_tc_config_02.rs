//! Design acceptance case TC-config-02: a specified config file takes effect
//! — the observable effects (listen address, local discovery, cache cap, wait
//! semantics) change with the config (`docs/03-acceptance.md` §G AC-61).
//!
//! Absorbed the overlapping cases from `tests/config_test.rs` (section
//! parsing for connections/dht/cache/timeouts) and `tests/settings_test.rs`
//! (custom-storage listen readback); those old copies are deleted, so each
//! criterion has one implementation.

use std::io::Write;

pub const TC_ID: &str = "TC-config-02";

/// `Session::new_with_custom_storage` builds the libtorrent session on a deep
/// C++ stack; run it on a worker with an explicit large stack (same pattern as
/// `tests/settings_test.rs`).
fn with_large_stack<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    std::thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

/// Helper: write a TOML string to a temp file and load it.
fn load_config_from_str(toml_content: &str) -> Result<torrentfs::TorrentfsConfig, String> {
    let mut file = tempfile::NamedTempFile::new().map_err(|e| e.to_string())?;
    write!(file, "{}", toml_content).map_err(|e| e.to_string())?;
    torrentfs::TorrentfsConfig::from_file(file.path()).map_err(|e| e.to_string())
}

#[test]
fn tc_config_02_connections_apply() {
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
fn tc_config_02_dht_apply() {
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
fn tc_config_02_cache_size_apply() {
    let toml = r#"
[cache]
cache_size = 67108864
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.cache.cache_size, Some(67108864));
}

#[test]
fn tc_config_02_engine_cache_size_reaches_cache_manager() {
    use torrentfs::download::DownloadEngine;

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = torrentfs::TorrentfsConfig::default_config();
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

#[test]
fn tc_config_02_wait_semantics_apply() {
    let toml = r#"
[timeouts]
read_timeout_secs = 90
peer_discovery_wait_secs = 45
peer_wait_cap_secs = 10
no_seeder_read_timeout_secs = 7
"#;
    let cfg = load_config_from_str(toml).expect("Failed to load config");
    assert_eq!(cfg.timeouts.resolved_read_timeout_secs(), 90);
    assert_eq!(cfg.timeouts.resolved_peer_discovery_wait_secs(), 45);
    assert_eq!(cfg.timeouts.resolved_peer_wait_cap_secs(), Some(10));
    assert_eq!(cfg.timeouts.resolved_no_seeder_read_timeout_secs(), 7);
}

/// The production daemon path (`new_with_custom_storage`) must carry the
/// configured listen interface and local-discovery switch through to the live
/// libtorrent session — the `.stats` "Listen" line's observable source and
/// AC-61's local-discovery observable.
#[test]
fn tc_config_02_listen_and_local_discovery_reach_libtorrent() {
    let dir = tempfile::TempDir::new().unwrap();
    let config_path = dir.path().join("listen.toml");
    std::fs::write(
        &config_path,
        "[connections]\nlisten_interfaces = \"0.0.0.0:0\"\n\
         [local_discovery]\nlsd_enabled = false\n",
    )
    .unwrap();
    let config = torrentfs::TorrentfsConfig::from_file(&config_path).unwrap();

    with_large_stack(move || {
        let session = torrentfs::Session::new_with_custom_storage(&config, dir.path())
            .expect("custom-storage session");
        assert_eq!(
            session
                .get_str_setting("listen_interfaces")
                .expect("listen_interfaces must be readable"),
            "0.0.0.0:0",
            "listen_interfaces must reach libtorrent on the production path"
        );
        assert!(
            !session
                .get_bool_setting("enable_lsd")
                .expect("enable_lsd must be readable"),
            "lsd_enabled = false must switch local discovery off in the live session"
        );

        // And the flipped switch takes effect as well, so the assertion is
        // not satisfied by an always-off default.
        let config_path = dir.path().join("lsd-on.toml");
        std::fs::write(&config_path, "[local_discovery]\nlsd_enabled = true\n").unwrap();
        let config = torrentfs::TorrentfsConfig::from_file(&config_path).unwrap();
        let session = torrentfs::Session::new_with_custom_storage(&config, dir.path())
            .expect("custom-storage session (lsd on)");
        assert!(
            session
                .get_bool_setting("enable_lsd")
                .expect("enable_lsd must be readable"),
            "lsd_enabled = true must switch local discovery on in the live session"
        );
    });
}

/// The root `.stats` renderer must show the configured listen address and
/// cache cap — AC-61's design-stated observable surface.  `generate_global_stats`
/// is the production renderer behind `/.stats`; feeding it the config-derived
/// inputs (listen address, engine cache manager) pins the exact lines a
/// mounted daemon publishes.
#[test]
fn tc_config_02_root_stats_reflects_listen_and_cache_cap() {
    use torrentfs::fuse::stats::generate_global_stats;

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.performance.aio_threads = Some(2);
    config.connections.listen_interfaces = Some("0.0.0.0:6881".to_string());
    config.cache.cache_size = Some(67_108_864); // 64 MiB
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config).unwrap();

    let listen_addr = config
        .connections
        .listen_interfaces
        .clone()
        .expect("listen_interfaces set above");
    let stats = generate_global_stats(
        std::time::Duration::from_secs(0),
        &None,
        Some(engine.snapshot_stats()),
        || Some(engine.cache_manager()),
        &listen_addr,
        None,
    );
    let text = String::from_utf8_lossy(&stats);
    assert!(
        text.contains("Listen:       0.0.0.0:6881"),
        "root .stats must publish the configured listen address:\n{}",
        text
    );
    assert!(
        text.contains("64.00 MB"),
        "root .stats cache cap must reflect [cache] cache_size (64 MiB):\n{}",
        text
    );

    engine.shutdown();
}

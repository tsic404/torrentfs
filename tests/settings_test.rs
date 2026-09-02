//! Integration tests for settings readback API (TSI-2013).
//!
//! Custom-storage tests validate that settings are correctly applied
//! when creating a session with PieceStorageDiskIO from the start
//! (new_with_custom_storage). CI runs on Debian Sid (libtorrent 2.1.x).

mod common;

use std::thread;
use torrentfs::{Session, TorrentfsConfig};

fn with_large_stack<F, R>(f: F) -> R
where
    F: FnOnce() -> R + Send + 'static,
    R: Send + 'static,
{
    thread::Builder::new()
        .stack_size(32 * 1024 * 1024)
        .spawn(f)
        .unwrap()
        .join()
        .unwrap()
}

fn non_default_config() -> TorrentfsConfig {
    let mut c = TorrentfsConfig::default_config();
    c.connections.allow_multiple_connections_per_ip = Some(true);
    c.dht.enabled = Some(false);
    c
}

fn assert_setting(session: &Session, key: &str, expected: bool) {
    let actual = session
        .get_bool_setting(key)
        .unwrap_or_else(|e| panic!("get_bool_setting({key}) failed: {e:?}"));
    assert_eq!(
        actual, expected,
        "setting '{key}' expected {expected}, got {actual}"
    );
}

#[test]
fn session_new_works() {
    let config = TorrentfsConfig::default_config();
    let _session = Session::new(&config).unwrap();
}

#[test]
fn get_bool_setting_with_explicit_config() {
    let config = non_default_config();
    let session = Session::new(&config).unwrap();
    assert_setting(&session, "allow_multiple_connections_per_ip", true);
    assert_setting(&session, "enable_dht", false);
    assert!(session.get_bool_setting("nonexistent_key").is_err());
}

fn assert_int_setting(session: &Session, key: &str, expected: i32) {
    let actual = session
        .get_int_setting(key)
        .unwrap_or_else(|e| panic!("get_int_setting({key}) failed: {e:?}"));
    assert_eq!(
        actual, expected,
        "setting '{key}' expected {expected}, got {actual}"
    );
}

#[test]
fn proxy_type_maps_to_libtorrent_proxy_type() {
    let mut config = TorrentfsConfig::default_config();
    config.proxy.proxy_type = Some("socks5".to_string());
    let session = Session::new(&config).unwrap();
    // settings_pack::proxy_type_t::socks5 == 2
    assert_int_setting(&session, "proxy_type", 2);
    assert!(session.get_int_setting("nonexistent_key").is_err());
}

/// TSI-2566: `proxy_type` must round-trip through the production
/// `new_with_custom_storage` path, not just `Session::new`. Both bake the
/// config JSON through the C wrapper's `build_settings_pack`, so the
/// readback must be asserted on the custom-storage path that the daemon
/// actually uses.
#[test]
fn proxy_type_maps_to_libtorrent_proxy_type_with_custom_storage() {
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let mut config = TorrentfsConfig::default_config();
        config.proxy.proxy_type = Some("socks5".to_string());
        let session = Session::new_with_custom_storage(&config, dir.path()).unwrap();
        // settings_pack::proxy_type_t::socks5 == 2
        assert_int_setting(&session, "proxy_type", 2);
    });
}

#[test]
fn proxy_host_port_reach_libtorrent() {
    let mut config = TorrentfsConfig::default_config();
    config.proxy.proxy_type = Some("socks5".to_string());
    config.proxy.host = Some("127.0.0.1".to_string());
    config.proxy.port = Some(1080);
    let session = Session::new(&config).unwrap();
    assert_int_setting(&session, "proxy_port", 1080);
    assert_eq!(
        session
            .get_str_setting("proxy_hostname")
            .expect("proxy_hostname must be readable"),
        "127.0.0.1"
    );
}

#[test]
fn peer_fingerprint_still_reaches_libtorrent() {
    // TSI-2538 regression: restoring the proxy_hostname branch must not
    // clobber the existing peer_fingerprint mapping (wrapper apply_str_setting).
    let mut config = TorrentfsConfig::default_config();
    config.user_agent.peer_fingerprint = Some("TS".to_string());
    let session = Session::new(&config).unwrap();
    assert_eq!(
        session
            .get_str_setting("peer_fingerprint")
            .expect("peer_fingerprint must be readable"),
        "TS"
    );
}

#[test]
fn settings_work_with_custom_storage_session() {
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let config = non_default_config();
        let session = Session::new_with_custom_storage(&config, dir.path()).unwrap();
        assert_setting(&session, "allow_multiple_connections_per_ip", true);
        assert_setting(&session, "enable_dht", false);
        // TSI-2467: close_redundant_connections must be false in custom
        // storage sessions to prevent seed peer disconnection when
        // torrent_finished fires prematurely during selective downloading.
        assert_setting(&session, "close_redundant_connections", false);
    });
}

/// Regression test for TSI-2042: verify that an unwritable cache directory
/// causes session creation to fail gracefully instead of SIGSEGV.
///
/// Uses a file-as-directory-blocker: create a regular file at a path
/// component inside the cache dir so that ensure_dir_recursive() fails
/// with ENOTDIR.  This works under root (CAP_DAC_OVERRIDE doesn't help
/// against ENOTDIR) and unprivileged users alike.
#[test]
fn custom_storage_readonly_dir_rejected() {
    let dir = tempfile::TempDir::new().unwrap();

    // Place a regular file where a directory component of the cache path
    // would be.  ensure_dir_recursive will fail because mkdir(2) on a
    // path whose prefix is a regular file returns ENOTDIR.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"").unwrap();

    let cache_dir = blocker.join("pieces");

    with_large_stack(move || {
        let config = TorrentfsConfig::default_config();
        let result = Session::new_with_custom_storage(&config, &cache_dir);
        assert!(
            result.is_err(),
            "Expected Session::new_with_custom_storage to fail when a path component is a regular file"
        );
    });
}

/// TSI-2467: When the user explicitly sets close_redundant_connections=true
/// in config, the custom storage session must respect it instead of
/// forcing false. The default (unset) injects false to prevent seed
/// peer disconnection during premature torrent_finished.
#[test]
fn close_redundant_connections_user_override_respected() {
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let mut config = TorrentfsConfig::default_config();
        config.misc.close_redundant_connections = Some(true);
        let session = Session::new_with_custom_storage(&config, dir.path()).unwrap();
        assert_setting(&session, "close_redundant_connections", true);
    });
}

/// TSI-2529: a configured `proxy_type` string must survive the C wrapper's
/// `apply_str_setting` mapping and land in the live session as the matching
/// `proxy_type_t` value — not be silently dropped as an unknown string key.
#[test]
fn proxy_type_applies_to_session() {
    let mut cases: Vec<(&str, i32)> = vec![
        ("socks4", 1),
        ("socks5", 2),
        ("socks5_pw", 3),
        ("http", 4),
        ("http_pw", 5),
    ];
    if unsafe { libtorrent_sys::lt_torrent_i2p_enabled() } != 0 {
        cases.push(("i2p_proxy", 6));
    }
    for (kind, expected) in cases {
        let mut config = TorrentfsConfig::default_config();
        config.proxy.proxy_type = Some(kind.to_string());
        let session = Session::new(&config).unwrap();
        let actual = session
            .get_int_setting("proxy_type")
            .unwrap_or_else(|e| panic!("get_int_setting(proxy_type) failed: {e:?}"));
        assert_eq!(
            actual, expected,
            "proxy_type={kind} must map to {expected}, got {actual}"
        );
    }
}

/// TSI-2798: `listen_interfaces` / `outgoing_interfaces` are wired to the
/// settings_pack (connections.rs → JSON → wrapper `apply_str_setting`) but
/// previously had no readback path, so the wiring could never be verified.
/// Read them back from the live session to prove both settings reach libtorrent.
#[test]
fn listen_and_outgoing_interfaces_reach_libtorrent() {
    let mut config = TorrentfsConfig::default_config();
    config.connections.listen_interfaces = Some("0.0.0.0:6881".to_string());
    config.connections.outgoing_interfaces = Some("10.20.33.70".to_string());
    let session = Session::new(&config).unwrap();
    assert_eq!(
        session
            .get_str_setting("listen_interfaces")
            .expect("listen_interfaces must be readable"),
        "0.0.0.0:6881"
    );
    assert_eq!(
        session
            .get_str_setting("outgoing_interfaces")
            .expect("outgoing_interfaces must be readable"),
        "10.20.33.70"
    );
}

/// TSI-2798: same readback assertion on the production path — the daemon
/// creates sessions via `new_with_custom_storage`, which bakes settings into
/// `session_params` on the C++ side rather than applying them post-hoc.
#[test]
fn listen_and_outgoing_interfaces_reach_libtorrent_with_custom_storage() {
    let dir = tempfile::TempDir::new().unwrap();
    with_large_stack(move || {
        let mut config = TorrentfsConfig::default_config();
        config.connections.listen_interfaces = Some("0.0.0.0:6881".to_string());
        config.connections.outgoing_interfaces = Some("10.20.33.70".to_string());
        let session = Session::new_with_custom_storage(&config, dir.path()).unwrap();
        assert_eq!(
            session
                .get_str_setting("listen_interfaces")
                .expect("listen_interfaces must be readable"),
            "0.0.0.0:6881"
        );
        assert_eq!(
            session
                .get_str_setting("outgoing_interfaces")
                .expect("outgoing_interfaces must be readable"),
            "10.20.33.70"
        );
    });
}

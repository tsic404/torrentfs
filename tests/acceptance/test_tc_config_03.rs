//! Design acceptance case TC-config-03: invalid configuration fails fast —
//! non-zero exit with an error naming the cause, identically for direct start
//! and offline `--config-check` (`docs/03-acceptance.md` §G AC-62).
//!
//! Migrated from `tests/config_test.rs::test_config_check_*` — the five
//! invalid families (bad TOML, unknown section, unknown key, out-of-range
//! value, invalid enum) must all exit non-zero via `--config-check` with a
//! diagnostic naming the reason.

pub const TC_ID: &str = "TC-config-03";

use torrentfs::config::{MAX_DOWNLOAD_QUEUE_DEPTH, MAX_DOWNLOAD_WORKERS, MAX_LOCAL_TIMEOUT_SECS};

fn logs_of(out: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

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
    // tracing's default subscriber writes to stdout, not stderr; the
    // diagnostic must name the config as the failure cause.
    let stdout = String::from_utf8_lossy(&out.stdout);
    let stderr = String::from_utf8_lossy(&out.stderr);
    let logs = format!("{}{}", stdout, stderr);
    assert!(
        logs.contains("Invalid configuration file") || logs.contains("Invalid config"),
        "{}: output should explain the config error, got stdout={} stderr={}",
        case,
        stdout,
        stderr
    );
}

#[test]
fn tc_config_03_rejects_invalid_toml() {
    let out = run_config_check("this is not valid toml {{{");
    assert_rejected(&out, "invalid TOML");
}

#[test]
fn tc_config_03_rejects_unknown_section() {
    let out = run_config_check("[bogus_section]\nfoo = 1\n");
    assert_rejected(&out, "unknown section");
}

#[test]
fn tc_config_03_rejects_unknown_key() {
    let out = run_config_check("[cache]\nmax_size = \"not-a-size\"\n");
    assert_rejected(&out, "unknown key");
}

#[test]
fn tc_config_03_rejects_invalid_value_type() {
    let out = run_config_check("[cache]\ncache_size = \"not-a-size\"\n");
    assert_rejected(&out, "invalid value type");
}

#[test]
fn tc_config_03_rejects_out_of_range_enum() {
    // enum fields must reject out-of-domain integers via `--config-check`
    // instead of silently forwarding them to libtorrent.
    let out = run_config_check("[algorithms]\nchoking_algorithm = 999\n");
    assert_rejected(&out, "out-of-range choking_algorithm");
}

#[test]
fn tc_config_03_rejects_negative_max_connections() {
    let out = run_config_check("[connections]\nmax_connections = -1\n");
    assert_rejected(&out, "negative max_connections");
}

/// `--config-check` without `--config` is a usage error.
#[test]
fn tc_config_03_requires_config_option() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .arg("--config-check")
        .output()
        .expect("failed to run torrentfs");
    assert!(
        !out.status.success(),
        "--config-check without --config must be a usage error"
    );
}

/// Every torrentfs-level timeout key has an upper bound: a value past it is an
/// out-of-range value, rejected with the key and the allowed range named.
#[test]
fn tc_config_03_rejects_timeout_above_the_ceiling() {
    for key in [
        "read_timeout_secs",
        "peer_discovery_wait_secs",
        "peer_wait_cap_secs",
        "no_seeder_read_timeout_secs",
    ] {
        let over = format!("[timeouts]\n{} = {}\n", key, MAX_LOCAL_TIMEOUT_SECS + 1);
        let out = run_config_check(&over);
        assert_rejected(&out, key);
        let logs = logs_of(&out);
        assert!(
            logs.contains(key),
            "{key}: the diagnostic must name the key, got {logs}"
        );
        assert!(
            logs.contains(&MAX_LOCAL_TIMEOUT_SECS.to_string()),
            "{key}: the diagnostic must state the allowed range, got {logs}"
        );
    }
}

/// The concurrency keys are likewise bounded: an absurd worker count or queue
/// depth must fail at load time, not at thread/channel allocation.
#[test]
fn tc_config_03_rejects_concurrency_above_the_ceiling() {
    for (key, over) in [
        ("download_workers", MAX_DOWNLOAD_WORKERS + 1),
        ("download_queue_depth", MAX_DOWNLOAD_QUEUE_DEPTH + 1),
    ] {
        let toml = format!("[concurrency]\n{} = {}\n", key, over);
        let out = run_config_check(&toml);
        assert_rejected(&out, key);
        assert!(
            logs_of(&out).contains(key),
            "{key}: the diagnostic must name the key"
        );
    }
}

/// The bound itself is legal — the rejection starts one past it.
#[test]
fn tc_config_03_accepts_the_configured_ceilings() {
    let at_limit = format!(
        "[timeouts]\nread_timeout_secs = {limit}\npeer_discovery_wait_secs = {limit}\n\
         peer_wait_cap_secs = {limit}\nno_seeder_read_timeout_secs = {limit}\n\
         [concurrency]\ndownload_workers = {workers}\ndownload_queue_depth = {depth}\n",
        limit = MAX_LOCAL_TIMEOUT_SECS,
        workers = MAX_DOWNLOAD_WORKERS,
        depth = MAX_DOWNLOAD_QUEUE_DEPTH,
    );
    let out = run_config_check(&at_limit);
    assert!(
        out.status.success(),
        "the ceiling values must be accepted, got {}",
        logs_of(&out)
    );
}

/// A direct start (not the precheck) rejects the same out-of-range timeout, so
/// both entry points share one loader and one exit status.
#[test]
fn tc_config_03_direct_start_rejects_timeout_above_the_ceiling() {
    let dir = tempfile::TempDir::new().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(
        &config,
        format!(
            "[timeouts]\nread_timeout_secs = {}\n",
            MAX_LOCAL_TIMEOUT_SECS + 1
        ),
    )
    .unwrap();
    let mountpoint = dir.path().join("mnt");
    std::fs::create_dir_all(&mountpoint).unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .arg("--config")
        .arg(&config)
        .arg(&mountpoint)
        .output()
        .expect("failed to run torrentfs");

    let logs = logs_of(&out);
    assert_eq!(
        out.status.code(),
        Some(1),
        "a rejected config must exit 1 (C-1.2.2), got {logs}"
    );
    assert!(
        logs.contains("read_timeout_secs"),
        "the diagnostic must name the offending key, got {logs}"
    );
    assert!(
        !logs.contains("panicked"),
        "an out-of-range timeout must not panic, got {logs}"
    );
}

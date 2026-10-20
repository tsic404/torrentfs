//! Design acceptance case TC-config-03: invalid configuration fails fast —
//! non-zero exit with an error naming the cause, identically for direct start
//! and offline `--config-check` (`docs/03-acceptance.md` §G AC-62).
//!
//! Migrated from `tests/config_test.rs::test_config_check_*` — the five
//! invalid families (bad TOML, unknown section, unknown key, out-of-range
//! value, invalid enum) must all exit non-zero via `--config-check` with a
//! diagnostic naming the reason.

pub const TC_ID: &str = "TC-config-03";

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

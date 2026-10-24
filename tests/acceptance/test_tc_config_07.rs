//! Design acceptance case TC-config-07: the peer-discovery cap key bounds the
//! effective window and the diagnostic names the knob that really constrains
//! it (`docs/03-acceptance.md` §G AC-66; `docs/02-contracts.md` §C-4.2.4,
//! §C-4.4.1).
//!
//! Each case drives a sourceless read on a real engine with an unreachable
//! tracker: the wall time proves which window was spent, the returned
//! `NoPeers` text carries the window's composition, and the **daemon error
//! stream** (fd 2, where C-4.4.1 places the frozen advice) is asserted from a
//! child process.  The returned text and the error stream are different
//! surfaces: only the former carries the `min(...)` formula.
//!
//! The capped case also drives a second read on the same engine.  The first
//! read leaves the AC-19 sourceless verdict behind, so the second one settles
//! in milliseconds without waiting; C-4.3.7 requires that inherited settlement
//! to reuse the discovery-window text, which is the fast path a missing
//! write would silently turn into "ENODATA with no recorded cause".
//!
//! Timeouts load through the shipped file loader (`--config`'s path), not by
//! assigning struct fields: the criterion is about the key a deployment
//! configures, so a key that never reaches the engine fails the elapsed-time
//! assertions here.

use std::fs::{File, read_to_string};
use std::io::Write;
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::common::base::create_single_piece_torrent;
use crate::common::{acquire_session_lock, wave_slot};
use torrentfs::TorrentError;
use torrentfs::download::DownloadEngine;

pub const TC_ID: &str = "TC-config-07";

/// Set in the child spawn so the child body performs the reads (unique to this
/// TC, so no other test's body can pick it up).
const CHILD_ENV: &str = "TORRENTFS_TC_CONFIG_07_CHILD";
const TEST_NAME: &str = "test_tc_config_07::tc_config_07_cap_bounds_the_window_and_the_error_stream";

/// The cap case's effective window, shared by the child's assertions and the
/// parent's stderr expectations.
const CAP_SECS: u64 = 2;

#[test]
fn tc_config_07_cap_bounds_the_window_and_the_error_stream() {
    if std::env::var_os(CHILD_ENV).is_some() {
        exercise_sourceless_reads();
        return;
    }

    let dir = tempfile::TempDir::new().expect("Failed to create log dir");
    let log_path = dir.path().join("daemon.log");
    let status = Command::new(std::env::current_exe().expect("test binary path"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            File::create(&log_path).expect("Failed to create daemon log"),
        ))
        .status()
        .expect("Failed to spawn the stderr probe");
    let stderr = read_to_string(&log_path).unwrap_or_default();
    assert!(
        status.success(),
        "the stderr probe exited with {status}:\n{stderr}"
    );

    // C-4.4.1: the capped discovery window is frozen as
    // `within the {secs}s peer-discovery window; {advice}`, and the advice
    // must name the cap — the term that actually binds.
    let capped = lines_starting_with(
        &stderr,
        "no seeder connected (Peers:0 Seeds:0) within the 2s peer-discovery window; ",
    );
    assert!(
        capped.iter().any(|line| line.contains(
            "raise [timeouts] peer_wait_cap_secs (it caps the window) and retry"
        )),
        "the capped read must write the registered cause naming the binding \
         cap to the daemon error stream:\n{stderr}"
    );
    assert!(
        capped.iter().all(|line| !line.contains("raise [timeouts] peer_discovery_wait_secs")),
        "a capped window must not send the operator to a non-binding knob:\n{stderr}"
    );
    // The probe spends the window and the read that follows inherits its
    // verdict: both settlements must leave the line (C-4.3.7), so a missing
    // write on the fast path cannot pass as "no eligible cause".
    assert!(
        capped.len() >= 2,
        "the probe and the inherited read must each write the frozen cause; \
         got {} line(s):\n{stderr}",
        capped.len()
    );

    // Unset and non-positive caps fall back to the two-way min and name the
    // discovery wait; the cap key must not appear anywhere in their advice.
    let uncapped = lines_starting_with(
        &stderr,
        "no seeder connected (Peers:0 Seeds:0) within the 1s peer-discovery window; ",
    );
    assert!(
        uncapped.len() >= 2,
        "the unset-cap and non-positive-cap reads must both write the \
         discovery-window cause; got {} line(s):\n{stderr}",
        uncapped.len()
    );
    for line in &uncapped {
        assert!(
            line.contains("raise [timeouts] peer_discovery_wait_secs and retry"),
            "an uncapped window is bound by the discovery wait, not the cap: {line:?}"
        );
        assert!(
            !line.contains("peer_wait_cap_secs"),
            "an unset or non-positive cap is not a window term: {line:?}"
        );
    }

    // The `min(...)` formula belongs to the returned text (C-4.4.1's frozen
    // branch text does not carry it); asserting it on the error stream would
    // be a different, invented criterion.
    assert!(
        !stderr.lines().any(|line| line.contains("min(")),
        "the frozen error-stream text must not be expected to carry the \
         formula that lives in the returned text:\n{stderr}"
    );
}

fn lines_starting_with<'a>(log: &'a str, prefix: &str) -> Vec<&'a str> {
    log.lines().filter(|line| line.starts_with(prefix)).collect()
}

/// Child body: drive the sourceless reads; the parent asserts the stderr side.
fn exercise_sourceless_reads() {
    let _slot = wave_slot();

    capped_reads_share_one_window();
    unset_cap_does_not_bound();
    nonpositive_cap_does_not_bound();
}

/// One engine, two reads: the first spends the capped window, the second
/// inherits its verdict and settles without waiting.  Both must report the
/// same window composition, and the inherited one must still name the cap.
fn capped_reads_share_one_window() {
    let _guard = acquire_session_lock();
    let info = no_seeder_torrent("tc-config-07-capped!!");

    let config = config_from_toml(&format!(
        "[timeouts]\nread_timeout_secs = 60\npeer_discovery_wait_secs = 30\n\
         peer_wait_cap_secs = {CAP_SECS}\n"
    ));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");

    let start = Instant::now();
    let first = engine.read_file_range(info.clone(), 0, 0, 50);
    let elapsed = start.elapsed();
    assert!(
        elapsed >= Duration::from_millis(1800),
        "the capped window must actually be spent, returned in {elapsed:?}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "the cap ({CAP_SECS}s) must bound the window well below the 30s \
         discovery wait; the read returned in {elapsed:?}"
    );
    let first = expect_no_peers(first);
    assert!(
        first.contains(
            "window 2s = min(read_timeout_secs, peer_discovery_wait_secs, \
             peer_wait_cap_secs)"
        ),
        "the diagnostic must state the three-way composition: {first:?}"
    );
    assert!(
        first.contains("raise [timeouts] peer_wait_cap_secs (it caps the window)"),
        "the advice must name the cap, the term actually binding: {first:?}"
    );
    assert!(
        !first.contains("raise [timeouts] peer_discovery_wait_secs"),
        "the advice must not send the operator to a non-binding knob: {first:?}"
    );

    // The verdict is fresh (its TTL is the window), so this read must settle
    // without a second wait — and still report the same frozen composition.
    let start = Instant::now();
    let inherited = engine.read_file_range(info, 0, 0, 50);
    let inherited_elapsed = start.elapsed();
    assert!(
        inherited_elapsed < Duration::from_secs(1),
        "a read covered by the live sourceless verdict must not spend the \
         window again; it returned in {inherited_elapsed:?}"
    );
    let inherited = expect_no_peers(inherited);
    assert!(
        inherited.contains(
            "window 2s = min(read_timeout_secs, peer_discovery_wait_secs, \
             peer_wait_cap_secs)"
        ),
        "the inherited settlement must reuse the discovery-window text: {inherited:?}"
    );
    assert!(
        inherited.contains("raise [timeouts] peer_wait_cap_secs (it caps the window)"),
        "the inherited settlement must also name the binding cap: {inherited:?}"
    );
    engine.shutdown();
}

/// Unset cap: the window falls back to the two-way min, and the advice names
/// the discovery wait instead of the absence of the cap.
fn unset_cap_does_not_bound() {
    let _guard = acquire_session_lock();
    let info = no_seeder_torrent("tc-config-07-uncapped!!");

    let config = config_from_toml(
        "[timeouts]\nread_timeout_secs = 60\npeer_discovery_wait_secs = 1\n",
    );

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");
    let start = Instant::now();
    let result = engine.read_file_range(info, 0, 0, 50);
    let elapsed = start.elapsed();
    engine.shutdown();
    assert!(
        elapsed >= Duration::from_millis(800) && elapsed < Duration::from_secs(20),
        "without a cap the window must be the discovery wait (1s), not the 60s \
         read timeout; the read returned in {elapsed:?}"
    );
    let message = expect_no_peers(result);
    assert!(
        message.contains("window 1s = min(read_timeout_secs, peer_discovery_wait_secs)"),
        "an unset cap must not appear in the composition: {message:?}"
    );
    assert!(
        message.contains("raise [timeouts] peer_discovery_wait_secs and retry"),
        "the advice must name the binding discovery wait: {message:?}"
    );
    assert!(
        !message.contains("peer_wait_cap_secs"),
        "an unset cap is not a window term: {message:?}"
    );
}

/// A non-positive cap is the same as unset — it must not contribute a term or
/// produce a zero window.
fn nonpositive_cap_does_not_bound() {
    let _guard = acquire_session_lock();
    let info = no_seeder_torrent("tc-config-07-nonpositive!!");

    let config = config_from_toml(
        "[timeouts]\nread_timeout_secs = 60\npeer_discovery_wait_secs = 1\n\
         peer_wait_cap_secs = 0\n",
    );

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");
    let start = Instant::now();
    let result = engine.read_file_range(info, 0, 0, 50);
    let elapsed = start.elapsed();
    engine.shutdown();
    assert!(
        elapsed >= Duration::from_millis(800) && elapsed < Duration::from_secs(20),
        "a non-positive cap must neither cap nor zero the window; the read \
         returned in {elapsed:?}"
    );
    let message = expect_no_peers(result);
    assert!(
        message.contains("window 1s = min(read_timeout_secs, peer_discovery_wait_secs)"),
        "a non-positive cap must not appear in the composition: {message:?}"
    );
    assert!(
        !message.contains("peer_wait_cap_secs"),
        "a non-positive cap is not a window term: {message:?}"
    );
}

fn expect_no_peers(result: Result<Vec<u8>, TorrentError>) -> String {
    match result {
        Err(TorrentError::NoPeers(message)) => message,
        other => panic!("a sourceless read must fail with NoPeers, got {other:?}"),
    }
}

fn no_seeder_torrent(name: &str) -> Arc<torrentfs::TorrentInfo> {
    let (torrent_data, _content) = create_single_piece_torrent("http://127.0.0.1:1/announce", name);
    Arc::new(torrentfs::TorrentInfo::from_bytes(torrent_data).expect("parse torrent"))
}

/// Load `text` through the shipped file loader — read → parse → `validate()`
/// → `validate_enum_ranges()` — so the key under test is the one a deployment
/// configures through `--config`, not a struct field assigned after the fact.
/// A section or key the daemon never reads leaves the window uncapped here,
/// and the elapsed-time assertions fail.
fn config_from_toml(text: &str) -> torrentfs::TorrentfsConfig {
    let mut file = tempfile::NamedTempFile::new().expect("temp config file");
    file.write_all(text.as_bytes()).expect("write temp config");
    let mut config =
        torrentfs::TorrentfsConfig::from_file(file.path()).expect("test config must load");
    // Keep the fixture hermetic: no ambient discovery can leak a peer into a
    // swarm the read must find empty.
    config.local_discovery.lsd_enabled = Some(false);
    config.dht.enabled = Some(false);
    config
}

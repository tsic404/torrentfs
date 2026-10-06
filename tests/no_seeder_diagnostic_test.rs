//! The contract channel for a failed read: the daemon's own stderr.
//!
//! `03-contracts.md` §4 freezes the two read-failure prefixes and requires them
//! written to the daemon's own stderr (`writeln!(std::io::stderr(), …)`, not a
//! tracing event).  That is the only stream both a direct run's
//! `2> daemon.log` and a container's `docker logs` reach unchanged, and it is
//! what TC-read-05 greps (`grep -c 'no seeder connected (Peers:0 Seeds:0)'`).
//! Nothing asserted the line end to end before: this test is that assertion as
//! code, on the real read path, so a regression that drops it or reformats it
//! fails CI instead of only a container acceptance run.
//!
//! The read runs in a child of this test binary with its stderr pointed at a
//! file: fd 2 is process-wide, so asserting in-process would mean redirecting
//! the whole test binary's stderr around the read.

mod common;

use std::fs::{File, read_to_string};
use std::process::{Command, Stdio};
use std::sync::Arc;

use common::{acquire_session_lock, create_single_piece_torrent, local_test_config};
use torrentfs::download::DownloadEngine;

/// Set in the child spawn so this test's body performs the read (and any other
/// test that ever reads this variable would still run its own body).
const CHILD_ENV: &str = "TORRENTFS_NO_SEEDER_DIAGNOSTIC_CHILD";
const TEST_NAME: &str = "no_seeder_read_leaves_the_frozen_prefix_on_the_daemon_stderr";

#[test]
fn no_seeder_read_leaves_the_frozen_prefix_on_the_daemon_stderr() {
    if std::env::var_os(CHILD_ENV).is_some() {
        read_with_no_seeder();
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
    // The child's own output is the evidence: report it with the failure.
    let stderr = read_to_string(&log_path).unwrap_or_default();
    assert!(
        status.success(),
        "the stderr probe exited with {status}:\n{stderr}"
    );

    // A read that finds an empty swarm must leave the §4 cause on the daemon's
    // stderr, with the frozen prefix at the start of the line and one of the two
    // registered variants behind it — external consumers grep exactly that.
    let hint = stderr
        .lines()
        .find(|line| line.starts_with("no seeder connected (Peers:0 Seeds:0)"))
        .unwrap_or_else(|| {
            panic!("the daemon stderr holds no frozen-prefix cause:\n{stderr}")
        });
    assert!(
        hint.starts_with("no seeder connected (Peers:0 Seeds:0) within the ")
            || hint.starts_with("no seeder connected (Peers:0 Seeds:0) after the "),
        "diagnostic {hint:?} is not a registered §4 variant"
    );
}

/// Child body: a sourceless read must fail with `NoPeers` (→ `ENODATA`) and its
/// cause must reach the process's stderr.
fn read_with_no_seeder() {
    let _session_guard = acquire_session_lock();

    // Unreachable tracker: the swarm stays empty, which is the state the frozen
    // line is the diagnosis for.  The torrent name keeps its own info hash, so
    // no local seeder (another test session, LSD) can serve the read.
    let (torrent_data, _content) = create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "no_seeder_stderr_diagnostic!!",
    );
    let info = Arc::new(torrentfs::TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));

    let mut config = local_test_config();
    // Short windows: the shipped 30s peer-discovery window would spend the
    // whole budget on a wait the assertion does not depend on.  LSD off so
    // discovery cannot pair with another session on this host.
    config.local_discovery.lsd_enabled = Some(false);
    config.timeouts.read_timeout_secs = Some(10);
    config.timeouts.peer_discovery_wait_secs = Some(1);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");
    let result = engine.read_file_range(info, 0, 0, 50);
    assert!(
        matches!(result, Err(torrentfs::TorrentError::NoPeers(_))),
        "a sourceless read must fail with NoPeers (ENODATA), got {result:?}"
    );
    engine.shutdown();
}

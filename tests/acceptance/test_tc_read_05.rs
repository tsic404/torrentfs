//! Design acceptance case TC-read-05: a read that settles through a wait
//! window leaves one registered cause on the daemon's error stream
//! (`docs/03-acceptance.md` §B AC-17; `docs/02-contracts.md` §C-4.4).
//!
//! The three classified stall families are exercised against the real engine:
//! an empty swarm (no seeder), a connected seeder that never delivers
//! (slow swarm) and a read range larger than the whole cache (cache stall).
//! Each read must finish with `ENODATA`'s domain error and write its frozen
//! prefix to stderr — the only stream both a direct run's `2> log` and a
//! container's `docker logs` reach unchanged.
//!
//! The read runs in a child of this test binary with its stderr pointed at a
//! file: fd 2 is process-wide, so asserting in-process would mean redirecting
//! the whole test binary's stderr around the reads.

use std::fs::{File, read_to_string};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::common::base::create_single_piece_torrent;
use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
};
use torrentfs::TorrentError;
use torrentfs::download::DownloadEngine;

use crate::seed_peer::SeedPeer;

pub const TC_ID: &str = "TC-read-05";

/// Set in the child spawn so the child body performs the reads (the variable
/// is unique to this TC, so no other test's body can pick it up).
const CHILD_ENV: &str = "TORRENTFS_TC_READ_05_CHILD";
const TEST_NAME: &str = "test_tc_read_05::tc_read_05_failed_reads_leave_a_registered_cause";

#[test]
fn tc_read_05_failed_reads_leave_a_registered_cause() {
    if std::env::var_os(CHILD_ENV).is_some() {
        exercise_failed_reads();
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

    // No seeder: the discovery-window variant, with the live counts.
    let no_seeder = find_line(&stderr, "no seeder connected (Peers:0 Seeds:0) within the ")
        .unwrap_or_else(|| panic!("no frozen no-seeder cause in the daemon stderr:\n{stderr}"));
    assert!(
        no_seeder.contains("raise [timeouts] peer_discovery_wait_secs"),
        "the no-seeder cause must name the knob that widens its window: {no_seeder:?}"
    );

    // Slow swarm: a connected seeder that delivered nothing.  The fixture
    // reads the whole 1 MiB file from offset 0 in one call with
    // `read_timeout_secs = 2`, so the C-4.4.3 fields have known values: a
    // wrong window or piece index in the engine's call site would otherwise
    // only be caught by the literal-argument unit test.
    let slow_swarm = find_line(&stderr, "read stalled on a slow swarm (Peers:")
        .unwrap_or_else(|| panic!("no frozen slow-swarm cause in the daemon stderr:\n{stderr}"));
    assert!(
        slow_swarm.contains("Seeds:1"),
        "the slow-swarm cause must name the connected seeder: {slow_swarm:?}"
    );
    assert!(
        slow_swarm.contains(
            "piece 0 did not arrive within the 2s piece-wait window at \
             0.00% torrent progress"
        ),
        "the slow-swarm cause must carry the blocking piece, the window this \
         read actually exhausted and the progress: {slow_swarm:?}"
    );
    assert!(
        !slow_swarm.contains("no seeder"),
        "slow-swarm wording must not imply a sourceless swarm: {slow_swarm:?}"
    );

    // Cache stall: the read span exceeds the whole cache.
    let cache_stall = find_line(&stderr, "read stalled on the on-disk cache (cache_size=")
        .unwrap_or_else(|| panic!("no frozen cache-stall cause in the daemon stderr:\n{stderr}"));
    assert!(
        cache_stall.contains("read span exceeds cache"),
        "the cache-stall cause must carry its evidence marker: {cache_stall:?}"
    );
}

fn find_line<'a>(log: &'a str, prefix: &str) -> Option<&'a str> {
    log.lines().find(|line| line.starts_with(prefix))
}

/// Child body: drive one read per stall family and assert each one fails with
/// the documented domain error.  The parent asserts the stderr side.
fn exercise_failed_reads() {
    let _slot = wave_slot();

    read_with_no_seeder();
    read_on_a_stalled_swarm(false);
    read_on_a_stalled_swarm(true);
}

fn read_with_no_seeder() {
    let _session_guard = acquire_session_lock();
    let (torrent_data, _content) = create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "tc-read-05-no-seeder!!",
    );
    let info = Arc::new(torrentfs::TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));

    let mut config = quiet_config();
    config.timeouts.read_timeout_secs = Some(5);
    config.timeouts.peer_discovery_wait_secs = Some(1);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");
    let result = engine.read_file_range(info, 0, 0, 50);
    assert!(
        matches!(result, Err(TorrentError::NoPeers(_))),
        "a sourceless read must fail with NoPeers (ENODATA), got {result:?}"
    );
    engine.shutdown();
}

/// A stalled peer declares every piece and unchokes but never sends data.  A
/// whole-file read on the default cache must classify as slow swarm; the same
/// read with a cache smaller than the range classifies as a cache stall.
fn read_on_a_stalled_swarm(use_tiny_cache: bool) {
    let _session_guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let name = if use_tiny_cache {
        "tc-read-05-cache-stall.bin"
    } else {
        "tc-read-05-slow-swarm.bin"
    };
    let (torrent_data, content) = build_multipiece_torrent_named(&announce_url, name);
    let info = Arc::new(torrentfs::TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));
    let info_hash = info.info_hash().expect("info hash");
    let _peer = SeedPeer::start_stalled(
        info_hash,
        content.clone(),
        info.piece_length() as usize,
        &announce_url,
    );

    let mut config = quiet_config();
    config.timeouts.read_timeout_secs = Some(2);
    config.timeouts.peer_discovery_wait_secs = Some(2);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);
    if use_tiny_cache {
        // Smaller than the read's whole 1 MiB span, so the cache cannot hold
        // the pieces the read waits on.
        config.cache.cache_size = Some(64 * 1024);
    }

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create engine");
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure handle");
    wait_for_seed(&engine, &hex::encode(info_hash));

    let result = engine.read_file_range(info, 0, 0, content.len() as u32);
    assert!(
        matches!(result, Err(TorrentError::Timeout(_))),
        "a stalled read must fail with Timeout (ENODATA), got {result:?}"
    );
    engine.shutdown();
}

/// Serialized, discovery-off config: each case must see only its own swarm.
fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    config.dht.enabled = Some(false);
    config
}

/// Wait until the stalled peer is counted as a seeder, so the read starts in
/// the piece-wait phase instead of spending a discovery window.
fn wait_for_seed(engine: &DownloadEngine, info_hash: &str) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        if engine
            .try_torrent_status(info_hash)
            .is_some_and(|s| s.num_seeds >= 1)
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("the stalled peer was never counted as a seeder");
}

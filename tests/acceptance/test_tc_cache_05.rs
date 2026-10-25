//! Design acceptance case TC-cache-05: a read the on-disk cache cannot serve
//! ends as `ENODATA` and leaves the frozen cache-stall diagnosis on the
//! daemon's error stream (`docs/03-acceptance.md` §D AC-34;
//! `docs/02-contracts.md` §C-4.4.2, §C-4.4.6, §C-4.4.7).
//!
//! Two stall shapes are driven: a read span larger than the whole cache
//! (`read span exceeds cache`) and a read blocked on a piece whose on-disk data
//! was removed after libtorrent declared it complete (`piece the read waits on
//! is gone from cache`).  The child process captures fd 2 — the daemon's own
//! error stream — because the engine writes the frozen hint there while
//! `tracing` writes to stdout.

use std::fs::{File, read_to_string};
use std::process::{Command, Stdio};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
    TestHarness,
};
use crate::seed_peer::SeedPeer;
use torrentfs::domain::fs_error::FsError;
use torrentfs::download::DownloadEngine;
use torrentfs::{TorrentError, TorrentInfo};

pub const TC_ID: &str = "TC-cache-05";

const CHILD_ENV: &str = "TORRENTFS_TC_CACHE_05_CHILD";
const TEST_NAME: &str = "test_tc_cache_05::tc_cache_05_cache_stall_diagnosis_and_enodata";
const CACHE_STALL_PREFIX: &str = "read stalled on the on-disk cache (cache_size=";
const EVIDENCE_SPAN: &str = "read span exceeds cache";
const EVIDENCE_PIECE_GONE: &str = "piece the read waits on is gone from cache";

#[test]
fn tc_cache_05_cache_stall_diagnosis_and_enodata() {
    if std::env::var_os(CHILD_ENV).is_some() {
        exercise_cache_stalls();
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

    // Each cache stall leaves exactly one frozen line, carrying the cache
    // limit and the read span (C-4.4.2).
    let stalls: Vec<&str> = stderr
        .lines()
        .filter(|line| line.starts_with(CACHE_STALL_PREFIX))
        .collect();
    assert_eq!(
        stalls.len(),
        2,
        "each cache stall must leave one frozen diagnosis line:\n{stderr}"
    );
    for line in &stalls {
        assert!(
            line.contains("cache_size="),
            "the diagnosis must carry the cache limit: {line:?}"
        );
        assert!(
            line.contains("read span="),
            "the diagnosis must carry the read span: {line:?}"
        );
        let evidence_span = line.contains(EVIDENCE_SPAN);
        let evidence_gone = line.contains(EVIDENCE_PIECE_GONE);
        assert!(
            evidence_span ^ evidence_gone,
            "the evidence must be exactly one of the two frozen values: {line:?}"
        );
    }
    assert!(
        stalls.iter().any(|line| line.contains(EVIDENCE_SPAN)),
        "the span-larger-than-cache stall must report `{EVIDENCE_SPAN}`:\n{stderr}"
    );
    assert!(
        stalls.iter().any(|line| line.contains(EVIDENCE_PIECE_GONE)),
        "the removed-piece stall must report `{EVIDENCE_PIECE_GONE}`:\n{stderr}"
    );
}

fn exercise_cache_stalls() {
    let _slot = wave_slot();
    read_span_larger_than_the_cache();
    read_blocked_on_a_piece_removed_from_the_cache();
}

/// A stalled seeder is connected but never delivers; the read's span is larger
/// than the whole cache, so the cache cannot hold the pieces the read needs.
fn read_span_larger_than_the_cache() {
    let _session_guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        build_multipiece_torrent_named(&announce_url, "tc-cache-05-span.bin");
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));
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
    // Smaller than one piece and far smaller than the whole 1 MiB read span.
    config.cache.cache_size = Some(64 * 1024);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("engine");
    engine.ensure_handle(Arc::clone(&info)).expect("handle");
    wait_for_seed(&engine, &hex::encode(info_hash));

    let result = engine.read_file_range(info, 0, 0, content.len() as u32);
    assert_cache_stall_is_enodata(result, "A seeder is connected");
    engine.shutdown();
}

/// Warm the cache from a real seeder, stop that seeder, then remove one piece
/// file behind libtorrent's back.  The piece bit stays set while the data is
/// gone, so the read is blocked on a piece that left the cache — the stale
/// evidence, classified ahead of the empty swarm (C-4.4.6).
fn read_blocked_on_a_piece_removed_from_the_cache() {
    let _session_guard = acquire_session_lock();
    let mut harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_named(announce_url, "tc-cache-05-stale.bin")
    });
    let info = Arc::new(TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("parse"));
    let info_hash = hex::encode(info.info_hash().expect("info hash"));
    let total = info.total_size();

    let mut config = quiet_config();
    // Long enough for the whole-file warm read to finish against a live seeder;
    // the stalled re-read below keeps its short no-seeder piece wait because no
    // seeder is connected by then.
    config.timeouts.read_timeout_secs = Some(30);
    config.timeouts.peer_discovery_wait_secs = Some(1);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("engine");
    engine.ensure_handle(Arc::clone(&info)).expect("handle");
    wait_for_seed(&engine, &info_hash);
    // Warm the whole file.  Retries keep the precondition (every piece cached)
    // independent of a transiently starved swarm, which says nothing about the
    // stall this case classifies.
    let warm = read_whole_with_retry(&engine, &info, total as u32, Duration::from_secs(90))
        .expect("warm read");
    assert_eq!(warm, harness.file_content, "warm read returned the wrong bytes");

    // Remove the only source, then drop the on-disk data the cache holds for
    // piece 0.  The cache still has the piece registered, but the file is gone
    // — exactly the stale state the classification must report as a cache
    // stall rather than as a slow swarm.
    harness.stop_seeder();
    wait_until("the seeder to leave the swarm", Duration::from_secs(20), || {
        engine
            .try_torrent_status(&info_hash)
            .map(|status| status.num_seeds == 0)
            .unwrap_or(false)
    });

    let cache = engine.cache_manager();
    let piece_key = format!("{info_hash}:piece:0");
    let piece_path = cache.lock().expect("cache lock").piece_path(&piece_key);
    assert!(piece_path.is_file(), "precondition: piece 0 must be on disk");
    std::fs::remove_file(&piece_path).expect("remove piece 0 from the cache");

    let result = engine.read_file_range(info.clone(), 0, 0, info.piece_length());
    assert_cache_stall_is_enodata(result, "No seeder is connected");
    engine.shutdown();
}

/// AC-34's two assertions: the caller sees `ENODATA` (not `EIO`), and the
/// error text is the cache-stall one (not the no-seeder or slow-swarm text).
fn assert_cache_stall_is_enodata(result: Result<Vec<u8>, TorrentError>, swarm_state: &str) {
    let error = match result {
        Ok(data) => panic!(
            "a read the cache cannot serve must fail, but it returned {} bytes",
            data.len()
        ),
        Err(error) => error,
    };
    let message = match &error {
        TorrentError::Timeout(message) => message.clone(),
        other => panic!("a cache stall must surface as Timeout, got {other:?}"),
    };
    assert!(
        message.contains("on-disk cache"),
        "the error text must name the on-disk cache: {message}"
    );
    assert!(
        message.contains(swarm_state),
        "the error text must carry the seeder connection state: {message}"
    );

    // The exact FUSE errno exit point: TorrentError → FsError → errno.
    let fs_error: FsError = error.into();
    let errno: libc::c_int = fs_error.into();
    assert_eq!(
        errno,
        libc::ENODATA,
        "a cache stall must map to ENODATA, not EIO: {message}"
    );
}

/// Serialized, discovery-off config: each case must see only its own swarm.
fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    config.dht.enabled = Some(false);
    config
}

/// Wait until the peer is counted as a seeder, so the read starts in the
/// piece-wait phase instead of spending a discovery window.
fn wait_for_seed(engine: &DownloadEngine, info_hash: &str) {
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(15) {
        if engine
            .try_torrent_status(info_hash)
            .is_some_and(|status| status.num_seeds >= 1)
        {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("the seeder was never counted in the swarm");
}

/// Warm-cache read: retry until the whole range arrives or the budget is spent.
fn read_whole_with_retry(
    engine: &DownloadEngine,
    info: &Arc<TorrentInfo>,
    size: u32,
    budget: Duration,
) -> Result<Vec<u8>, TorrentError> {
    let start = Instant::now();
    loop {
        match engine.read_file_range(Arc::clone(info), 0, 0, size) {
            Ok(data) if data.len() == size as usize => return Ok(data),
            other => {
                if start.elapsed() >= budget {
                    return match other {
                        Ok(data) => Err(TorrentError::PieceNotReady(format!(
                            "warm read returned {} of {size} bytes",
                            data.len()
                        ))),
                        Err(error) => Err(error),
                    };
                }
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_until(what: &str, budget: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + budget;
    loop {
        if condition() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(100));
    }
}

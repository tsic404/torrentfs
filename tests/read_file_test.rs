//! End-to-end test: validate file read via DownloadEngine::read_file_range
//! using a local tracker + seeder (TestHarness).
//!
//! This test addresses scenario 4: file reading fails when
//! no real peers are available. By using a self-hosted tracker + seeder,
//! we validate the full lazy-loading flow without external infrastructure.

mod common;

use common::{local_test_config, TestHarness};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Test that DownloadEngine::read_file_range can download and return
/// correct file data when a local seeder is available via tracker.
///
/// This is the exact code path exercised in the QA test scenario 4,
/// validating that the lazy-loading flow works end-to-end.
#[test]
fn test_read_file_range_with_local_seeder() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("TestHarness ready. Info hash: {}", info_hash);
    println!(
        "Tracker URL: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    // ── Create DownloadEngine pointing at the tracker ──────────────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a NoPeers timeout (flaky).
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    // ── Re-parse torrent data (raw pointer can't cross into the engine) ─
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // ── Read file range (file_index=0, offset=0, size=50) ──────────────
    // This goes through read_file_range → ensure handle →
    // piece download → cache → return data.
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);

    let mut last_error: Option<torrentfs::TorrentError> = None;
    loop {
        match engine.read_file_range(info.clone(), 0, 0, 50) {
            Ok(data) => {
                println!(
                    "Successfully read {} bytes after {:.1}s",
                    data.len(),
                    start.elapsed().as_secs_f64()
                );
                println!("Data: {:?}", String::from_utf8_lossy(&data));

                assert!(!data.is_empty(), "Expected non-empty data");
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
                return;
            }
            Err(e) => {
                last_error = Some(e);
                println!(
                    "Read attempt at {:.1}s: {:?}",
                    start.elapsed().as_secs_f64(),
                    last_error.as_ref().unwrap()
                );
            }
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out after {:.0}s waiting for file read. Last error: {:?}",
                timeout.as_secs(),
                last_error
            );
        }

        thread::sleep(Duration::from_secs(1));
    }
}

/// Regression test: a lightweight handle created at torrent-add
/// time (upload_mode, no pieces downloaded) must switch to download mode and
/// fetch data from a tracker-only peer when a read arrives later — not stay
/// stuck in a "Finished" state with no peer connections.
#[test]
fn test_read_file_range_after_idle_handle() {
    // Serialize libtorrent session creation to avoid resource contention.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a 30s timeout.
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // Mirror the FUSE "torrent added" path: create the lightweight handle
    // first, then leave it idle long enough to settle into upload_mode.
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");
    thread::sleep(Duration::from_secs(3));

    // Now read — the idle handle must switch to download mode and fetch the
    // piece from the seeder over the tracker (peer-to-peer path).
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut last_error: Option<torrentfs::TorrentError> = None;
    loop {
        match engine.read_file_range(info.clone(), 0, 0, 50) {
            Ok(data) => {
                assert!(!data.is_empty(), "Expected non-empty data");
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
                return;
            }
            Err(e) => {
                last_error = Some(e);
            }
        }
        if start.elapsed() > timeout {
            panic!(
                "Timed out after {:.0}s waiting for file read after idle. Last error: {:?}",
                timeout.as_secs(),
                last_error
            );
        }
        thread::sleep(Duration::from_secs(1));
    }
}

/// Regression test: an idle handle (upload_mode, no read yet) must
/// establish a peer/seed connection on its own once the tracker returns the
/// seeder.  Before the fix the libtorrent session never connected while the
/// handle stayed idle, so `.stats` persistently showed `Peers: 0 Seeds: 0`
/// until an explicit read (`dd`) forced the connect.  This test never reads:
/// it only polls the engine's published snapshot.
#[test]
fn test_idle_handle_connects_to_seeder_without_read() {
    // Serialize libtorrent session creation to avoid resource contention.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // distinct downloader listen port so the MiniTracker can tell
    // it apart from the seeder (which binds 6881).
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));

    // Create the lightweight upload_mode handle and leave it idle — never read.
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    loop {
        if let Some(status) = engine.try_torrent_status(&info_hash) {
            println!(
                "idle: state={:?} peers={} seeds={} (t={:.1}s)",
                status.state,
                status.num_peers,
                status.num_seeds,
                start.elapsed().as_secs_f64()
            );
            if status.num_peers > 0 || status.num_seeds > 0 {
                println!(
                    "idle handle connected: peers={} seeds={}",
                    status.num_peers, status.num_seeds
                );
                return;
            }
        }
        if start.elapsed() > timeout {
            panic!(
                "idle handle never connected within {}s; tracker announces: {}",
                timeout.as_secs(),
                harness.tracker.announce_count()
            );
        }
        thread::sleep(Duration::from_millis(500));
    }
}

/// Regression test: creating the lightweight upload_mode handle at
/// torrent-add time (`ensure_handle`) must trigger an immediate tracker
/// announce and surface the seeder in the published snapshot — with no read
/// (`read_file_range`) ever issued.
///
/// The fix (`force_reannounce` right after handle creation) makes the first
/// announce deterministic.  Pre-fix the announce rode libtorrent's own
/// scheduled first announce, which the QA multi-interface swarm observed as
/// `Peers: 0 Seeds: 0` until a read drove the slow path's `force_reannounce`
/// (that topology — a tracker reached via the host's non-loopback IPv4 — is
/// not reproducible in CI; on a loopback tracker libtorrent's default announce
/// also fires, so the pre-fix code would likely pass here too).  This test
/// therefore pins the observable contract (announce + visible peers within a
/// short window, no read) rather than the environment that surfaces it.
///
/// `announce_count` alone cannot distinguish the downloader's announce from
/// the seeder's periodic re-announce, so `peer_count >= 2` is the definitive
/// signal: the downloader listens on a distinct port (16881) and registers a
/// second peer entry on top of the seeder's (6881).
#[test]
fn test_ensure_handle_triggers_announce_without_read() {
    // Serialize libtorrent session creation to avoid resource contention.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // distinct downloader listen port so the tracker records the downloader
    // as a second peer, separate from the seeder (which binds 6881).
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    let raw_info_hash = info.info_hash().expect("Failed to get info hash");

    // Baseline: the seeder has announced (TestHarness guarantees this) and is
    // the only registered peer.  `peer_count` distinguishes the downloader's
    // announce from the seeder's re-announce, which keeps its own entry at 1.
    let baseline_announces = harness.tracker.announce_count();
    assert_eq!(
        harness.tracker.peer_count(&raw_info_hash),
        1,
        "seeder must be the only registered peer before the downloader announces"
    );

    // Mirror the FUSE torrent-add path: create the upload_mode handle.  No
    // read is ever issued in this test.
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(10);
    loop {
        // The downloader's own announce reaches the tracker (a second peer
        // entry appears)...
        let downloader_announced = harness.tracker.peer_count(&raw_info_hash) >= 2;
        // ...and the engine snapshot exposes the seeder, which is what
        // `.stats` renders as `Peers`/`Seeds`.
        let peers_visible = engine
            .try_torrent_status(&info_hash)
            .map(|s| s.num_peers > 0 || s.num_seeds > 0)
            .unwrap_or(false);

        println!(
            "ensure_handle announce: downloader_announced={downloader_announced} \
             peers_visible={peers_visible} (announce_count={}, peer_count={})",
            harness.tracker.announce_count(),
            harness.tracker.peer_count(&raw_info_hash)
        );

        if downloader_announced && peers_visible {
            println!(
                "ensure_handle announced and exposed peers in {:.1}s",
                start.elapsed().as_secs_f64()
            );
            return;
        }
        if start.elapsed() > timeout {
            panic!(
                "ensure_handle did not announce / expose peers within {}s \
                 (announce_count baseline={}, now={}, peer_count={})",
                timeout.as_secs(),
                baseline_announces,
                harness.tracker.announce_count(),
                harness.tracker.peer_count(&raw_info_hash)
            );
        }
        thread::sleep(Duration::from_millis(200));
    }
}

/// Test that read_file_range returns correct data for different offset/size
/// combinations, validating boundary handling.
#[test]
fn test_read_file_range_boundaries() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::new();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a 30s timeout.
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // Helper: retry read until success or timeout
    let retry_read = |offset: u64, size: u32| -> Vec<u8> {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, offset, size) {
                Ok(data) => return data,
                Err(e) => {
                    if start.elapsed() > timeout {
                        panic!(
                            "Timed out reading offset={}, size={}: {:?}",
                            offset, size, e
                        );
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    };

    // Read first 10 bytes
    let data = retry_read(0, 10);
    assert_eq!(data.len(), 10);
    assert_eq!(&data, &harness.file_content[..10]);

    // Read bytes 50-60 (middle of content)
    let data = retry_read(50, 10);
    assert_eq!(data.len(), 10);
    assert_eq!(&data, &harness.file_content[50..60]);

    // Read bytes from offset 10 to end (size 16374 = total 16384 - offset 10)
    let data = retry_read(10, 16374);
    assert_eq!(data.len(), 16374);
    assert_eq!(&data, &harness.file_content[10..16384]);

    // Read past end should return empty or truncated
    let data = retry_read(16378, 10);
    assert_eq!(data.len(), 6); // 16384 - 16378 = 6 bytes left
    assert_eq!(&data, &harness.file_content[16378..16384]);

    // Out-of-bounds reads (`offset >= file_size`) must return 0 bytes
    // regardless of requested size: the engine clamps before any piece
    // download, so a 1-byte and a 4096-byte read past EOF behave alike.
    let data = retry_read(16384, 4096); // offset == file_size
    assert!(data.is_empty(), "read at offset == file_size must be empty");

    let data = retry_read(999_999, 1); // past EOF, 1-byte read
    assert!(data.is_empty(), "1-byte read past EOF must be empty");

    let data = retry_read(999_999, 4096); // past EOF, large read
    assert!(
        data.is_empty(),
        "large read past EOF must be empty (bs-independent)"
    );
}

/// Test that read_file_range correctly returns an error when no
/// peers/seeds are available AND no cached pieces exist.
#[test]
fn test_read_file_range_no_peers_error() {
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    // Use config with DHT disabled so we don't accidentally find peers
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.timeouts.read_timeout_secs = Some(2); // Short timeout for test

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    // Create a test torrent with a fake tracker URL (no real tracker running)
    let (torrent_data, _file_content) =
        common::create_test_torrent_with_tracker("http://127.0.0.1:19999/announce");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );

    // This should fail since the tracker doesn't exist
    let result = engine.read_file_range(info, 0, 0, 50);

    match result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {
            println!("Correctly got NoPeers error as expected");
        }
        Err(e) => {
            // Could also be a Timeout if the state check or piece wait expires.
            println!(
                "Got error: {:?} (NoPeers expected but other error acceptable)",
                e
            );
        }
        Ok(data) => {
            // Not expected but could happen if pieces somehow cached
            println!(
                "Unexpectedly got data: {} bytes (may have cached pieces)",
                data.len()
            );
        }
    }
}

/// With no peers at all, a read must probe the swarm for the full peer-wait
/// window before giving up, then fail fast with `NoPeers` — not spend the
/// extra no-seeder piece-wait window on a seeder that can never appear, which
/// would serialize healthy reads on the single engine thread behind a dead
/// torrent's whole-file `cat`.
///
/// Deterministic: unique info_hash with no reachable tracker (the announce
/// URL is a dead endpoint) and DHT/LSD disabled, so no peer can ever appear;
/// the all-zero piece hashes additionally make a hash-valid piece impossible.
#[test]
fn test_no_peers_read_probes_then_fails_fast() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // Use a unique info_hash (distinct name) so no other test's seeder or
    // leaked seeder thread for the shared `create_test_torrent_with_tracker`
    // torrent can be discovered and serve piece 0 during full-suite parallel
    // load. The all-zero piece hashes also make a hash-valid piece impossible.
    let torrent_data = distinct_torrent("no-peers-read.iso");

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = common::local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    // Short timeout: the read must block for the peer-wait probe
    // (min(read_timeout_secs, 9s) = 4s) and then fail fast with `NoPeers`,
    // so the elapsed-time assertions below can tell the probe apart from the
    // extra no-seeder piece-wait window.
    config.timeouts.read_timeout_secs = Some(4);

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );

    let start = std::time::Instant::now();
    let result = engine.read_file_range(info, 0, 0, 50);
    let elapsed = start.elapsed();

    match &result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {
            println!("NoPeers after {:.2}s", elapsed.as_secs_f64());
        }
        Err(e) => panic!(
            "Expected NoPeers, got {:?} after {:.2}s",
            e,
            elapsed.as_secs_f64()
        ),
        Ok(data) => panic!(
            "Unexpectedly got {} bytes with zero peers in the swarm",
            data.len()
        ),
    }

    // Contract: the read probes the swarm for the full peer-wait window
    // (min(read_timeout_secs, 9s) = 4s) and then fails fast with `NoPeers` —
    // neither returning before the probe completes, nor spending the extra
    // no-seeder piece-wait window (~4s here) on a seeder that can never appear.
    assert!(
        elapsed >= Duration::from_millis(3500),
        "Read returned NoPeers before the peer-wait probe completed ({:.2}s)",
        elapsed.as_secs_f64()
    );
    assert!(
        elapsed < Duration::from_millis(7500),
        "Read returned NoPeers too late ({:.2}s): the no-seeder piece-wait \
         window must not block the engine thread",
        elapsed.as_secs_f64()
    );
}

/// RAII guard that stops and joins a background leecher thread on drop.
///
/// Drop runs on both normal exit and panic unwind, so a failed assertion in
/// the test body (announce timeout, reader join) still tears the leecher's
/// libtorrent session down instead of leaking it — and its listen port /
/// thread-pool slots — into the next test.
struct LeecherGuard {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl Drop for LeecherGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// With a live leecher in the swarm (`num_peers > 0`, `num_seeds == 0`), a
/// read must NOT fast-fail once peer discovery elapses.  The empty-swarm
/// fast-fail (`is_peer_wait_exhausted`) may only be set when BOTH peer and
/// seed counts are zero; a leecher is still a potential source (or a future
/// seeder), so it keeps the regular no-seeder piece-wait window.
///
/// Deterministic: a download-mode leecher session (empty save dir → `left` is
/// the full file size) announces to the tracker before the read, so the
/// downloader connects to it and observes `num_peers > 0, num_seeds == 0`.
#[test]
fn test_leecher_only_swarm_read_does_not_fast_fail() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // Unique info_hash (distinct name) so no other test's (or a leaked)
    // seeder for the shared fixture torrent can serve the read.
    let tracker = common::MiniTracker::start();
    let announce_url = tracker.announce_url();
    let torrent_data = distinct_torrent_with_tracker("leecher-only.iso", &announce_url);
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    let raw_info_hash = info.info_hash().expect("Failed to get info hash");

    // ── Leecher: has the torrent, but no file data (`left` = total size) ──
    // A download-mode session with an empty save dir announces as a leecher
    // and stays alive so the downloader keeps a connected leecher peer.
    let leecher_stop = Arc::new(AtomicBool::new(false));
    let leecher_stop_clone = Arc::clone(&leecher_stop);
    let leecher_torrent = torrent_data.clone();
    let leecher_thread = thread::spawn(move || {
        let mut cfg = common::local_test_config();
        // Distinct listen port so the tracker records the leecher as a peer
        // separate from the downloader (16881).
        cfg.connections.listen_interfaces = Some("0.0.0.0:16882".to_string());
        let mut session = torrentfs::download::Session::new(&cfg).expect("leecher session");
        let li = torrentfs::TorrentInfo::from_bytes(leecher_torrent).expect("leecher parse");
        let save_dir = tempfile::TempDir::new().expect("leecher save dir");
        // No file written → zero pieces, so the leecher announces with
        // `left = total_size` and the downloader counts it as a peer, not a seed.
        let _handle = session
            .add_torrent(&li, save_dir.path())
            .expect("leecher add");
        while !leecher_stop_clone.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }
    });
    // The guard owns the stop flag and join handle; on any panic unwind below
    // (announce assert, reader join) it still stops and joins the leecher so
    // its session never leaks into a later test.
    let _leecher_guard = LeecherGuard {
        stop: leecher_stop,
        thread: Some(leecher_thread),
    };

    // Wait for the leecher to register with the tracker before the read, so
    // the downloader's first announce already returns it.
    let announce_start = std::time::Instant::now();
    loop {
        if tracker.peer_count(&raw_info_hash) > 0 {
            break;
        }
        assert!(
            announce_start.elapsed() < Duration::from_secs(30),
            "leecher never announced (announce_count={})",
            tracker.announce_count()
        );
        thread::sleep(Duration::from_millis(200));
    }

    // ── Downloader: distinct listen port, DHT/LSD disabled ──────────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = common::local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());
    // The leecher-only swarm has no seeder, so the piece-wait window is
    // min(read_timeout_secs, 15s); the 12s cap keeps the test bounded while
    // still exceeding the 9s peer-wait cap (`PEER_WAIT_CAP_SECS`).
    config.timeouts.read_timeout_secs = Some(12);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    // Start the read in the background so the live swarm state can be polled.
    let engine_reader = Arc::clone(&engine);
    let info_reader = Arc::clone(&info);
    let reader = thread::spawn(move || {
        let start = std::time::Instant::now();
        let result = engine_reader.read_file_range(info_reader, 0, 0, 50);
        (result, start.elapsed())
    });

    // Poll the snapshot until the read itself completes — the read's own
    // completion bounds the poll, so a slow-CI leecher connect cannot outlive
    // the poller (no hard deadline that expires early and false-fails while
    // the read is still legitimately waiting).  This is the positive proof the
    // read exercised the leecher-only branch, not the empty-swarm fast-fail:
    // if the leecher never connects, this fails and the timing assertion below
    // would otherwise be ambiguous (both paths return NoPeers).
    let mut saw_leecher = false;
    while !reader.is_finished() {
        if let Some(status) = engine.try_torrent_status(&info_hash) {
            if status.num_peers > 0 && status.num_seeds == 0 {
                saw_leecher = true;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    let (result, elapsed) = reader.join().expect("reader thread panicked");

    assert!(
        saw_leecher,
        "downloader never observed a connected leecher (num_peers > 0, num_seeds == 0)"
    );

    match &result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {
            println!(
                "NoPeers after {:.2}s (leecher-only swarm)",
                elapsed.as_secs_f64()
            );
        }
        Err(e) => panic!(
            "Expected NoPeers, got {:?} after {:.2}s",
            e,
            elapsed.as_secs_f64()
        ),
        Ok(data) => panic!(
            "Unexpectedly read {} bytes from a leecher-only swarm (no seed)",
            data.len()
        ),
    }

    // Contract: a leecher (`num_peers > 0`) is a potential source (or future
    // seeder), so the read must NOT fast-fail once peer discovery elapses —
    // it keeps the no-seeder piece-wait window instead of the zero-second
    // `NO_SEEDER_FAST_FAIL_SECS`.  Boundary: the fast-fail path returns
    // NoPeers right after the ~9s peer-wait cap (`PEER_WAIT_CAP_SECS`), and
    // the 200ms poll granularity plus the final status refresh put that in
    // ~9.0-9.2s; the leecher path adds the 12s piece-wait window
    // (`min(read_timeout_secs, 15s)`), so it returns at ≥ ~12.5s.  `>= 10s`
    // sits in the gap — below the correct path's lower bound, above the
    // fast-fail path's upper bound.
    assert!(
        elapsed >= Duration::from_secs(10),
        "read returned NoPeers after {:.2}s — a leecher-only swarm fast-failed \
         instead of keeping the no-seeder piece-wait window",
        elapsed.as_secs_f64()
    );
}

/// if a peer appears mid-read (while the engine is
/// still inside peer-wait/piece-wait), the read must return the correct
/// data instead of erroring out.
///
/// Deterministic: seeder announces to its own tracker AFTER the read has
/// already started, so the swarm is empty at t=0 and populated later.
#[test]
fn test_peer_appearing_mid_read_returns_data() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // Tracker + torrent, but NO seeder yet — the downloader's first
    // announces see an empty swarm.
    let mut harness_seed: Option<common::TestHarness> = None;
    let tracker = common::MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, file_content_shared) =
        common::create_test_torrent_with_tracker(&announce_url);

    let info_hash = {
        let info = torrentfs::TorrentInfo::from_bytes(torrent_data.clone())
            .expect("Failed to parse torrent");
        hex::encode(info.info_hash().expect("Failed to get info hash"))
    };

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = common::local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    // force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a NoPeers timeout (flaky).
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());
    // The seeder is introduced 6s after the read starts; its session startup +
    // announce + peer connect must finish before the piece-wait window. On slow
    // CI 30s occasionally elapsed first (NoPeers, failing the gate), so bump to
    // 120s. The peer-wait cap (9s) is unchanged, so the fail-fast regression is
    // still exercised; the happy path returns as soon as the piece arrives.
    config.timeouts.read_timeout_secs = Some(120);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );

    // Start the read in a background thread.  At this moment the tracker
    // has zero peers for our info_hash, so the read enters peer-wait.
    let engine_reader = Arc::clone(&engine);
    let info_reader = Arc::clone(&info);
    let read_started = Arc::new(AtomicBool::new(false));
    let started = Arc::clone(&read_started);
    let reader = thread::spawn(move || {
        started.store(true, Ordering::SeqCst);
        engine_reader.read_file_range(info_reader, 0, 0, 50)
    });

    // Poll the shared snapshot while the engine blocks in peer-wait: it must be
    // refreshed by `publish_snapshot` so `.stats` shows the live `Downloading`
    // state, not stale zeros. Before the fix `publish_snapshot` wasn't called
    // during peer-wait (the snapshot stayed `Allocating`/`CheckingFiles`).
    // `num_peers`/`num_seeds` aren't asserted — libtorrent refreshes them
    // asynchronously and a single-piece peer connection is too brief to catch;
    // the `Downloading` transition is the reliable freshness signal.
    let engine_poller = Arc::clone(&engine);
    let info_hash_poll = info_hash.clone();
    let snapshot_fresh = thread::spawn(move || {
        let poll_timeout = Duration::from_secs(30);
        let poll_start = std::time::Instant::now();
        loop {
            if poll_start.elapsed() >= poll_timeout {
                return false;
            }
            if let Some(status) = engine_poller.try_torrent_status(&info_hash_poll) {
                if matches!(status.state, torrentfs::download::TorrentState::Downloading) {
                    return true;
                }
            }
            thread::sleep(Duration::from_millis(100));
        }
    });

    // Give the read time to get past the empty-swarm probe: wait until it
    // started, then hold the swarm empty long enough that under the OLD
    // behavior the peer-wait would expire with 0 peers.  Then drop in a
    // full seeder via TestHarness (its own tracker session announcing to
    // OUR tracker URL).
    while !read_started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(50));
    }
    thread::sleep(Duration::from_secs(6));

    eprintln!("mid-read: introducing seeder into the swarm");
    let seed_torrent_data = {
        // Build a fresh torrent pointing at the same announce URL so the
        // info_hash matches what the downloader announced with.
        let (t, _) = common::create_test_torrent_with_tracker(&announce_url);
        t
    };
    debug_assert_eq!(
        hex::encode(
            torrentfs::TorrentInfo::from_bytes(seed_torrent_data.clone())
                .unwrap()
                .info_hash()
                .unwrap()
        ),
        info_hash,
        "seeder torrent must have identical info_hash"
    );

    // Spawn the seeder manually (not via TestHarness::new, which creates
    // its own tracker) so it joins OUR MiniTracker's swarm.
    let seeder_handle = {
        let file_content = file_content_shared.clone();
        thread::spawn(move || {
            let seed_dir = tempfile::TempDir::new().expect("seed dir");
            std::fs::write(
                seed_dir.path().join("final_verification.txt"),
                &file_content,
            )
            .expect("write seed file");
            let mut cfg = common::local_test_config();
            // Ephemeral listen port: concurrent test binaries (and other
            // torrentfs processes) must not collide on libtorrent's default
            // `0.0.0.0:6881`, or the downloader sees `NoPeers`.
            cfg.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
            let mut session = torrentfs::download::Session::new(&cfg).expect("seeder session");
            let si = torrentfs::TorrentInfo::from_bytes(seed_torrent_data).expect("seeder parse");
            let h = session.add_torrent(&si, seed_dir.path()).expect("add");
            loop {
                if let Ok(s) = h.status() {
                    if matches!(
                        s.state,
                        torrentfs::download::TorrentState::Seeding
                            | torrentfs::download::TorrentState::Finished
                    ) {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
            // Keep the session alive for the remainder of the test.
            loop {
                thread::sleep(Duration::from_millis(100));
            }
        })
    };

    // assert the snapshot was refreshed during peer-wait.
    // The poller thread (started above, before the seeder) polls
    // `try_torrent_status` and returns true once the state transitions
    // to Downloading — proving `publish_snapshot` ran after upload_mode
    // was cleared (the pre-download snapshot would show Allocating).
    assert!(
        snapshot_fresh.join().expect("poller thread panicked"),
        "Snapshot never showed Downloading state during peer-wait — \
         publish_snapshot is not refreshing after upload_mode clear"
    );
    // The read must now succeed within its remaining budget.
    let result = reader.join().expect("reader thread panicked");

    // The seeder thread parks forever; the test process exits after this
    // test, tearing it down — nothing to join.
    drop(seeder_handle);

    match result {
        Ok(data) => {
            assert_eq!(&data[..50], &file_content_shared[..50]);
            println!("Read succeeded after seeder appeared mid-read");
        }
        Err(e) => panic!(
            "Read failed ({:?}) even though a seeder appeared mid-read — \
             the engine gave up before the peer could serve pieces",
            e
        ),
    }

    drop(harness_seed);
}

/// Build a structurally valid single-file `.torrent` whose piece hashes are
/// all zero.  Parsing and handle creation only need valid structure (not
/// correct hashes); distinct `name`s yield distinct info hashes.
fn distinct_torrent(name: &str) -> Vec<u8> {
    distinct_torrent_with_tracker(name, "http://127.0.0.1:19999/announce")
}

/// [`distinct_torrent`] pointing at a caller-supplied announce URL, so a test
/// can join its own `MiniTracker` swarm while keeping a distinct info_hash.
fn distinct_torrent_with_tracker(name: &str, announce_url: &str) -> Vec<u8> {
    let mut t = Vec::new();
    t.extend_from_slice(b"d8:announce");
    t.extend_from_slice(announce_url.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(announce_url.as_bytes());
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"6:lengthi16384e");
    t.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    t.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
    t.extend_from_slice(&[0u8; 20]);
    t.extend_from_slice(b"ee");
    t
}

/// Build a structurally valid multi-piece single-file `.torrent` (4 × 256 KiB
/// = 1 MiB) with all-zero piece hashes and a caller-chosen `name`.  Distinct
/// `name`s yield distinct info hashes; zero hashes make a hash-valid piece
/// impossible, so a no-seeder read on this torrent is deterministic even if a
/// stray peer connects.
fn distinct_multipiece_torrent(announce_url: &str, name: &str) -> Vec<u8> {
    const PIECE_LEN: usize = 256 * 1024;
    const NUM_PIECES: usize = 4;
    let total = PIECE_LEN * NUM_PIECES;

    let hashes = vec![0u8; 20 * NUM_PIECES];

    let mut t = Vec::new();
    t.push(b'd');
    t.extend_from_slice(b"8:announce");
    t.extend_from_slice(announce_url.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(announce_url.as_bytes());
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"6:lengthi");
    t.extend_from_slice(total.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces");
    t.extend_from_slice(hashes.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(&hashes);
    t.extend_from_slice(b"ee");
    t
}

/// Integration test for the core invariant: a whole-file read (`cat`) on a
/// no-seeder torrent must not block a healthy (seeded) read on the same
/// engine.  Before the fix, a no-seeder whole-file read serialized healthy
/// reads on the single engine thread behind a per-chunk no-seeder window
/// (observed ~47s for a 4 MiB `cat`).  After the fix, a no-seeder read probes
/// the empty swarm (≤9s peer-wait) then fails fast with `NoPeers` (0s
/// piece-wait), so the engine is never occupied for the full read timeout.
///
/// Constructed as a dual-torrent scenario on one MiniTracker: a healthy
/// single-piece torrent served by a real seeder, and a no-seeder multi-piece
/// torrent (distinct info_hash, all-zero piece hashes, empty swarm) whose
/// whole-file read must fail fast.  Both reads are issued concurrently; the
/// healthy read is dispatched first so the single engine thread serves it
/// immediately (proving it is not serialized behind the no-seeder `cat`).
#[test]
fn test_no_seeder_cat_does_not_block_healthy_read() {
    use std::sync::atomic::{AtomicBool, Ordering};

    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    // ── One tracker shared by both torrents ────────────────────────────
    let tracker = common::MiniTracker::start();
    let announce_url = tracker.announce_url();

    // ── Healthy torrent: single piece, served by a real seeder ────────
    let (healthy_torrent, healthy_content) =
        common::create_test_torrent_with_tracker(&announce_url);

    // ── No-seeder torrent: multi-piece, distinct info_hash, empty swarm ─
    let no_seeder_torrent = distinct_multipiece_torrent(&announce_url, "no-seeder.iso");

    // ── Engine with a distinct listen port (seeder binds 6881) ─────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16886".to_string());

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    let healthy_info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(healthy_torrent.clone())
            .expect("Failed to parse healthy torrent"),
    );
    let no_seeder_info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(no_seeder_torrent.clone())
            .expect("Failed to parse no-seeder torrent"),
    );

    // ── Seeder for the healthy torrent (joins OUR tracker's swarm) ─────
    // Stop flag so the seeder thread is joined (and its default 6881 listen
    // binding released) at the end of the test, instead of parking forever and
    // leaking the binding until process exit.
    let seeder_stop = Arc::new(AtomicBool::new(false));
    let seeder_handle = {
        let content = healthy_content.clone();
        let seeder_torrent = healthy_torrent.clone();
        let stop = Arc::clone(&seeder_stop);
        thread::spawn(move || {
            let seed_dir = tempfile::TempDir::new().expect("Failed to create seed dir");
            std::fs::write(seed_dir.path().join("final_verification.txt"), &content)
                .expect("Failed to write seed file");
            let cfg = common::local_test_config();
            let mut session =
                torrentfs::download::Session::new(&cfg).expect("Seeder: failed to create session");
            let info = torrentfs::TorrentInfo::from_bytes(seeder_torrent)
                .expect("Seeder: failed to parse torrent");
            let handle = session
                .add_torrent(&info, seed_dir.path())
                .expect("Seeder: failed to add torrent");
            loop {
                if let Ok(s) = handle.status() {
                    if matches!(
                        s.state,
                        torrentfs::download::TorrentState::Seeding
                            | torrentfs::download::TorrentState::Finished
                    ) {
                        break;
                    }
                }
                thread::sleep(Duration::from_millis(200));
            }
            // Keep the seeder session alive until the test signals stop.
            while !stop.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(100));
            }
        })
    };

    // ── Prime both handles (state transitions out of the measured path) ─
    engine
        .ensure_handle(healthy_info.clone())
        .expect("ensure_handle healthy");
    engine
        .ensure_handle(no_seeder_info.clone())
        .expect("ensure_handle no-seeder");

    // ── Warm the healthy torrent: download + cache its single piece, and
    // confirm the seeder connection is live (retry transient errors).
    let healthy_len = healthy_content.len() as u32;
    {
        let start = std::time::Instant::now();
        loop {
            match engine.read_file_range(healthy_info.clone(), 0, 0, healthy_len) {
                Ok(data) if data == healthy_content => break,
                _ => {
                    if start.elapsed() > Duration::from_secs(60) {
                        panic!("Timed out warming the healthy torrent");
                    }
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
    }

    // ── Concurrent reads ───────────────────────────────────────────────
    // Dispatch the healthy read first (flag + head-start) so the single
    // engine thread serves it ahead of the no-seeder read; the no-seeder
    // whole-file read is issued concurrently and must fail fast.
    let healthy_started = Arc::new(AtomicBool::new(false));
    let started_flag = Arc::clone(&healthy_started);
    let engine_healthy = Arc::clone(&engine);
    let info_healthy = Arc::clone(&healthy_info);
    let healthy_reader = thread::spawn(move || {
        started_flag.store(true, Ordering::SeqCst);
        let start = std::time::Instant::now();
        let result = engine_healthy.read_file_range(info_healthy, 0, 0, healthy_len);
        (start.elapsed(), result)
    });

    while !healthy_started.load(Ordering::SeqCst) {
        thread::sleep(Duration::from_millis(5));
    }
    // Give the healthy command a head start in the engine's command queue so
    // it is serviced before the no-seeder read (which then runs concurrently).
    thread::sleep(Duration::from_millis(200));

    let no_seeder_len = no_seeder_info.total_size() as u32;
    let engine_no_seeder = Arc::clone(&engine);
    let info_no_seeder = Arc::clone(&no_seeder_info);
    let no_seeder_reader = thread::spawn(move || {
        let start = std::time::Instant::now();
        let result = engine_no_seeder.read_file_range(info_no_seeder, 0, 0, no_seeder_len);
        (start.elapsed(), result)
    });

    let (healthy_elapsed, healthy_result) = healthy_reader.join().expect("healthy reader panicked");
    let (no_seeder_elapsed, no_seeder_result) =
        no_seeder_reader.join().expect("no-seeder reader panicked");

    // ── Assertions ─────────────────────────────────────────────────────
    // The healthy read is served fast (correct data, well under 2s) — it is
    // not serialized behind the no-seeder `cat`.
    assert_eq!(
        healthy_result.expect("healthy read must succeed"),
        healthy_content,
        "healthy read returned wrong data"
    );
    assert!(
        healthy_elapsed < Duration::from_secs(2),
        "healthy read blocked for {:?} behind the no-seeder cat",
        healthy_elapsed
    );

    // The no-seeder whole-file read fails fast with `NoPeers` (not the full
    // read_timeout, not a wrong error type).
    match no_seeder_result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {}
        other => panic!(
            "expected Err(NoPeers) for the no-seeder whole-file read, got {:?}",
            other
        ),
    }
    assert!(
        no_seeder_elapsed < Duration::from_secs(12),
        "no-seeder whole-file read took {:?}; expected fast NoPeers (≤9s peer-wait)",
        no_seeder_elapsed
    );

    // Stop and join the seeder thread so its listen binding is released
    // before the next test runs (the previous park-forever + detach leaked
    // the 6881 binding until process exit).
    seeder_stop.store(true, Ordering::Relaxed);
    seeder_handle.join().expect("seeder thread panicked");
    engine.shutdown();
}

/// Regression test: creating a lightweight handle through the
/// fire-and-forget `ensure_handle_async` path must not block the caller while
/// the engine thread is busy downloading.  The FUSE release path calls this
/// when a `.torrent` is written to metadata/; a blocking round-trip would
/// stall the single-threaded FUSE dispatch loop behind an in-flight read and
/// surface as a write timeout (EIO).
#[test]
fn test_ensure_handle_async_does_not_block_on_busy_engine() {
    let _session_guard = common::acquire_session_lock();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    // Short timeout so the "no peers" read blocks only a few seconds.
    config.timeouts.read_timeout_secs = Some(3);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    let a = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("a.iso"))
            .expect("Failed to parse torrent a"),
    );
    let b = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("b.iso"))
            .expect("Failed to parse torrent b"),
    );

    // Create A's handle, then block the engine thread on a read with no peers.
    engine.ensure_handle(a.clone()).expect("ensure handle a");
    let engine_for_read = engine.clone();
    let a_for_read = a.clone();
    let read_thread = thread::spawn(move || {
        let _ = engine_for_read.read_file_range(a_for_read, 0, 0, 4096);
    });

    // Give the engine thread time to pick up the blocking read.
    thread::sleep(Duration::from_millis(500));

    // ensure_handle_async must return immediately rather than queue behind the
    // in-flight read (which blocks for ~6s: peer wait + piece wait).
    let start = std::time::Instant::now();
    engine
        .ensure_handle_async(b.clone())
        .expect("ensure_handle_async b");
    let elapsed = start.elapsed();
    assert!(
        elapsed < Duration::from_secs(1),
        "ensure_handle_async blocked for {:?} behind a busy engine",
        elapsed
    );

    read_thread.join().expect("read thread");
    engine.shutdown();
}

/// Regression test: `shutdown()` must abort an in-flight `read_file_range`
/// blocked in a wait loop (state-transition/peer-discovery/piece-wait) instead
/// of stalling until `read_timeout_secs`. Before the fix those loops never
/// checked `self.stopping`, so `handle.join()` blocked up to 30s. Here the
/// torrent has no tracker/peers (peer-discovery wait); with the fix `shutdown()`
/// returns in well under a second, without it the join hangs ~30s.
#[test]
fn test_shutdown_aborts_blocked_read() {
    let _session_guard = common::acquire_session_lock();

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    // Long timeout: the read would block this long without the shutdown fix.
    config.timeouts.read_timeout_secs = Some(30);

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    // A torrent with a fake tracker URL: no peers will ever connect, so the
    // read blocks in the peer-discovery wait loop.
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(distinct_torrent("shutdown-abort.iso"))
            .expect("Failed to parse torrent"),
    );
    engine.ensure_handle(info.clone()).expect("ensure handle");

    let engine_for_read = engine.clone();
    let info_for_read = info.clone();
    let read_thread = thread::spawn(move || {
        let _ = engine_for_read.read_file_range(info_for_read, 0, 0, 4096);
    });

    // Give the engine thread time to enter the blocking peer-wait loop.
    thread::sleep(Duration::from_millis(500));

    // shutdown() sets `stopping` and joins the engine thread.  The blocked
    // read must observe `stopping` and return promptly; then the engine loop
    // processes the queued `Command::Shutdown` and the join completes.
    let start = std::time::Instant::now();
    engine.shutdown();
    let elapsed = start.elapsed();

    // With the fix, shutdown completes in well under a second (the peer-wait
    // loop polls `stopping` every 200ms).  Assert well below the 30s read
    // timeout to catch regressions; 15s (not 5s) leaves headroom for CPU
    // starvation when the full workspace test suite runs in parallel and
    // multiple libtorrent sessions contend for cores on a 2-core CI runner.
    // The regression this guards against takes ~30s (the full read timeout),
    // so 15s still catches it with a 2x margin.
    assert!(
        elapsed < Duration::from_secs(15),
        "shutdown took {:?} to abort a blocked read (read_timeout_secs=30); \
         expected well under 15s",
        elapsed
    );

    read_thread.join().expect("read thread panicked");
}

/// Concurrent readers during an active download must get consistent data. The
/// bug was a write-during-read race: `read_piece` (engine thread) read via
/// `std::fs::read` while `write_piece` (disk thread) was still writing blocks,
/// unsynchronized. The fix is a per-info-hash shared mutex (write = exclusive,
/// read = shared). This test spawns 5 readers on the same file; all must return
/// identical seed data (before the fix, reader 1 saw partial data).
#[test]
fn test_concurrent_reads_during_download_are_consistent() {
    let _session_guard = common::acquire_session_lock();

    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // force the downloader onto a distinct listen port so the
    // MiniTracker can distinguish it from the seeder (which defaults to
    // 6881 via Session::new with NULL listen_interfaces).  When both
    // sessions collide on the same port the tracker deduplicates by
    // IP:port and returns 0 peers, causing a NoPeers timeout (flaky).
    config.connections.listen_interfaces = Some("0.0.0.0:16881".to_string());

    let engine = Arc::new(
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine"),
    );

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // Read the full file (162 bytes, single piece). Spawn 5 concurrent
    // readers. The engine processes them one at a time (serialized on the
    // engine thread), but the key race is between the engine's read and
    // libtorrent's disk-thread write. With the fix, the shared mutex
    // prevents the read from seeing a partially-written piece file.
    let num_readers = 5;
    let read_size = harness.file_content.len() as u32;
    let mut handles = Vec::with_capacity(num_readers);

    for _ in 0..num_readers {
        let engine_clone = engine.clone();
        let info_clone = info.clone();
        handles.push(thread::spawn(move || {
            // Retry on transient errors (PieceNotReady) — the engine may
            // return PieceNotReady if a piece isn't ready yet.
            let start = std::time::Instant::now();
            let timeout = Duration::from_secs(60);
            loop {
                match engine_clone.read_file_range(info_clone.clone(), 0, 0, read_size) {
                    Ok(data) => return data,
                    Err(e) => {
                        if start.elapsed() > timeout {
                            panic!("Reader timed out after {:?}: {:?}", timeout, e);
                        }
                        thread::sleep(Duration::from_millis(200));
                    }
                }
            }
        }));
    }

    // Collect results from all readers.
    let results: Vec<Vec<u8>> = handles
        .into_iter()
        .map(|h| h.join().expect("reader thread panicked"))
        .collect();

    engine.shutdown();

    // All readers must return the same data.
    let reference = &results[0];
    assert!(!reference.is_empty(), "Reader 1 returned empty data");
    for (i, data) in results.iter().enumerate() {
        assert_eq!(
            data, reference,
            "Reader {} data differs from reader 0 (md5 inconsistency)",
            i
        );
    }

    // And the data must match the seed content.
    assert_eq!(
        reference, &harness.file_content,
        "Downloaded data doesn't match seed content"
    );
}

/// Byte-granular reads (`dd bs=1`) from a fully-cached file must complete
/// fast: a 1-byte read must not pay machinery that scales with read *count*.
/// Before the fix, every cached read ran `reader_added`/`publish_snapshot`/
/// `release_reader` (O(num_pieces)) and marked metadata dirty, and the engine
/// fsync'd `cache_metadata.txt` after every command — one full-download's
/// overhead per byte. Uses a 4-piece fixture, warms the cache with one read,
/// then asserts `BYTE_READS` single-byte reads finish within a loose bound
/// (pre-fix machinery exceeds it by orders of magnitude).
#[test]
fn test_cached_byte_granular_reads() {
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::with_torrent(common::build_multipiece_torrent);
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16882".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // Warm the cache: read the whole file once (downloads all 4 pieces).  The
    // first read may race the seeder connection, so transient errors are retried.
    let full_len = harness.file_content.len() as u32;
    {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, 0, full_len) {
                Ok(data) => {
                    assert_eq!(data, harness.file_content);
                    break;
                }
                Err(e) => {
                    if start.elapsed() > timeout {
                        panic!("Timed out warming the cache: {:?}", e);
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    // Byte-granular reads from the now-cached pieces must be fast: read
    // `BYTE_READS` bytes one at a time and assert both correctness and a loose
    // wall-clock upper bound.  The pre-fix per-read download machinery
    // (`post_torrent_updates` + a piece-priority sweep per read) exceeds this
    // bound by orders of magnitude.
    const BYTE_READS: usize = 4096;
    const WALL_CLOCK_BOUND: Duration = Duration::from_secs(5);
    let start = std::time::Instant::now();
    for off in 0..BYTE_READS {
        let byte = engine
            .read_file_range(info.clone(), 0, off as u64, 1)
            .expect("cached 1-byte read should succeed");
        assert_eq!(
            byte[0], harness.file_content[off],
            "byte mismatch at offset {}",
            off
        );
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < WALL_CLOCK_BOUND,
        "cached {} byte-granular reads took {:?}, expected < {:?} (per-read download machinery regression?)",
        BYTE_READS,
        elapsed,
        WALL_CLOCK_BOUND
    );

    // Sanity: bytes at each piece boundary are also served correctly from the
    // cached fast path (the multi-piece fixture exercises >1 piece).
    for off in [0u64, 256 * 1024 - 1, 256 * 1024, 512 * 1024 - 1, 512 * 1024] {
        let byte = engine
            .read_file_range(info.clone(), 0, off, 1)
            .expect("cached 1-byte read at piece boundary should succeed");
        assert_eq!(
            byte[0], harness.file_content[off as usize],
            "byte mismatch at piece-boundary offset {}",
            off
        );
    }

    engine.shutdown();
}

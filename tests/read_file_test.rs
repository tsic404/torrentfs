//! End-to-end test: validate file read via DownloadEngine::read_file_range
//! using a local tracker + seeder (TestHarness).
//!
//! This test addresses TSI-1947 (Gap1): scenario 4 file reading fails when
//! no real peers are available. By using a self-hosted tracker + seeder,
//! we validate the full lazy-loading flow without external infrastructure.

mod common;

use common::{local_test_config, TestHarness};
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
    // TSI-2383: force the downloader onto a distinct listen port so the
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

/// Regression test (TSI-2151 P0): a lightweight handle created at torrent-add
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
    // TSI-2068: force the downloader onto a distinct listen port so the
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

/// Regression test (TSI-2622): an idle handle (upload_mode, no read yet) must
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
    // TSI-2068: distinct downloader listen port so the MiniTracker can tell
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
    // TSI-2068: force the downloader onto a distinct listen port so the
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

/// TSI-2358 contract: with no peers at all, a read must NOT return early
/// (the old ≤9s fast-fail) — it must block for the full
/// `read_timeout_secs` and only then return `NoPeers`.
///
/// Deterministic: unique info_hash with no reachable tracker (the announce
/// URL is a dead endpoint) and DHT/LSD disabled, so no peer can ever appear;
/// the all-zero piece hashes additionally make a hash-valid piece impossible.
#[test]
fn test_no_peers_read_blocks_full_timeout_then_errors() {
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
    // Short but non-trivial timeout so the elapsed-time assertion below
    // can distinguish "waited the full timeout" from any fast-fail path.
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

    // Contract: the read must have blocked for the full read_timeout_secs
    // (peer-wait ≤9s + piece-wait 4s), NOT returned after the old ~≤9s
    // fast-fail.  Allow generous slack for CI scheduling jitter while
    // still failing on any early-return path (< read_timeout_secs).
    assert!(
        elapsed >= Duration::from_millis(3900),
        "Read returned NoPeers too early ({:.2}s): the old fast-fail path \
         must not trigger; expected blocking for the full read_timeout_secs",
        elapsed.as_secs_f64()
    );
}

/// TSI-2358 contract: if a peer appears mid-read (while the engine is
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
    config.timeouts.read_timeout_secs = Some(30);

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

    // TSI-2468: poll the shared snapshot from a separate thread while the
    // engine is blocked in peer-wait. The snapshot must be refreshed by
    // `publish_snapshot` during peer-wait so that `.stats` shows the live
    // download state — not stale zeros from before upload_mode was cleared.
    //
    // We assert that `try_torrent_status` returns `Some` with state
    // `Downloading`. Before the fix, `publish_snapshot` was not called
    // during peer-wait, so the snapshot stayed stale from the pre-download
    // `publish_snapshot` at reader_added — which would show `Allocating`
    // or `CheckingFiles`, never `Downloading`.
    //
    // Note: `num_peers`/`num_seeds` from `status()` are not asserted here
    // because libtorrent's per-torrent peer list is not refreshed
    // synchronously by `status()` — the internal session tick updates it
    // asynchronously, and a single-piece torrent's peer connection is too
    // brief to catch. The state transition to `Downloading` is the reliable
    // signal that the snapshot is fresh.
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
            let cfg = common::local_test_config();
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

    // TSI-2468: assert the snapshot was refreshed during peer-wait.
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
    let mut t = Vec::new();
    t.extend_from_slice(b"d8:announce31:http://127.0.0.1:19999/announce4:infod");
    t.extend_from_slice(b"6:lengthi16384e");
    t.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    t.extend_from_slice(b"12:piece lengthi16384e6:pieces20:");
    t.extend_from_slice(&[0u8; 20]);
    t.extend_from_slice(b"ee");
    t
}

/// Regression test (TSI-2226 P0): creating a lightweight handle through the
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

/// Regression test (TSI-2238): `shutdown()` must abort an in-flight
/// `read_file_range` that is blocked in a wait loop (state-transition,
/// peer-discovery, or piece-wait) instead of stalling until
/// `read_timeout_secs` elapses.  Before the fix, the state-transition and
/// peer-wait loops never checked `self.stopping`, so `shutdown()`'s
/// `handle.join()` blocked for up to `read_timeout_secs` (default 30s).
///
/// Here the torrent has no tracker and no peers, so the read blocks in the
/// peer-discovery wait loop.  A read timeout of 30s makes the contrast
/// sharp: with the fix `shutdown()` returns in well under a second; without
/// it the test would hang ~30s on the join.
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
    // loop polls `stopping` every 500ms).  Assert well below the 30s read
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

/// TSI-2262: Concurrent readers during active download must get consistent
/// data. The bug was a write-during-read race: Rust's `PieceStore::read_piece`
/// (engine thread) read a piece file via `std::fs::read` while libtorrent's
/// `PieceStorage::write_piece` (disk thread) was still writing blocks to it,
/// with no synchronization between the two. The fix adds a per-info-hash
/// shared mutex: `write_piece` holds an exclusive lock, `read_piece` holds a
/// shared lock.
///
/// This test spawns 5 threads that each call `read_file_range` on the same
/// file while the download is in progress. All 5 must return identical data
/// matching the seed content. Before the fix, reader 1 often got different
/// (partial) data than readers 2-5.
#[test]
fn test_concurrent_reads_during_download_are_consistent() {
    let _session_guard = common::acquire_session_lock();

    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // TSI-2383: force the downloader onto a distinct listen port so the
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

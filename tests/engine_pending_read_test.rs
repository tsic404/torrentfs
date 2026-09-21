//! Regression tests for reads parked on the swarm.
//!
//! A no-seeder read spends its peer-discovery window (up to
//! `PEER_WAIT_CAP_SECS`) waiting for a seeder that will never arrive.  While
//! that wait ran inline on the engine thread, every other command on the same
//! mount — including reads of healthy, fully cached torrents — queued behind
//! it.  The fix parks such a read on the engine's pending-read queue and polls
//! it, so the engine keeps serving commands during the window.  Parking also
//! means several reads can be in flight on one torrent at once, so each holds
//! its own reader id and releases exactly that reader.
//!
//! Ignored by default: they need a local tracker and spend ~10-13s of real
//! wall-clock in the peer-wait window.  Run with
//! `cargo test --test engine_pending_read_test -- --ignored`.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    acquire_session_lock, build_multipiece_torrent, create_test_torrent_with_tracker,
    local_test_config, MiniTracker,
};
use torrentfs::download::DownloadEngine;

/// While a no-seeder read is parked in its peer-wait window, a synchronous
/// engine command must still be served promptly.  Pre-fix the engine thread sat
/// inside the read for the whole window, so the probe took ~9s; post-fix it is
/// served on the next engine-loop iteration.
#[test]
#[ignore = "requires local tracker; ~10s wall-clock"]
fn parked_no_seeder_read_does_not_block_engine_commands() {
    // Serialize libtorrent session creation to avoid resource contention with
    // the other tests in this binary.
    let _session_guard = acquire_session_lock();

    // ── Tracker with no seeder behind it ──────────────────────────────
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, _file_content) = create_test_torrent_with_tracker(&announce_url);
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));

    let mut config = local_test_config();
    // Large read timeout: the no-seeder read then parks in the peer-wait window
    // (capped at 9s) instead of failing fast, which is the state under test.
    config.timeouts.read_timeout_secs = Some(60);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );

    // Create the handle up front so the probe command has a torrent to report
    // on, independent of the read's own handle creation.
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    // ── Park a no-seeder read on the engine ───────────────────────────
    let reader_engine = Arc::clone(&engine);
    let reader_info = Arc::clone(&info);
    let reader = std::thread::spawn(move || reader_engine.read_file_range(reader_info, 0, 0, 50));

    // Give the read time to reach the peer-wait window.
    std::thread::sleep(Duration::from_millis(500));

    // ── Probe: a synchronous command must be served now ───────────────
    let probe_start = Instant::now();
    let served = engine.get_pieces_status(&info_hash, 1).is_ok();
    let probe_elapsed = probe_start.elapsed();

    let _ = reader.join();

    assert!(
        served,
        "piece-status command failed while a read was parked"
    );
    assert!(
        probe_elapsed < Duration::from_secs(2),
        "engine served a command only after {:.2}s: a parked no-seeder read \
         blocked the engine thread",
        probe_elapsed.as_secs_f64()
    );
}

/// Two concurrent slow reads on the *same* torrent each hold their own reader
/// registration; releasing one must not release the other's.  With the old
/// LIFO `reader_released`, the first read to finish popped the second read's
/// gradient, so the retained prefetch window belonged to the wrong read.
///
/// The reads are staggered by a second so completion order is deterministic
/// (both wait the same peer-discovery window from their own start), and the
/// access window is narrowed to zero so each read's gradient is local and the
/// two are distinguishable in `.stats`.
#[test]
#[ignore = "requires local tracker; ~13s wall-clock"]
fn concurrent_slow_reads_release_their_own_reader() {
    let _session_guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, _file_content) = build_multipiece_torrent(&announce_url);
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));

    let mut config = local_test_config();
    config.timeouts.read_timeout_secs = Some(60);
    // Zero access window: each reader's gradient covers only its own piece (plus
    // the four step-priority pieces ahead), so the two readers' gradients are
    // distinguishable in the published piece priorities.
    config.piece_priority.access_window_mb = Some(0);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    const PIECE_LEN: u64 = 256 * 1024;

    // Read A covers piece 0; read B covers piece 3.
    let engine_a = Arc::clone(&engine);
    let info_a = Arc::clone(&info);
    let read_a =
        std::thread::spawn(move || engine_a.read_file_range(info_a, 0, 0, PIECE_LEN as u32));
    std::thread::sleep(Duration::from_secs(1));
    let engine_b = Arc::clone(&engine);
    let info_b = Arc::clone(&info);
    let read_b = std::thread::spawn(move || {
        engine_b.read_file_range(info_b, 0, 3 * PIECE_LEN, PIECE_LEN as u32)
    });

    let result_a = read_a.join().expect("read A thread");
    let result_b = read_b.join().expect("read B thread");
    assert!(
        result_a.is_err() && result_b.is_err(),
        "no-seeder reads must fail with NoPeers"
    );

    // Let the engine publish a fresh snapshot after both readers released.
    std::thread::sleep(Duration::from_millis(2500));

    let (_, pieces) = engine
        .try_pieces_status(&info_hash)
        .expect("piece status after both reads");

    // Reader B finished last, so its gradient is the retained prefetch window:
    // piece 3 stays wanted while piece 0 (only reader A wanted it) does not.
    assert_eq!(
        pieces[3].priority, 7,
        "last reader's gradient must be retained"
    );
    assert_eq!(
        pieces[0].priority, 0,
        "released reader's gradient must be dropped"
    );
}

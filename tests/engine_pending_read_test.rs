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
    acquire_session_lock, build_multipiece_torrent, create_single_piece_torrent, local_test_config,
    MiniTracker, TestHarness,
};
use torrentfs::download::DownloadEngine;

/// While a no-seeder read is parked in its peer-wait window, a synchronous
/// engine command must still be served promptly.  Pre-fix the engine thread sat
/// inside the read for the whole window, so the probe was answered only once
/// the read had already finished; post-fix it is served on the next engine-loop
/// iteration, while the read is still parked.
#[test]
#[ignore = "requires local tracker; ~10s wall-clock"]
fn parked_no_seeder_read_does_not_block_engine_commands() {
    // Serialize libtorrent session creation to avoid resource contention with
    // the other tests in this binary.
    let _session_guard = acquire_session_lock();

    // ── Tracker with no seeder behind it ──────────────────────────────
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    // Own swarm: the info hash covers the torrent `name`, and libtorrent LSD
    // pairs two sessions serving one info hash on a host — a seeder for the
    // widely-seeded `final_verification.txt` (another ignored test, in this or
    // a sibling binary) serves this read before it can park, which silently
    // voids the probe below.
    let (torrent_data, _file_content) =
        create_single_piece_torrent(&announce_url, "parked_no_seeder_read!!");
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

    // Wait for the read to be *parked*, not merely started: `begin_waiting`
    // publishes the reader's elevated piece priority before the read enters its
    // peer-wait phase, so a non-zero priority is observable evidence that the
    // engine reached the state under test.  A fixed sleep cannot tell "parked"
    // from "still starting up" on a loaded host.
    let parked_by = Instant::now() + Duration::from_secs(5);
    while engine
        .try_pieces_status(&info_hash)
        .and_then(|(_, pieces)| pieces.first().map(|p| p.priority))
        .unwrap_or(0)
        == 0
    {
        assert!(
            Instant::now() < parked_by,
            "the no-seeder read never reached its peer-wait window"
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    // ── Probe: a synchronous command must be served now ───────────────
    let probe_start = Instant::now();
    let served = engine.get_pieces_status(&info_hash, 1).is_ok();
    let probe_elapsed = probe_start.elapsed();
    // The engine thread must answer *while the read is still parked*: pre-fix
    // it sat inside the read for the whole peer-wait window, so the probe was
    // answered only after the read had finished.  A wall-clock bound cannot
    // express that — on a loaded host the engine loop's own housekeeping
    // (snapshot publish) can delay the answer by seconds with nothing blocking
    // it, while a blocked engine thread answers only after the read's whole
    // ~9s window — so the condition is checked structurally.
    let read_still_parked = !reader.is_finished();

    let read_result = reader.join().expect("reader thread");

    assert!(
        served,
        "piece-status command failed while a read was parked"
    );
    // The probe only proves anything if the read really was sourceless and
    // waited: a seeder reaching this info_hash (LSD pairs sessions serving one
    // info_hash on a host) serves the read instantly and voids the assertion
    // below.
    assert!(
        read_result.is_err(),
        "a seeder served the no-seeder read (result {:?}), so nothing was \
         parked and the probe proved nothing: this test must own its info_hash",
        read_result.as_ref().map(|data| data.len())
    );
    assert!(
        read_still_parked,
        "the engine answered the probe only after {:.2}s, once the parked read \
         had already finished: the read blocked the engine thread",
        probe_elapsed.as_secs_f64()
    );
}

/// Concurrent cold reads of one info_hash share a single discovery window
/// (single flight), and that window retires when it ends.
///
/// Two end-to-end observable properties, each discriminating against a
/// different regression:
/// * a reader joining *inside* the window fails at the shared deadline instead
///   of opening a window of its own (pre-flight, N concurrent readers ran N
///   independent probes);
/// * a reader arriving *after* the window gets a fresh flight — a new
///   `force_reannounce` and a fresh window — instead of failing instantly
///   against an expired one.  Retaining the expired flight would leave a seeder
///   that came online after the probe unreachable until libtorrent's own
///   announce schedule fired, turning intermittent ENODATA into persistent
///   `NoPeers`.
#[test]
#[ignore = "requires local tracker; ~22s wall-clock"]
fn concurrent_cold_reads_share_one_discovery_window() {
    let _session_guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    // A name unique to this test, so the info hash — and therefore the swarm —
    // is this test's own; the tracker behind it returns no peers for it.
    let (torrent_data, _file_content) =
        create_single_piece_torrent(&announce_url, "cold_flight_window!!");
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );

    let mut config = local_test_config();
    // Large read timeout: a no-seeder read parks in the peer-wait window
    // (capped at `PEER_WAIT_CAP_SECS`) instead of failing fast.
    config.timeouts.read_timeout_secs = Some(60);
    // Only an empty swarm routes a read through the shared flight, and every
    // assertion below reads "the swarm stayed empty for the whole window".  LSD
    // pairs *any* session on the host serving this info hash — including a
    // concurrent run of this very test in another process, which a parallel
    // suite produces — and that peer puts the joining read on the no-seeder
    // piece-wait window (`NO_SEEDER_READ_TIMEOUT_SECS`) instead of the flight.
    // Disable it so the tracker's empty peer list is the only discovery path.
    config.local_discovery.lsd_enabled = Some(false);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    /// One spawned read's result, plus the instants that tell a *shared* flight
    /// deadline apart from a window of the reader's own.
    struct ReadOutcome {
        result: torrentfs::TorrentResult<Vec<u8>>,
        finished_at: Instant,
        elapsed: Duration,
    }

    fn spawn_read(
        engine: &Arc<DownloadEngine>,
        info: &Arc<torrentfs::TorrentInfo>,
    ) -> std::thread::JoinHandle<ReadOutcome> {
        let reader_engine = Arc::clone(engine);
        let reader_info = Arc::clone(info);
        std::thread::spawn(move || {
            let start = Instant::now();
            let result = reader_engine.read_file_range(reader_info, 0, 0, 4096);
            ReadOutcome {
                result,
                finished_at: Instant::now(),
                elapsed: start.elapsed(),
            }
        })
    }

    // ── Five concurrent cold reads on one torrent ─────────────────────
    let readers: Vec<_> = (0..5).map(|_| spawn_read(&engine, &info)).collect();

    // A reader joining while the shared window is still running.  It must
    // observe the flight's deadline, so it gives up with the readers that
    // started the flight, not a window of its own later.
    const JOIN_DELAY: Duration = Duration::from_secs(6);
    std::thread::sleep(JOIN_DELAY);
    let joiner = spawn_read(&engine, &info);

    let reader_outcomes: Vec<ReadOutcome> = readers
        .into_iter()
        .map(|reader| reader.join().expect("reader thread"))
        .collect();
    for outcome in &reader_outcomes {
        assert!(
            outcome.result.is_err(),
            "a no-seeder cold read must fail with NoPeers"
        );
    }
    let joiner = joiner.join().expect("joiner thread");
    assert!(
        joiner.result.is_err(),
        "a no-seeder cold read must fail with NoPeers"
    );

    // The readers started the flight, so the instant they gave up *is* the
    // shared deadline.  A joiner attached to that flight gives up in the same
    // engine-loop poll; one that opened a window of its own trails them by a
    // whole window.  Comparing the two instants states "shares the flight
    // deadline" directly — a wall-clock bound on the joiner alone cannot, since
    // a loaded host stretches every wait together.
    let shared_deadline = reader_outcomes
        .iter()
        .map(|outcome| outcome.finished_at)
        .max()
        .expect("five readers ran");
    let readers_elapsed = reader_outcomes
        .iter()
        .map(|outcome| outcome.elapsed)
        .max()
        .expect("five readers ran");
    let trailing = joiner
        .finished_at
        .saturating_duration_since(shared_deadline);
    assert!(
        trailing < Duration::from_secs(2),
        "a reader joining {:.0}s into the window gave up {:.1}s after the \
         readers that started the flight (it waited {:.1}s, they {:.1}s); it \
         must share the info_hash's flight deadline, not open its own window",
        JOIN_DELAY.as_secs_f64(),
        trailing.as_secs_f64(),
        joiner.elapsed.as_secs_f64(),
        readers_elapsed.as_secs_f64()
    );

    // ── A reader arriving after the window elapsed ────────────────────
    // The flight retired with the window, so this read must probe again: a
    // fresh `force_reannounce` and a fresh window.
    let recovery_start = Instant::now();
    let recovery = engine.read_file_range(info.clone(), 0, 0, 4096);
    let recovery_elapsed = recovery_start.elapsed();
    assert!(
        recovery.is_err(),
        "no seeder is reachable, so the read must fail"
    );
    assert!(
        recovery_elapsed >= Duration::from_secs(8),
        "a reader arriving after the window waited only {:.1}s; the expired \
         flight must be retired so this read probes again instead of failing \
         instantly",
        recovery_elapsed.as_secs_f64()
    );
}

/// The acceptance path: concurrent cold reads of one torrent must all succeed
/// with the seeded bytes once a seeder is reachable — no ENODATA.
///
/// Same shape as the QA repro (cold handle, several readers on one info_hash)
/// but against the harness's real tracker + seeder, so the success path the
/// fix must not regress has automated coverage.
#[test]
#[ignore = "requires process-internal tracker + seeder; ~20s wall-clock"]
fn concurrent_cold_reads_all_succeed_with_a_seeder() {
    let _session_guard = acquire_session_lock();

    let harness = TestHarness::with_torrent(|announce_url| {
        create_single_piece_torrent(announce_url, "cold_success_path!!")
    });

    let mut config = local_test_config();
    // Ephemeral listen port, matching the seeder's (see `TestHarness`): a
    // fixed port is shared with the downloader of every other test binary
    // (`small_cache_read_test`, `multifile_past_eof_test` both pin 16883), and
    // cargo runs those binaries in parallel — the loser gets `AddrInUse`.  The
    // OS guarantees a port distinct from the seeder's, so the tracker still
    // tells the two apart by IP:port.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    const READ_SIZE: usize = 4096;
    let readers: Vec<_> = (0..5)
        .map(|_| {
            let reader_engine = Arc::clone(&engine);
            let reader_info = Arc::clone(&info);
            std::thread::spawn(move || reader_engine.read_file_range(reader_info, 0, 0, 4096))
        })
        .collect();

    for reader in readers {
        let data = reader
            .join()
            .expect("reader thread")
            .expect("a cold read with a reachable seeder must not return ENODATA");
        assert_eq!(
            data,
            harness.file_content[..READ_SIZE],
            "downloaded bytes must match the seeded content"
        );
    }
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

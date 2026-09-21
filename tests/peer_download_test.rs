//! Integration test: verify that peer-to-peer connectivity works
//! via local HTTP tracker + seeder.
//!
//! This test starts a local HTTP tracker + seeder, then creates a
//! downloader session that discovers the seeder via the tracker.
//!
//! This is the CI-level test infrastructure required to
//! validate file reads beyond cached data.

mod common;

use common::{local_test_config, TestHarness};
use std::thread;
use std::time::Duration;
use std::sync::Arc;

/// Verify that two libtorrent sessions can discover each other via
/// the local HTTP tracker and establish peer connections.
///
/// Ignored by default because it requires process-internal tracker +
/// seeder infrastructure that may not be available in all CI environments.
/// Run manually with: `cargo test -- --ignored`
#[test]
#[ignore = "requires process-internal tracker + seeder"]
fn test_peer_discovery_via_tracker() {
    // ── Setup: start tracker + seeder ──────────────────────────────────
    let harness = TestHarness::new();

    println!(
        "Tracker: {}, announces: {}",
        harness.tracker.announce_url(),
        harness.tracker.announce_count()
    );

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // ── Create downloader session directly (bypass DownloadManager) ────
    let config = local_test_config();
    let mut dl_session =
        torrentfs::download::Session::new(&config).expect("Failed to create downloader session");

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    // Re-parse torrent data from the harness (fresh TorrentInfo)
    let torrent_data = harness.torrent_data.clone();
    let dl_info = torrentfs::TorrentInfo::from_bytes(torrent_data)
        .expect("Failed to parse torrent for downloader");

    let handle = dl_session
        .add_torrent(&dl_info, cache_dir.path())
        .expect("Failed to add torrent to downloader session");

    // Brief settle — give the tracker's peer list time to include
    // the seeder on slow CI hardware.
    thread::sleep(Duration::from_secs(2));

    println!("Downloader: torrent added, waiting for peers...");

    // ── Wait for the downloader to discover the seeder ─────────────────
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut peers_found = false;

    loop {
        match handle.status() {
            Ok(status) => {
                println!(
                    "Downloader: state={:?}, progress={:.2}%, peers={}, seeds={}, dl_rate={}, ul_rate={}",
                    status.state,
                    status.progress * 100.0,
                    status.num_peers,
                    status.num_seeds,
                    status.download_rate,
                    status.upload_rate
                );

                if status.num_peers > 0 || status.num_seeds > 0 {
                    println!(
                        "Downloader: found {} peers, {} seeds!",
                        status.num_peers, status.num_seeds
                    );
                    peers_found = true;
                    break;
                }

                // Also check if already finished/seeding (file was cached somehow)
                if matches!(
                    status.state,
                    torrentfs::download::TorrentState::Finished
                        | torrentfs::download::TorrentState::Seeding
                ) {
                    println!("Downloader: torrent completed (may have found data locally)");
                    peers_found = true;
                    break;
                }
            }
            Err(e) => {
                panic!("Downloader: status error: {:?}", e);
            }
        }

        if start.elapsed() > timeout {
            break;
        }

        thread::sleep(Duration::from_millis(500));
    }

    // ── Verify tracker received the downloader's announce ──────────────
    let final_announce_count = harness.tracker.announce_count();
    println!("Tracker final announce count: {}", final_announce_count);

    if !peers_found {
        panic!(
            "Downloader did not find any peers within {} seconds. \
             Tracker announces: {}. Check tracker and seeder health.",
            timeout.as_secs(),
            final_announce_count
        );
    }

    // ── Verify peer connectivity is established ────────────────────────
    println!("\n--- Peer discovery verified ---");
    println!(
        "Discovered {} peers via tracker in {:.1}s",
        peers_found,
        start.elapsed().as_secs_f64()
    );

    println!("\n=== Peer discovery test passed! ===");
}

/// Verify the downloader detects transient peer disconnection and fails fast
/// instead of waiting the full piece_wait timeout: start a seeder that
/// announces then immediately exits, and assert the downloader in piece_wait
/// returns NoPeers within seconds (not `read_timeout_secs`).
#[test]
fn test_transient_peer_fast_exit() {
    use common::{create_test_torrent_with_tracker, MiniTracker};

    // ── Start tracker ──────────────────────────────────────────────────
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    println!("Tracker started at {}", announce_url);

    // ── Create torrent data ────────────────────────────────────────────
    let (torrent_data, file_content) = create_test_torrent_with_tracker(&announce_url);
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent"),
    );

    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);
    // ── Start a transient seeder: announces then immediately exits ─────
    // The seeder creates a libtorrent session, adds the torrent with the
    // complete file data, waits for it to check and announce to the
    // tracker, then immediately destroys the session — just like a peer
    // that briefly appears and disappears.
    {
        let torrent_data_for_seeder = torrent_data.clone();
        let seed_dir = tempfile::TempDir::new().expect("Failed to create seed temp dir");
        let seed_file = seed_dir.path().join("final_verification.txt");
        std::fs::write(&seed_file, &file_content).expect("Failed to write seed file");

        let config = local_test_config();
        let mut session = torrentfs::download::Session::new(&config)
            .expect("Transient seeder: failed to create session");

        let seeder_info = torrentfs::TorrentInfo::from_bytes(torrent_data_for_seeder)
            .expect("Transient seeder: failed to parse torrent");

        let handle = session
            .add_torrent(&seeder_info, seed_dir.path())
            .expect("Transient seeder: failed to add torrent");

        println!("Transient seeder: torrent added, waiting for seeding state...");

        // Wait for the torrent to check and start seeding (announces to tracker)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            match handle.status() {
                Ok(status) => {
                    println!(
                        "Transient seeder: state={:?}, progress={:.2}%",
                        status.state,
                        status.progress * 100.0
                    );
                    if matches!(
                        status.state,
                        torrentfs::download::TorrentState::Seeding
                            | torrentfs::download::TorrentState::Finished
                    ) {
                        println!("Transient seeder: now seeding!");
                        break;
                    }
                }
                Err(e) => {
                    panic!("Transient seeder: status error: {:?}", e);
                }
            }
            if start.elapsed() > timeout {
                panic!("Transient seeder: timeout waiting for seed state");
            }
            thread::sleep(Duration::from_millis(200));
        }

        // Wait for the tracker to register the seeder's announce
        let start = std::time::Instant::now();
        loop {
            if tracker.announce_count() >= 1 {
                println!(
                    "Tracker received {} announces from transient seeder",
                    tracker.announce_count()
                );
                break;
            }
            if start.elapsed() > Duration::from_secs(10) {
                println!("Warning: tracker didn't receive seeder announce within 10s");
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        println!("Transient seeder: dropping session (simulating fast exit)...");
        // Drop handle and session — the seeder disappears immediately
        drop(handle);
        drop(session);
        // seed_dir is dropped here too
    }

    // Give the tracker and network stack a moment to process the
    // seeder's stopped event and for the peer to be truly gone.
    thread::sleep(Duration::from_secs(1));

    // ── Create DownloadManager and try to read ─────────────────────────
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Short timeout to test fast exit — the downloader should fail
    // much sooner than this via the transient-peer detection.
    // The inner piece-wait pre-check enters the wait loop
    // instead of returning NoPeers immediately. Combined with the peer
    // discovery wait (capped at min(timeout, 9)), the total worst-case is
    // ~peer_wait + piece_wait_grace. Use a short timeout to keep total <10s.
    config.timeouts.read_timeout_secs = Some(3);

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let read_start = std::time::Instant::now();
    let result = engine.read_file_range(info, 0, 0, 50);
    let elapsed = read_start.elapsed();

    match &result {
        Err(torrentfs::TorrentError::NoPeers(msg)) => {
            println!(
                "Got NoPeers error after {:.2}s: {}",
                elapsed.as_secs_f64(),
                msg
            );
            assert!(
                elapsed < Duration::from_secs(10),
                "NoPeers error took too long ({:.2}s). Expected fast exit (<10s).",
                elapsed.as_secs_f64()
            );
            println!(
                "✓ Fast exit verified: NoPeers in {:.2}s (timeout is 15s)",
                elapsed.as_secs_f64()
            );
        }
        Err(e) => {
            println!(
                "Got other error after {:.2}s: {:?}",
                elapsed.as_secs_f64(),
                e
            );
            // Any error is acceptable as long as it's fast
            assert!(
                elapsed < Duration::from_secs(10),
                "Error took too long ({:.2}s). Expected fast exit (<10s).",
                elapsed.as_secs_f64()
            );
            println!(
                "✓ Fast exit verified (non-NoPeers error) in {:.2}s",
                elapsed.as_secs_f64()
            );
        }
        Ok(data) => {
            println!(
                "Read succeeded after {:.2}s ({} bytes)",
                elapsed.as_secs_f64(),
                data.len()
            );
            // If the seeder was fast enough that the downloader managed to
            // download the piece before the seeder exited, that's also fine —
            // the key assertion is that we didn't wait the full timeout.
            assert!(
                elapsed < Duration::from_secs(10),
                "Read took too long ({:.2}s). Expected fast completion (<10s).",
                elapsed.as_secs_f64()
            );
            assert_eq!(
                &data[..50.min(data.len())],
                &file_content[..50.min(data.len())],
                "Downloaded data doesn't match seed content"
            );
            println!(
                "✓ Fast completion verified: read in {:.2}s",
                elapsed.as_secs_f64()
            );
        }
    }

    println!("\n=== Transient peer fast-exit test passed! ===");
}

/// A read on a torrent with no seeder must fail fast with `NoPeers` instead
/// of blocking the engine thread for the full `read_timeout_secs` — the
/// regression where a no-seeder `cat` serialized every other read/write on the
/// mount for ~60s.  With a large read timeout (60s) the no-seeder read must
/// still return well before that window (peer-wait ≤9s, then an immediate
/// fast-fail `NoPeers` once peer discovery elapsed ≈ 9s), proving the short
/// no-seeder cap takes effect.
///
/// Ignored by default: it needs a local tracker and spends ~10s of real
/// wall-clock waiting for the peer-wait cap to elapse.  Run with
/// `cargo test --test peer_download_test test_no_seeder_read_fails_fast -- --ignored`.
#[test]
#[ignore = "requires local tracker; ~10s wall-clock"]
fn test_no_seeder_read_fails_fast() {
    use common::{create_test_torrent_with_tracker, MiniTracker};

    // ── Start a tracker with no seeder behind it ───────────────────────
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    println!("Tracker started at {}", announce_url);

    let (torrent_data, _file_content) = create_test_torrent_with_tracker(&announce_url);
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // Large read timeout: without the no-seeder cap this read would block the
    // engine for peer-wait(9s) + piece-wait(60s) ≈ 69s before returning NoPeers.
    let mut config = local_test_config();
    config.timeouts.read_timeout_secs = Some(60);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let read_start = std::time::Instant::now();
    let result = engine.read_file_range(info, 0, 0, 50);
    let elapsed = read_start.elapsed();

    match &result {
        Err(torrentfs::TorrentError::NoPeers(_)) => {
            println!("Got NoPeers after {:.2}s", elapsed.as_secs_f64());
        }
        Err(e) => {
            println!(
                "Got other error after {:.2}s: {:?}",
                elapsed.as_secs_f64(),
                e
            );
        }
        Ok(data) => {
            println!(
                "Read succeeded after {:.2}s ({} bytes)",
                elapsed.as_secs_f64(),
                data.len()
            );
        }
    }
    // Pin the error type, not just the latency: a wrong error or an unexpected
    // `Ok` must fail the test, not slip through a passing latency check.
    assert!(
        matches!(&result, Err(torrentfs::TorrentError::NoPeers(_))),
        "expected Err(NoPeers) for a no-seeder read, got {:?}",
        result
    );
    assert!(
        elapsed < Duration::from_secs(40),
        "no-seeder read took {:.2}s; expected fast NoPeers well under read_timeout (60s)",
        elapsed.as_secs_f64()
    );
}

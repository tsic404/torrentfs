//! Design acceptance case TC-seeding-01: a peer downloads correct bytes from
//! a seeding instance (`docs/03-acceptance.md` §F AC-50).
//!
//! Absorbed `tests/peer_download_test.rs::test_peer_discovery_via_tracker`
//! (the old copy is deleted): the downloader discovers the seeder via the
//! local tracker and reads the exact seeded content — i.e. the seeding
//! instance uploaded the correct bytes to the swarm.

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
    TestHarness,
};
use std::thread;
use std::time::{Duration, Instant};

pub const TC_ID: &str = "TC-seeding-01";

#[test]
fn tc_seeding_01_peer_downloads_correct_bytes_from_seeder() {
    let _slot = wave_slot();
    let harness = TestHarness::new();

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // Downloader session discovers the seeder via the shared tracker.
    let config = local_test_config();
    let mut dl_session =
        torrentfs::download::Session::new(&config).expect("Failed to create downloader session");

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    let torrent_data = harness.torrent_data.clone();
    let dl_info = torrentfs::TorrentInfo::from_bytes(torrent_data)
        .expect("Failed to parse torrent for downloader");

    let handle = dl_session
        .add_torrent(&dl_info, cache_dir.path())
        .expect("Failed to add torrent to downloader session");

    // Brief settle — give the tracker's peer list time to include
    // the seeder on slow CI hardware.
    thread::sleep(Duration::from_secs(2));

    // ── Wait for the downloader to discover the seeder ─────────────────
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut peers_found = false;

    loop {
        match handle.status() {
            Ok(status) => {
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

    let final_announce_count = harness.tracker.announce_count();
    assert!(
        peers_found,
        "Downloader did not find any peers within {} seconds. Tracker announces: {}",
        timeout.as_secs(),
        final_announce_count
    );

    // The downloader must also be able to read the full seeded content — this
    // is the observable "another peer obtains the correct bytes" criterion.
    let download_dir = tempfile::TempDir::new().expect("temp dir");
    let content: Vec<u8> = {
        // Wait for the download to complete.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            match handle.status() {
                Ok(status) => {
                    if matches!(
                        status.state,
                        torrentfs::download::TorrentState::Finished
                            | torrentfs::download::TorrentState::Seeding
                    ) {
                        break;
                    }
                }
                Err(_) => break,
            }
            if std::time::Instant::now() > deadline {
                panic!("downloader did not complete the transfer in time");
            }
            thread::sleep(Duration::from_millis(500));
        }
        let path = cache_dir.path().join("final_verification.txt");
        let content = std::fs::read(&path)
            .unwrap_or_else(|_| panic!("downloaded file missing at {:?}", path));
        assert_eq!(
            content, harness.file_content,
            "downloaded bytes must equal the seeded content"
        );
        content
    };
    assert_eq!(content.len(), 16384);
    let _ = download_dir;

    println!("\n=== Peer upload/download verified: exact bytes ===");
}

/// Restart shape of the same criterion: the disk piece cache is the resume
/// state.  A session started onto an already-warm cache must report those
/// pieces as held, because the resumed instance is the only holder left in the
/// swarm — the joining peer can only get the bytes if the restarted instance
/// advertises them in its bitfield and uploads them.
#[test]
fn tc_seeding_01_restarted_instance_seeds_warm_cache() {
    use torrentfs::download::{DownloadEngine, Session, TorrentState};
    use torrentfs::TorrentInfo;

    let _slot = wave_slot();
    // Session creation and use must not interleave with the other tests in
    // this binary: three live sessions here, capped by the wave slot anyway.
    let _session_guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let (torrent_data, content) = build_multipiece_torrent_named(
        &tracker.announce_url(),
        "seeding-01-restart-cache.bin",
    );
    let info = std::sync::Arc::new(
        TorrentInfo::from_bytes(torrent_data.clone()).expect("Failed to parse torrent"),
    );
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Ephemeral listen port: three sessions in this test, and other test
    // binaries run concurrently on the same host.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    // ── Warm the disk cache, then withdraw every session holding the data ──
    {
        let seed_dir = tempfile::TempDir::new().expect("Failed to create seed dir");
        std::fs::write(seed_dir.path().join(info.name()), &content)
            .expect("Failed to write seed file");
        let mut seeder = Session::new(&config).expect("Failed to create seeder session");
        let seed_info = TorrentInfo::from_bytes(torrent_data.clone()).expect("parse torrent");
        let seed_handle = seeder
            .add_torrent(&seed_info, seed_dir.path())
            .expect("Failed to add seeder torrent");

        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            if let Ok(status) = seed_handle.status() {
                if matches!(
                    status.state,
                    TorrentState::Seeding | TorrentState::Finished
                ) {
                    seed_handle.force_reannounce();
                    break;
                }
            }
            assert!(
                Instant::now() < deadline,
                "the temporary seeder never passed its hash check"
            );
            thread::sleep(Duration::from_millis(100));
        }

        let engine = DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine");
        let full_len = content.len() as u32;
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, 0, full_len) {
                Ok(data) => {
                    assert_eq!(data, content, "warm read must return the seeded bytes");
                    break;
                }
                Err(e) => assert!(
                    Instant::now() < deadline,
                    "the engine never warmed the cache: {:?}",
                    e
                ),
            }
            thread::sleep(Duration::from_millis(500));
        }
        engine.shutdown();

        // The seed data leaves with the seeder: the restarted instance below is
        // the only peer that still holds the content.
        drop(seed_handle);
        drop(seeder);
        drop(seed_dir);
    }

    let pieces_dir = cache_dir.path().join("pieces").join(&info_hash);
    let complete_pieces = std::fs::read_dir(&pieces_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .filter(|e| {
                    let name = e.file_name().to_string_lossy().into_owned();
                    name.contains(":piece:") && !name.ends_with(".incomplete")
                })
                .count()
        })
        .unwrap_or(0);
    assert_eq!(
        complete_pieces, 4,
        "the warm cache must hold all four piece files under {:?}",
        pieces_dir
    );

    // ── Start a new session onto the existing cache ─────────────────────
    let restarted = DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create restarted DownloadEngine");
    restarted
        .ensure_handle(info.clone())
        .expect("Failed to create restarted handle");

    // ── A fresh peer joins and asks that instance for the content ───────
    let download_dir = tempfile::TempDir::new().expect("Failed to create download dir");
    let mut downloader = Session::new(&config).expect("Failed to create downloader session");
    let download_info = TorrentInfo::from_bytes(torrent_data.clone()).expect("parse torrent");
    let download_handle = downloader
        .add_torrent(&download_info, download_dir.path())
        .expect("Failed to add downloader torrent");

    let deadline = Instant::now() + Duration::from_secs(90);
    loop {
        match download_handle.status() {
            Ok(status)
                if matches!(status.state, TorrentState::Seeding | TorrentState::Finished) =>
            {
                break;
            }
            Ok(status) => {
                assert!(
                    Instant::now() < deadline,
                    "the peer never downloaded from the restarted instance \
                     (state {:?}, progress {:.2}%, peers {}, seeds {})",
                    status.state,
                    status.progress * 100.0,
                    status.num_peers,
                    status.num_seeds
                );
            }
            Err(e) => panic!("downloader status failed: {:?}", e),
        }
        thread::sleep(Duration::from_millis(200));
    }

    let downloaded = std::fs::read(download_dir.path().join(info.name()))
        .expect("the peer must have written the downloaded file");
    assert_eq!(
        downloaded, content,
        "the restarted instance must serve the cached pieces byte-for-byte"
    );

    // AC-50's second half: the restarted instance's upload counter grows.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut uploaded = 0i64;
    while Instant::now() < deadline {
        uploaded = restarted.snapshot_stats().total_uploaded;
        if uploaded > 0 {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        uploaded > 0,
        "the restarted instance must report uploaded bytes, got {}",
        uploaded
    );

    restarted.shutdown();
}

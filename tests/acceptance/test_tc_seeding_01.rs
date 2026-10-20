//! Design acceptance case TC-seeding-01: a peer downloads correct bytes from
//! a seeding instance (`docs/03-acceptance.md` §F AC-50).
//!
//! Absorbed `tests/peer_download_test.rs::test_peer_discovery_via_tracker`
//! (the old copy is deleted): the downloader discovers the seeder via the
//! local tracker and reads the exact seeded content — i.e. the seeding
//! instance uploaded the correct bytes to the swarm.

use crate::common::{local_test_config, wave_slot, TestHarness};
use std::thread;
use std::time::Duration;

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

//! Design acceptance case TC-read-01: full-file read returns the exact
//! content (`docs/03-acceptance.md` §B AC-10).
//!
//! Migrated from `tests/read_file_test.rs::test_read_file_range_with_local_seeder`
//! and `::test_read_file_range_after_idle_handle` — the engine read path with
//! a real tracker + seeder.  The observable criteria: output byte count equals
//! the torrent's declared size, and the bytes are identical to the seeded
//! content.

use crate::common::{local_test_config, unique_torrent, wave_slot, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const TC_ID: &str = "TC-read-01";

/// Read the full file through the download engine with a local seeder: the
/// returned bytes must be byte-identical to the seed content and exactly the
/// seeded size.
#[test]
fn tc_read_01_full_read_matches_seed_content() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("read-01-full-read.txt"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Ephemeral downloader port: a fixed one would collide between tests
    // now that they run in parallel, while the MiniTracker must still see
    // the downloader as a peer distinct from the seeder.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    let full_len = harness.file_content.len() as u32;
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    loop {
        match engine.read_file_range(info.clone(), 0, 0, full_len) {
            Ok(data) => {
                assert_eq!(
                    data.len(),
                    full_len as usize,
                    "read byte count must equal the seeded file size"
                );
                assert_eq!(
                    data, harness.file_content,
                    "full read must be byte-identical to the seed content"
                );
                break;
            }
            Err(e) => {
                if start.elapsed() > timeout {
                    panic!(
                        "Timed out after {:.0}s reading the full file. Last error: {:?}",
                        timeout.as_secs(),
                        e
                    );
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    engine.shutdown();
}

/// The FUSE "torrent added" path creates the lightweight handle first and
/// leaves it idle; a subsequent read must still switch it to download mode
/// and fetch the exact bytes.
#[test]
fn tc_read_01_read_after_idle_handle() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("read-01-after-idle.txt"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");
    thread::sleep(Duration::from_secs(3));

    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    loop {
        match engine.read_file_range(info.clone(), 0, 0, 50) {
            Ok(data) => {
                assert!(!data.is_empty(), "Expected non-empty data");
                assert_eq!(
                    &data[..50.min(data.len())],
                    &harness.file_content[..50.min(data.len())],
                    "Downloaded data doesn't match seed content"
                );
                break;
            }
            Err(e) => {
                if start.elapsed() > timeout {
                    panic!(
                        "Timed out after {:.0}s waiting for file read after idle. Last error: {:?}",
                        timeout.as_secs(),
                        e
                    );
                }
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    engine.shutdown();
}

/// AC-10 against a peer that is not libtorrent: a peer which completes the
/// handshake, unchokes and declares every piece must be used as a data source.
///
/// The libtorrent-seeded cases above cannot prove this on their own — both
/// ends share one implementation, so a client that never acts on a foreign
/// peer's bitfield still passes them.  Here the read starts only after the
/// peer's declaration has taken effect (`.stats` reports a seed), which is the
/// order a cold mount produces: handle added idle, peer connects and declares,
/// read arrives afterwards.  A read that yields nothing fails the case.
#[test]
fn tc_read_01_read_from_protocol_peer_complete_bitfield() {
    use crate::common::{build_multipiece_torrent_named, MiniTracker};
    use crate::seed_peer::SeedPeer;

    let _slot = wave_slot();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        build_multipiece_torrent_named(&announce_url, "read-01-protocol-peer.bin");
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(torrent_data).expect("Failed to parse torrent"),
    );
    let info_hash = info.info_hash().expect("Failed to get info hash");
    let peer = SeedPeer::start(
        info_hash,
        content.clone(),
        info.piece_length() as usize,
        &announce_url,
    );

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");
    engine
        .ensure_handle(info.clone())
        .expect("Failed to ensure lightweight handle");

    let hex_hash = hex::encode(info_hash);
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(30) {
        if engine
            .try_torrent_status(&hex_hash)
            .is_some_and(|s| s.num_seeds >= 1)
        {
            break;
        }
        thread::sleep(Duration::from_millis(200));
    }
    assert!(
        engine
            .try_torrent_status(&hex_hash)
            .is_some_and(|s| s.num_seeds >= 1),
        "the peer declared every piece but never counted as a seed \
         (peer listen port {})",
        peer.port()
    );

    let full_len = content.len() as u32;
    let start = std::time::Instant::now();
    loop {
        match engine.read_file_range(info.clone(), 0, 0, full_len) {
            Ok(data) => {
                assert_eq!(
                    data.len(),
                    full_len as usize,
                    "read byte count must equal the seeded file size"
                );
                assert_eq!(data, content, "read bytes must match the seed content");
                break;
            }
            Err(e) => {
                assert!(
                    start.elapsed() <= Duration::from_secs(60),
                    "timed out reading from an unchoked, complete peer ({:?}); \
                     pieces served: {}",
                    e,
                    peer.pieces_served()
                );
            }
        }
        thread::sleep(Duration::from_secs(1));
    }
    assert!(
        peer.pieces_served() > 0,
        "the read produced bytes without the peer serving a piece"
    );
    engine.shutdown();
}

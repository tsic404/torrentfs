//! End-to-end test: the per-torrent `.stats` `-- Trackers --` section.
//!
//! The section is rendered from the download engine's shared snapshot, so this
//! test covers the whole path — torrent bytes → handle → snapshot → `.stats`
//! text — and would fail if any hop stopped carrying the announce targets.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::infrastructure::config::TorrentfsConfig;
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

#[test]
fn test_torrent_stats_lists_handle_trackers() {
    let _lock = common::acquire_session_lock();
    let temp_dir = tempfile::TempDir::new().unwrap();

    // Unique torrent name: libtorrent LSD pairs sessions on one host by info
    // hash, so a name no other test uses keeps this session's swarm to itself.
    let tracker_url = "http://127.0.0.1:6977/announce";
    let (torrent_bytes, _content) =
        common::create_single_piece_torrent(tracker_url, "tracker_stats_verify.txt");
    let info = TorrentInfo::from_bytes(torrent_bytes).unwrap();
    let info_hash = hex::encode(info.info_hash().unwrap());

    let mut db = Database::open_in_memory().unwrap();
    let InsertTorrentResult::Inserted(torrent_id) = db
        .insert_torrent(
            "data",
            "tracker-stats",
            "tracker-stats.torrent",
            16384,
            &info_hash,
            1,
        )
        .unwrap()
    else {
        panic!("expected a fresh insert");
    };
    let db = Some(Arc::new(Mutex::new(db)));

    let config = TorrentfsConfig::default_config();
    let cache_dir = temp_dir.path().join("cache");
    let service = Arc::new(DownloadService::new(&cache_dir, &config).unwrap());
    service.ensure_handle_lightweight(Arc::new(info)).unwrap();

    // The handle is created on the engine thread; wait for the snapshot tick
    // that carries its tracker list.
    let deadline = Instant::now() + Duration::from_secs(15);
    while service.try_trackers(&info_hash).is_none() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }

    let stats = generate_torrent_stats(torrent_id, &info_hash, &db, &Some(service.clone()), || {
        service.get_cache_manager()
    });
    let text = String::from_utf8_lossy(&stats);

    assert!(
        text.contains("\n-- Trackers --\n"),
        "leaf .stats must carry a tracker section, got:\n{text}"
    );
    assert!(
        text.contains(&format!("\n  tier 0  {tracker_url}\n")),
        "tracker URL and tier must be listed, got:\n{text}"
    );
    // The existing per-torrent fields keep their content and order.
    assert!(
        text.contains(&format!("info_hash: {info_hash}\n")),
        "{text}"
    );
    assert!(
        text.contains("private: no  tracker_isolation: merge-eligible\n"),
        "{text}"
    );

    service.shutdown();
}

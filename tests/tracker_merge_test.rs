//! tracker merge for duplicate info_hash torrents.
//!
//! When two torrents share the same info_hash but advertise different
//! trackers, the download engine handle must merge the new trackers into
//! the existing handle and force-reannounce, so download/seeding reach
//! every site the swarm was published to. Private torrents
//! are isolated: their trackers are never cross-merged.

mod common;

use std::sync::Arc;
use std::time::Duration;

use torrentfs::infrastructure::config::TorrentfsConfig;
use torrentfs::infrastructure::download::DownloadEngine;
use torrentfs::TorrentInfo;

use common::create_test_torrent_with_tracker;

/// Two non-private torrents with the same info_hash but different announce
/// URLs: the bare `announce` key is outside the `info` dict, so the
/// info_hash is identical regardless of the tracker URL.  After merging,
/// the handle's tracker list should contain both URLs.
#[test]
fn test_merge_trackers_combines_non_private_torrents() {
    let _lock = common::acquire_session_lock();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");
    let config = TorrentfsConfig::default_config();

    let tracker_a = "http://127.0.0.1:6881/announce";
    let tracker_b = "http://127.0.0.1:6882/announce";
    let (torrent_a, _content) = create_test_torrent_with_tracker(tracker_a);
    let (torrent_b, _content) = create_test_torrent_with_tracker(tracker_b);

    let info_a = TorrentInfo::from_bytes(torrent_a.clone()).unwrap();
    let info_b = TorrentInfo::from_bytes(torrent_b.clone()).unwrap();
    let hash_a = info_a.info_hash().unwrap();
    let hash_b = info_b.info_hash().unwrap();
    assert_eq!(hash_a, hash_b, "torrents must share info_hash");

    let info_hash_hex = hex::encode(hash_a);

    let engine = DownloadEngine::new(&cache_dir, &config).unwrap();

    // Create the handle from torrent A.
    engine.ensure_handle(Arc::new(info_a)).unwrap();
    std::thread::sleep(Duration::from_millis(1200));

    // Handle starts with 1 tracker (A's).
    let trackers_before = engine.get_trackers(&info_hash_hex).unwrap();
    assert_eq!(trackers_before.len(), 1, "one tracker after first add");

    // Merge torrent B's tracker (fire-and-forget, processed before
    // get_trackers because commands are sequential on the engine thread).
    engine.merge_trackers(Arc::new(info_b)).unwrap();

    // get_trackers blocks until the engine thread responds, so the
    // MergeTrackers command (sent before it) is already processed.
    let trackers_after = engine.get_trackers(&info_hash_hex).unwrap();
    assert_eq!(
        trackers_after.len(),
        2,
        "two trackers after merge (A + B deduplicated)"
    );

    let urls: Vec<&str> = trackers_after.iter().map(|t| t.url.as_str()).collect();
    assert!(urls.contains(&tracker_a), "tracker A present");
    assert!(urls.contains(&tracker_b), "tracker B present");

    engine.shutdown();
}

/// Merging a duplicate torrent with the same tracker URL must NOT
/// double-count (dedup by URL).
#[test]
fn test_merge_trackers_idempotent_remerge() {
    let _lock = common::acquire_session_lock();
    let temp_dir = tempfile::TempDir::new().unwrap();
    let cache_dir = temp_dir.path().join("cache");
    let config = TorrentfsConfig::default_config();

    let tracker_a = "http://127.0.0.1:6883/announce";
    let (torrent_a, _content) = create_test_torrent_with_tracker(tracker_a);
    let info_a = TorrentInfo::from_bytes(torrent_a.clone()).unwrap();
    let info_hash_hex = hex::encode(info_a.info_hash().unwrap());

    let engine = DownloadEngine::new(&cache_dir, &config).unwrap();
    engine.ensure_handle(Arc::new(info_a)).unwrap();
    std::thread::sleep(Duration::from_millis(1200));

    // Merge the same torrent again — must not duplicate.
    let info_a_again = TorrentInfo::from_bytes(torrent_a.clone()).unwrap();
    engine.merge_trackers(Arc::new(info_a_again)).unwrap();
    let trackers = engine.get_trackers(&info_hash_hex).unwrap();
    assert_eq!(trackers.len(), 1, "re-merge does not duplicate");

    engine.shutdown();
}

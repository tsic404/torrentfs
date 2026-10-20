//! Design acceptance case TC-read-06: 5 concurrent full-file readers must all
//! return identical content (`docs/03-acceptance.md` §B AC-14).
//!
//! Migrated from `tests/read_file_test.rs::test_concurrent_reads_during_download_are_consistent`.

use crate::common::{local_test_config, unique_torrent, wave_slot, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const TC_ID: &str = "TC-read-06";

#[test]
fn tc_read_06_five_concurrent_reads_are_identical() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("read-06-concurrent.txt"));
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Ephemeral downloader port: a fixed one would collide between tests
    // now that they run in parallel, while the MiniTracker must still see
    // the downloader as a peer distinct from the seeder.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

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

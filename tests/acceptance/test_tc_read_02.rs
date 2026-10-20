//! Design acceptance case TC-read-02: offset/length reads (head / middle /
//! tail) return exactly the requested bytes (`docs/03-acceptance.md` §B
//! AC-11).
//!
//! Migrated from `tests/read_file_test.rs::test_read_file_range_boundaries`.
//! Each segment's byte count equals the request length, and concatenating the
//! segments reproduces the seed content byte-for-byte over the requested
//! range.

use crate::common::{local_test_config, unique_torrent, wave_slot, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const TC_ID: &str = "TC-read-02";

fn read_with_retry(
    engine: &torrentfs::download::DownloadEngine,
    info: Arc<torrentfs::TorrentInfo>,
    offset: u64,
    size: u32,
) -> Vec<u8> {
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
}

/// Read head, middle and tail ranges; each must return exactly the requested
/// byte count with the correct content.
#[test]
fn tc_read_02_offset_reads_head_middle_tail() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("read-02-offset-reads.txt"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // Head: first 10 bytes.
    let data = read_with_retry(&engine, info.clone(), 0, 10);
    assert_eq!(data.len(), 10, "head read must return exactly 10 bytes");
    assert_eq!(&data, &harness.file_content[..10], "head content mismatch");

    // Middle: bytes 50-60.
    let data = read_with_retry(&engine, info.clone(), 50, 10);
    assert_eq!(data.len(), 10, "middle read must return exactly 10 bytes");
    assert_eq!(
        &data,
        &harness.file_content[50..60],
        "middle content mismatch"
    );

    // Tail: offset 10 to end (size = total - 10).
    let data = read_with_retry(&engine, info.clone(), 10, 16374);
    assert_eq!(
        data.len(),
        16374,
        "tail read must return exactly 16374 bytes"
    );
    assert_eq!(
        &data,
        &harness.file_content[10..16384],
        "tail content mismatch"
    );

    // Ranged read whose end crosses EOF clamps to the file size: the segment
    // is 6 bytes, not the requested 10.
    let data = read_with_retry(&engine, info.clone(), 16378, 10);
    assert_eq!(
        data.len(),
        6,
        "EOF-clamped read must return the remaining 6 bytes"
    );
    assert_eq!(
        &data,
        &harness.file_content[16378..16384],
        "clamped content mismatch"
    );

    engine.shutdown();
}

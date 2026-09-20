//! Regression test: with the on-disk piece cache smaller than the torrent, a
//! whole-file read — issued as a sequence of one-piece reads, exactly how the
//! FUSE layer serves `cat` — must return the full, correct data.
//!
//! The bug made libtorrent download the *whole* torrent on a small read
//! (every piece defaulted to a download priority), evicting the just-read
//! pieces before `read_from_disk` captured them → `PieceNotReady`, and the
//! stale `have_piece` bits then re-triggered `force_recheck` on nearly every
//! read (the observed multi-minute read).  `ensure_handle` now resets every
//! piece to `dont_download`, so only the read range plus its capped forward
//! prefetch downloads and the served piece stays resident.

mod common;

use common::{local_test_config, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// Build a 16 × 256 KiB = 4 MiB single-file `.torrent` (multi-piece).  Larger
/// than [`common::build_multipiece_torrent`] (4 pieces) so a whole-file read
/// spans far more pieces than a 1 MiB cache holds.
fn build_large_torrent(announce_url: &str) -> (Vec<u8>, Vec<u8>) {
    const PIECE_LEN: usize = 256 * 1024;
    const NUM_PIECES: usize = 16;
    let total = PIECE_LEN * NUM_PIECES;

    let mut content = Vec::with_capacity(total);
    for i in 0..total {
        content.push((i as u8).wrapping_mul(31).wrapping_add(7));
    }

    let mut hashes = Vec::with_capacity(20 * NUM_PIECES);
    for p in 0..NUM_PIECES {
        use sha1_smol::Sha1;
        let mut hasher = Sha1::new();
        hasher.update(&content[p * PIECE_LEN..(p + 1) * PIECE_LEN]);
        hashes.extend_from_slice(&hasher.digest().bytes());
    }

    let mut t = Vec::new();
    t.push(b'd');
    t.extend_from_slice(b"8:announce");
    t.extend_from_slice(announce_url.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(announce_url.as_bytes());
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"6:lengthi");
    t.extend_from_slice(total.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"4:name9:large.bin");
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces");
    t.extend_from_slice(hashes.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(&hashes);
    t.extend_from_slice(b"ee");

    (t, content)
}

/// Blocking read of `[offset, offset + size)` with the repo's retry pattern:
/// the first read may race the seeder connection, so transient errors are
/// retried within a bounded window instead of failing the test.
fn read_once(
    engine: &torrentfs::download::DownloadEngine,
    info: &Arc<torrentfs::TorrentInfo>,
    offset: u64,
    size: u32,
    timeout: Duration,
) -> Vec<u8> {
    let start = std::time::Instant::now();
    loop {
        match engine.read_file_range(info.clone(), 0, offset, size) {
            // A slow piece-wait window can return a short (partial) read
            // rather than an error; only a full-length result is success.
            Ok(data) if data.len() == size as usize => return data,
            Ok(data) => {
                if start.elapsed() >= timeout {
                    panic!(
                        "timed out reading full file (got {} of {} bytes)",
                        data.len(),
                        size
                    );
                }
                thread::sleep(Duration::from_millis(500));
            }
            Err(e) => {
                if start.elapsed() >= timeout {
                    panic!("timed out reading full file: {:?}", e);
                }
                thread::sleep(Duration::from_millis(500));
            }
        }
    }
}

#[test]
fn test_small_cache_sequential_read_returns_full_data() {
    // Serialize libtorrent session creation to avoid resource contention
    // when multiple tests run in parallel within the same binary.
    let _session_guard = common::acquire_session_lock();

    let harness = TestHarness::with_torrent(build_large_torrent);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Distinct listen port so the tracker distinguishes this downloader from
    // the seeder (see `test_read_file_range_with_local_seeder`).
    config.connections.listen_interfaces = Some("0.0.0.0:16883".to_string());
    // Cache holds 1 MiB = 4 pieces; the file below spans 16 pieces.
    config.cache.cache_size = Some(1024 * 1024);

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

    // Read the whole 4 MiB file in one-piece (256 KiB) chunks — the shape of a
    // FUSE `cat`.  Each chunk spans one piece and therefore fits in the 4-piece
    // cache, so no read must fail with a missing (evicted) piece.
    const PIECE_LEN: usize = 256 * 1024;
    let timeout = Duration::from_secs(120);
    let mut offset = 0usize;
    let mut result = Vec::with_capacity(harness.file_content.len());
    while offset < harness.file_content.len() {
        let n = PIECE_LEN.min(harness.file_content.len() - offset);
        let data = read_once(&engine, &info, offset as u64, n as u32, timeout);
        assert_eq!(data, &harness.file_content[offset..offset + n]);
        result.extend_from_slice(&data);
        offset += n;
    }
    assert_eq!(result, harness.file_content);
}

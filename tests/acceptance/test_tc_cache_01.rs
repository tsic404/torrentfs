//! Design acceptance case TC-cache-01: after the first read the cache
//! directory holds the data; a second read hits the cache without re-triggering
//! a download (`docs/03-acceptance.md` §D AC-30).
//!
//! Migrated from `tests/read_file_test.rs::test_cached_byte_granular_reads`
//! and `tests/small_cache_read_test.rs`: a whole-file read downloads and
//! caches the pieces, a second read is served from the cache (fast, correct,
//! no re-download).

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, TestHarness,
};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

pub const TC_ID: &str = "TC-cache-01";

/// Warm the cache with one whole-file read, then byte-granular re-reads must
/// be served fast from the cache (correct bytes, no per-read download
/// machinery).
#[test]
fn tc_cache_01_first_read_caches_second_read_hits() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(|url| {
        build_multipiece_torrent_named(url, "cache-01-first-read-caches.bin")
    });
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // Warm the cache: read the whole file once (downloads all 4 pieces).  The
    // first read may race the seeder connection, so transient errors are retried.
    let full_len = harness.file_content.len() as u32;
    {
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, 0, full_len) {
                Ok(data) => {
                    assert_eq!(data, harness.file_content);
                    break;
                }
                Err(e) => {
                    if start.elapsed() > timeout {
                        panic!("Timed out warming the cache: {:?}", e);
                    }
                    thread::sleep(Duration::from_secs(1));
                }
            }
        }
    }

    // The cache must hold this torrent's *complete* pieces on disk.  The
    // `pieces/` directory itself is created at engine init, so a bare
    // directory-exists check would pass with zero data; assert actual
    // `{info_hash}:piece:{index}` files instead.
    let info_hash = hex::encode(info.info_hash().expect("Failed to get info hash"));
    let pieces_dir = cache_dir.path().join("pieces").join(&info_hash);
    let piece_files: Vec<String> = std::fs::read_dir(&pieces_dir)
        .map(|entries| {
            entries
                .filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().into_owned())
                .filter(|n| n.contains(":piece:") && !n.ends_with(".incomplete"))
                .collect()
        })
        .unwrap_or_default();
    assert!(
        !piece_files.is_empty(),
        "first read must write complete piece files under {:?}",
        pieces_dir
    );

    // Byte-granular reads from the now-cached pieces must be fast: read
    // `BYTE_READS` bytes one at a time and assert both correctness and a loose
    // wall-clock upper bound.  The pre-fix per-read download machinery
    // (`post_torrent_updates` + a piece-priority sweep per read) exceeds this
    // bound by orders of magnitude.
    const BYTE_READS: usize = 4096;
    const WALL_CLOCK_BOUND: Duration = Duration::from_secs(5);
    let start = std::time::Instant::now();
    for off in 0..BYTE_READS {
        let byte = engine
            .read_file_range(info.clone(), 0, off as u64, 1)
            .expect("cached 1-byte read should succeed");
        assert_eq!(
            byte[0], harness.file_content[off],
            "byte mismatch at offset {}",
            off
        );
    }
    let elapsed = start.elapsed();
    assert!(
        elapsed < WALL_CLOCK_BOUND,
        "cached {} byte-granular reads took {:?}, expected < {:?} (per-read download machinery regression?)",
        BYTE_READS,
        elapsed,
        WALL_CLOCK_BOUND
    );

    // Sanity: bytes at each piece boundary are also served correctly from the
    // cached fast path (the multi-piece fixture exercises >1 piece).
    for off in [0u64, 256 * 1024 - 1, 256 * 1024, 512 * 1024 - 1, 512 * 1024] {
        let byte = engine
            .read_file_range(info.clone(), 0, off, 1)
            .expect("cached 1-byte read at piece boundary should succeed");
        assert_eq!(
            byte[0], harness.file_content[off as usize],
            "byte mismatch at piece-boundary offset {}",
            off
        );
    }

    engine.shutdown();
}

/// Build a 16 × 256 KiB = 4 MiB single-file `.torrent` (multi-piece).  Larger
/// than the 4-piece fixture so a whole-file read spans far more pieces than a
/// 1 MiB cache holds.
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
    t.extend_from_slice(format!("4:name{}:{}", "large.bin".len(), "large.bin").as_bytes());
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

/// With the on-disk piece cache smaller than the torrent, a whole-file read —
/// issued as a sequence of one-piece reads, exactly how the FUSE layer serves
/// `cat` — must return the full, correct data.
///
/// Migrated from `tests/small_cache_read_test.rs`.  The cache holds 4 pieces;
/// the file spans 16, so serving the whole file requires the cache to evict
/// and re-download while the read is in flight — the first-read-caches and
/// subsequent pieces-hit surface of AC-30.
#[test]
fn tc_cache_01_small_cache_chunked_full_read() {
    let _session_guard = acquire_session_lock();

    let harness = TestHarness::with_torrent(build_large_torrent);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Distinct listen port so the tracker distinguishes this downloader from
    // the seeder.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    // Cache holds 1 MiB = 4 pieces; the file below spans 16 pieces.
    config.cache.cache_size = Some(1024 * 1024);

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent for downloader"),
    );

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
    engine.shutdown();
}

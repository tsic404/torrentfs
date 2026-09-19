//! Regression: reading a torrent file at large offsets (`999_999_999` and
//! both sides of the 1 GiB boundary) must return the byte-exact file content.
//!
//! This is the full download-engine read path (slow download + cached fast
//! path) exercised at the offsets where a large-offset read was reported
//! returning forged data.  The seed content is salted per piece *and* per
//! within-piece byte, so a "wrong piece" or "wrong within-piece offset" bug
//! cannot hide behind a periodic fixture.

mod common;

use common::{local_test_config, TestHarness};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

/// 256 KiB pieces — the Ubuntu ISO torrent piece length reported by QA.
const PIECE_LEN: usize = 256 * 1024;
/// Just above 1 GiB so `1 << 30` and `(1 << 30) + 4096` stay in bounds.
const TOTAL: usize = 1_074_000_000;

fn content_byte(i: usize) -> u8 {
    let piece = i / PIECE_LEN;
    let within = i % PIECE_LEN;
    let salt = (piece as u8).wrapping_mul(37).wrapping_add(11);
    salt ^ (within as u8)
}

fn build_big_torrent(announce_url: &str) -> (Vec<u8>, Vec<u8>) {
    use sha1_smol::Sha1;

    let num_pieces = TOTAL.div_ceil(PIECE_LEN);
    let mut content = Vec::with_capacity(TOTAL);
    for piece in 0..num_pieces {
        let salt = (piece as u8).wrapping_mul(37).wrapping_add(11);
        for j in 0..PIECE_LEN {
            if piece * PIECE_LEN + j >= TOTAL {
                break;
            }
            content.push(salt ^ (j as u8));
        }
    }

    let mut hashes = Vec::with_capacity(20 * num_pieces);
    for p in 0..num_pieces {
        let start = p * PIECE_LEN;
        let end = std::cmp::min(start + PIECE_LEN, TOTAL);
        let mut hasher = Sha1::new();
        hasher.update(&content[start..end]);
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
    t.extend_from_slice(TOTAL.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"4:name7:big.bin");
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
            Ok(data) => return data,
            Err(e) => {
                eprintln!("read offset={} err={:?}", offset, e);
            }
        }
        assert!(
            start.elapsed() < timeout,
            "timed out reading offset={}",
            offset
        );
        thread::sleep(Duration::from_millis(500));
    }
}

fn assert_content(data: &[u8], offset: u64, size: u32) {
    assert_eq!(data.len(), size as usize, "offset={}", offset);
    for (k, &b) in data.iter().enumerate() {
        let expected = content_byte(offset as usize + k);
        assert_eq!(
            b,
            expected,
            "offset={} byte {} (absolute {}) mismatch: got {:#04x}, want {:#04x}",
            offset,
            k,
            offset as usize + k,
            b,
            expected
        );
    }
}

#[test]
fn test_read_near_1g_boundary_returns_correct_content() {
    let _session_guard = common::acquire_session_lock();
    let harness = TestHarness::with_torrent(build_big_torrent);

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16884".to_string());
    let engine =
        torrentfs::download::DownloadEngine::new(cache_dir.path(), &config).expect("engine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("parse torrent"),
    );

    let timeout = Duration::from_secs(120);
    let gib = 1u64 << 30;

    for (offset, size) in [
        (999_999_999u64, 4096u32),
        (gib - 4096, 4096),
        (gib, 4096),
        (gib + 4096, 4096),
    ] {
        // Slow path (download), then fast path (cached).
        let data = read_once(&engine, &info, offset, size, timeout);
        assert_content(&data, offset, size);

        let cached = read_once(&engine, &info, offset, size, timeout);
        assert_content(&cached, offset, size);
    }
}

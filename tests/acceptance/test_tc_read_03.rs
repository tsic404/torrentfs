//! Design acceptance case TC-read-03: reads at offset ≥ file size are a clean
//! EOF — 0 bytes, success, no errno, no panic (`docs/03-acceptance.md` §B
//! AC-12).
//!
//! Migrated from the EOF portion of
//! `tests/read_file_test.rs::test_read_file_range_boundaries` plus the
//! multi-file past-EOF regression (`tests/multifile_past_eof_test.rs`): a read
//! past one file's EOF must return 0 bytes, never residual content from a
//! neighbouring file sharing the same piece.

use crate::common::{local_test_config, unique_torrent, wave_slot, MiniTracker, TestHarness};
use sha1_smol::Sha1;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use torrentfs::{TorrentInfo, TorrentState};

pub const TC_ID: &str = "TC-read-03";

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

/// Single-file torrent: reads at and past EOF return empty data for any
/// request size (1-byte and 4096-byte reads behave alike).
#[test]
fn tc_read_03_past_eof_returns_empty() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("read-03-past-eof.txt"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );

    // offset == file_size: empty regardless of requested size.
    let data = read_with_retry(&engine, info.clone(), 16384, 4096);
    assert!(data.is_empty(), "read at offset == file_size must be empty");

    // Past EOF, 1-byte read.
    let data = read_with_retry(&engine, info.clone(), 999_999, 1);
    assert!(data.is_empty(), "1-byte read past EOF must be empty");

    // Past EOF, large read.
    let data = read_with_retry(&engine, info.clone(), 999_999, 4096);
    assert!(
        data.is_empty(),
        "large read past EOF must be empty (bs-independent)"
    );

    engine.shutdown();
}

const PIECE_LEN: usize = 256 * 1024;
const SMALL_LEN: usize = 1000;
const BIG_LEN: usize = 4 * 1024 * 1024;

/// Build a two-file torrent: `small.txt` (1000 bytes) followed by `big.bin`
/// (4 MiB), sharing piece 0.  Returns `(torrent_bytes, small, big)`.
fn build_two_file_torrent(announce_url: &str) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let small: Vec<u8> = vec![b'x'; SMALL_LEN];
    let big: Vec<u8> = vec![b'a'; BIG_LEN];
    let content: Vec<u8> = small.iter().chain(big.iter()).copied().collect();

    let mut pieces = Vec::new();
    for chunk in content.chunks(PIECE_LEN) {
        pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
    }

    fn bencode_bytes(out: &mut Vec<u8>, s: &[u8]) {
        out.extend_from_slice(s.len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(s);
    }
    fn bencode_int(out: &mut Vec<u8>, v: i64) {
        out.push(b'i');
        out.extend_from_slice(v.to_string().as_bytes());
        out.push(b'e');
    }

    let mut t = Vec::new();
    t.push(b'd');
    bencode_bytes(&mut t, b"announce");
    bencode_bytes(&mut t, announce_url.as_bytes());
    bencode_bytes(&mut t, b"info");
    t.push(b'd');
    bencode_bytes(&mut t, b"files");
    t.push(b'l');
    for (name, size) in [
        (b"small.txt".as_slice(), SMALL_LEN),
        (b"big.bin".as_slice(), BIG_LEN),
    ] {
        t.push(b'd');
        bencode_bytes(&mut t, b"length");
        bencode_int(&mut t, size as i64);
        bencode_bytes(&mut t, b"path");
        t.push(b'l');
        bencode_bytes(&mut t, name);
        t.push(b'e');
        t.push(b'e');
    }
    t.push(b'e'); // close files list
    bencode_bytes(&mut t, b"name");
    bencode_bytes(&mut t, b"multi");
    bencode_bytes(&mut t, b"piece length");
    bencode_int(&mut t, PIECE_LEN as i64);
    bencode_bytes(&mut t, b"pieces");
    bencode_bytes(&mut t, &pieces);
    t.push(b'e'); // close info
    t.push(b'e'); // close top-level
    (t, small, big)
}

/// Stops and joins the seeder thread on drop (normal exit and panic unwind),
/// then removes the seed directory.
struct SeederGuard {
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
    _seed_dir: tempfile::TempDir,
}

impl Drop for SeederGuard {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(handle) = self.thread.take() {
            let _ = handle.join();
        }
    }
}

/// Seed both files of a two-file torrent and wait until it reaches Seeding.
fn start_seeder(announce_url: &str) -> SeederGuard {
    let (torrent_data, small, big) = build_two_file_torrent(announce_url);
    let seed_dir = tempfile::TempDir::new().expect("seed dir");
    std::fs::create_dir_all(seed_dir.path().join("multi")).expect("mkdir multi");
    std::fs::write(seed_dir.path().join("multi/small.txt"), &small).expect("write small");
    std::fs::write(seed_dir.path().join("multi/big.bin"), &big).expect("write big");

    let stop = Arc::new(AtomicBool::new(false));
    let stop_clone = Arc::clone(&stop);
    let seed_path = seed_dir.path().to_path_buf();

    let thread = thread::spawn(move || {
        let config = local_test_config();
        let mut session = torrentfs::download::Session::new(&config).expect("seeder session");
        let info = TorrentInfo::from_bytes(torrent_data).expect("parse torrent");
        let handle = session
            .add_torrent(&info, &seed_path)
            .expect("add torrent to seeder");

        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            if stop_clone.load(Ordering::SeqCst) {
                break;
            }
            if let Ok(status) = handle.status() {
                if matches!(status.state, TorrentState::Seeding | TorrentState::Finished) {
                    break;
                }
            }
            if std::time::Instant::now() > deadline {
                panic!("seeder did not reach Seeding in time");
            }
            thread::sleep(Duration::from_millis(100));
        }
        // Keep the session alive (seeding) until the test tears down.
        while !stop_clone.load(Ordering::SeqCst) {
            thread::sleep(Duration::from_millis(100));
        }
    });

    SeederGuard {
        stop,
        thread: Some(thread),
        _seed_dir: seed_dir,
    }
}

fn read_once(
    engine: &torrentfs::DownloadEngine,
    info: &Arc<TorrentInfo>,
    file_index: i32,
    offset: u64,
    size: u32,
) -> Vec<u8> {
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        match engine.read_file_range(info.clone(), file_index, offset, size) {
            Ok(d) => return d,
            Err(e) => {
                if std::time::Instant::now() > deadline {
                    panic!("timed out reading file_index={file_index} offset={offset}: {e:?}");
                }
                thread::sleep(Duration::from_secs(1));
            }
        }
    }
}

/// Multi-file torrent: a read past one file's EOF must return 0 bytes, never
/// residual content from a neighbour sharing the same piece.
#[test]
fn tc_read_03_multifile_past_eof_is_empty() {
    let _slot = wave_slot();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, _small, _big) = build_two_file_torrent(&announce_url);

    let _seeder = start_seeder(&announce_url);

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = torrentfs::DownloadEngine::new(cache_dir.path(), &config).expect("engine");
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));

    // Positive control: an in-file read of big.bin (file 1) must return the
    // full 4096 bytes of 'a', proving the reader↔seeder download path is
    // alive and the neighbour's bytes are obtainable. The empty past-EOF
    // reads below are therefore due to the file-end clamp, not to a download
    // that never happened.
    let d = read_once(&engine, &info, 1, 999_999, 4096);
    assert_eq!(
        d.len(),
        4096,
        "in-file read of big.bin must return 4096 bytes"
    );
    assert!(d.iter().all(|&b| b == b'a'), "big.bin content must be 'a'");

    // The regression under test: reading small.txt (file 0) past its EOF must
    // return empty, NOT big.bin's 'a' bytes from the shared piece.
    let d = read_once(&engine, &info, 0, SMALL_LEN as u64, 4096);
    assert!(
        d.is_empty(),
        "read at small.txt EOF must be empty, got {} bytes",
        d.len()
    );

    let d = read_once(&engine, &info, 0, 999_999, 4096);
    assert!(
        d.is_empty(),
        "read past small.txt EOF must be empty (no neighbour 'a' leak), got {} bytes",
        d.len()
    );

    // And past big.bin's EOF too.
    let d = read_once(&engine, &info, 1, BIG_LEN as u64, 4096);
    assert!(
        d.is_empty(),
        "read at big.bin EOF must be empty, got {} bytes",
        d.len()
    );

    engine.shutdown();
}

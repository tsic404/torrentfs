//! Hit accounting is per cache record: a read that has to download a piece
//! creates that piece's record, so it counts as a miss and leaves the record's
//! hit count at zero; a later read served from the record counts one hit.
//!
//! The aggregate counters (`Hits` / `Misses` in `.stats`) follow the same rule:
//! one cold read of a piece is one miss and no hit, and one cached read of it
//! is one hit with no extra miss.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use common::{acquire_session_lock, local_test_config, MiniTracker};
use sha1_smol::Sha1;
use torrentfs::download::{DownloadEngine, Session, TorrentState};
use torrentfs::TorrentInfo;

fn build_torrent(announce_url: &str, piece_len: usize, num_pieces: usize) -> (Vec<u8>, Vec<u8>) {
    let total = piece_len * num_pieces;
    let mut content = Vec::with_capacity(total);
    for i in 0..total {
        content.push((i as u8).wrapping_mul(31).wrapping_add(7));
    }
    let mut hashes = Vec::with_capacity(20 * num_pieces);
    for p in 0..num_pieces {
        let mut hasher = Sha1::new();
        hasher.update(&content[p * piece_len..(p + 1) * piece_len]);
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
    t.extend_from_slice(b"4:name9:multi.bin");
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(piece_len.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces");
    t.extend_from_slice(hashes.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(&hashes);
    t.extend_from_slice(b"ee");
    (t, content)
}

/// Run a local seeder for `torrent_data`/`content` and wait until it announced.
fn start_seeder(torrent_data: &[u8], content: &[u8], tracker: &MiniTracker) -> Arc<std::sync::Mutex<bool>> {
    let seed_dir = tempfile::TempDir::new().unwrap();
    std::fs::write(seed_dir.path().join("multi.bin"), content).unwrap();
    let stop = Arc::new(std::sync::Mutex::new(false));
    let stop_clone = Arc::clone(&stop);
    let td_clone = torrent_data.to_vec();
    thread::spawn(move || {
        let config = local_test_config();
        let mut session = Session::new(&config).unwrap();
        let info = TorrentInfo::from_bytes(td_clone).unwrap();
        let handle = session.add_torrent(&info, seed_dir.path()).unwrap();
        loop {
            if *stop_clone.lock().unwrap() {
                break;
            }
            if let Ok(s) = handle.status() {
                let _ = matches!(s.state, TorrentState::Seeding | TorrentState::Finished);
            }
            thread::sleep(Duration::from_millis(500));
        }
    });

    let start = std::time::Instant::now();
    while tracker.announce_count() < 1 {
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "seeder never announced"
        );
        thread::sleep(Duration::from_millis(200));
    }
    thread::sleep(Duration::from_secs(3));
    stop
}

/// `(aggregate hits, aggregate misses, piece hit count)` for piece 0.
fn snapshot(engine: &DownloadEngine, piece_key: &str) -> (u64, u64, u64) {
    let cm = engine.cache_manager();
    let guard = cm.lock().unwrap();
    (
        guard.hit_count,
        guard.miss_count,
        guard.piece_hit_count(piece_key),
    )
}

#[test]
fn cold_read_is_one_miss_and_no_hit() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    const PIECE_LEN: usize = 262144;
    const PROBE_LEN: u32 = 32768;
    let (torrent_data, content) = build_torrent(&announce_url, PIECE_LEN, 1);
    let stop = start_seeder(&torrent_data, &content, &tracker);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16898".to_string());
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).unwrap());
    let piece_key = format!("{}:piece:0", hex::encode(info.info_hash().unwrap()));

    let data = engine
        .read_file_range(info.clone(), 0, 0, PROBE_LEN)
        .expect("cold read");
    assert_eq!(data.len(), PROBE_LEN as usize);
    assert_eq!(&data[..], &content[..PROBE_LEN as usize]);

    let (hits, misses, piece_hits) = snapshot(&engine, &piece_key);
    assert_eq!(
        piece_hits, 0,
        "the read that downloaded the piece must not count a hit on the record it created"
    );
    assert_eq!(hits, 0, "a cold read is not a cache hit");
    assert_eq!(misses, 1, "a cold read of one piece is exactly one miss");

    let data = engine
        .read_file_range(info.clone(), 0, 0, PROBE_LEN)
        .expect("cached read");
    assert_eq!(&data[..], &content[..PROBE_LEN as usize]);

    let (hits, misses, piece_hits) = snapshot(&engine, &piece_key);
    assert_eq!(piece_hits, 1, "one cached read is one hit on the record");
    assert_eq!(hits, 1, "one cached read is exactly one aggregate hit");
    assert_eq!(misses, 1, "a cached read adds no miss");

    *stop.lock().unwrap() = true;
}

#[test]
fn rebuilt_record_starts_at_zero_then_counts_one() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    const PIECE_LEN: usize = 262144;
    const PROBE_LEN: u32 = 32768;
    let (torrent_data, content) = build_torrent(&announce_url, PIECE_LEN, 2);
    let stop = start_seeder(&torrent_data, &content, &tracker);

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16899".to_string());
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).unwrap());
    let piece_key = format!("{}:piece:0", hex::encode(info.info_hash().unwrap()));

    // Warm both pieces, then drop piece 0's record and file the way an
    // eviction does, so the next read has to build a new record.
    let data = engine
        .read_file_range(info.clone(), 0, 0, PIECE_LEN as u32 * 2)
        .expect("warm-up read");
    assert_eq!(data.len(), PIECE_LEN * 2);
    {
        let cm = engine.cache_manager();
        let mut guard = cm.lock().unwrap();
        guard.delete_piece(&piece_key).expect("drop the piece");
        assert!(!guard.has_piece(&piece_key));
    }

    // The read that rebuilds the record may end in the documented cache stall
    // (`PieceNotReady`/`Timeout`) while the piece is fetched again; either way
    // the record it creates starts at zero, so wait for the piece to land
    // instead of reading again (a read that finds it cached would count a hit).
    let _ = engine.read_file_range(info.clone(), 0, 0, PROBE_LEN);
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    loop {
        let cached = {
            let cm = engine.cache_manager();
            let guard = cm.lock().unwrap();
            guard.has_piece(&piece_key)
        };
        if cached {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the evicted piece never came back to the cache"
        );
        thread::sleep(Duration::from_millis(250));
    }
    let (_, _, piece_hits) = snapshot(&engine, &piece_key);
    assert_eq!(
        piece_hits, 0,
        "the rebuilt record must start at zero hits; the rebuilding read did not use it"
    );

    let data = engine
        .read_file_range(info.clone(), 0, 0, PROBE_LEN)
        .expect("read of the rebuilt record");
    assert_eq!(&data[..], &content[..PROBE_LEN as usize]);
    let (_, _, piece_hits) = snapshot(&engine, &piece_key);
    assert_eq!(piece_hits, 1, "the first write to the rebuilt record is one hit");

    *stop.lock().unwrap() = true;
}

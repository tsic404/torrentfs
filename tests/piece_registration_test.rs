//! Regression test: pieces served from the local disk must be registered in
//! cache metadata. Before the fix, a piece fetched eagerly by the
//! access-window prefetch was written to disk but never registered, so
//! `pieces_on_disk` kept returning `false` (forcing the slow path), and after
//! a restart it was treated as unverified and re-downloaded — timing out with
//! EIO when no peer was available.

mod common;

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

#[test]
fn test_read_registers_prefetched_pieces() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    const PIECE_LEN: usize = 262144;
    const NUM_PIECES: usize = 4;
    let (torrent_data, content) = build_torrent(&announce_url, PIECE_LEN, NUM_PIECES);

    // Seeder holding the complete file.
    let seed_dir = tempfile::TempDir::new().unwrap();
    std::fs::write(seed_dir.path().join("multi.bin"), &content).unwrap();
    let stop = Arc::new(std::sync::Mutex::new(false));
    let stop_clone = Arc::clone(&stop);
    let td_clone = torrent_data.clone();
    let seeder = thread::spawn(move || {
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
    loop {
        if tracker.announce_count() >= 1 {
            break;
        }
        assert!(start.elapsed() < Duration::from_secs(60), "seeder never announced");
        thread::sleep(Duration::from_millis(200));
    }
    thread::sleep(Duration::from_secs(3));

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16893".to_string());
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).unwrap());

    // Read the whole file in 128 KiB chunks (matches FUSE max read).
    let total = (PIECE_LEN * NUM_PIECES) as u64;
    let mut offset = 0u64;
    let mut assembled = Vec::new();
    while offset < total {
        let n = std::cmp::min(131072u64, total - offset) as u32;
        let start = std::time::Instant::now();
        let mut data = None;
        while start.elapsed() < Duration::from_secs(120) {
            match engine.read_file_range(info.clone(), 0, offset, n) {
                Ok(d) => {
                    data = Some(d);
                    break;
                }
                Err(_) => thread::sleep(Duration::from_secs(1)),
            }
        }
        let data = data.expect("read timed out");
        assert_eq!(data.len() as u64, n as u64);
        assembled.extend_from_slice(&data);
        offset += n as u64;
    }
    assert_eq!(assembled, content);

    // Every piece that was read must now be registered (verified) in the
    // cache metadata — this is the regression.
    let info_hash = hex::encode(info.info_hash().unwrap());
    let cm = engine.cache_manager();
    let guard = cm.lock().unwrap();
    for p in 0..NUM_PIECES {
        let key = format!("{}:piece:{}", info_hash, p);
        assert!(
            guard.has_piece(&key),
            "piece {} was read but is not registered in cache metadata",
            p
        );
        assert!(
            guard.is_piece_verified(&key),
            "piece {} was read but is not verified",
            p
        );
    }
    drop(guard);

    *stop.lock().unwrap() = true;
    let _ = seeder.join();
}

#[test]
fn test_background_prefetch_registers_via_piece_finished_alert() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    const PIECE_LEN: usize = 262144;
    const NUM_PIECES: usize = 4;
    let (torrent_data, content) = build_torrent(&announce_url, PIECE_LEN, NUM_PIECES);

    // Seeder holding the complete file.
    let seed_dir = tempfile::TempDir::new().unwrap();
    std::fs::write(seed_dir.path().join("multi.bin"), &content).unwrap();
    let stop = Arc::new(std::sync::Mutex::new(false));
    let stop_clone = Arc::clone(&stop);
    let td_clone = torrent_data.clone();
    let seeder = thread::spawn(move || {
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
    loop {
        if tracker.announce_count() >= 1 {
            break;
        }
        assert!(
            start.elapsed() < Duration::from_secs(60),
            "seeder never announced"
        );
        thread::sleep(Duration::from_millis(200));
    }
    thread::sleep(Duration::from_secs(3));

    let cache_dir = tempfile::TempDir::new().unwrap();
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:16895".to_string());
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).unwrap());

    // Read only piece 0. Pieces 1..=3 are then prefetched by the read-ahead
    // window and complete in the background — never through a read's
    // piece-wait loop. Their registration therefore proves the piece_finished
    // alert reached the engine (the alert_mask must include piece_progress).
    let data = engine
        .read_file_range(info.clone(), 0, 0, PIECE_LEN as u32)
        .expect("first-piece read timed out");
    assert_eq!(data.len(), PIECE_LEN);
    assert_eq!(&data[..], &content[..PIECE_LEN]);

    let info_hash = hex::encode(info.info_hash().unwrap());
    let cm = engine.cache_manager();
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let guard = cm.lock().unwrap();
        let all_verified = (1..NUM_PIECES).all(|p| {
            let key = format!("{}:piece:{}", info_hash, p);
            guard.has_piece(&key) && guard.is_piece_verified(&key)
        });
        drop(guard);
        if all_verified {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for background-prefetch piece registration"
        );
        thread::sleep(Duration::from_millis(500));
    }

    // Sanity: every piece is now verified — piece 0 via the read path,
    // pieces 1..=3 via the piece_finished alert.
    let guard = cm.lock().unwrap();
    for p in 0..NUM_PIECES {
        let key = format!("{}:piece:{}", info_hash, p);
        assert!(guard.has_piece(&key), "piece {} not registered", p);
        assert!(guard.is_piece_verified(&key), "piece {} not verified", p);
    }
    drop(guard);

    *stop.lock().unwrap() = true;
    let _ = seeder.join();
}

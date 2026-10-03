//! Regression test: pieces served from the local disk must be registered in
//! cache metadata. Before the fix, a piece fetched eagerly by the
//! access-window prefetch was written to disk but never registered, so
//! `pieces_on_disk` kept returning `false` (forcing the slow path), and after
//! a restart it was treated as unverified and re-downloaded — timing out with
//! EIO when no peer was available.

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

    // Seeder holding ONLY piece 0 (the file is truncated to one piece):
    // pieces 1..=3 must stay undownloadable until the first read has settled,
    // so no priority-zeroing path other than the piece_finished alert can run
    // for them (see the wanted-set comment below). Once the read is done the
    // test writes the full file and flips `complete_seeder`; the seeder then
    // rechecks and starts serving the remaining pieces.
    let seed_dir = tempfile::TempDir::new().unwrap();
    let seed_file = seed_dir.path().join("multi.bin");
    std::fs::write(&seed_file, &content[..PIECE_LEN]).unwrap();
    let stop = Arc::new(std::sync::Mutex::new(false));
    let stop_clone = Arc::clone(&stop);
    let complete_seeder = Arc::new(AtomicBool::new(false));
    let complete_clone = Arc::clone(&complete_seeder);
    let td_clone = torrent_data.clone();
    let seeder = thread::spawn(move || {
        let config = local_test_config();
        let mut session = Session::new(&config).unwrap();
        let info = TorrentInfo::from_bytes(td_clone).unwrap();
        let handle = session.add_torrent(&info, seed_dir.path()).unwrap();
        let mut rechecked = false;
        loop {
            if *stop_clone.lock().unwrap() {
                break;
            }
            if !rechecked && complete_clone.load(Ordering::Relaxed) {
                handle.force_recheck();
                handle.force_reannounce();
                rechecked = true;
            }
            thread::sleep(Duration::from_millis(200));
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

    let info_hash = hex::encode(info.info_hash().unwrap());

    // Read only piece 0. Pieces 1..=3 are then prefetched by the read-ahead
    // window and complete in the background — never through a read's
    // piece-wait loop. Their registration therefore proves the piece_finished
    // alert reached the engine (the alert_mask must include piece_progress).
    // The read itself is not what this case asserts, so a transient
    // NoPeers/Timeout verdict on a loaded host is retried rather than failed
    // (same bounded-retry pattern as `test_read_registers_prefetched_pieces`).
    // The wanted-set half of the verdict must attribute the priority drop to
    // the piece_finished alert, so every priority-zeroing source has to be
    // ruled out: (a) the init baseline at handle creation, (b) `recompute`
    // forcing cached/on-disk pieces to 0 on reader events — and
    // `has_piece_on_disk` checks mere file existence, so a piece only has to
    // start downloading to qualify, (c) `recompute`'s idle bulk reset when
    // nothing is wanted any more, and (d) `piece_ready`. The partial seeder
    // keeps pieces 1..=3 off disk until after `reader_released` — the last
    // reader event in this test — so (a) precedes the baseline the sampler
    // records while the read is parked, (b) finds nothing on disk for them at
    // the one remaining reader event and never runs again (no more reads, no
    // recheck without a read, and the 1 GiB default cache cannot evict four
    // pieces), (c) cannot fire because the released reader's gradient is
    // retained as the prefetch window, and only (d) remains. An absolute
    // `priority == 0` check cannot make this distinction: 0 is also the idle
    // baseline, and on a fast host (b) reaches it without any alert.
    const BASELINE_ALLOWANCE_SECS: u64 = 30;
    const FIRST_READ_ALLOWANCE_SECS: u64 = 120;
    let (data, baseline) = thread::scope(|scope| {
        let sampler = scope.spawn(|| {
            let deadline = std::time::Instant::now() + Duration::from_secs(BASELINE_ALLOWANCE_SECS);
            let mut peaks = vec![0i32; NUM_PIECES];
            loop {
                if let Ok(statuses) = engine.get_pieces_status(&info_hash, NUM_PIECES as i32) {
                    for (peak, status) in peaks.iter_mut().zip(statuses.iter()) {
                        *peak = (*peak).max(status.priority);
                    }
                    if peaks[1..].iter().all(|&p| p > 0) {
                        return Ok(peaks);
                    }
                }
                if std::time::Instant::now() >= deadline {
                    return Err(format!(
                        "pieces 1..={} never all entered the wanted set within {}s \
                         (peak priorities observed: {:?})",
                        NUM_PIECES - 1,
                        BASELINE_ALLOWANCE_SECS,
                        peaks
                    ));
                }
                thread::sleep(Duration::from_millis(50));
            }
        });

        let read_deadline =
            std::time::Instant::now() + Duration::from_secs(FIRST_READ_ALLOWANCE_SECS);
        let mut first_piece = None;
        while std::time::Instant::now() < read_deadline {
            match engine.read_file_range(info.clone(), 0, 0, PIECE_LEN as u32) {
                Ok(d) => {
                    first_piece = Some(d);
                    break;
                }
                Err(_) => thread::sleep(Duration::from_secs(1)),
            }
        }
        let data = first_piece.expect("first-piece read timed out");

        let baseline = sampler
            .join()
            .expect("baseline sampler thread panicked")
            .unwrap_or_else(|msg| panic!("{msg}"));
        (data, baseline)
    });
    assert_eq!(data.len(), PIECE_LEN);
    assert_eq!(&data[..], &content[..PIECE_LEN]);

    // Pieces 1..=3 could not be downloaded before this point — the seeder had
    // only piece 0 — so their sampled baseline is still the live priority and
    // no zeroing path has run for them yet. Complete the seeder's data now;
    // its recheck starts serving the remaining pieces.
    std::fs::write(&seed_file, &content).unwrap();
    complete_seeder.store(true, Ordering::Relaxed);

    let piece_key = |p: i32| format!("{}:piece:{}", info_hash, p);
    let cm = engine.cache_manager();

    // Two bounded polls, because the pipeline mixes a stage the design bounds
    // with stages it does not: the background download and libtorrent's
    // post-write hash verification are swarm/disk bound, while the alert ->
    // engine convergence is bounded by registered constants. Waiting on them
    // separately keeps a loaded host failing the precondition instead of
    // masquerading as an alert-path regression.

    // (1) Download precondition: libtorrent's `write_piece` writes every block
    // straight to the final piece path, so a full-size piece file is the
    // download-complete signal.
    const PREFETCH_DOWNLOAD_ALLOWANCE_SECS: u64 = 120;
    let download_deadline =
        std::time::Instant::now() + Duration::from_secs(PREFETCH_DOWNLOAD_ALLOWANCE_SECS);
    loop {
        let guard = cm.lock().unwrap();
        let is_on_disk = (1..NUM_PIECES)
            .all(|p| guard.piece_on_disk_at_least(&piece_key(p as i32), PIECE_LEN as u64));
        drop(guard);
        if is_on_disk {
            break;
        }
        assert!(
            std::time::Instant::now() < download_deadline,
            "background prefetch did not write pieces 1..={} to disk within {}s",
            NUM_PIECES - 1,
            PREFETCH_DOWNLOAD_ALLOWANCE_SECS
        );
        thread::sleep(Duration::from_millis(200));
    }

    // (2) Alert path: `drain_piece_finished` registers the piece and clears it
    // from the scheduler's wanted set. The alert's own bound is registered
    // (the consumer's missed-wakeup fallback `SAFETY_NET_TIMEOUT`, 1s, plus
    // one engine-loop round, 1s), but the piece file reaching full size is not
    // the same event as the alert: libtorrent still hash-verifies the piece on
    // its disk thread first, a step the design bounds no more than it bounds
    // the download. The allowance therefore has to absorb that verification
    // plus host scheduling slack in addition to the registered 2s.
    const ALERT_CONVERGENCE_ALLOWANCE_SECS: u64 = 120;
    let alert_deadline =
        std::time::Instant::now() + Duration::from_secs(ALERT_CONVERGENCE_ALLOWANCE_SECS);
    loop {
        let guard = cm.lock().unwrap();
        let is_registered = (1..NUM_PIECES).all(|p| {
            let key = piece_key(p as i32);
            guard.has_piece(&key) && guard.is_piece_verified(&key)
        });
        drop(guard);
        // Wanted-set cleanup = each prefetched piece dropped strictly below
        // the peak priority the sampler recorded (`piece_ready` drives it to
        // 0); the absolute value alone cannot distinguish cleanup from idle.
        let priorities = engine
            .get_pieces_status(&info_hash, NUM_PIECES as i32)
            .expect("piece status query failed");
        let is_wanted_cleared = (1..NUM_PIECES).all(|p| priorities[p].priority < baseline[p]);
        if is_registered && is_wanted_cleared {
            break;
        }
        assert!(
            std::time::Instant::now() < alert_deadline,
            "piece_finished alert did not reach the engine within {}s \
             (registered={}, priorities={:?}, baseline={:?})",
            ALERT_CONVERGENCE_ALLOWANCE_SECS,
            is_registered,
            priorities.iter().map(|s| s.priority).collect::<Vec<_>>(),
            baseline
        );
        thread::sleep(Duration::from_millis(200));
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

/// Regression: an *externally truncated* piece file (still present, but
/// shorter than its expected size) must be re-downloaded, not surfaced as
/// EIO.  The old stale check used `Path::exists`, so a truncated file counted
/// as present: `force_recheck` never fired, the piece-wait loop saw
/// `have_piece` and broke immediately, and `read_from_disk` returned
/// `PieceNotReady`.  The stale check now compares the real on-disk length, so
/// the truncated piece is treated as stale and re-fetched from the seeder.
#[test]
fn test_truncated_piece_reheals_via_recheck_and_redownload() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    const PIECE_LEN: usize = 262144;
    const NUM_PIECES: usize = 2;
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
    config.connections.listen_interfaces = Some("0.0.0.0:16897".to_string());
    let engine = DownloadEngine::new(cache_dir.path(), &config).unwrap();
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).unwrap());

    // Warm the cache: read the whole file so every piece is on disk + verified.
    let total = (PIECE_LEN * NUM_PIECES) as u64;
    let mut offset = 0u64;
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
        assert_eq!(data.expect("warm-up read timed out").len() as u64, n as u64);
        offset += n as u64;
    }

    // Truncate piece 0 externally — the file stays present, but shorter than
    // its expected size, which `Path::exists` alone cannot detect.
    let info_hash = hex::encode(info.info_hash().unwrap());
    let piece_key = format!("{}:piece:0", info_hash);
    let piece_path = {
        let cm = engine.cache_manager();
        let guard = cm.lock().unwrap();
        guard.piece_path(&piece_key)
    };
    assert!(
        std::fs::metadata(&piece_path).unwrap().len() >= PIECE_LEN as u64,
        "piece 0 should be fully cached before truncation"
    );
    std::fs::write(&piece_path, &content[..4096]).unwrap();

    // Reading the truncated piece's range must re-download it, not EIO.
    let start = std::time::Instant::now();
    let mut data = None;
    while start.elapsed() < Duration::from_secs(120) {
        match engine.read_file_range(info.clone(), 0, 0, PIECE_LEN as u32) {
            Ok(d) => {
                data = Some(d);
                break;
            }
            Err(_) => thread::sleep(Duration::from_secs(1)),
        }
    }
    let data = data.expect("truncated piece read timed out (EIO)");
    assert_eq!(&data[..], &content[..PIECE_LEN]);
    assert!(
        std::fs::metadata(&piece_path).unwrap().len() >= PIECE_LEN as u64,
        "piece 0 must be re-downloaded to its full length"
    );

    *stop.lock().unwrap() = true;
    let _ = seeder.join();
}

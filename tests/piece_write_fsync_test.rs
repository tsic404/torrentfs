//! Evidence for the piece write path's disk contract: a completed piece is
//! flushed to disk with exactly one `fsync`, and no file I/O runs inside the
//! libtorrent network thread's disk callbacks.
//!
//! The fixture is one 4 MiB file in four 1 MiB pieces (256 blocks total). A
//! block-at-a-time implementation would report 256 fsyncs; the contract is one
//! per piece. The counter diagnostic also records any file operation that a
//! disk callback performs on the network thread itself.

mod common;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{acquire_session_lock, local_test_config, MiniTracker};
use sha1_smol::Sha1;
use torrentfs::download::Session;
use torrentfs::TorrentInfo;

const PIECE_LEN: usize = 1024 * 1024;
const NUM_PIECES: usize = 4;
const BLOCK_LEN: usize = 16 * 1024;

fn build_torrent(announce_url: &str) -> (Vec<u8>, Vec<u8>) {
    let total = PIECE_LEN * NUM_PIECES;
    let content: Vec<u8> = (0..total)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect();

    let mut hashes = Vec::with_capacity(20 * NUM_PIECES);
    for piece in 0..NUM_PIECES {
        let mut hasher = Sha1::new();
        hasher.update(&content[piece * PIECE_LEN..(piece + 1) * PIECE_LEN]);
        hashes.extend_from_slice(&hasher.digest().bytes());
    }

    let mut torrent = Vec::new();
    torrent.push(b'd');
    torrent.extend_from_slice(b"8:announce");
    torrent.extend_from_slice(announce_url.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(announce_url.as_bytes());
    torrent.extend_from_slice(b"4:infod");
    torrent.extend_from_slice(b"6:lengthi");
    torrent.extend_from_slice(total.to_string().as_bytes());
    torrent.push(b'e');
    torrent.extend_from_slice(b"4:name9:multi.bin");
    torrent.extend_from_slice(b"12:piece lengthi");
    torrent.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    torrent.push(b'e');
    torrent.extend_from_slice(b"6:pieces");
    torrent.extend_from_slice(hashes.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(&hashes);
    torrent.extend_from_slice(b"ee");

    (torrent, content)
}

#[test]
fn a_download_fsyncs_once_per_piece_off_the_network_thread() {
    let _guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let (torrent_data, content) = build_torrent(&tracker.announce_url());
    let info = TorrentInfo::from_bytes(torrent_data.clone()).expect("parse torrent");
    let raw_info_hash = info.info_hash().expect("info hash");

    // Seeder: the stock libtorrent storage serves the complete file.
    let seed_dir = tempfile::TempDir::new().expect("seed dir");
    std::fs::write(seed_dir.path().join("multi.bin"), &content).expect("seed file");
    let stop = Arc::new(AtomicBool::new(false));
    let stop_seeder = Arc::clone(&stop);
    let seeder_data = torrent_data.clone();
    let seeder = thread::spawn(move || {
        let config = local_test_config();
        let mut session = Session::new(&config).expect("seeder session");
        let info = TorrentInfo::from_bytes(seeder_data).expect("seeder parse");
        let _handle = session
            .add_torrent(&info, seed_dir.path())
            .expect("seeder add torrent");
        while !stop_seeder.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(100));
        }
    });

    let wait_start = Instant::now();
    while tracker.peer_count(&raw_info_hash) < 1 {
        assert!(
            wait_start.elapsed() < Duration::from_secs(60),
            "seeder never announced to the tracker"
        );
        thread::sleep(Duration::from_millis(100));
    }

    // Downloader: the torrentfs custom piece storage.
    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    let mut session =
        Session::new_with_custom_storage(&config, cache_dir.path()).expect("downloader session");

    let mut counters = libtorrent_sys::lt_disk_io_counters_t {
        piece_fsyncs: 0,
        piece_markers: 0,
        write_blocks: 0,
        net_thread_io_calls: 0,
        net_callback_calls: 0,
        net_callback_total_us: 0,
        net_callback_max_us: 0,
        read_buffers_allocated: 0,
        read_buffers_released: 0,
        read_buffers_live: 0,
        read_buffers_dropped: 0,
        piece_publish_failures: 0,
        write_completion_errors: 0,
        write_completion_errors_delivered: 0,
    };
    unsafe { libtorrent_sys::lt_disk_io_counters_reset() };

    let handle = session
        .add_torrent(&info, cache_dir.path())
        .expect("add torrent to custom-storage session");

    let download_start = Instant::now();
    while !(0..NUM_PIECES as i32).all(|piece| handle.have_piece(piece)) {
        assert!(
            download_start.elapsed() < Duration::from_secs(120),
            "download did not complete; counters so far: {:?}",
            read_counters(&mut counters)
        );
        thread::sleep(Duration::from_millis(50));
    }

    let final_counters = read_counters(&mut counters);
    let blocks = (PIECE_LEN * NUM_PIECES / BLOCK_LEN) as u64;

    assert_eq!(
        final_counters.piece_fsyncs,
        NUM_PIECES as u64,
        "expected one fsync per piece ({}), got {}; completed pieces={} blocks={} \
         network-thread I/O calls={}",
        NUM_PIECES,
        final_counters.piece_fsyncs,
        final_counters.piece_markers,
        final_counters.write_blocks,
        final_counters.net_thread_io_calls
    );
    assert_eq!(
        final_counters.piece_markers, NUM_PIECES as u64,
        "the incomplete marker must be written once per completed piece"
    );
    assert_eq!(
        final_counters.write_blocks, blocks,
        "every 16 KiB block must go through the write path"
    );
    assert_eq!(
        final_counters.net_thread_io_calls, 0,
        "a disk callback performed file I/O on the network thread"
    );
    assert!(
        final_counters.net_callback_calls >= blocks,
        "expected one measured disk callback per block, got {}",
        final_counters.net_callback_calls
    );
    let average_callback_us =
        final_counters.net_callback_total_us / final_counters.net_callback_calls;
    assert!(
        average_callback_us < 1000,
        "network-thread disk callbacks averaged {} us (max {} us); the write path still blocks the network thread",
        average_callback_us,
        final_counters.net_callback_max_us
    );

    println!(
        "piece write counters: fsyncs={} markers={} blocks={} net_io={} callbacks={} \
         avg_callback_us={} max_callback_us={}",
        final_counters.piece_fsyncs,
        final_counters.piece_markers,
        final_counters.write_blocks,
        final_counters.net_thread_io_calls,
        final_counters.net_callback_calls,
        average_callback_us,
        final_counters.net_callback_max_us
    );

    stop.store(true, Ordering::Relaxed);
    seeder.join().expect("seeder thread");
}

fn read_counters(
    out: &mut libtorrent_sys::lt_disk_io_counters_t,
) -> &libtorrent_sys::lt_disk_io_counters_t {
    unsafe { libtorrent_sys::lt_disk_io_counters_get(out) };
    out
}

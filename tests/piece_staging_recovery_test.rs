//! Every block of a piece is written to a staging path and only published
//! under the final piece name after the last block has landed and the file has
//! been fsynced. Two observable consequences are covered here:
//!
//! - A restart that finds a full-length staging file (the last block arrived
//!   before the process stopped) must not declare that piece complete, even
//!   when hash checks are disabled and the restart scan is what builds the
//!   piece bitmap.
//! - While a piece is still being written, the final piece name never exists
//!   at a partial length.

mod common;

use std::ffi::CString;
use std::io::{Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use common::{
    acquire_session_lock, build_multipiece_torrent_pieces, local_test_config, MiniTracker,
};
use torrentfs::{Session, TorrentInfo};

const PIECE_LEN: usize = 256 * 1024;
const NUM_PIECES: usize = 2;
const BLOCK_LEN: usize = 16 * 1024;

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
}

fn piece_key(info_hash_hex: &str, index: usize) -> String {
    format!("{info_hash_hex}:piece:{index}")
}

fn wait_for_piece_check(handle: &torrentfs::TorrentHandle) {
    use torrentfs::TorrentState;

    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let status = handle.status().expect("status");
        if !matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::CheckingResumeData
                | TorrentState::Allocating
        ) {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("piece check did not finish");
}

/// A crash after the last block of a piece landed first leaves a full-length
/// sparse staging file. With hash checks disabled the restart scan is the only
/// thing that decides what the cache contains, so it must ignore that file.
#[test]
fn staged_half_piece_is_not_declared_complete_after_restart() {
    let _guard = acquire_session_lock();
    let (torrent_bytes, content) = build_multipiece_torrent_pieces(
        "http://127.0.0.1:1/announce",
        "staged-half-piece",
        NUM_PIECES,
    );
    let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));

    let root = tempfile::tempdir().expect("temp dir");
    let pieces_root = root.path().join("pieces");
    let piece_dir = pieces_root.join(&info_hash_hex);
    std::fs::create_dir_all(&piece_dir).expect("create piece dir");

    // Piece 0: the last block arrived first, so the staging file already has
    // the full piece length with a hole in front of that block.
    let staging = piece_dir.join(format!("{}.part", piece_key(&info_hash_hex, 0)));
    {
        let mut file = std::fs::File::create(&staging).expect("create staging file");
        file.set_len(PIECE_LEN as u64).expect("size staging file");
        let last_block_start = PIECE_LEN - BLOCK_LEN;
        file.seek(SeekFrom::Start(last_block_start as u64))
            .expect("seek staging file");
        file.write_all(&content[last_block_start..PIECE_LEN])
            .expect("write last block");
        file.sync_all().expect("sync staging file");
    }

    // Piece 1 is a published piece; the restart scan must still accept it.
    std::fs::write(
        piece_dir.join(piece_key(&info_hash_hex, 1)),
        &content[PIECE_LEN..2 * PIECE_LEN],
    )
    .expect("write published piece");

    let mut config = quiet_config();
    config.misc.disable_hash_checks = Some(true);
    let mut session =
        Session::new_with_custom_storage(&config, &pieces_root).expect("create session");
    let handle = session
        .add_torrent(&info, &pieces_root)
        .expect("add torrent to custom-storage session");
    wait_for_piece_check(&handle);

    assert!(
        !handle.have_piece(0),
        "a full-length staging file was declared as a complete piece"
    );
    assert!(
        handle.have_piece(1),
        "a published piece must still be declared complete"
    );
    assert!(
        !piece_dir.join(piece_key(&info_hash_hex, 0)).exists(),
        "the half-written piece was published under its final name"
    );
    assert!(
        staging.exists(),
        "the restart scan consumed the staged file instead of ignoring it"
    );
}

/// While a piece is being written, only its staging file may exist: a final
/// piece name that is visible but shorter than the piece is exactly the
/// half-written state the restart scan used to trust.
#[test]
fn in_flight_writes_never_publish_a_partial_final_piece() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let (torrent_data, content) =
        build_multipiece_torrent_pieces(&tracker.announce_url(), "staging-live", NUM_PIECES);
    let info = TorrentInfo::from_bytes(torrent_data.clone()).expect("parse torrent");
    let raw_info_hash = info.info_hash().expect("info hash");
    let info_hash_hex = hex::encode(raw_info_hash);

    // Seeder: stock libtorrent storage serves the complete file.
    let seed_dir = tempfile::TempDir::new().expect("seed dir");
    std::fs::write(seed_dir.path().join("staging-live"), &content).expect("seed file");
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

    // Downloader: the custom piece storage. The per-hash read lock is held
    // for the whole first block write, which pins it in the staging file and
    // keeps the published name from existing until the lock is released.
    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let hash_c = CString::new(info_hash_hex.clone()).expect("hash as C string");
    unsafe { libtorrent_sys::lt_lock_piece_read(hash_c.as_ptr()) };
    let mut session = Session::new_with_custom_storage(&quiet_config(), cache_dir.path())
        .expect("downloader session");
    let handle = session
        .add_torrent(&info, cache_dir.path())
        .expect("add torrent to custom-storage session");

    let piece_dir = cache_dir.path().join(&info_hash_hex);
    let mut saw_staging = false;
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Ok(entries) = std::fs::read_dir(&piece_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.ends_with(".part") {
                    saw_staging = true;
                } else if name.starts_with(&format!("{info_hash_hex}:piece:"))
                    && !name.ends_with(".incomplete")
                {
                    let len = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    assert_eq!(
                        len, PIECE_LEN as u64,
                        "final piece {name} was visible at {len} bytes before the piece was complete"
                    );
                }
            }
        }
        if saw_staging {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "no staging file appeared while the first block was pinned"
        );
        thread::sleep(Duration::from_millis(2));
    }

    // Nothing may have been published under the final name yet.
    for index in 0..NUM_PIECES {
        assert!(
            !piece_dir.join(piece_key(&info_hash_hex, index)).exists(),
            "piece {index} was published while its first block was still staged"
        );
    }
    unsafe { libtorrent_sys::lt_unlock_piece_read(hash_c.as_ptr()) };

    let finish_deadline = Instant::now() + Duration::from_secs(60);
    while !(0..NUM_PIECES as i32).all(|piece| handle.have_piece(piece)) {
        assert!(
            Instant::now() < finish_deadline,
            "download did not finish after the write lock was released"
        );
        thread::sleep(Duration::from_millis(5));
    }
    for index in 0..NUM_PIECES {
        let final_path = piece_dir.join(piece_key(&info_hash_hex, index));
        assert_eq!(
            std::fs::metadata(&final_path).expect("final piece").len(),
            PIECE_LEN as u64,
            "final piece {index} is not the full piece"
        );
    }
    let leftovers: Vec<String> = std::fs::read_dir(&piece_dir)
        .expect("read piece dir")
        .flatten()
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".part"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "staging files survived publication: {leftovers:?}"
    );

    stop.store(true, Ordering::Relaxed);
    seeder.join().expect("seeder thread");
}

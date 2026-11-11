//! Removing a torrent with `delete_files` must reclaim its piece cache.
//!
//! The custom disk storage keeps a torrent's pieces in
//! `<pieces>/<info_hash>/<info_hash>:piece:<n>` plus `<piece>.incomplete`
//! markers. A torrent removed with `lt::session::delete_files` reaches
//! `PieceStorageDiskIO::async_delete_files`, which has to empty that directory
//! before removing it — a plain `rmdir` on a non-empty directory silently left
//! the cache behind. Production reclaims the cache through
//! `TorrentService::remove_torrent`; this test drives the FFI path that the
//! wrapper itself owns.

mod common;

use std::time::{Duration, Instant};

use common::{acquire_session_lock, create_single_piece_torrent, local_test_config};
use torrentfs::{Session, TorrentInfo};

const TIMEOUT: Duration = Duration::from_secs(10);

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
}

/// The piece directory of a torrent removed with `remove_files` is gone,
/// including the piece files and the in-progress markers it held.
#[test]
fn remove_torrent_with_files_deletes_piece_cache_directory() {
    let _guard = acquire_session_lock();
    let temp = tempfile::tempdir().expect("temp dir");
    let pieces_root = temp.path().join("pieces");
    std::fs::create_dir_all(&pieces_root).expect("create pieces root");

    let (torrent_bytes, _) =
        create_single_piece_torrent("http://127.0.0.1:1/announce", "delete-probe");
    let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));

    let mut session =
        Session::new_with_custom_storage(&quiet_config(), &pieces_root).expect("create session");
    let handle = session
        .add_torrent(&info, &pieces_root)
        .expect("add torrent to custom-storage session");

    // The storage claims `<pieces>/<info_hash>` when the torrent is added and
    // names every piece file after the info hash.
    let piece_dir = pieces_root.join(&info_hash_hex);
    assert!(
        wait_until(TIMEOUT, || piece_dir.is_dir()),
        "storage did not create the piece directory"
    );
    std::fs::write(
        piece_dir.join(format!("{info_hash_hex}:piece:0")),
        b"cached piece",
    )
    .expect("write piece file");
    std::fs::write(
        piece_dir.join(format!("{info_hash_hex}:piece:0.incomplete")),
        b"",
    )
    .expect("write in-progress marker");

    let detached = session.remove_torrent(handle, true);

    // libtorrent performs the delete on its disk thread and only then drops the
    // storage, so the directory disappears asynchronously.
    assert!(
        wait_until(TIMEOUT, || !piece_dir.exists()),
        "piece cache directory survived remove_torrent(delete_files)"
    );
    drop(detached);
}

fn wait_until(timeout: Duration, condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

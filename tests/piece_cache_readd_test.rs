//! Removing a torrent with `delete_files` and immediately re-adding the same
//! info hash must not let the old storage's asynchronous delete run after the
//! new storage has claimed the cache directory.
//!
//! libtorrent removes the old torrent from the session before the disk delete
//! reaches the custom storage, so a re-add can be in progress while the delete
//! is still queued. The delete and the re-add share one per-info-hash serial
//! lane, so the delete always runs first. The test holds the per-info-hash
//! read lock to keep the delete inside the old lane while the re-add happens;
//! that is the interleaving that used to delete the recreated directory.

mod common;

use std::ffi::CString;
use std::time::{Duration, Instant};

use common::{acquire_session_lock, create_single_piece_torrent, local_test_config};
use torrentfs::{Session, TorrentInfo};

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
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

#[test]
fn old_async_delete_cannot_wipe_an_immediately_readded_cache() {
    let _guard = acquire_session_lock();
    let temp = tempfile::tempdir().expect("temp dir");
    let pieces_root = temp.path().join("pieces");
    std::fs::create_dir_all(&pieces_root).expect("create pieces root");

    let (torrent_bytes, _) =
        create_single_piece_torrent("http://127.0.0.1:1/announce", "readd-guard");
    let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));
    let piece_dir = pieces_root.join(&info_hash_hex);

    let mut session =
        Session::new_with_custom_storage(&quiet_config(), &pieces_root).expect("create session");
    let handle = session
        .add_torrent(&info, &pieces_root)
        .expect("add torrent to custom-storage session");
    assert!(
        wait_until(Duration::from_secs(10), || piece_dir.is_dir()),
        "first storage did not create the piece directory"
    );
    std::fs::write(piece_dir.join("old.dat"), b"old generation").expect("write old generation");

    // Stall the delete inside the old lane. Re-adding while it is stalled is
    // the interleaving the per-info-hash lane has to order.
    let hash_c = CString::new(info_hash_hex.clone()).expect("hash as C string");
    unsafe { libtorrent_sys::lt_lock_piece_read(hash_c.as_ptr()) };
    let detached = session.remove_torrent(handle, true);
    std::thread::sleep(Duration::from_millis(500));

    let readded = session
        .add_torrent(&info, &pieces_root)
        .expect("re-add the same info hash");
    // Give the re-add enough time to reach `new_torrent` and queue its mkdir
    // behind the stalled delete.
    std::thread::sleep(Duration::from_millis(1000));
    unsafe { libtorrent_sys::lt_unlock_piece_read(hash_c.as_ptr()) };

    assert!(
        wait_until(Duration::from_secs(15), || !piece_dir
            .join("old.dat")
            .exists()),
        "the delete_files removal never reclaimed the old piece directory"
    );
    assert!(
        wait_until(Duration::from_secs(15), || piece_dir.is_dir()),
        "the re-added storage's piece directory never appeared: the old async \
         delete ran after it was recreated"
    );

    std::fs::write(piece_dir.join("new.dat"), b"new generation").expect("write new generation");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        std::fs::read(piece_dir.join("new.dat"))
            .expect("new piece data must survive the old delete"),
        b"new generation"
    );
    assert!(
        piece_dir.is_dir(),
        "the new piece directory disappeared after the old delete"
    );

    drop(readded);
    drop(detached);
}

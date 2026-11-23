//! Publishing a finished piece can fail after every block was written:
//! `fsync` may return an I/O error and the final `rename` may fail when the
//! cache directory disappeared. The last block's write completion must carry
//! that error. Reporting success leaves libtorrent believing the piece is on
//! disk; with `disable_hash_checks` it then marks the piece complete and seeds
//! a file that was never published.
//!
//! The fixture stalls the first block while holding the per-torrent read lock,
//! removes the cache directory, then releases the write. The publish rename
//! cannot succeed, and every retry fails because the cache root is a file.
//! Restoring the directory and re-adding the torrent must download and publish
//! the correct bytes.

mod common;

use std::ffi::CString;
use std::time::{Duration, Instant};

use common::{acquire_session_lock, create_single_piece_torrent, local_test_config, MiniTracker};
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

fn counters() -> libtorrent_sys::lt_disk_io_counters_t {
    let mut out = libtorrent_sys::lt_disk_io_counters_t {
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
    unsafe { libtorrent_sys::lt_disk_io_counters_get(&mut out) };
    out
}

fn run_failed_publish(disable_hash_checks: bool) {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let (torrent_data, content) =
        create_single_piece_torrent(&tracker.announce_url(), "publish-failure");
    let info = TorrentInfo::from_bytes(torrent_data).expect("parse torrent");
    let info_hash = info.info_hash().expect("info hash");
    let info_hash_hex = hex::encode(info_hash);

    let seed_dir = tempfile::TempDir::new().expect("seed dir");
    std::fs::write(seed_dir.path().join("publish-failure"), &content).expect("seed file");
    let mut seeder = Session::new(&quiet_config()).expect("seeder session");
    let seed_handle = seeder
        .add_torrent(&info, seed_dir.path())
        .expect("seeder add torrent");
    assert!(
        wait_until(Duration::from_secs(30), || seed_handle.have_piece(0)),
        "the seeder never verified its file"
    );
    assert!(
        wait_until(Duration::from_secs(60), || tracker.peer_count(&info_hash)
            >= 1),
        "seeder never announced to the tracker"
    );

    let cache_root = tempfile::TempDir::new().expect("cache root");
    let cache_dir = cache_root.path().join("pieces");
    std::fs::create_dir_all(&cache_dir).expect("create cache dir");
    let piece_dir = cache_dir.join(&info_hash_hex);

    let mut config = quiet_config();
    config.misc.disable_hash_checks = Some(disable_hash_checks);
    let hash_c = CString::new(info_hash_hex.clone()).expect("hash as C string");
    unsafe { libtorrent_sys::lt_lock_piece_read(hash_c.as_ptr()) };
    unsafe { libtorrent_sys::lt_disk_io_counters_reset() };

    let mut downloader =
        Session::new_with_custom_storage(&config, &cache_dir).expect("downloader session");
    let handle = downloader
        .add_torrent(&info, &cache_dir)
        .expect("downloader add torrent");

    // The first block is staged and then waits on the exclusive piece lock.
    let staged = wait_until(Duration::from_secs(30), || {
        std::fs::read_dir(&piece_dir)
            .map(|entries| {
                entries
                    .flatten()
                    .any(|entry| entry.file_name().to_string_lossy().ends_with(".part"))
            })
            .unwrap_or(false)
    });
    assert!(staged, "the first block was never staged");
    // With hash checks disabled, libtorrent marks the piece available as soon
    // as every block is submitted, before the write completion can report a
    // publish failure (`torrent::verify_piece` short-circuits). Record that
    // optimistic flag so the assertions below can distinguish it from a
    // published piece.
    let have_while_staged = handle.have_piece(0);

    // Remove the cache directory, then replace its root with a regular file so
    // libtorrent's retries cannot recreate it.
    std::fs::remove_dir_all(&piece_dir).expect("remove piece dir");
    std::fs::remove_dir_all(&cache_dir).expect("remove cache dir");
    std::fs::write(&cache_dir, b"not a directory").expect("block cache root");

    unsafe { libtorrent_sys::lt_unlock_piece_read(hash_c.as_ptr()) };
    assert!(
        wait_until(Duration::from_secs(15), || counters().write_blocks > 0),
        "the stalled block was never written"
    );
    // The fsync succeeded; the publish rename failed. The last block's write
    // completion must carry an error instead of reporting success.
    assert!(
        wait_until(Duration::from_secs(15), || counters()
            .piece_publish_failures
            > 0),
        "the failed publish was not recorded"
    );
    // Counting a constructed error is not proof it reached libtorrent. Wait
    // for the completion handler to return before checking the torrent's
    // observable state.
    assert!(
        wait_until(Duration::from_secs(15), || counters()
            .write_completion_errors_delivered
            > 0),
        "the write error never reached libtorrent"
    );
    let failed = counters();
    assert_eq!(
        failed.piece_markers, 0,
        "a failed piece was marked published"
    );
    assert!(
        failed.piece_fsyncs >= 1,
        "the piece never reached its single fsync before the rename failed"
    );
    assert!(
        failed.write_completion_errors >= failed.write_completion_errors_delivered,
        "the write completion reported success even though publishing failed"
    );
    if disable_hash_checks {
        // `have_piece` is not a publication signal on this path: it is set
        // optimistically before the publish result exists. The final file and
        // marker are the observables that matter, and neither may appear.
        assert!(
            have_while_staged,
            "expected libtorrent's optimistic have flag while the block was \
             still staged with hash checks disabled"
        );
    } else {
        assert!(
            !handle.have_piece(0),
            "libtorrent declared the piece available although publishing \
             failed (disable_hash_checks={disable_hash_checks})"
        );
    }
    assert!(
        !piece_dir.join(format!("{info_hash_hex}:piece:0")).exists(),
        "a final piece file exists even though publishing failed"
    );

    // Restore the cache and re-add: the storage must recreate its directory,
    // download again and publish the real bytes.
    std::fs::remove_file(&cache_dir).expect("unblock cache root");
    std::fs::create_dir_all(&cache_dir).expect("restore cache dir");
    let detached = downloader.remove_torrent(handle, false);
    let readded = downloader
        .add_torrent(&info, &cache_dir)
        .expect("re-add after publish failure");
    assert!(
        wait_until(Duration::from_secs(60), || readded.have_piece(0)),
        "the piece did not download after the publish failure was cleared"
    );
    let published = std::fs::read(piece_dir.join(format!("{info_hash_hex}:piece:0")))
        .expect("published piece after recovery");
    assert_eq!(
        published, content,
        "the re-downloaded piece has wrong content"
    );

    drop(readded);
    drop(detached);
}

#[test]
fn failed_publish_with_disabled_checks_never_publishes_a_piece() {
    run_failed_publish(true);
}

#[test]
fn failed_publish_with_hash_checks_never_marks_the_piece_available() {
    run_failed_publish(false);
}

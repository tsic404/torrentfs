//! A successful `async_read` completion owns its malloc'd buffer until the
//! completion hands it to libtorrent or is destroyed. Session shutdown closes
//! the completion mailbox, which destroys completions that never drained; each
//! one must release its buffer exactly once.
//!
//! The discard path is normally raced against a live teardown and is hard to
//! hit. The mailbox test hooks park completions deliberately, then close the
//! mailbox while the disk object and its allocator are still alive, so this
//! test observes the real discard branch: the mailbox reports a non-zero queue
//! depth before it is closed, close destroys exactly those queued completions,
//! the handler never runs, and every allocated buffer is released by completion
//! destruction with the live count back at zero. A second case proves normal
//! delivery releases exactly once too.

mod common;

use std::ffi::CString;
use std::time::{Duration, Instant};

use common::{
    acquire_session_lock, build_multipiece_torrent_pieces, local_test_config, MiniTracker,
};
use torrentfs::{Session, TorrentInfo};

const PIECE_LEN: usize = 256 * 1024;
const NUM_PIECES: usize = 8;

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

fn mailbox_stats(cache_dir: &CString) -> (u64, u64, u64) {
    let (mut pending, mut discarded, mut delivered) = (0u64, 0u64, 0u64);
    let found = unsafe {
        libtorrent_sys::lt_disk_io_test_completion_mailbox_stats(
            cache_dir.as_ptr(),
            &mut pending,
            &mut discarded,
            &mut delivered,
        )
    };
    assert_eq!(found, 1, "the custom disk object was not found");
    (pending, discarded, delivered)
}

fn wait_until_have_all(handle: &torrentfs::TorrentHandle) {
    assert!(
        wait_until(Duration::from_secs(30), || {
            (0..NUM_PIECES as i32).all(|piece| handle.have_piece(piece))
        }),
        "seeder never verified its cached pieces"
    );
}

/// A seeder on a fully populated custom-storage cache plus a stock downloader
/// that keeps requesting blocks. The seeder's read path is the wrapper's
/// `async_read`, so every served block goes through the buffer path.
struct Swarm {
    seeder: Session,
    seed_handle: torrentfs::TorrentHandle,
    downloader: Session,
    download_handle: torrentfs::TorrentHandle,
    cache_dir: CString,
    _pieces_root: tempfile::TempDir,
    _download_root: tempfile::TempDir,
}

fn start_swarm(tracker: &MiniTracker, pause_mailbox: bool) -> Swarm {
    let (torrent_data, content) = build_multipiece_torrent_pieces(
        &tracker.announce_url(),
        "read-buffer-teardown",
        NUM_PIECES,
    );
    let info = TorrentInfo::from_bytes(torrent_data).expect("parse torrent");
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));

    let pieces_root = tempfile::TempDir::new().expect("pieces root");
    let piece_dir = pieces_root.path().join(&info_hash_hex);
    std::fs::create_dir_all(&piece_dir).expect("create piece dir");
    for (index, piece) in content.chunks(PIECE_LEN).enumerate() {
        std::fs::write(
            piece_dir.join(format!("{info_hash_hex}:piece:{index}")),
            piece,
        )
        .expect("write cached piece");
    }

    let mut seeder = Session::new_with_custom_storage(&quiet_config(), pieces_root.path())
        .expect("seeder session");
    let seed_handle = seeder
        .add_torrent(&info, pieces_root.path())
        .expect("seeder add torrent");
    wait_until_have_all(&seed_handle);
    assert!(
        wait_until(Duration::from_secs(60), || tracker
            .peer_count(&info.info_hash().expect("info hash"))
            >= 1),
        "seeder never announced to the tracker"
    );

    let cache_dir = CString::new(pieces_root.path().to_string_lossy().into_owned())
        .expect("cache dir as C string");
    // Park completions before the downloader asks for a single block, so the
    // discard case cannot be contaminated by an already delivered read.
    if pause_mailbox {
        assert_eq!(
            unsafe {
                libtorrent_sys::lt_disk_io_test_pause_completion_mailbox(cache_dir.as_ptr(), 1)
            },
            1,
            "the custom disk object was not found by its cache directory"
        );
    }

    let download_root = tempfile::TempDir::new().expect("download root");
    let mut downloader = Session::new(&quiet_config()).expect("downloader session");
    let download_handle = downloader
        .add_torrent(&info, download_root.path())
        .expect("downloader add torrent");

    Swarm {
        seeder,
        seed_handle,
        downloader,
        download_handle,
        cache_dir,
        _pieces_root: pieces_root,
        _download_root: download_root,
    }
}

#[test]
fn closing_the_mailbox_discards_pending_read_completions_and_releases_once() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    unsafe { libtorrent_sys::lt_disk_io_counters_reset() };
    let swarm = start_swarm(&tracker, true);

    // A paused mailbox keeps real successful read completions queued. Wait
    // until one is observably inside the mailbox, not merely allocated in a
    // worker: this is the state close() has to destroy.
    assert!(
        wait_until(Duration::from_secs(60), || mailbox_stats(&swarm.cache_dir)
            .0
            > 0),
        "no successful read completion ever reached the mailbox"
    );
    let (pending_before, discarded_before, delivered_before) = mailbox_stats(&swarm.cache_dir);
    assert!(pending_before > 0);
    assert_eq!(discarded_before, 0, "a completion was discarded too early");
    assert!(counters().read_buffers_allocated > 0);
    assert_eq!(
        counters().read_buffers_dropped,
        0,
        "a buffer was dropped before the mailbox was closed"
    );

    // Close the mailbox with real successful read completions still queued.
    // The owner and its allocator are alive; the destructor safety net has not
    // run yet, so any release must come from destroying the completions.
    assert_eq!(
        unsafe {
            libtorrent_sys::lt_disk_io_test_close_completion_mailbox(swarm.cache_dir.as_ptr())
        },
        1,
        "the custom disk object was not found by its cache directory"
    );
    assert!(
        wait_until(Duration::from_secs(10), || {
            let current = counters();
            current.read_buffers_live == 0
                && current.read_buffers_released == current.read_buffers_allocated
                && current.read_buffers_dropped > 0
        }),
        "closing the mailbox did not release the queued read buffers"
    );

    let (pending_after, discarded_after, delivered_after) = mailbox_stats(&swarm.cache_dir);
    assert_eq!(pending_after, 0, "close left completions in the mailbox");
    assert!(
        discarded_after >= pending_before && discarded_after > 0,
        "close did not destroy the queued completions (queued={pending_before}, \
         discarded={discarded_after})"
    );
    assert_eq!(
        delivered_after, delivered_before,
        "the mailbox drained a completion after it was paused"
    );

    let after = counters();
    assert_eq!(after.read_buffers_live, 0, "a read buffer leaked");
    assert_eq!(
        after.read_buffers_allocated, after.read_buffers_released,
        "a read buffer was released more or less than once"
    );
    assert_eq!(
        after.read_buffers_dropped, after.read_buffers_allocated,
        "a buffer was released outside completion destruction (for example by \
         the destructor safety net): allocated={} dropped={}",
        after.read_buffers_allocated, after.read_buffers_dropped
    );

    let mut swarm = swarm;
    swarm.download_handle = swarm.downloader.remove_torrent(swarm.download_handle, true);
    drop(swarm);
}

#[test]
fn delivered_read_completions_release_exactly_once() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    unsafe { libtorrent_sys::lt_disk_io_counters_reset() };
    let swarm = start_swarm(&tracker, false);

    assert!(
        wait_until(Duration::from_secs(60), || counters().read_buffers_released
            > 0),
        "no read completion was delivered"
    );
    let (_, discarded, delivered) = mailbox_stats(&swarm.cache_dir);
    assert!(delivered > 0, "the mailbox never drained a completion");
    assert_eq!(discarded, 0, "a delivered completion was discarded");

    let mut swarm = swarm;
    swarm.download_handle = swarm.downloader.remove_torrent(swarm.download_handle, true);
    drop(swarm);

    let after = counters();
    assert!(after.read_buffers_allocated > 0);
    assert_eq!(
        after.read_buffers_dropped, 0,
        "a delivered read completion was reported as discarded"
    );
    assert_eq!(
        after.read_buffers_allocated, after.read_buffers_released,
        "a read buffer was released more or less than once"
    );
    assert_eq!(after.read_buffers_live, 0, "a read buffer leaked");
}

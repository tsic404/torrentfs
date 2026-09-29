//! End-to-end test: the per-torrent `.stats` wait-period fields.
//!
//! All three fields come from one engine source — the parked-read aggregation
//! (`waiting_reads`) published in the download snapshot — so this test covers
//! the whole path: torrent bytes → handle → parked read → snapshot → `.stats`
//! text.  The piece grid's retained gradient is asserted alongside as the
//! contrast signal: it outlives the read that produced it, the wait fields do
//! not.

mod common;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

/// `Waited:` seconds from a rendered `.stats` body, or `None` when no read is
/// parked and the line is absent.
fn waited_secs(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix("  Waited: "))
        .and_then(|value| value.strip_suffix('s'))
        .and_then(|secs| secs.parse().ok())
}

/// Renders the leaf `.stats` from the shared service state.
fn render_stats(
    torrent_id: i64,
    info_hash: &str,
    db: &Option<Arc<Mutex<Database>>>,
    service: &Arc<DownloadService>,
) -> String {
    String::from_utf8_lossy(&generate_torrent_stats(
        torrent_id,
        info_hash,
        db,
        &Some(Arc::clone(service)),
        || service.get_cache_manager(),
    ))
    .into_owned()
}

/// A read parked on an empty swarm must be visible in `.stats`: `Waiting: yes`
/// with the read's own `Waited` age (growing while it stays parked), the live
/// reader count, and the "waiting for peers" alert — which must fire
/// immediately, not only after the sustained-empty-swarm grace window.  Once
/// the read gives up, the wait fields must not linger and the reader count must
/// drop even though its retained prefetch gradient stays in the piece grid.
#[test]
fn test_torrent_stats_reports_a_waiting_read() {
    let _lock = common::acquire_session_lock();
    let temp_dir = tempfile::TempDir::new().unwrap();

    // Unique torrent name: libtorrent LSD pairs sessions on one host by info
    // hash, so a name no other test uses keeps this session's swarm to itself.
    // The announce URL has no listener, so the swarm stays empty for the whole
    // peer-discovery window.
    let (torrent_bytes, _content) = common::create_single_piece_torrent(
        "http://127.0.0.1:6979/announce",
        "stats_waiting_verify.txt",
    );
    let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes).unwrap());
    let info_hash = hex::encode(info.info_hash().unwrap());

    let mut db = Database::open_in_memory().unwrap();
    let InsertTorrentResult::Inserted(torrent_id) = db
        .insert_torrent(
            "data",
            "stats-waiting",
            "stats-waiting.torrent",
            16384,
            &info_hash,
            1,
        )
        .unwrap()
    else {
        panic!("expected a fresh insert");
    };
    let db = Some(Arc::new(Mutex::new(db)));

    let cache_dir = temp_dir.path().join("cache");
    let service = Arc::new(DownloadService::new(&cache_dir, &common::local_test_config()).unwrap());
    service.ensure_handle_lightweight(info.clone()).unwrap();

    let reader_service = Arc::clone(&service);
    let reader_info = Arc::clone(&info);
    let reader =
        std::thread::spawn(move || reader_service.read_file_range(reader_info, 0, 0, 4096));

    // The engine republishes its snapshot about once per second, so the parked
    // read shows up well inside the peer-discovery window.  Wait for the
    // snapshot that carries the parked read's wait entry.
    let deadline = Instant::now() + Duration::from_secs(8);
    let mut waiting_text = None;
    while Instant::now() < deadline {
        let text = render_stats(torrent_id, &info_hash, &db, &service);
        if text.contains("  Waiting: yes\n") && text.contains("  Waited: ") {
            waiting_text = Some(text);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let waiting_text = waiting_text.expect("a parked read must surface `Waiting: yes` in `.stats`");

    assert!(
        waiting_text.contains("  Active readers: 1\n"),
        "the parked read must be counted as a live reader, got:\n{waiting_text}"
    );
    assert!(
        waiting_text.contains("⚠ Waiting for peers ("),
        "an empty swarm with a waiting reader must alert immediately, got:\n{waiting_text}"
    );
    let first_waited =
        waited_secs(&waiting_text).expect("a parked read must report `Waited:` seconds");

    // The read stays parked until the peer-discovery window elapses, so the
    // reported wait must keep growing rather than restarting.
    let grow_deadline = Instant::now() + Duration::from_secs(4);
    let mut grew = false;
    while Instant::now() < grow_deadline {
        match waited_secs(&render_stats(torrent_id, &info_hash, &db, &service)) {
            Some(secs) if secs > first_waited => {
                grew = true;
                break;
            }
            _ => std::thread::sleep(Duration::from_millis(200)),
        }
    }
    assert!(
        grew,
        "`Waited:` must grow while the read stays parked (started at {first_waited}s)"
    );

    assert!(
        reader.join().expect("reader thread").is_err(),
        "a read with no seeder must fail, not return data"
    );

    // With the read gone the wait fields drop rather than reporting a stale
    // wait: nobody is waiting is not the same as waiting zero seconds.  The
    // reader count must drop too, and it must do so *while* the piece grid
    // still shows the reader's retained prefetch gradient — that gradient is a
    // window, not a parked read.
    let settle_deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let text = render_stats(torrent_id, &info_hash, &db, &service);
        if !text.contains("  Waited: ") && text.contains("  Waiting: no\n") {
            assert!(
                text.contains("  Active readers: 0\n"),
                "a released reader must not be counted, got:\n{text}"
            );
            assert!(
                text.contains("  Pieces: [7]"),
                "the released reader's prefetch gradient must still be in the \
                 piece grid, got:\n{text}"
            );
            assert!(
                !text.contains("⚠"),
                "an idle torrent must carry no alert, got:\n{text}"
            );
            break;
        }
        if Instant::now() >= settle_deadline {
            panic!(".stats kept a stale wait after the read ended:\n{text}");
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    service.shutdown();
}

//! Design acceptance case TC-seeding-02: a delivered torrent is discoverable
//! before any file is read, and the first read turns that discovery into a
//! cached download (`docs/03-acceptance.md` §F AC-51, contracts C-7.5).
//!
//! The torrent goes through the shipped ingest path
//! (`TorrentService::add_torrent`), which persists the row and registers the
//! lightweight upload-mode handle. DHT and LSD are off, so the mini tracker is
//! the only discovery channel: a non-zero peer/seed count proves the loopback
//! announce was not skipped. Before that read no piece may be cached; the read
//! itself must fetch the piece inside the engine's read budget and leave it in
//! the cached marker class.

use crate::common::{base, local_test_config, wave_slot};
use crate::seed_peer::SeedPeer;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::services::torrent::TorrentService;
use torrentfs::TorrentInfo;

pub const TC_ID: &str = "TC-seeding-02";

const TORRENT_NAME: &str = "tc_seeding_02_announce.txt";
/// Shortened wait windows: with a seeder already connected the read only uses
/// the piece wait, so a failure surfaces inside this test rather than after the
/// shipped 175 s worst-case budget.
const READ_TIMEOUT_SECS: i64 = 15;
const PEER_DISCOVERY_WAIT_SECS: i64 = 10;
const NO_SEEDER_WAIT_SECS: i64 = 5;

/// Render the leaf `.stats` through the same entry point the FUSE read uses.
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

/// `Peers`/`Seeds` counts from the rendered `.stats` peers section.
fn peers_and_seeds(text: &str) -> Option<(i32, i32)> {
    let line = text.lines().find(|line| line.starts_with("  Peers: "))?;
    let rest = line.strip_prefix("  Peers: ")?;
    let (peers, seeds) = rest.split_once("  Seeds: ")?;
    Some((peers.parse().ok()?, seeds.parse().ok()?))
}

/// The `.stats` piece marker string, e.g. `[]`, `[x]` or `[X 2]`.
fn piece_markers(text: &str) -> Option<&str> {
    text.lines()
        .find_map(|line| line.strip_prefix("  Pieces: "))
        .map(str::trim_end)
}

/// Cached marker class (`[x]` or `[X n]`) for a one-piece torrent.
fn is_cached_marker(markers: &str) -> bool {
    markers == "[x]" || (markers.starts_with("[X ") && markers.ends_with(']'))
}

#[test]
fn tc_seeding_02_announce_makes_peers_visible_before_first_read() {
    let _slot = wave_slot();
    let tracker = base::MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) = base::create_single_piece_torrent(&announce_url, TORRENT_NAME);
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data.clone()).expect("torrent must parse"));
    let info_hash_bytes = info.info_hash().expect("info hash");
    let info_hash_hex = hex::encode(info_hash_bytes);
    let piece_len = info.piece_length() as usize;
    let full_len = content.len() as u32;
    assert_eq!(
        full_len as usize, piece_len,
        "fixture must stay a single-piece torrent"
    );

    // A peer that declares the whole piece and announces to the same tracker:
    // the swarm has a usable seeder before the client's handle exists, so the
    // only open question is whether the client announces and connects.
    let seeder = SeedPeer::start(
        info_hash_bytes,
        content.clone(),
        piece_len,
        &announce_url,
    );

    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    // Tracker-only discovery: LSD could pair the two same-host sessions without
    // any announce, which would hide a skipped loopback announce.
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.timeouts.read_timeout_secs = Some(READ_TIMEOUT_SECS);
    config.timeouts.peer_discovery_wait_secs = Some(PEER_DISCOVERY_WAIT_SECS);
    config.timeouts.no_seeder_read_timeout_secs = Some(NO_SEEDER_WAIT_SECS);

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let service = Arc::new(DownloadService::new(cache_dir.path(), &config).expect("service"));
    let db = Arc::new(Mutex::new(
        Database::open_in_memory().expect("in-memory db"),
    ));
    let torrent_service = TorrentService::new(Arc::clone(&db), Some(Arc::clone(&service)));
    torrent_service
        .add_torrent(&torrent_data, "data", &format!("{TORRENT_NAME}.torrent"))
        .expect("ingest must persist the row and register the handle");
    let torrent_id = db
        .lock()
        .expect("db lock")
        .get_torrent_by_info_hash(&info_hash_hex)
        .expect("query by info hash")
        .expect("the ingested torrent must have a row")
        .id;
    let stats_db = Some(Arc::clone(&db));

    // ── Before any read: the announce must make the seeder visible ────────
    let deadline = Instant::now() + Duration::from_secs(30);
    let before_read = loop {
        let text = render_stats(torrent_id, &info_hash_hex, &stats_db, &service);
        if let Some((peers, seeds)) = peers_and_seeds(&text) {
            if peers > 0 || seeds > 0 {
                break text;
            }
        }
        assert!(
            Instant::now() < deadline,
            "no peer or seed became visible before the first read:\n{text}"
        );
        thread::sleep(Duration::from_millis(200));
    };
    let (peers, seeds) = peers_and_seeds(&before_read).expect("peers line");
    println!("before first read: Peers: {peers}  Seeds: {seeds}");

    // The seeder announced once; a second entry can only be the client's own
    // loopback announce, which the tracker would not have answered otherwise.
    assert!(
        tracker.announce_count() >= 2,
        "the client's announce never reached the tracker (count {})",
        tracker.announce_count()
    );
    assert!(
        tracker.peer_count(&info_hash_bytes) >= 2,
        "the tracker holds {} peer entries; expected the seeder and the client",
        tracker.peer_count(&info_hash_bytes)
    );
    // Discovery alone must not fetch data: the lightweight handle stays in
    // upload mode with every piece priority at zero.
    assert_eq!(
        piece_markers(&before_read),
        Some("[]"),
        "no piece may be cached before the first read:\n{before_read}"
    );
    assert!(
        before_read.contains("Progress: 0.0%"),
        "progress must stay at zero before the first read:\n{before_read}"
    );

    // ── The first read downloads the piece and converges ─────────────────
    let budget_secs = service.read_wait_budget_secs();
    let (tx, rx) = mpsc::channel();
    let reader_service = Arc::clone(&service);
    let reader_info = Arc::clone(&info);
    let reader = thread::spawn(move || {
        let started = Instant::now();
        let result = reader_service.read_file_range(reader_info, 0, 0, full_len);
        let _ = tx.send((started.elapsed(), result));
    });
    let (elapsed, result) = rx
        .recv_timeout(Duration::from_secs(budget_secs))
        .expect("the first read must settle inside the engine's read budget");
    reader.join().expect("reader thread");
    let data = result.expect("the first read must download the piece");
    assert_eq!(
        data, content,
        "the first read must return the seeded bytes"
    );
    assert!(
        elapsed <= Duration::from_secs(budget_secs),
        "the read took {elapsed:?}, beyond the {budget_secs}s engine budget"
    );
    assert!(
        seeder.pieces_served() > 0,
        "the seeder served nothing; the read was not what fetched the piece"
    );

    // The read must leave the piece in the cached marker class and report the
    // whole torrent as downloaded.
    let deadline = Instant::now() + Duration::from_secs(15);
    let after_read = loop {
        let text = render_stats(torrent_id, &info_hash_hex, &stats_db, &service);
        if piece_markers(&text).is_some_and(is_cached_marker) && text.contains("Progress: 100.0%") {
            break text;
        }
        assert!(
            Instant::now() < deadline,
            "the read piece never reached the cached marker class:\n{text}"
        );
        thread::sleep(Duration::from_millis(200));
    };
    println!(
        "after first read: marker {:?}, {}",
        piece_markers(&after_read),
        after_read
            .lines()
            .find(|line| line.contains("Progress:"))
            .unwrap_or("")
    );

    service.shutdown();
}

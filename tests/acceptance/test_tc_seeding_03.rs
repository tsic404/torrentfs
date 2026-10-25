//! Design acceptance case TC-seeding-03: a genuinely empty swarm raises the
//! health alert only after the engine's effective grace, and `.stats` stays
//! readable throughout (`docs/03-acceptance.md` §F AC-52, contracts C-6.4.4,
//! C-6.4.5, C-6.4.6, C-6.5.5).
//!
//! The tracker is reachable and records the daemon's own announce; no peer ever
//! joins, so the swarm has no usable seeder rather than an unreachable tracker.
//! The expected grace is read back from the running service, not assumed — the
//! shortened configured window is only the input. Every `.stats` read is timed,
//! and the body must stay complete while the alert is present.

use crate::common::{base, local_test_config, wave_slot};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-seeding-03";

const TORRENT_NAME: &str = "tc_seeding_03_empty_swarm.txt";
/// Shortened input window: the engine's resolved value is what the test
/// expects, so a value the engine rewrites cannot make the assertion pass.
const CONFIGURED_NO_SEEDER_WAIT_SECS: i64 = 5;
/// Snapshot tick + polling interval + startup slack on top of the grace.
const ALERT_LATENCY_ALLOWANCE: Duration = Duration::from_secs(8);
/// `.stats` renders from non-blocking snapshots; seconds means it blocked.
const STATS_READ_LIMIT: Duration = Duration::from_secs(2);
const HEALTH_ALERT: &str = "⚠ Health: 0 peers / 0 seeds";

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

#[test]
fn tc_seeding_03_empty_swarm_alerts_after_grace_and_stats_stay_readable() {
    let _slot = wave_slot();
    let _session_lock = base::acquire_session_lock();

    let tracker = base::MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_bytes, _content) =
        base::create_single_piece_torrent(&announce_url, TORRENT_NAME);
    let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes).expect("torrent must parse"));
    let info_hash = hex::encode(info.info_hash().expect("info hash"));

    let mut db = Database::open_in_memory().expect("in-memory db");
    let InsertTorrentResult::Inserted(torrent_id) = db
        .insert_torrent(
            "data",
            TORRENT_NAME,
            &format!("{TORRENT_NAME}.torrent"),
            16384,
            &info_hash,
            1,
        )
        .expect("insert torrent row")
    else {
        panic!("expected a fresh insert");
    };
    let db = Some(Arc::new(Mutex::new(db)));

    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    // No discovery channel may produce a peer: the tracker is the only one, and
    // nothing announces to it besides the daemon itself.
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.timeouts.no_seeder_read_timeout_secs = Some(CONFIGURED_NO_SEEDER_WAIT_SECS);
    // Keep the read timeout above the no-seeder window: the alert grace must be
    // the no-seeder value itself, never narrowed by a shorter read timeout.
    config.timeouts.read_timeout_secs = Some(60);

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let service = Arc::new(DownloadService::new(cache_dir.path(), &config).expect("service"));
    let resolved_grace_secs = service.no_seeder_read_timeout_secs();
    assert_eq!(
        resolved_grace_secs, CONFIGURED_NO_SEEDER_WAIT_SECS as u64,
        "the engine must expose the configured no-seeder window"
    );
    let grace = Duration::from_secs(resolved_grace_secs);
    assert!(grace > Duration::ZERO, "the alert grace must not be zero");

    let started = Instant::now();
    service
        .ensure_handle_lightweight(info)
        .expect("the lightweight handle must be queued");

    let deadline = started + grace + ALERT_LATENCY_ALLOWANCE;
    let (alert_after, stats_while_alerted) = loop {
        let read_started = Instant::now();
        let text = render_stats(torrent_id, &info_hash, &db, &service);
        let read_elapsed = read_started.elapsed();
        assert!(
            read_elapsed < STATS_READ_LIMIT,
            "`.stats` blocked for {read_elapsed:?} (limit {STATS_READ_LIMIT:?}):\n{text}"
        );
        let elapsed = started.elapsed();
        if text.contains(HEALTH_ALERT) {
            assert!(
                elapsed >= grace,
                "the empty-swarm alert fired after {elapsed:?}, before the {grace:?} grace"
            );
            break (elapsed, text);
        }
        assert!(
            Instant::now() < deadline,
            "no empty-swarm alert within {:?} of the engine's {grace:?} grace:\n{text}",
            elapsed
        );
        std::thread::sleep(Duration::from_millis(200));
    };
    assert!(
        alert_after <= grace + ALERT_LATENCY_ALLOWANCE,
        "the alert fired after {alert_after:?}, more than {ALERT_LATENCY_ALLOWANCE:?} past the {grace:?} grace"
    );
    println!("empty-swarm alert after {alert_after:?} (grace {grace:?})");

    // The alert describes live counts, and C-6.4.6 forbids claiming the tracker
    // is unreachable.
    let alert_line = stats_while_alerted
        .lines()
        .find(|line| line.contains(HEALTH_ALERT))
        .expect("the alert line must be present");
    assert!(
        alert_line.contains("0 peers / 0 seeds"),
        "the alert must state the zero counts, got {alert_line:?}"
    );
    assert!(
        !alert_line.contains("unreachable"),
        "the alert must not claim the tracker is unreachable, got {alert_line:?}"
    );

    // `.stats` remains readable while the swarm stays empty: repeated reads
    // return the complete body without blocking, and the alert persists.
    for _ in 0..2 {
        let read_started = Instant::now();
        let again = render_stats(torrent_id, &info_hash, &db, &service);
        let read_elapsed = read_started.elapsed();
        assert!(
            read_elapsed < STATS_READ_LIMIT,
            "`.stats` blocked for {read_elapsed:?} while alerting:\n{again}"
        );
        assert!(
            again.contains(HEALTH_ALERT),
            "a still-empty swarm must keep the alert:\n{again}"
        );
        assert!(
            again.starts_with(&format!("===== torrent: {TORRENT_NAME} =====")),
            "the `.stats` body must stay complete:\n{again}"
        );
        assert!(
            again.contains("-- Status --") && again.contains("-- Peers --"),
            "the status and peers sections must stay present:\n{again}"
        );
        assert!(
            again.contains("  Peers: 0  Seeds: 0\n"),
            "the rendered counts must still be zero:\n{again}"
        );
        assert!(
            again.contains("-- Pieces ("),
            "the piece grid must stay present:\n{again}"
        );
        assert!(
            again.contains("-- Trackers --") && again.contains("info_hash: "),
            "the trailing sections must stay present:\n{again}"
        );
        std::thread::sleep(Duration::from_secs(1));
    }

    // The tracker was reachable and answered the daemon's announce, so the
    // alert reflects an empty swarm rather than a discovery failure.
    assert!(
        tracker.announce_count() > 0,
        "the daemon never announced to the reachable tracker"
    );

    service.shutdown();
}

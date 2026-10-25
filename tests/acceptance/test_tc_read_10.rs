//! Design acceptance case TC-read-10: a sourceless verdict speaks only for a
//! swarm that is still empty — inside its lifetime a read fails at once with
//! `ENODATA`, yet a seeder that comes online after the probe is still found and
//! the same read then returns the complete file, digest-equal, without a remount
//! (`docs/03-acceptance.md` §B AC-19; `docs/02-contracts.md` §C-4.3.6).
//!
//! One engine serves both halves, so "without a remount" holds by construction.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
};
use crate::seed_peer::SeedPeer;
use torrentfs::download::DownloadEngine;
use torrentfs::{TorrentError, TorrentInfo};

pub const TC_ID: &str = "TC-read-10";

/// The verdict's shelf life is one discovery window, so this is also how long
/// the "fail fast" half holds.
const DISCOVERY_SECS: u64 = 4;
const READ_TIMEOUT_SECS: i64 = 20;
/// A read inside a live verdict returns at once; a read that re-opened the
/// window would take a whole `w`.
const IMMEDIATE_BOUND: Duration = Duration::from_secs(DISCOVERY_SECS / 2);
/// How long the late seeder may take to be found and deliver the file.
///
/// libtorrent defers a forced announce to the tracker's `min interval` — 30 s
/// when the tracker response omits it, which the local tracker does — so the
/// verdict's half-window cadence cannot surface a new peer sooner than that.
/// The AC bounds correctness (found, complete, digest-equal), not that latency;
/// the budget covers one deferred announce plus the download.
const JOIN_BUDGET: Duration = Duration::from_secs(60);

#[test]
fn tc_read_10_a_seeder_joining_after_the_verdict_is_still_found() {
    let _slot = wave_slot();
    let _session_guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        build_multipiece_torrent_named(&announce_url, "tc-read-10-late-seeder.bin");
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("torrent must parse"));
    let info_hash = info.info_hash().expect("info hash");
    let piece_len = info.piece_length() as usize;
    let file_len = content.len() as u32;

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Tracker-only discovery: the swarm must be genuinely empty for the probe,
    // and the peer introduced below is what refills it.
    config.local_discovery.lsd_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.timeouts.read_timeout_secs = Some(READ_TIMEOUT_SECS);
    config.timeouts.peer_discovery_wait_secs = Some(DISCOVERY_SECS as i64);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);

    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("engine must start");
    engine.ensure_handle(info.clone()).expect("ensure_handle");
    let window = Duration::from_secs(DISCOVERY_SECS);

    // ── The probe: an empty swarm is declared sourceless ───────────────
    let probe_start = Instant::now();
    let probe = engine.read_file_range(info.clone(), 0, 0, file_len);
    let probe_elapsed = probe_start.elapsed();
    match probe {
        Err(TorrentError::NoPeers(_)) => {}
        Ok(data) => panic!(
            "an empty swarm's whole-file read returned {} bytes; it must fail with ENODATA",
            data.len()
        ),
        Err(err) => panic!("an empty swarm's read failed with {err:?}, not the ENODATA error"),
    }
    assert!(
        probe_elapsed >= window - Duration::from_secs(2),
        "the probe returned after only {}s: it must spend the {DISCOVERY_SECS}s discovery \
         window before the verdict exists",
        probe_elapsed.as_secs_f64()
    );

    // ── Inside the verdict's lifetime a read fails immediately ─────────
    let inherited_start = Instant::now();
    let inherited = engine.read_file_range(info.clone(), 0, 0, file_len);
    let inherited_elapsed = inherited_start.elapsed();
    match inherited {
        Err(TorrentError::NoPeers(_)) => {}
        Ok(data) => panic!(
            "the read inside the verdict returned {} bytes; the swarm is still empty, so it \
             must fail with ENODATA",
            data.len()
        ),
        Err(err) => panic!(
            "the read inside the verdict failed with {err:?}, not the ENODATA no-seeder error"
        ),
    }
    assert!(
        inherited_elapsed < IMMEDIATE_BOUND,
        "the read inside the verdict waited {}s: the verdict must fail it at once instead of \
         re-opening the discovery window",
        inherited_elapsed.as_secs_f64()
    );

    // ── The seeder comes online after the probe ────────────────────────
    // It announces itself only now, so reaching it requires the probe's
    // announce cadence to survive the verdict.
    let seeder = SeedPeer::start(info_hash, content.clone(), piece_len, &announce_url);

    let deadline = Instant::now() + JOIN_BUDGET;
    let mut attempts = 0u32;
    let mut served: Option<Vec<u8>> = None;
    while Instant::now() < deadline {
        attempts += 1;
        match engine.read_file_range(info.clone(), 0, 0, file_len) {
            Ok(data) => {
                served = Some(data);
                break;
            }
            Err(TorrentError::NoPeers(_)) => thread::sleep(Duration::from_millis(200)),
            Err(err) => {
                panic!("an unexpected error while retrying against the joining seeder: {err:?}")
            }
        }
    }
    let served = served.unwrap_or_else(|| {
        panic!(
            "the seeder that joined after the probe was never found in {attempts} reads over \
             {JOIN_BUDGET:?}: the sourceless verdict wedged peer discovery"
        )
    });

    // ── The same read now returns the complete file ────────────────────
    assert_eq!(
        served.len(),
        content.len(),
        "the read served after the seeder joined returned {} of {} bytes",
        served.len(),
        content.len()
    );
    assert_eq!(
        sha1(&served),
        sha1(&content),
        "the complete file served after the seeder joined does not match the seeded digest"
    );
    assert_eq!(
        served, content,
        "the complete file served after the seeder joined does not match the seeded bytes"
    );
    assert!(
        seeder.pieces_served() > 0,
        "the late seeder served no piece; the read was not driven by the peer that joined"
    );

    engine.shutdown();
}

/// SHA-1 of a byte string, the digest the AC's "摘要全等" is stated in.
fn sha1(bytes: &[u8]) -> [u8; 20] {
    sha1_smol::Sha1::from(bytes).digest().bytes()
}

//! Design acceptance case TC-read-09: on an empty swarm a whole-file read spends
//! one peer-discovery window — not one per 128 KiB chunk — and a read arriving
//! after that verdict's shelf life opens a fresh window instead of inheriting a
//! permanent one (`docs/03-acceptance.md` §B AC-18; `docs/02-contracts.md`
//! §C-4.2.4, §C-4.3.1–§C-4.3.2).
//!
//! `w = min(read_timeout_secs, peer_discovery_wait_secs)` is configurable, so the
//! case judges wall-clock time against the AC's `[w-2, 2w)` bound.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
};
use torrentfs::download::DownloadEngine;
use torrentfs::{TorrentError, TorrentInfo};

pub const TC_ID: &str = "TC-read-09";

/// `peer_discovery_wait_secs` is the binding term of the `min`, so it is the
/// measured window.  The 60 s read timeout leaves it binding.
const DISCOVERY_SECS: u64 = 4;
const READ_TIMEOUT_SECS: i64 = 60;
/// Slack the AC allows on the low side of the window: `[w-2, 2w)`.
const EARLY_SLACK: Duration = Duration::from_secs(2);
/// One read chunk, matching what a whole-file `cat` costs at the FUSE layer.
const CHUNK: u32 = 128 * 1024;

#[test]
fn tc_read_09_a_whole_file_read_spends_one_discovery_window() {
    let _slot = wave_slot();
    let _session_guard = acquire_session_lock();

    // A live tracker with no peers: the swarm is genuinely empty, so the
    // discovery window is what ends every read here.
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        build_multipiece_torrent_named(&announce_url, "tc-read-09-one-window.bin");
    let file_len = content.len() as u32;
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("torrent must parse"));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Tracker-only discovery: LSD would find a session on this host and make
    // the swarm non-empty.
    config.local_discovery.lsd_enabled = Some(false);
    config.timeouts.read_timeout_secs = Some(READ_TIMEOUT_SECS);
    config.timeouts.peer_discovery_wait_secs = Some(DISCOVERY_SECS as i64);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);

    let engine = DownloadEngine::new(cache_dir.path(), &config).expect("engine must start");
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    let window = Duration::from_secs(DISCOVERY_SECS);

    // ── Whole-file read, one chunk at a time ───────────────────────────
    let whole_start = Instant::now();
    let mut failures: Vec<String> = Vec::new();
    let mut chunk_elapsed: Vec<(u64, Duration)> = Vec::new();
    let chunks = file_len.div_ceil(CHUNK);
    for index in 0..chunks {
        let offset = index as u64 * CHUNK as u64;
        let size = CHUNK.min(file_len - offset as u32);
        let chunk_start = Instant::now();
        match engine.read_file_range(info.clone(), 0, offset, size) {
            Err(TorrentError::NoPeers(_)) => {}
            Ok(data) => failures.push(format!(
                "the chunk at offset {offset} returned {} bytes; a sourceless read must \
                 fail with the ENODATA no-seeder error",
                data.len()
            )),
            Err(err) => failures.push(format!(
                "the chunk at offset {offset} failed with {err:?} instead of the ENODATA \
                 no-seeder error"
            )),
        }
        chunk_elapsed.push((offset, chunk_start.elapsed()));
    }
    let whole = whole_start.elapsed();
    assert!(failures.is_empty(), "{}", failures.join("\n"));

    // The first chunk opens — and spends — the one discovery window.
    let (first_offset, first) = chunk_elapsed[0];
    assert!(
        first >= window - EARLY_SLACK,
        "the whole-file read's first chunk (offset {first_offset}) gave up after only {}s: \
         it must spend the {DISCOVERY_SECS}s discovery window before calling the swarm \
         sourceless",
        first.as_secs_f64()
    );

    // Every later chunk inherits that verdict: a chunk paying its own window
    // is exactly the N-windows-per-cat regression this AC forbids.
    for &(offset, elapsed) in &chunk_elapsed[1..] {
        assert!(
            elapsed < window / 2,
            "the chunk at offset {offset} waited {}s after the probe: it re-spent the \
             discovery window instead of inheriting the probe's verdict",
            elapsed.as_secs_f64()
        );
    }

    // The whole-file read as a client sees it: one window, within `[w-2, 2w)`.
    assert!(
        whole >= window - EARLY_SLACK,
        "the whole-file read returned after only {}s: it did not spend its discovery window",
        whole.as_secs_f64()
    );
    assert!(
        whole < 2 * window,
        "the whole 1 MiB read ({} chunks of 128 KiB) took {}s: more than the {DISCOVERY_SECS}s \
         window by the AC's own `2w` bound, so the chunks did not share one probe",
        chunks,
        whole.as_secs_f64()
    );

    // ── After the verdict's shelf life the next read probes again ──────
    // The verdict lapses one window after the probe; a read arriving later
    // must re-open the window — no permanent short-circuit — and must still
    // fail with ENODATA rather than hang.
    thread::sleep(window + Duration::from_secs(1));
    let later_start = Instant::now();
    let later = engine.read_file_range(info.clone(), 0, 0, CHUNK);
    let later_elapsed = later_start.elapsed();
    match later {
        Err(TorrentError::NoPeers(_)) => {}
        Ok(data) => panic!(
            "a read after the verdict lapsed returned {} bytes; the swarm is still empty, \
             so it must fail with ENODATA",
            data.len()
        ),
        Err(err) => panic!(
            "a read after the verdict lapsed failed with {err:?} instead of the ENODATA \
             no-seeder error"
        ),
    }
    assert!(
        later_elapsed >= window - EARLY_SLACK,
        "the read after the verdict lapsed gave up after only {}s: it must probe again \
         instead of inheriting an aged-out verdict",
        later_elapsed.as_secs_f64()
    );
    assert!(
        later_elapsed < 2 * window,
        "the read after the verdict lapsed took {}s: more than the AC's `2w` bound",
        later_elapsed.as_secs_f64()
    );

    engine.shutdown();
}

//! Shared helpers for the acceptance suite.
//!
//! Re-exports the repo's integration-test infrastructure (`tests/common`,
//! the MiniTracker + TestHarness + torrent fixtures), adds the wave-slot
//! concurrency cap used by the engine-level read tests, and provides the
//! service-level rig (`rig`/`ingest`) that drives `FsService` directly so
//! namespace carriers assert the errno the FUSE adapter maps.

#[path = "../common/mod.rs"]
pub mod base;

pub use base::{
    acquire_session_lock, build_multipiece_torrent_named, build_multipiece_torrent_pieces,
    local_test_config, MiniTracker, TestHarness,
};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::fs_types::{FileKind, StatsKind};
use torrentfs::fuse::inodes::METADATA_INO;
use torrentfs::{Database, FsError, FsService, TorrentInfo};

/// How many tests in this binary may run at once.
///
/// Each acceptance test drives one to three libtorrent sessions.  Letting
/// them all run at once makes libtorrent 2.1 segfault inside `add_torrent`
/// and starves the engine threads enough to stretch a read's wait windows;
/// capping the wave bounds how many sessions are live while the module stays
/// far below libtest's 60s slow-test warning.  Four is what a 4-core CI
/// runner already gets from libtest's own default thread count.
const MAX_CONCURRENT_TESTS: usize = 4;

/// Tests currently holding a wave slot.
static ACTIVE_TESTS: AtomicUsize = AtomicUsize::new(0);

/// Claim a wave slot, blocking until one frees.
///
/// Hold the returned guard for the whole test — it is the test's *sessions*,
/// not merely their creation, that must stay bounded.
pub fn wave_slot() -> WaveSlot {
    loop {
        let active = ACTIVE_TESTS.load(Ordering::SeqCst);
        if active < MAX_CONCURRENT_TESTS
            && ACTIVE_TESTS
                .compare_exchange(active, active + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return WaveSlot;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Releases its wave slot on drop.
pub struct WaveSlot;

impl Drop for WaveSlot {
    fn drop(&mut self) {
        ACTIVE_TESTS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The single-piece fixture torrent, under a `name` unique to one test.
///
/// Every test in this binary runs in parallel and `local_test_config()`
/// leaves LSD enabled: libtorrent pairs two same-host sessions that serve
/// the same info hash, so a shared fixture name would let one test's
/// downloader read from another test's seeder — and lose that connection
/// mid-read when that test tears its session down.  A per-test `name` gives
/// each test its own swarm identity.
pub fn unique_torrent(name: &'static str) -> impl FnOnce(&str) -> (Vec<u8>, Vec<u8>) {
    move |announce_url| base::create_single_piece_torrent(announce_url, name)
}

// ── FUSE-service rig (service-level acceptance carriers) ─────────────────────

/// Announce URL for fixtures that never need a swarm: projection and parsing
/// read the bencode only, so no tracker has to answer.
pub const UNUSED_ANNOUNCE_URL: &str = "http://127.0.0.1:6973/announce";

/// Bound on the detached `release` persist landing its DB row.
const INGEST_TIMEOUT: Duration = Duration::from_secs(10);

/// A `FsService` wired to an in-memory DB, plus the cache directory it uses.
pub struct Rig {
    pub svc: FsService,
    /// Keeps the cache path alive for as long as the service uses it.
    _cache: tempfile::TempDir,
}

/// Build a service with an in-memory DB and a real `data/` projection source.
///
/// The tests that use this rig drive `FsService`'s public methods directly:
/// the FUSE adapter forwards to them unchanged and maps their `FsError`
/// through `src/fuse/errno.rs`, so the errno assertions match the kernel.
pub fn rig() -> Rig {
    let cache = tempfile::TempDir::new().expect("create cache dir");
    let mut config = local_test_config();
    // Ephemeral port: acceptance tests run in parallel and a fixed one collides.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let svc = FsService::new_with_db_and_cache(
        Database::open_in_memory().expect("create in-memory db"),
        cache.path().to_path_buf(),
        &config,
    );
    Rig {
        svc,
        _cache: cache,
    }
}

/// Ingest `torrent_bytes` as `filename` under `metadata/` through the shipped
/// create → write → flush → release path, then wait for the DB row to land so
/// `data/` resolves the projection.
pub fn ingest(svc: &mut FsService, filename: &str, torrent_bytes: &[u8]) -> TorrentInfo {
    let info = TorrentInfo::from_bytes(torrent_bytes.to_vec()).expect("parse fixture torrent");
    let created = svc
        .create(METADATA_INO, filename)
        .expect("create metadata entry");
    svc.write(created.attr.ino, 0, torrent_bytes)
        .expect("write torrent bytes");
    svc.flush(created.attr.ino).expect("flush valid torrent");
    svc.release(created.fh).expect("release valid torrent");

    let deadline = Instant::now() + INGEST_TIMEOUT;
    loop {
        let has_row = svc
            .db
            .as_ref()
            .expect("service must own the database")
            .lock()
            .expect("lock db")
            .get_torrent_by_filename_and_source_path(filename, "")
            .expect("query torrent row")
            .is_some();
        let is_idle = svc
            .processing_torrents
            .lock()
            .expect("lock pending map")
            .is_empty();
        if has_row && is_idle {
            return info;
        }
        assert!(
            Instant::now() < deadline,
            "{filename}: the DB row must land within {INGEST_TIMEOUT:?}"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// The errno the FUSE adapter replies with — the single mapping exit point
/// `impl From<FsError> for libc::c_int` (`src/fuse/errno.rs`, C-2.3.2).
pub fn errno(err: FsError) -> libc::c_int {
    err.into()
}

/// One directory's complete entry set, order-independent.
pub fn listing(svc: &mut FsService, ino: u64) -> Vec<(String, FileKind)> {
    let mut entries: Vec<(String, FileKind)> = svc
        .readdir(ino, -1)
        .unwrap_or_else(|e| panic!("readdir({ino}) failed: {e:?}"))
        .into_iter()
        .map(|entry| (entry.name, entry.kind))
        .collect();
    // Names are unique per directory, so sorting by name is order-independent.
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    entries
}

/// Root `.stats` with the wall-clock `Uptime:` value masked.
///
/// Elapsed time is the one rendered line a rejected mutation cannot have
/// changed, and it can tick between two renders; every other byte must match.
pub fn root_stats_without_uptime(svc: &FsService) -> Vec<u8> {
    let content = svc.read_stats(StatsKind::Global);
    let mut masked = Vec::with_capacity(content.len());
    for line in content.split(|byte| *byte == b'\n') {
        if line.starts_with(b"  Uptime:") {
            masked.extend_from_slice(b"  Uptime: <elapsed>");
        } else {
            masked.extend_from_slice(line);
        }
        masked.push(b'\n');
    }
    masked
}

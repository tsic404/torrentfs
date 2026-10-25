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
    ingest_into(svc, METADATA_INO, "", filename, torrent_bytes)
}

/// `ingest`, into the metadata directory at `parent` — which projects as
/// `source_path` under `data/`.
pub fn ingest_into(
    svc: &mut FsService,
    parent: u64,
    source_path: &str,
    filename: &str,
    torrent_bytes: &[u8],
) -> TorrentInfo {
    let info = TorrentInfo::from_bytes(torrent_bytes.to_vec()).expect("parse fixture torrent");
    let created = svc.create(parent, filename).expect("create metadata entry");
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
            .get_torrent_by_filename_and_source_path(filename, source_path)
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
            "{source_path}/{filename}: the DB row must land within {INGEST_TIMEOUT:?}"
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

/// A config that touches no network discovery: DHT, LSD, UPnP and NAT-PMP are
/// off and the engine listens on an ephemeral port.  State-service tests only
/// need the engine present (or absent); a quiet engine keeps parallel modules
/// from pairing through a same-host swarm and from colliding on a fixed port.
pub fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config
}

/// An `FsService` backed by an in-memory database and a fresh cache dir.
///
/// The service owns its state; keep the returned `TempDir` alive for the
/// service's lifetime (it backs the piece cache).  Callers hold a
/// [`wave_slot`]: constructing the service starts a libtorrent session.
pub fn service_with_state() -> (torrentfs::fuse::FsService, tempfile::TempDir) {
    service_with_config(&quiet_config())
}

/// [`service_with_state`] with an explicit engine config.
pub fn service_with_config(
    config: &torrentfs::TorrentfsConfig,
) -> (torrentfs::fuse::FsService, tempfile::TempDir) {
    let cache = tempfile::TempDir::new().expect("Failed to create cache dir");
    let db = torrentfs::infrastructure::db::Database::open_in_memory()
        .expect("Failed to open in-memory database");
    let service = torrentfs::fuse::FsService::new_with_db_and_cache(
        db,
        cache.path().to_path_buf(),
        config,
    );
    (service, cache)
}

/// Ingest `torrent_bytes` through the service's real `metadata/` write path
/// (`create` → `write` → `flush` → `release`) and wait until the detached
/// persistence settles, so `data/` carries the projection.
pub fn ingest_torrent(
    service: &mut torrentfs::fuse::FsService,
    parent: u64,
    filename: &str,
    torrent_bytes: &[u8],
) {
    let created = service
        .create(parent, filename)
        .unwrap_or_else(|e| panic!("create {filename}: {e:?}"));
    let written = service
        .write(created.attr.ino, 0, torrent_bytes)
        .unwrap_or_else(|e| panic!("write {filename}: {e:?}"));
    assert_eq!(
        written as usize,
        torrent_bytes.len(),
        "{filename}: the write must accept the whole torrent"
    );
    service
        .flush(created.attr.ino)
        .unwrap_or_else(|e| panic!("flush {filename}: {e:?}"));
    service
        .release(created.fh)
        .unwrap_or_else(|e| panic!("release {filename}: {e:?}"));

    // `release` persists and registers the handle on a detached thread; the
    // dedup key is dropped only once both have settled.
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while std::time::Instant::now() < deadline {
        if service
            .processing_torrents
            .lock()
            .map(|pending| pending.is_empty())
            .unwrap_or(false)
        {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("{filename}: persistence did not settle within 20s");
}

/// Build a multi-file `.torrent` from `(relative/path, size)` entries.
///
/// The fixture never seeds, so the piece hashes are placeholder zeroes of the
/// right width; only the file tree and sizes matter to the projection and
/// `.stats` surfaces these tests exercise.
pub fn build_multi_file_torrent(
    announce_url: &str,
    name: &str,
    files: &[(&str, usize)],
) -> Vec<u8> {
    const PIECE_LEN: usize = 16 * 1024;

    fn bencode_str(out: &mut Vec<u8>, value: &str) {
        out.extend_from_slice(value.as_bytes().len().to_string().as_bytes());
        out.push(b':');
        out.extend_from_slice(value.as_bytes());
    }

    let total: usize = files.iter().map(|(_, size)| *size).sum();
    let piece_count = total.div_ceil(PIECE_LEN).max(1);

    let mut torrent = Vec::new();
    torrent.push(b'd');
    bencode_str(&mut torrent, "announce");
    bencode_str(&mut torrent, announce_url);
    bencode_str(&mut torrent, "info");
    torrent.push(b'd');
    bencode_str(&mut torrent, "files");
    torrent.push(b'l');
    for (path, size) in files {
        torrent.push(b'd');
        torrent.extend_from_slice(b"6:lengthi");
        torrent.extend_from_slice(size.to_string().as_bytes());
        torrent.push(b'e');
        bencode_str(&mut torrent, "path");
        torrent.push(b'l');
        for component in path.split('/') {
            bencode_str(&mut torrent, component);
        }
        torrent.push(b'e');
        torrent.push(b'e');
    }
    torrent.push(b'e');
    bencode_str(&mut torrent, "name");
    bencode_str(&mut torrent, name);
    torrent.extend_from_slice(b"12:piece lengthi");
    torrent.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    torrent.push(b'e');
    bencode_str(&mut torrent, "pieces");
    torrent.extend_from_slice((piece_count * 20).to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(&vec![0u8; piece_count * 20]);
    torrent.push(b'e');
    torrent.push(b'e');
    torrent
}

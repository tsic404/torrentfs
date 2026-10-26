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
    create_single_piece_torrent, local_test_config, MiniTracker, TestHarness,
};

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::fs_types::{DirEntry, FileKind, ReadOutcome, StatsKind};
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::{Database, FsError, FsService, Torrent, TorrentInfo};

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
    Rig::with_database(Database::open_in_memory().expect("create in-memory db"))
}

impl Rig {
    /// Wire `db` to a fresh cache directory and the test config — the one
    /// construction path behind [`rig`] and the remount-capable harness.
    fn with_database(db: Database) -> Self {
        let cache = tempfile::TempDir::new().expect("create cache dir");
        let mut config = local_test_config();
        // Ephemeral port: acceptance tests run in parallel and a fixed one collides.
        config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
        Rig {
            svc: FsService::new_with_db_and_cache(db, cache.path().to_path_buf(), &config),
            _cache: cache,
        }
    }

    /// Rebuild the service from `db_path`, keeping the cache directory alive.
    fn reopen(self, db_path: &std::path::Path) -> Self {
        let Rig { svc, _cache } = self;
        drop(svc);
        let mut config = local_test_config();
        config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
        Rig {
            svc: FsService::new_with_db_and_cache(
                Database::open(db_path).expect("open database"),
                _cache.path().to_path_buf(),
                &config,
            ),
            _cache,
        }
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

/// Every entry a directory has after `offset`, paging the way the kernel does.
///
/// One `readdir` returns at most one page; the next starts after the offset of
/// the last entry its predecessor accepted.  A caller that wants the whole
/// remaining listing — the assertion helpers below do — has to page, or it
/// silently compares the first page only.
pub fn readdir_from(svc: &mut FsService, ino: u64, offset: i64) -> Vec<DirEntry> {
    let mut entries = Vec::new();
    let mut cursor = offset;
    loop {
        let page = svc
            .readdir(ino, cursor)
            .unwrap_or_else(|e| panic!("readdir({ino}, {cursor}) failed: {e:?}"));
        let Some(last) = page.last() else { break };
        assert!(
            last.offset > cursor,
            "readdir({ino}, {cursor}) must continue past the requested offset, got {page:?}"
        );
        cursor = last.offset;
        entries.extend(page);
    }
    entries
}

/// One directory's complete entry set, order-independent.
pub fn listing(svc: &mut FsService, ino: u64) -> Vec<(String, FileKind)> {
    let mut entries: Vec<(String, FileKind)> = readdir_from(svc, ino, -1)
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

// ── Ingest harness (metadata/ → data/ projection) ───────────────────────────

/// One entry the `data/` projection exposes, relative to the mount root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectedEntry {
    pub path: String,
    pub kind: FileKind,
    pub perm: u16,
    pub size: u64,
}

/// A FUSE-facing service driving the ingest path without a kernel mount.
///
/// Every ingest effect crosses `FsService` through the same calls the adapter
/// makes: `create` / `write` / `flush` / `release` for a copy, `lookup` /
/// `readdir` / `getattr` for observation, `rename` / `unlink` / `rmdir` for
/// mutation.  Only the kernel dentry cache sits above this boundary; its
/// invalidation is adapter-side, so the mount-form scripts
/// (`tc_ingest_03.sh`, `tc_ingest_06.sh`) cover it instead.
pub struct IngestHarness {
    rig: Rig,
    db_dir: Option<tempfile::TempDir>,
}

impl IngestHarness {
    /// The [`rig`] wiring over an in-memory database (cases without a remount).
    pub fn new() -> Self {
        Self {
            rig: rig(),
            db_dir: None,
        }
    }

    /// The [`rig`] wiring over a file-backed database, so [`Self::remount`]
    /// can reopen it and prove the projection survived the restart.
    pub fn with_file_db() -> Self {
        let db_dir = tempfile::TempDir::new().expect("db dir");
        let rig = Rig::with_database(
            Database::open(&db_dir.path().join("ingest.db")).expect("open database"),
        );
        Self {
            rig,
            db_dir: Some(db_dir),
        }
    }

    /// One unmount/mount cycle: drop the service, then rebuild [`Rig`] from
    /// the same database file and cache directory.
    pub fn remount(self) -> Self {
        let Self { rig, db_dir } = self;
        let db_dir = db_dir.expect("remount requires a file-backed database");
        let rig = rig.reopen(&db_dir.path().join("ingest.db"));
        Self {
            rig,
            db_dir: Some(db_dir),
        }
    }

    pub fn service(&mut self) -> &mut FsService {
        &mut self.rig.svc
    }

    /// Create `metadata/<parts…>` top-down like `mkdir -p`, returning the
    /// deepest inode.  Existing levels are reused, not re-created.
    pub fn mkdir(&mut self, parts: &[&str]) -> u64 {
        let mut parent = METADATA_INO;
        for part in parts {
            parent = match self.rig.svc.lookup(parent, part) {
                Ok(Some(entry)) => entry.ino,
                Ok(None) => {
                    self.rig.svc
                        .mkdir(parent, part)
                        .unwrap_or_else(|e| {
                            panic!("mkdir metadata/{} failed: {:?}", parts.join("/"), e)
                        })
                        .ino
                }
                Err(e) => panic!("lookup metadata/{} failed: {:?}", parts.join("/"), e),
            };
        }
        parent
    }

    /// Copy `.torrent` bytes into `metadata/` exactly as `cp` does, then wait
    /// for the detached persistence to settle.
    pub fn copy_torrent(&mut self, parent: u64, filename: &str, bytes: &[u8]) {
        if let Err(e) = self.try_copy_torrent(parent, filename, bytes) {
            let rendered = format!("{:?}", e);
            let errno = libc::c_int::from(e);
            panic!(
                "copy of {} must be accepted, got {} (errno {})",
                filename, rendered, errno
            );
        }
        self.wait_persisted();
    }

    /// The `cp` sequence, returning the close-time verdict instead of
    /// unwrapping it.  A `create` failure is returned too: a stale inode left
    /// behind by an earlier rejected write would surface as `EEXIST` here.
    pub fn try_copy_torrent(
        &mut self,
        parent: u64,
        filename: &str,
        bytes: &[u8],
    ) -> Result<(), FsError> {
        let created = self.rig.svc.create(parent, filename)?;
        let close = match self.rig.svc.write(created.attr.ino, 0, bytes) {
            Ok(_) => self.rig.svc.flush(created.attr.ino),
            Err(e) => Err(e),
        };
        let released = self.rig.svc.release(created.fh);
        match close {
            Err(e) => Err(e),
            Ok(()) => released,
        }
    }

    /// Block until every background persist spawned by `release` settled and
    /// assert none failed.  `release` spawns the database write detached, so
    /// a projection assertion made too early would race it.
    pub fn wait_persisted(&mut self) {
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            let pending = self
                .rig
                .svc
                .processing_torrents
                .lock()
                .map(|g| g.len())
                .unwrap_or(0);
            if pending == 0 {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "background persist did not settle within 20s ({} pending)",
                pending
            );
            thread::sleep(Duration::from_millis(10));
        }
        if let Ok(errors) = self.rig.svc.persist_errors.lock() {
            assert!(
                errors.is_empty(),
                "persist failures recorded: {:?}",
                *errors
            );
        }
    }

    /// Directory entries the service returns, without `.` / `..`.
    pub fn children(&mut self, ino: u64) -> Vec<DirEntry> {
        self.rig.svc
            .readdir(ino, 0)
            .unwrap_or_else(|e| panic!("readdir(ino={}) failed: {:?}", ino, e))
            .into_iter()
            .filter(|entry| entry.name != "." && entry.name != "..")
            .collect()
    }

    /// Every directory the projection exposes, relative to `data/`, sorted.
    /// `.stats` is the design's virtual observability file, not a projection
    /// of a metadata entry, so it is excluded.
    pub fn data_dirs(&mut self) -> Vec<String> {
        let mut dirs = Vec::new();
        let mut pending = vec![(DATA_INO, String::new())];
        while let Some((ino, prefix)) = pending.pop() {
            let mut children = self.children(ino);
            children.sort_by(|a, b| a.name.cmp(&b.name));
            for child in children {
                if child.name == ".stats" || child.kind != FileKind::Directory {
                    continue;
                }
                let path = join_path(&prefix, &child.name);
                pending.push((child.ino, path.clone()));
                dirs.push(path);
            }
        }
        dirs.sort();
        dirs
    }

    /// Every entry under `metadata/`: directories and `.torrent` files,
    /// relative to the mount root.  The `data/` projection mirrors each one as
    /// a directory of the same relative path.
    pub fn metadata_projection_paths(&mut self) -> Vec<String> {
        let mut paths = Vec::new();
        let mut pending = vec![(METADATA_INO, String::new())];
        while let Some((ino, prefix)) = pending.pop() {
            let mut children = self.children(ino);
            children.sort_by(|a, b| a.name.cmp(&b.name));
            for child in children {
                let path = join_path(&prefix, &child.name);
                if child.kind == FileKind::Directory {
                    pending.push((child.ino, path.clone()));
                }
                paths.push(path);
            }
        }
        paths.sort();
        paths
    }

    /// Full `data/` projection snapshot — path, kind, permission bits, size —
    /// excluding the virtual `.stats` entries and sorted by path.
    pub fn data_snapshot(&mut self) -> Vec<ProjectedEntry> {
        let mut out = Vec::new();
        let mut pending = vec![(DATA_INO, String::new())];
        while let Some((ino, prefix)) = pending.pop() {
            let mut children = self.children(ino);
            children.sort_by(|a, b| a.name.cmp(&b.name));
            for child in children {
                if child.name == ".stats" {
                    continue;
                }
                let path = join_path(&prefix, &child.name);
                let attr = self
                    .rig
                    .svc
                    .getattr(child.ino)
                    .unwrap_or_else(|e| panic!("getattr data/{} failed: {:?}", path, e));
                out.push(ProjectedEntry {
                    path: path.clone(),
                    kind: attr.kind,
                    perm: attr.perm,
                    size: attr.size,
                });
                if child.kind == FileKind::Directory {
                    pending.push((child.ino, path));
                }
            }
        }
        out.sort_by(|a, b| a.path.cmp(&b.path));
        out
    }

    /// Resolve a path below `data/` through the service `lookup`.  A name the
    /// daemon no longer projects must not resolve.
    pub fn data_lookup(&mut self, parts: &[&str]) -> Option<u64> {
        let mut ino = DATA_INO;
        for part in parts {
            match self.rig.svc.lookup(ino, part) {
                Ok(Some(entry)) => ino = entry.ino,
                Ok(None) => return None,
                Err(e) => panic!("lookup data/{} failed: {:?}", parts.join("/"), e),
            }
        }
        Some(ino)
    }

    /// `.stats` content of a `data/` directory or torrent root.
    pub fn stats_text(&mut self, dir_ino: u64) -> String {
        let entry = self
            .rig
            .svc
            .lookup(dir_ino, ".stats")
            .unwrap_or_else(|e| panic!("lookup .stats failed: {:?}", e))
            .expect("a data/ directory must expose .stats");
        match self.rig.svc.read(entry.ino, 0, 8 * 1024 * 1024) {
            Ok(ReadOutcome::Ready(bytes)) => String::from_utf8(bytes).expect(".stats is UTF-8"),
            Ok(ReadOutcome::Pending { .. }) => panic!(".stats must be served synchronously"),
            Err(e) => panic!("read .stats failed: {:?}", e),
        }
    }

    /// Every source row in the database, each carrying its content id.
    pub fn torrents(&self) -> Vec<Torrent> {
        let db = self.rig.svc.db.as_ref().expect("harness always has a database");
        db.lock()
            .expect("database lock")
            .get_all_torrents()
            .expect("list torrents")
    }

    /// Distinct content-record ids (`torrents.id`) behind `info_hash`.
    pub fn content_ids(&self, info_hash: &str) -> BTreeSet<i64> {
        self.torrents()
            .into_iter()
            .filter(|t| t.info_hash == info_hash)
            .map(|t| t.torrent_id)
            .collect()
    }

    /// Number of content rows (`torrents`) carrying `info_hash`, read straight
    /// from the SQLite file.
    ///
    /// [`Self::torrents`] joins `torrent_sources`, so a content row whose last
    /// source was deleted is invisible there — an orphaned content row would
    /// pass any source-level assertion.  Only the content table can prove the
    /// row was deleted, hence the direct read.  Requires a file-backed
    /// database ([`Self::with_file_db`]).
    pub fn content_rows(&self, info_hash: &str) -> i64 {
        let dir = self
            .db_dir
            .as_ref()
            .expect("content_rows needs a file-backed database");
        let conn = rusqlite::Connection::open(dir.path().join("ingest.db"))
            .expect("open the ingest database");
        conn.busy_timeout(Duration::from_secs(5))
            .expect("set busy timeout");
        conn.query_row(
            "SELECT COUNT(*) FROM torrents WHERE info_hash = ?1",
            [info_hash],
            |row| row.get(0),
        )
        .expect("count content rows")
    }
}

fn join_path(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{}/{}", prefix, name)
    }
}

// ── daemon-level helpers ─────────────────────────────────────────────────────
//
// The lifecycle cases need the real binary: a signal, an exit status and the
// kernel mount table are invisible to the in-process engine harness.  One
// `Daemon` owns a private mountpoint, cache, state database and log, and
// tears all of it down on drop, so an assertion panic never leaves a mount
// behind for the next test.

/// How long one daemon-side operation may take before the case fails instead
/// of hanging.  Waiting forever would make a wedged mount look like CI
/// slowness rather than a broken contract.
pub const DAEMON_MOUNT_TIMEOUT: Duration = Duration::from_secs(60);

/// The fusermount helper on `PATH` (`fusermount3` first, then `fusermount`).
fn fusermount_binary() -> Option<&'static str> {
    ["fusermount3", "fusermount"].into_iter().find(|name| {
        std::env::var_os("PATH")
            .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(name).is_file()))
            .unwrap_or(false)
    })
}

/// Why this environment cannot mount FUSE at all, or `None` when it can.
///
/// The recovery cases are FUSE-level by design; an environment without the
/// device or the mount helper cannot execute them.  The case reports that as
/// a skip with the reason instead of a false pass.
pub fn fuse_mount_blocker() -> Option<String> {
    if !Path::new("/dev/fuse").exists() {
        return Some("/dev/fuse is absent".to_string());
    }
    if fusermount_binary().is_none() {
        return Some("neither fusermount3 nor fusermount is on PATH".to_string());
    }
    None
}

/// Escape a path the way `/proc/self/mountinfo` does.
fn mountinfo_escape(path: &str) -> String {
    path.replace('\\', "\\\\")
        .replace(' ', "\\040")
        .replace('\t', "\\011")
        .replace('\n', "\\012")
}

/// Number of FUSE mounts whose mountpoint is exactly `path`.
pub fn mountinfo_fuse_count(path: &Path) -> usize {
    let escaped = mountinfo_escape(&path.to_string_lossy());
    let Ok(mountinfo) = fs::read_to_string("/proc/self/mountinfo") else {
        return 0;
    };
    mountinfo
        .lines()
        .filter(|line| {
            let fields: Vec<&str> = line.split(' ').collect();
            if fields.len() < 5 || fields[4] != escaped {
                return false;
            }
            match fields.iter().position(|field| *field == "-") {
                Some(sep) => fields
                    .get(sep + 1)
                    .is_some_and(|kind| kind.starts_with("fuse")),
                None => false,
            }
        })
        .count()
}

/// Outcome of waiting for the daemon's mount.
pub enum MountGate {
    Mounted,
    /// This environment refused the mount; the case cannot execute here.
    Environment(String),
    /// The daemon failed for a reason that is not environmental.
    Failed(String),
}

/// A torrentfs daemon process with its own FUSE mountpoint and state.
pub struct Daemon {
    child: Option<Child>,
    work: tempfile::TempDir,
    log_path: PathBuf,
    stderr_path: PathBuf,
}

impl Daemon {
    /// Start `target/debug/torrentfs` on a private mountpoint.
    ///
    /// `config_toml` is written into the work directory and passed as
    /// `--config`; `log_level` feeds `--log-level`.  The daemon's own stderr
    /// is captured separately file so the frozen read-failure diagnostics are
    /// readable, while tracing output goes to `--log-file`.
    pub fn start(config_toml: Option<&str>, log_level: &str) -> Self {
        let base = std::env::var_os("CARGO_TARGET_TMPDIR")
            .map(PathBuf::from)
            .unwrap_or_else(std::env::temp_dir);
        let work = tempfile::TempDir::new_in(&base).expect("create daemon work dir");
        let mountpoint = work.path().join("mnt");
        fs::create_dir_all(&mountpoint).expect("create mountpoint");
        let cache = work.path().join("cache");
        fs::create_dir_all(&cache).expect("create cache dir");
        let db_dir = work.path().join("db");
        fs::create_dir_all(&db_dir).expect("create db dir");

        let log_path = work.path().join("daemon.log");
        let stderr_path = work.path().join("daemon.stderr");
        let stderr = fs::File::create(&stderr_path).expect("create daemon stderr file");

        let mut cmd = Command::new(env!("CARGO_BIN_EXE_torrentfs"));
        cmd.arg(&mountpoint)
            .arg("--cache")
            .arg(&cache)
            .arg("--db")
            .arg(db_dir.join("metadata.db"))
            .arg("--log-level")
            .arg(log_level)
            .arg("--log-file")
            .arg(&log_path);
        if let Some(toml) = config_toml {
            let config_path = work.path().join("config.toml");
            fs::write(&config_path, toml).expect("write daemon config");
            cmd.arg("--config").arg(&config_path);
        }
        cmd.stdout(Stdio::null()).stderr(Stdio::from(stderr));

        let child = cmd.spawn().expect("spawn torrentfs daemon");
        Daemon {
            child: Some(child),
            work,
            log_path,
            stderr_path,
        }
    }

    pub fn mountpoint(&self) -> PathBuf {
        self.work.path().join("mnt")
    }

    pub fn cache_dir(&self) -> PathBuf {
        self.work.path().join("cache")
    }

    pub fn log_path(&self) -> PathBuf {
        self.log_path.clone()
    }

    /// Everything the daemon wrote to its log file and its stderr, for a
    /// failure message.
    pub fn output(&self) -> String {
        let log = fs::read_to_string(&self.log_path).unwrap_or_default();
        let err = fs::read_to_string(&self.stderr_path).unwrap_or_default();
        format!("--- daemon log ---\n{log}\n--- daemon stderr ---\n{err}")
    }

    fn poll_exit(&mut self) -> Option<ExitStatus> {
        let child = self.child.as_mut()?;
        match child.try_wait().expect("poll daemon") {
            Some(status) => {
                self.child = None;
                Some(status)
            }
            None => None,
        }
    }

    /// Wait for the FUSE mount, classifying an early exit.
    pub fn wait_for_mount(&mut self, timeout: Duration) -> MountGate {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if mountinfo_fuse_count(&self.mountpoint()) > 0 {
                return MountGate::Mounted;
            }
            if let Some(status) = self.poll_exit() {
                let output = self.output();
                // A refused mount, not any startup failure.  The exit status
                // says the daemon reached the mount (2 = refused, 3 = /dev/fuse
                // vanished, 100 = the device could not be opened, C-1.2.3..5)
                // and the line must name a refusal only a mount attempt
                // writes.  The `user_allow_other` warning is printed before
                // every mount, so on its own it would turn a state-database,
                // engine-init or instance-lock failure into a skip.
                let mount_refused = [
                    "Mount failed: Operation not permitted",
                    "Mount failed and /dev/fuse cannot be opened",
                    "mountpoint is not empty",
                    "fusermount:",
                    "/dev/fuse not found",
                ]
                .iter()
                .any(|marker| output.contains(marker));
                let mount_stage_exit = matches!(status.code(), Some(2) | Some(3) | Some(100));
                return if mount_refused && mount_stage_exit {
                    MountGate::Environment(output)
                } else {
                    MountGate::Failed(format!(
                        "daemon exited with {status} before mounting\n{output}"
                    ))
                };
            }
            if std::time::Instant::now() >= deadline {
                return MountGate::Failed(format!(
                    "FUSE mount did not appear within {timeout:?}\n{}",
                    self.output()
                ));
            }
            thread::sleep(Duration::from_millis(50));
        }
    }

    /// Deliver a signal to the daemon.  Returns false when it already exited.
    pub fn signal(&self, signal: libc::c_int) -> bool {
        match self.child.as_ref() {
            Some(child) => unsafe { libc::kill(child.id() as libc::pid_t, signal) == 0 },
            None => false,
        }
    }

    /// Wait for the daemon to exit, polling until `timeout`.
    pub fn wait_for_exit(&mut self, timeout: Duration) -> Option<ExitStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if let Some(status) = self.poll_exit() {
                return Some(status);
            }
            if std::time::Instant::now() >= deadline {
                return None;
            }
            thread::sleep(Duration::from_millis(20));
        }
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        // A killed daemon leaves its mount attached; lazy-detach so the work
        // directory can be removed and no later test sees a stale mount.
        if mountinfo_fuse_count(&self.mountpoint()) > 0 {
            if let Some(bin) = fusermount_binary() {
                let _ = Command::new(bin)
                    .arg("-u")
                    .arg("-z")
                    .arg("--")
                    .arg(self.mountpoint())
                    .status();
            }
        }
    }
}

/// The config the lifecycle cases use: ephemeral listen port (several test
/// daemons run in parallel), no DHT, tracker discovery kept on, and the three
/// read windows set by the caller.
pub fn daemon_config(read_timeout_secs: u64, no_seeder_secs: u64, discovery_secs: u64) -> String {
    format!(
        r#"
[connections]
listen_interfaces = "127.0.0.1:0"
allow_multiple_connections_per_ip = true
peer_connect_timeout = 5

[dht]
enabled = false

[local_discovery]
lsd_enabled = true
upnp_enabled = false
natpmp_enabled = false

[tracker]
announce_to_all_trackers = true
announce_to_all_tiers = true
min_announce_interval = 5

[timeouts]
read_timeout_secs = {read_timeout_secs}
no_seeder_read_timeout_secs = {no_seeder_secs}
peer_discovery_wait_secs = {discovery_secs}
"#
    )
}

/// Write a `.torrent` into the mount's `metadata/` namespace.
pub fn ingest_torrent_through_mount(mountpoint: &Path, filename: &str, torrent_data: &[u8]) {
    fs::write(mountpoint.join("metadata").join(filename), torrent_data)
        .unwrap_or_else(|e| panic!("write metadata/{filename}: {e}"));
}

/// Read a file through the mount with a wall-clock ceiling, so a wedged
/// request fails the case instead of stalling the suite.
pub fn read_with_deadline(path: &Path, timeout: Duration) -> std::io::Result<Vec<u8>> {
    let (sender, receiver) = std::sync::mpsc::channel();
    let read_path = path.to_path_buf();
    thread::spawn(move || {
        let _ = sender.send(fs::read(&read_path));
    });
    receiver.recv_timeout(timeout).unwrap_or_else(|_| {
        panic!(
            "read of {} did not return within {timeout:?} (the mount is hanging)",
            path.display()
        )
    })
}

/// Read a file that was just ingested, retrying while the torrent is still in
/// the delivery window.
///
/// C-2.5.3 keeps a freshly closed `.torrent` listed as a temporary projection
/// until the background persist finishes; that projection has no download
/// handle yet, so its first reads answer `ENOENT`.  Retrying `ENOENT` for up
/// to `timeout` waits out that window, and every other error is returned at
/// once.
pub fn read_when_ready(path: &Path, timeout: Duration) -> std::io::Result<Vec<u8>> {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match read_with_deadline(path, timeout) {
            Ok(bytes) => return Ok(bytes),
            Err(error)
                if error.kind() == std::io::ErrorKind::NotFound
                    && std::time::Instant::now() < deadline =>
            {
                thread::sleep(Duration::from_millis(100));
            }
            Err(error) => return Err(error),
        }
    }
}

/// Wait for the projection of `filename` to appear under `data/`.
pub fn wait_for_projection(mountpoint: &Path, filename: &str, timeout: Duration) -> bool {
    let path = mountpoint.join("data").join(filename);
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if path.is_dir() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Wait for `path` to exist (a projected file appears after its directory).
pub fn wait_for_path(path: &Path, timeout: Duration) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    while std::time::Instant::now() < deadline {
        if path.exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

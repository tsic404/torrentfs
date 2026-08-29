//! FUSE module — the thin protocol adapter (`TorrentFs`) over `FsService`.
//!
//! `TorrentFs` is now pure glue: every `Filesystem` method converts the FUSE
//! request parameters into a domain call, delegates to `FsService`, and
//! converts the domain result back into a `fuser` reply. All domain logic,
//! inode management, data resolution and stats rendering live in `FsService`
//! (and its helpers), which is unit-testable without a FUSE session.
//!
//! The errno mapping (`impl From<FsError> for libc::c_int`) lives in `errno`
//! — the single exit point from domain errors to kernel error codes.

pub mod errno;
pub mod fs_service;
pub mod fs_types;
pub mod inodes;
pub mod lookup;
pub mod stats;
pub mod worker_pool;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::os::raw::c_int;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Instant;
use std::time::{Duration, UNIX_EPOCH};

pub use self::fs_service::FsService;
use self::fs_types::{Attr, FileKind, OpenOutcome, ReadOutcome, StatsKind};
pub use self::worker_pool::WorkerPool;
use fuser::{
    consts::FOPEN_DIRECT_IO, Filesystem, KernelConfig, ReplyAttr, ReplyCreate, ReplyData,
    ReplyDirectory, ReplyEmpty, ReplyEntry, ReplyOpen, ReplyWrite, Request,
};
use tracing::{debug, warn};

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::Database;
use crate::domain::fs_error::FsError;
use crate::infrastructure::metrics::Metrics;
use crate::services::download::DownloadService;

// ── Pending reply table ────────────────────────────────────────────────────

/// Maximum number of concurrent pending (deferred) read replies.
const MAX_PENDING: usize = 256;

/// A reply that can be resolved exactly once — either with data or an errno.
///
/// Implemented for `fuser::ReplyData` in production and for a counting mock in
/// tests, which lets the pending-table invariants (bounded capacity, deadline,
/// cancellation, exactly-once consumption) be unit-tested without a FUSE
/// session.
trait PendingReply {
    fn resolve_data(self, data: &[u8]);
    fn resolve_error(self, errno: libc::c_int);
}

impl PendingReply for ReplyData {
    fn resolve_data(self, data: &[u8]) {
        self.data(data);
    }
    fn resolve_error(self, errno: libc::c_int) {
        self.error(errno);
    }
}

/// A single entry in the pending reply table.
struct PendingEntry<R: PendingReply> {
    reply: R,
    torrent_id: i64,
    deadline: Instant,
}

/// Bounded table of FUSE read replies that are waiting for pieces to download.
///
/// * Capacity is bounded at `MAX_PENDING`.  When full, [`insert`](Self::insert)
///   blocks the caller (the FUSE dispatch thread) on a condvar until a worker
///   or the deadline checker removes an entry — the design §9 backpressure
///   model: block briefly, never error and never return truncated data.
/// * Each entry carries a deadline; a background thread expires overdue entries
///   with ENODATA ("no data available" — the piece-wait limit elapsed with no
///   seeder to serve it).
/// * `unlink` / `remove_torrent` cancels in-flight reads for the removed
///   torrent and resolves their tickets with EIO.
/// * Every reply is consumed exactly once (ok or error), zero leak.
struct PendingTable<R: PendingReply = ReplyData> {
    inner: Mutex<Inner<R>>,
    /// Signalled whenever an entry is removed, so a blocked `insert` can retry
    /// once a slot frees up.
    slot_freed: Condvar,
}

struct Inner<R: PendingReply> {
    entries: HashMap<u64, PendingEntry<R>>,
    next_id: u64,
}

impl<R: PendingReply> PendingTable<R> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                entries: HashMap::with_capacity(MAX_PENDING),
                next_id: 0,
            }),
            slot_freed: Condvar::new(),
        }
    }

    /// Insert a pending reply, blocking while the table is full (backpressure).
    /// Returns the ticket id.
    fn insert(&self, reply: R, torrent_id: i64, deadline: Instant) -> u64 {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        while inner.entries.len() >= MAX_PENDING {
            inner = self
                .slot_freed
                .wait(inner)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
        let id = inner.next_id;
        inner.next_id = inner.next_id.wrapping_add(1);
        inner.entries.insert(
            id,
            PendingEntry {
                reply,
                torrent_id,
                deadline,
            },
        );
        id
    }

    /// Resolve a pending entry with data.  Notifies a blocked insert.
    fn resolve(&self, id: u64, data: &[u8]) {
        let removed = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.entries.remove(&id).map(|entry| entry.reply)
        };
        if let Some(reply) = removed {
            reply.resolve_data(data);
            self.slot_freed.notify_one();
        }
    }

    /// Resolve a pending entry with an errno.  Notifies a blocked insert.
    fn resolve_error(&self, id: u64, errno: libc::c_int) {
        let removed = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            inner.entries.remove(&id).map(|entry| entry.reply)
        };
        if let Some(reply) = removed {
            reply.resolve_error(errno);
            self.slot_freed.notify_one();
        }
    }

    /// Cancel all pending entries for a given `torrent_id` (unlink / remove_torrent).
    /// Returns the number of entries cancelled.
    fn cancel_by_torrent_id(&self, torrent_id: i64) -> usize {
        let removed: Vec<R> = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let ids: Vec<u64> = inner
                .entries
                .iter()
                .filter(|(_, e)| e.torrent_id == torrent_id)
                .map(|(id, _)| *id)
                .collect();
            ids.iter()
                .filter_map(|id| inner.entries.remove(id).map(|e| e.reply))
                .collect()
        };
        let count = removed.len();
        for reply in removed {
            reply.resolve_error(libc::EIO);
        }
        if count > 0 {
            self.slot_freed.notify_all();
        }
        count
    }

    /// Expire all entries whose deadline has passed.  Resolves them with
    /// `ENODATA` ("no data available"): a deferred read only expires when the
    /// piece-wait limit elapsed without the data arriving, i.e. the swarm has
    /// no seeder to serve it (TSI-2483).  Returns the number of entries
    /// expired.
    fn expire(&self) -> usize {
        let now = Instant::now();
        let removed: Vec<R> = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let ids: Vec<u64> = inner
                .entries
                .iter()
                .filter(|(_, e)| e.deadline <= now)
                .map(|(id, _)| *id)
                .collect();
            ids.iter()
                .filter_map(|id| inner.entries.remove(id).map(|e| e.reply))
                .collect()
        };
        let count = removed.len();
        for reply in removed {
            reply.resolve_error(libc::ENODATA);
        }
        if count > 0 {
            self.slot_freed.notify_all();
        }
        count
    }

    /// Resolve a pending entry with data, or with `ENODATA` when the data is
    /// empty but a non-zero `size` was requested (TSI-2293).
    ///
    /// The deferred-read worker job calls this instead of `resolve` so that
    /// an `Ok(Vec::new())` from `read_file_range_blocking` — which happens
    /// when the engine's internal `file_offset` computation disagrees with
    /// `pieces_on_disk`'s summed-file-sizes computation, landing on an
    /// early-return that yields 0 bytes without error — is translated into
    /// `ENODATA` rather than a 0-byte reply.  A 0-byte reply makes the
    /// kernel see EOF, so `dd` exits 0 and the user never learns the
    /// download failed.
    ///
    /// The `size` parameter is the originally requested read size (always
    /// greater than zero for deferred reads: `fs_service::read` verifies
    /// `offset < actual_size` before entering the Pending path).  When
    /// `size == 0` the empty data is a legitimate EOF and is passed
    /// through unchanged.
    fn resolve_or_enodata(&self, id: u64, data: &[u8], size: u32) {
        if data.is_empty() && size > 0 {
            self.resolve_error(id, libc::ENODATA);
        } else {
            self.resolve(id, data);
        }
    }
}
/// FUSE entry TTL (seconds).
const TTL: Duration = Duration::from_secs(1);

pub struct TorrentFs {
    service: FsService,
    pending_table: Arc<PendingTable>,
    read_timeout_secs: u64,
    worker_pool: Arc<WorkerPool>,
}
impl TorrentFs {
    fn read_timeout(config: &TorrentfsConfig) -> u64 {
        config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30)
    }

    /// Number of download worker threads (bounded pool). Defaults to the
    /// number of logical CPUs when the config leaves it unset.
    fn download_workers(config: &TorrentfsConfig) -> usize {
        config
            .concurrency
            .download_workers
            .filter(|&v| v > 0)
            .unwrap_or_else(|| {
                std::thread::available_parallelism()
                    .map(|n| n.get())
                    .unwrap_or(1)
            })
    }

    /// Capacity of the bounded download submission queue. Defaults to 256.
    fn download_queue_depth(config: &TorrentfsConfig) -> usize {
        config
            .concurrency
            .download_queue_depth
            .filter(|&v| v > 0)
            .unwrap_or(256)
    }
    /// Spawn a background thread that expires overdue pending replies every
    /// second, resolving each with ENODATA and decrementing the pending-reads
    /// metric once per expired entry.
    fn spawn_deadline_checker(pending_table: Arc<PendingTable>, metrics: Arc<Metrics>) {
        std::thread::spawn(move || {
            let tick = Duration::from_secs(1);
            loop {
                std::thread::sleep(tick);
                let expired = pending_table.expire();
                if expired > 0 {
                    for _ in 0..expired {
                        metrics.pending_reads_dec();
                    }
                    warn!(
                        "Pending table: {} entries expired (deadline exceeded)",
                        expired
                    );
                }
            }
        });
    }

    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        let timeout = Self::read_timeout(config);
        let service = FsService::new_with_cache_path(cache_path, config);
        let pending_table = Arc::new(PendingTable::new());
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(config),
            Self::download_queue_depth(config),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: timeout,
            worker_pool,
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        let service = FsService::new();
        let pending_table = Arc::new(PendingTable::new());
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(&TorrentfsConfig::default_config()),
            Self::download_queue_depth(&TorrentfsConfig::default_config()),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: 30,
            worker_pool,
        }
    }

    #[allow(dead_code)]
    pub fn new_with_db(_db: Database) -> Self {
        Self::new()
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &TorrentfsConfig,
    ) -> Self {
        let timeout = Self::read_timeout(config);
        let service = FsService::new_with_db_and_cache(db, cache_path, config);
        let pending_table = Arc::new(PendingTable::new());
        let metrics = service.metrics.clone();
        Self::spawn_deadline_checker(pending_table.clone(), metrics);
        let worker_pool = WorkerPool::new(
            Self::download_workers(config),
            Self::download_queue_depth(config),
        );
        Self {
            service,
            pending_table,
            read_timeout_secs: timeout,
            worker_pool,
        }
    }

    /// The download service, for the background alert consumer.
    pub fn download_service(&self) -> Option<&Arc<DownloadService>> {
        self.service.download_service.as_ref()
    }

    /// The bounded download worker pool, for graceful shutdown from `main`.
    pub fn worker_pool(&self) -> Arc<WorkerPool> {
        self.worker_pool.clone()
    }

    /// TSI-2454: clone the `Arc<OnceLock<Option<Notifier>>>` handle before
    /// `spawn_mount2` moves `self`.  After the session is live, `main`
    /// calls `notifier.set(Some(bg.notifier()))` on this handle to wire
    /// the kernel invalidation channel.
    pub fn notifier_handle(&self) -> Arc<std::sync::OnceLock<Option<fuser::Notifier>>> {
        self.service.notifier.clone()
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.service.get_cache_manager()
    }

    /// Generate global stats (delegates to FsService).
    pub fn generate_stats(&self) -> Vec<u8> {
        self.service.read_stats(StatsKind::Global)
    }

    /// Convert a domain `Attr` into a `fuser::FileAttr`, filling the fields the
    /// adapter owns: timestamps (from inode creation time) and uid/gid (process).
    fn to_fuse_attr(&self, attr: &Attr) -> fuser::FileAttr {
        let t = self.service.inode_mgr.creation_time;
        fuser::FileAttr {
            ino: attr.ino,
            size: attr.size,
            blocks: attr.size.div_ceil(512),
            atime: UNIX_EPOCH + t,
            mtime: UNIX_EPOCH + t,
            ctime: UNIX_EPOCH + t,
            crtime: UNIX_EPOCH + t,
            kind: kind_to_fuse(attr.kind),
            perm: attr.perm,
            nlink: attr.nlink,
            // SAFETY: libc::getuid() / libc::getgid() are always safe to call
            // in POSIX environments — they simply return the current process's
            // real user/group IDs and have no preconditions.
            uid: unsafe { libc::getuid() },
            gid: unsafe { libc::getgid() },
            rdev: 0,
            blksize: 512,
            flags: 0,
        }
    }
}

fn kind_to_fuse(kind: FileKind) -> fuser::FileType {
    match kind {
        FileKind::Directory => fuser::FileType::Directory,
        FileKind::RegularFile => fuser::FileType::RegularFile,
    }
}

// NOTE: fuser 0.16 requires `&mut self` on all Filesystem trait methods.
// Upgrading to a fuser version that accepts `&self` would enable true
// multi-threaded FUSE dispatch without serializing on a global lock.

impl Filesystem for TorrentFs {
    fn init(&mut self, _req: &Request<'_>, config: &mut KernelConfig) -> Result<(), c_int> {
        if let Err(e) = config.add_capabilities(fuser::consts::FUSE_ASYNC_READ) {
            tracing::warn!("Failed to set FUSE_CAP_ASYNC_READ: {:?}", e);
        } else {
            tracing::info!("FUSE_CAP_ASYNC_READ enabled");
        }
        Ok(())
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        match self.service.lookup(parent, &name.to_string_lossy()) {
            Ok(Some(entry)) => reply.entry(&TTL, &self.to_fuse_attr(&entry.attr), 0),
            Ok(None) => reply.error(FsError::NotFound.into()),
            Err(e) => reply.error(e.into()),
        }
    }

    fn getattr(&mut self, _req: &Request, ino: u64, _fh: Option<u64>, reply: ReplyAttr) {
        match self.service.getattr(ino) {
            Ok(attr) => reply.attr(&TTL, &self.to_fuse_attr(&attr)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn readdir(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        mut reply: ReplyDirectory,
    ) {
        match self.service.readdir(ino, offset) {
            Ok(entries) => {
                for entry in entries {
                    if reply.add(
                        entry.ino,
                        entry.offset,
                        kind_to_fuse(entry.kind),
                        &entry.name,
                    ) {
                        break;
                    }
                }
                reply.ok();
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn open(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.service.open(ino) {
            Ok(OpenOutcome { fh, direct_io }) => {
                // TSI-2246: data/ torrent files set direct_io so the kernel
                // bypasses its page cache; otherwise `filemap_read_folio`
                // converts any failed read into EIO, masking the daemon's
                // real errno (e.g. ENODATA for "no seeder").
                let flags = if direct_io { FOPEN_DIRECT_IO } else { 0 };
                reply.opened(fh, flags);
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn flush(&mut self, _req: &Request, ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        match self.service.flush(ino) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn release(
        &mut self,
        _req: &Request,
        _ino: u64,
        fh: u64,
        _flags: i32,
        _lock_owner: Option<u64>,
        _flush: bool,
        reply: ReplyEmpty,
    ) {
        match self.service.release(fh) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn read(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        size: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyData,
    ) {
        match self.service.read(ino, offset, size) {
            Ok(ReadOutcome::Ready(data)) => reply.data(&data),
            Ok(ReadOutcome::Pending {
                info,
                file_index,
                offset,
                size,
                info_hash,
                torrent_id,
            }) => {
                // Insert into the bounded pending table. `insert` blocks the
                // FUSE dispatch thread while the table is full (design §9
                // backpressure): never error, never return truncated data.
                let deadline = Instant::now() + Duration::from_secs(self.read_timeout_secs + 5);
                let id = self.pending_table.insert(reply, torrent_id, deadline);
                self.service.metrics.pending_reads_inc();
                debug!(
                    "Deferred read queued (ticket={}, info_hash={}, torrent_id={})",
                    id, info_hash, torrent_id
                );
                // Dispatch the read to the bounded worker pool and reply
                // asynchronously.
                match self.service.download_service.clone() {
                    Some(ds) => {
                        let metrics = self.service.metrics.clone();
                        let pt = self.pending_table.clone();
                        let job = Box::new(move || {
                            match ds.read_file_range_blocking(info, file_index, offset, size) {
                                Ok(data) => {
                                    if data.is_empty() && size > 0 {
                                        warn!(
                                            "Deferred read returned 0 bytes \
                                             for non-zero size (ticket={}, \
                                             size={}); resolving as ENODATA",
                                            id, size
                                        );
                                    }
                                    // TSI-2293: `resolve_or_enodata` guards
                                    // against `Ok(empty)` for a non-zero
                                    // `size`, which would otherwise be a
                                    // silent 0-byte EOF.  See the method's
                                    // doc comment for the root-cause analysis.
                                    pt.resolve_or_enodata(id, &data, size);
                                    metrics.pending_reads_dec();
                                }
                                Err(e) => {
                                    warn!("Failed to read torrent file data (async): {:?}", e);
                                    pt.resolve_error(id, FsError::from(e).into());
                                    metrics.pending_reads_dec();
                                }
                            }
                        });
                        // `submit` blocks while the queue is full (backpressure);
                        // it only returns `Err` when the pool is shutting down.
                        if let Err(_job) = self.worker_pool.submit(job) {
                            warn!("Download worker pool shutting down, dropping read");
                            self.pending_table.resolve_error(id, libc::EIO);
                            self.service.metrics.pending_reads_dec();
                        }
                    }
                    None => {
                        self.pending_table.resolve_error(
                            id,
                            FsError::Internal("download manager not available".to_string()).into(),
                        );
                        self.service.metrics.pending_reads_dec();
                    }
                }
            }
            Err(e) => reply.error(e.into()),
        }
    }

    fn opendir(&mut self, _req: &Request, ino: u64, _flags: i32, reply: ReplyOpen) {
        match self.service.opendir(ino) {
            Ok(()) => reply.opened(0, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: i32, reply: ReplyEmpty) {
        reply.ok();
    }

    fn mknod(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _rdev: u32,
        reply: ReplyEntry,
    ) {
        match self.service.mknod(parent, &name.to_string_lossy()) {
            Ok(entry) => reply.entry(&TTL, &self.to_fuse_attr(&entry.attr), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn create(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: ReplyCreate,
    ) {
        match self.service.create(parent, &name.to_string_lossy()) {
            Ok(created) => reply.created(&TTL, &self.to_fuse_attr(&created.attr), 0, created.fh, 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn write(
        &mut self,
        _req: &Request,
        ino: u64,
        _fh: u64,
        offset: i64,
        data: &[u8],
        _write_flags: u32,
        _flags: i32,
        _lock_owner: Option<u64>,
        reply: ReplyWrite,
    ) {
        match self.service.write(ino, offset, data) {
            Ok(n) => reply.written(n),
            Err(e) => reply.error(e.into()),
        }
    }

    fn mkdir(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        reply: ReplyEntry,
    ) {
        match self.service.mkdir(parent, &name.to_string_lossy()) {
            Ok(attr) => reply.entry(&TTL, &self.to_fuse_attr(&attr), 0),
            Err(e) => reply.error(e.into()),
        }
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.unlink(parent, &name.to_string_lossy()) {
            Ok(Some(torrent_id)) => {
                // Cancel any in-flight reads for the removed torrent.
                let cancelled = self.pending_table.cancel_by_torrent_id(torrent_id);
                if cancelled > 0 {
                    warn!(
                        "Cancelled {} pending read(s) for removed torrent_id={}",
                        cancelled, torrent_id
                    );
                    for _ in 0..cancelled {
                        self.service.metrics.pending_reads_dec();
                    }
                }
                reply.ok();
            }
            Ok(None) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
    fn setattr(
        &mut self,
        _req: &Request,
        ino: u64,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        _size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // Attributes are virtual and immutable; return the current attributes.
        // TSI-2533: chmod on the read-only `data/` namespace must return
        // EROFS (delegated to `FsService::setattr`), never a silent success.
        match self.service.setattr(ino) {
            Ok(attr) => reply.attr(&TTL, &self.to_fuse_attr(&attr)),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rename(
        &mut self,
        _req: &Request,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        _flags: u32,
        reply: ReplyEmpty,
    ) {
        match self.service.rename(
            parent,
            &name.to_string_lossy(),
            newparent,
            &newname.to_string_lossy(),
        ) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        match self.service.rmdir(parent, &name.to_string_lossy()) {
            Ok(()) => reply.ok(),
            Err(e) => reply.error(e.into()),
        }
    }
}
#[cfg(test)]
mod pending_tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicI32, AtomicUsize, Ordering};
    /// Counts how many times the reply is resolved (with data or error).  This
    /// is the test double for `fuser::ReplyData` and verifies the "consumed
    /// exactly once" invariant.
    #[derive(Debug)]
    struct MockReply {
        resolves: Arc<AtomicUsize>,
        /// Last errno passed to `resolve_error` (0 if the reply was resolved
        /// with data, or never resolved).
        last_errno: Arc<AtomicI32>,
        /// Whether `resolve_data` was called with a non-empty slice.
        got_data: Arc<AtomicBool>,
    }

    impl MockReply {
        /// Create a reply that tracks resolution count only (legacy tests).
        fn tracking(resolves: Arc<AtomicUsize>) -> Self {
            Self {
                resolves,
                last_errno: Arc::new(AtomicI32::new(0)),
                got_data: Arc::new(AtomicBool::new(false)),
            }
        }

        /// Create a reply with full tracking: resolution count, last errno,
        /// and whether non-empty data was delivered.
        fn tracked() -> (Self, Arc<AtomicUsize>, Arc<AtomicI32>, Arc<AtomicBool>) {
            let resolves = Arc::new(AtomicUsize::new(0));
            let last_errno = Arc::new(AtomicI32::new(0));
            let got_data = Arc::new(AtomicBool::new(false));
            (
                Self {
                    resolves: Arc::clone(&resolves),
                    last_errno: Arc::clone(&last_errno),
                    got_data: Arc::clone(&got_data),
                },
                resolves,
                last_errno,
                got_data,
            )
        }
    }

    impl PendingReply for MockReply {
        fn resolve_data(self, data: &[u8]) {
            if !data.is_empty() {
                self.got_data.store(true, Ordering::Relaxed);
            }
            self.resolves.fetch_add(1, Ordering::Relaxed);
        }
        fn resolve_error(self, errno: libc::c_int) {
            self.last_errno.store(errno, Ordering::Relaxed);
            self.resolves.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn deadline_in(d: std::time::Duration) -> Instant {
        Instant::now() + d
    }

    #[test]
    fn insert_blocks_when_full_and_resumes_after_slot_frees() {
        let table = Arc::new(PendingTable::<MockReply>::new());
        let resolves = Arc::new(AtomicUsize::new(0));

        for i in 0..MAX_PENDING {
            let id = table.insert(
                MockReply::tracking(resolves.clone()),
                i as i64,
                deadline_in(std::time::Duration::from_secs(60)),
            );
            assert_eq!(id, i as u64, "insert {i} should succeed");
        }

        // The (MAX_PENDING + 1)-th insert blocks instead of failing: it waits
        // for a free slot (backpressure, no error).
        let table_clone = Arc::clone(&table);
        let resolves_clone = Arc::clone(&resolves);
        let blocker = std::thread::spawn(move || {
            table_clone.insert(
                MockReply::tracking(resolves_clone),
                MAX_PENDING as i64,
                deadline_in(std::time::Duration::from_secs(60)),
            )
        });

        // Let the blocker reach the condvar wait, then free one slot.
        std::thread::sleep(std::time::Duration::from_millis(100));
        table.resolve(0, b"ok");

        let new_id = blocker.join().unwrap();
        assert_eq!(new_id, MAX_PENDING as u64);
        // Only the resolved entry (id 0) was consumed.
        assert_eq!(resolves.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn cancel_by_torrent_id_resolves_only_matching_tickets() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));
        let a1 = table.insert(
            MockReply::tracking(resolves.clone()),
            1,
            deadline_in(std::time::Duration::from_secs(60)),
        );
        let a2 = table.insert(
            MockReply::tracking(resolves.clone()),
            1,
            deadline_in(std::time::Duration::from_secs(60)),
        );
        let b1 = table.insert(
            MockReply::tracking(resolves.clone()),
            2,
            deadline_in(std::time::Duration::from_secs(60)),
        );

        let cancelled = table.cancel_by_torrent_id(1);
        assert_eq!(cancelled, 2);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);

        // The remaining entry (torrent_id=2) resolves exactly once.
        table.resolve(b1, b"ok");
        assert_eq!(resolves.load(Ordering::Relaxed), 3);

        // Cancelled tickets are gone: resolving them again is a no-op.
        table.resolve(a1, b"dup");
        table.resolve(a2, b"dup");
        assert_eq!(resolves.load(Ordering::Relaxed), 3);
    }

    /// TSI-2483: an expired (piece-wait-elapsed) deferred read resolves with
    /// ENODATA, not EIO, so `dd`/`cat` see "no data available" instead of
    /// "input/output error" when the swarm has no seeder.
    #[test]
    fn expire_resolves_overdue_tickets_with_enodata() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (reply, resolves, last_errno, _got_data) = MockReply::tracked();

        let overdue = table.insert(reply, 7, deadline_in(std::time::Duration::from_secs(0)));
        let future = table.insert(
            MockReply::tracking(resolves.clone()),
            7,
            deadline_in(std::time::Duration::from_secs(60)),
        );

        let expired = table.expire();
        assert_eq!(expired, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 1);
        assert_eq!(
            last_errno.load(Ordering::Relaxed),
            libc::ENODATA,
            "expired deferred read must resolve as ENODATA"
        );

        // The future entry is unaffected.
        table.resolve(future, b"ok");
        assert_eq!(resolves.load(Ordering::Relaxed), 2);

        // The overdue entry is gone.
        table.resolve(overdue, b"dup");
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
    }

    /// TSI-2293: when a deferred read's worker returns `Ok(empty)` for a
    /// non-zero-size request, `resolve_or_enodata` must resolve the reply
    /// with `ENODATA` — not 0 bytes (which the kernel interprets as EOF
    /// and `dd` exits 0).  This tests the production method directly.
    #[test]
    fn empty_data_for_nonzero_size_resolves_as_enodata() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (reply, _resolves, last_errno, got_data) = MockReply::tracked();

        let id = table.insert(reply, 42, deadline_in(std::time::Duration::from_secs(60)));

        // Worker got Ok(empty) for size=4096 → resolve_or_enodata must
        // translate to ENODATA.
        table.resolve_or_enodata(id, &[], 4096);

        assert_eq!(
            last_errno.load(Ordering::Relaxed),
            libc::ENODATA,
            "empty data for non-zero size must resolve as ENODATA"
        );
        assert!(
            !got_data.load(Ordering::Relaxed),
            "no data should have been delivered"
        );
    }

    /// TSI-2293: empty data for a zero-size read is a legitimate EOF —
    /// `resolve_or_enodata` must pass it through as data, not ENODATA.
    #[test]
    fn empty_data_for_zero_size_resolves_as_data() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (reply, _resolves, last_errno, got_data) = MockReply::tracked();

        let id = table.insert(reply, 42, deadline_in(std::time::Duration::from_secs(60)));

        table.resolve_or_enodata(id, &[], 0);

        assert_eq!(
            last_errno.load(Ordering::Relaxed),
            0,
            "zero-size read should not produce an error"
        );
        assert!(
            !got_data.load(Ordering::Relaxed),
            "empty data should not set got_data"
        );
    }

    /// TSI-2293: non-empty data is always resolved as data, regardless of
    /// `size`.
    #[test]
    fn nonempty_data_resolves_as_data() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (reply, _resolves, _last_errno, got_data) = MockReply::tracked();

        let id = table.insert(reply, 42, deadline_in(std::time::Duration::from_secs(60)));

        table.resolve_or_enodata(id, b"payload", 7);

        assert!(
            got_data.load(Ordering::Relaxed),
            "non-empty data must be delivered"
        );
    }
}

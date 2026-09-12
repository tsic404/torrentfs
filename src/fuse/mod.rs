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

/// Dispatch margin (seconds) added to the engine's read budget when computing
/// the deferred-read deadline: covers worker-pool scheduling and the alert
/// consumer latency.  This is a heuristic safety pad, not a hard guarantee
/// (TSI-2751).
const READ_DEADLINE_MARGIN_SECS: u64 = 5;

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

/// Byte-range identity of a deferred read.
///
/// Two reads with the same key resolve to identical bytes, so they coalesce
/// onto a single engine download and the result fans out to every waiter
/// (TSI-2896: concurrent first reads of the same uncached piece previously
/// serialized on the engine thread, so later readers' tickets expired with
/// ENODATA while waiting their turn).
///
/// Keyed by `info_hash` (content identity) rather than `torrent_id`, so
/// duplicate torrents sharing an info_hash also coalesce; `file_index`,
/// `offset` and `size` select the exact byte range the engine downloads.
#[derive(Hash, Eq, PartialEq, Clone)]
struct RangeKey {
    info_hash: String,
    file_index: i32,
    offset: u64,
    size: u32,
}

/// A single waiter in a pending group: one FUSE reply, its cancellation scope
/// (`torrent_id`), and its own deadline.  Waiters in a group share one engine
/// download but may belong to different torrents (duplicate info_hash) and
/// arrive at different times, so cancellation and deadline expiry are both
/// tracked per waiter, not per group (TSI-2896 review: a late joiner must not
/// inherit the leader's earlier deadline).
struct PendingEntry<R: PendingReply> {
    reply: R,
    torrent_id: i64,
    deadline: Instant,
}

/// A group of waiters for the same byte range, sharing one engine download.
struct PendingGroup<R: PendingReply> {
    key: RangeKey,
    waiters: Vec<PendingEntry<R>>,
}

/// Bounded table of FUSE read replies that are waiting for pieces to download.
///
/// * Capacity is bounded at `MAX_PENDING` concurrent *ranges* (groups).  When
///   full, [`insert`](Self::insert) blocks the caller (the FUSE dispatch
///   thread) on a condvar until a worker or the deadline checker removes a
///   group — the design §9 backpressure model: block briefly, never error and
///   never return truncated data.
/// * Concurrent readers of the same byte range coalesce onto one group and
///   share a single engine download; the result fans out to every waiter
///   (TSI-2896).
/// * Each waiter carries a deadline; a background thread expires overdue
///   waiters with ENODATA ("no data available" — the piece-wait limit elapsed
///   with no seeder to serve it).
/// * `unlink` / `remove_torrent` cancels in-flight reads for the removed
///   torrent and resolves their tickets with EIO.
/// * Every reply is consumed exactly once (ok or error), zero leak.
struct PendingTable<R: PendingReply = ReplyData> {
    inner: Mutex<Inner<R>>,
    /// Signalled whenever a group is removed, so a blocked `insert` can retry
    /// once a slot frees up.
    slot_freed: Condvar,
}

struct Inner<R: PendingReply> {
    /// ticket id → group of waiters.
    groups: HashMap<u64, PendingGroup<R>>,
    /// range key → ticket id (the coalescing index).
    by_range: HashMap<RangeKey, u64>,
    next_id: u64,
}

/// Result of inserting a pending reply into the table.
enum InsertOutcome {
    /// This insert created a new in-flight group; the caller must dispatch a
    /// worker to download the range and resolve the returned ticket id.
    Leader(u64),
    /// This insert joined an existing in-flight group for the same range; the
    /// leader's worker will resolve this reply.  No worker dispatch needed.
    Joined,
}

impl<R: PendingReply> PendingTable<R> {
    fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                groups: HashMap::with_capacity(MAX_PENDING),
                by_range: HashMap::with_capacity(MAX_PENDING),
                next_id: 0,
            }),
            slot_freed: Condvar::new(),
        }
    }

    /// Insert a pending reply for `key`, blocking while the table is full
    /// (backpressure).  Coalesces with an existing in-flight group for the
    /// same range so the piece downloads once and the result fans out.
    fn insert(&self, reply: R, torrent_id: i64, deadline: Instant, key: RangeKey) -> InsertOutcome {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        loop {
            // Coalesce onto an existing in-flight read of the same range: the
            // engine downloads the piece once and the result fans out to every
            // waiter.
            if let Some(&id) = inner.by_range.get(&key) {
                let group = inner
                    .groups
                    .get_mut(&id)
                    .expect("range index must reference a live group");
                group.waiters.push(PendingEntry {
                    reply,
                    torrent_id,
                    deadline,
                });
                return InsertOutcome::Joined;
            }
            if inner.groups.len() < MAX_PENDING {
                let id = inner.next_id;
                inner.next_id = inner.next_id.wrapping_add(1);
                inner.by_range.insert(key.clone(), id);
                inner.groups.insert(
                    id,
                    PendingGroup {
                        key,
                        waiters: vec![PendingEntry {
                            reply,
                            torrent_id,
                            deadline,
                        }],
                    },
                );
                return InsertOutcome::Leader(id);
            }
            inner = self
                .slot_freed
                .wait(inner)
                .unwrap_or_else(|poisoned| poisoned.into_inner());
        }
    }

    /// Remove a group by ticket id, cleaning up the coalescing index.  Returns
    /// the removed group.
    fn remove_group(&self, id: u64) -> Option<PendingGroup<R>> {
        let mut inner = self
            .inner
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let group = inner.groups.remove(&id)?;
        inner.by_range.remove(&group.key);
        Some(group)
    }

    /// Resolve a pending group with data, fanning out to every waiter.  Returns
    /// the number of waiters resolved.
    fn resolve(&self, id: u64, data: &[u8]) -> usize {
        match self.remove_group(id) {
            Some(group) => {
                let n = group.waiters.len();
                for waiter in group.waiters {
                    waiter.reply.resolve_data(data);
                }
                self.slot_freed.notify_all();
                n
            }
            None => 0,
        }
    }

    /// Resolve a pending group with an errno, fanning out to every waiter.
    /// Returns the number of waiters resolved.
    fn resolve_error(&self, id: u64, errno: libc::c_int) -> usize {
        match self.remove_group(id) {
            Some(group) => {
                let n = group.waiters.len();
                for waiter in group.waiters {
                    waiter.reply.resolve_error(errno);
                }
                self.slot_freed.notify_all();
                n
            }
            None => 0,
        }
    }

    /// Cancel all pending waiters for a given `torrent_id` (unlink /
    /// remove_torrent), resolving each with EIO.  A group with remaining
    /// waiters (other torrents sharing the info_hash) stays in flight and its
    /// worker still resolves those.  Returns the number of waiters cancelled.
    fn cancel_by_torrent_id(&self, torrent_id: i64) -> usize {
        let removed: Vec<R> = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut removed = Vec::new();
            let ids: Vec<u64> = inner.groups.keys().copied().collect();
            for id in ids {
                let group_empty = {
                    let group = inner
                        .groups
                        .get_mut(&id)
                        .expect("id collected from live groups");
                    let mut i = 0;
                    while i < group.waiters.len() {
                        if group.waiters[i].torrent_id == torrent_id {
                            removed.push(group.waiters.remove(i).reply);
                        } else {
                            i += 1;
                        }
                    }
                    group.waiters.is_empty()
                };
                if group_empty {
                    if let Some(group) = inner.groups.remove(&id) {
                        inner.by_range.remove(&group.key);
                    }
                }
            }
            removed
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

    /// Expire every waiter whose own deadline has passed, resolving each with
    /// `ENODATA` ("no data available"): a deferred read only expires when the
    /// piece-wait limit elapsed without the data arriving, i.e. the swarm has
    /// no seeder to serve it (TSI-2483).  Expiry is per waiter (TSI-2896
    /// review): a late joiner keeps its own later deadline instead of
    /// inheriting the leader's.  A group is dropped only when all its waiters
    /// are gone.  Returns the number of waiters expired.
    fn expire(&self) -> usize {
        let now = Instant::now();
        let removed: Vec<R> = {
            let mut inner = self
                .inner
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let mut removed = Vec::new();
            let ids: Vec<u64> = inner.groups.keys().copied().collect();
            for id in ids {
                let group_empty = {
                    let group = inner
                        .groups
                        .get_mut(&id)
                        .expect("id collected from live groups");
                    let mut i = 0;
                    while i < group.waiters.len() {
                        if group.waiters[i].deadline <= now {
                            removed.push(group.waiters.remove(i).reply);
                        } else {
                            i += 1;
                        }
                    }
                    group.waiters.is_empty()
                };
                if group_empty {
                    if let Some(group) = inner.groups.remove(&id) {
                        inner.by_range.remove(&group.key);
                    }
                }
            }
            removed
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

    /// Resolve a pending group with data, or with `ENODATA` when the data is
    /// empty but a non-zero `size` was requested (TSI-2293).  Returns the
    /// number of waiters resolved.
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
    fn resolve_or_enodata(&self, id: u64, data: &[u8], size: u32) -> usize {
        if data.is_empty() && size > 0 {
            self.resolve_error(id, libc::ENODATA)
        } else {
            self.resolve(id, data)
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
                // TSI-2751: the deadline must cover the engine's worst-case
                // read budget (state wait + recheck wait + peer-discovery wait
                // + piece wait), not just `read_timeout_secs`.  The old
                // `read_timeout + 5` (35s default) expired tickets before the
                // engine's own ~39s slow-path budget elapsed, so a
                // slow-but-present seeder that would have served the piece got
                // a premature ENODATA.
                let budget_secs = self
                    .service
                    .download_service
                    .as_ref()
                    .map(|ds| ds.read_wait_budget_secs())
                    .unwrap_or(self.read_timeout_secs);
                let deadline = Instant::now()
                    + Duration::from_secs(budget_secs.saturating_add(READ_DEADLINE_MARGIN_SECS));

                // TSI-2896: coalesce concurrent readers of the same byte range
                // onto a single engine download so the piece is fetched once
                // and the result fans out to every waiter.  `insert` blocks the
                // FUSE dispatch thread while the table is full (design §9
                // backpressure): never error, never return truncated data.
                //
                // The pending-reads counter is bumped *before* `insert` so the
                // waiter is counted before it becomes visible to any resolver
                // (worker, deadline checker, or `cancel_by_torrent_id`); the
                // old order (inc after insert) left a window where a resolver
                // could `dec` first, then the late `inc` permanently inflated
                // the gauge (TOCTOU, TSI-2896 review).
                self.service.metrics.pending_reads_inc();
                let key = RangeKey {
                    info_hash: info_hash.clone(),
                    file_index,
                    offset,
                    size,
                };
                let outcome = self.pending_table.insert(reply, torrent_id, deadline, key);

                match outcome {
                    InsertOutcome::Joined => {
                        debug!(
                            "Deferred read coalesced onto existing download \
                             (info_hash={}, torrent_id={})",
                            info_hash, torrent_id
                        );
                    }
                    InsertOutcome::Leader(id) => {
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
                                    match ds
                                        .read_file_range_blocking(info, file_index, offset, size)
                                    {
                                        Ok(data) => {
                                            if data.is_empty() && size > 0 {
                                                warn!(
                                                    "Deferred read returned 0 bytes \
                                                     for non-zero size (ticket={}, \
                                                     size={}); resolving as ENODATA",
                                                    id, size
                                                );
                                            }
                                            // TSI-2293: `resolve_or_enodata`
                                            // guards against `Ok(empty)` for a
                                            // non-zero `size`.  Resolves every
                                            // coalesced waiter with the shared
                                            // result (TSI-2896).
                                            let n = pt.resolve_or_enodata(id, &data, size);
                                            metrics.pending_reads_dec_n(n);
                                        }
                                        Err(e) => {
                                            warn!(
                                                "Failed to read torrent file data (async): {:?}",
                                                e
                                            );
                                            let n = pt.resolve_error(id, FsError::from(e).into());
                                            metrics.pending_reads_dec_n(n);
                                        }
                                    }
                                });
                                // `submit` blocks while the queue is full
                                // (backpressure); it only returns `Err` when
                                // the pool is shutting down.
                                if let Err(_job) = self.worker_pool.submit(job) {
                                    warn!("Download worker pool shutting down, dropping read");
                                    let n = self.pending_table.resolve_error(id, libc::EIO);
                                    self.service.metrics.pending_reads_dec_n(n);
                                }
                            }
                            None => {
                                let n = self.pending_table.resolve_error(
                                    id,
                                    FsError::Internal("download manager not available".to_string())
                                        .into(),
                                );
                                self.service.metrics.pending_reads_dec_n(n);
                            }
                        }
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
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<std::time::SystemTime>,
        _fh: Option<u64>,
        crtime: Option<std::time::SystemTime>,
        chgtime: Option<std::time::SystemTime>,
        bkuptime: Option<std::time::SystemTime>,
        _flags: Option<u32>,
        reply: ReplyAttr,
    ) {
        // TSI-2533/TSI-2536: chmod on a virtual/read-only namespace must not
        // silently succeed — `FsService::setattr` returns EROFS for `data/`
        // and EPERM for `metadata/`, `.stats`, and the root directory.
        //
        // TSI-3064: a *pure truncate* (the `O_TRUNC` overwrite path, e.g.
        // `cp` onto an existing `metadata/` file) is a legitimate write, not
        // an attribute change — it must reach the service with `size =
        // Some(n)` instead of being folded into the blanket EPERM. A request
        // that also carries a mode/uid/gid/timestamp change is treated as an
        // attribute change (truncate = None) and still returns EPERM.
        let attr_change = mode.is_some()
            || uid.is_some()
            || gid.is_some()
            || atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some();
        let truncate = if attr_change { None } else { size };
        match self.service.setattr(ino, truncate) {
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

    fn symlink(
        &mut self,
        _req: &Request,
        parent: u64,
        _link_name: &OsStr,
        _target: &std::path::Path,
        reply: ReplyEntry,
    ) {
        // TSI-2537: `symlink` is unsupported.  `data/` must return `EROFS`
        // (matching chmod/write), every other namespace `EPERM` — the fuser
        // default returns `EPERM` for all parents, which never reached the
        // read-only-namespace guard.
        reply.error(self.service.symlink(parent).into());
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

    /// A distinct range key per `salt`, so tests that exercise separate
    /// tickets don't accidentally coalesce.
    fn key(salt: u32) -> RangeKey {
        RangeKey {
            info_hash: format!("ih{salt}"),
            file_index: 0,
            offset: salt as u64,
            size: 1,
        }
    }

    /// Extract the ticket id from a `Leader` outcome, panicking on `Joined`.
    fn leader(outcome: InsertOutcome) -> u64 {
        match outcome {
            InsertOutcome::Leader(id) => id,
            InsertOutcome::Joined => panic!("expected a Leader outcome"),
        }
    }

    #[test]
    fn insert_blocks_when_full_and_resumes_after_slot_frees() {
        let table = Arc::new(PendingTable::<MockReply>::new());
        let resolves = Arc::new(AtomicUsize::new(0));

        for i in 0..MAX_PENDING {
            let outcome = table.insert(
                MockReply::tracking(resolves.clone()),
                i as i64,
                deadline_in(std::time::Duration::from_secs(60)),
                key(i as u32),
            );
            assert_eq!(leader(outcome), i as u64, "insert {i} should succeed");
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
                key(MAX_PENDING as u32),
            )
        });

        // Let the blocker reach the condvar wait, then free one slot.
        std::thread::sleep(std::time::Duration::from_millis(100));
        table.resolve(0, b"ok");

        let outcome = blocker.join().unwrap();
        assert_eq!(leader(outcome), MAX_PENDING as u64);
        // Only the resolved group (id 0) was consumed.
        assert_eq!(resolves.load(Ordering::Relaxed), 1);
    }
    #[test]
    fn cancel_by_torrent_id_resolves_only_matching_tickets() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));
        let a1 = leader(table.insert(
            MockReply::tracking(resolves.clone()),
            1,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));
        let a2 = leader(table.insert(
            MockReply::tracking(resolves.clone()),
            1,
            deadline_in(std::time::Duration::from_secs(60)),
            key(1),
        ));
        let b1 = leader(table.insert(
            MockReply::tracking(resolves.clone()),
            2,
            deadline_in(std::time::Duration::from_secs(60)),
            key(2),
        ));

        let cancelled = table.cancel_by_torrent_id(1);
        assert_eq!(cancelled, 2);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);

        // The remaining group (torrent_id=2) resolves exactly once.
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

        let overdue = leader(table.insert(
            reply,
            7,
            deadline_in(std::time::Duration::from_secs(0)),
            key(0),
        ));
        let future = leader(table.insert(
            MockReply::tracking(resolves.clone()),
            7,
            deadline_in(std::time::Duration::from_secs(60)),
            key(1),
        ));

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

        let id = leader(table.insert(
            reply,
            42,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));

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

        let id = leader(table.insert(
            reply,
            42,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));

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

        let id = leader(table.insert(
            reply,
            42,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));

        table.resolve_or_enodata(id, b"payload", 7);

        assert!(
            got_data.load(Ordering::Relaxed),
            "non-empty data must be delivered"
        );
    }

    /// TSI-2896: concurrent readers of the same byte range coalesce onto one
    /// group sharing a single engine download; resolving the leader fans the
    /// data out to every waiter exactly once.
    #[test]
    fn coalesces_same_range_into_one_group_and_fans_out_data() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (r0, resolves, last_errno, got_data) = MockReply::tracked();

        let id = leader(table.insert(
            r0,
            9,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));
        for _ in 0..2 {
            match table.insert(
                MockReply::tracking(resolves.clone()),
                9,
                deadline_in(std::time::Duration::from_secs(60)),
                key(0),
            ) {
                InsertOutcome::Joined => {}
                InsertOutcome::Leader(_) => panic!("same-range insert must coalesce"),
            }
        }

        // Resolving the leader fans the data out to all three waiters.
        let resolved = table.resolve(id, b"shared");
        assert_eq!(resolved, 3);
        assert_eq!(resolves.load(Ordering::Relaxed), 3);
        assert_eq!(last_errno.load(Ordering::Relaxed), 0);
        assert!(got_data.load(Ordering::Relaxed));

        // The group is gone: a duplicate resolve is a no-op.
        assert_eq!(table.resolve(id, b"dup"), 0);
        assert_eq!(resolves.load(Ordering::Relaxed), 3);
    }

    /// TSI-2896: a coalesced group resolves every waiter with the same errno
    /// when the shared download fails, not just the leader.
    #[test]
    fn coalesced_group_fans_out_error() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));
        let last_errno = Arc::new(AtomicI32::new(0));
        let got_data = Arc::new(AtomicBool::new(false));

        // Two waiters for the same range, tracking into the same counters.
        let r0 = MockReply {
            resolves: resolves.clone(),
            last_errno: last_errno.clone(),
            got_data: got_data.clone(),
        };
        let r1 = MockReply {
            resolves: resolves.clone(),
            last_errno: last_errno.clone(),
            got_data: got_data.clone(),
        };
        let id = leader(table.insert(
            r0,
            5,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));
        match table.insert(
            r1,
            5,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ) {
            InsertOutcome::Joined => {}
            InsertOutcome::Leader(_) => panic!("same-range insert must coalesce"),
        }

        let resolved = table.resolve_error(id, libc::EIO);
        assert_eq!(resolved, 2);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
        assert_eq!(last_errno.load(Ordering::Relaxed), libc::EIO);
        assert!(!got_data.load(Ordering::Relaxed));
    }

    /// TSI-2896: a coalesced group expires together — every waiter gets
    /// ENODATA, and `expire` reports the waiter count, not the group count.
    #[test]
    fn coalesced_group_expires_together() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (r0, resolves, last_errno, _) = MockReply::tracked();
        let _ = table.insert(
            r0,
            5,
            deadline_in(std::time::Duration::from_secs(0)),
            key(0),
        );
        match table.insert(
            MockReply::tracking(resolves.clone()),
            5,
            deadline_in(std::time::Duration::from_secs(0)),
            key(0),
        ) {
            InsertOutcome::Joined => {}
            InsertOutcome::Leader(_) => panic!("same-range insert must coalesce"),
        }

        let expired = table.expire();
        assert_eq!(expired, 2);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
        assert_eq!(last_errno.load(Ordering::Relaxed), libc::ENODATA);
    }

    /// TSI-2896: cancellation is per waiter — cancelling one torrent in a
    /// coalesced group leaves the other torrent's waiter in flight.
    #[test]
    fn coalesced_group_cancels_only_matching_torrent() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (r0, resolves, last_errno, _) = MockReply::tracked();
        let id = leader(table.insert(
            r0,
            1,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));
        match table.insert(
            MockReply::tracking(resolves.clone()),
            2,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ) {
            InsertOutcome::Joined => {}
            InsertOutcome::Leader(_) => panic!("same-range insert must coalesce"),
        }

        // Cancel torrent_id=1: only its waiter is resolved with EIO.
        let cancelled = table.cancel_by_torrent_id(1);
        assert_eq!(cancelled, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 1);
        assert_eq!(last_errno.load(Ordering::Relaxed), libc::EIO);

        // The surviving waiter (torrent_id=2) still resolves with data.
        let resolved = table.resolve(id, b"ok");
        assert_eq!(resolved, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
    }

    /// TSI-2896 review: after a group is resolved (removing its `by_range`
    /// entry), re-inserting the same range key must create a fresh group, not
    /// hit a stale coalescing index entry.
    #[test]
    fn reinsert_same_range_after_resolve_creates_new_group() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let (r0, resolves, _, _) = MockReply::tracked();

        let id0 = leader(table.insert(
            r0,
            3,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ));
        assert_eq!(table.resolve(id0, b"first"), 1);

        // Same range after resolution → a new group, not a stale Joined.
        match table.insert(
            MockReply::tracking(resolves.clone()),
            3,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ) {
            InsertOutcome::Leader(id1) => {
                assert_ne!(id1, id0, "reinsert must mint a fresh group id");
                assert_eq!(table.resolve(id1, b"second"), 1);
            }
            InsertOutcome::Joined => panic!("reinsert must create a new group, not coalesce"),
        }
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
    }

    /// TSI-2896 review: a waiter that joins after the leader's deadline keeps
    /// its own later deadline — expiry removes only the overdue leader, not the
    /// still-fresh joiner.
    #[test]
    fn late_joiner_keeps_own_deadline() {
        let table: PendingTable<MockReply> = PendingTable::new();
        let resolves = Arc::new(AtomicUsize::new(0));
        let last_errno0 = Arc::new(AtomicI32::new(0));
        let got_data0 = Arc::new(AtomicBool::new(false));
        let last_errno1 = Arc::new(AtomicI32::new(0));
        let got_data1 = Arc::new(AtomicBool::new(false));

        let r0 = MockReply {
            resolves: resolves.clone(),
            last_errno: last_errno0.clone(),
            got_data: got_data0.clone(),
        };
        let r1 = MockReply {
            resolves: resolves.clone(),
            last_errno: last_errno1.clone(),
            got_data: got_data1.clone(),
        };

        // Leader is already overdue; joiner is still fresh.
        let id = leader(table.insert(
            r0,
            5,
            deadline_in(std::time::Duration::from_secs(0)),
            key(0),
        ));
        match table.insert(
            r1,
            5,
            deadline_in(std::time::Duration::from_secs(60)),
            key(0),
        ) {
            InsertOutcome::Joined => {}
            InsertOutcome::Leader(_) => panic!("same-range insert must coalesce"),
        }

        // Expiry removes only the overdue leader; the fresh joiner survives.
        let expired = table.expire();
        assert_eq!(expired, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 1);
        assert_eq!(last_errno0.load(Ordering::Relaxed), libc::ENODATA);

        // The surviving joiner still resolves with data via the shared worker.
        let resolved = table.resolve(id, b"ok");
        assert_eq!(resolved, 1);
        assert_eq!(resolves.load(Ordering::Relaxed), 2);
        assert_eq!(last_errno1.load(Ordering::Relaxed), 0);
        assert!(got_data1.load(Ordering::Relaxed));
    }
}

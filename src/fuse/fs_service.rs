//! `FsService` — the filesystem application service layer.
//!
//! This is where the domain logic lives: inode resolution, `.torrent`
//! lifecycle persistence, data reads (local vs. remote), and `.stats`
//! rendering. `FsService` imports **neither** `fuser` **nor** `libc`; it
//! returns domain results (`FsError`, `Attr`, `ReadOutcome`, …) and leaves all
//! protocol/errno adaptation to `TorrentFs` (the thin adapter in `super::mod`).
//!
//! See the architecture design doc §5.2 for the namespace × operation table.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use tracing::{debug, error, info, warn};

use sha1_smol::Sha1;

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::{Database, MoveOverwriteResult};
use crate::domain::fs_error::{FsError, FsResult};
use crate::infrastructure::metrics::Metrics;
use crate::metadata::TorrentInfo;
use crate::services::download::DownloadService;
use crate::services::torrent::{ContentVersion, FileIdentity, TorrentService};

use super::fs_types::{
    Attr, Created, DirEntry, Entry, FileKind, OpenOutcome, ReadOutcome, StatsKind,
};
use super::inodes::{
    DataInode, InodeData, InodeManager, DATA_DIR_INO_BASE, DATA_INO, DATA_TORRENT_INO_BASE,
    INVALID_INO, MAX_TORRENT_SIZE, METADATA_INO, NEXT_FH, NEXT_INO, ROOT_INO, STATS_INO,
};
#[cfg(test)]
use super::inodes::{DATA_FILE_INO_BASE, SOURCE_PATH_DIR_INO_BASE};
use super::lookup::{DataResolver, PendingIndexCache};
use super::stats::{generate_directory_stats, generate_global_stats, generate_torrent_stats};

/// maximum number of entries in the L1 range cache.  Each entry
/// caches a `(offset, size)` slice read from a torrent file.  When the
/// cache is full, inserting a new entry clears all existing entries —
/// coarse but bounded: the L1 cache is a hot-zone optimization, and
/// clearing it only causes a few L2 disk re-reads.
const MAX_L1_ENTRIES: usize = 256;

/// how long `rename` waits for a concurrently in-flight
/// `add_torrent` (spawned detached by `release`) to settle its DB
/// insert before giving up and failing the rename.
const PENDING_ADD_TIMEOUT: Duration = Duration::from_secs(5);

/// `(source row id, landed content id)` whose in-flight reads are queued for
/// the adapter's drain; `None` marks a removed row.
type CancelledRead = (i64, Option<i64>);

/// Guard against a silent 0-byte EOF: when `read_file_range` returns empty
/// data for a non-zero `size`, translate it into `Err(FsError::NoPeers)` (→
/// ENODATA). An empty result can happen when `pieces_on_disk` and the
/// engine's `file_offset` FFI disagree on `file_start_offset`; returning it
/// as EOF would make the kernel (and `dd`) see a clean end-of-file and the
/// user never learn the download failed. `read` guarantees `size > 0`
/// whenever data is expected.
fn guard_empty_read(data: &[u8], size: u32) -> Result<(), FsError> {
    if data.is_empty() && size > 0 {
        Err(FsError::NoPeers(format!(
            "Read returned 0 bytes for size {} \
             (possible file_offset mismatch)",
            size
        )))
    } else {
        Ok(())
    }
}

/// Cached listing of one `data/` directory.
///
/// The kernel reads a directory a few hundred entries at a time, so one `ls` of
/// a 14 000-file torrent issues ~25 `readdir` calls; rebuilding the listing — a
/// full SQL pass over the directory — for each of them burned ~1.2 s of
/// dispatch CPU per `ls`. The listing is keyed by the state it was derived from
/// (`data_readdir_state`), so any write or in-flight add invalidates it: there
/// is no TTL, and a directory whose contents changed is never served stale.
///
/// Only the most recently listed directory is kept — the cache exists for the
/// consecutive pages of one listing — which bounds its memory to one listing.
struct DataReaddirCache {
    ino: u64,
    state: (u64, u64),
    entries: Vec<DirEntry>,
}

/// Snapshot of the metadata-namespace inode table.
///
/// `by_parent` keeps each directory's children in inode order, the order
/// `readdir` assigns offsets in; `by_name` answers a by-name lookup for one
/// parent. Both are rebuilt only after the table changed, so a directory
/// listing and a by-name resolution cost O(page) / O(1) instead of a scan of
/// every inode (C-11.6 / F2).
struct MetadataIndex {
    generation: u64,
    inode_count: usize,
    by_parent: HashMap<u64, Vec<(u64, FileKind, String)>>,
    by_name: HashMap<u64, HashMap<String, u64>>,
}

/// Entries kept in the L3 (parsed metadata) cache. Also implementation-defined
/// (the contract only requires a bound); the cache clears on overflow, so a
/// cold entry costs one extra parse.
const MAX_L3_ENTRIES: usize = 64;

/// Most entries one `readdir` call returns.
///
/// A kernel readdir reply holds roughly a hundred entries, so a page this size
/// never forces an extra round trip.  The cap is what keeps one call's cost
/// independent of the directory size: returning the whole remainder made a
/// walk at one-entry granularity quadratic — every call filtered the listing
/// and cloned its tail.
const READDIR_PAGE_ENTRIES: usize = 128;

/// At most one page of the entries after `offset`, the last one the kernel
/// accepted.
///
/// `entries` carry strictly increasing offsets, so the resume point is found by
/// binary search and only the returned page is cloned.
fn page_after_offset(entries: &[DirEntry], offset: i64) -> Vec<DirEntry> {
    let start = entries.partition_point(|entry| entry.offset <= offset);
    let end = start
        .saturating_add(READDIR_PAGE_ENTRIES)
        .min(entries.len());
    entries[start..end].to_vec()
}

/// Sink for kernel dentry invalidation.
///
/// Production wires `fuser::Notifier` (via `TorrentFs::notifier_handle`); the
/// trait exists so tests can record which `data/` entries a daemon-side
/// removal drops, instead of the concrete type being uninjectable.
pub trait EntryInvalidator: Send + Sync {
    /// Drop the kernel's cached dentry for `name` under `parent`.
    fn inval_entry(&self, parent: u64, name: &OsStr) -> io::Result<()>;
}

/// Sink the background persist thread uses to settle in-flight reads of a
/// source row it just repointed, without waiting for another FUSE operation.
///
/// Waiters are tagged with the content id from the same authoritative read
/// that produced their bytes. Only waiters of another content are cancelled:
/// a read of the landed content carries the landed id and survives a late
/// cancel keyed on the same source row (C-4.5.3).
pub trait ReadCancelSink: Send + Sync {
    /// Cancel every waiter for `torrent_id` that is not for `content_id`;
    /// `None` cancels all of them (the source row was removed). Returns the
    /// number of waiters settled with `EIO`.
    fn cancel_reads_not_for_content(&self, torrent_id: i64, content_id: Option<i64>) -> usize;
}

/// Send one `inval_entry` and log a failure.
///
/// The persist thread invalidates a superset of what the kernel may hold, so
/// the sink is expected to treat a missing parent/entry as success
/// (`Notifier` maps `ENOENT` that way). Anything it does surface is a real
/// failure worth a warning.
fn notify_inval_entry(sink: &Arc<dyn EntryInvalidator>, parent: u64, name: &str) {
    if let Err(error) = sink.inval_entry(parent, OsStr::new(name)) {
        warn!(
            "inval_entry(parent={}, name={}) failed: {}",
            parent, name, error
        );
    }
}

pub struct FsService {
    pub inode_mgr: InodeManager,
    pub db: Option<Arc<Mutex<Database>>>,
    pub torrent_service: Option<TorrentService>,
    pub processing_torrents: Arc<Mutex<HashMap<(String, String), ()>>>,
    /// signalled whenever an entry is removed from
    /// `processing_torrents`, so a `rename` blocked on an in-flight
    /// `add_torrent` wakes immediately instead of polling.
    pub processing_torrents_cv: Arc<Condvar>,
    /// `(source_path, filename) -> error message` for `.torrent`
    /// files whose async DB persistence failed.  `lookup`/`readdir` on the
    /// `data/` mirror consult this to surface the failure as EIO instead of
    /// silently omitting the torrent (FUSE_RELEASE errors are ignored by the
    /// kernel, so the failure cannot propagate through `release` itself).
    pub persist_errors: Arc<Mutex<HashMap<(String, String), String>>>,
    pub download_service: Option<Arc<DownloadService>>,
    pub torrent_data_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub torrent_info_cache: Arc<Mutex<HashMap<String, Arc<TorrentInfo>>>>,
    pub listen_addr: String,
    pub metrics: Arc<Metrics>,
    /// kernel cache invalidation handle.  Set once after
    /// `fuser::spawn_mount2` returns (main wires `bg.notifier()` into
    /// here).  When set, `unlink`/`rmdir`/`rename` call
    /// [`EntryInvalidator::inval_entry`] to immediately purge the kernel dentry
    /// cache for removed `data/` entries, so the stale mirror vanishes
    /// at once instead of lingering for the 1s FUSE TTL.
    pub notifier: Arc<OnceLock<Option<Arc<dyn EntryInvalidator>>>>,
    /// most recent `data/` listing, so the kernel's per-page `readdir` calls do
    /// not each rebuild it. See [`DataReaddirCache`].
    data_readdir_cache: Option<DataReaddirCache>,
    /// `(source_path, filename)` keys whose background persist has landed and
    /// whose cached `data/` projection therefore has to be evicted.
    ///
    /// A background thread cannot touch the inode tables (FUSE dispatch is
    /// single-threaded), so it queues the key here and the dispatch side drains
    /// the queue before answering the next request. See
    /// [`FsService::drain_projection_evictions`].
    pending_projection_evictions: Arc<Mutex<Vec<(String, String)>>>,
    /// Snapshot of the metadata-namespace inode table: children per parent in
    /// inode order (the order readdir offsets are assigned in) plus a
    /// name->inode map. Rebuilt only when the table changed, so name
    /// resolution and directory paging never scan the whole table per request
    /// (C-11.6 / F2).
    metadata_index: Option<MetadataIndex>,
    /// Bumped by every operation that can change the metadata inode table.
    /// A cached [`MetadataIndex`] whose generation differs is rebuilt before
    /// use, so it can never outlive its snapshot.
    metadata_generation: u64,
    /// Read identity per live data-file inode (see [`FileIdentity`]).
    ///
    /// Filled by `lookup`/`readdir` from the identities the write and
    /// projection already prepared for the source, so a read of an open file
    /// never resolves identity from the database — evicting the prepared
    /// source cache cannot turn a read into a whole-table load. Entries live
    /// exactly as long as the inode they describe.
    read_identities: HashMap<u64, Arc<FileIdentity>>,
    /// Source row ids whose in-flight reads must be resolved with `EIO`
    /// because the row was removed or repointed, paired with the landed
    /// content id (`None` for a removal) that superseded them (C-4.5.3).
    /// Filled by the removal paths and by the persist thread; the adapter
    /// drains this after the operation that produced it returns, and the
    /// persist thread also settles its entry immediately through
    /// [`Self::read_cancel_sink`].
    pub(crate) cancelled_reads: Arc<Mutex<Vec<CancelledRead>>>,
    /// Authoritative content pointer per source row, shared with
    /// [`TorrentService`]: the write that moves the pointer publishes the new
    /// content id here before its identities become observable. A deferred
    /// read registers the content id from its own identity snapshot, so a
    /// read of the landed content can never be mistaken for the old one.
    pub(crate) content_versions: Arc<Mutex<HashMap<i64, ContentVersion>>>,
    /// Immediate cancellation sink for the background persist thread. Wired by
    /// the FUSE adapter once the pending table exists; a service used without
    /// an adapter (unit tests) leaves it unset and relies on the queue.
    pub read_cancel_sink: Arc<OnceLock<Arc<dyn ReadCancelSink>>>,
    /// Last persisted bytes of a metadata `.torrent` inode, shared with the
    /// persist thread instead of copied: `release` moves the buffer here as an
    /// `Arc` and the thread takes another handle. Reads/writes adopt it back
    /// (zero-copy once the thread is done) when the inode's own buffer is
    /// empty.
    buffered_torrents: HashMap<u64, Arc<Vec<u8>>>,
    /// Built pending-torrent lookups, invalidated whenever metadata content or
    /// inode structure changes (see [`PendingIndexCache`]).
    pending_index_cache: Arc<Mutex<PendingIndexCache>>,
}

// ── Kernel cache invalidation ─────────────────────────────────
// `EntryInvalidator::inval_entry` sends `FUSE_NOTIFY_INVAL_ENTRY`, purging the
// kernel's cached dentry immediately — otherwise the kernel serves a stale
// `data/` entry for the full 1s TTL after `rm metadata/foo.torrent`. The
// production sink is the `fuser::Notifier` extracted from `BackgroundSession`
// before `TorrentFs` is moved into the session and set once it is live; all
// calls are best-effort (a failure is swallowed by `Notifier::send_inval`).
impl FsService {
    /// Invalidate the kernel's cached dentry for `name` under `parent` in the
    /// `data/` subtree.  No-op when no sink is wired.
    fn inval_data_entry(&self, parent: u64, name: &str) {
        if let Some(Some(notifier)) = self.notifier.get() {
            if let Err(e) = notifier.inval_entry(parent, OsStr::new(name)) {
                warn!(
                    "inval_entry(parent={}, name={}) failed: {}",
                    parent, name, e
                );
            }
        }
    }

    /// Split a `data/` source-path directory into its `(parent_ino, name)`
    /// for `inval_entry`.  A top-level path like `"cat-a"` has parent
    /// `DATA_INO` and name `"cat-a"`.  A nested path like `"cat-a/sub"`
    /// has parent `make_source_path_dir_ino("cat-a")` and name `"sub"`.
    fn split_data_dir_parent(path: &str) -> (u64, String) {
        match path.rsplit_once('/') {
            Some((parent_path, name)) => (
                InodeManager::make_source_path_dir_ino(parent_path),
                name.to_string(),
            ),
            None => (DATA_INO, path.to_string()),
        }
    }

    /// Drop the cached `data/` inodes an overwrite superseded, promote a
    /// pending-window root to the landed row, and refresh the root's display
    /// name.
    ///
    /// Runs on the dispatch thread; the kernel is told by the persist thread
    /// instead (see `release`), because a notification from here would take the
    /// lock an in-flight lookup already holds. A row-backed root is refreshed
    /// in place: its inode is derived from the source row, which survives the
    /// overwrite. A pending-window root is promoted in place for the same
    /// reason one step earlier — the kernel may already hold its hash-derived
    /// inode, and the contract wants the temporary projection gone, not the
    /// walk broken. Pending children are dropped: the promoted root resolves
    /// children from the row, so no reader is left for the bencode entries.
    fn evict_projection(&mut self, source_path: &str, filename: &str) {
        // Cached children carry their root's source id; once the source was
        // repointed, every cached child under that id is replaced content.
        let superseded_ids: Vec<i64> = self
            .inode_mgr
            .data_inodes
            .values()
            .filter_map(|data_inode| match data_inode {
                DataInode::TorrentRoot {
                    torrent_id,
                    source_path: cached_source_path,
                    filename: cached_filename,
                    ..
                } if *torrent_id != 0
                    && cached_source_path.as_str() == source_path
                    && cached_filename.as_str() == filename =>
                {
                    Some(*torrent_id)
                }
                _ => None,
            })
            .collect();

        // The row is the post-overwrite truth. Read it before touching the
        // tables so the promotion below can carry its id and display name.
        let landed = self.db.as_ref().and_then(|db| {
            let guard = db.lock().ok()?;
            guard
                .get_torrent_by_filename_and_source_path(filename, source_path)
                .ok()
                .flatten()
        });

        self.inode_mgr
            .data_inodes
            .retain(|_, data_inode| match data_inode {
                DataInode::TorrentRoot { .. } | DataInode::SourcePathDir { .. } => true,
                DataInode::TorrentDir {
                    torrent_id: 0,
                    torrent_source_path,
                    torrent_filename,
                    ..
                }
                | DataInode::TorrentFile {
                    torrent_id: 0,
                    torrent_source_path,
                    torrent_filename,
                    ..
                } => {
                    torrent_source_path.as_str() != source_path
                        || torrent_filename.as_str() != filename
                }
                DataInode::TorrentDir { torrent_id, .. }
                | DataInode::TorrentFile { torrent_id, .. } => !superseded_ids.contains(torrent_id),
            });
        self.retain_live_read_identities();

        let Some(torrent) = landed else {
            return;
        };
        let pending_ino = InodeManager::make_pending_torrent_ino(source_path, filename);
        for (ino, data_inode) in self.inode_mgr.data_inodes.iter_mut() {
            if let DataInode::TorrentRoot {
                torrent_id, name, ..
            } = data_inode
            {
                if *torrent_id == torrent.id || (*torrent_id == 0 && *ino == pending_ino) {
                    *torrent_id = torrent.id;
                    name.clone_from(&torrent.name);
                }
            }
        }
    }

    /// Apply every projection eviction a background persist queued.
    ///
    /// Runs on the dispatch thread before a request is answered: superseded
    /// cached inodes are dropped, so the request resolves against the row the
    /// persist wrote. No kernel notification is issued here — the persist
    /// thread owns that (see `release`), because a notification from this
    /// context would take the lock the in-flight lookup already holds.
    fn drain_projection_evictions(&mut self) {
        let queued = match self.pending_projection_evictions.lock() {
            Ok(mut guard) => std::mem::take(&mut *guard),
            Err(e) => {
                error!("Mutex poisoned in drain_projection_evictions(): {}", e);
                return;
            }
        };
        for (source_path, filename) in queued {
            self.evict_projection(&source_path, &filename);
        }
    }

    /// Mark the cached metadata index stale. Called by every operation that
    /// can add, remove, rename, or retire a metadata-namespace inode.
    fn touch_metadata_index(&mut self) {
        self.metadata_generation = self.metadata_generation.wrapping_add(1);
        self.invalidate_pending_index();
    }

    /// Drop every built pending-torrent index. A pending `.torrent` can be
    /// rewritten while its persist is in flight, so any structural or content
    /// change must not leave a lookup serving the old tree.
    fn invalidate_pending_index(&mut self) {
        if let Ok(mut cache) = self.pending_index_cache.lock() {
            cache.invalidate();
        }
    }

    /// Authoritative content state of one source row, if this process has
    /// published one. `None` means the row predates this mount; a read of it
    /// is live because no replacement could have been observed.
    pub(crate) fn content_version(&self, torrent_id: i64) -> Option<ContentVersion> {
        self.content_versions
            .lock()
            .ok()
            .and_then(|versions| versions.get(&torrent_id).copied())
    }

    /// Read identity captured for a live data-file inode.
    pub(crate) fn read_identity(&self, ino: u64) -> Option<Arc<FileIdentity>> {
        self.read_identities.get(&ino).cloned()
    }

    /// Bind the identity prepared for `ino`'s file to the inode.
    ///
    /// Runs on the projection path (lookup/readdir), never on `read`. A miss
    /// means the inode belongs to content the source has already left behind;
    /// the read then fails with `EIO` rather than materializing the new
    /// content's file table under an old inode.
    fn cache_read_identity(&mut self, ino: u64) {
        if self.read_identities.contains_key(&ino) {
            return;
        }
        let (source_id, file_id) = match self.inode_mgr.data_inodes.get(&ino) {
            Some(DataInode::TorrentFile {
                torrent_id,
                file_id,
                ..
            }) if *torrent_id != 0 => (*torrent_id, *file_id),
            _ => return,
        };
        let Some(ts) = &self.torrent_service else {
            return;
        };
        if let Some(identity) = ts.prepared_file_identity(source_id, file_id) {
            self.read_identities.insert(ino, identity);
        }
    }

    /// Drop identities whose inode no longer exists. Called after the
    /// projection removes data inodes so the map tracks inode lifetime.
    fn retain_live_read_identities(&mut self) {
        let live = &self.inode_mgr.data_inodes;
        self.read_identities.retain(|ino, _| live.contains_key(ino));
    }

    /// Publish the content pointer a write just landed and settle every
    /// in-flight read of another content id immediately (C-4.5.3).
    ///
    /// The queue lets the adapter drain after the operation that produced it;
    /// the sink settles now so the background persist thread does not wait
    /// for another FUSE request.
    #[cfg(test)]
    pub(crate) fn record_content_published(&self, torrent_id: i64, content_id: i64) -> usize {
        Self::record_content_published_shared(
            &self.content_versions,
            &self.read_cancel_sink,
            &self.cancelled_reads,
            &self.metrics,
            torrent_id,
            Some(content_id),
        )
    }

    /// Mark a source row as removed and settle every in-flight read of it.
    pub(crate) fn record_source_removed(&self, torrent_id: i64) -> usize {
        Self::record_content_published_shared(
            &self.content_versions,
            &self.read_cancel_sink,
            &self.cancelled_reads,
            &self.metrics,
            torrent_id,
            None,
        )
    }

    /// Body of the two methods above, callable from the background persist
    /// thread, which cannot borrow the service.
    fn record_content_published_shared(
        versions: &Arc<Mutex<HashMap<i64, ContentVersion>>>,
        cancel_sink: &Arc<OnceLock<Arc<dyn ReadCancelSink>>>,
        cancelled: &Arc<Mutex<Vec<CancelledRead>>>,
        metrics: &Arc<Metrics>,
        torrent_id: i64,
        content_id: Option<i64>,
    ) -> usize {
        let version = match content_id {
            Some(id) => ContentVersion::Current(id),
            None => ContentVersion::Removed,
        };
        if let Ok(mut versions) = versions.lock() {
            versions.insert(torrent_id, version);
        }
        let settled = if let Some(sink) = cancel_sink.get() {
            let settled = sink.cancel_reads_not_for_content(torrent_id, content_id);
            metrics.pending_reads_dec_n(settled);
            settled
        } else {
            0
        };
        if let Ok(mut queue) = cancelled.lock() {
            if !queue.contains(&(torrent_id, content_id)) {
                queue.push((torrent_id, content_id));
            }
        }
        settled
    }

    /// Move the buffered bytes of one metadata `.torrent` inode out of the
    /// inode, returning `(name, parent, data)`.
    ///
    /// `None` when the inode is absent or not a regular file. The caller owns
    /// the buffer: either the persist thread takes it, or it is parked in
    /// [`Self::buffered_torrents`] as a shared handle so reads still work.
    fn take_buffered_file(&mut self, ino: u64) -> Option<(String, u64, Vec<u8>)> {
        let (name, parent, data) = match self.inode_mgr.inodes.get_mut(&ino) {
            Some(InodeData::File {
                name, parent, data, ..
            }) => (name.clone(), *parent, std::mem::take(data)),
            _ => return None,
        };
        if !data.is_empty() {
            return Some((name, parent, data));
        }
        // The inode's own buffer is empty because `release` already moved it
        // into `buffered_torrents`. A read-only open/close must hand back the
        // same bytes, not an empty buffer — otherwise the close would read as
        // a zero-byte overwrite and delete the row.
        let data = match self.buffered_torrents.remove(&ino) {
            Some(shared) => match Arc::try_unwrap(shared) {
                Ok(bytes) => bytes,
                Err(shared) => shared.as_slice().to_vec(),
            },
            None => Vec::new(),
        };
        Some((name, parent, data))
    }

    /// Put a buffer `take_buffered_file` moved out back when a recoverable
    /// step fails, so the close can be retried without losing the bytes.
    /// Prefers the inode's own slot (what `write`/`flush` expect) and parks a
    /// shared handle if the inode is already gone or occupied.
    fn restore_buffered_file(&mut self, ino: u64, data: Vec<u8>) {
        if let Some(InodeData::File { data: slot, .. }) = self.inode_mgr.inodes.get_mut(&ino) {
            if slot.is_empty() {
                *slot = data;
                return;
            }
        }
        self.buffered_torrents.insert(ino, Arc::new(data));
    }

    /// Bytes buffered for one metadata `.torrent` inode.
    ///
    /// The inode's own buffer is authoritative while it is non-empty; after
    /// `release` moved the bytes into [`Self::buffered_torrents`] the shared
    /// handle serves reads until a write adopts it back.
    fn buffered_torrent_bytes(&self, ino: u64) -> Option<&[u8]> {
        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File { data, .. }) if !data.is_empty() => Some(data.as_slice()),
            Some(InodeData::File { .. }) => Some(
                self.buffered_torrents
                    .get(&ino)
                    .map_or(&[], |bytes| bytes.as_slice()),
            ),
            _ => None,
        }
    }

    /// Move a shared buffer parked by `release` back into the inode before a
    /// write mutates it. The move is zero-copy once the persist thread dropped
    /// its handle; only a write that races the in-flight persist copies.
    fn adopt_buffered_torrent(&mut self, ino: u64) {
        let Some(shared) = self.buffered_torrents.remove(&ino) else {
            return;
        };
        let Some(InodeData::File { data, .. }) = self.inode_mgr.inodes.get_mut(&ino) else {
            return;
        };
        if !data.is_empty() {
            return;
        }
        match Arc::try_unwrap(shared) {
            Ok(bytes) => *data = bytes,
            Err(shared) => *data = shared.as_slice().to_vec(),
        }
    }

    /// The metadata-namespace child index, rebuilt when the inode table
    /// changed since the snapshot was taken.
    fn metadata_index(&mut self) -> &MetadataIndex {
        let stale = match &self.metadata_index {
            Some(index) => {
                index.generation != self.metadata_generation
                    || index.inode_count != self.inode_mgr.inodes.len()
            }
            None => true,
        };
        if stale {
            let mut by_parent: HashMap<u64, Vec<(u64, FileKind, String)>> = HashMap::new();
            let mut by_name: HashMap<u64, HashMap<String, u64>> = HashMap::new();
            for (ino, data) in &self.inode_mgr.inodes {
                let (parent, name, kind) = match data {
                    InodeData::Directory { parent, name } if !name.is_empty() => {
                        (*parent, name, FileKind::Directory)
                    }
                    InodeData::File {
                        parent,
                        name,
                        unlinked,
                        ..
                    } if !*unlinked => (*parent, name, FileKind::RegularFile),
                    // Root ("") and unlinked-but-open files are invisible.
                    _ => continue,
                };
                by_parent
                    .entry(parent)
                    .or_default()
                    .push((*ino, kind, name.clone()));
                by_name
                    .entry(parent)
                    .or_default()
                    .insert(name.clone(), *ino);
            }
            // Entry order fixes the paging offsets, so it must not depend on
            // the map's iteration order.
            for children in by_parent.values_mut() {
                children.sort_unstable_by_key(|(ino, ..)| *ino);
            }
            self.metadata_index = Some(MetadataIndex {
                generation: self.metadata_generation,
                inode_count: self.inode_mgr.inodes.len(),
                by_parent,
                by_name,
            });
        }
        self.metadata_index
            .as_ref()
            .expect("metadata index was just built")
    }

    /// Resolve one child of a metadata directory by name.
    fn metadata_child(&mut self, parent: u64, name: &str) -> Option<u64> {
        self.metadata_index()
            .by_name
            .get(&parent)?
            .get(name)
            .copied()
    }
}

impl FsService {
    // ── Construction ────────────────────────────────────────────────────────

    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }
        let metrics = Arc::new(Metrics::new());
        let download_service = match DownloadService::new_with_metrics(
            cache_path.as_path(),
            config,
            metrics.clone(),
        ) {
            Ok(ds) => Some(Arc::new(ds)),
            Err(e) => {
                error!(
                    "Failed to initialize download engine (piece cache {:?}): {:?}. \
                         File content reads and seeding will be unavailable. \
                         Ensure the cache directory is writable by the current user — \
                         a state directory left over from a previous container run \
                         under a different user is the usual cause.",
                    cache_path, e
                );
                None
            }
        };

        let creation_time = Duration::from_secs(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        let listen_addr = config
            .connections
            .listen_interfaces
            .clone()
            .unwrap_or_else(|| "0.0.0.0:6881".to_string());
        Self {
            inode_mgr: InodeManager::new(creation_time),
            db: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            download_service,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr,
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions: Arc::new(Mutex::new(HashMap::new())),
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_cache_path(
            PathBuf::from("/tmp/torrentfs-cache"),
            &TorrentfsConfig::default_config(),
        )
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &TorrentfsConfig,
    ) -> Self {
        let mut svc = Self::new_with_cache_path(cache_path, config);

        // Collect data from database before moving it.
        let (dirs, torrents, torrent_datas) = {
            let dirs = db.get_all_metadata_directories().unwrap_or_default();
            let torrents = db.get_all_torrents().unwrap_or_default();
            let torrent_datas: Vec<Vec<u8>> = torrents
                .iter()
                .filter_map(|t| t.torrent_data.clone())
                .collect();
            (dirs, torrents, torrent_datas)
        };

        let db_arc = Arc::new(Mutex::new(db));
        let torrent_service = TorrentService::new(db_arc.clone(), svc.download_service.clone());
        // Reads validate against the same publish map the write path updates.
        svc.content_versions = torrent_service.content_versions();
        svc.torrent_service = Some(torrent_service);
        svc.db = Some(db_arc);
        svc.inode_mgr.restore_metadata_inodes(dirs, torrents);

        // Recreate lightweight libtorrent handles for all persisted torrents so
        // peer/seed information and `.stats` piece status are visible without a
        // read.
        let mut infos_by_hash: HashMap<String, Arc<TorrentInfo>> = HashMap::new();
        if let Some(ds) = &svc.download_service {
            for data in torrent_datas {
                match TorrentInfo::from_bytes(data) {
                    Ok(info) => {
                        let name = info.name();
                        let info = Arc::new(info);
                        if let Ok(ih) = info.info_hash() {
                            infos_by_hash.insert(hex::encode(ih), info.clone());
                        }
                        if let Err(e) = ds.ensure_handle_lightweight(info) {
                            warn!(
                                "Failed to restore lightweight handle for torrent '{}': {:?}",
                                name, e
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse stored torrent data on startup: {:?}", e);
                    }
                }
            }
        }

        // background SHA-1 verification of on-disk cached pieces that
        // the startup scan re-registered as unverified. Runs asynchronously so
        // FUSE mount readiness is never blocked.
        if !infos_by_hash.is_empty() {
            if let Some(cache) = svc.get_cache_manager() {
                spawn_cache_verification(cache, infos_by_hash);
            }
        }

        svc
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.download_service
            .as_ref()
            .and_then(|ds| ds.get_cache_manager())
    }

    // ── Namespace-agnostic queries ──────────────────────────────────────────

    /// map a `data/` torrent-root lookup `(parent, name)` to its
    /// `(source_path, filename)` persist-error key, or `None` when `parent`
    /// is not a torrent-root parent (DATA_INO or a SourcePathDir).
    fn torrent_root_lookup_key(&self, parent: u64, name: &str) -> Option<(String, String)> {
        let source_path = if parent == DATA_INO {
            String::new()
        } else {
            match self.inode_mgr.data_inodes.get(&parent) {
                Some(DataInode::SourcePathDir { path }) => path.clone(),
                _ => return None,
            }
        };
        Some((source_path, name.to_string()))
    }

    /// the `source_path` a `data/` directory inode lists, or `None`
    /// when `ino` is not a directory whose children are torrent roots (i.e.
    /// DATA_INO or a SourcePathDir).
    fn data_dir_source_path(&self, ino: u64) -> Option<String> {
        if ino == DATA_INO {
            Some(String::new())
        } else {
            match self.inode_mgr.data_inodes.get(&ino) {
                Some(DataInode::SourcePathDir { path }) => Some(path.clone()),
                _ => None,
            }
        }
    }

    pub fn lookup(&mut self, parent: u64, name: &str) -> FsResult<Option<Entry>> {
        self.drain_projection_evictions();
        if parent == ROOT_INO {
            return Ok(match name {
                "metadata" => Some(Entry {
                    ino: METADATA_INO,
                    attr: self.inode_mgr.attr_for_dir(METADATA_INO, true),
                }),
                "data" => Some(Entry {
                    ino: DATA_INO,
                    attr: self.inode_mgr.attr_for_dir(DATA_INO, false),
                }),
                ".stats" => {
                    let stats_size = self.read_stats(StatsKind::Global).len() as u64;
                    Some(Entry {
                        ino: STATS_INO,
                        attr: self.inode_mgr.attr_for_file(STATS_INO, stats_size),
                    })
                }
                _ => None,
            });
        }

        // Handle .stats virtual file for data/ subtree directories.
        if name == ".stats" {
            if parent == DATA_INO {
                let stats_ino = InodeManager::make_stats_ino(parent);
                let content = self.generate_dir_stats_content("");
                return Ok(Some(Entry {
                    ino: stats_ino,
                    attr: self
                        .inode_mgr
                        .attr_for_file(stats_ino, content.len() as u64),
                }));
            }

            if let Some(data_inode) = self.inode_mgr.data_inodes.get(&parent) {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_dir_stats_content(path);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    DataInode::TorrentRoot { torrent_id, .. } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_torrent_stats_for_id(*torrent_id);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    _ => return Ok(None),
                }
            }
            // Parent not yet cached in data_inodes — fall through to the normal
            // data/ lookup below.
        }

        if parent == DATA_INO || InodeManager::is_data_ino(parent) {
            // surface a previously-failed async persist as EIO
            // (FUSE_RELEASE errors are dropped by the kernel, so `release`
            // cannot propagate the DB failure itself).
            if let Some(key) = self.torrent_root_lookup_key(parent, name) {
                if let Some(msg) = self
                    .persist_errors
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&key).cloned())
                {
                    return Err(FsError::Database(msg));
                }
            }
            if let Some(db) = &self.db {
                // Warm the cached `(info_hash, file_index)` identities for the
                // torrent this lookup descends into, outside the read path: a
                // later `read` must not resolve them from the database.
                if let Some(DataInode::TorrentRoot { torrent_id, .. })
                | Some(DataInode::TorrentDir { torrent_id, .. }) =
                    self.inode_mgr.data_inodes.get(&parent)
                {
                    if *torrent_id != 0 {
                        self.ensure_file_identities(*torrent_id);
                    }
                }
                if let Some((ino, kind, size)) = DataResolver::lookup_data_inode_with_buffers(
                    &mut self.inode_mgr,
                    db,
                    &self.processing_torrents,
                    &self.pending_index_cache,
                    &self.buffered_torrents,
                    parent,
                    name,
                ) {
                    if ino == INVALID_INO {
                        // The row id cannot be encoded (see
                        // `InodeManager::make_torrent_root_ino`). Never hand
                        // the kernel a node id that aliases another inode.
                        self.inode_mgr.data_inodes.remove(&ino);
                        return Err(FsError::Internal(format!(
                            "torrent '{}' has a database row id outside the addressable inode range",
                            name
                        )));
                    }
                    // Bind the identity to the inode here, on the projection
                    // path, so `read` never resolves it from the database.
                    self.cache_read_identity(ino);
                    let attr = match kind {
                        FileKind::Directory => self.inode_mgr.attr_for_dir(ino, false),
                        FileKind::RegularFile => self.inode_mgr.attr_for_file(ino, size),
                    };
                    return Ok(Some(Entry { ino, attr }));
                }
            }
            return Ok(None);
        }

        if let Some(child_ino) = self.metadata_child(parent, name) {
            if let Some(data) = self.inode_mgr.inodes.get(&child_ino) {
                let attr = match data {
                    InodeData::Directory { .. } => self.inode_mgr.attr_for_dir(child_ino, true),
                    InodeData::File { .. } => {
                        // The inode's own buffer is empty after `release`
                        // moved it into `buffered_torrents`; attributes must
                        // report the authoritative bytes, not zero.
                        let size = self
                            .buffered_torrent_bytes(child_ino)
                            .map_or(0, |bytes| bytes.len() as u64);
                        self.inode_mgr.attr_for_file(child_ino, size)
                    }
                };
                return Ok(Some(Entry {
                    ino: child_ino,
                    attr,
                }));
            }
        }

        Ok(None)
    }

    pub fn getattr(&mut self, ino: u64) -> FsResult<Attr> {
        self.drain_projection_evictions();
        match ino {
            ROOT_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            METADATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, true)),
            DATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            STATS_INO => {
                let stats_size = self.generate_global_stats_content().len() as u64;
                Ok(self.inode_mgr.attr_for_file(ino, stats_size))
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let content = self.generate_data_stats_for_ino(ino);
                    return Ok(self.inode_mgr.attr_for_file(ino, content.len() as u64));
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(data_inode) = self.inode_mgr.data_inodes.get(&ino) {
                        return Ok(match data_inode {
                            DataInode::SourcePathDir { .. }
                            | DataInode::TorrentRoot { .. }
                            | DataInode::TorrentDir { .. } => {
                                self.inode_mgr.attr_for_dir(ino, false)
                            }
                            DataInode::TorrentFile { size, .. } => {
                                self.inode_mgr.attr_for_file(ino, *size as u64)
                            }
                        });
                    }

                    let torrent_id = (ino - DATA_TORRENT_INO_BASE) as i64;
                    if (DATA_TORRENT_INO_BASE..DATA_DIR_INO_BASE).contains(&ino) {
                        if self
                            .torrent_service
                            .as_ref()
                            .map(|ts| ts.torrent_exists_by_id(torrent_id))
                            .unwrap_or(false)
                        {
                            return Ok(self.inode_mgr.attr_for_dir(ino, false));
                        } else {
                            return Err(FsError::NotFound);
                        }
                    }

                    return Err(FsError::NotFound);
                }

                if let Some(data) = &self.inode_mgr.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => Ok(self
                            .inode_mgr
                            .attr_for_dir(ino, self.inode_mgr.is_metadata_child(ino))),
                        InodeData::File { .. } => {
                            let size = self
                                .buffered_torrent_bytes(ino)
                                .map_or(0, |bytes| bytes.len() as u64);
                            Ok(self.inode_mgr.attr_for_file(ino, size))
                        }
                    }
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    /// `setattr` on a virtual/immutable namespace must not silently succeed
    /// for an attribute change (chmod/chown/utimens) — those return `EPERM`.
    /// A *truncate* (size change) is different: `cp` overwriting a `.torrent`
    /// opens with `O_TRUNC`, so the kernel sends `open` → `SETATTR(size=0)`;
    /// rejecting it broke the overwrite path (new files worked, overwrites
    /// failed). A pure truncate on `metadata/` therefore resizes the in-memory
    /// buffer and returns updated attributes; `data/` stays `EROFS`, `.stats`
    /// and root stay `EPERM`. `size` is `Some(n)` only for a pure truncate.
    pub fn setattr(&mut self, ino: u64, size: Option<u64>) -> FsResult<Attr> {
        // `.stats` files belong to the stats namespace, not to the read-only
        // `data/` projection, and must be classified first: a write/attribute
        // change returns EPERM, never EROFS. (`is_data_ino` excludes the stats
        // window; the ordering keeps the classification explicit.)
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            return Err(FsError::NotPermitted);
        }
        if InodeManager::is_data_namespace(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if self.inode_mgr.is_metadata_child(ino) || ino == ROOT_INO {
            if let Some(target) = size {
                // A truncate on a `metadata/` `.torrent` buffer (the
                // `O_TRUNC` overwrite path). Only a `File` inode carries
                // resizable data; a directory has no size to set.
                self.invalidate_pending_index();
                self.adopt_buffered_torrent(ino);
                return match self.inode_mgr.inodes.get_mut(&ino) {
                    Some(InodeData::File { data, name, .. }) => {
                        if target > MAX_TORRENT_SIZE as u64 {
                            return Err(FsError::FileTooLarge(format!(
                                "truncate {} to {} bytes exceeds limit {}",
                                name, target, MAX_TORRENT_SIZE
                            )));
                        }
                        let target = target as usize;
                        if target > data.len() {
                            data.resize(target, 0);
                        } else {
                            data.truncate(target);
                        }
                        Ok(self.inode_mgr.attr_for_file(ino, target as u64))
                    }
                    _ => Err(FsError::NotPermitted),
                };
            }
            return Err(FsError::NotPermitted);
        }
        self.getattr(ino)
    }

    /// `symlink` is never supported: torrentfs exposes only directories and
    /// regular files.  The read-only `data/` namespace must return `EROFS`
    /// (`ReadOnlyFileSystem`) to match `write`/`chmod`/`rmdir`/`unlink`/`rename`;
    /// every other namespace returns `EPERM` (`NotPermitted`) because the
    /// operation is unsupported, not a permissions question.
    pub fn symlink(&mut self, parent: u64) -> FsError {
        if InodeManager::is_data_namespace(parent) {
            FsError::ReadOnlyFileSystem
        } else {
            FsError::NotPermitted
        }
    }

    pub fn readdir(&mut self, ino: u64, offset: i64) -> FsResult<Vec<DirEntry>> {
        self.drain_projection_evictions();
        if ino == DATA_INO || InodeManager::is_data_ino(ino) {
            // surface failed persists in this directory as EIO.
            if let Some(sp) = self.data_dir_source_path(ino) {
                let failed = self
                    .persist_errors
                    .lock()
                    .ok()
                    .map(|g| g.keys().any(|(p, _)| p == &sp))
                    .unwrap_or(false);
                if failed {
                    let label = if sp.is_empty() { "data/" } else { &sp };
                    return Err(FsError::Database(format!(
                        "a torrent under '{}' failed to persist; see daemon log",
                        label
                    )));
                }
            }
            if let Some(db) = &self.db {
                let state = DataResolver::current_state(db, &self.processing_torrents);
                if let Some(cache) = &self.data_readdir_cache {
                    if cache.ino == ino && cache.state == state {
                        return Ok(page_after_offset(&cache.entries, offset));
                    }
                }
                if let Some(DataInode::TorrentRoot { torrent_id, .. })
                | Some(DataInode::TorrentDir { torrent_id, .. }) =
                    self.inode_mgr.data_inodes.get(&ino)
                {
                    if *torrent_id != 0 {
                        self.ensure_file_identities(*torrent_id);
                    }
                }
                // The listing carries the state it was built from, so the cache
                // key can never describe a different snapshot than the entries.
                if let Some(listing) = DataResolver::readdir_data_with_buffers(
                    &mut self.inode_mgr,
                    db,
                    &self.processing_torrents,
                    &self.pending_index_cache,
                    &self.buffered_torrents,
                    ino,
                ) {
                    for (entry_ino, _, kind, _) in &listing.entries {
                        if *kind == FileKind::RegularFile {
                            self.cache_read_identity(*entry_ino);
                        }
                    }
                    let entries: Vec<DirEntry> = listing
                        .entries
                        .into_iter()
                        .map(|(entry_ino, entry_offset, kind, name)| DirEntry {
                            ino: entry_ino,
                            offset: entry_offset,
                            kind,
                            name,
                        })
                        .collect();
                    if entries.iter().any(|entry| entry.ino == INVALID_INO) {
                        // An unaddressable row id must fail loudly instead of
                        // listing an entry that can never be opened.
                        self.inode_mgr.data_inodes.remove(&INVALID_INO);
                        return Err(FsError::Internal(
                            "a torrent in this directory has a database row id outside \
                             the addressable inode range"
                                .to_string(),
                        ));
                    }
                    let page = page_after_offset(&entries, offset);
                    self.data_readdir_cache = Some(DataReaddirCache {
                        ino,
                        state: listing.state,
                        entries,
                    });
                    return Ok(page);
                }
            }
            return Err(FsError::NotFound);
        }

        let mut entries: Vec<DirEntry> = Vec::new();

        if ino == ROOT_INO {
            entries.push(DirEntry {
                ino,
                offset: 1,
                kind: FileKind::Directory,
                name: ".".to_string(),
            });
            entries.push(DirEntry {
                ino: ROOT_INO,
                offset: 2,
                kind: FileKind::Directory,
                name: "..".to_string(),
            });
            entries.push(DirEntry {
                ino: METADATA_INO,
                offset: 3,
                kind: FileKind::Directory,
                name: "metadata".to_string(),
            });
            entries.push(DirEntry {
                ino: DATA_INO,
                offset: 4,
                kind: FileKind::Directory,
                name: "data".to_string(),
            });
            entries.push(DirEntry {
                ino: STATS_INO,
                offset: 5,
                kind: FileKind::RegularFile,
                name: ".stats".to_string(),
            });
        } else if let Some(InodeData::Directory { parent, .. }) = self.inode_mgr.inodes.get(&ino) {
            let parent = *parent;
            // Page straight out of the index: child `i` keeps offset `3 + i`,
            // so a page costs O(page) regardless of the directory size. The
            // order is inode order and therefore stable between calls.
            let first = (offset + 1).max(1);
            let last = first.saturating_add(READDIR_PAGE_ENTRIES as i64);
            let index = self.metadata_index();
            let children = index.by_parent.get(&ino).map(Vec::as_slice).unwrap_or(&[]);
            for entry_offset in first..last {
                let entry = match entry_offset {
                    1 => DirEntry {
                        ino,
                        offset: 1,
                        kind: FileKind::Directory,
                        name: ".".to_string(),
                    },
                    2 => DirEntry {
                        ino: parent,
                        offset: 2,
                        kind: FileKind::Directory,
                        name: "..".to_string(),
                    },
                    child_offset => {
                        let Some((child_ino, kind, name)) =
                            children.get((child_offset - 3) as usize)
                        else {
                            break;
                        };
                        DirEntry {
                            ino: *child_ino,
                            offset: child_offset,
                            kind: *kind,
                            name: name.clone(),
                        }
                    }
                };
                entries.push(entry);
            }
            return Ok(entries);
        } else {
            // C-2.2.6: a directory listing aimed at a file (or an unknown
            // inode, C-2.5.8) is reported as `ENOENT`, never `ENOTDIR`.
            return Err(FsError::NotFound);
        }

        Ok(page_after_offset(&entries, offset))
    }

    /// Open a file, returning the file handle and open-mode hints.
    ///
    /// Data torrent files set `direct_io: true` so the adapter applies
    /// `FOPEN_DIRECT_IO`, bypassing the kernel page cache — this lets the
    /// daemon's errno (e.g. ENODATA for "no seeder") reach userspace instead
    /// of being converted to EIO by `filemap_read_folio`.
    pub fn open(&mut self, ino: u64) -> FsResult<OpenOutcome> {
        self.drain_projection_evictions();
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(0.into()),
            STATS_INO => {
                let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                self.inode_mgr.open_files.insert(fh, ino);
                // C-6.1.4: every read re-renders the file, so its length must
                // come from that read. Serving it through the kernel page
                // cache would freeze a stale rendering (and length).
                Ok(OpenOutcome {
                    fh,
                    direct_io: true,
                })
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    return Ok(OpenOutcome {
                        fh,
                        direct_io: true,
                    });
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) =
                        self.inode_mgr.data_inodes.get(&ino)
                    {
                        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                        self.inode_mgr.open_files.insert(fh, ino);
                        // Bypass page cache so read errors propagate directly.
                        Ok(OpenOutcome {
                            fh,
                            direct_io: true,
                        })
                    } else {
                        Ok(0.into())
                    }
                } else if self.inode_mgr.inodes.contains_key(&ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    Ok(fh.into())
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }
    pub fn opendir(&mut self, ino: u64) -> FsResult<()> {
        self.drain_projection_evictions();
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(()),
            _ => {
                if InodeManager::is_data_ino(ino) {
                    // Only directories may be opened as a directory handle;
                    // a torrent file inode is reported as absent (C-2.2.6).
                    match self.inode_mgr.data_inodes.get(&ino) {
                        Some(DataInode::TorrentFile { .. }) | None => Err(FsError::NotFound),
                        Some(_) => Ok(()),
                    }
                } else {
                    match self.inode_mgr.inodes.get(&ino) {
                        Some(InodeData::Directory { .. }) => Ok(()),
                        Some(InodeData::File { .. }) | None => Err(FsError::NotFound),
                    }
                }
            }
        }
    }

    // ── Metadata namespace (.torrent lifecycle) ─────────────────────────────

    pub fn mknod(&mut self, parent: u64, name: &str) -> FsResult<Entry> {
        self.touch_metadata_index();
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::InvalidArgument);
        }

        if self.metadata_child(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
                unlinked: false,
            },
        );

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Entry {
            ino: new_ino,
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
        })
    }

    pub fn create(&mut self, parent: u64, name: &str) -> FsResult<Created> {
        self.touch_metadata_index();
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::InvalidArgument);
        }

        if self.metadata_child(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
                unlinked: false,
            },
        );

        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.open_files.insert(fh, new_ino);

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Created {
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
            fh,
        })
    }

    pub fn write(&mut self, ino: u64, offset: i64, data: &[u8]) -> FsResult<u32> {
        // `.stats` files have virtual, immutable attributes — not a read-only
        // namespace — so a write is rejected with `EPERM` (`NotPermitted`),
        // matching `setattr` and `symlink`. Derived stats inodes belong to the
        // stats namespace, not to the read-only `data/` projection, so they
        // are classified before the data/ guard below: a write returns EPERM,
        // never EROFS.
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            return Err(FsError::NotPermitted);
        }

        // data/ namespace is read-only. Data inodes live in
        // `data_inodes`, not `inodes`, so without this guard `write` would
        // fall through to the `None` arm and return `ENOENT` — the inode
        // exists, it is just not writable. Return `EROFS` instead.
        if InodeManager::is_data_namespace(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // A re-opened `.torrent` whose previous close handed its bytes to the
        // persist thread starts from that shared buffer, moved back if the
        // thread is done (zero-copy), so a partial rewrite keeps the old bytes.
        self.invalidate_pending_index();
        self.adopt_buffered_torrent(ino);

        match self.inode_mgr.inodes.get_mut(&ino) {
            Some(InodeData::File {
                data: file_data,
                name,
                ..
            }) => {
                // FUSE hands us `offset: i64`. A negative offset
                // casts to a huge `usize` and a super-large offset makes
                // `Vec::resize` attempt a multi-GB allocation (OOM/abort on
                // the FUSE dispatch thread), while `offset + data.len()` can
                // overflow and the subsequent slice `panic`. Validate before
                // any allocation: reject negatives, cap the post-write size
                // at MAX_TORRENT_SIZE (the only writable inodes are .torrent
                // buffers), and use checked arithmetic for the end offset.
                if offset < 0 {
                    warn!("write to {} with negative offset {}", name, offset);
                    return Err(FsError::InvalidArgument);
                }
                let offset = offset as usize;

                let end = match offset.checked_add(data.len()) {
                    Some(e) => e,
                    None => {
                        return Err(FsError::FileTooLarge(format!(
                            "{}: write at offset {} + {} bytes overflows usize",
                            name,
                            offset,
                            data.len()
                        )));
                    }
                };

                if end > MAX_TORRENT_SIZE {
                    warn!(
                        "write to {} would grow file to {} bytes (limit {})",
                        name, end, MAX_TORRENT_SIZE
                    );
                    return Err(FsError::FileTooLarge(format!(
                        "{} exceeds {} bytes",
                        name, MAX_TORRENT_SIZE
                    )));
                }

                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }
                if end > file_data.len() {
                    file_data.resize(end, 0);
                }

                file_data[offset..end].copy_from_slice(data);

                info!("Wrote {} bytes to file {}", data.len(), name);
                Ok(data.len() as u32)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    /// Validate a buffered `.torrent` on close (flush).
    pub fn flush(&mut self, ino: u64) -> FsResult<()> {
        let name = match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File { name, .. }) if name.ends_with(".torrent") => name.clone(),
            _ => return Ok(()),
        };
        // Validate the buffer in place: the close path must not copy the whole
        // (up to 10 MiB) buffer just to parse it and throw the parse away.
        let data = self.buffered_torrent_bytes(ino).unwrap_or(&[]);

        // Every close of a zero-byte `.torrent` fails with EINVAL:
        // FUSE can't distinguish `cp` of an empty seed from `touch`,
        // `>` redirection, or a truncate-to-zero then close. The
        // rejection lands in `flush`/`close`, so only callers that
        // check `close(2)` (e.g. `cp`) can witness the errno; a tool
        // that ignores it (`touch`, `>`, `dd`) reporting rc=0 is its
        // own semantics, not an acceptance criterion. `release` still
        // discards the empty inode, so no ghost `data/` mirror survives.
        if data.is_empty() {
            warn!("Zero-byte torrent file {} rejected", name);
            return Err(FsError::InvalidArgument);
        }

        if data.len() > MAX_TORRENT_SIZE {
            warn!(
                "Torrent file {} exceeds size limit ({} bytes)",
                name,
                data.len()
            );
            return Err(FsError::FileTooLarge(format!(
                "{} exceeds {} bytes",
                name, MAX_TORRENT_SIZE
            )));
        }

        match TorrentInfo::validate_slice(data) {
            Ok(()) => {
                info!("Torrent {} validated successfully", name);
            }
            Err(e) => {
                warn!("Invalid .torrent file {}: {}", name, e.reason());
                return Err(FsError::CorruptTorrent(e.reason().to_string()));
            }
        }
        Ok(())
    }

    /// Persist a closed `.torrent` into the database (release).
    pub fn release(&mut self, fh: u64) -> FsResult<()> {
        self.release_with_pending_timeout(fh, PENDING_ADD_TIMEOUT)
    }

    /// [`Self::release`] with an injectable pending-add deadline, so the
    /// "previous persist still in flight" path is testable in milliseconds.
    pub(crate) fn release_with_pending_timeout(
        &mut self,
        fh: u64,
        pending_timeout: Duration,
    ) -> FsResult<()> {
        self.touch_metadata_index();
        if let Some(ino) = self.inode_mgr.open_files.remove(&fh) {
            // the file handle is now closed. If the inode was
            // unlinked while still open, this `release` is the "last close"
            // point at which the inode is finally destroyed (unless another
            // handle is still open). An unlinked `.torrent` was already
            // removed from the DB by `unlink`, so do NOT re-persist its
            // buffered bytes here — just retire the inode.
            if self.inode_mgr.is_unlinked_file(ino) {
                if self.inode_mgr.open_handle_count(ino) == 0 {
                    self.inode_mgr.inodes.remove(&ino);
                    self.buffered_torrents.remove(&ino);
                }
                return Ok(());
            }

            // Move the buffered bytes out of the inode instead of cloning the
            // whole (up to 10 MiB) buffer: the persist thread takes ownership,
            // and the dispatch side keeps a shared `Arc` handle so a read of
            // the same `.torrent` still sees its bytes.
            let Some((name, parent, data)) = self.take_buffered_file(ino) else {
                return Ok(());
            };
            {
                if name.ends_with(".torrent") {
                    let source_path = self.inode_mgr.extract_source_path(parent);
                    let dedup_key = (source_path.clone(), name.clone());

                    if data.is_empty() {
                        warn!("Zero-byte torrent file {} removed", name);
                        // overwriting an existing `metadata/` file
                        // with empty content removes the metadata inode but
                        // left the DB row behind, orphaning the
                        // `data/<seed>.torrent/` mirror.  Wait for an in-flight
                        // `add_torrent` first so its row lands
                        // before we delete it, then drop the row + inode.
                        if let Err(e) =
                            self.wait_for_pending_add(&dedup_key, pending_timeout, "release")
                        {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        if let Err(e) = self.remove_torrent_and_data_state(&source_path, &name) {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        self.inode_mgr.inodes.remove(&ino);
                        return Err(FsError::NotFound);
                    }

                    if data.len() > MAX_TORRENT_SIZE {
                        // Defensive guard — unreachable via the public API:
                        // `write` and `setattr` both cap the buffer at
                        // MAX_TORRENT_SIZE, so an oversized `cp` fails in
                        // `write` with EFBIG and lands in the invalid-bencode
                        // branch below.  Kept (mirroring `flush`'s size guard)
                        // in case a future path lets the buffer overshoot.
                        if let Err(e) =
                            self.wait_for_pending_add(&dedup_key, pending_timeout, "release")
                        {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        if let Err(e) = self.remove_torrent_and_data_state(&source_path, &name) {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    if TorrentInfo::validate_slice(&data).is_err() {
                        warn!("Torrent {} invalid, removing inode", name);
                        // same orphan fix + pending-add wait for
                        // unparseable content (the oversized-`cp` case lands
                        // here: `write` rejects the tail with EFBIG, leaving
                        // partial invalid bytes behind).
                        if let Err(e) =
                            self.wait_for_pending_add(&dedup_key, pending_timeout, "release")
                        {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        if let Err(e) = self.remove_torrent_and_data_state(&source_path, &name) {
                            self.restore_buffered_file(ino, data);
                            return Err(e);
                        }
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    // An overwrite whose close lands while the previous
                    // content is still being persisted must not be dropped:
                    // wait for the in-flight add to settle (bounded by
                    // C-2.5.5's close-path deadline), then persist this
                    // content on top of it. Returning early here left the
                    // database holding the OLD bytes while `metadata/` showed
                    // the new torrent, so `data/` and a restart disagreed.
                    // C-2.5.5: this wait may time out. The bytes must survive a
                    // recoverable early return — the kernel ignores the
                    // RELEASE error, so dropping them here would lose the new
                    // content permanently and a later close would see an empty
                    // buffer and delete the previously landed source.
                    if let Err(e) =
                        self.wait_for_pending_add(&dedup_key, pending_timeout, "release")
                    {
                        self.restore_buffered_file(ino, data);
                        return Err(e);
                    }

                    // Mark the key in-flight, then DROP the lock before
                    // spawning background work. Keyed by
                    // `(source_path, filename)` so same-directory torrents
                    // (root has `source_path == ""`) don't collide. Persistence
                    // + handle registration run detached so the FUSE
                    // dispatcher is never blocked: a DB write failure is
                    // recorded and surfaced as EIO by the next lookup/readdir
                    // of `data/` (the kernel drops the FUSE_RELEASE error).
                    {
                        let mut processing = self.processing_torrents.lock().map_err(|e| {
                            error!("Mutex poisoned in release(): {}", e);
                            FsError::LockPoisoned
                        })?;
                        processing.insert(dedup_key.clone(), ());
                    }
                    // Lock released here.

                    // The inode is NOT removed here: if persistence fails, the
                    // file must remain visible so the user can retry or delete
                    // it, and the failure is recorded for
                    // `lookup`/`readdir` to surface as EIO.  On success the
                    // DB-backed entry supersedes this inode (lookup queries the
                    // DB), so keeping it is harmless.
                    if let Some(ts) = self.torrent_service.clone() {
                        let shared = Arc::new(data);
                        self.buffered_torrents.insert(ino, Arc::clone(&shared));
                        let processing = self.processing_torrents.clone();
                        let processing_cv = self.processing_torrents_cv.clone();
                        let persist_errors = self.persist_errors.clone();
                        let evictions = self.pending_projection_evictions.clone();
                        let versions = self.content_versions.clone();
                        let cancel_sink = self.read_cancel_sink.clone();
                        let cancelled = self.cancelled_reads.clone();
                        let metrics = self.metrics.clone();
                        let notifier = self.notifier.clone();
                        let key = dedup_key.clone();
                        std::thread::spawn(move || {
                            let persisted =
                                match ts.persist_torrent(shared.as_slice(), &source_path, &name) {
                                    Ok(persisted) => {
                                        // Clear any stale failure from a prior
                                        // attempt; this one succeeded.
                                        if let Ok(mut guard) = persist_errors.lock() {
                                            guard.remove(&key);
                                        }
                                        // A replacement must settle the old
                                        // content's in-flight reads now, without
                                        // waiting for another FUSE request
                                        // (C-4.5.3): publish the landed content id
                                        // and cancel waiters of any other content,
                                        // then queue the dispatch-side eviction. A
                                        // re-persist of identical bytes changes
                                        // nothing and cancels nothing.
                                        if persisted.content_changed() {
                                            FsService::record_content_published_shared(
                                                &versions,
                                                &cancel_sink,
                                                &cancelled,
                                                &metrics,
                                                persisted.source_id(),
                                                Some(persisted.content_id()),
                                            );
                                        }
                                        // The new content has landed: queue an
                                        // eviction of the projection inodes the
                                        // row superseded, so the dispatch side
                                        // stops serving the replaced content.
                                        if let Ok(mut queue) = evictions.lock() {
                                            if !queue.contains(&key) {
                                                queue.push(key.clone());
                                            }
                                        }
                                        ts.register_torrent_handle(&persisted);
                                        Some(persisted)
                                    }
                                    Err(e) => {
                                        error!("Failed to persist torrent {}: {}", name, e);
                                        // record the failure so the next
                                        // user-visible lookup/readdir of data/
                                        // surfaces it as EIO.
                                        if let Ok(mut guard) = persist_errors.lock() {
                                            guard.insert(key.clone(), e.to_string());
                                        }
                                        None
                                    }
                                };
                            // Clear the key and wake a rename/release blocked
                            // on this pending add under one lock hold: a notify
                            // after the guard dropped could land between the
                            // waiter's predicate check and its wait, and the
                            // waiter would sleep out the full deadline.
                            if let Ok(mut guard) = processing.lock() {
                                guard.remove(&key);
                                processing_cv.notify_one();
                            }
                            // Purge the kernel's cached entries for what the
                            // write replaced. Sent here, never from the drain:
                            // the notify takes the parent lock the in-flight
                            // lookup already holds. The key is cleared first so
                            // no waiter on this add is blocked behind it.
                            if let Some(persisted) = persisted {
                                if let Some(Some(sink)) = notifier.get() {
                                    let (parent_ino, _) =
                                        FsService::split_data_dir_parent(&source_path);
                                    notify_inval_entry(sink, parent_ino, &name);
                                    // Root-level entries of the replaced and
                                    // the landed content. The kernel holds them
                                    // under whichever inode it resolved for the
                                    // seed root: the landed row's, or the
                                    // pending-window hash when the seed was
                                    // looked up before the row landed.
                                    let roots = [
                                        InodeManager::make_torrent_root_ino(persisted.source_id()),
                                        InodeManager::make_pending_torrent_ino(&source_path, &name),
                                    ];
                                    for root in roots {
                                        if root == INVALID_INO {
                                            continue;
                                        }
                                        for stale in persisted.changed_root_names() {
                                            notify_inval_entry(sink, root, stale);
                                        }
                                    }
                                }
                            }
                        });
                    } else {
                        info!(
                            "Torrent {} received (no DB configured, skipping insert)",
                            name
                        );
                        self.buffered_torrents.insert(ino, Arc::new(data));
                        if let Ok(mut guard) = self.processing_torrents.lock() {
                            guard.remove(&dedup_key);
                            // wake any waiter (defensive; no-DB path).
                            self.processing_torrents_cv.notify_one();
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Remove a torrent by `(source_path, filename)` and clean up every
    /// `data/`-side trace: cached data inodes, kernel dentries, the
    /// processing-lock key, and the L1 range cache. Shared by `unlink` and
    /// `release`'s invalid-content path (where an overwrite removed the
    /// metadata inode but left the DB row behind). Callers run
    /// `wait_for_pending_add` first so an in-flight add lands its row *before*
    /// the removal, not after. Returns `Ok(Some((torrent_id, info_hash)))` when
    /// a row was removed, `Ok(None)` when none existed; DB errors propagate.
    fn remove_torrent_and_data_state(
        &mut self,
        source_path: &str,
        filename: &str,
    ) -> FsResult<Option<(i64, String)>> {
        // clear any recorded persistence failure for this
        // (source_path, filename) — the torrent is being removed, so the
        // `data/` mirror must not keep surfacing a stale EIO for it.
        if let Ok(mut guard) = self.persist_errors.lock() {
            guard.remove(&(source_path.to_string(), filename.to_string()));
        }

        let Some(ts) = &self.torrent_service else {
            return Ok(None);
        };

        let (torrent_id, info_hash) = match ts.remove_torrent(filename, source_path)? {
            Some(pair) => pair,
            None => {
                // No DB row (never added): still invalidate any stale pending
                // TorrentRoot cached from a prior lookup.
                let data_parent = if source_path.is_empty() {
                    DATA_INO
                } else {
                    InodeManager::make_source_path_dir_ino(source_path)
                };
                self.inval_data_entry(data_parent, filename);
                return Ok(None);
            }
        };

        self.cleanup_removed_torrent_data_state(torrent_id, &info_hash, source_path, filename)?;

        Ok(Some((torrent_id, info_hash)))
    }

    /// Evict every in-memory/`data/`-side trace of a torrent whose DB row has
    /// been removed: cached data inodes, the kernel dentry, the
    /// processing-lock key, and L1 range-cache entries. Pure cache cleanup —
    /// it takes the already-known `(torrent_id, info_hash, source_path,
    /// filename)` so callers that removed the row through a non-`remove_torrent`
    /// path (rename-overwrite) can reuse it. Empty metadata directories must
    /// persist (and be restored on restart), so no directory rows are pruned
    /// here — source-path directory rows and their cached `SourcePathDir`
    /// entries stay valid (the directories still exist, now empty).
    fn cleanup_removed_torrent_data_state(
        &mut self,
        torrent_id: i64,
        _info_hash: &str,
        source_path: &str,
        filename: &str,
    ) -> FsResult<()> {
        self.inode_mgr
            .data_inodes
            .retain(|_, data_inode| match data_inode {
                DataInode::TorrentRoot {
                    torrent_id: tid, ..
                } => *tid != torrent_id,
                DataInode::TorrentDir {
                    torrent_id: tid, ..
                } => *tid != torrent_id,
                DataInode::TorrentFile {
                    torrent_id: tid, ..
                } => *tid != torrent_id,
                DataInode::SourcePathDir { .. } => true,
            });

        // purge the kernel dentry cache for the removed torrent root
        // so the stale mirror vanishes at once instead of lingering for the
        // 1s FUSE TTL.
        let data_parent = if source_path.is_empty() {
            DATA_INO
        } else {
            InodeManager::make_source_path_dir_ino(source_path)
        };
        self.inval_data_entry(data_parent, filename);

        {
            let mut processing = self.processing_torrents.lock().map_err(|e| {
                error!(
                    "Mutex poisoned in cleanup_removed_torrent_data_state() processing_torrents: {}",
                    e
                );
                FsError::LockPoisoned
            })?;
            processing.remove(&(source_path.to_string(), filename.to_string()));
        }

        // L1 entries are keyed by the content-scoped `file_id`, which is never
        // reused, so entries of a removed torrent can no longer be hit; the
        // bounded cache reclaims them on overflow.
        self.retain_live_read_identities();
        if let Some(ts) = &self.torrent_service {
            ts.invalidate_file_identities(torrent_id);
        }

        // Any in-flight read for this source row settles with `EIO`
        // (C-4.5.3). The sink settles it now; the queued entry is the adapter's
        // fallback drain.
        self.record_source_removed(torrent_id);

        Ok(())
    }

    pub fn mkdir(&mut self, parent: u64, name: &str) -> FsResult<Attr> {
        self.touch_metadata_index();
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if self.metadata_child(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let source_path = if parent == METADATA_INO {
            name.to_string()
        } else {
            let parent_source_path = self.inode_mgr.extract_source_path(parent);
            if parent_source_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_source_path, name)
            }
        };

        // Persist before touching the in-memory tree: a database failure must
        // leave `metadata/` unchanged (the directory row and its closure are
        // the restart-time truth), not create an inode that vanishes on the
        // next mount.
        if let Some(ref ts) = self.torrent_service {
            ts.ensure_metadata_directories(&source_path)?;
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::Directory {
                parent,
                name: name.to_string(),
            },
        );

        info!(
            "Created directory {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(self.inode_mgr.attr_for_dir(new_ino, true))
    }

    pub fn rmdir(&mut self, parent: u64, name: &str) -> FsResult<()> {
        self.touch_metadata_index();
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        let ino = match self.metadata_child(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::Directory { .. }) => {
                let has_children = self
                    .metadata_index()
                    .by_parent
                    .get(&ino)
                    .is_some_and(|children| !children.is_empty());

                if has_children {
                    return Err(FsError::DirectoryNotEmpty);
                }

                let source_path = self.inode_mgr.extract_source_path(ino);

                // Every source row under this directory disappears with it;
                // remember them so their in-flight reads settle with EIO
                // (C-4.5.3). The rows are still present before the delete.
                let removed_ids: Vec<i64> = self
                    .db
                    .as_ref()
                    .and_then(|db| db.lock().ok())
                    .and_then(|guard| guard.get_torrents_by_source_path_prefix(&source_path).ok())
                    .map(|torrents| torrents.iter().map(|t| t.id).collect())
                    .unwrap_or_default();

                // Delete the row first: on failure the inode (and the
                // directory) stays visible, matching the database truth that
                // would otherwise restore it on the next mount.
                if let Some(ref ts) = self.torrent_service {
                    ts.delete_metadata_directory(&source_path)?;
                }

                for torrent_id in &removed_ids {
                    if let Some(ref ts) = self.torrent_service {
                        ts.invalidate_file_identities(*torrent_id);
                    }
                    self.record_source_removed(*torrent_id);
                }

                self.inode_mgr.inodes.remove(&ino);

                // Clean up data_inodes cache.
                self.inode_mgr
                    .data_inodes
                    .retain(|_, data_inode| match data_inode {
                        DataInode::SourcePathDir { path } => {
                            if path == &source_path {
                                false
                            } else if source_path.is_empty() {
                                true
                            } else {
                                !path.starts_with(&format!("{}/", source_path))
                            }
                        }
                        _ => true,
                    });
                self.retain_live_read_identities();

                // purge the kernel dentry cache for the removed
                // `data/` SourcePathDir so the stale mirror vanishes at
                // once instead of lingering for the 1s FUSE TTL.
                let (dir_parent, dir_name) = Self::split_data_dir_parent(&source_path);
                self.inval_data_entry(dir_parent, &dir_name);

                info!(
                    "Deleted directory '{}' (source_path='{}')",
                    name, source_path
                );
                Ok(())
            }
            Some(InodeData::File { .. }) => Err(FsError::NotDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn unlink(&mut self, parent: u64, name: &str) -> FsResult<Option<i64>> {
        self.unlink_with_pending_timeout(parent, name, PENDING_ADD_TIMEOUT)
    }

    /// like `unlink`, but the pending-add wait timeout is
    /// injectable so tests can exercise the timeout path in milliseconds
    /// instead of hard-waiting out the production 5s deadline.
    pub(crate) fn unlink_with_pending_timeout(
        &mut self,
        parent: u64,
        name: &str,
        pending_timeout: Duration,
    ) -> FsResult<Option<i64>> {
        self.touch_metadata_index();
        let mut removed_id = None;

        // `.stats` is a reserved virtual filename whose files have
        // immutable attributes — `rm` on it returns `EPERM` (`NotPermitted`),
        // matching `write`, `setattr`, and `symlink`.
        // The root `.stats` (`STATS_INO`) previously fell
        // through the metadata-child guard below → `EACCES`; the `data/`
        // subtree `.stats` (derived stats inode) previously hit the data/
        // guard → `EROFS`.  Both are rejections, but `.stats` must return a
        // single `EPERM` consistent with every other mutating operation on it.
        if name == ".stats" {
            return Err(FsError::NotPermitted);
        }
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::InvalidArgument);
        }

        let ino = match self.metadata_child(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File {
                name,
                parent: file_parent,
                ..
            }) => {
                let filename = name.clone();
                let source_path = self.inode_mgr.extract_source_path(*file_parent);

                if self.torrent_service.is_some() {
                    // A fast `cp` followed by `rm` races the detached
                    // `add_torrent` from `release`: remove finds no row yet and
                    // marks the inode unlinked, then the in-flight add lands
                    // the row after the unlink and `data/` serves an orphaned
                    // root. Wait (bounded) for the pending add to settle so the
                    // remove finds and deletes the row; on timeout fail cleanly
                    // (inode untouched, user can retry).
                    let dedup_key = (source_path.clone(), filename.clone());
                    self.wait_for_pending_add(&dedup_key, pending_timeout, "unlink")?;

                    match self.remove_torrent_and_data_state(&source_path, &filename) {
                        Ok(Some((torrent_id, _info_hash))) => {
                            removed_id = Some(torrent_id);
                            // Defer inode destruction: mark it unlinked (so the
                            // name vanishes from lookup/readdir) but keep the
                            // inode + open handles alive — read/write/flush/
                            // release must keep working until the last handle
                            // closes. With no open handle, `unlink_file`
                            // destroys it now; `open_files` is NOT stripped (it
                            // would orphan the handle and drop buffered bytes).
                            self.inode_mgr.unlink_file(ino);
                            if !self.inode_mgr.inodes.contains_key(&ino) {
                                self.buffered_torrents.remove(&ino);
                            }
                            info!(
                                "Deleted torrent '{}' (id={}, source_path='{}')",
                                filename, torrent_id, source_path
                            );
                        }
                        Ok(None) => {
                            // same deferred-destruction logic.
                            self.inode_mgr.unlink_file(ino);
                            if !self.inode_mgr.inodes.contains_key(&ino) {
                                self.buffered_torrents.remove(&ino);
                            }
                            info!("Deleted file '{}' (not yet in database)", filename);
                        }
                        Err(e) => {
                            error!("Failed to delete torrent: {:?}", e);
                            return Err(e);
                        }
                    }
                } else {
                    // same deferred-destruction logic (no DB).
                    self.inode_mgr.unlink_file(ino);
                    if !self.inode_mgr.inodes.contains_key(&ino) {
                        self.buffered_torrents.remove(&ino);
                    }
                    info!("Deleted file '{}' (no database)", filename);
                }

                // The inode survives an unlink while a handle is open, so its
                // count is unchanged: invalidate the metadata index explicitly
                // or `readdir` keeps serving the now-unlinked name.
                self.touch_metadata_index();
                Ok(removed_id)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
    ) -> FsResult<()> {
        self.rename_with_pending_timeout(parent, name, newparent, newname, PENDING_ADD_TIMEOUT)
    }

    /// the pending-add wait timeout is injectable so
    /// tests can exercise the timeout/rollback path in milliseconds instead
    /// of hard-waiting out the 5s production deadline.
    pub(crate) fn rename_with_pending_timeout(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        pending_timeout: Duration,
    ) -> FsResult<()> {
        self.touch_metadata_index();
        // data/ is a read-only namespace — renames into or out
        // of it must return `EROFS`, not `ENOENT` or `EPERM`. Check this
        // before parent existence: an inode number in the data range is
        // inherently read-only, regardless of whether it is currently
        // materialised.
        if InodeManager::is_data_namespace(parent) || InodeManager::is_data_namespace(newparent) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // Check parent existence in both inodes and data_inodes tables.
        let parent_exists = self.inode_mgr.inodes.contains_key(&parent)
            || self.inode_mgr.data_inodes.contains_key(&parent);
        if !parent_exists {
            return Err(FsError::NotFound);
        }

        // Destination parent missing → ENOENT, not ENOTDIR: the kernel
        // resolved the path but forwarded a stale or absent `newparent`
        // inode. `mv x missing/` never reaches this handler with a
        // non-directory source — the VFS answers ENOTDIR itself (fs/namei.c
        // do_renameat2) before calling into FUSE, exactly as on ext4/xfs/
        // tmpfs, so that wording is not ours to change. A directory source is
        // forwarded and renamed, matching the local filesystems.
        let newparent_exists = self.inode_mgr.inodes.contains_key(&newparent)
            || self.inode_mgr.data_inodes.contains_key(&newparent);
        if !newparent_exists {
            return Err(FsError::NotFound);
        }

        if !self.inode_mgr.is_metadata_child(parent) || !self.inode_mgr.is_metadata_child(newparent)
        {
            return Err(FsError::NotPermitted);
        }

        let source_ino = match self.metadata_child(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        // POSIX `rename` overwrite semantics: a regular file may be renamed
        // over an existing regular file — the destination is removed and the
        // source takes its name. Every other destination collision is still
        // rejected: a directory cannot silently replace (or be replaced by) a
        // file, and directory-over-directory keeps its existing EEXIST.
        let overwrite_target = if let Some(target_ino) = self.metadata_child(newparent, newname) {
            if target_ino == source_ino {
                return Ok(());
            }
            let source_is_file = matches!(
                self.inode_mgr.inodes.get(&source_ino),
                Some(InodeData::File { .. })
            );
            let target_is_file = matches!(
                self.inode_mgr.inodes.get(&target_ino),
                Some(InodeData::File { .. })
            );
            if source_is_file && target_is_file {
                Some(target_ino)
            } else {
                return Err(FsError::AlreadyExists);
            }
        } else {
            None
        };

        let is_directory = matches!(
            self.inode_mgr.inodes.get(&source_ino),
            Some(InodeData::Directory { .. })
        );

        if is_directory {
            // --- Directory rename ---
            let old_source_path = self.inode_mgr.extract_source_path(source_ino);
            let new_source_path = if newparent == METADATA_INO {
                newname.to_string()
            } else {
                let parent_path = self.inode_mgr.extract_source_path(newparent);
                if parent_path.is_empty() {
                    newname.to_string()
                } else {
                    format!("{}/{}", parent_path, newname)
                }
            };

            // Persist first: the row rename is one transaction, and applying
            // the new name in memory before it committed left `metadata/`
            // showing a moved directory whose database rows still pointed at
            // the old path (the next mount restored the old name, and `data/`
            // disagreed in between). A failed rename now leaves both names
            // exactly as they were in memory and in the database.
            if let Some(ref ts) = self.torrent_service {
                ts.rename_metadata_directory(&old_source_path, newname, &new_source_path)?;
                info!(
                    "Renamed metadata directory '{}' to '{}' (source_path: '{}' -> '{}')",
                    name, newname, old_source_path, new_source_path
                );
            } else {
                info!(
                    "Renamed directory '{}' to '{}' (no database)",
                    name, newname
                );
            }

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::Directory {
                    parent: newparent,
                    name: newname.to_string(),
                },
            );

            // Update data_inodes cache.
            let old_prefix = format!("{}/", old_source_path);
            let new_prefix = format!("{}/", new_source_path);
            for data_inode in self.inode_mgr.data_inodes.values_mut() {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        if path == &old_source_path {
                            *path = new_source_path.clone();
                        } else if path.starts_with(&old_prefix) {
                            *path = format!("{}{}", new_prefix, &path[old_prefix.len()..]);
                        }
                    }
                    DataInode::TorrentRoot { source_path, .. } => {
                        if source_path == &old_source_path {
                            *source_path = new_source_path.clone();
                        } else if source_path.starts_with(&old_prefix) {
                            *source_path =
                                format!("{}{}", new_prefix, &source_path[old_prefix.len()..]);
                        }
                    }
                    _ => {}
                }
            }

            // purge the kernel dentry cache for the renamed directory's old
            // `data/` projection so the stale mirror name vanishes at once
            // instead of lingering for the 1s FUSE TTL.
            let (dir_parent, dir_name) = Self::split_data_dir_parent(&old_source_path);
            self.inval_data_entry(dir_parent, &dir_name);

            // Renaming reuses the same inode, so the index's inode count does
            // not change: invalidate explicitly or `readdir`/`lookup` keep
            // resolving the old name.
            self.touch_metadata_index();
            Ok(())
        } else {
            // --- File rename ---
            // When both parents and the source exist and the destination
            // name does not already resolve (the preceding guards return
            // ENOENT/EEXIST otherwise), `metadata/` only accepts `.torrent`
            // files — also enforced in `create` — so dropping the suffix on
            // either side is rejected with `InvalidArgument` → EINVAL, not
            // `NotFound`/ENOENT: the destination parent exists, only the
            // target name is invalid. A missing destination *parent* fails
            // the kernel VFS intermediate LOOKUP with ENOENT regardless of
            // trailing slash; a missing *final* name reaches FUSE without a
            // slash (this EINVAL path) and yields ENOTDIR only under a
            // trailing slash.
            if !name.ends_with(".torrent") || !newname.ends_with(".torrent") {
                return Err(FsError::InvalidArgument);
            }

            let (old_name, was_unlinked) = match self.inode_mgr.inodes.get(&source_ino) {
                Some(InodeData::File { name, unlinked, .. }) => (name.clone(), *unlinked),
                None => {
                    return Err(FsError::NotFound);
                }
                _ => unreachable!(),
            };

            self.move_file_inode(source_ino, newparent, newname, was_unlinked)?;

            if let Some(ref ts) = self.torrent_service {
                let old_source_path = self.inode_mgr.extract_source_path(parent);
                let new_source_path = self.inode_mgr.extract_source_path(newparent);

                // a fast `cp` followed immediately by `mv` races
                // with the detached `add_torrent` thread spawned by
                // `release`: the DB row may not exist yet, so
                // `rename_torrent` silently no-ops (`Ok(None)`) and the row
                // later lands at the OLD name — the data/ mirror then keeps
                // serving the stale filename and the rename is lost on
                // restart. Wait (bounded) for the pending add to settle
                // first; on timeout fail so the caller retries.
                let dedup_key = (old_source_path.clone(), old_name.clone());
                // On timeout the helper returns Err and the inode is rolled
                // back to the pre-rename name below — a bare `?` would leave
                // metadata/ showing the new name while the still-pending
                // background add lands the DB row at the OLD one,
                if let Err(e) = self.wait_for_pending_add(&dedup_key, pending_timeout, "rename") {
                    self.restore_renamed_file(source_ino, parent, &old_name, was_unlinked);
                    return Err(e);
                }

                if let Some(target_ino) = overwrite_target {
                    // Overwrite: the destination and source rows must change
                    // atomically, so a source-side failure can never leave the
                    // destination already destroyed. Wait for the
                    // destination's in-flight add to settle first — a
                    // still-pending add would otherwise land a row at
                    // `(new_source_path, newname)` after our move and clobber
                    // the source. On timeout roll the source inode back; the
                    // destination is untouched.
                    let target_dedup_key = (new_source_path.clone(), newname.to_string());
                    if let Err(e) =
                        self.wait_for_pending_add(&target_dedup_key, pending_timeout, "rename")
                    {
                        self.restore_renamed_file(source_ino, parent, &old_name, was_unlinked);
                        return Err(e);
                    }

                    let result = match ts.rename_torrent_overwriting(
                        &old_name,
                        &old_source_path,
                        newname,
                        &new_source_path,
                    ) {
                        Ok(r) => r,
                        Err(e) => {
                            self.restore_renamed_file(source_ino, parent, &old_name, was_unlinked);
                            return Err(e);
                        }
                    };

                    match result {
                        MoveOverwriteResult::SourceAbsent => {
                            // The source's pending add failed to persist — no
                            // row to move. Roll the in-memory move back and
                            // FAIL the rename: both names stay in place, so the
                            // kernel's dentry view and the directory state
                            // agree. Surfacing an error (rather than the
                            // non-overwrite path's silent inode-only move) is
                            // deliberate — the destination must not be
                            // destroyed for a source that cannot replace it.
                            self.restore_renamed_file(source_ino, parent, &old_name, was_unlinked);
                            return Err(FsError::Internal(format!(
                                "cannot rename '{}': torrent has no persisted row",
                                old_name
                            )));
                        }
                        MoveOverwriteResult::Moved { removed_target } => {
                            // DB committed — the in-memory + cache mutations
                            // are now safe.
                            if let Ok(mut guard) = self.persist_errors.lock() {
                                guard.remove(&(new_source_path.clone(), newname.to_string()));
                            }
                            self.retire_overwritten_file(target_ino);
                            if let Some((target_id, target_info_hash)) = removed_target {
                                self.cleanup_removed_torrent_data_state(
                                    target_id,
                                    &target_info_hash,
                                    &new_source_path,
                                    newname,
                                )?;
                            }
                        }
                    }
                } else {
                    ts.rename_torrent(&old_name, &old_source_path, newname, &new_source_path)?;
                }

                // a cross-directory move leaves the old source-path
                // directory empty, but empty directories must persist (and be
                // restored on restart) — so no directory rows are pruned
                // here. Only the moved torrent's stale TorrentRoot cache
                // entry is evicted; the source-path directory rows and their
                // cached SourcePathDir entries stay valid.
                if old_source_path != new_source_path {
                    // Evict cached TorrentRoot entries bound to the old
                    // location so later lookups re-resolve from the DB at the
                    // new one.
                    self.inode_mgr
                        .data_inodes
                        .retain(|_, data_inode| match data_inode {
                            DataInode::TorrentRoot { source_path, .. } => {
                                source_path != &old_source_path
                            }
                            _ => true,
                        });

                    // purge the kernel dentry cache for the old
                    // data/ location so the stale name vanishes at once
                    // instead of lingering for the 1s FUSE TTL.
                    let old_data_parent = if old_source_path.is_empty() {
                        DATA_INO
                    } else {
                        InodeManager::make_source_path_dir_ino(&old_source_path)
                    };
                    self.inval_data_entry(old_data_parent, &old_name);
                } else {
                    // an intra-directory rename keeps the
                    // source_path but changes the filename — a cached
                    // TorrentRoot entry still carries the old `filename`, so
                    // data/ would keep listing the stale name (and the DB row
                    // under the new one would be shadowed) until restart.
                    // Evict it so later lookups re-resolve from the DB.
                    self.inode_mgr
                        .data_inodes
                        .retain(|_, data_inode| match data_inode {
                            DataInode::TorrentRoot {
                                source_path,
                                filename,
                                ..
                            } => !(source_path == &old_source_path && filename == &old_name),
                            _ => true,
                        });

                    // purge the kernel dentry cache for the old
                    // filename so the stale name vanishes from `data/` at
                    // once instead of lingering for the 1s FUSE TTL.
                    let data_parent = if old_source_path.is_empty() {
                        DATA_INO
                    } else {
                        InodeManager::make_source_path_dir_ino(&old_source_path)
                    };
                    self.inval_data_entry(data_parent, &old_name);
                }
                info!(
                    "Renamed torrent '{}' to '{}' (source_path: '{}' -> '{}')",
                    old_name, newname, old_source_path, new_source_path
                );
            } else {
                // No database: an overwrite still retires the destination
                // inode so the source takes its name.
                if let Some(target_ino) = overwrite_target {
                    self.retire_overwritten_file(target_ino);
                }
                info!("Renamed file '{}' to '{}' (no database)", old_name, newname);
            }
            // Same reason as the directory branch: the moved inode keeps its
            // count, so a cached index would still list it under the old name.
            self.touch_metadata_index();
            Ok(())
        }
    }

    /// Remove a rename destination that the source replaces.
    ///
    /// `InodeManager::unlink_file` destroys the inode immediately when no
    /// handle is open and only marks it unlinked otherwise. Its parked shared
    /// buffer must follow the same lifetime: reclaim it now on immediate
    /// destruction, or keep it until the last handle closes (`release` already
    /// removes it there). Without this, repeated overwrite-renames leak one
    /// `Arc<Vec<u8>>` per destroyed destination.
    fn retire_overwritten_file(&mut self, target_ino: u64) {
        if self.inode_mgr.unlink_file(target_ino) {
            self.buffered_torrents.remove(&target_ino);
        }
    }

    /// Move a file inode's directory entry, carrying its buffer without a
    /// copy. The caller restores the old entry with
    /// [`Self::restore_renamed_file`] if the persist step fails.
    fn move_file_inode(
        &mut self,
        ino: u64,
        parent: u64,
        name: &str,
        unlinked: bool,
    ) -> FsResult<()> {
        match self.inode_mgr.inodes.remove(&ino) {
            Some(InodeData::File { data, .. }) => {
                self.inode_mgr.inodes.insert(
                    ino,
                    InodeData::File {
                        parent,
                        name: name.to_string(),
                        data,
                        unlinked,
                    },
                );
                Ok(())
            }
            Some(other) => {
                self.inode_mgr.inodes.insert(ino, other);
                Err(FsError::NotFound)
            }
            None => Err(FsError::NotFound),
        }
    }

    /// Put a file inode back at its pre-rename `(parent, name)`. The buffered
    /// `.torrent` bytes move back instead of being cloned, so a failed rename
    /// never copies the (up to 10 MiB) buffer a second time.
    fn restore_renamed_file(&mut self, ino: u64, parent: u64, name: &str, unlinked: bool) {
        if let Some(InodeData::File { data, .. }) = self.inode_mgr.inodes.remove(&ino) {
            self.inode_mgr.inodes.insert(
                ino,
                InodeData::File {
                    parent,
                    name: name.to_string(),
                    data,
                    unlinked,
                },
            );
        }
    }

    /// Wait (bounded by `timeout`) for a pending `add_torrent` at `dedup_key`
    /// to clear, sleeping on `processing_torrents_cv` rather than polling the
    /// map. On timeout the caller has already rolled back inode state and must
    /// fail. `timeout` is a parameter so tests can use a short deadline; `op`
    /// names the calling operation ("rename"/"unlink") for the timeout message.
    fn wait_for_pending_add(
        &self,
        dedup_key: &(String, String),
        timeout: Duration,
        op: &str,
    ) -> FsResult<()> {
        let deadline = crate::domain::time::deadline_after(timeout);
        let mut guard = self
            .processing_torrents
            .lock()
            .map_err(|_| FsError::LockPoisoned)?;
        while guard.contains_key(dedup_key) {
            let now = std::time::Instant::now();
            if now >= deadline {
                error!(
                    "Timed out waiting for pending add of '{:?}' before {}",
                    dedup_key, op
                );
                return Err(FsError::Internal(format!(
                    "torrent is still being processed; retry the {}",
                    op
                )));
            }
            let (g, timed_out) = self
                .processing_torrents_cv
                .wait_timeout(guard, deadline - now)
                .map_err(|_| {
                    error!("Condvar poisoned in {}() pending-add wait", op);
                    FsError::LockPoisoned
                })?;
            guard = g;
            if timed_out.timed_out() && guard.contains_key(dedup_key) {
                continue;
            }
        }
        drop(guard);
        Ok(())
    }

    // ── Data namespace (read-only: local / remote) ──────────────────────────

    /// Serve a read request across all namespaces (EISDIR, stats, data, metadata).
    pub fn read(&mut self, ino: u64, offset: i64, size: u32) -> FsResult<ReadOutcome> {
        self.drain_projection_evictions();
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Err(FsError::IsDirectory),
            STATS_INO => {
                let offset = offset as usize;
                let stats = self.generate_global_stats_content();
                if offset >= stats.len() {
                    Ok(ReadOutcome::Ready(Vec::new()))
                } else {
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    Ok(ReadOutcome::Ready(stats[offset..end].to_vec()))
                }
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let stats = self.generate_data_stats_for_ino(ino);
                    let offset = offset as usize;
                    if offset >= stats.len() {
                        return Ok(ReadOutcome::Ready(Vec::new()));
                    }
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    return Ok(ReadOutcome::Ready(stats[offset..end].to_vec()));
                }

                if InodeManager::is_data_ino(ino) {
                    let file = match self.inode_mgr.data_inodes.get(&ino) {
                        Some(DataInode::TorrentFile {
                            torrent_id,
                            file_id,
                            size,
                            ..
                        }) => Some((*torrent_id, *file_id, *size)),
                        _ => None,
                    };
                    if let Some((torrent_id, file_id, file_size)) = file {
                        let offset = offset as usize;
                        let read_size = size as usize;

                        debug!(
                            "Read request for torrent file (torrent_id={}, file_id={}, offset={}, size={})",
                            torrent_id, file_id, offset, read_size
                        );

                        let actual_size = file_size as usize;
                        if offset >= actual_size {
                            return Ok(ReadOutcome::Ready(Vec::new()));
                        }

                        let end = std::cmp::min(offset + read_size, actual_size);
                        let result_size = end - offset;

                        // A zero-length read returns zero bytes without
                        // entering the download path (C-4.1.6).
                        if result_size == 0 {
                            return Ok(ReadOutcome::Ready(Vec::new()));
                        }

                        // A pending projection has no landed source row yet
                        // (its inode carries the sentinel `torrent_id == 0`).
                        // The read becomes possible once the persist publishes
                        // the row, so report the retryable miss instead of the
                        // `EIO` reserved for superseded content.
                        if torrent_id == 0 || file_id == 0 {
                            return Err(FsError::NotFound);
                        }
                        // The identity was bound to this inode by the same
                        // projection that created it. Without it the inode
                        // belongs to superseded content: settle `EIO` instead
                        // of resolving (and materializing) the new content's
                        // file table underneath it.
                        let identity = self.read_identity(ino).ok_or_else(|| {
                            FsError::Io(format!(
                                "read identity unavailable for inode {ino} \
                                 (content was replaced or removed)",
                            ))
                        })?;
                        return self.read_data(
                            torrent_id,
                            file_id,
                            identity,
                            offset as u64,
                            result_size as u32,
                        );
                    }

                    // C-2.2.6: reading a directory is `EISDIR`, not `ENOENT`.
                    if self.inode_mgr.data_inodes.contains_key(&ino) {
                        return Err(FsError::IsDirectory);
                    }
                    return Err(FsError::NotFound);
                }

                if let Some(data) = self.buffered_torrent_bytes(ino) {
                    let offset = offset as usize;
                    let end = std::cmp::min(offset + size as usize, data.len());
                    if offset < data.len() {
                        Ok(ReadOutcome::Ready(data[offset..end].to_vec()))
                    } else {
                        Ok(ReadOutcome::Ready(Vec::new()))
                    }
                } else if matches!(
                    self.inode_mgr.inodes.get(&ino),
                    Some(InodeData::Directory { .. })
                ) {
                    Err(FsError::IsDirectory)
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    /// Read torrent file data from local cache or the BitTorrent network.
    pub fn read_data(
        &mut self,
        torrent_id: i64,
        file_id: i64,
        identity: Arc<FileIdentity>,
        offset: u64,
        size: u32,
    ) -> FsResult<ReadOutcome> {
        if size == 0 {
            return Ok(ReadOutcome::Ready(Vec::new()));
        }

        // L1 range cache first (C-4.1.1): the key is content identity + file +
        // offset + length. The content-scoped, never-reused `file_id` carries
        // the content identity, so an L1 hit is answered without touching the
        // state database at all. `size` stays in the key so a smaller cached
        // read cannot satisfy a larger request with fewer bytes.
        let cache_key = format!("{}:{}:{}", file_id, offset, size);
        {
            let cache = self
                .torrent_data_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.l1_hit();
                return Ok(ReadOutcome::Ready(cached.clone()));
            }
        }
        self.metrics.l1_miss();

        // The identity was captured by the projection that created this
        // inode, from the same statement that produced the bytes it names, so
        // a read never loads the torrent's file list (C-11.4 / F2) and its
        // content token cannot disagree with the info-hash.
        let info_hash = identity.info_hash.as_ref();
        let file_index = identity.file_index;

        if let Some(ds) = &self.download_service {
            // L3 (metadata) cache: parsed `TorrentInfo` keyed by info_hash,
            // avoiding a DB re-read + bencode re-parse on every read. The
            // lookup is content-scoped: the source row may already point at
            // replacement bytes while this inode still belongs to the old
            // content.
            let info = self.get_or_parse_torrent_info(&identity)?;

            // Fast pre-check: are all needed pieces already on disk?
            match ds.pieces_on_disk(&info, file_index, offset, size) {
                Ok(true) => {
                    self.metrics.l2_hit();
                    match ds.read_file_range(info.clone(), file_index, offset, size) {
                        Ok(data) => {
                            // `pieces_on_disk` said on-disk but `read_file_range`
                            // returned empty: defer to the download engine. Both
                            // sub-scenarios converge on ENODATA in the deferred
                            // worker — no seeder waits the 30s window then
                            // surfaces `NoPeers` (`resolve_error`), while a
                            // file_offset mismatch returns `Ok(Vec::new())`
                            // immediately (`resolve_or_enodata`), no 30s wait.
                            if guard_empty_read(&data, size).is_err() {
                                warn!(
                                    "Sync read returned 0 bytes for non-zero \
                                     size (torrent_id={}, file_id={}, size={}); \
                                     deferring to download path",
                                    torrent_id, file_id, size
                                );
                                self.metrics.deferred_read();
                                return Ok(ReadOutcome::Pending {
                                    info: info.clone(),
                                    file_index,
                                    offset,
                                    size,
                                    info_hash: info_hash.to_string(),
                                    torrent_id,
                                });
                            }
                            // populate the L1 range cache so a
                            // repeated read of this exact range is served
                            // from RAM, bypassing L2 disk + metadata fsync.
                            // Bounded: when the cache is full, clear all
                            // entries before inserting (review issue 2) —
                            // coarse but bounded, the L1 cache is a hot-zone
                            // optimization and clearing it only causes a few
                            // L2 disk re-reads.
                            if !data.is_empty() {
                                if let Ok(mut cache) = self.torrent_data_cache.lock() {
                                    if cache.len() >= MAX_L1_ENTRIES {
                                        cache.clear();
                                    }
                                    cache.insert(cache_key.clone(), data.clone());
                                }
                            }
                            // `info!` here printed ~2 lines per 128 KiB chunk, so a
                            // single `cat`/`cp` of a large seed produced hundreds of
                            // thousands of identical lines.  Read activity is observable
                            // in `.stats` (per-layer read counts: Cache L1/L2/L3 hits,
                            // deferred reads); byte totals are not tracked — per-chunk
                            // detail stays at DEBUG.
                            debug!(
                                "Read {} bytes from torrent file (torrent_id={}, file_id={}, cache=L2)",
                                data.len(),
                                torrent_id,
                                file_id
                            );
                            Ok(ReadOutcome::Ready(data))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to read from torrent file (torrent_id={}, file_id={}): {}. \
                                 The torrent may have no active peers/seeds. \
                                 Check tracker health with `cat .stats`.",
                                torrent_id, file_id, e
                            );
                            Err(e.into())
                        }
                    }
                }
                Ok(false) => {
                    self.metrics.l2_miss();
                    self.metrics.deferred_read();
                    debug!(
                        "Deferring read for torrent file (torrent_id={}, file_id={}): \
                         pieces not on disk, blocking in worker thread",
                        torrent_id, file_id
                    );
                    Ok(ReadOutcome::Pending {
                        info,
                        file_index,
                        offset,
                        size,
                        info_hash: info_hash.to_string(),
                        torrent_id,
                    })
                }
                Err(e) => {
                    error!(
                        "Failed to check pieces on disk (torrent_id={}, file_id={}): {:?}",
                        torrent_id, file_id, e
                    );
                    Err(e.into())
                }
            }
        } else {
            error!("Download manager not available");
            Err(FsError::Internal(
                "download manager not available".to_string(),
            ))
        }
    }

    /// Return the parsed `TorrentInfo` for `info_hash`, hitting the L3
    /// (metadata) cache when possible.
    ///
    /// On a miss this reads the raw `.torrent` bytes from the DB once and
    /// parses them once; subsequent reads for the same torrent reuse the
    /// cached `Arc<TorrentInfo>` and skip both the DB re-read and the
    /// bencode re-parse.
    /// Parsed metadata of the content a read identity was minted for.
    ///
    /// The L3 key and the database lookup both use the identity's content,
    /// never the source row: a replacement can already have repointed the row
    /// to different bytes while this inode still belongs to the old content.
    /// A parsed hash that disagrees with the identity is reported as `EIO`
    /// and cached in neither L1 nor L3.
    fn get_or_parse_torrent_info(&self, identity: &FileIdentity) -> FsResult<Arc<TorrentInfo>> {
        let info_hash = identity.info_hash.as_ref();
        {
            let cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(info) = cache.get(info_hash) {
                self.metrics.l3_hit();
                return Ok(info.clone());
            }
        }

        self.metrics.l3_miss();
        let info = Arc::new(
            TorrentInfo::from_bytes(self.get_torrent_raw_data(identity)?).map_err(|e| {
                error!("Failed to parse torrent info for download: {:?}", e);
                FsError::Internal(format!("failed to parse torrent info for download: {e:?}"))
            })?,
        );

        let parsed_hash = info.info_hash().map_err(|e| {
            error!("Failed to read parsed torrent info hash: {:?}", e);
            FsError::Internal(format!("failed to read parsed torrent info hash: {e:?}"))
        })?;
        if hex::encode(parsed_hash) != info_hash {
            error!(
                "Torrent metadata mismatch: identity expects info hash {} but parsed {}",
                info_hash,
                hex::encode(parsed_hash)
            );
            return Err(FsError::Io(format!(
                "torrent metadata for info hash {info_hash} does not describe that content"
            )));
        }

        {
            let mut cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if cache.len() >= MAX_L3_ENTRIES {
                // Bounded cache (the contract leaves the bound to the
                // implementation): clear instead of growing without limit.
                cache.clear();
            }
            cache.insert(info_hash.to_string(), info.clone());
        }

        Ok(info)
    }

    /// Prepare every file identity of one source row before the read path
    /// needs them (a lookup/readdir of the torrent's data tree). Best-effort:
    /// a failure leaves the affected inodes without an identity, so their
    /// reads fail with `EIO` instead of falling back to a table load.
    fn ensure_file_identities(&self, source_id: i64) {
        if let Some(ts) = &self.torrent_service {
            if let Err(e) = ts.prepare_file_identities(source_id) {
                warn!(
                    "Failed to prepare file identities for torrent_id={}: {:?}",
                    source_id, e
                );
            }
        }
    }

    /// Raw `.torrent` bytes of the content `identity` was minted for.
    ///
    /// The content id is the authoritative pointer. The DB exposes source-row
    /// accessors only, so the identity's info hash narrows the candidate rows
    /// and [`Torrent::torrent_id`] selects the exact content; a row that has
    /// already moved to other bytes is never read. Missing content is `EIO`:
    /// the inode belongs to superseded content, not to a retryable miss.
    fn get_torrent_raw_data(&self, identity: &FileIdentity) -> FsResult<Vec<u8>> {
        let db = DataResolver::get_db(&self.db)?;
        let stored = {
            let db_guard = db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;
            let candidates = db_guard
                .get_torrents_by_infohash(identity.info_hash.as_ref())
                .map_err(|e| {
                    error!("Failed to look up torrent metadata by info hash: {:?}", e);
                    FsError::from(e)
                })?;
            let mut matched = None;
            for (source_id, ..) in candidates {
                let torrent = db_guard.get_torrent_by_id(source_id).map_err(|e| {
                    error!("Failed to get torrent {}: {:?}", source_id, e);
                    FsError::from(e)
                })?;
                if let Some(torrent) = torrent {
                    if torrent.torrent_id == identity.content_id {
                        matched = Some(torrent);
                        break;
                    }
                }
            }
            matched
        };

        if let Some(torrent) = stored {
            if let Some(data) = torrent.torrent_data {
                if !data.is_empty() {
                    return Ok(data);
                }
            }
        }

        for data in self.inode_mgr.inodes.values() {
            if let InodeData::File {
                name,
                data: file_data,
                unlinked,
                ..
            } = data
            {
                // skip unlinked-but-open inodes — their torrent_id
                // was already removed from the DB by `unlink`, so matching
                // their buffered bytes here would be a stale dirty read.
                if !*unlinked && name.ends_with(".torrent") && !file_data.is_empty() {
                    if let Ok(info) = TorrentInfo::from_bytes(file_data.clone()) {
                        if let Ok(metadata) = info.metadata() {
                            if hex::encode(metadata.info_hash) == identity.info_hash.as_ref() {
                                return Ok(file_data.clone());
                            }
                        }
                    }
                }
            }
        }

        // A close whose persist has not landed yet moved its bytes into the
        // shared buffer map; the inode itself is empty by then.
        for bytes in self.buffered_torrents.values() {
            if let Ok(info) = TorrentInfo::from_bytes(bytes.as_slice().to_vec()) {
                if let Ok(metadata) = info.metadata() {
                    if hex::encode(metadata.info_hash) == identity.info_hash.as_ref() {
                        return Ok(bytes.as_slice().to_vec());
                    }
                }
            }
        }

        Err(FsError::Io(format!(
            "torrent metadata for info hash {} is no longer available",
            identity.info_hash
        )))
    }

    // ── Stats namespace ─────────────────────────────────────────────────────

    /// Render the requested `.stats` content.
    pub fn read_stats(&self, kind: StatsKind) -> Vec<u8> {
        match kind {
            StatsKind::Global => self.generate_global_stats_content(),
            StatsKind::StatsInode { ino } => self.generate_data_stats_for_ino(ino),
        }
    }

    fn generate_global_stats_content(&self) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        let session_stats = self.download_service.as_ref().map(|ds| ds.snapshot_stats());
        // report the current L1 range cache depth so `.stats`
        // reflects the now-populated memory cache.
        let l1_entries = self
            .torrent_data_cache
            .lock()
            .map(|c| c.len() as u64)
            .unwrap_or(0);
        self.metrics.set_l1_entries(l1_entries);
        generate_global_stats(
            self.inode_mgr.creation_time,
            &self.db,
            session_stats,
            get_cm,
            &self.listen_addr,
            Some(self.metrics.snapshot()),
        )
    }

    fn generate_dir_stats_content(&self, source_path: &str) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        generate_directory_stats(source_path, &self.db, &self.download_service, get_cm)
    }

    fn generate_torrent_stats_for_id(&self, torrent_id: i64) -> Vec<u8> {
        // Extract info_hash inside a short-lived lock scope, then release
        // before calling generate_torrent_stats() which takes its own lock.
        // The lookup stays non-blocking (C-6.5.1 / F6): the render sits on the
        // dispatch thread, so a contended state lock degrades the output
        // instead of stalling behind an ingest transaction.
        let info_hash = {
            let Some(db) = self.db.as_ref() else {
                return b"Database not available\n".to_vec();
            };
            let db_guard = match db.try_lock() {
                Ok(g) => g,
                Err(_) => return super::stats::locked_rendering(),
            };
            match db_guard.get_torrent_by_id(torrent_id).ok().flatten() {
                Some(t) => t.info_hash,
                None => return format!("Torrent not found (id={})\n", torrent_id).into_bytes(),
            }
        };
        let get_cm = || self.get_cache_manager();
        generate_torrent_stats(
            torrent_id,
            &info_hash,
            &self.db,
            &self.download_service,
            get_cm,
        )
    }

    fn generate_data_stats_for_ino(&self, ino: u64) -> Vec<u8> {
        let dir_ino = match InodeManager::stats_ino_to_dir_ino(ino) {
            Some(d) => d,
            None => return b"Invalid stats inode\n".to_vec(),
        };

        if dir_ino == DATA_INO {
            return self.generate_dir_stats_content("");
        }

        match self.inode_mgr.data_inodes.get(&dir_ino) {
            Some(DataInode::SourcePathDir { path }) => self.generate_dir_stats_content(path),
            Some(DataInode::TorrentRoot { torrent_id, .. }) => {
                self.generate_torrent_stats_for_id(*torrent_id)
            }
            _ => b"Stats not available for this inode\n".to_vec(),
        }
    }
}

/// Background SHA-1 verification of on-disk cached pieces. After a restart,
/// [`CacheManager::scan_pieces_subdirectory`] re-registers every piece
/// unverified (it may be complete or a crash-leftover); this worker recomputes
/// each SHA-1 against the torrent's expected hash, marking matches verified
/// and purging mismatches/incomplete pieces so they re-download on demand.
/// Pieces still being written (`.incomplete` marker) are excluded via
/// `unverified_pieces`. Runs detached so it never blocks mount readiness.
fn spawn_cache_verification(
    cache: Arc<Mutex<CacheManager>>,
    infos_by_hash: HashMap<String, Arc<TorrentInfo>>,
) {
    let spawned = std::thread::Builder::new()
        .name("cache-verify".to_string())
        .spawn(move || {
            let unverified = match cache.lock() {
                Ok(c) => c.unverified_pieces(),
                Err(_) => {
                    warn!("Cache lock poisoned during piece verification");
                    return;
                }
            };

            let mut verified = 0usize;
            let mut purged = 0usize;
            let mut skipped = 0usize;

            for piece_key in unverified {
                let (info_hash, piece_index) = match split_piece_key(&piece_key) {
                    Some(v) => v,
                    None => {
                        warn!("Skipping malformed piece key: {}", piece_key);
                        skipped += 1;
                        continue;
                    }
                };

                let info = match infos_by_hash.get(info_hash) {
                    Some(info) => info,
                    None => {
                        // Torrent no longer present in the DB: leave untouched.
                        skipped += 1;
                        continue;
                    }
                };

                // Resolve the on-disk path inside a brief lock, then do all
                // file I/O lock-free so active reads (PieceStore /
                // DownloadService / .stats — all take the same cache mutex)
                // are never blocked by the verification thread.
                let path = match cache.lock() {
                    Ok(c) => c.piece_path(&piece_key),
                    Err(_) => {
                        warn!("Cache lock poisoned during piece verification");
                        return;
                    }
                };
                let Some(path) = path else {
                    warn!("Skipping piece verification for invalid cache key {piece_key}");
                    skipped += 1;
                    continue;
                };

                let outcome = verify_single_piece(&path, piece_index, info, &piece_key);

                match outcome {
                    VerifyOutcome::Verified => {
                        if let Ok(mut c) = cache.lock() {
                            c.mark_verified(&piece_key);
                        }
                        verified += 1;
                    }
                    VerifyOutcome::Purged => {
                        if let Ok(mut c) = cache.lock() {
                            let _ = c.delete_piece(&piece_key);
                        }
                        purged += 1;
                    }
                    VerifyOutcome::Skipped => {
                        skipped += 1;
                    }
                }
            }

            info!(
                "Cache piece verification complete: {} verified, {} purged, {} skipped",
                verified, purged, skipped
            );
        });

    if let Err(e) = spawned {
        warn!("Failed to spawn cache verification thread: {}", e);
    }
}

/// Outcome of verifying a single cached piece.
enum VerifyOutcome {
    Verified,
    Purged,
    Skipped,
}

/// Verify a single on-disk cached piece against its expected SHA-1 hash.
///
/// Pure file I/O — no cache lock is held.  The caller resolves the on-disk
/// `path` (and `piece_index`) from the piece key inside a brief lock, then
/// calls this function lock-free.  The caller is responsible for applying
/// the side effect (mark verified / delete piece).
fn verify_single_piece(
    path: &std::path::Path,
    piece_index: i32,
    info: &TorrentInfo,
    piece_key: &str,
) -> VerifyOutcome {
    let expected = match info.hash_for_piece(piece_index) {
        Some(h) => h,
        None => return VerifyOutcome::Skipped,
    };
    let expected_size = match info.piece_size(piece_index) {
        Some(s) => s,
        None => return VerifyOutcome::Skipped,
    };

    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => {
            warn!("Cached piece file missing, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };

    let file_size = meta.len();
    if file_size != expected_size {
        warn!(
            "Cached piece has wrong size ({} != {}), purging incomplete piece: {}",
            file_size, expected_size, piece_key
        );
        return VerifyOutcome::Purged;
    }

    // a piece file may have the correct *logical* size
    // but still be incomplete — `write_piece` writes blocks at arbitrary
    // offsets, and a crash between block writes leaves a sparse file whose
    // zero-filled gaps make st_size match piece_length while the physical
    // allocation is smaller. Purge it: an incomplete piece left by a crash
    // is garbage and the space should be reclaimed. It will be re-downloaded
    // on demand, identical to a never-downloaded piece.
    if is_sparse_file(&meta, file_size) {
        warn!(
            "Cached piece is sparse (partial write), purging incomplete piece: {}",
            piece_key
        );
        return VerifyOutcome::Purged;
    }

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => {
            warn!("Cached piece file disappeared, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };
    let actual = Sha1::from(&data[..]).digest().bytes();
    if actual == expected {
        VerifyOutcome::Verified
    } else {
        warn!(
            "Cached piece failed SHA-1 verification, purging: {}",
            piece_key
        );
        VerifyOutcome::Purged
    }
}

/// Split a piece key of the form `{info_hash}:piece:{index}` into its parts.
fn split_piece_key(key: &str) -> Option<(&str, i32)> {
    let mut parts = key.split(':');
    let info_hash = parts.next()?;
    if parts.next()? != "piece" {
        return None;
    }
    let index = parts.next()?.parse::<i32>().ok()?;
    Some((info_hash, index))
}

/// Whether a piece file is *sparse* (physical allocation < logical size).
/// `write_piece` writes blocks at arbitrary offsets; a crash between writes
/// leaves `st_size` correct but zero-filled gaps the filesystem never
/// allocated — not a complete piece. Compare `st_blocks * 512` (physical)
/// against `st_size` (logical) to detect it; an all-zero piece also looks
/// sparse and is purged too, then re-downloaded and re-verified with no data
/// loss. Takes the already-fetched `Metadata` to avoid a duplicate `stat`.
fn is_sparse_file(meta: &std::fs::Metadata, logical_size: u64) -> bool {
    if logical_size == 0 {
        return false;
    }
    use std::os::unix::fs::MetadataExt;
    let physical = meta.blocks() * 512;
    physical < logical_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::{HashMap, HashSet};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::db::{Database, FileEntry, InsertTorrentResult};
    use crate::infrastructure::metrics::Metrics;
    use crate::metadata::TorrentInfo;
    use crate::services::torrent::TorrentService;

    use crate::fuse::inodes::InodeManager;

    /// Minimal single-file bencode so `TorrentInfo::from_bytes` parses.
    fn minimal_torrent_bytes() -> Vec<u8> {
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi16e");
        t.extend_from_slice(b"4:name3:foo");
        t.extend_from_slice(b"12:piece lengthi16384e");
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&[0u8; 20]);
        t.extend_from_slice(b"ee");
        t
    }

    /// A single-file torrent whose `name` field (and therefore info-hash)
    /// differs per call — lets tests overwrite one torrent with a distinct
    /// one and assert the stored bytes actually changed.
    fn minimal_torrent_named(name: &str) -> Vec<u8> {
        assert!(!name.is_empty(), "name must be non-empty");
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi16e");
        t.extend_from_slice(b"4:name");
        t.extend_from_slice(name.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(name.as_bytes());
        t.extend_from_slice(b"12:piece lengthi16384e");
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&[0u8; 20]);
        t.extend_from_slice(b"ee");
        t
    }

    fn service_with_torrent(torrent_bytes: &[u8]) -> (FsService, String, i64, Arc<Metrics>) {
        let info = TorrentInfo::from_bytes(torrent_bytes.to_vec()).expect("parse torrent");
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        drop(info);

        let mut db = Database::open_in_memory().expect("in-memory db");
        let torrent_id = match db
            .insert_torrent("src", "foo", "foo.torrent", 16, &info_hash, 1)
            .expect("insert torrent")
        {
            InsertTorrentResult::Inserted(id) => id,
            other => panic!("unexpected insert result: {:?}", other),
        };
        db.set_torrent_data(torrent_id, torrent_bytes)
            .expect("set torrent data");

        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let torrent_service = TorrentService::new(db_arc.clone(), None);
        let content_versions = torrent_service.content_versions();
        let svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(torrent_service),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            processing_torrents_cv: Arc::new(Condvar::new()),
            download_service: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics: metrics.clone(),
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions,
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        };
        (svc, info_hash, torrent_id, metrics)
    }

    #[test]
    fn torrent_info_cached_by_info_hash() {
        let torrent_bytes = minimal_torrent_bytes();
        let (svc, info_hash, torrent_id, metrics) = service_with_torrent(&torrent_bytes);
        let content_id = {
            let db = svc.db.as_ref().expect("test service has a db");
            let guard = db.lock().expect("db lock");
            guard
                .get_torrent_by_id(torrent_id)
                .expect("read row")
                .expect("row exists")
                .torrent_id
        };
        let identity = identity_for(&info_hash, 0, content_id);

        // First lookup: miss → parse + insert.
        let first = svc.get_or_parse_torrent_info(&identity).unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 0);

        // Second lookup: hit → same Arc, no re-parse.
        let second = svc.get_or_parse_torrent_info(&identity).unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn split_piece_key_parses_info_hash_and_index() {
        assert_eq!(
            split_piece_key("abcdef0123456789:piece:42"),
            Some(("abcdef0123456789", 42))
        );
        // Non-`piece` segment or missing index are rejected.
        assert_eq!(split_piece_key("abc:data:0"), None);
        assert_eq!(split_piece_key("abc:piece:x"), None);
        assert_eq!(split_piece_key("abc"), None);
        assert_eq!(split_piece_key(""), None);
    }

    /// an unwritable cache directory must leave the download
    /// engine unset (`download_service == None`) instead of crashing or
    /// silently mounting a filesystem that can only browse metadata.  `main`
    /// turns this `None` into a fatal exit before mounting, so the behavior
    /// is observable here at the construction-failure path.
    #[test]
    fn unwritable_cache_dir_yields_no_download_service() {
        let dir = tempfile::TempDir::new().expect("temp dir");
        // A regular file where the cache directory component should be: the
        // engine's `create_dir_all(<cache>/pieces)` fails with ENOTDIR, so the
        // download service is left unset.
        let blocker = dir.path().join("blocker");
        std::fs::write(&blocker, b"").expect("write blocker");

        let svc = FsService::new_with_cache_path(blocker, &TorrentfsConfig::default_config());
        assert!(
            svc.download_service.is_none(),
            "unwritable cache dir must leave download_service unset"
        );
    }

    /// Bare service without any torrents — sufficient for testing
    /// that mutating operations on the read-only `data/` namespace return
    /// `EROFS` (`ReadOnlyFileSystem`), not `ENOENT` or `EACCES`.
    fn bare_service() -> FsService {
        let metrics = Arc::new(Metrics::new());
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions: Arc::new(Mutex::new(HashMap::new())),
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        }
    }

    fn test_identity(file_index: i32, content_id: i64) -> Arc<FileIdentity> {
        Arc::new(FileIdentity {
            info_hash: Arc::from("0123456789abcdef0123456789abcdef01234567"),
            file_index,
            content_id,
        })
    }

    fn identity_for(info_hash: &str, file_index: i32, content_id: i64) -> Arc<FileIdentity> {
        Arc::new(FileIdentity {
            info_hash: Arc::from(info_hash),
            file_index,
            content_id,
        })
    }

    #[test]
    fn data_namespace_write_returns_erofs() {
        let mut svc = bare_service();
        // DATA_INO (the `data/` root) is a directory → would be EISDIR
        // without the read-only guard, but the guard fires first.
        let err = svc.write(DATA_INO, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A data file inode (not in `inodes`, only in `data_inodes`) would
        // previously fall through to the `None` arm → `NotFound` → ENOENT.
        let data_file_ino = DATA_FILE_INO_BASE + 1;
        let err = svc.write(data_file_ino, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// `setattr` (chmod/chown/truncate/utimens) on the read-only
    /// `data/` namespace must return `EROFS`. Before this guard, `getattr`
    /// returned the current attributes (exit 0) and left the mode untouched —
    /// a silent success that differed from `write`/`rmdir`/`unlink`/`rename`.
    #[test]
    fn data_namespace_setattr_returns_erofs() {
        let mut svc = bare_service();

        let err = svc.setattr(DATA_INO, None).unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A data file inode lives in `data_inodes`, not `inodes`; without the
        // guard `getattr` would resolve it and return its attributes.
        let data_file_ino = DATA_FILE_INO_BASE + 1;
        let err = svc.setattr(data_file_ino, None).unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// an *attribute change* (chmod/chown/utimens) on the
    /// `metadata/` namespace must return `EPERM` — never a silent success.
    /// `metadata/` is writable (create/write/rename/unlink work), so `EROFS`
    /// would be misleading; `EPERM` reflects "attributes are virtual and
    /// immutable". A truncate (size change) is *not* an attribute change —
    /// it must succeed (see `metadata_file_truncate_succeeds`).
    #[test]
    fn metadata_namespace_setattr_returns_eperm() {
        let mut svc = bare_service();

        let err = svc.setattr(METADATA_INO, None).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // A metadata/ file (writable .torrent buffer, but attributes are
        // virtual and immutable) must also reject an attribute change.
        let ino = svc
            .create(METADATA_INO, "ubuntu.iso.torrent")
            .expect("create metadata file")
            .attr
            .ino;
        let err = svc.setattr(ino, None).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);
    }

    /// a *truncate* (size change) on a `metadata/` file is a
    /// legitimate write — `cp` overwriting an existing `.torrent` opens it
    /// with `O_TRUNC`, which the kernel turns into `setattr(size=0)`. That
    /// must resize the in-memory buffer and succeed, not return `EPERM`.
    #[test]
    fn metadata_file_truncate_succeeds() {
        let mut svc = bare_service();
        let ino = svc
            .create(METADATA_INO, "debian.torrent")
            .expect("create metadata file")
            .attr
            .ino;
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write torrent bytes");

        // Truncate to 0 (the O_TRUNC overwrite path) must succeed.
        let attr = svc.setattr(ino, Some(0)).expect("truncate must succeed");
        assert_eq!(attr.size, 0);

        // The in-memory buffer is cleared.
        let file_data = match svc.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File { data, .. }) => data,
            other => panic!("expected File inode, got {:?}", other),
        };
        assert!(
            file_data.is_empty(),
            "truncate to 0 must clear buffered bytes"
        );

        // A subsequent write repopulates from offset 0 (no stale tail).
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("rewrite after truncate");
        let file_data = match svc.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File { data, .. }) => data,
            other => panic!("expected File inode, got {:?}", other),
        };
        assert_eq!(file_data, &minimal_torrent_bytes());
    }

    /// truncating to `MAX_TORRENT_SIZE + 1` must be
    /// rejected with `FileTooLarge` (→ EFBIG), mirroring the write-path
    /// guard — never a silent cap or an over-limit allocation.
    #[test]
    fn truncate_beyond_max_size_returns_file_too_large() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "big.torrent");
        let over = MAX_TORRENT_SIZE as u64 + 1;
        let err = svc.setattr(ino, Some(over)).unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
        // Buffer untouched (still empty).
        assert!(svc
            .inode_mgr
            .inodes
            .get(&ino)
            .and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.is_empty()),
                _ => None,
            })
            .unwrap());
    }

    /// truncating to exactly `MAX_TORRENT_SIZE` is the
    /// positive boundary the reject test hovers around — the guard uses
    /// strict `>`, so the limit itself must be accepted.
    #[test]
    fn truncate_to_exact_max_size_is_allowed() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "max.torrent");
        let attr = svc
            .setattr(ino, Some(MAX_TORRENT_SIZE as u64))
            .expect("truncate at limit");
        assert_eq!(attr.size, MAX_TORRENT_SIZE as u64);
        assert_eq!(
            svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.len()),
                _ => None,
            }),
            Some(MAX_TORRENT_SIZE)
        );
    }

    /// truncating to a *larger* size (grow) must
    /// zero-fill the extended region and make `getattr` report the new size;
    /// a subsequent write past the grown tail must still append correctly.
    #[test]
    fn truncate_grow_zero_fills_and_reflects_new_size() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "grow.torrent");
        svc.write(ino, 0, b"AB").expect("write 2 bytes");

        // Grow from 2 → 4: the [2..4] region must be zero-filled.
        let attr = svc.setattr(ino, Some(4)).expect("grow truncate");
        assert_eq!(attr.size, 4);
        let data = svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
            InodeData::File { data, .. } => Some(data.clone()),
            _ => None,
        });
        assert_eq!(data.as_deref(), Some(b"AB\0\0".as_slice()));

        // getattr reflects the grown size.
        assert_eq!(svc.getattr(ino).expect("getattr").size, 4);

        // A subsequent write after the grown tail appends normally.
        svc.write(ino, 4, b"CD").expect("write after grow");
        let data = svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
            InodeData::File { data, .. } => Some(data.clone()),
            _ => None,
        });
        assert_eq!(data.as_deref(), Some(b"AB\0\0CD".as_slice()));
    }

    /// the complete host-side `cp`-overwrite sequence on an existing
    /// `metadata/` file. The first `cp` creates the file (create → write →
    /// flush → release); the second `cp` overwrites it (lookup → O_TRUNC
    /// truncate → open → write → flush → release). Every step — especially
    /// the truncate — must succeed, and the stored bytes must be replaced by
    /// the new torrent.
    #[test]
    fn cp_overwrite_existing_torrent_flow() {
        let wait_idle = |svc: &FsService| {
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            loop {
                if svc.processing_torrents.lock().unwrap().is_empty() {
                    return;
                }
                if std::time::Instant::now() > deadline {
                    panic!("processing_torrents not cleaned up after 10s");
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        };

        let mut svc = service_with_db();
        let first = minimal_torrent_named("first");
        let second = minimal_torrent_named("second");
        assert_ne!(first, second);

        // First `cp`: create a new file.
        let created = svc.create(METADATA_INO, "debian.torrent").expect("create");
        let ino = created.attr.ino;
        svc.write(ino, 0, &first).expect("first write");
        svc.flush(ino).expect("first flush");
        svc.release(created.fh).expect("first release");
        wait_idle(&svc);

        // Second `cp`: overwrite the existing file.
        let entry = svc
            .lookup(METADATA_INO, "debian.torrent")
            .expect("lookup")
            .expect("existing file must resolve");
        assert_eq!(entry.ino, ino, "overwrite must target the same inode");

        // `cp` with `O_TRUNC`: the kernel sends `FUSE_OPEN` first, then the
        // `SETATTR(size=0)` truncate (fs/fuse/file.c). Follow that order.
        let fh = match svc.open(ino).expect("open") {
            OpenOutcome { fh, .. } => fh,
        };
        // Truncate to 0 — the regression: must not return EPERM.
        let attr = svc.setattr(ino, Some(0)).expect("truncate must not EPERM");
        assert_eq!(attr.size, 0);
        svc.write(ino, 0, &second).expect("overwrite write");
        svc.flush(ino).expect("overwrite flush");
        svc.release(fh).expect("overwrite release");
        wait_idle(&svc);

        // The DB row now holds the second torrent's bytes.
        let db = svc.db.as_ref().unwrap().lock().unwrap();
        let row = db
            .get_torrent_by_filename_and_source_path("debian.torrent", "")
            .expect("db query")
            .expect("torrent persisted");
        assert_eq!(
            row.torrent_data.as_deref(),
            Some(second.as_slice()),
            "overwrite must replace the stored bytes"
        );
    }

    /// Concurrent host-side overwrites of an existing `metadata/` file must
    /// never surface `EPERM`; each writer does the full `cp`-overwrite under
    /// a shared mutex. The mutex mirrors production: the FUSE dispatcher is
    /// single-threaded (`&mut self` on every `Filesystem` method), so host
    /// requests are serialized at exactly this boundary. The test asserts the
    /// serialized truncate→write sequence never returns `EPERM` and always
    /// leaves one writer's full payload (torn writes are precluded by design).
    #[test]
    fn concurrent_overwrite_existing_torrent_never_eperm() {
        let svc = Arc::new(Mutex::new(bare_service()));

        // Prime an existing file (the first `cp`).
        {
            let mut s = svc.lock().unwrap();
            let created = s.create(METADATA_INO, "debian.torrent").expect("create");
            s.write(created.attr.ino, 0, &minimal_torrent_bytes())
                .expect("prime write");
            s.release(created.fh).expect("prime release");
        }

        const WRITERS: usize = 4;
        const ITERATIONS: usize = 20;
        let barrier = Arc::new(std::sync::Barrier::new(WRITERS));
        let payload = minimal_torrent_named("concurrent");
        let mut handles = Vec::new();

        for _ in 0..WRITERS {
            let svc = Arc::clone(&svc);
            let barrier = Arc::clone(&barrier);
            let payload = payload.clone();
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                for _ in 0..ITERATIONS {
                    let mut s = svc.lock().unwrap();
                    let entry = s
                        .lookup(METADATA_INO, "debian.torrent")
                        .expect("lookup")
                        .expect("file exists");
                    let ino = entry.ino;
                    // O_TRUNC truncate — the regression point.
                    s.setattr(ino, Some(0)).expect("truncate must not EPERM");
                    let fh = match s.open(ino).expect("open") {
                        OpenOutcome { fh, .. } => fh,
                    };
                    s.write(ino, 0, &payload).expect("write");
                    s.flush(ino).expect("flush");
                    s.release(fh).expect("release");
                }
            }));
        }
        for h in handles {
            h.join().expect("writer thread panicked");
        }

        // Final content is exactly one full payload (no torn/partial write).
        let mut s = svc.lock().unwrap();
        let entry = s
            .lookup(METADATA_INO, "debian.torrent")
            .expect("lookup")
            .expect("file exists");
        // `release` moves the buffer into the shared handle now, so the current
        // bytes live in whichever of the inode / parked Arc holds them.
        let data = s
            .buffered_torrent_bytes(entry.ino)
            .expect("file inode has buffered bytes")
            .to_vec();
        assert_eq!(data, payload);
    }

    /// the root directory and the `.stats` virtual files have
    /// fixed modes (0o555 / 0o444) just like `metadata/`, so `setattr` on
    /// them must also return `EPERM` instead of silently returning the
    /// current attributes (exit 0) while leaving the mode untouched.
    #[test]
    fn root_and_stats_setattr_return_eperm() {
        let mut svc = bare_service();

        let err = svc.setattr(ROOT_INO, None).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        let err = svc.setattr(STATS_INO, None).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // A derived stats inode is not part of the read-only projection; it
        // must be classified as stats and rejected with EPERM, never EROFS.
        let stats_ino = InodeManager::make_stats_ino(DATA_INO);
        assert!(InodeManager::is_stats_ino(stats_ino));
        let err = svc.setattr(stats_ino, None).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);
    }

    /// `write` on the `.stats` virtual files must return `EPERM`
    /// (`NotPermitted`) — their attributes are virtual and immutable, not a
    /// read-only namespace — matching `setattr` and `symlink`.
    /// Before the change it wrongly returned `EROFS`.
    #[test]
    fn stats_write_returns_eperm() {
        let mut svc = bare_service();

        let err = svc.write(STATS_INO, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // A derived stats inode is not part of the read-only projection; it
        // must be classified as stats and rejected with EPERM, never EROFS.
        let stats_ino = InodeManager::make_stats_ino(DATA_INO);
        assert!(InodeManager::is_stats_ino(stats_ino));
        let err = svc.write(stats_ino, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::NotPermitted);
    }

    /// `unlink` (`rm`) on the `.stats` virtual files must return
    /// `EPERM` (`NotPermitted`), matching `write` — not `EACCES`
    /// (the root `.stats` previously fell through the metadata-child guard)
    /// nor `EROFS` (the `data/` subtree `.stats` previously hit the data/
    /// guard).
    #[test]
    fn stats_unlink_returns_eperm() {
        let mut svc = bare_service();

        // Root `.stats` (STATS_INO, parent ROOT_INO).
        let err = svc.unlink(ROOT_INO, ".stats").unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // `data/` subtree `.stats` (derived stats inode, parent DATA_INO).
        let err = svc.unlink(DATA_INO, ".stats").unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // No side effects: the root `.stats` inode must still exist.
        assert!(svc.inode_mgr.inodes.contains_key(&STATS_INO));
    }

    /// Helper: create an empty writable `.torrent` inode via the public
    /// `create()` path (not direct `inodes` manipulation) and return its
    /// ino. Using the real path keeps the test honest about the
    /// `InodeData::File` layout without duplicating its constructor.
    fn writable_file_ino(svc: &mut FsService, name: &str) -> u64 {
        svc.create(METADATA_INO, name)
            .expect("create writable file")
            .attr
            .ino
    }

    /// a negative offset must be rejected before any allocation
    /// rather than cast to a huge `usize` and fed to `Vec::resize` (OOM).
    #[test]
    fn write_rejects_negative_offset() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "f.torrent");
        let err = svc.write(ino, -1, b"x").unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
        // No partial allocation: the file is still empty.
        assert_eq!(
            svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.len()),
                _ => None,
            }),
            Some(0)
        );
    }

    /// an offset that would grow the buffer past MAX_TORRENT_SIZE
    /// is rejected with `FileTooLarge` (→ EFBIG) instead of allocating.
    #[test]
    fn write_rejects_offset_beyond_max_size() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "big.torrent");
        let over = MAX_TORRENT_SIZE as i64 + 1;
        let err = svc.write(ino, over, b"x").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
        // Buffer untouched.
        assert!(svc
            .inode_mgr
            .inodes
            .get(&ino)
            .and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.is_empty()),
                _ => None,
            })
            .unwrap());
    }

    /// `offset + data.len()` exceeding `MAX_TORRENT_SIZE` (even
    /// when offset alone is within bounds) is rejected, not silently capped.
    #[test]
    fn write_rejects_end_beyond_max_size() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "edge.torrent");
        // Offset alone is within the limit, but offset + 2 pushes end past
        // it — no pre-seeded data needed.
        let base = MAX_TORRENT_SIZE - 1;
        let err = svc.write(ino, base as i64, b"xy").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
    }

    /// a write whose `end` lands exactly on MAX_TORRENT_SIZE is
    /// allowed (the guard uses strict `>`, mirroring `flush`'s `>`). This
    /// is the positive boundary the reject tests hover around.
    #[test]
    fn write_at_exact_max_size_is_allowed() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "max.torrent");
        // MAX_TORRENT_SIZE bytes from offset 0 → end == MAX_TORRENT_SIZE.
        let payload = vec![0u8; MAX_TORRENT_SIZE];
        let n = svc.write(ino, 0, &payload).expect("write at limit");
        assert_eq!(n, MAX_TORRENT_SIZE as u32);
        assert_eq!(
            svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.len()),
                _ => None,
            }),
            Some(MAX_TORRENT_SIZE)
        );
    }

    /// a normal write at a positive offset still works and grows
    /// the buffer with a zero gap (regression guard for the guard logic).
    #[test]
    fn write_at_positive_offset_grows_with_gap() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "gap.torrent");
        let n = svc.write(ino, 4, b"AB").expect("write ok");
        assert_eq!(n, 2);
        let data = svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
            InodeData::File { data, .. } => Some(data.clone()),
            _ => None,
        });
        assert_eq!(data.as_deref(), Some(b"\0\0\0\0AB".as_slice()));
    }

    /// a huge positive offset (would cast to a multi-GB `usize`
    /// and trigger a runaway `Vec::resize`) is rejected by the size cap
    /// before any allocation. This is the local-DoS vector the guard closes.
    #[test]
    fn write_rejects_huge_positive_offset() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "big.torrent");
        // Far beyond MAX_TORRENT_SIZE but still a valid positive i64.
        let huge = MAX_TORRENT_SIZE as i64 * 2;
        let err = svc.write(ino, huge, b"x").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
        // No allocation happened.
        assert!(svc
            .inode_mgr
            .inodes
            .get(&ino)
            .and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.is_empty()),
                _ => None,
            })
            .unwrap());
    }

    #[test]
    fn data_namespace_create_mknod_mkdir_return_erofs() {
        let mut svc = bare_service();

        let err = svc.create(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mknod(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mkdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A nested data inode as parent.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.mkdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// Writing a file without the `.torrent` suffix into `metadata/` is an
    /// invalid name, not a permission failure: the user must name the file
    /// `.torrent`, so `create`/`mknod`/`unlink` reject it with
    /// `InvalidArgument` → EINVAL, not `PermissionDenied` → EACCES.
    #[test]
    fn metadata_suffix_rejection_returns_einval_not_eacces() {
        let mut svc = bare_service();

        let err = svc.create(METADATA_INO, "no-suffix").unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);

        let err = svc.mknod(METADATA_INO, "no-suffix").unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);

        let err = svc.unlink(METADATA_INO, "no-suffix").unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
    }

    /// Dropping the `.torrent` suffix on a rename is the same invalid-name
    /// rejection as `create` — `InvalidArgument` → EINVAL, not EACCES.
    #[test]
    fn metadata_rename_suffix_drop_returns_einval_not_eacces() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "keep.torrent").expect("create");

        let err = svc
            .rename(METADATA_INO, "keep.torrent", METADATA_INO, "dropped")
            .unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);

        // The inode is untouched and still resolvable under its old name.
        assert!(
            svc.inode_mgr
                .find_child_by_name(METADATA_INO, "keep.torrent")
                == Some(created.attr.ino)
        );
    }

    /// `rmdir`/`unlink` on the read-only `data/` namespace must return `EROFS`
    /// with no side effect — no inode/`data_inodes` mutation, no cache
    /// eviction. A mutation would corrupt FUSE session state (stale/missing
    /// readdir entries, or a half-removed inode dereference aborting the
    /// session). Pins the early-return contract so a future refactor can't
    /// bypass the guard.
    #[test]
    fn data_namespace_unlink_rmdir_return_erofs() {
        let mut svc = bare_service();

        // Snapshot the inode tables before any EROFS operation.
        let inodes_before = svc.inode_mgr.inodes.len();
        let data_inodes_before = svc.inode_mgr.data_inodes.len();

        let err = svc.unlink(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.rmdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.unlink(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
        let err = svc.rmdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // No side effects — the early-return guard must not
        // mutate inode state, which would corrupt the FUSE session.
        assert_eq!(
            svc.inode_mgr.inodes.len(),
            inodes_before,
            "EROFS path must not mutate the inode table"
        );
        assert_eq!(
            svc.inode_mgr.data_inodes.len(),
            data_inodes_before,
            "EROFS path must not mutate the data_inodes cache"
        );
    }

    #[test]
    fn data_namespace_rename_returns_erofs() {
        let mut svc = bare_service();

        // Rename *into* data/ as the new parent.
        let err = svc
            .rename(METADATA_INO, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Rename *out of* data/ as the source parent.
        let err = svc
            .rename(DATA_INO, "foo", METADATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Both parents in data/ namespace.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc
            .rename(data_dir_ino, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// `symlink` is unsupported everywhere, but the read-only
    /// `data/` namespace must return `EROFS` (matching `write`/`chmod`/
    /// `rmdir`/`unlink`/`rename`), while every other namespace returns
    /// `EPERM`.  Before the guard, `symlink` never reached the service: the
    /// fuser default answered `EPERM` for every parent.
    #[test]
    fn symlink_returns_erofs_in_data_namespace_only() {
        let mut svc = bare_service();

        // data/ root and a nested data inode → EROFS.
        assert_eq!(svc.symlink(DATA_INO), FsError::ReadOnlyFileSystem);
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        assert_eq!(svc.symlink(data_dir_ino), FsError::ReadOnlyFileSystem);

        // metadata/ and a nested metadata directory → EPERM (unsupported).
        assert_eq!(svc.symlink(METADATA_INO), FsError::NotPermitted);
        svc.mkdir(METADATA_INO, "dir").expect("mkdir");
        let dir_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "dir")
            .expect("dir inode");
        assert_eq!(svc.symlink(dir_ino), FsError::NotPermitted);
    }

    /// A cross-directory mv of a `.torrent` from the metadata root moves the
    /// DB row and must evict the stale TorrentRoot cache entry bound to the
    /// old (root) source_path, so a later lookup re-resolves under cat-b.
    #[test]
    fn cross_directory_mv_evicts_stale_torrent_root_cache() {
        let mut svc = service_with_db();

        // mkdir cat-b under metadata/ (persisted via ensure_metadata_directories).
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        // Simulate a torrent previously added at the root: a DB row with
        // source_path == "" plus the restored File inode in metadata/.
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("", "ubuntu-iso", "ubuntu.iso.torrent", 16, "hash-2373", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: METADATA_INO,
                name: "ubuntu.iso.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ cache so eviction is actually exercised.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: String::new(),
                name: "ubuntu-iso".to_string(),
                filename: "ubuntu.iso.torrent".to_string(),
            },
        );

        // Cross-directory move: metadata/ubuntu.iso.torrent → metadata/cat-b/.
        svc.rename(
            METADATA_INO,
            "ubuntu.iso.torrent",
            cat_b_ino,
            "ubuntu.iso.torrent",
        )
        .expect("cross-directory rename");

        // The DB row moved to cat-b; nothing references the old location.
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(
                db.get_torrent_by_filename_and_source_path("ubuntu.iso.torrent", "")
                    .unwrap()
                    .is_none(),
                "old location must be empty after the move"
            );
            assert!(
                db.get_torrent_by_filename_and_source_path("ubuntu.iso.torrent", "cat-b")
                    .unwrap()
                    .is_some(),
                "torrent must live at cat-b after the move"
            );
        }

        // The stale TorrentRoot cached for the old (root) source_path must be
        // evicted — a later lookup re-resolves from the DB under cat-b.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry for the old source_path must be evicted"
        );
    }

    /// a cross-directory mv of a `.torrent` empties the old
    /// source-path directory, but empty directories must persist (and be
    /// restored on restart) — so the `cat-a` row survives in
    /// `metadata_directories`, while only the moved torrent's stale
    /// TorrentRoot cache entry is evicted.
    #[test]
    fn cross_directory_mv_persists_emptied_source_dir() {
        let mut svc = service_with_db();

        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "t1.torrent", 16, "hash-a1", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ caches for both paths before the move.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_source_path_dir_ino("cat-a"),
            DataInode::SourcePathDir {
                path: "cat-a".to_string(),
            },
        );
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "cat-a".to_string(),
                name: "t1".to_string(),
                filename: "t1.torrent".to_string(),
            },
        );

        // Cross-directory move: metadata/cat-a/t1.torrent → metadata/cat-b/t1.torrent.
        svc.rename(cat_a_ino, "t1.torrent", cat_b_ino, "t1.torrent")
            .expect("cross-directory rename");

        // DB: the torrent moved to cat-b, but the now-empty cat-a directory
        // row must survive (empty directories persist across restart).
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(db
                .get_torrent_by_filename_and_source_path("t1.torrent", "cat-a")
                .unwrap()
                .is_none());
            assert!(db
                .get_torrent_by_filename_and_source_path("t1.torrent", "cat-b")
                .unwrap()
                .is_some());
            assert!(
                db.get_source_path_prefixes("")
                    .unwrap()
                    .contains(&"cat-a".to_string()),
                "emptied cat-a directory must persist in metadata_directories"
            );
            assert!(
                db.get_source_path_prefixes("")
                    .unwrap()
                    .contains(&"cat-b".to_string()),
                "destination directory must survive"
            );
        }

        // The moved torrent's TorrentRoot cache entry bound to the old
        // location must be evicted — later lookups re-resolve from the DB.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry for cat-a must be evicted"
        );
        // The SourcePathDir cache entry for cat-a stays valid — the empty
        // directory still exists.
        assert!(
            svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_source_path_dir_ino("cat-a")),
            "SourcePathDir cache entry for persisted empty cat-a must remain"
        );
    }

    /// removing the last torrent from a directory must NOT prune
    /// the now-empty directory from `metadata_directories` — the empty
    /// directory persists so it is restored on restart.
    #[test]
    fn unlink_last_torrent_persists_emptied_directory() {
        let mut svc = service_with_db();

        svc.mkdir(METADATA_INO, "media").expect("mkdir media");
        let media_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "media")
            .expect("media inode");

        // Add a torrent under media/ directly (source_path = "media").
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("media", "t1", "t1.torrent", 16, "hash-media", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: media_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Remove the torrent — the directory it lived in becomes empty.
        let removed = svc.unlink(media_ino, "t1.torrent").expect("unlink torrent");
        assert!(removed.is_some(), "unlink must remove the landed DB row");

        // The now-empty media directory must survive in the DB.
        let db = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db.get_source_path_prefixes("")
                .unwrap()
                .contains(&"media".to_string()),
            "emptied media directory must persist in metadata_directories"
        );
    }

    /// an intra-directory rename (same parent) must NOT run
    /// orphan cleanup — only the filename changes.
    #[test]
    fn intra_directory_rename_skips_orphan_cleanup() {
        let mut svc = service_with_db();

        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "old.torrent", 16, "hash-r1", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "old.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        svc.rename(cat_a_ino, "old.torrent", cat_a_ino, "new.torrent")
            .expect("intra-directory rename");

        // Same source_path: the directory must remain — it still holds t1.
        let db = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db.get_source_path_prefixes("")
                .unwrap()
                .contains(&"cat-a".to_string()),
            "intra-directory rename must not prune the still-populated directory"
        );
    }

    /// a fast `cp` (release → detached add_torrent) followed
    /// immediately by a rename must wait for the pending add to settle, so
    /// the DB row lands at the NEW name instead of the old one. Simulate the
    /// race by inserting a pending entry in `processing_torrents` before
    /// calling rename; the rename must block until it clears and then move
    /// the row.
    #[test]
    fn intra_directory_rename_waits_for_pending_add() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        // The .torrent file exists as an inode but NOT yet in the DB —
        // exactly the window between release() spawning add_torrent and
        // insert_torrent_with_files committing.
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Mark the add as pending, and clear it from another thread after a
        // short delay — mimicking the background insert committing. The
        // notify_one is essential: it exercises the same wake path
        // `release`'s background add uses, so this test verifies the
        // Condvar wakeup instead of falling through to the timeout.
        {
            let processing = svc.processing_torrents.clone();
            let processing_cv = svc.processing_torrents_cv.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                if let Ok(mut guard) = processing.lock() {
                    guard.remove(&("cat-a".to_string(), "t1.torrent".to_string()));
                }
                processing_cv.notify_one();
            });
            svc.processing_torrents
                .lock()
                .unwrap()
                .insert(("cat-a".to_string(), "t1.torrent".to_string()), ());
        }

        let started = std::time::Instant::now();

        svc.rename(cat_a_ino, "t1.torrent", cat_a_ino, "renamed.torrent")
            .expect("rename must wait for the pending add and succeed");

        // The rename must return via the Condvar wakeup, not by waiting out
        // the 5s production timeout.
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "rename must be woken by notify_one promptly, took {:?}",
            started.elapsed()
        );

        // The pending entry was consumed by the simulated add; nothing is
        // left behind.
        assert!(
            svc.processing_torrents.lock().unwrap().is_empty(),
            "pending-add map must be drained"
        );
    }

    /// while the add is still pending past the timeout, the rename
    /// must FAIL rather than silently no-op the DB update (which would leave
    /// the data/ mirror on the stale filename).
    #[test]
    fn intra_directory_rename_fails_when_add_stays_pending() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        let t1_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            t1_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Pending forever — no background thread will clear it.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(("cat-a".to_string(), "t1.torrent".to_string()), ());

        // Inject a short deadline: exercising the full rename timeout path
        // must not hard-wait out the 5s production constant.
        let started = std::time::Instant::now();
        let result = svc.rename_with_pending_timeout(
            cat_a_ino,
            "t1.torrent",
            cat_a_ino,
            "renamed.torrent",
            Duration::from_millis(100),
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout path must honor the injected deadline, took {:?}",
            started.elapsed()
        );
        assert!(
            result.is_err(),
            "rename over a stuck pending add must fail instead of losing the rename"
        );

        match svc.inode_mgr.inodes.get(&t1_ino) {
            Some(InodeData::File { name, .. }) if name == "t1.torrent" => {}
            other => panic!(
                "inode must be rolled back to old name 't1.torrent' after rename timeout, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }
    }

    /// the 5s production timeout must not be
    /// baked into the wait loop — exercise the timeout path with a short
    /// injected deadline so this test stays fast.
    #[test]
    fn wait_for_pending_add_times_out_with_short_deadline() {
        let svc = service_with_db();
        let key = ("cat-a".to_string(), "t1.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let started = std::time::Instant::now();
        let err = svc
            .wait_for_pending_add(&key, Duration::from_millis(100), "rename")
            .expect_err("short timeout must fail");
        assert!(
            matches!(err, FsError::Internal(_)),
            "timeout must surface as Internal error, got {:?}",
            err
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout must honor the injected deadline, took {:?}",
            started.elapsed()
        );
    }

    /// an intra-directory rename must evict the cached TorrentRoot
    /// carrying the OLD filename — otherwise data/ keeps listing the stale
    /// name until restart even though the DB row is updated.
    #[test]
    fn intra_directory_rename_evicts_stale_torrent_root_cache() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "old.torrent", 16, "hash-2378", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "old.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ cache with the pre-rename TorrentRoot entry.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "cat-a".to_string(),
                name: "t1".to_string(),
                filename: "old.torrent".to_string(),
            },
        );

        svc.rename(cat_a_ino, "old.torrent", cat_a_ino, "new.torrent")
            .expect("intra-directory rename");

        // The stale cached entry (old filename) must be evicted so later
        // lookups re-resolve from the DB at the new filename.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry with the old filename must be evicted"
        );

        // And the DB row must now carry the new filename at the same path.
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(
                db.get_torrent_by_filename_and_source_path("new.torrent", "cat-a")
                    .unwrap()
                    .is_some(),
                "DB row must be renamed in place"
            );
        }
    }

    /// `cp` over an existing metadata torrent, then a fast `mv`. The `cp`
    /// releases a NEW info-hash under the OLD filename while the previous
    /// DB row still exists, so the background `add_torrent` takes the
    /// `Duplicate` branch and keeps the stale row; the rename then finds no
    /// row at the new name and no-ops. On restart the OLD torrent is restored
    /// and metadata/ + data/ show unexpected duplicates.
    #[test]
    fn rename_after_overwrite_syncs_stale_row() {
        let mut svc = service_with_db();

        // A previous torrent already persisted at (root, "a.torrent").
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("", "stale", "a.torrent", 16, "hash-old", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }

        // `cp` overwrites it with different torrent bytes and closes the
        // handle. The inode now carries the NEW bytes but keeps the name.
        let (ino, fh) = create_torrent_file(&mut svc, "a.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write ok");
        svc.release(fh).expect("release ok");

        // Simulate the background add_torrent having settled via the
        // Duplicate branch (row id 1 kept as-is) — drain the pending map
        // like the real thread would after its insert attempt.
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // The fast `mv`: the row at the new name does not exist yet, so the
        // pre-fix rename no-ops and the stale row survives at "a.torrent".
        svc.rename(METADATA_INO, "a.torrent", METADATA_INO, "b.torrent")
            .expect("rename ok");

        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let row = db_guard
            .get_torrent_by_filename_and_source_path("b.torrent", "")
            .unwrap()
            .expect("row must exist at the new name");

        // The overwritten row must reflect the NEW torrent content — the
        // pre-fix Duplicate branch kept the stale info-hash/bytes, so the
        // restart restored the old torrent and data/ served its file tree.
        let expected = {
            let info = TorrentInfo::from_bytes(minimal_torrent_bytes()).unwrap();
            hex::encode(info.info_hash().unwrap())
        };
        assert_eq!(
            row.info_hash, expected,
            "row must carry the NEW info-hash after overwrite+rename, got {}",
            row.info_hash
        );
    }

    /// Renaming into a destination whose parent inode is absent must surface
    /// `NotFound` (→ `ENOENT`, "no such file or directory"), not
    /// `NotDirectory` (→ `ENOTDIR`): the destination parent does not exist,
    /// so "not a directory" misdescribes the failure. This covers a rename
    /// arriving with a `newparent` inode no longer in the tables — the
    /// `mv metadata/seed metadata/nonexistent/` form cannot be its trigger,
    /// since the kernel answers ENOTDIR for it before FUSE is consulted.
    #[test]
    fn rename_missing_destination_parent_returns_not_found() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "seed").expect("mkdir seed");

        // A parent inode absent from both tables and outside every reserved
        // range (below DATA_TORRENT_INO_BASE, so the data-namespace guard
        // does not preempt it with EROFS).
        let missing_newparent = 999_999;
        assert!(!svc.inode_mgr.inodes.contains_key(&missing_newparent));
        assert!(!svc.inode_mgr.data_inodes.contains_key(&missing_newparent));

        let err = svc
            .rename(METADATA_INO, "seed", missing_newparent, "seed")
            .expect_err("rename into a missing destination parent must fail");

        assert_eq!(err, FsError::NotFound);
    }

    /// `mv metadata/cat-b/x.torrent metadata/cat-a/x.torrent` when
    /// `cat-a/x.torrent` already exists must overwrite the destination per
    /// POSIX `rename` semantics — the source torrent replaces the target
    /// (target row + inode removed, source row re-pointed at the new
    /// location) — instead of returning `AlreadyExists`/EEXIST.
    #[test]
    fn rename_over_existing_torrent_overwrites_target() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        // Two distinct torrents at the same filename in each directory.
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "target", "x.torrent", 16, "hash-target", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
            match db.insert_torrent("cat-b", "source", "x.torrent", 16, "hash-source", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        let target_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            target_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );
        let source_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            source_ino,
            InodeData::File {
                parent: cat_b_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        svc.rename(cat_b_ino, "x.torrent", cat_a_ino, "x.torrent")
            .expect("same-name move must overwrite the destination, not return EEXIST");

        // The source inode now lives at (cat-a, "x.torrent").
        match svc.inode_mgr.inodes.get(&source_ino) {
            Some(InodeData::File { parent, name, .. }) => {
                assert_eq!(*parent, cat_a_ino, "source inode must move into cat-a");
                assert_eq!(name, "x.torrent");
            }
            other => panic!(
                "source inode must survive the rename, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }
        // The target inode is retired (no open handles in this test).
        assert!(
            !svc.inode_mgr.inodes.contains_key(&target_ino),
            "target inode must be removed by the overwrite"
        );

        // DB: exactly one row at (cat-a, "x.torrent") carrying the source's
        // content; the target row is gone and cat-b no longer holds one.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let moved = db_guard
            .get_torrent_by_filename_and_source_path("x.torrent", "cat-a")
            .unwrap()
            .expect("source row must land at cat-a/x.torrent");
        assert_eq!(
            moved.info_hash, "hash-source",
            "cat-a must carry the source torrent, not the target"
        );
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("x.torrent", "cat-b")
                .unwrap()
                .is_none(),
            "the source location cat-b/x.torrent must be vacated"
        );
        assert_eq!(
            db_guard.get_all_torrents().unwrap().len(),
            1,
            "target row must be removed, leaving only the source"
        );
    }

    /// A same-name `mv` (POSIX `renameat` overwrite) over an existing torrent
    /// must replace the destination's full byte stream with the source's — the
    /// destination row carries the source `.torrent` bytes, never the old
    /// bytes and never an empty stream. This pins the QA regression contract
    /// (`renameat(AT_FDCWD)` replacing an existing torrent's full byte stream).
    #[test]
    fn rename_over_existing_torrent_replaces_full_byte_stream() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        // Distinct byte streams: the bencode `name` field differs, so each
        // parses to a distinct info-dict and a distinct content row.
        let source_bytes = minimal_torrent_named("source");
        let dest_bytes = minimal_torrent_named("dest");

        {
            let ts = svc.torrent_service.clone().expect("torrent service");
            ts.add_torrent(&source_bytes, "cat-b", "x.torrent")
                .expect("add source torrent");
            ts.add_torrent(&dest_bytes, "cat-a", "x.torrent")
                .expect("add destination torrent");
        }

        let target_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            target_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "x.torrent".to_string(),
                data: dest_bytes.clone(),
                unlinked: false,
            },
        );
        let source_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            source_ino,
            InodeData::File {
                parent: cat_b_ino,
                name: "x.torrent".to_string(),
                data: source_bytes.clone(),
                unlinked: false,
            },
        );

        svc.rename(cat_b_ino, "x.torrent", cat_a_ino, "x.torrent")
            .expect("same-name move must overwrite the destination");

        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let moved = db_guard
            .get_torrent_by_filename_and_source_path("x.torrent", "cat-a")
            .unwrap()
            .expect("source row must land at cat-a/x.torrent");
        assert_eq!(
            moved.torrent_data.as_deref(),
            Some(source_bytes.as_slice()),
            "cat-a/x.torrent must carry the source's full byte stream"
        );
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("x.torrent", "cat-b")
                .unwrap()
                .is_none(),
            "the source location cat-b/x.torrent must be vacated"
        );
    }

    /// The overwrite path must be atomic: when the source's pending add does
    /// not settle in time (simulated with a pre-seeded `processing_torrents`
    /// key and a short injected deadline), the rename fails and BOTH names
    /// survive — the source inode/row stay at `cat-b`, the target inode/row
    /// stay at `cat-a`. A non-atomic implementation (destroy the target first)
    /// would leave the target name vacant and its data lost.
    #[test]
    fn rename_overwrite_failure_keeps_both_names() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "target", "x.torrent", 16, "hash-target", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
            match db.insert_torrent("cat-b", "source", "x.torrent", 16, "hash-source", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        let target_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            target_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );
        let source_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            source_ino,
            InodeData::File {
                parent: cat_b_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // The source's add stays pending forever — no background thread will
        // clear it, so the source-side wait times out under the short deadline.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(("cat-b".to_string(), "x.torrent".to_string()), ());

        let started = std::time::Instant::now();
        let result = svc.rename_with_pending_timeout(
            cat_b_ino,
            "x.torrent",
            cat_a_ino,
            "x.torrent",
            Duration::from_millis(100),
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout path must honor the injected deadline, took {:?}",
            started.elapsed()
        );
        assert!(
            result.is_err(),
            "rename over a stuck pending source add must fail, not destroy the target"
        );

        // The source inode is rolled back to (cat-b, "x.torrent").
        match svc.inode_mgr.inodes.get(&source_ino) {
            Some(InodeData::File { parent, name, .. }) => {
                assert_eq!(*parent, cat_b_ino, "source inode must stay in cat-b");
                assert_eq!(name, "x.torrent");
            }
            other => panic!(
                "source inode must survive the failed rename, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }
        // The target inode is untouched (still at cat-a, not unlinked).
        match svc.inode_mgr.inodes.get(&target_ino) {
            Some(InodeData::File {
                parent,
                name,
                unlinked,
                ..
            }) => {
                assert_eq!(*parent, cat_a_ino, "target inode must stay in cat-a");
                assert_eq!(name, "x.torrent");
                assert!(!*unlinked, "target inode must not be unlinked");
            }
            other => panic!(
                "target inode must survive the failed rename, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }

        // DB: both rows remain at their original locations.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("x.torrent", "cat-a")
                .unwrap()
                .is_some(),
            "target row must survive the failed rename at cat-a"
        );
        let source_row = db_guard
            .get_torrent_by_filename_and_source_path("x.torrent", "cat-b")
            .unwrap()
            .expect("source row must survive the failed rename at cat-b");
        assert_eq!(source_row.info_hash, "hash-source");
        assert_eq!(
            db_guard.get_all_torrents().unwrap().len(),
            2,
            "failed overwrite must not delete either row"
        );
    }

    /// When the source torrent's DB row is absent (a prior persist failed),
    /// the overwrite must FAIL — never silently report success. The source and
    /// destination both stay in place (inode + row), so the kernel's dentry
    /// view and the FUSE directory state stay consistent.
    #[test]
    fn rename_overwrite_source_absent_returns_error() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        // The destination has a persisted row; the source only has an inode
        // (its add never landed — e.g. a failed persist).
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "target", "x.torrent", 16, "hash-target", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        let target_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            target_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );
        let source_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            source_ino,
            InodeData::File {
                parent: cat_b_ino,
                name: "x.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        let err = svc
            .rename(cat_b_ino, "x.torrent", cat_a_ino, "x.torrent")
            .expect_err("rename over an unpersisted source must fail");

        assert!(
            matches!(err, FsError::Internal(_)),
            "expected Internal error for a missing source row, got {:?}",
            err
        );

        // Both names stay in place: source inode back at cat-b, target inode
        // still at cat-a (not unlinked).
        match svc.inode_mgr.inodes.get(&source_ino) {
            Some(InodeData::File { parent, name, .. }) => {
                assert_eq!(*parent, cat_b_ino, "source inode must stay in cat-b");
                assert_eq!(name, "x.torrent");
            }
            other => panic!(
                "source inode must survive the failed rename, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }
        match svc.inode_mgr.inodes.get(&target_ino) {
            Some(InodeData::File {
                parent,
                name,
                unlinked,
                ..
            }) => {
                assert_eq!(*parent, cat_a_ino, "target inode must stay in cat-a");
                assert_eq!(name, "x.torrent");
                assert!(!*unlinked, "target inode must not be unlinked");
            }
            other => panic!(
                "target inode must survive the failed rename, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }

        // The destination row survives; no source row exists (it never did).
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("x.torrent", "cat-a")
                .unwrap()
                .is_some(),
            "target row must survive the failed rename at cat-a"
        );
        assert_eq!(
            db_guard.get_all_torrents().unwrap().len(),
            1,
            "failed overwrite must not delete the destination row"
        );
    }

    #[test]
    fn data_namespace_inodes_correctly_identified() {
        // Unit-test the helper itself.
        assert!(InodeManager::is_data_namespace(DATA_INO));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE + 42));
        assert!(InodeManager::is_data_namespace(DATA_DIR_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(DATA_FILE_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(SOURCE_PATH_DIR_INO_BASE));

        // Non-data inodes are not in the data namespace.
        assert!(!InodeManager::is_data_namespace(ROOT_INO));
        assert!(!InodeManager::is_data_namespace(METADATA_INO));
        assert!(!InodeManager::is_data_namespace(STATS_INO));
        assert!(!InodeManager::is_data_namespace(
            NEXT_INO.load(Ordering::SeqCst),
        ));
    }

    /// opening a `data/` torrent file must set `direct_io: true`
    /// so the kernel bypasses its page cache and the daemon's errno
    /// (e.g. ENODATA) reaches userspace instead of being converted to EIO.
    #[test]
    fn open_data_torrent_file_sets_direct_io() {
        let mut svc = bare_service();
        let ino = DATA_FILE_INO_BASE + 1;
        svc.inode_mgr.data_inodes.insert(
            ino,
            DataInode::TorrentFile {
                torrent_id: 1,
                file_id: 1,
                name: "foo".to_string(),
                size: 16,
                torrent_source_path: String::new(),
                torrent_filename: String::new(),
            },
        );
        let outcome = svc.open(ino).expect("open data file");
        assert!(
            outcome.direct_io,
            "data torrent file must request direct_io"
        );
        assert_ne!(outcome.fh, 0, "data torrent file must get a real fh");
    }

    /// an out-of-bounds read (`offset >= file_size`) must return
    /// 0 bytes (EOF), never dirty piece data. The `read` handler's guard is
    /// the first line of defense before any piece is read from cache/network.
    #[test]
    fn read_at_or_past_eof_returns_empty() {
        let mut svc = bare_service();
        let ino = DATA_FILE_INO_BASE + 1;
        svc.inode_mgr.data_inodes.insert(
            ino,
            DataInode::TorrentFile {
                torrent_id: 1,
                file_id: 1,
                name: "foo".to_string(),
                size: 16,
                torrent_source_path: String::new(),
                torrent_filename: String::new(),
            },
        );

        // offset == file_size (exactly at EOF) → empty.
        let out = svc.read(ino, 16, 4096).expect("read at EOF");
        assert!(
            matches!(&out, ReadOutcome::Ready(d) if d.is_empty()),
            "read at offset == file_size must return empty (EOF)"
        );

        // offset > file_size (past EOF) → empty.
        let out = svc.read(ino, 999_999_999, 1).expect("read past EOF");
        assert!(
            matches!(&out, ReadOutcome::Ready(d) if d.is_empty()),
            "read at offset > file_size must return empty (EOF)"
        );

        // offset < file_size (in bounds) must NOT be served as EOF: it
        // proceeds to `read_data`, which fails here because `bare_service`
        // has no torrent service. This asserts the guard does not over-fire.
        svc.read_identities.insert(ino, test_identity(0, 1));
        let err = match svc.read(ino, 15, 4096) {
            Err(e) => e,
            Ok(_) => panic!("in-bounds read must not be served as Ready"),
        };
        assert!(
            matches!(&err, FsError::Internal(_)),
            "in-bounds read must not be treated as EOF; got {err:?}"
        );
    }

    /// non-data files (e.g. metadata) must NOT set direct_io —
    /// page cache is fine for static in-memory content.
    #[test]
    fn open_metadata_file_does_not_set_direct_io() {
        let mut svc = bare_service();
        let ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            ino,
            InodeData::File {
                parent: ROOT_INO,
                name: "x".to_string(),
                data: vec![b'x'],
                unlinked: false,
            },
        );
        let outcome = svc.open(ino).expect("open metadata file");
        assert!(
            !outcome.direct_io,
            "metadata file must not request direct_io"
        );
    }

    /// `C-6.1.4`: every `.stats` read re-renders the file, so the open must
    /// bypass the kernel page cache — otherwise the kernel serves a stale
    /// rendering (and an outdated length) until its cache expires.
    #[test]
    fn open_stats_file_sets_direct_io() {
        let mut svc = bare_service();
        let outcome = svc.open(STATS_INO).expect("open global stats");
        assert!(
            outcome.direct_io,
            "global .stats must request direct_io to re-render per read"
        );

        let derived = InodeManager::make_stats_ino(DATA_INO);
        let outcome = svc.open(derived).expect("open derived stats");
        assert!(
            outcome.direct_io,
            "derived .stats must request direct_io to re-render per read"
        );
    }

    /// `C-2.2.6`: a directory listing aimed at a file is `ENOENT`, never
    /// `ENOTDIR`.
    #[test]
    fn readdir_on_a_metadata_file_returns_not_found() {
        let mut svc = bare_service();
        let ino = svc
            .create(METADATA_INO, "x.torrent")
            .expect("create metadata file")
            .attr
            .ino;
        assert_eq!(
            svc.readdir(ino, 0)
                .expect_err("readdir on a file must fail"),
            FsError::NotFound,
            "C-2.2.6 requires ENOENT for a listing aimed at a file"
        );
    }

    /// `C-2.2.6`: reading a directory is `EISDIR`, not `ENOENT`.
    #[test]
    fn read_on_a_directory_returns_is_directory() {
        let mut svc = bare_service();
        let sub = svc.mkdir(METADATA_INO, "sub").expect("mkdir").ino;
        match svc.read(sub, 0, 4096) {
            Err(FsError::IsDirectory) => {}
            Err(other) => panic!("expected EISDIR for a metadata directory, got {:?}", other),
            Ok(_) => panic!("expected EISDIR for a metadata directory, got a read"),
        }

        let root_ino = DATA_DIR_INO_BASE + 7;
        svc.inode_mgr.data_inodes.insert(
            root_ino,
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: String::new(),
                name: "t".to_string(),
                filename: "t.torrent".to_string(),
            },
        );
        match svc.read(root_ino, 0, 4096) {
            Err(FsError::IsDirectory) => {}
            Err(other) => panic!("expected EISDIR for a data directory, got {:?}", other),
            Ok(_) => panic!("expected EISDIR for a data directory, got a read"),
        }
    }

    /// `C-4.1.6`: a zero-length read returns zero bytes without entering the
    /// download path (the old `.max(1)` clamp turned it into one byte).
    #[test]
    fn zero_length_read_returns_empty() {
        let mut svc = bare_service();
        let ino = DATA_FILE_INO_BASE + 2;
        svc.inode_mgr.data_inodes.insert(
            ino,
            DataInode::TorrentFile {
                torrent_id: 1,
                file_id: 2,
                name: "foo".to_string(),
                size: 16,
                torrent_source_path: String::new(),
                torrent_filename: String::new(),
            },
        );

        let out = svc.read(ino, 4, 0).expect("zero-length read");
        assert!(
            matches!(&out, ReadOutcome::Ready(d) if d.is_empty()),
            "a zero-length read must return zero bytes"
        );
        // The per-read helper must short-circuit too, not fall through to the
        // (here unavailable) torrent service.
        let out = svc
            .read_data(1, 2, test_identity(0, 1), 4, 0)
            .expect("zero-length read_data");
        assert!(matches!(&out, ReadOutcome::Ready(d) if d.is_empty()));
    }

    /// `C-11.4`/H3: an L1 hit is answered without touching the state database
    /// or the torrent service at all. A service with neither must still serve
    /// the cached range.
    #[test]
    fn read_hits_l1_without_the_state_database() {
        let mut svc = bare_service();
        svc.torrent_data_cache
            .lock()
            .unwrap()
            .insert(format!("{}:{}:{}", 42, 8, 4), vec![7, 8, 9, 10]);
        let out = svc
            .read_data(1, 42, test_identity(0, 1), 8, 4)
            .expect("L1 hit");
        assert!(matches!(&out, ReadOutcome::Ready(d) if d == &vec![7, 8, 9, 10]));
    }

    /// `C-4.5.3`/M4: unlinking a landed source row queues its id so the
    /// adapter can settle that row's in-flight reads with `EIO`.
    #[test]
    fn unlink_queues_cancellation_for_the_removed_source_row() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "media").expect("mkdir media");
        let media_ino = svc
            .metadata_child(METADATA_INO, "media")
            .expect("media inode");
        let torrent_id = {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("media", "t1", "t1.torrent", 16, "hash-cancel", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(id)) => id,
                other => panic!("unexpected insert result: {:?}", other),
            }
        };
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: media_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        svc.unlink(media_ino, "t1.torrent").expect("unlink torrent");
        let cancelled = svc.cancelled_reads.lock().unwrap();
        assert!(
            cancelled.iter().any(|(id, _)| *id == torrent_id),
            "removed source row must be queued for read cancellation"
        );
    }

    /// `C-4.5.3`/M4: removing a metadata directory removes every source row
    /// under it, so all of their ids are queued for cancellation.
    #[test]
    fn rmdir_queues_cancellation_for_every_source_row_under_it() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "media").expect("mkdir media");
        let ids: Vec<i64> = {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            ["a.torrent", "b.torrent"]
                .iter()
                .map(
                    |name| match db.insert_torrent("media", "t", name, 16, "hash-rmdir", 1) {
                        Ok(crate::db::InsertTorrentResult::Inserted(id)) => id,
                        other => panic!("unexpected insert result: {:?}", other),
                    },
                )
                .collect()
        };

        svc.rmdir(METADATA_INO, "media").expect("rmdir media");
        let cancelled = svc.cancelled_reads.lock().unwrap();
        for id in ids {
            assert!(
                cancelled.iter().any(|(queued, _)| *queued == id),
                "source row {} under the removed directory must be cancelled",
                id
            );
        }
    }

    #[test]
    fn is_sparse_file_detects_sparse_partial_write() {
        // Simulate a partial multi-block piece write: create a file with the
        // right logical size but a zero-filled gap (sparse hole) by seeking
        // past the start before writing the trailing block.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse_piece");
        use std::io::{Seek, Write};
        let mut file = std::fs::File::create(&path).unwrap();
        // Write 16 KiB at offset 256 KiB — the file's logical size becomes
        // 256 KiB + 16 KiB, but bytes 0..256 KiB are a sparse hole.
        file.seek(std::io::SeekFrom::Start(256 * 1024)).unwrap();
        file.write_all(&vec![0xABu8; 16 * 1024]).unwrap();
        drop(file);

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 256 * 1024 + 16 * 1024);
        assert!(
            is_sparse_file(&meta, meta.len()),
            "file with a sparse hole must be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_complete_file() {
        // A fully-written (non-sparse) file must not be flagged as sparse.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("complete_piece");
        std::fs::write(&path, vec![0xCDu8; 262_144]).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 262_144);
        assert!(
            !is_sparse_file(&meta, meta.len()),
            "fully-written file must not be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_piece");
        std::fs::write(&path, b"").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert!(!is_sparse_file(&meta, 0));
    }

    /// Build a single-file bencoded torrent whose single piece has a known
    /// SHA-1 hash.  Returns `(torrent_bytes, piece_content, piece_length)`.
    fn build_single_piece_torrent() -> (Vec<u8>, Vec<u8>, u64) {
        let piece_length: usize = 16_384;
        let content = (0..piece_length)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>();
        let hash = {
            use sha1_smol::Sha1;
            Sha1::from(&content).digest().bytes()
        };
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(content.len().to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&hash);
        t.extend_from_slice(b"ee");
        (t, content, piece_length as u64)
    }

    /// Write a sparse piece file to `piece_path` whose *logical* size equals
    /// `piece_length` but whose physical allocation is smaller — simulating an
    /// interrupted multi-block write where only the trailing block was
    /// flushed to disk.  The tail starts at `piece_length / 4` so it never
    /// underflows for small pieces (the prior `piece_length -
    /// 4096` could underflow when `piece_length < 4096`).
    fn write_sparse_piece_file(piece_path: &std::path::Path, content: &[u8], piece_length: u64) {
        use std::io::{Seek, Write};
        let tail_start = piece_length / 4;
        let mut file = std::fs::File::create(piece_path).unwrap();
        file.seek(std::io::SeekFrom::Start(tail_start)).unwrap();
        file.write_all(&content[tail_start as usize..]).unwrap();
        drop(file);
    }

    #[test]
    fn verify_single_piece_sparse_file_is_purged() {
        let (torrent_bytes, content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);
        let temp_dir = tempfile::tempdir().unwrap();

        // Create the piece file on disk *before* constructing the
        // CacheManager so the startup scan registers it as unverified
        // (exactly like a real restart).
        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir).unwrap();
        let piece_path = pieces_dir.join(&piece_key);

        // Sparse file: correct logical size but a zero-filled hole at the
        // start (simulates an interrupted multi-block write where only the
        // trailing block was flushed to disk).
        write_sparse_piece_file(&piece_path, &content, piece_length);

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        // Scan registered the piece but left it unverified.
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        // a sparse (partial-write) piece is purged, not skipped.
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "sparse piece file should be purged, not skipped"
        );
        // The caller (spawn_cache_verification) applies Purged by deleting
        // the piece from the cache; here we only assert the outcome, but
        // the purge contract implies the file is reclaimable.
    }

    #[test]
    fn verify_single_piece_complete_correct_hash_is_verified() {
        let (torrent_bytes, content, _piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        std::fs::write(&piece_path, &content).unwrap();
        cache.add_piece(&piece_key, content.len() as u64).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Verified),
            "complete piece with correct hash should be verified"
        );
    }

    #[test]
    fn verify_single_piece_complete_wrong_hash_is_purged() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a complete, non-sparse file with wrong content.
        std::fs::write(&piece_path, vec![0xFFu8; piece_length as usize]).unwrap();
        cache.add_piece(&piece_key, piece_length).unwrap();
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "complete non-sparse piece with wrong hash should be purged"
        );
    }

    #[test]
    fn verify_single_piece_wrong_size_is_purged() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a short file (incomplete piece).
        std::fs::write(&piece_path, vec![0u8; piece_length as usize / 2]).unwrap();
        cache.add_piece(&piece_key, piece_length / 2).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "wrong-size piece should be purged"
        );
    }

    /// a purged incomplete piece must be deleted from the cache
    /// (metadata + on-disk file) so the disk space is reclaimed.  This
    /// exercises the full `spawn_cache_verification` purge contract by
    /// applying `VerifyOutcome::Purged` via `delete_piece` — the same path
    /// the verification worker uses.
    #[test]
    fn purged_sparse_piece_frees_disk_and_metadata() {
        let (torrent_bytes, content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();

        // Create a sparse piece file on disk *before* constructing the
        // CacheManager so the startup scan registers it as unverified.
        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir).unwrap();
        let piece_path = pieces_dir.join(&piece_key);

        // Sparse file: correct logical size but a zero-filled hole at the
        // start (simulates an interrupted multi-block write where only the
        // trailing block was flushed to disk).
        write_sparse_piece_file(&piece_path, &content, piece_length);

        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));
        assert!(piece_path.exists());
        let size_before = cache.current_size();
        assert!(size_before > 0, "scan should credit the piece size");

        // Verify → Purged → the worker applies the side effect.
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(matches!(outcome, VerifyOutcome::Purged));
        cache.delete_piece(&piece_key).unwrap();

        // The piece is gone from metadata, verified set, and disk.
        assert!(!cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));
        assert!(!piece_path.exists(), "purged piece file must be deleted");
        assert_eq!(cache.current_size(), 0, "current_size must drop to zero");
    }

    // ── release must not block the FUSE dispatch thread ─────────

    /// Build a service backed by an in-memory DB + TorrentService so
    /// `release` can exercise the full add_torrent path.
    fn service_with_db() -> FsService {
        let db = Database::open_in_memory().expect("in-memory db");
        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let torrent_service = TorrentService::new(db_arc.clone(), None);
        let content_versions = torrent_service.content_versions();
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(torrent_service),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions,
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        }
    }

    /// Service with a real download engine, so `read` on an uncached range
    /// yields `ReadOutcome::Pending` exactly as the adapter sees it.
    fn service_with_download() -> (FsService, tempfile::TempDir) {
        let cache_dir = tempfile::TempDir::new().expect("cache dir");
        let download_service = Arc::new(
            DownloadService::new(cache_dir.path(), &TorrentfsConfig::default_config())
                .expect("download service"),
        );
        let db = Database::open_in_memory().expect("in-memory db");
        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let torrent_service = TorrentService::new(db_arc.clone(), Some(download_service.clone()));
        let content_versions = torrent_service.content_versions();
        let svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(torrent_service),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            processing_torrents_cv: Arc::new(Condvar::new()),
            download_service: Some(download_service),
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions,
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        };
        (svc, cache_dir)
    }

    /// Create a writable `.torrent` inode via the public `create()` path and
    /// return its (ino, fh).
    fn create_torrent_file(svc: &mut FsService, name: &str) -> (u64, u64) {
        let created = svc.create(METADATA_INO, name).expect("create file");
        (created.attr.ino, created.fh)
    }

    /// A mock sink that records each cancellation the persist thread issues.
    struct RecordingCancelSink {
        calls: Mutex<Vec<(i64, Option<i64>)>>,
    }

    impl RecordingCancelSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                calls: Mutex::new(Vec::new()),
            })
        }

        fn calls(&self) -> Vec<(i64, Option<i64>)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl ReadCancelSink for RecordingCancelSink {
        fn cancel_reads_not_for_content(&self, torrent_id: i64, content_id: Option<i64>) -> usize {
            self.calls.lock().unwrap().push((torrent_id, content_id));
            0
        }
    }

    /// Wait for the persist thread to issue `expected` sink calls. The
    /// detached persist thread can run before or after `release` returns, so
    /// an immediate assertion would race.
    fn wait_for_sink_calls(sink: &RecordingCancelSink, expected: usize) -> Vec<(i64, Option<i64>)> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let calls = sink.calls();
            if calls.len() >= expected || std::time::Instant::now() > deadline {
                return calls;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Insert a source row with `count` files (and no raw torrent bytes), so
    /// file-identity tests can exercise a wide table cheaply.
    fn insert_wide_torrent(svc: &mut FsService, count: usize) -> i64 {
        let info_hash = "0123456789abcdef0123456789abcdef01234567";
        insert_source_with_files(svc, "wide", "wide.torrent", info_hash, count)
    }

    fn insert_source_with_files(
        svc: &mut FsService,
        source_path: &str,
        filename: &str,
        info_hash: &str,
        count: usize,
    ) -> i64 {
        let files: Vec<FileEntry> = (0..count)
            .map(|index| FileEntry {
                path: format!("dir/f{index}.txt"),
                size: index as i64,
            })
            .collect();
        let db = svc.db.as_ref().expect("test service has a database");
        let mut guard = db.lock().expect("test database lock");
        match guard.insert_torrent_with_files(
            source_path,
            source_path,
            filename,
            files.iter().map(|file| file.size).sum(),
            info_hash,
            files.len() as i64,
            &files,
        ) {
            Ok(InsertTorrentResult::Inserted(id)) => id,
            other => panic!("unexpected insert result: {other:?}"),
        }
    }

    /// Read the last file's row id for the `count`-file source created by
    /// [`insert_wide_torrent`].
    fn last_file_id(svc: &mut FsService) -> i64 {
        let db = svc.db.as_ref().expect("test service has a database");
        let guard = db.lock().expect("test database lock");
        guard
            .conn
            .query_row("SELECT MAX(id) FROM torrent_files", [], |row| {
                row.get::<_, Option<i64>>(0)
            })
            .expect("read last file id")
            .expect("wide torrent has files")
    }

    /// `PersistedTorrent::content_changed` must report what the source row
    /// actually did: replacing the location's content differs from a re-persist
    /// of identical bytes or the first landing at a new source.
    #[test]
    fn persisted_content_changed_distinguishes_overwrite_from_first_persist() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "identity.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes()).expect("write");
        svc.flush(ino).expect("flush");
        svc.release(fh).expect("release");
        wait_for_processing_empty(&svc);

        let first = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .persist_torrent(&minimal_torrent_named("bar"), "", "identity.torrent")
            .expect("overwrite");
        assert!(first.content_changed(), "different bytes must be a replace");

        let same = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .persist_torrent(&minimal_torrent_named("bar"), "", "identity.torrent")
            .expect("identical re-persist");
        assert!(
            !same.content_changed(),
            "identical bytes re-persisted at the same location must not cancel"
        );

        let fresh = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .persist_torrent(&minimal_torrent_bytes(), "", "fresh.torrent")
            .expect("first persist at a new source");
        assert!(
            !fresh.content_changed(),
            "a new source has nothing to cancel"
        );
    }

    /// `release` moves the buffer into the shared handle (no 10 MiB copy) and
    /// a read-only close hands that same allocation back; `flush` validates the
    /// shared bytes in place. This is the E5 evidence.
    #[test]
    fn release_moves_the_buffer_and_flush_shares_it() {
        let mut svc = service_with_db();
        let bytes = minimal_torrent_bytes();
        let (ino, fh) = create_torrent_file(&mut svc, "shared.torrent");
        svc.write(ino, 0, &bytes).expect("write");
        let original_ptr = match svc.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File { data, .. }) => data.as_ptr(),
            _ => panic!("created inode must be a file"),
        };

        svc.flush(ino).expect("flush validates in place");
        svc.release(fh).expect("release");
        wait_for_processing_empty(&svc);

        let parked = svc
            .buffered_torrents
            .get(&ino)
            .expect("release parks the buffer as a shared handle");
        assert_eq!(
            parked.as_ptr(),
            original_ptr,
            "release must move the allocation into the Arc, not copy it"
        );
        svc.flush(ino)
            .expect("flush must validate the shared buffer without moving it");
        assert_eq!(
            svc.buffered_torrents.get(&ino).map(|bytes| bytes.as_ptr()),
            Some(original_ptr),
            "flush must not replace the shared allocation"
        );
        match svc
            .read(ino, 0, bytes.len() as u32)
            .expect("read parked torrent")
        {
            ReadOutcome::Ready(data) => {
                assert_eq!(data, bytes, "the shared buffer must still serve reads")
            }
            _ => panic!("a metadata read must be ready, not deferred"),
        }

        // A read-only close reclaims the same allocation (zero-copy).
        let (_, _, reclaimed) = svc.take_buffered_file(ino).expect("close reclaims bytes");
        assert_eq!(reclaimed.as_ptr(), original_ptr);
        assert_eq!(reclaimed.as_slice(), bytes.as_slice());

        let db = svc.db.as_ref().expect("test service has a database");
        let row = db
            .lock()
            .unwrap()
            .get_torrent_by_source_path("")
            .expect("read row")
            .expect("release persisted the torrent");
        assert!(
            row.torrent_data.is_some_and(|data| !data.is_empty()),
            "the close must not look like a zero-byte overwrite"
        );
    }

    /// A metadata close with unchanged bytes must not cancel in-flight data
    /// reads; a real content replace must fire the sink immediately with the
    /// landed content id.
    #[test]
    fn unchanged_metadata_close_does_not_cancel_but_a_replace_does() {
        let mut svc = service_with_db();
        let sink = RecordingCancelSink::new();
        svc.read_cancel_sink
            .set(sink.clone() as Arc<dyn ReadCancelSink>)
            .ok()
            .expect("sink is unset in a fresh test service");

        // First landing: a new source has nothing to cancel.
        let (ino, fh) = create_torrent_file(&mut svc, "cancel.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes()).expect("write");
        svc.flush(ino).expect("flush");
        svc.release(fh).expect("first release");
        wait_for_processing_empty(&svc);
        assert!(
            sink.calls().is_empty(),
            "persisting a new source must not cancel reads"
        );

        // Read-only open + close of the persisted file: same bytes, same
        // content row — still nothing to cancel.
        let entry = svc
            .lookup(METADATA_INO, "cancel.torrent")
            .expect("lookup metadata child")
            .expect("persisted file is visible");
        let fh = svc.open(entry.ino).expect("read-only open").fh;
        svc.release(fh).expect("identical release");
        wait_for_processing_empty(&svc);
        assert!(
            sink.calls().is_empty(),
            "an unchanged byte-for-byte close must not cancel in-flight reads"
        );

        let (source_id, first_content_id) = {
            let db = svc.db.as_ref().expect("test service has a database");
            let guard = db.lock().unwrap();
            let row = guard
                .get_torrent_by_source_path("")
                .expect("read row")
                .expect("row exists");
            (row.id, row.torrent_id)
        };
        assert_eq!(
            svc.content_version(source_id),
            Some(ContentVersion::Current(first_content_id)),
            "the landed content id is the authoritative publish token"
        );

        // Overwrite with different bytes through the kernel's sequence:
        // open → truncate → write → flush → release.
        let different = minimal_torrent_named("bar");
        let fh = svc.open(entry.ino).expect("open for overwrite").fh;
        svc.setattr(entry.ino, Some(0)).expect("truncate");
        svc.write(entry.ino, 0, &different)
            .expect("overwrite write");
        svc.flush(entry.ino).expect("flush");
        svc.release(fh).expect("overwrite release");
        let calls = wait_for_sink_calls(&sink, 1);
        let replaced_content_id = {
            let db = svc.db.as_ref().expect("test service has a database");
            let guard = db.lock().unwrap();
            guard
                .get_torrent_by_source_path("")
                .expect("read row")
                .expect("row exists")
                .torrent_id
        };
        assert_ne!(replaced_content_id, first_content_id);
        assert_eq!(
            calls,
            vec![(source_id, Some(replaced_content_id))],
            "a content replace must settle older reads immediately, once"
        );
        assert!(
            svc.cancelled_reads
                .lock()
                .unwrap()
                .contains(&(source_id, Some(replaced_content_id))),
            "the adapter drain must receive the landed content id"
        );
    }

    /// Identity resolution lives on the projection path: opening a file binds
    /// the identity its lookup prepared to the inode, so neither a cold read
    /// nor a read of an already-open file after the 256-source prepared cache
    /// overflowed (or after the source's identities were invalidated) visits a
    /// single file-table row. Only a fresh lookup may prepare the source once
    /// (H3 / C-11.4 / F2).
    #[test]
    fn open_file_reads_never_load_the_file_table_after_cache_overflow() {
        const FILES: usize = 20_000;
        let mut svc = service_with_db();
        let source_id = insert_wide_torrent(&mut svc, FILES);
        let file_id = last_file_id(&mut svc);
        let preparations = |svc: &FsService| {
            svc.torrent_service
                .as_ref()
                .unwrap()
                .file_identity_query_count()
        };
        let rows_visited = |svc: &FsService| {
            svc.torrent_service
                .as_ref()
                .unwrap()
                .file_identity_rows_read()
        };
        let table_loads = |svc: &FsService| {
            svc.torrent_service
                .as_ref()
                .unwrap()
                .full_file_list_load_count()
        };
        assert_eq!(preparations(&svc), 0);
        assert_eq!(rows_visited(&svc), 0);
        assert_eq!(table_loads(&svc), 0);

        // Walk the real data tree to the last file. The lookup of its parent
        // prepares the source once (one ordered statement over every row) and
        // binds the identity to the file inode.
        let source_dir = svc
            .lookup(DATA_INO, "wide")
            .expect("lookup source-path dir")
            .expect("source-path dir exists");
        let root = svc
            .lookup(source_dir.ino, "wide.torrent")
            .expect("lookup torrent root")
            .expect("torrent root exists");
        let dir = svc
            .lookup(root.ino, "dir")
            .expect("lookup torrent dir")
            .expect("torrent dir exists");
        let file = svc
            .lookup(dir.ino, "f19999.txt")
            .expect("lookup torrent file")
            .expect("torrent file exists");
        assert_eq!(preparations(&svc), 1, "one bulk preparation on lookup");
        assert_eq!(
            rows_visited(&svc),
            FILES as u64,
            "preparation visits one row per file, once"
        );
        assert_eq!(table_loads(&svc), 0, "never a whole-table load");
        let identity = svc
            .read_identity(file.ino)
            .expect("lookup binds the read identity to the inode");
        assert_eq!(identity.file_index, (FILES - 1) as i32);
        assert_eq!(
            identity.info_hash.as_ref(),
            "0123456789abcdef0123456789abcdef01234567"
        );
        assert_eq!(identity.content_id, {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            db.get_torrent_by_id(source_id).unwrap().unwrap().torrent_id
        });

        let before_prep = preparations(&svc);
        let before_rows = rows_visited(&svc);
        // Cold read: the inode already carries the identity.
        let _ = svc.read(file.ino, 0, 1);
        assert_eq!(preparations(&svc), before_prep, "cold read: zero prep");
        assert_eq!(rows_visited(&svc), before_rows, "cold read: zero rows");

        // Fill the 256-source prepared cache so the wide source is cleared by
        // real overflow, exactly as a long-lived daemon would.
        for index in 0..256 {
            let info_hash = format!("{:040x}", index + 1);
            let source = insert_source_with_files(
                &mut svc,
                &format!("src{index}"),
                &format!("src{index}.torrent"),
                &info_hash,
                1,
            );
            svc.ensure_file_identities(source);
        }
        assert!(
            svc.torrent_service
                .as_ref()
                .unwrap()
                .prepared_file_identity(source_id, file_id)
                .is_none(),
            "the 256-source overflow must have cleared the wide source"
        );

        // The file is still open: its inode-bound identity keeps the read off
        // the file table even though every prepared entry is gone.
        let overflow_prep = preparations(&svc);
        let overflow_rows = rows_visited(&svc);
        let _ = svc.read(file.ino, 0, 1);
        assert_eq!(
            preparations(&svc),
            overflow_prep,
            "post-overflow read: zero preparations"
        );
        assert_eq!(
            rows_visited(&svc),
            overflow_rows,
            "post-overflow read: zero rows"
        );

        // The same holds when only this source is invalidated (the old
        // per-file cache eviction path) and every other cache is dropped.
        svc.torrent_service
            .as_ref()
            .unwrap()
            .invalidate_file_identities(source_id);
        svc.torrent_data_cache.lock().unwrap().clear();
        svc.torrent_info_cache.lock().unwrap().clear();
        let _ = svc.read(file.ino, 0, 1);
        assert_eq!(preparations(&svc), overflow_prep);
        assert_eq!(rows_visited(&svc), overflow_rows);
        assert_eq!(table_loads(&svc), 0, "read path never loads the file table");

        // A fresh lookup after the overflow re-prepares the source once on the
        // projection path; the following read is still free.
        let _ = svc.lookup(dir.ino, "f19999.txt").expect("relookup file");
        assert_eq!(
            preparations(&svc),
            overflow_prep + 1,
            "lookup: one bulk preparation after overflow"
        );
        assert_eq!(
            rows_visited(&svc),
            overflow_rows + FILES as u64,
            "lookup: the only enumeration is one preparation"
        );
        let _ = svc.read(file.ino, 0, 1);
        assert_eq!(preparations(&svc), overflow_prep + 1);
        assert_eq!(rows_visited(&svc), overflow_rows + FILES as u64);
        assert_eq!(table_loads(&svc), 0);
    }

    /// Counting reply for the real pending-table interleaving test.
    struct CountReply {
        resolves: Arc<std::sync::atomic::AtomicUsize>,
        errno: Arc<std::sync::atomic::AtomicI32>,
        got_data: Arc<std::sync::atomic::AtomicBool>,
    }

    impl CountReply {
        fn new() -> (
            Self,
            Arc<std::sync::atomic::AtomicUsize>,
            Arc<std::sync::atomic::AtomicI32>,
            Arc<std::sync::atomic::AtomicBool>,
        ) {
            let resolves = Arc::new(std::sync::atomic::AtomicUsize::new(0));
            let errno = Arc::new(std::sync::atomic::AtomicI32::new(0));
            let got_data = Arc::new(std::sync::atomic::AtomicBool::new(false));
            (
                Self {
                    resolves: resolves.clone(),
                    errno: errno.clone(),
                    got_data: got_data.clone(),
                },
                resolves,
                errno,
                got_data,
            )
        }
    }

    impl super::super::PendingReply for CountReply {
        fn resolve_data(self, _data: &[u8]) {
            self.got_data
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.resolves
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        fn resolve_error(self, errno: libc::c_int) {
            self.errno
                .store(errno, std::sync::atomic::Ordering::Relaxed);
            self.resolves
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// The real lookup→read interleaving the review asks for: content B has
    /// already landed in the database (lookup and read both see it) while the
    /// persist thread has not yet published the cancellation. The identity
    /// B's read registers comes from the same statement as its bytes, so it
    /// must survive the sweep; A's older read must settle with `EIO` exactly
    /// once.
    #[test]
    fn read_of_db_landed_content_survives_publish_and_old_read_settles_once() {
        use std::sync::atomic::Ordering;

        let (mut svc, _cache_dir) = service_with_download();
        let table = Arc::new(super::super::PendingTable::<CountReply>::new());
        svc.read_cancel_sink
            .set(table.clone() as Arc<dyn ReadCancelSink>)
            .ok()
            .expect("sink is unset in a fresh test service");

        // Land content A through the kernel's real write→release path.
        let (ino, fh) = create_torrent_file(&mut svc, "identity.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write A");
        svc.flush(ino).expect("flush A");
        svc.release(fh).expect("release A");
        wait_for_processing_empty(&svc);

        let root = svc
            .lookup(DATA_INO, "identity.torrent")
            .expect("lookup root")
            .expect("root exists");
        let a_file = svc
            .lookup(root.ino, "foo")
            .expect("lookup A file")
            .expect("A file exists");
        let (source_id, content_a) = {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            let row = db
                .get_torrent_by_source_path("")
                .expect("read row")
                .expect("row exists");
            (row.id, row.torrent_id)
        };
        let (a_hash, a_index, a_content) = pending_identity(&mut svc, a_file.ino);
        assert_eq!(a_content, content_a);

        // Register A while its content is still the published one.
        let (a_reply, a_resolves, a_errno, a_data) = CountReply::new();
        let _ = expect_leader(super::super::register_content_validated(
            &table,
            a_reply,
            source_id,
            a_content,
            pending_deadline(),
            range_key(&a_hash, a_index),
            || svc.content_version(source_id),
        ));
        assert_eq!(a_resolves.load(Ordering::Relaxed), 0);

        // Content B lands in the database and its identities are prepared,
        // but the persist thread has not run its cancellation sweep yet.
        let b_bytes = minimal_torrent_named("bar");
        let persisted = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .persist_torrent(&b_bytes, "", "identity.torrent")
            .expect("persist B");
        assert!(persisted.content_changed(), "B replaces A");
        assert_eq!(persisted.source_id(), source_id);
        let content_b = persisted.content_id();
        assert_ne!(content_b, content_a);
        assert_eq!(
            svc.content_version(source_id),
            Some(ContentVersion::Current(content_b)),
            "the landed pointer is published before identities become visible"
        );
        assert_eq!(
            a_resolves.load(Ordering::Relaxed),
            0,
            "no sweep has run yet: A's waiter is still parked"
        );

        // DB-first lookup and read of B: both see the landed content.
        let root_b = svc
            .lookup(DATA_INO, "identity.torrent")
            .expect("relookup root")
            .expect("root exists");
        let b_file = svc
            .lookup(root_b.ino, "bar")
            .expect("lookup B file")
            .expect("B file exists");
        let (b_hash, b_index, b_content) = pending_identity(&mut svc, b_file.ino);
        assert_eq!(b_content, content_b);

        let (b_reply, b_resolves, b_errno, b_data) = CountReply::new();
        let b_ticket = expect_leader(super::super::register_content_validated(
            &table,
            b_reply,
            source_id,
            b_content,
            pending_deadline(),
            range_key(&b_hash, b_index),
            || svc.content_version(source_id),
        ));
        assert_eq!(
            b_resolves.load(Ordering::Relaxed),
            0,
            "B's read must not be cancelled by its own publish"
        );

        // The persist thread now publishes the new content and settles only
        // the old content's waiters.
        assert_eq!(
            svc.record_content_published(source_id, content_b),
            1,
            "exactly A's waiter is settled"
        );
        assert_eq!(a_resolves.load(Ordering::Relaxed), 1);
        assert_eq!(a_errno.load(Ordering::Relaxed), libc::EIO);
        assert!(!a_data.load(Ordering::Relaxed));
        assert_eq!(
            b_resolves.load(Ordering::Relaxed),
            0,
            "the landed content's read survives"
        );

        // A repeated publish settles nothing: no double settlement.
        assert_eq!(svc.record_content_published(source_id, content_b), 0);
        assert_eq!(a_resolves.load(Ordering::Relaxed), 1);

        assert_eq!(table.resolve(b_ticket, b"landed-content"), 1);
        assert_eq!(b_resolves.load(Ordering::Relaxed), 1);
        assert_eq!(b_errno.load(Ordering::Relaxed), 0);
        assert!(b_data.load(Ordering::Relaxed));
    }

    /// Land `bytes` at a source path through the real persist path and return
    /// `(source row id, content id, first file id)`.
    fn persist_at(
        svc: &mut FsService,
        bytes: &[u8],
        source_path: &str,
        filename: &str,
    ) -> (i64, i64, i64) {
        let persisted = svc
            .torrent_service
            .as_ref()
            .expect("test service has a torrent service")
            .persist_torrent(bytes, source_path, filename)
            .expect("persist torrent");
        let source_id = persisted.source_id();
        let content_id = persisted.content_id();
        let file_id = svc
            .db
            .as_ref()
            .expect("test service has a db")
            .lock()
            .expect("db lock")
            .get_files_by_torrent_id(source_id)
            .expect("read files")[0]
            .id;
        (source_id, content_id, file_id)
    }

    fn info_hash_of(bytes: &[u8]) -> String {
        hex::encode(
            TorrentInfo::from_bytes(bytes.to_vec())
                .expect("parse torrent")
                .info_hash()
                .expect("info hash"),
        )
    }

    /// The window the review reproduced: content B is already in the database
    /// and the source row points at it, but the projection eviction has not
    /// run, so the inode still carries content A's identity. A second source
    /// keeps A referenced, so the DB can still legitimately resolve A and the
    /// test isolates the source-row/content mismatch.
    fn stale_identity_fixture() -> (
        FsService,
        tempfile::TempDir,
        i64,
        i64,
        Arc<FileIdentity>,
        String,
    ) {
        let (mut svc, cache_dir) = service_with_download();
        let bytes_a = minimal_torrent_bytes();
        let bytes_b = minimal_torrent_named("bar");
        let (source_a, content_a, file_a) = persist_at(&mut svc, &bytes_a, "a", "a.torrent");
        let _shared = persist_at(&mut svc, &bytes_a, "shared", "shared.torrent");
        let (repointed, content_b, _) = persist_at(&mut svc, &bytes_b, "a", "a.torrent");

        assert_eq!(repointed, source_a, "the same source row is repointed");
        assert_ne!(content_a, content_b);
        let a_hash = info_hash_of(&bytes_a);
        let row_hash = {
            let db = svc.db.as_ref().expect("test service has a db");
            let guard = db.lock().expect("db lock");
            guard
                .get_torrent_by_id(source_a)
                .expect("read row")
                .expect("row exists")
                .info_hash
        };
        assert_eq!(
            row_hash,
            info_hash_of(&bytes_b),
            "the source row already serves B while the inode is still A"
        );
        (
            svc,
            cache_dir,
            source_a,
            file_a,
            identity_for(&a_hash, 0, content_a),
            a_hash,
        )
    }

    #[test]
    fn stale_inode_pending_read_keeps_its_own_content() {
        let (mut svc, _cache_dir, source_a, file_a, identity, a_hash) = stale_identity_fixture();

        let (info, info_hash, file_index) = match svc
            .read_data(source_a, file_a, identity, 0, 16)
            .expect("uncached read defers")
        {
            ReadOutcome::Pending {
                info,
                info_hash,
                file_index,
                ..
            } => (info, info_hash, file_index),
            ReadOutcome::Ready(data) => {
                panic!("uncached read must defer, got {} bytes", data.len())
            }
        };
        assert_eq!(
            hex::encode(info.info_hash().expect("pending info hash")),
            a_hash,
            "the deferred read must carry A's piece map, not the row's replacement"
        );
        assert_eq!(info_hash, a_hash);
        assert_eq!(file_index, 0);
        assert!(
            svc.torrent_data_cache.lock().unwrap().is_empty(),
            "a deferred read must not populate L1"
        );
        let cache = svc.torrent_info_cache.lock().unwrap();
        let cached = cache
            .get(&a_hash)
            .expect("A's parsed metadata is cached under A's key");
        assert_eq!(
            hex::encode(cached.info_hash().expect("cached info hash")),
            a_hash,
            "the L3 entry keyed by A must describe A"
        );
    }

    #[test]
    fn stale_inode_ready_arm_resolves_metadata_by_content() {
        let (mut svc, _cache_dir, source_a, file_a, identity, a_hash) = stale_identity_fixture();

        // `offset == file_size` makes the on-disk pre-check short-circuit to
        // true for an empty range, so the Ready arm runs; the engine then
        // returns an empty range, which defers with the metadata it was given.
        let (info, info_hash) = match svc
            .read_data(source_a, file_a, identity, 16, 1)
            .expect("ready-arm read")
        {
            ReadOutcome::Pending {
                info, info_hash, ..
            } => (info, info_hash),
            ReadOutcome::Ready(data) => {
                panic!("an empty range must defer, got {} bytes", data.len())
            }
        };
        assert_eq!(
            hex::encode(info.info_hash().expect("pending info hash")),
            a_hash,
            "the Ready arm must give the engine A's metadata, not B's"
        );
        assert_eq!(info_hash, a_hash);
        assert_eq!(
            svc.metrics.snapshot().l2_hits,
            1,
            "the on-disk fast path must have run"
        );
        assert!(
            svc.torrent_data_cache.lock().unwrap().is_empty(),
            "the Ready arm wrote no L1 entry for the empty range"
        );
        let cache = svc.torrent_info_cache.lock().unwrap();
        let cached = cache
            .get(&a_hash)
            .expect("A's parsed metadata is cached under A's key");
        assert_eq!(
            hex::encode(cached.info_hash().expect("cached info hash")),
            a_hash
        );
    }

    #[test]
    fn stale_inode_read_fails_when_its_content_is_gone() {
        let (mut svc, _cache_dir) = service_with_download();
        let bytes_a = minimal_torrent_bytes();
        let bytes_b = minimal_torrent_named("bar");
        let (source_a, content_a, file_a) = persist_at(&mut svc, &bytes_a, "a", "a.torrent");
        let a_hash = info_hash_of(&bytes_a);
        let (_, content_b, _) = persist_at(&mut svc, &bytes_b, "a", "a.torrent");
        assert_ne!(content_a, content_b);
        assert!(
            svc.db
                .as_ref()
                .expect("test service has a db")
                .lock()
                .expect("db lock")
                .get_torrents_by_infohash(&a_hash)
                .expect("look up content")
                .is_empty(),
            "the replaced content row is gone, so only B is reachable"
        );

        let err = match svc.read_data(source_a, file_a, identity_for(&a_hash, 0, content_a), 0, 16)
        {
            Err(e) => e,
            Ok(ReadOutcome::Ready(data)) => {
                panic!("superseded content served {} bytes", data.len())
            }
            Ok(ReadOutcome::Pending { .. }) => {
                panic!("superseded content produced a deferred transfer")
            }
        };
        assert!(
            matches!(err, FsError::Io(_)),
            "superseded content must settle as EIO, got {err:?}"
        );
        assert!(
            !svc.torrent_info_cache.lock().unwrap().contains_key(&a_hash),
            "a failed content lookup must not cache B under A's key"
        );
        assert!(
            svc.torrent_data_cache.lock().unwrap().is_empty(),
            "a failed content lookup must not populate L1"
        );
    }

    fn pending_identity(svc: &mut FsService, ino: u64) -> (String, i32, i64) {
        let content_id = svc
            .read_identity(ino)
            .expect("the inode is bound to a read identity")
            .content_id;
        match svc.read(ino, 0, 16).expect("read yields a pending outcome") {
            ReadOutcome::Pending {
                info_hash,
                file_index,
                ..
            } => (info_hash, file_index, content_id),
            ReadOutcome::Ready(_) => panic!("an uncached range must not be ready"),
        }
    }

    fn range_key(info_hash: &str, file_index: i32) -> super::super::RangeKey {
        super::super::RangeKey {
            info_hash: info_hash.to_string(),
            file_index,
            offset: 0,
            size: 16,
        }
    }

    fn pending_deadline() -> std::time::Instant {
        std::time::Instant::now() + std::time::Duration::from_secs(60)
    }

    fn expect_leader(outcome: super::super::ContentRegistration) -> u64 {
        match outcome {
            super::super::ContentRegistration::Live(super::super::InsertOutcome::Leader(id)) => id,
            super::super::ContentRegistration::Live(super::super::InsertOutcome::Joined) => {
                panic!("distinct ranges must not coalesce")
            }
            super::super::ContentRegistration::Stale { .. } => {
                panic!("the read's own content must be live")
            }
        }
    }

    /// Wait (bounded) for the detached `add_torrent` thread(s) spawned by
    /// `release` to clear `processing_torrents` — i.e. the DB insert landed —
    /// so a subsequent assertion on DB state observes the settled row.
    fn wait_for_processing_empty(svc: &FsService) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                return;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// Wait (bounded) for the persist thread to have issued `expected` entry
    /// invalidations. It notifies after clearing the pending key, so an empty
    /// `processing_torrents` does not mean the notifications already landed.
    fn wait_for_invalidations(
        recorder: &RecordingInvalidator,
        expected: usize,
    ) -> Vec<(u64, String)> {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        loop {
            let calls = recorder.calls();
            if calls.len() >= expected || std::time::Instant::now() > deadline {
                return calls;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }

    /// Closing an empty `.torrent` file hits the fast path.
    /// `release` discards the inode and returns `NotFound` (ENOENT) — a
    /// zero-byte file is treated as if it never existed — without touching
    /// `processing_torrents` or the DB.
    #[test]
    fn release_empty_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "empty.torrent");

        // release must return immediately — no DB insert, no background spawn.
        let err = svc
            .release(fh)
            .expect_err("empty torrent release should ENOENT");
        assert_eq!(err, FsError::NotFound);

        // Inode is removed.
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        // processing_torrents is untouched (empty).
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// Closing a `.torrent` with invalid (non-parseable) data must
    /// also hit the fast path — no DB insert, no background spawn.
    #[test]
    fn release_invalid_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "bad.torrent");
        // Write garbage that is non-empty but not a valid torrent.
        svc.write(ino, 0, b"not a torrent").expect("write ok");

        svc.release(fh).expect("release ok");

        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// overwriting an existing `metadata/` `.torrent` (already in the
    /// DB) with invalid bencode removes the metadata inode but must ALSO delete
    /// the existing DB row, so the `data/<seed>.torrent/` mirror does not
    /// become an orphan.  The overwrite path is `open` → `setattr(size=0)`
    /// (O_TRUNC) → `write` bad bytes → `release`.
    #[test]
    fn release_invalid_overwrite_removes_existing_db_row() {
        let mut svc = service_with_db();

        // First add a valid torrent and let the detached add_torrent land.
        let (ino, fh) = create_torrent_file(&mut svc, "seed.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write valid");
        svc.release(fh).expect("release valid");
        wait_for_processing_empty(&svc);

        {
            let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(
                db_guard
                    .get_torrent_by_filename_and_source_path("seed.torrent", "")
                    .expect("db query")
                    .is_some(),
                "valid torrent must be in the DB before overwrite"
            );
        }

        // The valid `release` keeps the inode (the DB-backed entry supersedes
        // it), so the overwrite targets the same inode.
        assert!(svc.inode_mgr.inodes.contains_key(&ino));

        // Overwrite with invalid bencode: O_TRUNC then bad write, then close.
        let fh2 = svc.open(ino).expect("reopen").fh;
        svc.setattr(ino, Some(0)).expect("truncate");
        svc.write(ino, 0, b"not a torrent").expect("write bad");
        svc.release(fh2).expect("release invalid overwrite");

        // The metadata inode is removed...
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        // ...and the DB row is gone too — no orphaned data/ mirror.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("seed.torrent", "")
                .expect("db query")
                .is_none(),
            "overwriting with invalid content must remove the existing DB row"
        );
    }

    /// overwriting an existing torrent with empty content
    /// (`cp /dev/null` onto it) hits `release`'s empty-data branch and must
    /// also remove the existing DB row.
    #[test]
    fn release_empty_overwrite_removes_existing_db_row() {
        let mut svc = service_with_db();

        let (ino, fh) = create_torrent_file(&mut svc, "seed.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write valid");
        svc.release(fh).expect("release valid");
        wait_for_processing_empty(&svc);

        // O_TRUNC to empty, then close.
        let fh2 = svc.open(ino).expect("reopen").fh;
        svc.setattr(ino, Some(0)).expect("truncate");
        let err = svc.release(fh2).expect_err("empty release should ENOENT");
        assert_eq!(err, FsError::NotFound);

        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("seed.torrent", "")
                .expect("db query")
                .is_none(),
            "empty overwrite must remove the existing DB row"
        );
    }

    /// an invalid-content overwrite racing a detached `add_torrent`
    /// (the same race `unlink` guards via `wait_for_pending_add`)
    /// must block until the add settles, then delete the row it landed —
    /// otherwise the in-flight add rebuilds the `data/` orphan this fix clears.
    #[test]
    fn release_invalid_overwrite_waits_for_pending_add_then_removes_row() {
        let mut svc = service_with_db();

        let key = ("".to_string(), "seed.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let (ino, fh) = create_torrent_file(&mut svc, "seed.torrent");
        svc.write(ino, 0, b"not a torrent").expect("write bad");

        // Simulate the detached `add_torrent` thread: after a short delay it
        // lands the DB row, then clears the pending key (signalling release to
        // proceed). Without the wait, release would observe no row yet and
        // return `Ok(None)`, leaving this row orphaned in `data/`.
        let db = svc.db.as_ref().unwrap().clone();
        let processing = svc.processing_torrents.clone();
        let cv = svc.processing_torrents_cv.clone();
        let key2 = key.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            {
                let mut db_guard = db.lock().unwrap();
                db_guard
                    .insert_torrent("", "seed", "seed.torrent", 16, "hash-3080", 1)
                    .expect("insert torrent");
            }
            processing.lock().unwrap().remove(&key2);
            cv.notify_one();
        });

        // release must block until the pending add settles, then remove the
        // freshly-inserted row.
        svc.release(fh).expect("release invalid overwrite");

        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("seed.torrent", "")
                .expect("db query")
                .is_none(),
            "invalid overwrite must wait for the pending add, then remove its row"
        );
    }

    /// Closing a valid `.torrent` file must NOT block the caller.
    /// `add_torrent` runs on a background thread; `release` returns `Ok(())`
    /// immediately.  The `processing_torrents` entry is cleaned up by the
    /// background thread after the DB insert completes, and the torrent is
    /// persisted in the DB.
    #[test]
    fn release_valid_torrent_returns_immediately() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "valid.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino, 0, &data).expect("write ok");

        // release should return instantly — the DB insert is deferred to a
        // background thread.
        let start = std::time::Instant::now();
        svc.release(fh).expect("release ok");
        let elapsed = start.elapsed();

        // Even an in-memory DB insert takes < 1s; the point is that
        // release itself does not wait for it.  Allow generous slack
        // for CI scheduling jitter.
        assert!(
            elapsed.as_secs() < 5,
            "release took {:?} — should be near-instant",
            elapsed
        );

        // Inode is NOT removed (data integrity: if add_torrent fails, the
        // file must remain visible to the user).
        assert!(svc.inode_mgr.inodes.contains_key(&ino));

        // Wait for the background thread to finish and clean up
        // processing_torrents.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Verify the torrent was persisted to the DB by the background thread.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let torrent = db_guard
            .get_torrent_by_filename_and_source_path("valid.torrent", "")
            .expect("db query");
        assert!(torrent.is_some(), "torrent should be in DB after release");
        assert_eq!(torrent.unwrap().filename, "valid.torrent");
    }

    // ── data/ readdir paging + listing cache ────────────────────

    /// Insert a torrent with `files` files, all inside one directory, and
    /// return `(torrent root ino, inner directory ino)` resolved the way the
    /// kernel resolves them (lookup by name).
    fn insert_torrent_with_one_directory(
        svc: &mut FsService,
        filename: &str,
        file_count: usize,
    ) -> (u64, u64) {
        let source_id = {
            let db = svc.db.as_ref().unwrap().clone();
            let mut guard = db.lock().unwrap();
            match guard
                .insert_torrent(
                    "cat",
                    "collection",
                    filename,
                    4096,
                    "hash-pages",
                    file_count as i64,
                )
                .unwrap()
            {
                InsertTorrentResult::Inserted(id) => id,
                other => panic!("expected Inserted, got {other:?}"),
            }
        };
        let files: Vec<FileEntry> = (0..file_count)
            .map(|i| FileEntry {
                path: format!("disc/track_{i:05}.flac"),
                size: 4096,
            })
            .collect();
        {
            let db = svc.db.as_ref().unwrap().clone();
            let mut guard = db.lock().unwrap();
            guard.insert_files(source_id, &files).unwrap();
        }

        let source_dir = svc
            .lookup(DATA_INO, "cat")
            .expect("lookup cat")
            .expect("cat source-path directory");
        let root = svc
            .lookup(source_dir.ino, filename)
            .expect("lookup torrent root")
            .expect("torrent root");
        let inner = svc
            .lookup(root.ino, "disc")
            .expect("lookup disc")
            .expect("inner directory");
        (root.ino, inner.ino)
    }

    /// The kernel reads a `data/` directory one page at a time and passes back
    /// the offset of the last entry it accepted. Walking a directory that way
    /// must yield every entry exactly once and strictly increasing offsets —
    /// the listing cache serves the later pages from the list the first page
    /// built, so a shifted or stale list would duplicate or drop entries.
    #[test]
    fn readdir_pages_to_the_end_without_gaps_or_duplicates() {
        let mut svc = service_with_db();
        let file_count = 300;
        let (_root_ino, dir_ino) =
            insert_torrent_with_one_directory(&mut svc, "pages.torrent", file_count);

        let mut names = Vec::new();
        let mut offset = 0i64;
        loop {
            let page = svc.readdir(dir_ino, offset).expect("readdir page");
            if page.is_empty() {
                break;
            }
            assert!(
                page.windows(2).all(|pair| pair[0].offset < pair[1].offset),
                "offsets must strictly increase within a page"
            );
            assert!(
                page[0].offset > offset,
                "page must continue after the requested offset"
            );
            offset = page.last().unwrap().offset;
            names.extend(page.into_iter().map(|entry| entry.name));
        }

        // `.`, `..` and the files themselves, each exactly once.
        assert_eq!(
            names.len(),
            file_count + 2,
            "walk must cover the whole listing"
        );
        let mut files: Vec<&String> = names
            .iter()
            .filter(|name| name.ends_with(".flac"))
            .collect();
        files.sort();
        files.dedup();
        assert_eq!(files.len(), file_count);
    }

    /// The listing cache is keyed on the state it was derived from: a torrent
    /// added after the listing was built must show up on the next `readdir`,
    /// not only after a restart or a timer.
    #[test]
    fn readdir_reflects_a_torrent_added_after_the_listing_was_cached() {
        let mut svc = service_with_db();
        insert_torrent_row(&svc, "first.torrent");
        assert!(readdir_names(&mut svc, DATA_INO).contains(&"first.torrent".to_string()));

        insert_torrent_row(&svc, "second.torrent");
        let names = readdir_names(&mut svc, DATA_INO);
        assert!(
            names.contains(&"first.torrent".to_string())
                && names.contains(&"second.torrent".to_string()),
            "listing must be refreshed after the database changed, got {names:?}"
        );
    }

    /// A `.torrent` whose background add is still in flight shows up in `data/`
    /// as a provisional entry. The listing cache is keyed on that in-flight set:
    /// when the add goes away without a single SQL statement (no database, or the
    /// first write failing — `total_changes` only counts statements that ran),
    /// the next `readdir` must not keep serving the provisional entry.
    #[test]
    fn readdir_drops_a_provisional_entry_whose_add_left_without_a_write() {
        let mut svc = service_with_db();
        insert_torrent_row(&svc, "persisted.torrent");

        svc.processing_torrents
            .lock()
            .unwrap()
            .insert((String::new(), "ghost.torrent".to_string()), ());
        let names = readdir_names(&mut svc, DATA_INO);
        assert!(
            names.contains(&"ghost.torrent".to_string()),
            "listing must show the in-flight add, got {names:?}"
        );

        svc.processing_torrents.lock().unwrap().clear();
        let names = readdir_names(&mut svc, DATA_INO);
        assert!(
            !names.contains(&"ghost.torrent".to_string()),
            "the provisional entry must vanish with its add, got {names:?}"
        );
        assert!(names.contains(&"persisted.torrent".to_string()));
    }

    fn insert_torrent_row(svc: &FsService, filename: &str) {
        let db = svc.db.as_ref().unwrap().clone();
        let mut guard = db.lock().unwrap();
        guard
            .insert_torrent("", "test", filename, 4096, &format!("hash-{filename}"), 1)
            .unwrap();
    }

    fn readdir_names(svc: &mut FsService, ino: u64) -> Vec<String> {
        svc.readdir(ino, 0)
            .expect("readdir")
            .into_iter()
            .map(|entry| entry.name)
            .collect()
    }

    /// a DB write failure (e.g. disk full → SQLite
    /// `SystemIoFailure`) must not be silent.  Persistence runs on a detached
    /// thread (keeping the FUSE dispatcher unblocked) and records the
    /// failure; the next user-visible `lookup`/`readdir` of `data/` surfaces
    /// it as `FsError::Database` → EIO.  The kernel drops the FUSE_RELEASE
    /// reply error, so `release` itself cannot propagate it.
    #[test]
    fn release_propagates_db_write_failure_as_eio() {
        let db = Database::open_in_memory().expect("in-memory db");
        // Simulate a full disk: make the connection read-only so the INSERT
        // fails.  Any SQLite write error maps to FsError::Database → EIO.
        db.conn
            .execute_batch("PRAGMA query_only = ON;")
            .expect("set query_only");
        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let torrent_service = TorrentService::new(db_arc.clone(), None);
        let content_versions = torrent_service.content_versions();
        let mut svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(torrent_service),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            processing_torrents_cv: Arc::new(Condvar::new()),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions,
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        };

        let (ino, fh) = create_torrent_file(&mut svc, "full.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write ok");

        // release returns Ok (persistence is async); the failure is recorded
        // for the data/ mirror to surface.
        svc.release(fh).expect("release ok");
        wait_for_processing_empty(&svc);

        // The metadata inode stays visible so the user can retry or delete.
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        // No leaked pending key.
        assert!(svc.processing_torrents.lock().unwrap().is_empty());

        // The failure is surfaced as EIO by the data/ mirror, not silently
        // omitted.
        let lookup_err = svc
            .lookup(DATA_INO, "full.torrent")
            .expect_err("failed torrent lookup must EIO");
        assert!(
            matches!(lookup_err, FsError::Database(_)),
            "expected FsError::Database, got {:?}",
            lookup_err
        );
        let readdir_err = svc
            .readdir(DATA_INO, 0)
            .expect_err("failed torrent readdir must EIO");
        assert!(
            matches!(readdir_err, FsError::Database(_)),
            "expected FsError::Database, got {:?}",
            readdir_err
        );
    }

    /// `processing_torrents` is NOT held during `add_torrent` —
    /// the lock is released before the background thread is spawned.  Two
    /// `.torrent` files in the root directory (both `source_path == ""`)
    /// must both be processed and persisted — the dedup key is
    /// `(source_path, filename)`, so they don't collide.
    #[test]
    fn release_does_not_hold_processing_lock_during_add() {
        let mut svc = service_with_db();
        let (ino1, fh1) = create_torrent_file(&mut svc, "a.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino1, 0, &data).expect("write ok");
        svc.release(fh1).expect("release ok");

        // At this point the background thread for a.torrent may or may not
        // have finished.  Either way, processing_torrents should be lockable
        // without blocking (it would be held only briefly by the background
        // thread's cleanup).  The second release for a *different* file
        // must succeed immediately — even though both share source_path "".
        let (ino2, fh2) = create_torrent_file(&mut svc, "b.torrent");
        svc.write(ino2, 0, &data).expect("write ok");

        let start = std::time::Instant::now();
        svc.release(fh2).expect("release ok");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 5,
            "second release took {:?} — should not wait for first",
            elapsed
        );

        // Wait for both background threads to finish.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Both torrents must be in the DB — the tuple dedup key prevents
        // the second from being skipped as a duplicate of the first.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let t1 = db_guard
            .get_torrent_by_filename_and_source_path("a.torrent", "")
            .expect("db query");
        assert!(t1.is_some(), "a.torrent should be in DB");
        let t2 = db_guard
            .get_torrent_by_filename_and_source_path("b.torrent", "")
            .expect("db query");
        assert!(t2.is_some(), "b.torrent should be in DB");
    }

    /// `flush` for an empty `.torrent` returns `EINVAL` — a zero-byte seed
    /// has no metadata to parse, so `cp` must fail on close with the
    /// invalid-seed signal rather than silently succeeding.
    #[test]
    fn flush_empty_torrent_returns_einval() {
        let mut svc = service_with_db();
        let (ino, _fh) = create_torrent_file(&mut svc, "empty.torrent");

        let err = svc.flush(ino).unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
    }

    /// a non-empty but unparseable `.torrent` fails `flush` with a
    /// reason-carrying `CorruptTorrent` (→ EINVAL), so the user sees
    /// `Invalid .torrent file: <reason>` rather than a bare "Invalid
    /// argument" that cannot distinguish an invalid seed from other I/O
    /// errors.
    #[test]
    fn flush_invalid_torrent_returns_corrupt_torrent() {
        let mut svc = service_with_db();
        let (ino, _fh) = create_torrent_file(&mut svc, "bad.torrent");
        svc.write(ino, 0, b"not a torrent").expect("write ok");

        let err = svc.flush(ino).unwrap_err();
        match err {
            FsError::CorruptTorrent(reason) => {
                assert!(
                    !reason.is_empty(),
                    "invalid .torrent must carry a non-empty reason"
                );
            }
            other => panic!("expected CorruptTorrent, got {:?}", other),
        }
    }

    // ── unlink-while-open keeps the inode alive ───────────────

    /// `unlink` on a file with no open handle destroys the
    /// inode immediately — the name is gone and the inode is absent.
    #[test]
    fn unlink_closed_file_destroys_inode() {
        let mut svc = bare_service();
        // Create a file, write valid torrent bytes, and close the handle.
        // `release` for valid data does NOT remove the inode (no DB in
        // bare_service, so the add_torrent path is skipped but the inode
        // lingers), so the inode is alive with zero open handles — the
        // precondition for immediate destruction on unlink.
        let created = svc.create(METADATA_INO, "closed.torrent").expect("create");
        let ino = created.attr.ino;
        let fh = created.fh;
        let data = minimal_torrent_bytes();
        svc.write(ino, 0, &data).expect("write valid torrent");
        svc.release(fh).expect("close handle");
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 0);

        // unlink with no open handle → immediate destruction.
        svc.unlink(METADATA_INO, "closed.torrent").expect("unlink");
        assert_eq!(
            svc.inode_mgr
                .find_child_by_name(METADATA_INO, "closed.torrent"),
            None
        );
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// an open handle keeps read/write working after unlink, and
    /// the inode is destroyed only on the LAST release (not the first).
    #[test]
    fn unlink_while_open_keeps_handle_until_last_release() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "open.torrent").expect("create");
        let ino = created.attr.ino;
        let fh1 = created.fh;
        // Open a second handle on the same inode.
        let fh2 = match svc.open(ino).expect("open") {
            OpenOutcome { fh, .. } => fh,
        };
        svc.write(ino, 0, b"hello").expect("write");

        // unlink while both handles are open.
        svc.unlink(METADATA_INO, "open.torrent").expect("unlink");
        // Name gone, inode lingers (unlinked).
        assert_eq!(
            svc.inode_mgr
                .find_child_by_name(METADATA_INO, "open.torrent"),
            None
        );
        assert!(svc.inode_mgr.is_unlinked_file(ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 2);

        // read/write via fh1's ino still succeed.
        svc.write(ino, 5, b"!").expect("write after unlink");
        let outcome = svc.read(ino, 0, 6).expect("read after unlink");
        let buf = match outcome {
            ReadOutcome::Ready(b) => b,
            _ => panic!("expected Ready read"),
        };
        assert_eq!(&buf, b"hello!");

        // First release: inode survives (fh2 still open).
        svc.release(fh1).expect("release fh1");
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 1);

        // Second (last) release: inode destroyed.
        svc.release(fh2).expect("release fh2");
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// after unlink, `readdir` no longer lists the file even
    /// though its inode lingers for an open handle.
    #[test]
    fn unlink_hides_name_from_readdir_while_open() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "hidden.torrent").expect("create");
        let ino = created.attr.ino;
        let fh = created.fh;
        // Visible before unlink.
        let names: Vec<String> = svc
            .readdir(METADATA_INO, 0)
            .expect("readdir")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "hidden.torrent"));

        svc.unlink(METADATA_INO, "hidden.torrent").expect("unlink");

        // Gone from readdir after unlink.
        let names: Vec<String> = svc
            .readdir(METADATA_INO, 0)
            .expect("readdir")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(!names.iter().any(|n| n == "hidden.torrent"));
        // Inode still alive.
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        svc.release(fh).expect("release");
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// a second `unlink` of the same name returns `ENOENT`
    /// because `find_child_by_name` skips the unlinked inode.
    #[test]
    fn unlink_twice_returns_not_found() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "twice.torrent").expect("create");
        let _fh = created.fh;
        svc.unlink(METADATA_INO, "twice.torrent")
            .expect("first unlink");
        let err = svc.unlink(METADATA_INO, "twice.torrent").unwrap_err();
        assert_eq!(err, FsError::NotFound);
    }

    /// creating a new file with the same name as an unlinked-but-
    /// still-open inode succeeds (the old name is freed even though the
    /// old inode lingers).
    #[test]
    fn recreate_name_after_unlink_while_open_succeeds() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "reuse.torrent").expect("create");
        let old_ino = created.attr.ino;
        let _fh = created.fh;
        svc.unlink(METADATA_INO, "reuse.torrent").expect("unlink");
        // Re-create with the same name — must not collide with the lingering
        // unlinked inode.
        let created2 = svc.create(METADATA_INO, "reuse.torrent").expect("recreate");
        assert_ne!(created2.attr.ino, old_ino);
        // Both inodes coexist: old one (unlinked, open) + new one (linked).
        assert!(svc.inode_mgr.is_unlinked_file(old_ino));
        assert!(!svc.inode_mgr.is_unlinked_file(created2.attr.ino));
    }

    /// an unlinked `.torrent` is NOT re-persisted on release —
    /// the torrent was already removed from the DB by `unlink`, so
    /// `release` just destroys the buffered inode.
    #[test]
    fn unlinked_torrent_not_repersisted_on_release() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "repersist.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes()).expect("write");
        svc.unlink(METADATA_INO, "repersist.torrent")
            .expect("unlink");
        assert!(svc.inode_mgr.is_unlinked_file(ino));
        // release must NOT spawn a background add_torrent for an unlinked
        // file — processing_torrents stays empty.
        svc.release(fh).expect("release");
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
        // Inode destroyed on last release.
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// a fast `cp` immediately followed by `rm` must not leave an
    /// orphaned DB row. `unlink` waits (bounded) for the detached
    /// `add_torrent` (spawned by `release`) to settle before calling
    /// `remove_torrent`, so the row the add just landed is found and deleted
    /// instead of surviving as a ghost `data/` entry with no metadata/.
    #[test]
    fn unlink_waits_for_pending_add_then_removes_row() {
        let mut svc = service_with_db();

        let key = ("".to_string(), "foo.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let _ino = writable_file_ino(&mut svc, "foo.torrent");

        // Simulate the detached `add_torrent` thread: after a short delay it
        // lands the DB row, then clears the pending key (signalling unlink to
        // proceed). Without the pending-add wait, unlink would observe no row
        // yet, return `Ok(None)`, and this row would be orphaned.
        let db = svc.db.as_ref().unwrap().clone();
        let processing = svc.processing_torrents.clone();
        let cv = svc.processing_torrents_cv.clone();
        let key2 = key.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            {
                let mut db_guard = db.lock().unwrap();
                db_guard
                    .insert_torrent("", "foo", "foo.torrent", 16, "hash-2967", 1)
                    .expect("insert torrent");
            }
            processing.lock().unwrap().remove(&key2);
            cv.notify_one();
        });

        // unlink must block until the pending add settles, then find + delete
        // the freshly-inserted row.
        let removed = svc
            .unlink(METADATA_INO, "foo.torrent")
            .expect("unlink must succeed");
        assert!(removed.is_some(), "unlink must remove the landed DB row");

        // No orphan: the row is gone.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db_guard
                .get_torrent_by_filename_and_source_path("foo.torrent", "")
                .expect("db query")
                .is_none(),
            "torrent row must be removed, not left as a data/ orphan"
        );
    }

    /// if the pending add never settles (stuck), `unlink` must fail
    /// cleanly (leaving the inode visible) instead of returning `Ok(None)` and
    /// letting the eventual add land an orphaned row. Exercises the
    /// injected-deadline path without hard-waiting the 5s production timeout.
    #[test]
    fn unlink_fails_cleanly_when_add_stays_pending() {
        let mut svc = service_with_db();
        let ino = writable_file_ino(&mut svc, "stuck.torrent");

        // Pending forever — no background thread will clear it.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(("".to_string(), "stuck.torrent".to_string()), ());

        let started = std::time::Instant::now();
        let result = svc.unlink_with_pending_timeout(
            METADATA_INO,
            "stuck.torrent",
            Duration::from_millis(100),
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout must honor the injected deadline, took {:?}",
            started.elapsed()
        );
        assert!(
            result.is_err(),
            "unlink over a stuck pending add must fail, not orphan"
        );

        // Clean failure: the inode is untouched and still visible under
        // metadata/, so the user can retry once the add settles.
        assert!(!svc.inode_mgr.is_unlinked_file(ino));
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// a directory whose only remaining
    /// child is an unlinked-but-still-open file must be removable — POSIX
    /// treats "only unlinked-but-open files" as empty. Before the fix,
    /// `rmdir` saw the lingering unlinked `File` inode as a child and
    /// wrongly returned `ENOTEMPTY`.
    #[test]
    fn rmdir_succeeds_after_unlink_while_open() {
        let mut svc = bare_service();
        // Create a subdirectory under metadata/ and a .torrent inside it.
        let dir = svc.mkdir(METADATA_INO, "sub").expect("mkdir sub").ino;
        let created = svc.create(dir, "x.torrent").expect("create");
        let _fh = created.fh;
        // Unlink the file while its handle is open — inode lingers (unlinked).
        svc.unlink(dir, "x.torrent").expect("unlink");
        assert!(svc.inode_mgr.is_unlinked_file(created.attr.ino));
        assert!(svc.inode_mgr.inodes.contains_key(&created.attr.ino));
        // The directory now has no *visible* children; rmdir must succeed.
        svc.rmdir(METADATA_INO, "sub")
            .expect("rmdir after unlink-while-open");
        assert!(!svc.inode_mgr.inodes.contains_key(&dir));
    }

    /// the L1 range cache is bounded by
    /// `MAX_L1_ENTRIES`.  When full, inserting a new entry clears the
    /// cache so memory does not grow unboundedly.
    #[test]
    fn l1_cache_evicts_when_full() {
        let svc = bare_service();
        {
            let mut cache = svc.torrent_data_cache.lock().unwrap();
            for i in 0..MAX_L1_ENTRIES {
                let key = format!("hash:file:{}:4096", i);
                cache.insert(key, vec![0xABu8; 4096]);
            }
            assert_eq!(cache.len(), MAX_L1_ENTRIES);
        }
        // One more insert triggers eviction.
        {
            let mut cache = svc.torrent_data_cache.lock().unwrap();
            if cache.len() >= MAX_L1_ENTRIES {
                cache.clear();
            }
            cache.insert("hash:file:999:4096".to_string(), vec![0xCDu8; 4096]);
        }
        let cache = svc.torrent_data_cache.lock().unwrap();
        assert_eq!(
            cache.len(),
            1,
            "cache should have been cleared and now hold only the new entry"
        );
        assert!(cache.contains_key("hash:file:999:4096"));
    }

    /// L1 cache keys include `size` so a
    /// read with a different size at the same offset does not collide.
    #[test]
    fn l1_cache_key_includes_size() {
        let svc = bare_service();
        let mut cache = svc.torrent_data_cache.lock().unwrap();
        // Simulate a 4 KB read at offset 0 and a 128 KB read at offset 0.
        let key_small = format!("{}:{}:{}:{}", "hash", 1, 0u64, 4096u32);
        let key_large = format!("{}:{}:{}:{}", "hash", 1, 0u64, 131072u32);
        assert_ne!(key_small, key_large, "keys must differ by size");
        cache.insert(key_small, vec![0u8; 4096]);
        cache.insert(key_large, vec![0u8; 131072]);
        // Both entries coexist — no truncation.
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_non_ascii_filenames_db_round_trip() {
        // Verify that multi-file torrents with non-ASCII (UTF-8)
        // file names can be inserted, retrieved, and looked up by name
        // through the DB — the same path FUSE lookup/readdir uses.
        let name = "测试种子".as_bytes();
        let file1 = "你好.txt".as_bytes().to_vec();
        let file2 = "世界.txt".as_bytes().to_vec();
        let torrent_bytes = crate::infrastructure::metadata::build_multifile_torrent(
            name,
            &[(file1, 16), (file2, 16)],
            16,
        );
        let info = TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent");
        let metadata = info.metadata().expect("get metadata");
        let info_hash_hex = hex::encode(metadata.info_hash);

        let dir = tempfile::TempDir::new().unwrap();
        let mut db = Database::open(&dir.path().join("test.db")).unwrap();

        let file_entries: Vec<FileEntry> = metadata
            .files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();

        let result = db
            .insert_torrent_with_files(
                "",
                &metadata.name,
                "test.torrent",
                metadata.total_size as i64,
                &info_hash_hex,
                metadata.num_files as i64,
                &file_entries,
            )
            .expect("insert");
        let torrent_id = match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("expected insertion"),
        };

        // Root directory = torrent name (first component of file_path).
        let root_dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .expect("get root dirs");
        assert_eq!(root_dirs.len(), 1);
        assert_eq!(root_dirs[0].name, "测试种子");

        // Files under the root directory.
        let dir_files = db
            .get_files_in_directory(root_dirs[0].id)
            .expect("get dir files");
        assert_eq!(dir_files.len(), 2);
        assert_eq!(dir_files[0].name, "你好.txt");
        assert_eq!(dir_files[1].name, "世界.txt");

        // Simulate FUSE lookup: find a file by name in the directory.
        let found = dir_files.iter().find(|f| f.name == "你好.txt");
        assert!(found.is_some(), "lookup should find 你好.txt");

        // The read path maps file_id → file_index via `files.iter().position`,
        // where `files` comes from `get_files_by_torrent_id` (ordered by `id`).
        // This must match libtorrent's `info.files()` order — a divergence
        // would use the wrong file_index → wrong piece range → EIO. Both DB
        // insertion (`insert_torrent_with_files`) and `info.files()` iterate
        // the file list in the same order.
        let all_files = db
            .get_files_by_torrent_id(torrent_id)
            .expect("get all files");
        let info_files = info.files().expect("get libtorrent files");
        assert_eq!(all_files.len(), info_files.len());
        for (db_idx, db_file) in all_files.iter().enumerate() {
            // Each DB file's `path` must match the corresponding
            // libtorrent file's path at the same index.
            assert_eq!(
                db_file.path, info_files[db_idx].path,
                "DB file at index {} (id={}, name={:?}) has path {:?} \
                 but libtorrent file at index {} has path {:?} — \
                 ordering mismatch would cause wrong file_index in reads",
                db_idx, db_file.id, db_file.name, db_file.path, db_idx, info_files[db_idx].path,
            );
        }
        // Two calls must return the same order (stability).
        let all_files_2 = db
            .get_files_by_torrent_id(torrent_id)
            .expect("get all files (2nd call)");
        let ids_1: Vec<i64> = all_files.iter().map(|f| f.id).collect();
        let ids_2: Vec<i64> = all_files_2.iter().map(|f| f.id).collect();
        assert_eq!(ids_1, ids_2, "DB file ordering must be stable across calls");
    }

    #[test]
    fn test_non_utf8_filenames_db_round_trip() {
        // Non-UTF-8 (e.g. GBK) file names are sanitized by
        // libtorrent to '_' before reaching Rust.  The sanitized names
        // are valid UTF-8 and can be stored/retrieved from the DB.
        let name: &[u8] = b"\xb2\xe2\xca\xd4"; // 测试 in GBK
        let file1: &[u8] = b"\xc4\xe3\xba\xc3.txt"; // 你好.txt in GBK
        let file2: &[u8] = b"\xca\xc0\xbd\xe7.txt"; // 世界.txt in GBK
        let torrent_bytes = crate::infrastructure::metadata::build_multifile_torrent(
            name,
            &[(file1.to_vec(), 16), (file2.to_vec(), 16)],
            16,
        );
        let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
        let metadata = info.metadata().expect("get metadata");

        // Sanitized names must be valid UTF-8 for SQLite TEXT storage.
        for f in &metadata.files {
            assert!(
                std::str::from_utf8(f.path.as_bytes()).is_ok(),
                "sanitized path must be valid UTF-8: {:?}",
                f.path
            );
        }

        let info_hash_hex = hex::encode(metadata.info_hash);
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = Database::open(&dir.path().join("test.db")).unwrap();

        let file_entries: Vec<FileEntry> = metadata
            .files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();

        let result = db
            .insert_torrent_with_files(
                "",
                &metadata.name,
                "test.torrent",
                metadata.total_size as i64,
                &info_hash_hex,
                metadata.num_files as i64,
                &file_entries,
            )
            .expect("insert");
        let torrent_id = match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("expected insertion"),
        };

        // Verify files are retrievable.
        let all_files = db.get_files_by_torrent_id(torrent_id).expect("get files");
        assert_eq!(all_files.len(), 2);
    }

    /// `guard_empty_read` must reject empty data for a non-zero
    /// `size` with `FsError::NoPeers` (→ ENODATA), preventing the silent
    /// 0-byte EOF that `read_data`'s sync path would otherwise produce.
    #[test]
    fn sync_empty_data_guard_maps_to_no_peers() {
        // Empty data, non-zero size → error.
        let err = guard_empty_read(&[], 4096).unwrap_err();
        assert!(matches!(err, FsError::NoPeers(_)));
    }
    /// when `pieces_on_disk` reports the range as on-disk but
    /// `read_file_range` returns empty, `read_data` must defer to the download
    /// engine (`Pending`) rather than fail immediately with ENODATA.  Reading
    /// at `offset == file_size` makes `pieces_on_disk` short-circuit to `true`
    /// (empty range) and the engine return `Ok(Vec::new())` immediately, so the
    /// sync empty-read deferral is exercised without a live seeder.
    #[test]
    fn sync_empty_read_defers_to_download_path() {
        let torrent_bytes = minimal_torrent_bytes(); // single 16-byte file
        let info = TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent");
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let files = info.files().expect("files");

        let mut db = Database::open_in_memory().expect("in-memory db");
        let file_entries: Vec<FileEntry> = files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();
        let torrent_id = match db
            .insert_torrent_with_files(
                "src",
                "foo",
                "foo.torrent",
                16,
                &info_hash,
                1,
                &file_entries,
            )
            .expect("insert torrent")
        {
            InsertTorrentResult::Inserted(id) => id,
            other => panic!("unexpected insert result: {:?}", other),
        };
        db.set_torrent_data(torrent_id, &torrent_bytes)
            .expect("set torrent data");
        let db_arc = Arc::new(Mutex::new(db));
        let file_id = db_arc
            .lock()
            .unwrap()
            .get_files_by_torrent_id(torrent_id)
            .expect("get files")[0]
            .id;

        let cache_dir = tempfile::TempDir::new().expect("cache dir");
        let config = TorrentfsConfig::default_config();
        let download_service =
            Arc::new(DownloadService::new(cache_dir.path(), &config).expect("download service"));

        let metrics = Arc::new(Metrics::new());
        let torrent_service = TorrentService::new(db_arc.clone(), Some(download_service.clone()));
        let content_versions = torrent_service.content_versions();
        let mut svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(torrent_service),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            persist_errors: Arc::new(Mutex::new(HashMap::new())),
            processing_torrents_cv: Arc::new(Condvar::new()),
            download_service: Some(download_service),
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
            data_readdir_cache: None,
            pending_projection_evictions: Arc::new(Mutex::new(Vec::new())),
            metadata_index: None,
            metadata_generation: 0,
            read_identities: HashMap::new(),
            cancelled_reads: Arc::new(Mutex::new(Vec::new())),
            content_versions,
            read_cancel_sink: Arc::new(OnceLock::new()),
            buffered_torrents: HashMap::new(),
            pending_index_cache: Arc::new(Mutex::new(PendingIndexCache::new())),
        };

        // `offset == file_size` (16) → `pieces_on_disk` says "empty range"
        // (true) and `read_file_range` returns empty → defer, not error.
        svc.ensure_file_identities(torrent_id);
        let identity = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .prepared_file_identity(torrent_id, file_id)
            .expect("source identities are prepared on insert");
        let outcome = svc
            .read_data(torrent_id, file_id, identity, 16, 1)
            .expect("read_data must succeed");
        assert!(
            matches!(outcome, ReadOutcome::Pending { .. }),
            "sync empty read must defer to the download path (Pending), not error"
        );
    }

    /// `guard_empty_read` must pass through non-empty data and
    /// zero-size reads without error.
    #[test]
    fn sync_empty_data_guard_passes_nonempty_and_zero_size() {
        // Non-empty data, non-zero size → ok.
        assert!(guard_empty_read(b"data", 4).is_ok());
        // Empty data, zero size → ok (legitimate EOF).
        assert!(guard_empty_read(&[], 0).is_ok());
    }

    /// `split_data_dir_parent` maps a top-level source-path to
    /// `DATA_INO` and a nested path to its parent's `SourcePathDir` inode.
    #[test]
    fn split_data_dir_parent_maps_correctly() {
        // Top-level: parent is data/ root.
        let (parent, name) = FsService::split_data_dir_parent("cat-a");
        assert_eq!(parent, DATA_INO);
        assert_eq!(name, "cat-a");

        // Nested: parent is the SourcePathDir inode of the parent path.
        let (parent, name) = FsService::split_data_dir_parent("cat-a/sub");
        assert_eq!(parent, InodeManager::make_source_path_dir_ino("cat-a"));
        assert_eq!(name, "sub");

        // Deeply nested.
        let (parent, name) = FsService::split_data_dir_parent("a/b/c");
        assert_eq!(parent, InodeManager::make_source_path_dir_ino("a/b"));
        assert_eq!(name, "c");
    }

    /// `inval_data_entry` is a no-op when no `Notifier` is wired
    /// (the test/constructor path — `OnceLock` is empty).  The call must
    /// not panic.
    #[test]
    fn inval_data_entry_noop_without_notifier() {
        let svc = bare_service();
        // No notifier set — this must be a silent no-op.
        svc.inval_data_entry(DATA_INO, "foo.torrent");
    }

    /// Records every kernel dentry invalidation the service issues, so tests
    /// can assert which `data/` entry a daemon-side removal drops.
    #[derive(Default)]
    struct RecordingInvalidator {
        calls: Mutex<Vec<(u64, String)>>,
    }

    impl RecordingInvalidator {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        fn calls(&self) -> Vec<(u64, String)> {
            self.calls.lock().unwrap().clone()
        }
    }

    impl EntryInvalidator for RecordingInvalidator {
        fn inval_entry(&self, parent: u64, name: &OsStr) -> std::io::Result<()> {
            self.calls
                .lock()
                .unwrap()
                .push((parent, name.to_string_lossy().into_owned()));
            Ok(())
        }
    }

    /// A directory rename must invalidate the OLD projection entry under the
    /// old parent, so `data/<old>` stops resolving at once.  Renaming to a
    /// different parent must still target that old parent, and the destination
    /// name must never be the invalidation target.
    #[test]
    fn rename_dir_invalidates_the_old_projection_entry() {
        let mut svc = bare_service();
        let recorder = RecordingInvalidator::new();
        svc.notifier.set(Some(recorder.clone())).ok();

        let a = svc.mkdir(METADATA_INO, "a").expect("mkdir a").ino;
        svc.mkdir(a, "tree").expect("mkdir a/tree");
        let b = svc.mkdir(METADATA_INO, "b").expect("mkdir b").ino;

        // Cross-parent rename: `a/tree` -> `b/sub`.
        svc.rename(a, "tree", b, "sub")
            .expect("rename a/tree -> b/sub");
        assert_eq!(
            recorder.calls(),
            vec![(
                InodeManager::make_source_path_dir_ino("a"),
                "tree".to_string()
            )],
            "the old parent's `tree` entry must be the invalidation target"
        );

        // Root-level rename: `b/sub` -> `moved`.
        svc.rename(b, "sub", METADATA_INO, "moved")
            .expect("rename b/sub -> moved");
        assert_eq!(
            recorder.calls(),
            vec![
                (
                    InodeManager::make_source_path_dir_ino("a"),
                    "tree".to_string()
                ),
                (
                    InodeManager::make_source_path_dir_ino("b"),
                    "sub".to_string()
                ),
            ],
            "moving a nested directory to the root invalidates it under its old parent"
        );

        // A directory already at the root is invalidated under `data/`.
        svc.mkdir(METADATA_INO, "top").expect("mkdir top");
        svc.rename(METADATA_INO, "top", METADATA_INO, "top2")
            .expect("rename top -> top2");
        assert_eq!(
            recorder.calls(),
            vec![
                (
                    InodeManager::make_source_path_dir_ino("a"),
                    "tree".to_string()
                ),
                (
                    InodeManager::make_source_path_dir_ino("b"),
                    "sub".to_string()
                ),
                (DATA_INO, "top".to_string()),
            ],
            "a root-level rename must invalidate the old name under data/"
        );
    }

    /// An overwrite whose close lands while the previous content is still
    /// being persisted must not be dropped. `release` waits for the in-flight
    /// add to settle and then persists the new bytes; before the fix it
    /// returned early, leaving the OLD bytes in the database while
    /// `metadata/` already showed the new torrent.
    #[test]
    fn release_overwrite_waits_for_in_flight_add_then_persists() {
        let mut svc = service_with_db();
        let key = ("".to_string(), "race.torrent".to_string());

        // The first close's detached persist is still running.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let (ino, fh) = create_torrent_file(&mut svc, "race.torrent");
        let bytes = minimal_torrent_named("second");
        svc.write(ino, 0, &bytes).expect("write");

        // The in-flight add settles shortly after the second close starts
        // waiting on the pending key.
        let processing = svc.processing_torrents.clone();
        let cv = svc.processing_torrents_cv.clone();
        let settle = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(100));
            processing.lock().unwrap().remove(&key);
            cv.notify_one();
        });

        svc.release(fh)
            .expect("release must persist the new bytes after the wait");
        settle.join().expect("settle thread");
        wait_for_processing_empty(&svc);

        let db = svc.db.as_ref().unwrap().lock().unwrap();
        let row = db
            .get_torrent_by_filename_and_source_path("race.torrent", "")
            .unwrap()
            .expect("the overwrite must have persisted a row");
        assert_eq!(
            row.torrent_data.as_deref(),
            Some(bytes.as_slice()),
            "the database must hold the second close's bytes, not the first's"
        );
    }

    /// An overwrite must evict the cached projection inodes of the replaced
    /// content: otherwise an inode the kernel already holds keeps reporting
    /// the old file tree (name, size, children) after the database moved on.
    #[test]
    fn release_overwrite_evicts_the_old_projection_inodes() {
        let mut svc = service_with_db();

        let (ino, fh) = create_torrent_file(&mut svc, "swap.torrent");
        svc.write(ino, 0, &minimal_torrent_named("first"))
            .expect("prime write");
        svc.release(fh).expect("prime release");
        wait_for_processing_empty(&svc);

        // Warm `data/` with the first content's root and file inodes.
        let old_root = svc
            .lookup(DATA_INO, "swap.torrent")
            .expect("lookup")
            .expect("the first content must project");
        let old_file = svc
            .lookup(old_root.ino, "first")
            .expect("lookup")
            .expect("the single-file torrent projects its content name");
        assert!(svc.inode_mgr.data_inodes.contains_key(&old_file.ino));

        // The replacement must also invalidate the kernel's dentry for the
        // torrent root, otherwise the old attrs stay cached for the TTL.
        let recorder = RecordingInvalidator::new();
        svc.notifier.set(Some(recorder.clone())).ok();

        // Overwrite with a torrent whose content name differs.
        let replacement = minimal_torrent_named("second");
        let fh = svc.open(ino).expect("reopen").fh;
        svc.setattr(ino, Some(0)).expect("truncate");
        svc.write(ino, 0, &replacement).expect("overwrite write");
        svc.release(fh).expect("overwrite release");
        wait_for_processing_empty(&svc);

        // The next dispatch-facing call drains the queued eviction.
        let new_root = svc
            .lookup(DATA_INO, "swap.torrent")
            .expect("lookup")
            .expect("the replacement must project");

        assert!(
            !svc.inode_mgr.data_inodes.contains_key(&old_file.ino),
            "the replaced content's file inode must be evicted"
        );
        // The source row id is unchanged by an overwrite, so the root inode is
        // stable — it must be re-resolved, not keep the replaced content's
        // metadata.
        match svc.inode_mgr.data_inodes.get(&new_root.ino) {
            Some(DataInode::TorrentRoot { name, .. }) => assert_eq!(
                name, "second",
                "the root inode must be re-resolved to the replacement's content"
            ),
            other => panic!("expected a cached TorrentRoot, got {other:?}"),
        }
        // The dentry invalidation is issued by the persist thread after it
        // clears the pending key, so wait for it instead of assuming it landed
        // before `wait_for_processing_empty` returned.
        let pending_ino = InodeManager::make_pending_torrent_ino("", "swap.torrent");
        let expected = vec![
            (DATA_INO, "swap.torrent".to_string()),
            (old_root.ino, "first".to_string()),
            (old_root.ino, "second".to_string()),
            (pending_ino, "first".to_string()),
            (pending_ino, "second".to_string()),
        ];
        assert_eq!(
            wait_for_invalidations(&recorder, expected.len()),
            expected,
            "the overwrite must drop the kernel's cached root dentry and both \
             contents' root entries, under the landed and the pending root id"
        );
        assert!(
            svc.lookup(new_root.ino, "second")
                .expect("lookup")
                .is_some(),
            "the projection must serve the replacement's content name"
        );
        assert!(
            svc.lookup(new_root.ino, "first").expect("lookup").is_none(),
            "the replaced content's name must no longer resolve"
        );
    }

    /// Regression for the overwrite that leaves the replaced content's entry
    /// resolvable on a mount that never lists the seed root: the persist thread
    /// must invalidate every root-level name of the replaced and the landed
    /// content, not just the seed root's own name.
    #[test]
    fn overwrite_without_readdir_invalidates_the_replaced_root_entries() {
        let mut svc = service_with_db();

        let (ino, fh) = create_torrent_file(&mut svc, "swap.torrent");
        svc.write(ino, 0, &minimal_torrent_named("old.bin"))
            .expect("prime write");
        svc.release(fh).expect("prime release");
        wait_for_processing_empty(&svc);

        // What a client that browsed the seed root holds when it overwrites.
        let root = svc
            .lookup(DATA_INO, "swap.torrent")
            .expect("lookup")
            .expect("the first content must project");
        assert!(svc.lookup(root.ino, "old.bin").expect("lookup").is_some());

        let recorder = RecordingInvalidator::new();
        svc.notifier.set(Some(recorder.clone())).ok();

        let fh = svc.open(ino).expect("reopen").fh;
        svc.setattr(ino, Some(0)).expect("truncate");
        svc.write(ino, 0, &minimal_torrent_named("new.bin"))
            .expect("overwrite write");
        svc.release(fh).expect("overwrite release");
        wait_for_processing_empty(&svc);

        // No readdir happens in this test: the drain runs from the next
        // lookup, exactly as on a mount that only ever resolves by name.
        let pending_ino = InodeManager::make_pending_torrent_ino("", "swap.torrent");
        let expected = vec![
            (DATA_INO, "swap.torrent".to_string()),
            (root.ino, "old.bin".to_string()),
            (root.ino, "new.bin".to_string()),
            (pending_ino, "old.bin".to_string()),
            (pending_ino, "new.bin".to_string()),
        ];
        assert_eq!(
            wait_for_invalidations(&recorder, expected.len()),
            expected,
            "each replaced root entry must be dropped from the kernel cache"
        );

        let after = svc
            .lookup(DATA_INO, "swap.torrent")
            .expect("lookup")
            .expect("the replacement must project");
        assert!(
            svc.lookup(after.ino, "old.bin").expect("lookup").is_none(),
            "the replaced entry must not resolve without a readdir"
        );
        assert!(
            svc.lookup(after.ino, "new.bin").expect("lookup").is_some(),
            "the replacement's entry must resolve without a readdir"
        );
    }

    /// The temporary projection of a landed source must not survive on the
    /// daemon side until some listing happens to observe the row: the drain
    /// evicts its children and promotes the pending root onto the row, so a
    /// kernel-held pending inode still resolves the landed content.
    #[test]
    fn drain_drops_the_temporary_projection_without_a_readdir() {
        let mut svc = service_with_db();

        let bytes = minimal_torrent_named("seed.txt");
        let (ino, _fh) = create_torrent_file(&mut svc, "race.torrent");
        svc.write(ino, 0, &bytes).expect("write");
        let key = ("".to_string(), "race.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        // A client resolves the seed root while the add is in flight.
        let pending_root = svc
            .lookup(DATA_INO, "race.torrent")
            .expect("lookup")
            .expect("pending projection")
            .ino;
        assert_eq!(
            pending_root,
            InodeManager::make_pending_torrent_ino("", "race.torrent")
        );
        let pending_child = svc
            .lookup(pending_root, "seed.txt")
            .expect("pending child lookup")
            .expect("the bencode tree exposes the entry")
            .ino;
        assert!(matches!(
            svc.inode_mgr.data_inodes.get(&pending_child),
            Some(DataInode::TorrentFile { torrent_id: 0, .. })
        ));

        // The detached persist lands and queues the eviction; the persist
        // thread is the only one that notifies, so the queue is all the
        // dispatch side gets.
        drop(
            svc.torrent_service
                .as_ref()
                .unwrap()
                .persist_torrent(&bytes, "", "race.torrent")
                .expect("persist"),
        );
        svc.pending_projection_evictions.lock().unwrap().push(key);
        svc.processing_torrents.lock().unwrap().clear();

        // Any dispatch request drains; this one is not a readdir.
        svc.getattr(DATA_INO).expect("getattr");

        assert!(
            !svc.inode_mgr.data_inodes.contains_key(&pending_child),
            "the temporary child entry must be evicted once the row lands"
        );
        match svc.inode_mgr.data_inodes.get(&pending_root) {
            Some(DataInode::TorrentRoot { torrent_id, .. }) => assert_ne!(
                *torrent_id, 0,
                "the pending root must be promoted onto the landed row"
            ),
            other => panic!("expected a promoted TorrentRoot, got {other:?}"),
        }
        assert!(
            svc.lookup(pending_root, "seed.txt")
                .expect("child lookup through the promoted root")
                .is_some(),
            "a kernel-held pending root must keep resolving through the row"
        );
        assert!(
            svc.lookup(pending_root, "gone.bin")
                .expect("lookup")
                .is_none(),
            "the promoted root must not invent entries the row lacks"
        );
        let row_root = svc
            .lookup(DATA_INO, "race.torrent")
            .expect("row-backed lookup")
            .expect("the row must project");
        assert_ne!(
            row_root.ino, pending_root,
            "the row-backed root keeps its own inode id"
        );
    }

    /// A torrent row id outside the addressable band must never be encoded
    /// into another namespace (a `.stats` inode or a source-path directory):
    /// lookup fails with an I/O error and the listing refuses the entry.
    #[test]
    fn data_lookup_rejects_unaddressable_torrent_row_id() {
        let mut svc = service_with_db();
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            db.conn
                .execute_batch(
                    "INSERT INTO torrents
                         (id, info_hash, name, total_size, file_count, status)
                     VALUES (1000000, 'h-big', 'big', 16, 1, 'pending');
                     INSERT INTO torrent_sources
                         (id, torrent_id, source_path, filename)
                     VALUES (1000000, 1000000, '', 'big.torrent');",
                )
                .expect("seed an out-of-range row id");
        }

        let err = svc
            .lookup(DATA_INO, "big.torrent")
            .expect_err("an unaddressable row id must not resolve");
        assert!(
            matches!(err, FsError::Internal(_)),
            "expected EIO-class error, got {err:?}"
        );
        assert!(
            !svc.inode_mgr.data_inodes.contains_key(&INVALID_INO),
            "the sentinel must not stay cached as a data inode"
        );

        let err = svc
            .readdir(DATA_INO, 0)
            .expect_err("a listing containing an unaddressable entry must fail");
        assert!(
            matches!(err, FsError::Internal(_)),
            "expected EIO-class error, got {err:?}"
        );
    }

    /// A lookup may already be walking through a pending-window root when the
    /// persist lands and queues its eviction. The drain must keep that root
    /// resolvable: its inode is hash-derived (the resolver cannot rebuild it
    /// from the number), so dropping it turns the in-flight child lookup into
    /// `ENOENT` — `data/<seed>.torrent` visible, its content missing. The drain
    /// therefore promotes the pending root onto the landed row instead of
    /// dropping it, which retires the temporary projection while the entry the
    /// kernel holds keeps serving the row's tree.
    #[test]
    fn pending_root_keeps_resolving_children_after_the_eviction_drain() {
        let mut svc = service_with_db();
        let (ino, _fh) = create_torrent_file(&mut svc, "race.torrent");
        let bytes = minimal_torrent_named("seed.txt");
        svc.write(ino, 0, &bytes).expect("write");

        let key = ("".to_string(), "race.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let pending_root = svc
            .lookup(DATA_INO, "race.torrent")
            .expect("lookup")
            .expect("pending projection")
            .ino;
        assert_eq!(
            pending_root,
            InodeManager::make_pending_torrent_ino("", "race.torrent")
        );
        assert!(svc
            .lookup(pending_root, "seed.txt")
            .expect("pending child lookup")
            .is_some());

        // The detached persist lands.
        let persisted = svc
            .torrent_service
            .as_ref()
            .unwrap()
            .persist_torrent(&bytes, "", "race.torrent")
            .expect("persist");
        drop(persisted);
        svc.pending_projection_evictions.lock().unwrap().push(key);
        svc.processing_torrents.lock().unwrap().clear();

        let after = svc
            .lookup(pending_root, "seed.txt")
            .expect("child lookup after persist");
        assert!(
            after.is_some(),
            "the kernel's pending root inode must still resolve its child after the persist lands"
        );
        assert!(
            svc.inode_mgr.data_inodes.contains_key(&pending_root),
            "the drain must leave the kernel-held pending root cached"
        );

        // A path walk that starts at `data/` resolves the row-backed root and
        // still reaches the content.
        let db_root = svc
            .lookup(DATA_INO, "race.torrent")
            .expect("row-backed lookup")
            .expect("the persisted row must project");
        assert_ne!(db_root.ino, pending_root);
        assert!(svc
            .lookup(db_root.ino, "seed.txt")
            .expect("row-backed child lookup")
            .is_some());
    }

    /// The dispatch-side drain runs inside a request the kernel is holding the
    /// parent directory's inode lock for; `FUSE_NOTIFY_INVAL_ENTRY` takes that
    /// same lock, so notifying from here deadlocks the mount until the kernel
    /// gives up. The dentry invalidation must come from the persist thread
    /// instead (`release`), never from the drain.
    #[test]
    fn dispatch_drain_never_notifies_the_kernel() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "deferred.torrent");
        svc.write(ino, 0, &minimal_torrent_named("first"))
            .expect("write");
        svc.release(fh).expect("release");
        wait_for_processing_empty(&svc);

        let root = svc
            .lookup(DATA_INO, "deferred.torrent")
            .expect("lookup")
            .expect("projection");
        let child = svc
            .lookup(root.ino, "first")
            .expect("lookup")
            .expect("content");
        assert!(svc.inode_mgr.data_inodes.contains_key(&child.ino));

        // A row has landed for this key: the drain has real eviction work.
        let recorder = RecordingInvalidator::new();
        svc.notifier.set(Some(recorder.clone())).ok();
        svc.pending_projection_evictions
            .lock()
            .unwrap()
            .push(("".to_string(), "deferred.torrent".to_string()));

        // Run every dispatch entry point that drains the queue.
        svc.lookup(DATA_INO, "deferred.torrent").expect("lookup");
        svc.readdir(DATA_INO, 0).expect("readdir");
        svc.getattr(root.ino).expect("getattr");
        svc.open(root.ino).expect("open");

        assert!(
            !svc.inode_mgr.data_inodes.contains_key(&child.ino),
            "the drain must still evict the superseded child inode"
        );
        assert!(
            recorder.calls().is_empty(),
            "the dispatch-side drain must not issue FUSE notifications: {:?}",
            recorder.calls()
        );
    }

    /// After `release` moves the bytes into `buffered_torrents`, attributes and
    /// lookup must report that authoritative length, not the empty inode
    /// buffer. A non-empty `.torrent` reported as 0 bytes breaks `cp`/`stat`.
    #[test]
    fn parked_metadata_bytes_drive_getattr_and_lookup_sizes() {
        let mut svc = service_with_db();
        let bytes = minimal_torrent_bytes();
        let (ino, fh) = create_torrent_file(&mut svc, "sized.torrent");
        svc.write(ino, 0, &bytes).expect("write");
        svc.flush(ino).expect("flush");
        svc.release(fh).expect("release");
        wait_for_processing_empty(&svc);

        assert!(
            matches!(
                svc.inode_mgr.inodes.get(&ino),
                Some(InodeData::File { data, .. }) if data.is_empty()
            ),
            "release must leave the inode buffer empty (bytes moved to the Arc)"
        );
        assert!(
            svc.buffered_torrents.contains_key(&ino),
            "release must park the bytes as a shared handle"
        );

        assert_eq!(
            svc.getattr(ino).expect("getattr").size,
            bytes.len() as u64,
            "getattr must report the parked byte length, not zero"
        );
        let entry = svc
            .lookup(METADATA_INO, "sized.torrent")
            .expect("lookup")
            .expect("metadata child is visible");
        assert_eq!(
            entry.attr.size,
            bytes.len() as u64,
            "lookup must report the parked byte length, not zero"
        );
        assert_eq!(entry.ino, ino);
    }

    /// In the window between `release` parking the bytes and the DB row
    /// landing, `data/` must parse the parked buffer: the declared torrent
    /// name (C-3.1.3), the directory listing and by-name resolution all come
    /// from it, not from the filename stem.
    #[test]
    fn pending_persist_uses_parked_bytes_for_name_listing_and_lookup() {
        let mut svc = service_with_db();
        let bytes = minimal_torrent_named("declared-name");
        let (ino, fh) = create_torrent_file(&mut svc, "paused.torrent");
        svc.write(ino, 0, &bytes).expect("write");
        svc.flush(ino).expect("flush");

        // Freeze the exact state `release` leaves before its persist thread
        // finishes: inode buffer moved out, bytes parked, key in flight.
        let (name, _parent, data) = svc.take_buffered_file(ino).expect("buffer");
        svc.buffered_torrents.insert(ino, Arc::new(data));
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert((String::new(), name.clone()), ());
        let _ = fh;

        let root = svc
            .lookup(DATA_INO, "paused.torrent")
            .expect("pending root lookup")
            .expect("pending root is visible");
        match svc.inode_mgr.data_inodes.get(&root.ino) {
            Some(DataInode::TorrentRoot { name, .. }) => {
                assert_eq!(
                    name, "declared-name",
                    "C-3.1.3: the first-level name is the metadata declaration"
                );
            }
            other => panic!("expected a pending TorrentRoot, got {other:?}"),
        }

        // The `data/` entry keeps the metadata filename; the declared name is
        // the torrent's first-level entry (C-3.1.3) and must come from the
        // parked bytes below.
        let data_entries = svc.readdir(DATA_INO, 0).expect("data listing");
        assert!(
            data_entries
                .iter()
                .any(|entry| entry.name == "paused.torrent"),
            "the data/ directory lists the metadata filename: {data_entries:?}"
        );

        let children = svc.readdir(root.ino, 0).expect("pending torrent listing");
        assert!(
            children.iter().any(|entry| entry.name == "declared-name"),
            "the pending torrent tree must come from the parked bytes: {children:?}"
        );
        let child = svc
            .lookup(root.ino, "declared-name")
            .expect("pending by-name lookup")
            .expect("pending child resolves by its declared path");
        assert_eq!(child.attr.size, 16, "the bencode file size must survive");
    }

    /// A close that finds the previous persist still in flight must not lose
    /// the new bytes: the timeout restores buffer ownership, keeps the
    /// previously landed source, and a later close persists the new content
    /// (C-2.5.5; the kernel drops RELEASE errors, so this cannot be an error
    /// path that discards data).
    #[test]
    fn release_timeout_keeps_new_bytes_and_previous_row() {
        let mut svc = service_with_db();
        let first_bytes = minimal_torrent_named("alpha");
        let (ino, fh) = create_torrent_file(&mut svc, "retry.torrent");
        svc.write(ino, 0, &first_bytes).expect("write");
        svc.flush(ino).expect("flush");
        svc.release(fh).expect("first release");
        wait_for_processing_empty(&svc);

        let row_bytes = |svc: &FsService| {
            svc.db
                .as_ref()
                .unwrap()
                .lock()
                .unwrap()
                .get_torrent_by_source_path("")
                .expect("read row")
                .and_then(|torrent| torrent.torrent_data)
        };
        assert_eq!(
            row_bytes(&svc).as_deref(),
            Some(first_bytes.as_slice()),
            "the first persist must have landed"
        );

        let entry = svc
            .lookup(METADATA_INO, "retry.torrent")
            .expect("lookup")
            .expect("persisted file");
        let second_bytes = minimal_torrent_named("bravo");
        let fh = svc.open(entry.ino).expect("open for overwrite").fh;
        svc.setattr(entry.ino, Some(0)).expect("truncate");
        svc.write(entry.ino, 0, &second_bytes).expect("overwrite");
        svc.flush(entry.ino).expect("flush");

        // Simulate the previous add still being in flight for this key.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert((String::new(), "retry.torrent".to_string()), ());
        let err = svc
            .release_with_pending_timeout(fh, Duration::from_millis(100))
            .expect_err("the injected pending add must time out");
        assert!(matches!(err, FsError::Internal(_)));

        assert_eq!(
            svc.buffered_torrent_bytes(entry.ino),
            Some(second_bytes.as_slice()),
            "a timeout must restore the new bytes, not drop them"
        );
        assert_eq!(
            row_bytes(&svc).as_deref(),
            Some(first_bytes.as_slice()),
            "the previous landed source must survive the timeout"
        );

        // Let the pending add settle and retry the close: the new content is
        // persisted, proving nothing was irrecoverably lost.
        svc.processing_torrents
            .lock()
            .unwrap()
            .remove(&(String::new(), "retry.torrent".to_string()));
        let fh = svc.open(entry.ino).expect("reopen after timeout").fh;
        svc.release(fh).expect("retry release");
        wait_for_processing_empty(&svc);
        assert_eq!(
            row_bytes(&svc).as_deref(),
            Some(second_bytes.as_slice()),
            "the retried close must persist the recovered bytes"
        );
    }

    /// Repeated overwrite-renames destroy each destination inode; their parked
    /// shared buffers must be reclaimed with the inode, or every overwrite
    /// leaks one (up to 10 MiB) allocation.
    #[test]
    fn repeated_overwrite_rename_reclaims_destination_buffers() {
        let mut svc = service_with_db();
        let source_bytes = minimal_torrent_named("source");
        let (source_ino, source_fh) = create_torrent_file(&mut svc, "source.torrent");
        svc.write(source_ino, 0, &source_bytes)
            .expect("source write");
        svc.flush(source_ino).expect("source flush");
        svc.release(source_fh).expect("source release");
        wait_for_processing_empty(&svc);

        let mut target_inos = Vec::new();
        let mut source_name = "source.torrent".to_string();
        for index in 0..5 {
            let name = format!("target-{index}.torrent");
            let (target_ino, target_fh) = create_torrent_file(&mut svc, &name);
            svc.write(target_ino, 0, &minimal_torrent_named(&format!("t{index}")))
                .expect("target write");
            svc.flush(target_ino).expect("target flush");
            svc.release(target_fh).expect("target release");
            wait_for_processing_empty(&svc);
            assert!(
                svc.buffered_torrents.contains_key(&target_ino),
                "the destination buffer is parked before the rename"
            );
            target_inos.push(target_ino);

            svc.rename(METADATA_INO, &source_name, METADATA_INO, &name)
                .expect("overwrite rename");
            source_name = name.clone();
            assert!(
                !svc.inode_mgr.inodes.contains_key(&target_ino),
                "no open handle: the destination inode is destroyed immediately"
            );
            assert!(
                !svc.buffered_torrents.contains_key(&target_ino),
                "an immediately destroyed destination must not leak its buffer"
            );
        }

        let live: HashSet<u64> = svc.inode_mgr.inodes.keys().copied().collect();
        for ino in svc.buffered_torrents.keys() {
            assert!(
                live.contains(ino),
                "a parked buffer must belong to a live inode: {ino} is gone"
            );
        }
        assert_eq!(
            svc.buffered_torrents.len(),
            1,
            "only the surviving source buffer remains"
        );
        assert!(svc.buffered_torrents.contains_key(&source_ino));
    }
}

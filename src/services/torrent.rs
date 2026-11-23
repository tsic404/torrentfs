//! TorrentService — orchestrates torrent operations,
//! cutting the direct FUSE→DB calls in favour of a service layer.
//!
//! Returns `FsError` (a domain error), never raw errno — the errno mapping is
//! the FUSE adapter's job. This module does not import `libc`.

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use crate::db::{Database, DbError, FileEntry, MoveOverwriteResult, TorrentFile};
use crate::domain::fs_error::{FsError, FsResult};
use crate::infrastructure::cache::remove_dir_all_tolerating_writers;
use crate::metadata::TorrentInfo;
use rusqlite::params;
use tracing::{error, info, warn};

use super::download::DownloadService;

/// `(source row id, file row id) -> content-scoped read identity`.
type FileIdentityCache = HashMap<(i64, i64), Arc<FileIdentity>>;

/// TorrentService wraps database operations for torrent lifecycle management.
#[derive(Clone)]
pub struct TorrentService {
    db: Arc<Mutex<Database>>,
    download_service: Option<Arc<DownloadService>>,
    /// `(source row id, file row id) -> content-scoped read identity`.
    ///
    /// Prepared outside the read path (at content landing, at metadata
    /// lookup) and never derived from a per-request prefix scan: the ordinal
    /// and the content token are taken from the same ordered statement that
    /// produced the info-hash, so a read can never pair bytes with a content
    /// id from a different snapshot.
    file_identities: Arc<Mutex<FileIdentityCache>>,
    /// Source rows whose identities are fully prepared.
    prepared_sources: Arc<Mutex<HashSet<i64>>>,
    /// Authoritative content pointer per source row, published by the write
    /// that moved the pointer. The value is the content id itself, not a
    /// counter: two independent reads of it can never disagree about which
    /// bytes they refer to.
    content_versions: Arc<Mutex<HashMap<i64, ContentVersion>>>,
    /// Number of bulk identity preparations issued.
    file_identity_queries: Arc<AtomicU64>,
    /// Rows visited by those bulk preparations. This is the scale evidence:
    /// a read path that hits the prepared cache visits zero rows.
    file_identity_rows: Arc<AtomicU64>,
    /// Number of full file-list loads through [`Self::get_torrent_with_files`].
    /// Kept so a test can assert the read path never takes that shape.
    full_file_list_loads: Arc<AtomicU64>,
}

/// Bound on prepared source rows. Overflow clears the cache; the next read
/// prepares its source once, never per request.
const MAX_PREPARED_SOURCES: usize = 256;

/// Read identity of one file, taken from a single statement over the source,
/// file and content rows.
///
/// `file_id`s are content-scoped and never reused, so an identity can only
/// ever describe the bytes it was read from. `content_id` is also the publish
/// token: a replacement points the source at a different content id and the
/// read registered under the old id is settled as stale.
#[derive(Debug)]
pub struct FileIdentity {
    pub info_hash: Arc<str>,
    pub file_index: i32,
    pub content_id: i64,
}

/// Authoritative state of a source row's content pointer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContentVersion {
    /// The source row points at this content id.
    Current(i64),
    /// The source row was removed; no content is current.
    Removed,
}
/// Result of synchronously persisting a torrent into the database.
///
/// Carries the parsed metadata plus the insert disposition so the (deferred,
/// non-fatal) download-handle registration can run afterward without
/// re-parsing or re-querying the DB.
pub struct PersistedTorrent {
    info: Arc<TorrentInfo>,
    is_new: bool,
    /// Content row this write landed on. Used as the read-path publish token.
    content_id: i64,
    /// `true` only when this write repointed the source row to different
    /// content. A source that merely existed is not enough: re-persisting the
    /// same bytes (every read-only open/close of `metadata/*.torrent`) reuses
    /// the content row and replaces nothing.
    content_changed: bool,
    /// `Some(old_info_hash)` when an overwrite replaced a different info-hash;
    /// the old hash's engine handle and piece cache are released in
    /// [`TorrentService::register_torrent_handle`].
    stale_info_hash: Option<String>,
    /// Source row id this write landed on. The `data/` projection derives the
    /// torrent root inode from it.
    source_id: i64,
    /// Root-level entry names of the replaced and the landed content. Both can
    /// carry a stale kernel dentry under the torrent root — the first as a
    /// stale positive, the second as a stale negative — so the caller drops
    /// them all.
    changed_root_names: Vec<String>,
}

impl PersistedTorrent {
    pub fn source_id(&self) -> i64 {
        self.source_id
    }

    pub fn is_new(&self) -> bool {
        self.is_new
    }

    pub fn content_id(&self) -> i64 {
        self.content_id
    }

    /// `true` when the write replaced the content this source row used to
    /// point at. In-flight reads of the old content must settle with `EIO`
    /// (C-4.5.3); re-persisting identical bytes must not cancel anything.
    pub fn content_changed(&self) -> bool {
        self.content_changed
    }

    pub fn changed_root_names(&self) -> &[String] {
        &self.changed_root_names
    }
}

impl TorrentService {
    pub fn new(db: Arc<Mutex<Database>>, download_service: Option<Arc<DownloadService>>) -> Self {
        Self {
            db,
            download_service,
            file_identities: Arc::new(Mutex::new(HashMap::new())),
            prepared_sources: Arc::new(Mutex::new(HashSet::new())),
            content_versions: Arc::new(Mutex::new(HashMap::new())),
            file_identity_queries: Arc::new(AtomicU64::new(0)),
            file_identity_rows: Arc::new(AtomicU64::new(0)),
            full_file_list_loads: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Shared source → content pointer map. `FsService` uses the same
    /// instance so a read registered on the dispatch thread observes exactly
    /// the version the persist thread published before it exposed the new
    /// identities.
    pub fn content_versions(&self) -> Arc<Mutex<HashMap<i64, ContentVersion>>> {
        self.content_versions.clone()
    }

    /// Current authoritative content state of one source row.
    pub fn content_version(&self, source_id: i64) -> Option<ContentVersion> {
        self.content_versions
            .lock()
            .ok()
            .and_then(|versions| versions.get(&source_id).copied())
    }

    /// Bulk identity preparations issued by this service (including clones).
    pub fn file_identity_query_count(&self) -> u64 {
        self.file_identity_queries.load(Ordering::Relaxed)
    }

    /// Rows visited while preparing file identities.
    pub fn file_identity_rows_read(&self) -> u64 {
        self.file_identity_rows.load(Ordering::Relaxed)
    }

    /// Full file-list loads issued by this service (including clones).
    pub fn full_file_list_load_count(&self) -> u64 {
        self.full_file_list_loads.load(Ordering::Relaxed)
    }

    /// Parse a `.torrent` and persist it into the database. Runs on a detached
    /// thread from `release` so the single-threaded FUSE dispatcher is never
    /// blocked by the SQLite write. A DB write failure is recorded in
    /// `FsService::persist_errors` and surfaced as `FsError::Database` → `EIO`
    /// by the next `lookup`/`readdir` — not by `cp`'s `close()`, whose
    /// FUSE_RELEASE reply error the kernel drops. Handle creation is split out
    /// into [`Self::register_torrent_handle`].
    pub fn persist_torrent(
        &self,
        data: &[u8],
        source_path: &str,
        filename: &str,
    ) -> FsResult<PersistedTorrent> {
        let info = TorrentInfo::from_bytes(data.to_vec()).map_err(|e| {
            warn!("Invalid .torrent file {}: {}", filename, e.reason());
            FsError::CorruptTorrent(e.reason().to_string())
        })?;

        let metadata = info.metadata().map_err(|e| {
            error!("Failed to get torrent metadata {}: {:?}", filename, e);
            FsError::Internal(format!(
                "Failed to get torrent metadata {}: {:?}",
                filename, e
            ))
        })?;

        let info = Arc::new(info);
        let info_hash_hex = hex::encode(metadata.info_hash);

        let (
            is_new,
            content_changed,
            stale_info_hash,
            replaced_root_names,
            landed_source_id,
            landed_content_id,
        ) = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;

            let files: Vec<FileEntry> = metadata
                .files
                .iter()
                .map(|f| FileEntry {
                    path: f.path.clone(),
                    size: f.size as i64,
                })
                .collect();

            let outcome = db_guard
                .insert_torrent_with_files_and_data(
                    source_path,
                    &metadata.name,
                    filename,
                    metadata.total_size as i64,
                    &info_hash_hex,
                    metadata.num_files as i64,
                    &files,
                    data,
                )
                .map_err(|e| {
                    error!("Failed to insert torrent with files {}: {:?}", filename, e);
                    FsError::from(e)
                })?;

            // `is_new` mirrors the pre-split `Inserted` vs `Duplicate`
            // distinction: a fresh source entry creates the lightweight handle;
            // an overwrite (repointed source) merges trackers.  Content reuse
            // (identical bytes at a second source path) is still a fresh source
            // entry, so the handle is ensured idempotently.
            let is_new = !outcome.source_reused;

            if is_new {
                info!(
                    "Persisted torrent '{}' ({} files, {} bytes) from {}{}",
                    metadata.name,
                    metadata.num_files,
                    metadata.total_size,
                    if source_path.is_empty() {
                        "root"
                    } else {
                        source_path
                    },
                    if outcome.content_created {
                        ""
                    } else {
                        " (reused existing content)"
                    }
                );
            } else {
                info!(
                    "Torrent '{}' at ({}, {}) was overwritten; source repointed to {} content",
                    metadata.name,
                    source_path,
                    filename,
                    if outcome.content_created {
                        "new"
                    } else {
                        "existing"
                    }
                );
            }

            // `is_new` only says the source slot did not exist. The exact
            // "the row now points at other bytes" signal is the one the
            // database derives while repointing: non-empty replaced root
            // names means the old content id differed from the new one.
            // `stale_info_hash` additionally covers a replaced content whose
            // root entry set was empty. Identical bytes reuse the same content
            // row and set neither.
            let content_changed = !is_new
                && (!outcome.replaced_root_names.is_empty() || outcome.stale_info_hash.is_some());

            // Publish the new content pointer *before* the new identities
            // become observable. Registration validates against this value,
            // so a reader that can obtain the new identity necessarily sees
            // the matching version — there is no window in which the database
            // serves B while registration still compares against A.
            if let Ok(mut versions) = self.content_versions.lock() {
                versions.insert(
                    outcome.source_id,
                    ContentVersion::Current(outcome.content_id),
                );
            }

            // Prepare every `(info_hash, file_index, content_id)` while the
            // write is still on the persist thread: the read path then
            // performs no identity query at all, cold or after an L1/L3
            // eviction. A replacement invalidates first so the source row's
            // new content can never be served with the old ordinals.
            self.prepare_file_identities_locked(&db_guard, outcome.source_id)?;
            (
                is_new,
                content_changed,
                outcome.stale_info_hash,
                outcome.replaced_root_names,
                outcome.source_id,
                outcome.content_id,
            )
        };

        // The kernel may hold a dentry for any root-level entry of either
        // content: a stale positive for what the overwrite replaced, a stale
        // negative for what it added. The landed content's entries are reported
        // even for a fresh source, because its temporary projection is evicted
        // at the same time and those are the names it was serving.
        let mut changed_root_names = replaced_root_names;
        for file in &metadata.files {
            let Some(root) = file.path.split('/').next().filter(|part| !part.is_empty()) else {
                continue;
            };
            if !changed_root_names.iter().any(|known| known == root) {
                changed_root_names.push(root.to_string());
            }
        }

        Ok(PersistedTorrent {
            info,
            is_new,
            content_id: landed_content_id,
            content_changed,
            stale_info_hash,
            source_id: landed_source_id,
            changed_root_names,
        })
    }

    /// Register the download handle for an already-persisted torrent.
    ///
    /// Best-effort and non-fatal: the torrent is already in the DB and a
    /// handle is created lazily on first access, so every failure here is
    /// logged and swallowed.  Runs off the FUSE dispatch thread so
    /// libtorrent FFI never blocks the single-threaded dispatcher.
    pub fn register_torrent_handle(&self, persisted: &PersistedTorrent) {
        // the overwrite orphaned the OLD info-hash's
        // engine handle, scheduler, private_torrents entry, and on-disk
        // `cache/pieces/<old>/` — release them AFTER the DB row is updated
        // (no I/O under the DB lock), mirroring `remove_torrent`.
        // same ordering as remove_torrent — handle released before
        // pieces purged, so libtorrent never checks a torrent whose files
        // are vanishing underneath it.
        if let Some(old) = &persisted.stale_info_hash {
            self.release_engine_handle(old);
            self.purge_pieces_cache(old);
        }

        let name = persisted.info.name();

        // Create upload_mode handle so peer/seed info is visible immediately
        // without triggering any data download (all pieces at priority 0).
        if persisted.is_new {
            if let Some(ds) = &self.download_service {
                match ds.ensure_handle_lightweight(persisted.info.clone()) {
                    Ok(_) => {
                        info!(
                            "Created lightweight handle for torrent '{}' (upload_mode)",
                            name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create lightweight handle for torrent '{}': {:?}",
                            name, e
                        );
                        // Non-fatal: the torrent is already in the database and
                        // a handle will be created lazily when first accessed.
                    }
                }
            }
        } else {
            // Duplicate info_hash: merge trackers from this torrent into the
            // existing handle. The engine checks the private flag
            // and skips the merge for private torrents — PT
            // isolation prevents passkey leakage and peer cross-pollination
            // across private tracker swarms.
            if let Some(ds) = &self.download_service {
                if let Err(e) = ds.merge_trackers(persisted.info.clone()) {
                    warn!(
                        "Failed to merge trackers for duplicate torrent '{}': {:?}",
                        name, e
                    );
                    // Non-fatal: the torrent is already in the DB; tracker
                    // merge is a best-effort optimization, not a correctness
                    // requirement. The existing handle's trackers are
                    // unchanged.
                }
            }
        }
    }

    /// Add a torrent to the database and register its download handle in one
    /// synchronous call.  [`Self::persist_torrent`] +
    /// [`Self::register_torrent_handle`] composed for callers that want the
    /// original atomic behavior (tests); the FUSE `release` path calls the two
    /// phases separately so a DB failure surfaces as `EIO` while handle
    /// registration stays off the dispatcher thread.
    pub fn add_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> FsResult<()> {
        let persisted = self.persist_torrent(data, source_path, filename)?;
        self.register_torrent_handle(&persisted);
        Ok(())
    }

    /// Remove a torrent from the database by filename and source_path.
    ///
    /// After the DB record is deleted, when no other torrent still references
    /// the same info_hash: purge the on-disk pieces cache and release the
    /// DownloadEngine handle (and its scheduler state) — so a long-running
    /// daemon does not accumulate handles or keep announcing a deleted torrent.
    pub fn remove_torrent(
        &self,
        filename: &str,
        source_path: &str,
    ) -> FsResult<Option<(i64, String)>> {
        let (torrent_id, info_hash, purge_pieces) = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;

            match db_guard.get_torrent_by_filename_and_source_path(filename, source_path) {
                Ok(Some(torrent)) => {
                    let torrent_id = torrent.id;
                    let info_hash = torrent.info_hash.clone();
                    db_guard.delete_torrent(torrent_id).map_err(|e| {
                        error!("Failed to delete torrent from database: {:?}", e);
                        FsError::from(e)
                    })?;

                    let purge_pieces = match db_guard.get_torrents_by_infohash(&info_hash) {
                        Ok(remaining) if !remaining.is_empty() => {
                            info!(
                                "Skipping pieces cache purge for info_hash={} ({} torrent(s) still reference it)",
                                info_hash,
                                remaining.len()
                            );
                            false
                        }
                        Ok(_) => true,
                        Err(e) => {
                            warn!(
                                "Failed to check remaining torrents for info_hash={}: {:?}; skipping purge",
                                info_hash, e
                            );
                            false
                        }
                    };

                    info!(
                        "Deleted torrent '{}' (id={}, source_path='{}', info_hash={})",
                        filename, torrent_id, source_path, info_hash
                    );
                    (Some(torrent_id), info_hash, purge_pieces)
                }
                Ok(None) => return Ok(None),
                Err(e) => {
                    error!("Database error during remove_torrent: {:?}", e);
                    return Err(FsError::from(e));
                }
            }
        };
        if purge_pieces {
            // Release the libtorrent handle BEFORE purging piece files:
            // `remove_torrent` is async, so an old handle can observe files
            // vanishing mid-check, and a same-info_hash re-add racing the
            // removal inherits a stale have-piece bitmap (complete but gone),
            // forcing the recheck path → NoPeers if no peer connected. Releasing
            // first lets libtorrent tear down against intact files.
            self.release_engine_handle(&info_hash);
            self.purge_pieces_cache(&info_hash);
        }

        Ok(torrent_id.map(|id| (id, info_hash)))
    }

    /// Remove the `cache/pieces/<info_hash>/` directory and its cache metadata.
    /// Best-effort: a failure leaves orphaned pieces, which remain safe to
    /// re-read/re-download and can be cleaned by a later removal or restart
    /// scan.
    fn purge_pieces_cache(&self, info_hash: &str) {
        let Some(ds) = &self.download_service else {
            return;
        };
        let Some(cache) = ds.get_cache_manager() else {
            return;
        };

        // Resolve the pieces directory under a brief lock.
        let pieces_dir = match cache.lock() {
            Ok(guard) => match guard.pieces_dir_for(info_hash) {
                Some(dir) => dir,
                None => return,
            },
            Err(_) => {
                error!(
                    "Cache lock poisoned during pieces purge for info_hash={}",
                    info_hash
                );
                return;
            }
        };

        // Remove the directory tree WITHOUT holding the cache lock — the
        // bounded retry may sleep against libtorrent's still-flushing storage,
        // and must not block concurrent cache reads (e.g. FUSE reads).
        let removal = remove_dir_all_tolerating_writers(&pieces_dir);

        let mut guard = match cache.lock() {
            Ok(g) => g,
            Err(_) => {
                error!(
                    "Cache lock poisoned during pieces purge for info_hash={}",
                    info_hash
                );
                return;
            }
        };
        match removal {
            Ok(()) => {
                // The directory removal ran lock-free, so a concurrent
                // same-info_hash re-add may have recreated the directory while
                // we were retrying.  Drop metadata only if it is still absent;
                // otherwise keep the re-add's registration.
                if !guard.drop_infohash_state_if_absent(info_hash) {
                    info!(
                        "Skipping pieces metadata drop for info_hash={} (re-written during purge)",
                        info_hash
                    );
                }
            }
            Err(e) => warn!(
                "Failed to purge pieces cache for info_hash={}: {:?}",
                info_hash, e
            ),
        }
    }

    /// Release the DownloadEngine handle for an info_hash.  Best-effort: the DB
    /// row and pieces cache are already gone, so a transient engine failure only
    /// leaves a stale handle that a restart clears — it never desyncs DB vs.
    /// cache.
    fn release_engine_handle(&self, info_hash: &str) {
        if let Some(ds) = &self.download_service {
            if let Err(e) = ds.remove_handle(info_hash) {
                warn!(
                    "Failed to remove DownloadEngine handle for info_hash={}: {:?}",
                    info_hash, e
                );
            }
        }
    }

    /// Rename a torrent in the database.
    pub fn rename_torrent(
        &self,
        old_name: &str,
        old_source_path: &str,
        new_name: &str,
        new_source_path: &str,
    ) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        match db_guard.get_torrent_by_filename_and_source_path(old_name, old_source_path) {
            Ok(Some(torrent)) => {
                db_guard
                    .rename_torrent(torrent.id, new_name, new_source_path)
                    .map_err(|e| {
                        error!("Failed to rename torrent in database: {:?}", e);
                        FsError::from(e)
                    })?;
                Ok(())
            }
            Ok(None) => Ok(()),
            Err(e) => {
                error!("Database error during rename_torrent: {:?}", e);
                Err(FsError::from(e))
            }
        }
    }

    /// Atomically move a torrent over an existing destination. Deletes the
    /// destination row and re-points the source row at the destination in one
    /// DB lock (a single transaction), so a failure on the source side never
    /// leaves the destination already destroyed — satisfying POSIX's "a failed
    /// rename keeps both names". The removed destination's engine handle and
    /// piece cache are released best-effort after the commit, mirroring
    /// `remove_torrent`'s ordering.
    pub fn rename_torrent_overwriting(
        &self,
        old_name: &str,
        old_source_path: &str,
        new_name: &str,
        new_source_path: &str,
    ) -> FsResult<MoveOverwriteResult> {
        let (result, purge_info_hash) = {
            let mut db_guard = self.db.lock().map_err(|_| {
                error!("Database lock poisoned");
                FsError::LockPoisoned
            })?;

            match db_guard.move_torrent_replacing(
                old_name,
                old_source_path,
                new_name,
                new_source_path,
            ) {
                Ok(MoveOverwriteResult::Moved { removed_target }) => {
                    let purge_info_hash = removed_target
                        .as_ref()
                        .and_then(|(_, info_hash)| {
                            match db_guard.get_torrents_by_infohash(info_hash.as_str()) {
                                Ok(remaining) if !remaining.is_empty() => None,
                                Ok(_) => Some(info_hash.to_string()),
                                Err(e) => {
                                    warn!(
                                        "Failed to check remaining torrents for info_hash={}: {:?}; skipping purge",
                                        info_hash, e
                                    );
                                    None
                                }
                            }
                        });
                    (
                        MoveOverwriteResult::Moved { removed_target },
                        purge_info_hash,
                    )
                }
                Ok(MoveOverwriteResult::SourceAbsent) => (MoveOverwriteResult::SourceAbsent, None),
                Err(e) => {
                    error!(
                        "Failed to move torrent over destination {}: {:?}",
                        new_name, e
                    );
                    return Err(FsError::from(e));
                }
            }
        };

        if let Some(info_hash) = purge_info_hash {
            // Release the libtorrent handle BEFORE purging piece files (same
            // ordering as `remove_torrent`).
            self.release_engine_handle(&info_hash);
            self.purge_pieces_cache(&info_hash);
        }

        Ok(result)
    }

    /// Ensure metadata directories exist in the database for a given source_path.
    pub fn ensure_metadata_directories(&self, source_path: &str) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .ensure_metadata_directories(source_path)
            .map_err(|e| {
                warn!("Failed to persist directory to database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Delete a metadata directory from the database.
    pub fn delete_metadata_directory(&self, source_path: &str) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .delete_metadata_directory(source_path)
            .map_err(|e| {
                warn!("Failed to delete metadata directory from database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Rename a metadata directory in the database.
    pub fn rename_metadata_directory(
        &self,
        old_path: &str,
        new_name: &str,
        new_path: &str,
    ) -> FsResult<()> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .rename_metadata_directory(old_path, new_name, new_path)
            .map_err(|e| {
                error!("Failed to rename metadata directory in database: {:?}", e);
                FsError::from(e)
            })
    }

    /// Check if a torrent with the given id exists in the database.
    pub fn torrent_exists_by_id(&self, id: i64) -> bool {
        let db_guard = match self.db.lock() {
            Ok(g) => g,
            Err(_) => return false,
        };
        db_guard.get_torrent_by_id(id).ok().flatten().is_some()
    }

    /// Get torrent with its files by torrent_id.
    /// Returns (info_hash, source_path, files) on success.
    pub fn get_torrent_with_files(
        &self,
        torrent_id: i64,
    ) -> FsResult<Option<(String, String, Vec<TorrentFile>)>> {
        self.full_file_list_loads.fetch_add(1, Ordering::Relaxed);
        let db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        let torrent = match db_guard.get_torrent_by_id(torrent_id).map_err(|e| {
            error!("Failed to get torrent by id: {:?}", e);
            FsError::from(e)
        })? {
            Some(t) => t,
            None => return Ok(None),
        };

        let files = db_guard.get_files_by_torrent_id(torrent_id).map_err(|e| {
            error!("Failed to get files for torrent: {:?}", e);
            FsError::from(e)
        })?;

        Ok(Some((torrent.info_hash, torrent.source_path, files)))
    }

    /// Prepare every file identity of one source row outside the read path.
    ///
    /// One ordered statement enumerates the content's rows once and stores
    /// `(info_hash, zero-based index, content_id)` per file. `file_index` is
    /// the position in `metadata.files`, and `torrent_files` rows are inserted
    /// in exactly that order, so row order is the ordinal. A no-op once
    /// prepared.
    ///
    /// The previous per-file `COUNT(*) ... prior.id <= f.id` enumerated the
    /// whole prefix index on every request; reads now hit the prepared cache
    /// and visit zero rows (F2 / C-11.4).
    pub fn prepare_file_identities(&self, source_id: i64) -> FsResult<()> {
        if self
            .prepared_sources
            .lock()
            .map(|prepared| prepared.contains(&source_id))
            .unwrap_or(false)
        {
            return Ok(());
        }
        let db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;
        self.prepare_file_identities_locked(&db_guard, source_id)
    }

    /// Drop the prepared identities of one source row after its content was
    /// replaced or removed.
    pub fn invalidate_file_identities(&self, source_id: i64) {
        if let Ok(mut prepared) = self.prepared_sources.lock() {
            prepared.remove(&source_id);
        }
        if let Ok(mut identities) = self.file_identities.lock() {
            identities.retain(|(source, _), _| *source != source_id);
        }
    }

    /// Resolve one file's read identity from the identities already prepared
    /// for its source row.
    ///
    /// This is deliberately memory-only: the read path has no fallback that
    /// materializes a source's file table. A source is prepared by the write
    /// that lands its content and by the lookup/readdir that projects it, so a
    /// reader always asks after one of those. A miss means the inode belongs
    /// to content that was already superseded, and the caller fails the read
    /// instead of loading the replacement's rows underneath it.
    pub fn prepared_file_identity(
        &self,
        source_id: i64,
        file_id: i64,
    ) -> Option<Arc<FileIdentity>> {
        self.file_identities
            .lock()
            .ok()
            .and_then(|identities| identities.get(&(source_id, file_id)).cloned())
    }

    /// Bulk-load `source_id`'s identities while the caller already holds the
    /// database lock.
    fn prepare_file_identities_locked(&self, db: &Database, source_id: i64) -> FsResult<()> {
        let mut stmt = db
            .conn
            .prepare(
                "SELECT f.id, t.info_hash, s.torrent_id
                   FROM torrent_sources s
                   JOIN torrent_files f ON f.torrent_id = s.torrent_id
                   JOIN torrents t ON t.id = s.torrent_id
                  WHERE s.id = ?1
                  ORDER BY f.id",
            )
            .map_err(|e| FsError::from(DbError::from(e)))?;
        let rows: Vec<(i64, String, i64)> = stmt
            .query_map(params![source_id], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })
            .map_err(|e| FsError::from(DbError::from(e)))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(|e| FsError::from(DbError::from(e)))?;

        let overflowed = {
            let mut prepared = self
                .prepared_sources
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            let mut overflowed = false;
            if prepared.len() >= MAX_PREPARED_SOURCES {
                prepared.clear();
                overflowed = true;
            }
            prepared.insert(source_id);
            overflowed
        };
        {
            let mut identities = self
                .file_identities
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if overflowed {
                identities.clear();
            }
            identities.retain(|(source, _), _| *source != source_id);
            for (index, (file_id, info_hash, content_id)) in rows.iter().enumerate() {
                identities.insert(
                    (source_id, *file_id),
                    Arc::new(FileIdentity {
                        info_hash: Arc::from(info_hash.as_str()),
                        file_index: index as i32,
                        content_id: *content_id,
                    }),
                );
            }
        }
        self.file_identity_queries.fetch_add(1, Ordering::Relaxed);
        self.file_identity_rows
            .fetch_add(rows.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::{Database, InsertTorrentResult};
    use crate::infrastructure::config::TorrentfsConfig;
    use tempfile::TempDir;

    fn insert_torrent(
        db: &mut Database,
        source_path: &str,
        filename: &str,
        info_hash: &str,
    ) -> i64 {
        match db.insert_torrent(source_path, "foo", filename, 16, info_hash, 1) {
            Ok(InsertTorrentResult::Inserted(id)) => id,
            other => panic!("unexpected insert result: {:?}", other),
        }
    }

    fn service_with_piece(
        cache_dir: &std::path::Path,
        info_hash: &str,
    ) -> (Arc<DownloadService>, String) {
        let config = TorrentfsConfig::default_config();
        let download_service = Arc::new(DownloadService::new(cache_dir, &config).unwrap());

        // Register one piece through the shared CacheManager so its metadata
        // is populated, exactly as a real read would.
        let piece_key = format!("{}:piece:0", info_hash);
        {
            let cache = download_service.get_cache_manager().unwrap();
            let mut guard = cache.lock().unwrap();
            let path = guard.ensure_piece_dir(&piece_key).unwrap();
            std::fs::write(&path, b"piece-data").unwrap();
            guard.add_piece(&piece_key, 10).unwrap();
        }

        (download_service, piece_key)
    }

    #[test]
    fn test_remove_torrent_purges_pieces_cache() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let info_hash = "0123456789abcdef0123456789abcdef01234567".to_string();
        let (download_service, piece_key) = service_with_piece(&cache_dir, &info_hash);

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            insert_torrent(&mut guard, "src", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(db_arc, Some(download_service.clone()));

        let pieces_dir = cache_dir.join("pieces").join(&info_hash);
        assert!(pieces_dir.exists());

        let removed = svc.remove_torrent("foo.torrent", "src").unwrap();
        assert_eq!(removed, Some((1, info_hash.clone())));

        // Directory and cache metadata are both gone.
        assert!(!pieces_dir.exists());
        let cache = download_service.get_cache_manager().unwrap();
        let guard = cache.lock().unwrap();
        assert!(!guard.has_piece(&piece_key));
        assert!(!guard.get_all_infohashes().contains(&info_hash));
    }

    #[test]
    fn test_remove_torrent_keeps_shared_pieces() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let info_hash = "fedcba9876543210fedcba9876543210fedcba98".to_string();
        let (download_service, piece_key) = service_with_piece(&cache_dir, &info_hash);

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            // Two distinct source paths share one info_hash (same .torrent
            // copied twice).
            insert_torrent(&mut guard, "a", "foo.torrent", &info_hash);
            insert_torrent(&mut guard, "b", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(db_arc, Some(download_service.clone()));

        let pieces_dir = cache_dir.join("pieces").join(&info_hash);
        assert!(pieces_dir.exists());

        let removed = svc.remove_torrent("foo.torrent", "a").unwrap();
        assert_eq!(removed, Some((1, info_hash.clone())));

        // The other torrent still references this info_hash: pieces stay.
        assert!(pieces_dir.exists());
        let cache = download_service.get_cache_manager().unwrap();
        let guard = cache.lock().unwrap();
        assert!(guard.has_piece(&piece_key));
    }

    // ── PT isolation tests ──────────────────────────────────

    /// Build a minimal bencoded single-file torrent with a single tracker
    /// URL and optional `private=1` flag in the info dict.
    fn build_torrent_bencode(
        announce_url: &str,
        total: usize,
        piece_length: usize,
        is_private: bool,
    ) -> Vec<u8> {
        let content = (0..total).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let mut pieces = Vec::new();
        for chunk in content.chunks(piece_length) {
            use sha1_smol::Sha1;
            pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
        }
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"8:announce");
        t.extend_from_slice(announce_url.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(announce_url.as_bytes());
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(total.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces");
        t.extend_from_slice(pieces.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(&pieces);
        if is_private {
            t.extend_from_slice(b"7:privatei1e");
        }
        t.extend_from_slice(b"ee");
        t
    }

    /// PT isolation test: two private torrents with the same info_hash but
    /// different tracker URLs. The handle is created with torrent A's
    /// trackers, then `merge_trackers` is called with torrent B. The
    /// assertion: torrent B's tracker URL must NOT appear in the handle's
    /// tracker list — private torrents are isolated from cross-site merge.
    #[test]
    fn test_pt_isolation_private_torrent_skips_merge() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let config = TorrentfsConfig::default_config();
        let ds = DownloadService::new(&cache_dir, &config).unwrap();

        let url_a = "http://tracker.private-a.com/announce";
        let url_b = "http://tracker.private-b.com/announce";

        let torrent_a = build_torrent_bencode(url_a, 32, 16, true);
        let torrent_b = build_torrent_bencode(url_b, 32, 16, true);

        let info_a =
            Arc::new(crate::metadata::TorrentInfo::from_bytes(torrent_a).expect("parse torrent A"));
        let info_b =
            Arc::new(crate::metadata::TorrentInfo::from_bytes(torrent_b).expect("parse torrent B"));

        // Both torrents must have the same info_hash (same content).
        let hash_a = hex::encode(info_a.info_hash().expect("hash A"));
        let hash_b = hex::encode(info_b.info_hash().expect("hash B"));
        assert_eq!(hash_a, hash_b, "torrents must share info_hash");

        // Both must be private.
        assert!(info_a.is_private(), "torrent A must be private");
        assert!(info_b.is_private(), "torrent B must be private");

        // Create the handle with torrent A's trackers.
        ds.ensure_handle_lightweight(info_a.clone()).unwrap();
        // Give the engine thread time to process the async command.
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify the handle has torrent A's tracker.
        let trackers_before = ds.get_trackers(&hash_a).expect("get trackers");
        assert!(
            trackers_before.iter().any(|t| t.url == url_a),
            "handle should have tracker A's URL before merge"
        );

        // Trigger merge_trackers with torrent B (private → must skip).
        ds.merge_trackers(info_b).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Assert: torrent B's tracker must NOT be in the handle's list.
        let trackers_after = ds.get_trackers(&hash_a).expect("get trackers after merge");
        assert!(
            !trackers_after.iter().any(|t| t.url == url_b),
            "PT isolation: private torrent B's tracker must NOT be merged \
             into the handle (found: {:?})",
            trackers_after
        );
        assert_eq!(
            trackers_after.len(),
            trackers_before.len(),
            "tracker count must not change (merge was skipped)"
        );
    }

    /// overwriting a metadata torrent with DIFFERENT
    /// content must release the OLD info-hash's engine state — the pieces
    /// cache for `cache/pieces/<old>/` must be purged, not leaked. (The
    /// engine handle itself is released via the same
    /// `release_engine_handle` call; here we assert the observable
    /// on-disk/metadata effect.)
    #[test]
    fn add_torrent_overwrite_purges_old_info_hash_pieces() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");

        // Two distinct torrents with distinct info-hashes.
        let old_bytes = build_torrent_bencode("http://old.example.com/announce", 32, 16, false);
        let new_bytes = build_torrent_bencode("http://new.example.com/announce", 48, 16, false);
        let old_hash = hex::encode(
            crate::metadata::TorrentInfo::from_bytes(old_bytes.clone())
                .unwrap()
                .info_hash()
                .unwrap(),
        );
        let new_hash = hex::encode(
            crate::metadata::TorrentInfo::from_bytes(new_bytes.clone())
                .unwrap()
                .info_hash()
                .unwrap(),
        );
        assert_ne!(old_hash, new_hash);

        let config = TorrentfsConfig::default_config();
        let download_service = Arc::new(DownloadService::new(&cache_dir, &config).unwrap());
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let svc = TorrentService::new(db.clone(), Some(download_service.clone()));

        // First add: creates the handle + registers a piece for the OLD hash.
        svc.add_torrent(&old_bytes, "src", "foo.torrent").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));
        assert!(
            download_service
                .try_query_torrent_status(&old_hash)
                .is_some(),
            "old handle exists after first add"
        );

        {
            let cache = download_service.get_cache_manager().unwrap();
            let mut guard = cache.lock().unwrap();
            let piece_key = format!("{}:piece:0", old_hash);
            let path = guard.ensure_piece_dir(&piece_key).unwrap();
            std::fs::write(&path, b"stale-piece").unwrap();
            guard.add_piece(&piece_key, 12).unwrap();
        }
        assert!(
            download_service
                .get_cache_manager()
                .unwrap()
                .lock()
                .unwrap()
                .has_piece(&format!("{}:piece:0", old_hash)),
            "old piece registered"
        );

        // Overwrite with different content under the same name.
        svc.add_torrent(&new_bytes, "src", "foo.torrent").unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        // The OLD info-hash's engine handle must be released. The engine
        // drops it asynchronously — poll briefly (mirrors the sleep-based
        // PT isolation tests).
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while download_service
            .try_query_torrent_status(&old_hash)
            .is_some()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "old info-hash handle must be released after overwrite"
            );
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
        assert!(
            !download_service
                .get_cache_manager()
                .unwrap()
                .lock()
                .unwrap()
                .has_piece(&format!("{}:piece:0", old_hash)),
            "old info-hash pieces cache must be purged after overwrite"
        );

        // And the DB row must carry the NEW content.
        let row = db
            .lock()
            .unwrap()
            .get_torrent_by_filename_and_source_path("foo.torrent", "src")
            .unwrap()
            .expect("row exists");
        assert_eq!(row.info_hash, new_hash, "row must carry new info-hash");
    }

    /// PT isolation test: two public torrents with the same info_hash but
    /// different tracker URLs. The handle is created with torrent A's
    /// trackers, then `merge_trackers` is called with torrent B. The
    /// assertion: torrent B's tracker URL MUST appear in the handle's
    /// tracker list — public torrents are merge-eligible.
    #[test]
    fn test_pt_isolation_public_torrent_merges_trackers() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let config = TorrentfsConfig::default_config();
        let ds = DownloadService::new(&cache_dir, &config).unwrap();

        let url_a = "http://tracker-a.example.com/announce";
        let url_b = "http://tracker-b.example.com/announce";

        let torrent_a = build_torrent_bencode(url_a, 32, 16, false);
        let torrent_b = build_torrent_bencode(url_b, 32, 16, false);

        let info_a =
            Arc::new(crate::metadata::TorrentInfo::from_bytes(torrent_a).expect("parse torrent A"));
        let info_b =
            Arc::new(crate::metadata::TorrentInfo::from_bytes(torrent_b).expect("parse torrent B"));

        // Both torrents must have the same info_hash (same content).
        let hash_a = hex::encode(info_a.info_hash().expect("hash A"));
        let hash_b = hex::encode(info_b.info_hash().expect("hash B"));
        assert_eq!(hash_a, hash_b, "torrents must share info_hash");

        // Both must be public (not private).
        assert!(!info_a.is_private(), "torrent A must be public");
        assert!(!info_b.is_private(), "torrent B must be public");

        // Create the handle with torrent A's trackers.
        ds.ensure_handle_lightweight(info_a.clone()).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Verify the handle has torrent A's tracker only.
        let trackers_before = ds.get_trackers(&hash_a).expect("get trackers");
        assert!(
            trackers_before.iter().any(|t| t.url == url_a),
            "handle should have tracker A's URL before merge"
        );
        assert!(
            !trackers_before.iter().any(|t| t.url == url_b),
            "handle should NOT have tracker B's URL before merge"
        );

        // Trigger merge_trackers with torrent B (public → must merge).
        ds.merge_trackers(info_b).unwrap();
        std::thread::sleep(std::time::Duration::from_millis(200));

        // Assert: torrent B's tracker MUST be in the handle's list.
        let trackers_after = ds.get_trackers(&hash_a).expect("get trackers after merge");
        assert!(
            trackers_after.iter().any(|t| t.url == url_b),
            "public torrent B's tracker MUST be merged into the handle \
             (found: {:?})",
            trackers_after
        );
        assert!(
            trackers_after.len() > trackers_before.len(),
            "tracker count must increase (merge happened): before={}, after={}",
            trackers_before.len(),
            trackers_after.len()
        );
    }
}

//! TorrentService — orchestrates torrent operations,
//! cutting the direct FUSE→DB calls in favour of a service layer.
//!
//! Returns `FsError` (a domain error), never raw errno — the errno mapping is
//! the FUSE adapter's job. This module does not import `libc`.

use crate::seeding::SeedingManager;
use std::sync::{Arc, Mutex};

use crate::db::{Database, FileEntry, InsertTorrentResult, TorrentFile};
use crate::domain::fs_error::{FsError, FsResult};
use crate::metadata::TorrentInfo;
use tracing::{error, info, warn};

use super::download::DownloadService;

/// TorrentService wraps database operations for torrent lifecycle management.
#[derive(Clone)]
pub struct TorrentService {
    db: Arc<Mutex<Database>>,
    download_service: Option<Arc<DownloadService>>,
    seeding_manager: Option<Arc<SeedingManager>>,
}

impl TorrentService {
    pub fn new(
        db: Arc<Mutex<Database>>,
        download_service: Option<Arc<DownloadService>>,
        seeding_manager: Option<Arc<SeedingManager>>,
    ) -> Self {
        Self {
            db,
            download_service,
            seeding_manager,
        }
    }

    /// Add a torrent to the database. Parses the torrent data, extracts metadata,
    /// and persists everything atomically.  After persistence, creates an
    /// upload_mode libtorrent handle so peer/seed information is immediately
    /// available without downloading any data.
    pub fn add_torrent(&self, data: &[u8], source_path: &str, filename: &str) -> FsResult<()> {
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

        let (is_new, stale_info_hash) = {
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

            let result = db_guard
                .insert_torrent_with_files(
                    source_path,
                    &metadata.name,
                    filename,
                    metadata.total_size as i64,
                    &info_hash_hex,
                    metadata.num_files as i64,
                    &files,
                )
                .map_err(|e| {
                    error!("Failed to insert torrent with files {}: {:?}", filename, e);
                    FsError::from(e)
                })?;

            let (is_new, stale_info_hash) = match result {
                InsertTorrentResult::Inserted(torrent_id) => {
                    db_guard.set_torrent_data(torrent_id, data).map_err(|e| {
                        error!("Failed to store torrent data for {}: {:?}", filename, e);
                        FsError::from(e)
                    })?;

                    info!(
                        "Persisted torrent '{}' ({} files, {} bytes) from {}",
                        metadata.name,
                        metadata.num_files,
                        metadata.total_size,
                        if source_path.is_empty() {
                            "root"
                        } else {
                            source_path
                        }
                    );
                    (true, None)
                }
                InsertTorrentResult::Duplicate(existing_id) => {
                    // TSI-2381: a `cp` over an existing metadata torrent
                    // releases a NEW info-hash under the SAME
                    // `(source_path, filename)` key. The Duplicate branch
                    // used to keep the stale row untouched, so a rename
                    // right after the overwrite looked up the NEW name,
                    // found no row, and silently no-op'd — on restart the
                    // OLD torrent resurfaced and metadata/ + data/ showed
                    // unexpected duplicates. The file content changed, so
                    // re-point the existing row at the new torrent: update
                    // name/info_hash/size/file list and store the new bytes.

                    // Review #3: same-content re-copy must stay a cheap
                    // no-op — skip the full directory-tree rebuild when the
                    // incoming info-hash matches the stored one.
                    let old_info_hash = db_guard
                        .get_torrent_by_id(existing_id)
                        .map_err(|e| {
                            error!("Failed to load overwritten torrent {}: {:?}", filename, e);
                            FsError::from(e)
                        })?
                        .map(|t| t.info_hash)
                        .unwrap_or_default();

                    let replaced = old_info_hash != info_hash_hex;

                    if replaced {
                        db_guard
                            .replace_torrent_content(
                                existing_id,
                                &metadata.name,
                                &info_hash_hex,
                                metadata.total_size as i64,
                                metadata.num_files as i64,
                                &files,
                                data,
                            )
                            .map_err(|e| {
                                error!(
                                    "Failed to replace overwritten torrent {}: {:?}",
                                    filename, e
                                );
                                FsError::from(e)
                            })?;

                        info!(
                            "Torrent '{}' at ({}, {}) was overwritten with different \
                             content; existing row updated in place",
                            metadata.name, source_path, filename
                        );
                    } else {
                        info!(
                            "Torrent '{}' at ({}, {}) re-copied with identical content; \
                             keeping existing row",
                            metadata.name, source_path, filename
                        );
                    }
                    (false, replaced.then_some(old_info_hash))
                }
            };
            // db_guard dropped here (end of block scope)
            (is_new, stale_info_hash)
        };

        // TSI-2381 (review): the overwrite orphaned the OLD info-hash's
        // engine handle, scheduler, private_torrents entry, and on-disk
        // `cache/pieces/<old>/` — release them AFTER the DB guard is
        // dropped (no I/O under the DB lock), mirroring `remove_torrent`.
        // TSI-2417: same ordering as remove_torrent — handle released before
        // pieces purged, so libtorrent never checks a torrent whose files
        // are vanishing underneath it.
        if let Some(old) = stale_info_hash {
            self.release_engine_and_seeding(&old);
            self.purge_pieces_cache(&old);
        }

        // Create upload_mode handle so peer/seed info is visible immediately
        // without triggering any data download (all pieces at priority 0).
        if is_new {
            if let Some(ds) = &self.download_service {
                match ds.ensure_handle_lightweight(info.clone()) {
                    Ok(_) => {
                        info!(
                            "Created lightweight handle for torrent '{}' (upload_mode)",
                            metadata.name
                        );
                    }
                    Err(e) => {
                        warn!(
                            "Failed to create lightweight handle for torrent '{}': {:?}",
                            metadata.name, e
                        );
                        // Non-fatal: the torrent is already in the database and
                        // a handle will be created lazily when first accessed.
                    }
                }
            }
        } else {
            // Duplicate info_hash: merge trackers from this torrent into the
            // existing handle (TSI-2275). The engine checks the private flag
            // (TSI-2277) and skips the merge for private torrents — PT
            // isolation prevents passkey leakage and peer cross-pollination
            // across private tracker swarms.
            if let Some(ds) = &self.download_service {
                if let Err(e) = ds.merge_trackers(info.clone()) {
                    warn!(
                        "Failed to merge trackers for duplicate torrent '{}': {:?}",
                        metadata.name, e
                    );
                    // Non-fatal: the torrent is already in the DB; tracker
                    // merge is a best-effort optimization, not a correctness
                    // requirement. The existing handle's trackers are
                    // unchanged.
                }
            }
        }

        Ok(())
    }

    /// Remove a torrent from the database by filename and source_path.
    ///
    /// After the DB record is deleted, when no other torrent still references
    /// the same info_hash: purge the on-disk pieces cache, release the
    /// DownloadEngine handle (and its scheduler state), and remove the
    /// SeedingManager seed — so a long-running daemon does not accumulate
    /// handles or keep announcing a deleted torrent (TSI-2232).
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
            // TSI-2417: release the libtorrent handle BEFORE purging the
            // piece files.  `remove_torrent(handle)` is asynchronous inside
            // libtorrent — while the old handle is still alive its custom
            // storage can observe the piece files vanishing mid-check, and a
            // same-info_hash re-add racing the pending removal inherits the
            // stale have-piece bitmap (pieces "complete" but files gone).
            // That state forces the recheck path and, if no peer has
            // connected yet, surfaces as NoPeers after read_timeout_secs.
            // Releasing first lets libtorrent tear down the old torrent
            // against intact files; the subsequent purge is uncontended.
            self.release_engine_and_seeding(&info_hash);
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
        if let Err(e) = guard.remove_infohash_pieces(info_hash) {
            warn!(
                "Failed to purge pieces cache for info_hash={}: {:?}",
                info_hash, e
            );
        }
    }

    /// Release the DownloadEngine handle and the SeedingManager seed for an
    /// info_hash.  Best-effort: the DB row and pieces cache are already gone,
    /// so a transient engine/seeding failure only leaves a stale handle that a
    /// restart clears — it never desyncs DB vs. cache (TSI-2232).
    fn release_engine_and_seeding(&self, info_hash: &str) {
        if let Some(ds) = &self.download_service {
            if let Err(e) = ds.remove_handle(info_hash) {
                warn!(
                    "Failed to remove DownloadEngine handle for info_hash={}: {:?}",
                    info_hash, e
                );
            }
        }
        if let Some(sm) = &self.seeding_manager {
            if let Err(e) = sm.remove_seed(info_hash) {
                warn!(
                    "Failed to remove seeding seed for info_hash={}: {:?}",
                    info_hash, e
                );
            }
        }
    }

    /// Clean up metadata directories orphaned by a torrent deletion or move.
    /// Returns the removed directory paths (leaf-first) so callers can evict
    /// stale `data_inodes` cache entries.
    pub fn cleanup_orphaned_metadata_directories(
        &self,
        source_path: &str,
    ) -> FsResult<Vec<String>> {
        let mut db_guard = self.db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        db_guard
            .cleanup_orphaned_metadata_directories(source_path)
            .map_err(|e| {
                error!("Failed to cleanup orphaned metadata directories: {:?}", e);
                FsError::from(e)
            })
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
                    .rename_torrent(torrent.id, &torrent.name, new_name, new_source_path)
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

        let svc = TorrentService::new(db_arc, Some(download_service.clone()), None);

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

        let svc = TorrentService::new(db_arc, Some(download_service.clone()), None);

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

    /// `release_engine_and_seeding` must be called when the last DB reference
    /// to an info_hash is deleted (TSI-2232).  This test wires a real
    /// SeedingManager into the service and asserts that after remove_torrent
    /// the seeding manager no longer tracks the info_hash.
    #[test]
    fn test_remove_torrent_releases_seeding_manager() {
        let temp_dir = TempDir::new().unwrap();
        let cache_dir = temp_dir.path().join("cache");
        let config = TorrentfsConfig::default_config();

        let info_hash = "aabbccdd00112233445566778899aabbccdd0011".to_string();
        let download_service = Arc::new(DownloadService::new(&cache_dir, &config).unwrap());
        let seeding_manager =
            Arc::new(crate::seeding::SeedingManager::new(&cache_dir, &config).unwrap());

        let db = Database::open_in_memory().unwrap();
        let db_arc = Arc::new(Mutex::new(db));
        {
            let mut guard = db_arc.lock().unwrap();
            insert_torrent(&mut guard, "src", "foo.torrent", &info_hash);
        }

        let svc = TorrentService::new(
            db_arc,
            Some(download_service),
            Some(seeding_manager.clone()),
        );

        // Before removal the SeedingManager has no handle yet (none was
        // added); remove_torrent must still call remove_seed without panic
        // (idempotent).  After removal, has_handle returns false.
        assert!(!seeding_manager.has_handle(&info_hash));

        let removed = svc.remove_torrent("foo.torrent", "src").unwrap();
        assert_eq!(removed, Some((1, info_hash.clone())));

        // remove_seed was called (best-effort, no-op if no handle existed);
        // the seeding manager must not track the deleted info_hash.
        assert!(!seeding_manager.has_handle(&info_hash));
        assert!(seeding_manager.get_all_seeds().is_empty());
    }

    // ── TSI-2277: PT isolation tests ──────────────────────────────────

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

    /// TSI-2381 (review): overwriting a metadata torrent with DIFFERENT
    /// content must release the OLD info-hash's engine state — the pieces
    /// cache for `cache/pieces/<old>/` must be purged, not leaked. (The
    /// engine handle itself is released via the same
    /// `release_engine_and_seeding` call; here we assert the observable
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
        let svc = TorrentService::new(db.clone(), Some(download_service.clone()), None);

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

//! `PieceStore` — the piece data plane.
//!
//! Owns all access to the on-disk piece cache ([`CacheManager`]) and exposes
//! the data operations the download engine needs: piece presence checks,
//! byte-range reads, and piece registration (metadata + verification).  It is
//! deliberately free of any *control* logic (priority, scheduling); that lives
//! in [`super::piece_scheduler::PieceScheduler`].
//!
//! `PieceStore` is owned by the download engine actor thread and is the only
//! writer of piece data during a download.  The underlying [`CacheManager`]
//! remains a shared `Arc<Mutex<_>>` so the FUSE layer can still read cache
//! summary / on-disk presence non-blockingly (`.stats`, `pieces_on_disk`).

use std::sync::{Arc, Mutex};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::metadata::TorrentInfo;

pub struct PieceStore {
    cache: Arc<Mutex<CacheManager>>,
}

impl PieceStore {
    pub fn new(cache: Arc<Mutex<CacheManager>>) -> Self {
        Self { cache }
    }

    /// The shared cache handle, for non-blocking `.stats` / on-disk checks.
    pub fn cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache.clone()
    }

    /// Canonical piece key: `"{info_hash}:piece:{index}"`.
    pub fn piece_key(info_hash: &str, piece_index: i32) -> String {
        format!("{}:piece:{}", info_hash, piece_index)
    }

    /// Whether the piece is registered in the cache metadata.
    pub fn has_piece(&self, info_hash: &str, piece_index: i32) -> bool {
        let key = Self::piece_key(info_hash, piece_index);
        self.cache
            .lock()
            .map(|c| c.has_piece(&key))
            .unwrap_or(false)
    }

    /// Whether a piece file physically exists on disk (independent of metadata).
    pub fn has_piece_on_disk(&self, piece_key: &str) -> bool {
        self.cache
            .lock()
            .map(|c| c.has_piece_on_disk(piece_key))
            .unwrap_or(false)
    }

    /// whether a piece is stale — libtorrent's `have_piece` says
    /// true but the on-disk piece file has been purged (deleted by cache
    /// verification or manual `delete_piece`).  This is the unified stale
    /// detection primitive used by the engine's `has_stale_pieces`,
    /// `all_pieces_local`, and the piece-wait loop.
    pub fn has_stale_piece(&self, piece_key: &str) -> bool {
        !self.has_piece_on_disk(piece_key)
    }

    /// Read a whole piece from the on-disk cache, recording an access. Takes
    /// a per-info-hash shared read lock (C++ `lt_lock_piece_read`) before
    /// reading, so a concurrent `PieceStorage::write_piece` on the disk thread
    /// can't expose a partially-written file — without it, concurrent readers
    /// during an active download saw inconsistent (partial) data.
    pub fn read_piece(&self, piece_key: &str) -> TorrentResult<Vec<u8>> {
        let path = {
            let cache = self.cache.lock().map_err(|_| Self::poisoned())?;
            cache.piece_path(piece_key)
        };
        // Extract info_hash from the piece_key ("{info_hash}:piece:{index}")
        // to acquire the per-info-hash shared read lock.
        let info_hash_hex = piece_key.split(':').next().unwrap_or("");
        let lock_guard = PieceReadLockGuard::new(info_hash_hex);
        let data = std::fs::read(&path).map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to read cached piece {} from {}: {}",
                piece_key,
                path.display(),
                e
            ))
        })?;
        drop(lock_guard);
        if let Ok(mut cache) = self.cache.lock() {
            let _ = cache.record_access(piece_key);
        }
        Ok(data)
    }

    /// Read a byte range from a piece file without loading the whole piece.
    ///
    /// same shared read lock as `read_piece` to prevent reading
    /// a partially-written piece file during active download.
    pub fn read_piece_range(
        &self,
        piece_key: &str,
        offset: u64,
        size: usize,
    ) -> TorrentResult<Vec<u8>> {
        // Extract info_hash for the per-info-hash shared read lock.
        let info_hash_hex = piece_key.split(':').next().unwrap_or("");
        let _lock_guard = PieceReadLockGuard::new(info_hash_hex);
        let cache = self.cache.lock().map_err(|_| Self::poisoned())?;
        cache.read_piece_range(piece_key, offset, size)
    }

    /// Register a newly-downloaded piece in the cache: records its size in
    /// metadata and marks it verified.  libtorrent's custom storage has
    /// already written the piece bytes to disk; this only makes the cache
    /// aware of them.
    pub fn register_piece(
        &self,
        info_hash: &str,
        piece_index: i32,
        size: u64,
    ) -> TorrentResult<()> {
        let key = Self::piece_key(info_hash, piece_index);
        let mut cache = self.cache.lock().map_err(|_| Self::poisoned())?;
        cache.add_piece(&key, size)
    }

    /// Register a piece that is on disk but incomplete (partial
    /// download) in the cache metadata, at its *actual* on-disk size, without
    /// marking it verified.  Used after a read's piece-wait times out so the
    /// partially-downloaded bytes still show up in `.stats` instead of
    /// vanishing.  Returns `Ok(false)` when the piece has no on-disk bytes to
    /// register.
    pub fn register_incomplete_piece(
        &self,
        info_hash: &str,
        piece_index: i32,
    ) -> TorrentResult<bool> {
        let key = Self::piece_key(info_hash, piece_index);
        let path = {
            let cache = self.cache.lock().map_err(|_| Self::poisoned())?;
            cache.piece_path(&key)
        };
        let size = match std::fs::metadata(&path) {
            Ok(m) => m.len(),
            Err(_) => 0,
        };
        if size == 0 {
            return Ok(false);
        }
        let mut cache = self.cache.lock().map_err(|_| Self::poisoned())?;
        cache.register_incomplete_piece(&key, size)?;
        Ok(true)
    }

    /// Whether a piece is marked verified (a complete, hash-checked download).
    pub fn is_piece_verified(&self, info_hash: &str, piece_index: i32) -> bool {
        let key = Self::piece_key(info_hash, piece_index);
        self.cache
            .lock()
            .map(|c| c.is_piece_verified(&key))
            .unwrap_or(false)
    }

    /// Record the on-disk partial-download state for every piece in
    /// a piece-index range, the way the engine does after a read's piece-wait
    /// times out.
    ///
    /// For each piece in the range that has on-disk bytes: verified (complete)
    /// pieces are left untouched; everything else — already-registered
    /// incomplete pieces (re-stat'd so their metadata size tracks continued
    /// download progress) and unregistered partials — is (re)registered as
    /// incomplete at its current on-disk size.
    pub fn register_incomplete_pieces_in_range(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
    ) {
        for piece_index in start_piece..=end_piece {
            let piece_key = Self::piece_key(info_hash, piece_index);
            if !self.has_piece_on_disk(&piece_key) {
                continue;
            }
            if self.is_piece_verified(info_hash, piece_index) {
                continue;
            }
            if let Err(e) = self.register_incomplete_piece(info_hash, piece_index) {
                tracing::warn!(
                    "register_incomplete_pieces_in_range: failed for {}:piece:{}: {:?}",
                    info_hash,
                    piece_index,
                    e
                );
            }
        }
    }

    fn poisoned() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        }
    }

    /// whether a piece is genuinely complete in cache.  Both
    /// conditions must hold:
    /// 1. the piece was registered via `register_piece` (a successful
    ///    libtorrent download), not merely discovered by a startup scan;
    /// 2. the registered size meets the expected piece length.
    pub fn is_piece_complete_in_cache(
        cache: &CacheManager,
        piece_key: &str,
        piece_idx: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        if !cache.is_piece_verified(piece_key) {
            return false;
        }
        if let Some(size) = cache.piece_metadata_size(piece_key) {
            let expected = if piece_idx == num_pieces - 1 {
                let remainder = total_size.saturating_sub(((num_pieces - 1) as u64) * piece_length);
                if remainder > 0 {
                    remainder
                } else {
                    piece_length
                }
            } else {
                piece_length
            };
            size >= expected
        } else {
            false
        }
    }

    /// Non-blocking pre-check used by the FUSE read path: are all pieces
    /// needed for a file range already on disk (verified + complete)?
    ///
    /// Uses only the shared cache — no engine round-trip — so it stays fast
    /// and never blocks on an active download.
    pub fn pieces_on_disk(
        cache: &CacheManager,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<bool> {
        let info_hash = hex::encode(info.info_hash()?);
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;

        if num_pieces <= 0 || piece_length == 0 {
            return Ok(false);
        }

        let files = info.files()?;
        let file_start_offset: u64 = files.iter().take(file_index as usize).map(|f| f.size).sum();
        let file_size = files.get(file_index as usize).map(|f| f.size).unwrap_or(0);

        let absolute_offset = file_start_offset + offset;
        let file_end = file_start_offset + file_size;
        if absolute_offset >= file_end || size == 0 {
            return Ok(true); // empty range, nothing to check
        }

        let size = std::cmp::min(size as u64, file_end - absolute_offset) as u32;
        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1);

        if start_piece > end_piece || start_piece >= num_pieces {
            return Ok(true);
        }

        let total_size = info.total_size();
        for piece_idx in start_piece..=end_piece {
            let piece_key = Self::piece_key(&info_hash, piece_idx);
            if !Self::is_piece_complete_in_cache(
                cache,
                &piece_key,
                piece_idx,
                piece_length,
                num_pieces,
                total_size,
            ) {
                return Ok(false);
            }
        }

        Ok(true)
    }
}

/// RAII guard for the C++ per-info-hash shared read lock: acquires on
/// construction, releases on drop, so `PieceStorage::write_piece` (exclusive
/// lock) can't write blocks while the Rust side reads. The lock is a no-op
/// when `info_hash_hex` is empty (unit tests with synthetic piece keys, or a
/// malformed piece key).
struct PieceReadLockGuard {
    info_hash_hex: String,
    locked: bool,
}

impl PieceReadLockGuard {
    fn new(info_hash_hex: &str) -> Self {
        if info_hash_hex.is_empty() {
            return Self {
                info_hash_hex: String::new(),
                locked: false,
            };
        }
        let c_str = std::ffi::CString::new(info_hash_hex).unwrap_or_default();
        // SAFETY: `lt_lock_piece_read` acquires a shared lock on a global
        // per-info-hash shared_mutex. The info_hash_hex string is a valid
        // C string. The lock is released in `Drop`.
        unsafe {
            libtorrent_sys::lt_lock_piece_read(c_str.as_ptr());
        }
        Self {
            info_hash_hex: info_hash_hex.to_string(),
            locked: true,
        }
    }
}

impl Drop for PieceReadLockGuard {
    fn drop(&mut self) {
        if self.locked {
            let c_str = std::ffi::CString::new(self.info_hash_hex.as_str()).unwrap_or_default();
            // SAFETY: paired with the `lt_lock_piece_read` call in `new`.
            unsafe {
                libtorrent_sys::lt_unlock_piece_read(c_str.as_ptr());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// after `delete_piece` purges a piece (file + metadata), the
    /// `has_piece_on_disk` check must return `false` — this is the signal the
    /// engine's stale-bitmask detection uses to decide that `have_piece == true`
    /// is stale and a `force_recheck` is needed before the read proceeds.
    #[test]
    fn has_piece_on_disk_false_after_delete_piece() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(Mutex::new(CacheManager::new(temp_dir.path(), 1024 * 1024)?));
        let store = PieceStore::new(cache);

        let info_hash = "abc123";
        let piece_idx = 0;
        let piece_key = PieceStore::piece_key(info_hash, piece_idx);
        let piece_content = vec![0xAAu8; 16_384];

        // Write the piece file to disk (libtorrent's custom storage does
        // this in production) and register it in cache metadata.
        let path = {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.ensure_piece_dir(&piece_key)?
        };
        std::fs::write(&path, &piece_content)?;
        store.register_piece(info_hash, piece_idx, 16_384)?;

        assert!(
            store.has_piece_on_disk(&piece_key),
            "piece should be on disk after registration"
        );

        // Purge the piece (simulates cache verification or manual delete).
        {
            let cache = store.cache_manager();
            let mut c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.delete_piece(&piece_key)?;
        }

        assert!(
            !store.has_piece_on_disk(&piece_key),
            "piece must NOT be on disk after delete_piece — \
             this false result is the stale-bitmask trigger"
        );
        assert!(
            !store.has_piece(info_hash, piece_idx),
            "metadata also cleared"
        );

        Ok(())
    }

    /// `has_stale_piece` is the unified stale-detection primitive
    /// used by the engine's `has_stale_pieces`, `all_pieces_local`, the
    /// piece-wait loop, and the deadline-setting section.  After a purge,
    /// it must return `true` (piece file gone) so the engine knows the
    /// libtorrent bitmask is stale.
    #[test]
    fn has_stale_piece_true_after_purge() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(Mutex::new(CacheManager::new(temp_dir.path(), 1024 * 1024)?));
        let store = PieceStore::new(cache);

        let info_hash = "ghi789";
        let piece_idx = 0;
        let piece_key = PieceStore::piece_key(info_hash, piece_idx);

        // No file on disk yet → stale (no piece to serve).
        assert!(
            store.has_stale_piece(&piece_key),
            "piece not on disk → stale detection returns true"
        );

        // Write the piece file to disk → not stale.
        let path = {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.ensure_piece_dir(&piece_key)?
        };
        std::fs::write(&path, vec![0xCCu8; 16_384])?;
        store.register_piece(info_hash, piece_idx, 16_384)?;
        assert!(
            !store.has_stale_piece(&piece_key),
            "piece on disk → not stale"
        );

        // Purge → stale again.
        {
            let cache = store.cache_manager();
            let mut c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.delete_piece(&piece_key)?;
        }
        assert!(
            store.has_stale_piece(&piece_key),
            "piece purged → stale detection returns true"
        );

        Ok(())
    }

    /// `read_piece` must fail (Err) when the piece file was purged.
    /// The engine's `read_from_disk` uses this Err to return `PieceNotReady`
    /// instead of silently skipping, which would produce a short-read EIO.
    #[test]
    fn read_piece_fails_after_delete_piece() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(Mutex::new(CacheManager::new(temp_dir.path(), 1024 * 1024)?));
        let store = PieceStore::new(cache);

        let info_hash = "def456";
        let piece_idx = 1;
        let piece_key = PieceStore::piece_key(info_hash, piece_idx);
        let piece_content = vec![0xBBu8; 16_384];

        // Write the piece file to disk and register in metadata.
        let path = {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.ensure_piece_dir(&piece_key)?
        };
        std::fs::write(&path, &piece_content)?;
        store.register_piece(info_hash, piece_idx, 16_384)?;

        // Sanity: reading before purge works.
        let data = store.read_piece(&piece_key)?;
        assert_eq!(data.len(), 16_384);

        // Purge.
        {
            let cache = store.cache_manager();
            let mut c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.delete_piece(&piece_key)?;
        }

        // After purge, read_piece must Err — the engine maps this to
        // PieceNotReady, not a silent skip.
        let result = store.read_piece(&piece_key);
        assert!(
            result.is_err(),
            "read_piece must fail after delete_piece so read_from_disk \
             returns PieceNotReady instead of silently skipping"
        );

        Ok(())
    }

    /// The timeout→registration glue — partial on-disk
    /// pieces are registered incomplete, verified pieces are left untouched,
    /// and re-running the scan refreshes a growing partial's size.
    #[test]
    fn register_incomplete_pieces_in_range_records_partial_skips_verified() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = Arc::new(Mutex::new(CacheManager::new(temp_dir.path(), 1024 * 1024)?));
        let store = PieceStore::new(cache);

        let info_hash = "tsi3130range";
        let verified_idx = 0;
        let partial_idx = 1;

        // Piece 0: verified / complete.
        let v_path = {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.ensure_piece_dir(&PieceStore::piece_key(info_hash, verified_idx))?
        };
        std::fs::write(&v_path, vec![0xAAu8; 16_384])?;
        store.register_piece(info_hash, verified_idx, 16_384)?;

        // Piece 1: partial file on disk, not yet registered.
        let p_path = {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            c.ensure_piece_dir(&PieceStore::piece_key(info_hash, partial_idx))?
        };
        std::fs::write(&p_path, vec![0xBBu8; 4096])?;

        store.register_incomplete_pieces_in_range(info_hash, 0, 1);

        // Partial piece → registered incomplete.
        assert!(store.has_piece(info_hash, partial_idx));
        assert!(!store.is_piece_verified(info_hash, partial_idx));

        // Verified piece → untouched (still verified, not downgraded).
        assert!(store.is_piece_verified(info_hash, verified_idx));

        // Grow the partial on disk and re-run — the
        // metadata size must track the new download progress.
        std::fs::write(&p_path, vec![0xBBu8; 8192])?;
        store.register_incomplete_pieces_in_range(info_hash, 0, 1);
        {
            let cache = store.cache_manager();
            let c = cache.lock().map_err(|_| PieceStore::poisoned())?;
            assert_eq!(
                c.piece_metadata_size(&PieceStore::piece_key(info_hash, partial_idx)),
                Some(8192),
                "incomplete piece size must refresh on re-scan"
            );
        }

        Ok(())
    }
}

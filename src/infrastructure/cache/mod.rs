use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{TorrentError, TorrentResult};

const CACHE_METADATA_FILE: &str = "cache_metadata.txt";
const INCOMPLETE_SUFFIX: &str = ".incomplete";

#[derive(Debug, Clone)]
pub struct PieceMetadata {
    pub last_accessed: u64,
    pub size: u64,
    pub hit_count: u64,
}

pub struct CacheManager {
    cache_dir: PathBuf,
    metadata: HashMap<String, PieceMetadata>,
    max_cache_size: u64,
    current_size: u64,
    pub miss_count: u64,
    pub hit_count: u64,
    evict_callbacks: Vec<Box<dyn Fn(String, i32) + Send + Sync>>,
    /// TSI-2491: pieces with an on-disk `.incomplete` marker — written by the
    /// C++ `PieceStorage::write_piece` while a piece is still being filled.
    /// Rebuilt by `scan_pieces_subdirectory` on restart and cleared by
    /// `add_piece` / `remove_piece` / `mark_verified`.
    incomplete_piece_keys: HashSet<String>,
    /// TSI-2048: pieces whose integrity was verified via add_piece
    /// (called after a successful libtorrent read_piece).  Pieces
    /// registered solely by scan_pieces_subdirectory at startup are
    /// NOT in this set — they may be incomplete sparse files.
    verified_piece_keys: HashSet<String>,
    /// TSI-2274: metadata dirtied since the last `save_metadata_file`.
    /// Mutating methods (`record_access`, `add_piece`, `remove_piece`,
    /// `remove_infohash_pieces`, `mark_verified`) set this instead of
    /// fsyncing on every call; the engine loop calls
    /// `flush_metadata_if_dirty` periodically and `flush` on shutdown.
    metadata_dirty: bool,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}
impl CacheManager {
    pub fn new(cache_dir: &Path, max_cache_size: u64) -> TorrentResult<Self> {
        let cache_dir = cache_dir.to_path_buf();
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).map_err(|e| {
                TorrentError::IoError(format!("Failed to create cache directory: {}", e))
            })?;
        }
        let mut manager = CacheManager {
            cache_dir,
            metadata: HashMap::new(),
            max_cache_size,
            current_size: 0,
            miss_count: 0,
            hit_count: 0,
            evict_callbacks: Vec::new(),
            incomplete_piece_keys: HashSet::new(),
            verified_piece_keys: HashSet::new(),
            metadata_dirty: false,
        };

        manager.rebuild_index()?;

        Ok(manager)
    }

    pub fn rebuild_index(&mut self) -> TorrentResult<()> {
        let metadata_path = self.cache_dir.join(CACHE_METADATA_FILE);

        if metadata_path.exists() {
            self.load_metadata_file(&metadata_path)?;
        }

        self.scan_cache_directory()?;

        self.save_metadata_file()?;
        self.metadata_dirty = false;

        Ok(())
    }

    /// TSI-2263 / TSI-2274: persist the cache metadata to disk with fsync.
    ///
    /// Mutating methods only flag `metadata_dirty` instead of fsyncing on
    /// every call (TSI-2274: `record_access` was fsyncing on every piece
    /// read, limiting throughput to ~1.2 MB/s).  This flush is called on
    /// shutdown (graceful) and periodically by the engine loop so the
    /// metadata state is durable before the process exits.  Without fsync,
    /// a container restart can leave `cache_metadata.txt` stale, causing
    /// the restart scan to register pieces at wrong sizes and the verifier
    /// to purge them.
    pub fn flush(&mut self) -> TorrentResult<()> {
        let result = self.save_metadata_file();
        if result.is_ok() {
            self.metadata_dirty = false;
        }
        result
    }

    /// TSI-2274: write the metadata file only if dirty since the last
    /// flush.  Called periodically from the engine loop so the on-disk
    /// metadata does not lag too far behind the in-memory state.  Returns
    /// `Ok(())` when nothing needs writing.
    pub fn flush_metadata_if_dirty(&mut self) -> TorrentResult<()> {
        if !self.metadata_dirty {
            return Ok(());
        }
        self.flush()
    }

    fn load_metadata_file(&mut self, path: &Path) -> TorrentResult<()> {
        let file = File::open(path)
            .map_err(|e| TorrentError::IoError(format!("Failed to open metadata file: {}", e)))?;
        let reader = BufReader::new(file);

        for line in reader.lines() {
            let line = line.map_err(|e| {
                TorrentError::IoError(format!("Failed to read metadata line: {}", e))
            })?;

            let parts: Vec<&str> = line.split('\t').collect();
            if parts.len() >= 3 {
                let piece_key = parts[0].to_string();
                if let Ok(last_accessed) = parts[1].parse::<u64>() {
                    if let Ok(size) = parts[2].parse::<u64>() {
                        let hit_count = if parts.len() >= 4 {
                            parts[3].parse::<u64>().unwrap_or(0)
                        } else {
                            0
                        };
                        self.metadata.insert(
                            piece_key,
                            PieceMetadata {
                                last_accessed,
                                size,
                                hit_count,
                            },
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn scan_cache_directory(&mut self) -> TorrentResult<()> {
        self.current_size = 0;

        if let Err(e) = self.migrate_old_cache_files() {
            tracing::warn!("Failed to migrate old cache files: {:?}", e);
        }

        let pieces_dir = self.cache_dir.join("pieces");
        if !pieces_dir.exists() {
            return Ok(());
        }

        self.scan_pieces_subdirectory(&pieces_dir)?;

        Ok(())
    }

    fn migrate_old_cache_files(&mut self) -> TorrentResult<()> {
        let entries = fs::read_dir(&self.cache_dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read cache directory: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                TorrentError::IoError(format!("Failed to read directory entry: {}", e))
            })?;
            let path = entry.path();

            if path.is_dir() {
                continue;
            }

            let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

            if filename == CACHE_METADATA_FILE {
                continue;
            }

            if !filename.contains(':') {
                continue;
            }

            let info_hash = match filename.split(':').next() {
                Some(hash) => hash,
                None => continue,
            };

            let new_dir = self.cache_dir.join("pieces").join(info_hash);
            if !new_dir.exists() {
                fs::create_dir_all(&new_dir).map_err(|e| {
                    TorrentError::IoError(format!("Failed to create pieces directory: {}", e))
                })?;
            }

            let new_path = new_dir.join(filename);
            fs::rename(&path, &new_path).map_err(|e| {
                TorrentError::IoError(format!("Failed to migrate cache file: {}", e))
            })?;

            tracing::info!(
                "Migrated old cache file: {} -> {}",
                path.display(),
                new_path.display()
            );
        }

        Ok(())
    }

    fn scan_pieces_subdirectory(&mut self, dir: &Path) -> TorrentResult<()> {
        let entries = fs::read_dir(dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read directory: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                TorrentError::IoError(format!("Failed to read directory entry: {}", e))
            })?;
            let path = entry.path();

            if path.is_dir() {
                self.scan_pieces_subdirectory(&path)?;
            } else if path.is_file() {
                let filename = path.file_name().and_then(|n| n.to_str()).unwrap_or("");

                if filename == CACHE_METADATA_FILE {
                    continue;
                }

                // TSI-2491: `.incomplete` files are markers, not piece data.
                // They are consumed below to tag their sibling piece as
                // incomplete; never register them as pieces themselves.
                if filename.ends_with(INCOMPLETE_SUFFIX) {
                    continue;
                }

                let piece_key = filename.to_string();

                let metadata = fs::metadata(&path).map_err(|e| {
                    TorrentError::IoError(format!("Failed to get file metadata: {}", e))
                })?;

                let size = metadata.len();

                let last_accessed = self
                    .metadata
                    .get(&piece_key)
                    .map(|m| m.last_accessed)
                    .unwrap_or_else(current_timestamp_ms);

                let hit_count = self
                    .metadata
                    .get(&piece_key)
                    .map(|m| m.hit_count)
                    .unwrap_or(0);

                // TSI-2491: a sibling `<piece_key>.incomplete` marker means
                // the C++ write path is still filling this piece. Keep it in
                // metadata (so has_piece/size stay consistent) but exclude it
                // from background SHA-1 verification.
                if self.piece_marker_path(&piece_key).exists() {
                    self.incomplete_piece_keys.insert(piece_key.clone());
                }

                self.metadata.insert(
                    piece_key.clone(),
                    PieceMetadata {
                        last_accessed,
                        size,
                        hit_count,
                    },
                );
                self.current_size += size;
            }
        }

        Ok(())
    }

    fn save_metadata_file(&self) -> TorrentResult<()> {
        let metadata_path = self.cache_dir.join(CACHE_METADATA_FILE);
        let file = File::create(&metadata_path)
            .map_err(|e| TorrentError::IoError(format!("Failed to create metadata file: {}", e)))?;
        let mut writer = BufWriter::new(file);

        for (piece_key, meta) in &self.metadata {
            writeln!(
                writer,
                "{}\t{}\t{}\t{}",
                piece_key, meta.last_accessed, meta.size, meta.hit_count
            )
            .map_err(|e| TorrentError::IoError(format!("Failed to write metadata: {}", e)))?;
        }

        // TSI-2263: flush the BufWriter into the OS page cache, then
        // fsync the underlying file descriptor so the metadata reaches
        // disk.  BufWriter::flush only pushes bytes to the OS buffer;
        // without fsync a container restart can leave cache_metadata.txt
        // empty or stale, causing scan_pieces_subdirectory to register
        // pieces at wrong sizes and the verifier to purge them.
        writer
            .flush()
            .map_err(|e| TorrentError::IoError(format!("Failed to flush metadata file: {}", e)))?;
        let inner = writer
            .into_inner()
            .map_err(|e| TorrentError::IoError(format!("Failed to flush metadata file: {}", e)))?;
        inner
            .sync_all()
            .map_err(|e| TorrentError::IoError(format!("Failed to fsync metadata file: {}", e)))?;

        Ok(())
    }

    pub fn record_access(&mut self, piece_key: &str) -> TorrentResult<()> {
        let now = current_timestamp_ms();

        if let Some(meta) = self.metadata.get_mut(piece_key) {
            meta.last_accessed = now;
            meta.hit_count += 1;
            self.hit_count += 1;
        } else {
            self.miss_count += 1;
            return Err(TorrentError::IoError(format!(
                "Piece not found in cache: {}",
                piece_key
            )));
        }

        // TSI-2274: do not fsync on every read — mark dirty and let
        // the engine loop flush periodically / on shutdown.
        self.metadata_dirty = true;
        Ok(())
    }

    pub fn add_piece(&mut self, piece_key: &str, size: u64) -> TorrentResult<()> {
        let now = current_timestamp_ms();

        // TSI-2231: insert returns the previous metadata if the piece_key
        // already existed (e.g. after restart scan_pieces_subdirectory
        // already counted the on-disk piece into current_size).  Account
        // only for the size delta instead of re-adding the full size,
        // which would inflate current_size and cause premature LRU
        // eviction of other torrents' valid cache pieces.
        let prev = self.metadata.insert(
            piece_key.to_string(),
            PieceMetadata {
                last_accessed: now,
                size,
                hit_count: 0,
            },
        );

        let is_new = prev.is_none();

        if is_new {
            self.miss_count += 1;
        }

        let old_size = prev.map(|m| m.size).unwrap_or(0);

        // TSI-2048: mark as verified — add_piece is only called after
        // a successful libtorrent read_piece, which is the authoritative
        // proof of piece completeness.
        self.verified_piece_keys.insert(piece_key.to_string());

        // TSI-2491: the piece is now provably complete — clear any
        // in-progress marker left by the C++ write path.
        self.incomplete_piece_keys.remove(piece_key);
        let marker = self.piece_marker_path(piece_key);
        if marker.exists() {
            if let Err(e) = fs::remove_file(&marker) {
                tracing::warn!(
                    "Failed to remove incomplete marker {}: {}",
                    marker.display(),
                    e
                );
            }
        }

        self.current_size += size.saturating_sub(old_size);

        if self.current_size > self.max_cache_size {
            self.evict_lru()?;
        }

        // TSI-2274: mark dirty — the engine loop flushes periodically
        // and on shutdown instead of fsyncing on every add_piece.
        self.metadata_dirty = true;
        Ok(())
    }

    /// Register a callback that will be invoked when a piece is evicted.
    /// The callback receives the info_hash of the affected torrent and the piece_index.
    pub fn on_evict(&mut self, callback: Box<dyn Fn(String, i32) + Send + Sync>) {
        self.evict_callbacks.push(callback);
    }

    pub fn evict_lru(&mut self) -> TorrentResult<()> {
        while self.current_size > self.max_cache_size && !self.metadata.is_empty() {
            let oldest = self
                .metadata
                .iter()
                .min_by_key(|(_, meta)| meta.last_accessed)
                .map(|(k, _)| k.clone());

            if let Some(piece_key) = oldest {
                let info_hash = self.extract_info_hash(&piece_key).to_string();
                let piece_index = self.extract_piece_index(&piece_key);
                self.remove_piece(&piece_key)?;
                // Notify registered callbacks about the evicted piece
                if info_hash != "unknown" {
                    for callback in &self.evict_callbacks {
                        callback(info_hash.clone(), piece_index);
                    }
                }
            } else {
                break;
            }
        }

        Ok(())
    }

    fn remove_piece(&mut self, piece_key: &str) -> TorrentResult<()> {
        let piece_path = self.piece_path(piece_key);

        // TSI-2242: debit the registered PieceMetadata.size — the same
        // value credited by add_piece / scan_pieces_subdirectory — so
        // current_size stays symmetric on add and remove.  Using the
        // on-disk file length here caused permanent upward drift when a
        // piece's disk file was shorter than its registered size
        // (partial write / crash-restart, the scenario the tests at
        // test_has_piece_true_but_disk_file_empty and
        // test_metadata_size_mismatch_disk_size document): the full
        // registered size was credited on add but only the actual disk
        // length was debited on remove, so current_size never returned
        // to zero and the cache over-evicted.
        let registered_size = self.metadata.get(piece_key).map(|m| m.size).unwrap_or(0);

        if piece_path.exists() {
            fs::remove_file(&piece_path).map_err(|e| {
                TorrentError::IoError(format!("Failed to remove piece file: {}", e))
            })?;
        }

        // TSI-2491: drop the in-progress marker alongside the piece file so
        // a stale marker never tags a future re-download of the same piece.
        let marker_path = self.piece_marker_path(piece_key);
        if marker_path.exists() {
            if let Err(e) = fs::remove_file(&marker_path) {
                tracing::warn!(
                    "Failed to remove incomplete marker {}: {}",
                    marker_path.display(),
                    e
                );
            }
        }
        self.current_size = self.current_size.saturating_sub(registered_size);

        self.metadata.remove(piece_key);
        self.verified_piece_keys.remove(piece_key);
        self.incomplete_piece_keys.remove(piece_key);

        // TSI-2274: mark dirty instead of fsyncing on every removal.
        self.metadata_dirty = true;
        Ok(())
    }

    fn extract_info_hash<'a>(&self, piece_key: &'a str) -> &'a str {
        match piece_key.split(':').next() {
            Some(hash) if !hash.is_empty() => hash,
            _ => {
                tracing::warn!("Invalid piece_key format (missing ':'): {}", piece_key);
                "unknown"
            }
        }
    }

    /// Extract piece_index from a piece_key of the format `{info_hash}:piece:{piece_index}`.
    /// Returns the piece_index as i32, or -1 if parsing fails.
    fn extract_piece_index(&self, piece_key: &str) -> i32 {
        // Format: "{info_hash}:piece:{piece_index}"
        match piece_key.rsplit(':').next() {
            Some(idx_str) => idx_str.parse::<i32>().unwrap_or(-1),
            None => -1,
        }
    }

    pub fn piece_path(&self, piece_key: &str) -> PathBuf {
        let info_hash = self.extract_info_hash(piece_key);
        let pieces_dir = self.cache_dir.join("pieces").join(info_hash);
        pieces_dir.join(piece_key)
    }
    /// TSI-2491: path of the `.incomplete` marker for a piece.
    fn piece_marker_path(&self, piece_key: &str) -> PathBuf {
        let mut os = self.piece_path(piece_key).into_os_string();
        os.push(INCOMPLETE_SUFFIX);
        PathBuf::from(os)
    }

    pub fn ensure_piece_dir(&self, piece_key: &str) -> TorrentResult<PathBuf> {
        let info_hash = self.extract_info_hash(piece_key);
        let pieces_dir = self.cache_dir.join("pieces").join(info_hash);

        if !pieces_dir.exists() {
            fs::create_dir_all(&pieces_dir).map_err(|e| {
                TorrentError::IoError(format!("Failed to create pieces directory: {}", e))
            })?;
        }

        Ok(pieces_dir.join(piece_key))
    }

    /// Read a byte range from a cached piece file without loading the
    /// entire piece into memory.  Uses seek + read to fetch only the
    /// requested range, which avoids the ~256 KiB full-piece fs::read
    /// when the caller only needs a subset (e.g. a 128 KiB FUSE read).
    pub fn read_piece_range(
        &self,
        piece_key: &str,
        offset: u64,
        size: usize,
    ) -> TorrentResult<Vec<u8>> {
        use std::io::{Read, Seek, SeekFrom};
        let piece_path = self.piece_path(piece_key);
        let mut file = File::open(&piece_path).map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to open cached piece {} for range read: {}",
                piece_key, e
            ))
        })?;
        if offset > 0 {
            file.seek(SeekFrom::Start(offset)).map_err(|e| {
                TorrentError::IoError(format!(
                    "Failed to seek in cached piece {}: {}",
                    piece_key, e
                ))
            })?;
        }
        let mut buf = vec![0u8; size];
        file.read_exact(&mut buf).map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to read cached piece {} (wanted {} bytes at offset {}): {}",
                piece_key, size, offset, e
            ))
        })?;
        Ok(buf)
    }

    pub fn has_piece(&self, piece_key: &str) -> bool {
        self.metadata.contains_key(piece_key)
    }

    /// Get the registered size for a piece from cache metadata.
    /// Returns None if the piece is not in metadata.
    pub fn piece_metadata_size(&self, piece_key: &str) -> Option<u64> {
        self.metadata.get(piece_key).map(|m| m.size)
    }

    /// Get the registered hit count (access count) for a piece from cache metadata.
    /// Returns 0 if the piece is not in metadata.
    pub fn piece_hit_count(&self, piece_key: &str) -> u64 {
        self.metadata
            .get(piece_key)
            .map(|m| m.hit_count)
            .unwrap_or(0)
    }

    /// TSI-2048: whether a piece's metadata was registered via add_piece
    /// (verified by a successful libtorrent read_piece).  Pieces
    /// registered only by scan_pieces_subdirectory at startup are NOT
    /// verified — they could be incomplete sparse files from a crash.
    pub fn is_piece_verified(&self, piece_key: &str) -> bool {
        self.verified_piece_keys.contains(piece_key)
    }

    /// All piece keys present in cache metadata but not yet marked verified.
    /// These are candidates for background SHA-1 verification after a restart
    /// (TSI-2199): pieces discovered by `scan_pieces_subdirectory` that may be
    /// complete, or may be incomplete/corrupt files left by a crash.
    /// TSI-2491: pieces tagged with an `.incomplete` marker are excluded —
    /// the C++ write path is still filling them, so verifying them now would
    /// always fail and purge a piece that was never corrupt.
    pub fn unverified_pieces(&self) -> Vec<String> {
        self.metadata
            .keys()
            .filter(|key| {
                !self.verified_piece_keys.contains(*key)
                    && !self.incomplete_piece_keys.contains(*key)
            })
            .cloned()
            .collect()
    }
    /// Mark a piece as verified after its on-disk content passed SHA-1
    /// verification against the torrent's expected piece hash (TSI-2199).
    pub fn mark_verified(&mut self, piece_key: &str) {
        self.verified_piece_keys.insert(piece_key.to_string());
        self.incomplete_piece_keys.remove(piece_key);
        // TSI-2274: mark dirty so the periodic flush persists the
        // verified flag without fsyncing on every verification.
        self.metadata_dirty = true;
    }

    /// Delete a piece from the cache (metadata + on-disk file + verified set).
    /// Used to purge pieces that failed SHA-1 verification so they can be
    /// re-downloaded on demand (TSI-2199).
    pub fn delete_piece(&mut self, piece_key: &str) -> TorrentResult<()> {
        self.remove_piece(piece_key)
    }
    /// Remove every cached piece belonging to `info_hash`: recursively delete
    /// the `cache/pieces/<info_hash>/` directory and purge all matching
    /// metadata entries.  Used when a torrent is deleted so its pieces do not
    /// linger as orphans (TSI-2205).
    pub fn remove_infohash_pieces(&mut self, info_hash: &str) -> TorrentResult<()> {
        // Defensive guard: only ever touch a leaf directory whose name is
        // literally this info_hash.  A path with separators (e.g. `../x`)
        // would change the leaf name and is refused, so this can never escape
        // the pieces directory.
        if info_hash.is_empty() {
            tracing::warn!("Refusing to purge pieces for empty info_hash");
            return Ok(());
        }

        let pieces_dir = self.cache_dir.join("pieces").join(info_hash);
        let dir_name = pieces_dir
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("");
        if dir_name != info_hash {
            tracing::warn!(
                "Refusing to purge pieces dir {} (expected leaf name {})",
                pieces_dir.display(),
                info_hash
            );
            return Ok(());
        }

        if pieces_dir.exists() {
            if !pieces_dir.is_dir() {
                tracing::warn!(
                    "Refusing to purge non-directory pieces path {}",
                    pieces_dir.display()
                );
                return Ok(());
            }

            let removed_size = self.infohash_total_size(info_hash);
            fs::remove_dir_all(&pieces_dir).map_err(|e| {
                TorrentError::IoError(format!(
                    "Failed to remove pieces directory {}: {}",
                    pieces_dir.display(),
                    e
                ))
            })?;
            self.current_size = self.current_size.saturating_sub(removed_size);
        }

        self.remove_infohash_metadata(info_hash);
        // TSI-2274: mark dirty instead of fsyncing on every purge.
        self.metadata_dirty = true;
        Ok(())
    }

    /// Sum of registered piece sizes for a given info_hash.
    fn infohash_total_size(&self, info_hash: &str) -> u64 {
        let prefix = format!("{}:", info_hash);
        self.metadata
            .iter()
            .filter(|(key, _)| key.starts_with(&prefix))
            .map(|(_, meta)| meta.size)
            .sum()
    }

    /// Drop metadata + verified/incomplete-set entries for a given info_hash.
    fn remove_infohash_metadata(&mut self, info_hash: &str) {
        let prefix = format!("{}:", info_hash);
        self.metadata.retain(|key, _| !key.starts_with(&prefix));
        self.verified_piece_keys
            .retain(|key| !key.starts_with(&prefix));
        self.incomplete_piece_keys
            .retain(|key| !key.starts_with(&prefix));
    }

    /// Check if a piece file exists on disk, regardless of whether it is
    /// registered in the in-memory metadata. This is critical for the case
    /// where libtorrent's custom storage has written a piece to disk but the
    /// cache_metadata.txt has not been updated yet (e.g. during active download,
    /// or after a crash that left cache_metadata.txt empty).
    pub fn has_piece_on_disk(&self, piece_key: &str) -> bool {
        self.piece_path(piece_key).exists()
    }

    #[allow(dead_code)]
    pub fn current_size(&self) -> u64 {
        self.current_size
    }

    #[allow(dead_code)]
    pub fn piece_count(&self) -> usize {
        self.metadata.len()
    }

    #[allow(dead_code)]
    pub fn max_cache_size(&self) -> u64 {
        self.max_cache_size
    }

    /// Get all unique info_hashes from cache metadata.
    pub fn get_all_infohashes(&self) -> Vec<String> {
        let mut set: std::collections::HashSet<String> = std::collections::HashSet::new();
        for key in self.metadata.keys() {
            if let Some(info_hash) = key.split(':').next() {
                if !info_hash.is_empty() {
                    set.insert(info_hash.to_string());
                }
            }
        }
        let mut result: Vec<String> = set.into_iter().collect();
        result.sort();
        result
    }

    /// Get aggregated cache stats for a specific info_hash.
    pub fn get_cache_stats_by_infohash(&self, info_hash: &str) -> InfohashCacheStats {
        let mut piece_count: u64 = 0;
        let mut total_size: u64 = 0;
        let mut total_hit_count: u64 = 0;

        for (key, meta) in &self.metadata {
            if let Some(hash) = key.split(':').next() {
                if hash == info_hash {
                    piece_count += 1;
                    total_size += meta.size;
                    total_hit_count += meta.hit_count;
                }
            }
        }

        InfohashCacheStats {
            info_hash: info_hash.to_string(),
            piece_count,
            total_size,
            hit_count: total_hit_count,
        }
    }
}

/// TSI-2274: flush dirty metadata when the CacheManager is dropped, so
/// tests and other callers that do not run the engine loop still persist
/// metadata mutations.  In production the engine loop flushes every tick
/// and `main.rs` flushes on shutdown; this is the safety net for
/// short-lived instances (tests, CLI tools).
impl Drop for CacheManager {
    fn drop(&mut self) {
        if self.metadata_dirty {
            let _ = self.flush();
        }
    }
}

/// Per-info_hash aggregated cache statistics.
#[derive(Debug, Clone)]
pub struct InfohashCacheStats {
    pub info_hash: String,
    pub piece_count: u64,
    pub total_size: u64,
    pub hit_count: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_cache_manager_basic() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert_eq!(cache.piece_count(), 0);

        let test_key = "abc123:piece:0";
        let piece_path = cache.ensure_piece_dir(test_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(test_key, 100)?;

        assert!(cache.has_piece(test_key));
        assert_eq!(cache.piece_count(), 1);

        Ok(())
    }

    #[test]
    fn test_lru_eviction() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;

        let piece_1_key = "hash1:piece:0";
        let piece_1_path = cache.ensure_piece_dir(piece_1_key)?;
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece(piece_1_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_2_key = "hash1:piece:1";
        let piece_2_path = cache.ensure_piece_dir(piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece(piece_2_key, 100)?;

        assert!(cache.has_piece(piece_1_key));
        assert!(cache.has_piece(piece_2_key));
        assert_eq!(cache.current_size(), 200);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_3_key = "hash1:piece:2";
        let piece_3_path = cache.ensure_piece_dir(piece_3_key)?;
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece(piece_3_key, 100)?;

        assert!(
            !cache.has_piece(piece_1_key),
            "piece_1 should be evicted (oldest)"
        );
        assert!(cache.has_piece(piece_2_key), "piece_2 should remain");
        assert!(cache.has_piece(piece_3_key), "piece_3 should remain");
        assert_eq!(cache.current_size(), 200);

        Ok(())
    }

    #[test]
    fn test_persistence_across_restart() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_key = "def456:piece:0";
            let piece_path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 50])?;
            cache.add_piece(piece_key, 50)?;
        }

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(cache.has_piece("def456:piece:0"));

        Ok(())
    }

    #[test]
    fn test_unverified_pieces_mark_verified_and_delete() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        // Simulate a piece file left on disk from a previous run (no add_piece,
        // so it is discovered by scan_pieces_subdirectory on restart).
        let piece_key = "cafe1234:piece:3";
        {
            let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&piece_path, vec![0xABu8; 100])?;
        }

        // Restart: the scanned piece is registered but NOT verified.
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(cache.has_piece(piece_key));
        assert!(!cache.is_piece_verified(piece_key));
        assert_eq!(cache.unverified_pieces(), vec![piece_key.to_string()]);

        // Mark verified after a (hypothetical) successful SHA-1 check.
        cache.mark_verified(piece_key);
        assert!(cache.is_piece_verified(piece_key));
        assert!(cache.unverified_pieces().is_empty());

        // A corrupted piece is purged: metadata, file and verified flag all go.
        cache.delete_piece(piece_key)?;
        assert!(!cache.has_piece(piece_key));
        assert!(!cache.has_piece_on_disk(piece_key));
        assert!(!cache.is_piece_verified(piece_key));

        Ok(())
    }

    #[test]
    fn test_delete_piece_removes_file_and_metadata() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "beef5678:piece:1";
        let piece_path = cache.ensure_piece_dir(piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 10])?;
        cache.add_piece(piece_key, 10)?;
        assert!(cache.is_piece_verified(piece_key));

        cache.delete_piece(piece_key)?;

        assert!(!cache.has_piece(piece_key));
        assert!(!piece_path.exists());
        assert!(!cache.is_piece_verified(piece_key));

        Ok(())
    }

    #[test]
    fn test_remove_infohash_pieces_purges_dir_and_metadata() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let key_a = "aaaa1111:piece:0";
        let key_b = "bbbb2222:piece:0";
        let path_a = cache.ensure_piece_dir(key_a)?;
        let path_b = cache.ensure_piece_dir(key_b)?;
        std::fs::write(&path_a, vec![0u8; 100])?;
        std::fs::write(&path_b, vec![0u8; 50])?;
        cache.add_piece(key_a, 100)?;
        cache.add_piece(key_b, 50)?;
        assert_eq!(cache.current_size(), 150);

        cache.remove_infohash_pieces("aaaa1111")?;

        assert!(!cache.has_piece(key_a));
        assert!(!path_a.exists());
        assert!(!path_a.parent().unwrap().exists());
        assert!(!cache.is_piece_verified(key_a));

        // A different info_hash must be left untouched.
        assert!(cache.has_piece(key_b));
        assert!(path_b.exists());
        assert!(cache.is_piece_verified(key_b));
        assert_eq!(cache.current_size(), 50);

        Ok(())
    }

    #[test]
    fn test_remove_infohash_pieces_idempotent_and_refuses_escape() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        // No pieces for this hash: a no-op, not an error.
        cache.remove_infohash_pieces("missing_hash")?;

        // A path with separators must be refused and never resolved against a
        // parent directory.
        let outside = temp_dir.path().join("outside.txt");
        std::fs::write(&outside, b"keep me")?;

        cache.remove_infohash_pieces("../outside.txt")?;

        assert!(outside.exists());
        assert!(!temp_dir.path().join("outside").exists());

        Ok(())
    }

    #[test]
    fn test_add_piece_marks_verified() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "deadbeef:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 20])?;
        cache.add_piece(piece_key, 20)?;

        assert!(cache.is_piece_verified(piece_key));
        assert!(cache.unverified_pieces().is_empty());

        Ok(())
    }

    #[test]
    fn test_record_access_updates_lru() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;

        let piece_1_key = "hash2:piece:0";
        let piece_1_path = cache.ensure_piece_dir(piece_1_key)?;
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece(piece_1_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_2_key = "hash2:piece:1";
        let piece_2_path = cache.ensure_piece_dir(piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece(piece_2_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.record_access(piece_1_key)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_3_key = "hash2:piece:2";
        let piece_3_path = cache.ensure_piece_dir(piece_3_key)?;
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece(piece_3_key, 100)?;

        assert!(
            cache.has_piece(piece_1_key),
            "piece_1 should remain (accessed recently)"
        );
        assert!(
            !cache.has_piece(piece_2_key),
            "piece_2 should be evicted (oldest after piece_1 access)"
        );
        assert!(cache.has_piece(piece_3_key), "piece_3 should remain");

        Ok(())
    }

    #[test]
    fn test_piece_directory_structure() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let info_hash = "abc123def456";
        let piece_key = format!("{}:piece:0", info_hash);

        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(&piece_key, 100)?;

        let expected_dir = temp_dir.path().join("pieces").join(info_hash);
        assert!(
            expected_dir.exists(),
            "pieces/<info_hash> directory should exist"
        );

        let expected_file = expected_dir.join(&piece_key);
        assert!(
            expected_file.exists(),
            "piece file should be in pieces/<info_hash>/ directory"
        );

        Ok(())
    }

    #[test]
    fn test_multiple_torrents_separate_directories() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let hash1 = "torrent_hash_1";
        let hash2 = "torrent_hash_2";

        let piece1_key = format!("{}:piece:0", hash1);
        let piece1_path = cache.ensure_piece_dir(&piece1_key)?;
        std::fs::write(&piece1_path, vec![0u8; 100])?;
        cache.add_piece(&piece1_key, 100)?;

        let piece2_key = format!("{}:piece:0", hash2);
        let piece2_path = cache.ensure_piece_dir(&piece2_key)?;
        std::fs::write(&piece2_path, vec![0u8; 100])?;
        cache.add_piece(&piece2_key, 100)?;

        let dir1 = temp_dir.path().join("pieces").join(hash1);
        let dir2 = temp_dir.path().join("pieces").join(hash2);

        assert!(dir1.exists(), "directory for torrent 1 should exist");
        assert!(dir2.exists(), "directory for torrent 2 should exist");
        assert!(dir1.join(&piece1_key).exists());
        assert!(dir2.join(&piece2_key).exists());

        Ok(())
    }

    #[test]
    fn test_backward_compatibility_migration() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "oldhash123";
        let piece_key = format!("{}:piece:0", info_hash);
        let old_path = temp_dir.path().join(&piece_key);
        std::fs::write(&old_path, vec![0u8; 100])?;

        assert!(old_path.exists(), "old file should exist before migration");

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(!old_path.exists(), "old file should be migrated");

        let new_path = temp_dir
            .path()
            .join("pieces")
            .join(info_hash)
            .join(&piece_key);
        assert!(new_path.exists(), "file should exist in new location");

        assert!(
            cache.has_piece(&piece_key),
            "migrated piece should be in metadata"
        );

        Ok(())
    }

    // ── Cache consistency: partial write / metadata-disk mismatch ──────
    //
    // These tests cover the scenario where cache metadata (has_piece) and
    // the on-disk piece file are out of sync — a key root-cause suspect
    // for the premature-EOF bug (TSI-2018).  When metadata says a piece
    // exists but the file is empty or truncated, the assembly loop in
    // read_file_range reads empty/short data and silently skips the piece.

    #[test]
    fn test_has_piece_true_but_disk_file_empty() -> TorrentResult<()> {
        // Regression: metadata registered via add_piece() but the disk
        // file was never actually written (or was truncated to 0 bytes).
        // has_piece() should still return true (metadata exists), but the
        // disk file size does not match — caller must validate size.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "hash_empty:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;

        // Register metadata with non-zero size…
        cache.add_piece(piece_key, 4096)?;
        // …but write only 0 bytes to disk (simulates partial write /
        // crash before fsync).
        std::fs::write(&piece_path, &[])?;

        // Metadata exists — has_piece is true.
        assert!(cache.has_piece(piece_key));

        // But the file on disk is empty, so reading it would yield
        // empty data.  This is the inconsistency that causes the
        // assembly loop to skip this piece.
        let disk_data = std::fs::read(&piece_path)?;
        assert!(
            disk_data.is_empty(),
            "disk file is empty despite metadata claiming 4096 bytes"
        );
        assert!(
            cache.has_piece_on_disk(piece_key),
            "has_piece_on_disk detects the file existence, not its size"
        );

        Ok(())
    }

    #[test]
    fn test_has_piece_on_disk_partial_file() -> TorrentResult<()> {
        // has_piece_on_disk returns true purely based on file existence;
        // it does not validate the file content size.  A partially written
        // file (e.g. libtorrent wrote 32 KiB of a 256 KiB piece before
        // a crash) would pass the check but yield short data.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "hash_partial:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;

        // Write 32 KiB of a "256 KiB" piece — partial write scenario.
        let partial_data = vec![0xABu8; 32768];
        std::fs::write(&piece_path, &partial_data)?;

        // has_piece_on_disk returns true — file exists.
        assert!(cache.has_piece_on_disk(piece_key));
        // has_piece returns false — metadata was never registered.
        assert!(!cache.has_piece(piece_key));

        // When add_piece is called later with the partial file size,
        // metadata will match the partial file — but the true piece
        // length might be much larger.
        cache.add_piece(piece_key, partial_data.len() as u64)?;
        assert!(cache.has_piece(piece_key));
        assert_eq!(
            std::fs::metadata(&piece_path)?.len(),
            partial_data.len() as u64
        );

        Ok(())
    }

    #[test]
    fn test_metadata_size_mismatch_disk_size() -> TorrentResult<()> {
        // Metadata registered with a large size but file on disk is
        // smaller — simulates metadata persisted before the write
        // completed.  The assembly loop trusts piece_data.len(), which
        // comes from the disk file, so a shorter file means shorter
        // read results.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "hash_mismatch:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;

        let real_data = vec![0xCDu8; 1024]; // 1 KiB
        let claimed_size: u64 = 262144; // 256 KiB
        std::fs::write(&piece_path, &real_data)?;
        cache.add_piece(piece_key, claimed_size)?;

        assert!(cache.has_piece(piece_key));
        let disk_size = std::fs::metadata(&piece_path)?.len();
        assert_ne!(
            disk_size, claimed_size,
            "disk file size ({}) != metadata claimed size ({})",
            disk_size, claimed_size
        );

        // Reading from disk would yield only 1024 bytes, not 262144.
        let disk_data = std::fs::read(&piece_path)?;
        assert_eq!(disk_data.len(), 1024);

        Ok(())
    }

    #[test]
    fn test_empty_piece_file_on_disk_detected_as_existing() -> TorrentResult<()> {
        // An empty file on disk passes has_piece_on_disk, but the
        // assembly loop would read 0 bytes and skip the piece entirely.
        let temp_dir = TempDir::new().unwrap();
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "hash_empty_disk:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;
        std::fs::write(&piece_path, &[])?;
        // Don't register in metadata — simulate pure disk-only scenario.
        assert!(
            cache.has_piece_on_disk(piece_key),
            "empty file on disk is still detected as existing"
        );
        assert!(!cache.has_piece(piece_key), "no metadata registered");

        // TSI-2048: the fast-path no longer consults has_piece_on_disk;
        // it only trusts metadata (has_piece) with size validation.
        // An empty file on disk without metadata registration will fall
        // through to read_piece (the authoritative source).
        let disk_data = std::fs::read(&piece_path)?;
        assert!(disk_data.is_empty());

        Ok(())
    }

    #[test]
    fn test_orphaned_disk_file_no_metadata() -> TorrentResult<()> {
        // A piece file exists on disk (possibly left by a previous
        // session) but cache_metadata.txt has no entry for it.
        // After rebuild_index, the file should be discovered and added
        // to metadata automatically.
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "orphaned_hash";
        let piece_key = format!("{}:piece:0", info_hash);

        // Create the file manually without going through CacheManager
        let pieces_dir = temp_dir.path().join("pieces").join(info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        let piece_path = pieces_dir.join(&piece_key);
        std::fs::write(&piece_path, vec![0xEFu8; 500])?;

        // Now create CacheManager — rebuild_index should discover it
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(
            cache.has_piece(&piece_key),
            "orphaned disk file should be discovered during rebuild_index"
        );

        Ok(())
    }

    // ── TSI-2048 regression: restart with partial piece files ─────────
    //
    // These tests verify that `scan_pieces_subdirectory`'s blind
    // registration of every on-disk file does not make incomplete
    // pieces appear trustworthy.  The fast-path must use
    // `piece_metadata_size` to validate the registered size against
    // the expected piece length.

    #[test]
    fn test_partial_piece_registered_by_scan_has_wrong_size() -> TorrentResult<()> {
        // Simulates a crash-restart scenario: libtorrent custom storage
        // wrote only 32 KiB of a 256 KiB piece before the crash.
        // scan_pieces_subdirectory registers it at the wrong size, which
        // piece_metadata_size exposes.
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "tsi2048_partial_hash";
        let piece_key = format!("{}:piece:0", info_hash);

        let pieces_dir = temp_dir.path().join("pieces").join(info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        let piece_path = pieces_dir.join(&piece_key);

        // Write only 32 KiB — partial piece after crash.
        let partial_data = vec![0xABu8; 32768];
        std::fs::write(&piece_path, &partial_data)?;

        // Create CacheManager — scan registers the file.
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        // has_piece returns true — the file was blindly registered.
        assert!(
            cache.has_piece(&piece_key),
            "partial piece file is registered in metadata by scan"
        );

        // But the registered size is only 32 KiB, not 256 KiB.
        let size = cache
            .piece_metadata_size(&piece_key)
            .expect("piece should have metadata");
        assert_eq!(
            size, 32768,
            "registered size should match the partial file, not the expected piece length"
        );

        // The fast-path's size validation would reject this piece
        // because 32768 < 262144 (piece_length).
        assert!(
            size < 262144,
            "partial piece size is smaller than expected piece length"
        );

        Ok(())
    }

    #[test]
    fn test_complete_piece_registered_by_scan_has_correct_size() -> TorrentResult<()> {
        // A complete piece file (256 KiB) registered by scan has the
        // right size — BUT it is NOT verified.  The fast-path rejects
        // unverified pieces even with correct size, because a sparse
        // file can have the same logical size with zero-filled holes.
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "tsi2048_complete_hash";
        let piece_key = format!("{}:piece:0", info_hash);

        let pieces_dir = temp_dir.path().join("pieces").join(info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        let piece_path = pieces_dir.join(&piece_key);

        let complete_data = vec![0xCDu8; 262144]; // 256 KiB
        std::fs::write(&piece_path, &complete_data)?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(cache.has_piece(&piece_key));

        let size = cache
            .piece_metadata_size(&piece_key)
            .expect("piece should have metadata");
        assert_eq!(
            size, 262144,
            "complete piece registered by scan should have the correct size"
        );

        // TSI-2048: scanned pieces are NOT verified — only add_piece
        // (after a successful read_piece) marks them as verified.
        assert!(
            !cache.is_piece_verified(&piece_key),
            "scanned piece should NOT be verified even with correct size"
        );

        Ok(())
    }

    #[test]
    fn test_piece_verified_after_add_piece() -> TorrentResult<()> {
        // A piece registered via add_piece (simulating a successful
        // read_piece) should be marked as verified.  The fast-path
        // trusts only verified pieces.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "tsi2048_addpiece:piece:0";
        let _piece_path = cache.ensure_piece_dir(piece_key)?;

        // Before add_piece: no metadata, not verified.
        assert!(!cache.has_piece(piece_key));
        assert!(!cache.is_piece_verified(piece_key));

        // add_piece (called after read_piece succeeds).
        cache.add_piece(piece_key, 262144)?;

        // After add_piece: has metadata AND verified.
        assert!(cache.has_piece(piece_key));
        assert!(
            cache.is_piece_verified(piece_key),
            "piece registered via add_piece should be verified"
        );
        assert_eq!(cache.piece_metadata_size(piece_key), Some(262144));

        Ok(())
    }

    #[test]
    fn test_add_piece_re_registration_no_double_count() -> TorrentResult<()> {
        // TSI-2231: after restart scan_pieces_subdirectory already
        // counted an on-disk piece into current_size.  When the engine
        // poll loop re-registers the same piece via add_piece (have ==
        // true), current_size must not re-add the full size.
        let temp_dir = TempDir::new().unwrap();

        let piece_key = "tsi2231_redup:piece:0";

        // Phase 1: write the piece file and register it so it lands on
        // disk + in the metadata file, then drop the manager.
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 512])?;
            cache.add_piece(piece_key, 512)?;
        }

        // Phase 2: restart.  scan_pieces_subdirectory re-counts the
        // on-disk piece into current_size.
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert_eq!(cache.current_size(), 512);

        // Phase 3: engine poll loop calls add_piece again for the same
        // have==true piece.  current_size must stay at 512, not 1024.
        cache.add_piece(piece_key, 512)?;
        assert_eq!(
            cache.current_size(),
            512,
            "re-registering an existing piece must not double-count"
        );

        Ok(())
    }

    // ── TSI-2242 regression: current_size add/remove symmetry ─────────
    //
    // remove_piece must debit the same value that add_piece /
    // scan_pieces_subdirectory credited (PieceMetadata.size), not the
    // on-disk file length.  When a piece's disk file is shorter than its
    // registered size (partial write / crash-restart), debiting the disk
    // length left current_size permanently inflated and caused
    // over-eviction.

    #[test]
    fn test_remove_piece_debits_registered_size_not_disk_size() -> TorrentResult<()> {
        // add_piece credits the registered size (4096); the disk file is
        // empty (0 bytes).  remove_piece must debit 4096 so current_size
        // returns to zero, not 0.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "tsi2242_short:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;

        // Register a 4096-byte piece…
        cache.add_piece(piece_key, 4096)?;
        assert_eq!(cache.current_size(), 4096);

        // …but the disk file is empty (crash before fsync).
        std::fs::write(&piece_path, &[])?;
        assert_eq!(std::fs::metadata(&piece_path)?.len(), 0);

        cache.delete_piece(piece_key)?;

        assert_eq!(
            cache.current_size(),
            0,
            "current_size must return to 0 after removing a piece whose disk file was shorter than its registered size"
        );
        assert!(!cache.has_piece(piece_key));

        Ok(())
    }

    #[test]
    fn test_evict_debits_registered_size_on_partial_file() -> TorrentResult<()> {
        // LRU eviction goes through remove_piece.  A piece whose disk
        // file is shorter than its registered size must not leave
        // residual current_size after eviction.
        let temp_dir = TempDir::new().unwrap();
        // max_cache_size just above one piece so the second add evicts the first.
        let mut cache = CacheManager::new(temp_dir.path(), 4096)?;

        let piece_key = "tsi2242_evict:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;

        cache.add_piece(piece_key, 4096)?;
        assert_eq!(cache.current_size(), 4096);

        // Disk file is shorter than registered size.
        std::fs::write(&piece_path, vec![0u8; 100])?;
        assert_eq!(std::fs::metadata(&piece_path)?.len(), 100);

        // Sleep so piece_2 gets a strictly newer last_accessed, making piece_1
        // the unambiguous LRU victim.
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Adding a second piece triggers eviction of the first.
        let piece_2_key = "tsi2242_evict:piece:1";
        let piece_2_path = cache.ensure_piece_dir(piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 4096])?;
        cache.add_piece(piece_2_key, 4096)?;

        assert!(!cache.has_piece(piece_key), "first piece should be evicted");
        assert_eq!(
            cache.current_size(),
            4096,
            "after evicting the partial piece, current_size should reflect only the surviving piece"
        );

        Ok(())
    }

    // ── TSI-2263: cache flush durability ──────────────────────────────

    #[test]
    fn test_flush_persists_metadata_for_restart() -> TorrentResult<()> {
        // flush() must persist the current metadata to disk so that a
        // restart discovers all pieces at their registered sizes.  This
        // is the explicit shutdown flush that prevents the "cache piece
        // cleaned after restart" bug.
        let temp_dir = TempDir::new().unwrap();

        let piece_key = "tsi2263_flush:piece:0";
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 256])?;
            cache.add_piece(piece_key, 256)?;
            assert!(cache.is_piece_verified(piece_key));

            // Explicit flush — simulates the shutdown path.
            cache.flush()?;
        }

        // Restart: metadata must reflect the flushed state.
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(cache.has_piece(piece_key));
        assert_eq!(cache.piece_metadata_size(piece_key), Some(256));

        Ok(())
    }

    #[test]
    fn test_flush_preserves_multiple_pieces_across_restart() -> TorrentResult<()> {
        // Multiple pieces from different torrents, each flushed before
        // shutdown, must all survive a restart at their correct sizes.
        let temp_dir = TempDir::new().unwrap();

        let key_a = "tsi2263_hash_a:piece:0";
        let key_b = "tsi2263_hash_b:piece:1";
        let key_c = "tsi2263_hash_a:piece:2";
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            for (key, sz) in [(key_a, 128u64), (key_b, 512u64), (key_c, 256u64)] {
                let path = cache.ensure_piece_dir(key)?;
                std::fs::write(&path, vec![0u8; sz as usize])?;
                cache.add_piece(key, sz)?;
            }
            cache.flush()?;
        }

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert_eq!(cache.piece_metadata_size(key_a), Some(128));
        assert_eq!(cache.piece_metadata_size(key_b), Some(512));
        assert_eq!(cache.piece_metadata_size(key_c), Some(256));
        assert_eq!(cache.piece_count(), 3);
        assert_eq!(cache.current_size(), 896);

        Ok(())
    }

    // ── TSI-2274: record_access must not fsync on every read ───────────

    #[test]
    fn test_record_access_does_not_persist_until_flush() -> TorrentResult<()> {
        // record_access updates the in-memory hit_count + last_accessed
        // but must NOT fsync the metadata file on every call (the old
        // behaviour limited read throughput to ~1.2 MB/s).  The metadata
        // file is only persisted on an explicit flush or Drop.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = "tsi2274:piece:0";
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&path, vec![0u8; 100])?;
            cache.add_piece(piece_key, 100)?;
            // flush so the initial add_piece state is on disk.
            cache.flush()?;

            // Record 50 accesses — none should touch the metadata file.
            let meta_path = temp_dir.path().join("cache_metadata.txt");
            let mtime_before = std::fs::metadata(&meta_path)?.modified()?;
            for _ in 0..50 {
                cache.record_access(piece_key)?;
            }
            let mtime_after = std::fs::metadata(&meta_path)?.modified()?;
            assert_eq!(
                mtime_before, mtime_after,
                "metadata file must not be touched by record_access (no fsync per read)"
            );
            // In-memory hit_count is still updated.
            assert_eq!(cache.piece_hit_count(piece_key), 50);
        }
        Ok(())
    }

    #[test]
    fn test_drop_flushes_dirty_metadata() -> TorrentResult<()> {
        // CacheManager::drop flushes dirty metadata so tests and other
        // short-lived callers persist mutations without an explicit flush.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = "tsi2274_drop:piece:0";
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let path = cache.ensure_piece_dir(piece_key)?;
            std::fs::write(&path, vec![0u8; 64])?;
            cache.add_piece(piece_key, 64)?;
            // No explicit flush — drop must persist.
        }
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(
            cache.has_piece(piece_key),
            "drop must have flushed dirty metadata so restart sees the piece"
        );
        assert_eq!(cache.piece_metadata_size(piece_key), Some(64));
        Ok(())
    }

    // ── TSI-2491 regression: incomplete-marker exclusion ─────────────
    //
    // The C++ write path drops `<piece_key>.incomplete` next to the piece
    // file while blocks are still being written.  On restart, those pieces
    // must stay in metadata (so `has_piece`/size stay consistent) but must
    // NOT be candidates for background SHA-1 verification — verifying a
    // half-written piece always fails and wrongly purges it.

    #[test]
    fn test_incomplete_marker_excludes_piece_from_verification() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "tsi2491_incomplete";
        let piece_key = format!("{}:piece:0", info_hash);

        let pieces_dir = temp_dir.path().join("pieces").join(info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        let piece_path = pieces_dir.join(&piece_key);
        let marker_path = pieces_dir.join(format!("{}.incomplete", piece_key));

        // Partial piece file + marker, exactly as the C++ write path leaves it.
        std::fs::write(&piece_path, vec![0xABu8; 32768])?;
        std::fs::write(&marker_path, b"")?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        // Still registered so has_piece / size stay consistent.
        assert!(cache.has_piece(&piece_key));
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(32768));

        // Marker file is not itself registered as a piece.
        assert_eq!(cache.piece_count(), 1);

        // Excluded from verification candidates — this is the fix.
        assert!(
            cache.unverified_pieces().is_empty(),
            "incomplete-marked piece must not be a verification candidate"
        );

        Ok(())
    }

    #[test]
    fn test_add_piece_clears_incomplete_marker() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "tsi2491_addpiece:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;
        let marker_path = piece_path.with_extension("incomplete");
        std::fs::write(&piece_path, vec![0xCDu8; 262144])?;
        std::fs::write(&marker_path, b"")?;

        // add_piece is the authoritative complete-piece registration point.
        cache.add_piece(piece_key, 262144)?;

        assert!(!marker_path.exists(), "marker must be cleared by add_piece");
        assert!(cache.is_piece_verified(piece_key));
        Ok(())
    }

    #[test]
    fn test_delete_piece_clears_incomplete_marker() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = "tsi2491_delete:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key)?;
        let marker_path = piece_path.with_extension("incomplete");
        std::fs::write(&piece_path, vec![0xEFu8; 64])?;
        std::fs::write(&marker_path, b"")?;

        cache.add_piece(piece_key, 64)?;
        cache.delete_piece(piece_key)?;

        assert!(
            !marker_path.exists(),
            "marker must be removed with the piece"
        );
        assert!(!cache.has_piece(piece_key));
        Ok(())
    }

    #[test]
    fn test_unmarked_scan_piece_still_verification_candidate() -> TorrentResult<()> {
        // Regression guard for the pre-existing behavior: a scanned piece
        // WITHOUT a marker remains an unverified verification candidate.
        let temp_dir = TempDir::new().unwrap();

        let info_hash = "tsi2491_unmarked";
        let piece_key = format!("{}:piece:0", info_hash);

        let pieces_dir = temp_dir.path().join("pieces").join(info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        std::fs::write(pieces_dir.join(&piece_key), vec![0x11u8; 100])?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert_eq!(
            cache.unverified_pieces(),
            vec![piece_key.to_string()],
            "unmarked scanned piece must still be verified"
        );
        Ok(())
    }

    #[test]
    fn test_flush_metadata_if_dirty_skips_clean() -> TorrentResult<()> {
        // flush_metadata_if_dirty is a no-op when nothing changed since the
        // last flush — the metadata file must not be touched.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = "tsi2274_clean:piece:0";
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        let path = cache.ensure_piece_dir(piece_key)?;
        std::fs::write(&path, vec![0u8; 32])?;
        cache.add_piece(piece_key, 32)?;
        cache.flush()?;

        let meta_path = temp_dir.path().join("cache_metadata.txt");
        let mtime_before = std::fs::metadata(&meta_path)?.modified()?;
        cache.flush_metadata_if_dirty()?;
        let mtime_after = std::fs::metadata(&meta_path)?.modified()?;
        assert_eq!(
            mtime_before, mtime_after,
            "clean metadata must not be re-written"
        );
        Ok(())
    }
}

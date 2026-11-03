use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::error::{TorrentError, TorrentResult};

const CACHE_METADATA_FILE: &str = "cache_metadata.txt";
/// Scratch names for the atomic metadata rewrite.  Each write uses its own
/// name (`cache_metadata.txt.<pid>.<seq>.tmp`) so two writers never share a
/// scratch file: the periodic flush runs outside the cache lock and can overlap
/// an abandoned engine thread's final write at shutdown.
const CACHE_METADATA_SCRATCH_PREFIX: &str = "cache_metadata.txt.";
const CACHE_METADATA_SCRATCH_SUFFIX: &str = ".tmp";
const INCOMPLETE_SUFFIX: &str = ".incomplete";
/// Separator between the info hash and the piece index of a cache key.
const PIECE_KEY_SEPARATOR: &str = ":piece:";
/// Hex length of a v1 (SHA-1) info hash.
const V1_INFO_HASH_HEX_LEN: usize = 40;
/// Hex length of a v2 (SHA-256) info hash.
const V2_INFO_HASH_HEX_LEN: usize = 64;

/// Whether `name` is a scratch file of the metadata rewrite, including one a
/// crash left behind.
fn is_metadata_scratch_name(name: &str) -> bool {
    name.starts_with(CACHE_METADATA_SCRATCH_PREFIX) && name.ends_with(CACHE_METADATA_SCRATCH_SUFFIX)
}

/// A scratch name no other write can pick: the pid attributes it to this
/// process, so another process's startup cleanup can tell an in-flight write
/// from a crash leftover, and the counter separates writers inside one
/// process.
fn scratch_file_name() -> String {
    static SCRATCH_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let seq = SCRATCH_SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!(
        "{}{}.{}{}",
        CACHE_METADATA_SCRATCH_PREFIX,
        std::process::id(),
        seq,
        CACHE_METADATA_SCRATCH_SUFFIX
    )
}

/// The pid a metadata scratch name was written by, or `None` when the name
/// carries no pid — a file that merely matches the scratch shape and was not
/// produced by this writer.
fn scratch_owner_pid(name: &str) -> Option<u32> {
    let rest = name
        .strip_prefix(CACHE_METADATA_SCRATCH_PREFIX)?
        .strip_suffix(CACHE_METADATA_SCRATCH_SUFFIX)?;
    let (pid, _seq) = rest.split_once('.')?;
    pid.parse::<u32>().ok().filter(|pid| *pid > 0)
}

/// Whether the process that wrote a scratch file is still running.
///
/// `kill(pid, 0)` performs the existence and permission checks without
/// delivering a signal: `ESRCH` means no such process, `EPERM` means one exists
/// under another uid.  Pid reuse under a container runtime makes this a
/// heuristic — a recycled pid keeps the earlier process's leftover around,
/// which costs one stray file (the piece scan skips it) and never a rename
/// failure.
#[cfg(unix)]
fn process_is_alive(pid: u32) -> bool {
    if pid == std::process::id() {
        return true;
    }
    let Ok(pid) = libc::pid_t::try_from(pid) else {
        return true;
    };
    // SAFETY: signal 0 only runs the existence and permission checks; nothing
    // is delivered and no pointer is dereferenced.
    if unsafe { libc::kill(pid, 0) } == 0 {
        return true;
    }
    std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

#[cfg(not(unix))]
fn process_is_alive(_pid: u32) -> bool {
    true
}

/// Whether `piece_key` has exactly the shape the write side produces:
/// `{info_hash}:piece:{index}`, 40 or 64 lowercase hex digits followed by a
/// decimal index.
///
/// The cache turns keys into paths, so this whitelist is what keeps a key
/// from naming anything but a leaf below `pieces/<info_hash>/` (C-5.4.6): the
/// info-hash alphabet has no separators and no `.`, so a key can never spell
/// an absolute path, a parent step or an empty leaf.
fn is_valid_piece_key(piece_key: &str) -> bool {
    let Some((info_hash, index)) = piece_key.split_once(PIECE_KEY_SEPARATOR) else {
        return false;
    };
    let hash_len_ok =
        info_hash.len() == V1_INFO_HASH_HEX_LEN || info_hash.len() == V2_INFO_HASH_HEX_LEN;
    hash_len_ok
        && info_hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        && !index.is_empty()
        && index.bytes().all(|b| b.is_ascii_digit())
        // The write side indexes pieces with an `int`; a key outside that
        // range cannot name a piece.
        && index.parse::<i32>().is_ok()
}

/// The info hash of a key that already passed [`is_valid_piece_key`].
fn piece_key_info_hash(piece_key: &str) -> &str {
    piece_key
        .split_once(PIECE_KEY_SEPARATOR)
        .map(|(info_hash, _)| info_hash)
        .unwrap_or_default()
}

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
    /// Cumulative number of pieces evicted by `evict_lru`.
    pub eviction_count: u64,
    evict_callbacks: Vec<Box<dyn Fn(String, i32) + Send + Sync>>,
    /// pieces with an on-disk `.incomplete` marker — written by the
    /// C++ `PieceStorage::write_piece` while a piece is still being filled.
    /// Rebuilt by `scan_pieces_subdirectory` on restart and cleared by
    /// `add_piece` / `remove_piece` / `mark_verified`.
    incomplete_piece_keys: HashSet<String>,
    /// pieces whose integrity was verified via add_piece
    /// (called after a successful libtorrent read_piece).  Pieces
    /// registered solely by scan_pieces_subdirectory at startup are
    /// NOT in this set — they may be incomplete sparse files.  The set is
    /// in-memory only: C-5.3.3 keeps the verified set out of the metadata
    /// file, so every restart re-verifies the scanned pieces.
    verified_piece_keys: HashSet<String>,
    /// Access order of `metadata`, keyed by `(last_accessed, piece_key)`.
    /// Eviction takes its victim from the front of this set instead of
    /// scanning every piece on each eviction.
    lru: BTreeSet<(u64, String)>,
    /// Serializes metadata writes for this cache directory and remembers which
    /// snapshot generation reached the disk.
    writer: std::sync::Arc<MetadataWriter>,
    /// Generation of the last snapshot taken; bumped under the cache lock so a
    /// later generation always describes an at-least-as-new map.
    snapshot_generation: u64,
    /// Metadata changed since the last write to disk.  Mutating methods
    /// (`record_access`, `add_piece`, `remove_piece`, `drop_infohash_state`)
    /// set this instead of writing on every call (C-5.2.1); the engine loop
    /// takes a snapshot with `take_dirty_snapshot` and writes it outside the
    /// cache lock, and `flush` writes it on shutdown.
    metadata_dirty: bool,
}

fn current_timestamp_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// Bounded retry budget for removing a pieces directory tree that a
/// concurrent writer may still be filling.
pub(crate) const PURGE_REMOVE_MAX_ATTEMPTS: u32 = 6;
pub(crate) const PURGE_REMOVE_RETRY_DELAY: Duration = Duration::from_millis(50);

/// Remove a directory tree, tolerating a concurrent writer.
///
/// libtorrent's custom storage keeps filling piece files into
/// `cache/pieces/<info_hash>/` while a torrent is re-checked or a piece is
/// being written.  When a stale info_hash directory is purged right after a
/// restart, `fs::remove_dir_all` walks the tree and then fails with
/// `DirectoryNotEmpty` (ENOTEMPTY) because a piece file landed after the walk
/// but before the directory itself was removed.  Retry a bounded number of
/// times so a transient writer finishes before giving up.
pub(crate) fn remove_dir_all_tolerating_writers(path: &Path) -> std::io::Result<()> {
    remove_dir_all_with_retry(
        path,
        PURGE_REMOVE_MAX_ATTEMPTS,
        PURGE_REMOVE_RETRY_DELAY,
        |p| fs::remove_dir_all(p),
    )
}

/// Retry loop shared by [`remove_dir_all_tolerating_writers`].  The attempt
/// count, delay, and removal operation are parameters so tests can drive the
/// control flow deterministically (an injected `remove` that returns
/// `DirectoryNotEmpty` a few times before succeeding) instead of relying on a
/// real filesystem race.
fn remove_dir_all_with_retry(
    path: &Path,
    max_attempts: u32,
    retry_delay: Duration,
    mut remove: impl FnMut(&Path) -> std::io::Result<()>,
) -> std::io::Result<()> {
    let mut last_error = None;
    for attempt in 0..max_attempts {
        match remove(path) {
            Ok(()) => return Ok(()),
            // Already gone: the goal ("directory removed") is met.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::DirectoryNotEmpty => {
                last_error = Some(e);
                if attempt + 1 < max_attempts {
                    tracing::debug!(
                        "remove_dir_all on {} raced a writer (attempt {}/{}); retrying",
                        path.display(),
                        attempt + 1,
                        max_attempts
                    );
                    std::thread::sleep(retry_delay);
                }
            }
            Err(e) => return Err(e),
        }
    }
    Err(last_error.expect("loop iterated at least once"))
}

/// Serialized writer for one cache directory's metadata file.
///
/// The periodic flush runs outside the cache lock, so its write can overlap a
/// shutdown flush from an abandoned engine thread.  Two guards make that safe:
/// the mutex serializes the writes (each with its own scratch name), and the
/// generation gate keeps an older snapshot from overwriting a newer one — a
/// snapshot is taken under the cache lock, so its generation orders snapshots
/// exactly like the map state they copy.
pub struct MetadataWriter {
    cache_dir: PathBuf,
    published_generation: std::sync::Mutex<u64>,
}

/// Point-in-time copy of the cache metadata: `(piece_key, last_accessed, size,
/// hit_count)` per piece, the writer that owns the cache directory, and the
/// generation the copy was taken at.
pub struct CacheMetadataSnapshot {
    writer: std::sync::Arc<MetadataWriter>,
    generation: u64,
    entries: Vec<(String, u64, u64, u64)>,
}

impl CacheMetadataSnapshot {
    /// Persist the snapshot atomically: write a scratch file beside the
    /// authoritative one, fsync its contents, then `rename(2)` it over the
    /// authoritative name and fsync the directory.
    ///
    /// An in-place truncating write is observable from outside: an interrupted
    /// shutdown or a crash leaves `cache_metadata.txt` empty or cut mid-line,
    /// and the next start registers pieces at wrong sizes and the verifier
    /// purges them.  The rename is atomic within the directory, so a reader
    /// sees either the old file or the new one.
    ///
    /// Skipped when a newer snapshot was already published.  Does no
    /// cache-state access, so the caller writes without holding the cache lock
    /// (C-5.2.1).
    pub fn write(&self) -> TorrentResult<()> {
        self.writer.write(self.generation, &self.entries)
    }

    /// Number of pieces in the snapshot.
    pub fn len(&self) -> usize {
        self.entries.len()
    }
}

impl MetadataWriter {
    fn new(cache_dir: PathBuf) -> Self {
        Self {
            cache_dir,
            published_generation: std::sync::Mutex::new(0),
        }
    }

    fn write(&self, generation: u64, entries: &[(String, u64, u64, u64)]) -> TorrentResult<()> {
        let mut published = self
            .published_generation
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        // A newer snapshot already reached the disk: publishing this one would
        // roll `last_accessed` / `hit_count` back to an older map.
        if *published >= generation {
            tracing::debug!(
                "Skipping stale cache metadata snapshot (generation {} <= {})",
                generation,
                *published
            );
            return Ok(());
        }

        let metadata_path = self.cache_dir.join(CACHE_METADATA_FILE);
        let scratch_path = self.cache_dir.join(scratch_file_name());

        if let Err(error) = write_metadata_scratch(&scratch_path, entries) {
            // The scratch is worthless once the rewrite failed; do not leave a
            // stray file for the piece scan to find.
            let _ = fs::remove_file(&scratch_path);
            return Err(error);
        }
        if let Err(e) = fs::rename(&scratch_path, &metadata_path) {
            let _ = fs::remove_file(&scratch_path);
            return Err(TorrentError::IoError(format!(
                "Failed to replace metadata file: {}",
                e
            )));
        }
        // The rename has to survive a crash as well, not only the contents.
        if let Ok(dir) = File::open(&self.cache_dir) {
            let _ = dir.sync_all();
        }
        *published = generation;
        Ok(())
    }
}

/// Write `entries` into `scratch_path` and fsync its contents.
fn write_metadata_scratch(
    scratch_path: &Path,
    entries: &[(String, u64, u64, u64)],
) -> TorrentResult<()> {
    let file = File::create(scratch_path).map_err(|e| {
        TorrentError::IoError(format!("Failed to create metadata scratch file: {}", e))
    })?;
    let mut writer = BufWriter::new(file);

    for (piece_key, last_accessed, size, hit_count) in entries {
        writeln!(
            writer,
            "{}\t{}\t{}\t{}",
            piece_key, last_accessed, size, hit_count
        )
        .map_err(|e| TorrentError::IoError(format!("Failed to write metadata: {}", e)))?;
    }

    // Flush the BufWriter into the OS page cache, then fsync the descriptor
    // so the contents reach disk before the rename publishes them.
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

impl CacheManager {
    pub fn new(cache_dir: &Path, max_cache_size: u64) -> TorrentResult<Self> {
        let cache_dir = cache_dir.to_path_buf();
        if !cache_dir.exists() {
            fs::create_dir_all(&cache_dir).map_err(|e| {
                TorrentError::IoError(format!("Failed to create cache directory: {}", e))
            })?;
        }
        let mut manager = CacheManager {
            writer: std::sync::Arc::new(MetadataWriter::new(cache_dir.clone())),
            cache_dir,
            metadata: HashMap::new(),
            max_cache_size,
            current_size: 0,
            miss_count: 0,
            hit_count: 0,
            eviction_count: 0,
            evict_callbacks: Vec::new(),
            incomplete_piece_keys: HashSet::new(),
            verified_piece_keys: HashSet::new(),
            lru: BTreeSet::new(),
            snapshot_generation: 0,
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

    /// Copy the metadata map and tag it with the next generation.  Every call
    /// runs under the cache lock, so a later generation always describes a map
    /// at least as new as an earlier one.
    fn next_snapshot(&mut self) -> CacheMetadataSnapshot {
        self.snapshot_generation += 1;
        CacheMetadataSnapshot {
            writer: std::sync::Arc::clone(&self.writer),
            generation: self.snapshot_generation,
            entries: self
                .metadata
                .iter()
                .map(|(key, meta)| (key.clone(), meta.last_accessed, meta.size, meta.hit_count))
                .collect(),
        }
    }

    /// Copy a dirty metadata map and clear the dirty flag.  Called under the
    /// cache lock; the serialization and fsync run outside it
    /// ([`CacheMetadataSnapshot::write`]), so a periodic flush never stalls
    /// readers (C-5.2.1).  Returns `None` when nothing changed since the last
    /// write.
    ///
    /// Mutations that land after this call re-dirty the map and are picked up
    /// by the next snapshot; a failed write is undone with
    /// [`Self::mark_metadata_dirty`].
    pub fn take_dirty_snapshot(&mut self) -> Option<CacheMetadataSnapshot> {
        if !self.metadata_dirty {
            return None;
        }
        self.metadata_dirty = false;
        Some(self.next_snapshot())
    }

    /// Re-dirty the metadata after a snapshot write failed, so the next
    /// periodic flush retries instead of dropping the change.
    pub fn mark_metadata_dirty(&mut self) {
        self.metadata_dirty = true;
    }

    /// Persist cache metadata to disk with fsync. Mutators only flag
    /// `metadata_dirty` instead of writing on every call (that limited
    /// throughput to ~1.2 MB/s); this flush runs on shutdown, where holding the
    /// cache lock costs nothing. Without fsync a restart can leave
    /// `cache_metadata.txt` stale, making the restart scan register wrong sizes
    /// and the verifier purge pieces.
    pub fn flush(&mut self) -> TorrentResult<()> {
        let result = self.next_snapshot().write();
        // A failed write must stay dirty so the next periodic tick retries;
        // an abandoned engine thread can also still be writing, and the
        // generation gate makes whichever snapshot is newer win.
        self.metadata_dirty = result.is_err();
        result
    }

    /// Load the persisted map, then reconcile it against what is actually on
    /// disk: a piece whose file is gone is dropped from the map and from the
    /// size accounting.  Without the reconciliation the map (and `has_piece`)
    /// keeps claiming pieces an external delete, a failed eviction or a crash
    /// left behind, and eviction keeps counting bytes the disk no longer
    /// holds (C-5.1.4, C-5.3.1).
    fn load_metadata_file(&mut self, path: &Path) -> TorrentResult<()> {
        let file = File::open(path)
            .map_err(|e| TorrentError::IoError(format!("Failed to open metadata file: {}", e)))?;
        let mut reader = BufReader::new(file);
        let mut raw = Vec::new();

        // Read raw bytes: `BufRead::lines` fails the whole load on a single
        // non-UTF-8 line, and one corrupt line must not keep the engine from
        // starting (C-5.3.1).
        loop {
            raw.clear();
            let read = reader.read_until(b'\n', &mut raw).map_err(|e| {
                TorrentError::IoError(format!("Failed to read metadata file: {}", e))
            })?;
            if read == 0 {
                break;
            }

            let line = String::from_utf8_lossy(&raw);
            let line = line.trim_end_matches(['\n', '\r']);
            if line.is_empty() {
                continue;
            }

            let mut parts = line.split('\t');
            let Some(piece_key) = parts.next() else {
                continue;
            };
            let Ok(last_accessed) = parts.next().unwrap_or_default().parse::<u64>() else {
                tracing::warn!("Dropping malformed cache metadata line: {:?}", line);
                continue;
            };
            let Ok(size) = parts.next().unwrap_or_default().parse::<u64>() else {
                tracing::warn!("Dropping malformed cache metadata line: {:?}", line);
                continue;
            };
            let hit_count = parts.next().and_then(|v| v.parse::<u64>().ok()).unwrap_or(0);

            // The key becomes a path, so the file is not trusted to name
            // anything but a piece leaf under `pieces/<info_hash>/` (C-5.4.6).
            if !is_valid_piece_key(piece_key) {
                tracing::warn!(
                    "Dropping cache metadata entry with invalid piece key {:?}",
                    piece_key
                );
                continue;
            }

            self.metadata.insert(
                piece_key.to_string(),
                PieceMetadata {
                    last_accessed,
                    size,
                    hit_count,
                },
            );
        }

        Ok(())
    }

    fn scan_cache_directory(&mut self) -> TorrentResult<()> {
        self.current_size = 0;

        self.remove_stale_metadata_scratch_files();

        if let Err(e) = self.migrate_old_cache_files() {
            tracing::warn!("Failed to migrate old cache files: {:?}", e);
        }

        let pieces_dir = self.cache_dir.join("pieces");
        let mut on_disk = HashSet::new();
        if pieces_dir.exists() {
            self.scan_pieces_subdirectory(&pieces_dir, &mut on_disk)?;
        }

        // Reconcile: the loaded map may reference pieces removed outside the
        // cache (a manual `rm`, a crashed writer, a failed eviction).  Those
        // entries would keep `has_piece` true and current_size inflated, so
        // they are dropped here instead of waiting for the background verifier.
        let stale: Vec<String> = self
            .metadata
            .keys()
            .filter(|key| !on_disk.contains(*key))
            .cloned()
            .collect();
        if !stale.is_empty() {
            tracing::warn!(
                "Dropping {} cache metadata entries whose piece file is missing",
                stale.len()
            );
            for key in &stale {
                self.metadata.remove(key);
            }
        }
        self.verified_piece_keys.retain(|key| on_disk.contains(key));
        self.incomplete_piece_keys.retain(|key| on_disk.contains(key));

        self.lru.clear();
        for (key, meta) in &self.metadata {
            self.lru.insert((meta.last_accessed, key.clone()));
        }

        Ok(())
    }

    /// Remove scratch files a crash left behind.
    ///
    /// A cache directory is not locked across processes, so two of them can
    /// write the same directory.  Each scratch name carries the writer's pid,
    /// and cleanup therefore only drops a scratch whose owner is gone: deleting
    /// a live writer's scratch would turn its `rename` into `ENOENT` and lose
    /// that flush.  A name without a parseable pid is not one this writer
    /// produces and is left alone; the piece scan skips it either way.
    fn remove_stale_metadata_scratch_files(&self) {
        let Ok(entries) = fs::read_dir(&self.cache_dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !is_metadata_scratch_name(name) {
                continue;
            }
            let Some(owner) = scratch_owner_pid(name) else {
                continue;
            };
            if process_is_alive(owner) {
                tracing::debug!(
                    "Keeping cache metadata scratch file {} of live writer pid {}",
                    path.display(),
                    owner
                );
                continue;
            }
            match fs::remove_file(&path) {
                Ok(()) => tracing::info!(
                    "Removed leftover cache metadata scratch file {}",
                    path.display()
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!(
                    "Failed to remove leftover cache metadata scratch file {}: {}",
                    path.display(),
                    e
                ),
            }
        }
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

            if filename == CACHE_METADATA_FILE || is_metadata_scratch_name(filename) {
                continue;
            }

            // Only migrate names the cache could have written itself: the
            // key becomes a path component below `pieces/`, so anything else
            // (a leftover scratch file, a name from another tool) is left
            // where it is (C-5.4.6).
            if !is_valid_piece_key(filename) {
                continue;
            }

            let info_hash = piece_key_info_hash(filename);

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

    /// Register every piece file under `dir` and collect its key in
    /// `on_disk`.  A file that is not a piece the cache could have written —
    /// a non-UTF-8 name, a name outside the key whitelist, a marker, a file
    /// whose parent is not its own info hash — is skipped with a warning: the
    /// key becomes a path, and the scan is the other place an outside actor
    /// can inject one (C-5.4.6).
    fn scan_pieces_subdirectory(
        &mut self,
        dir: &Path,
        on_disk: &mut HashSet<String>,
    ) -> TorrentResult<()> {
        let entries = fs::read_dir(dir)
            .map_err(|e| TorrentError::IoError(format!("Failed to read directory: {}", e)))?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                TorrentError::IoError(format!("Failed to read directory entry: {}", e))
            })?;
            let path = entry.path();

            if path.is_dir() {
                self.scan_pieces_subdirectory(&path, on_disk)?;
            } else if path.is_file() {
                let Some(piece_key) = path.file_name().and_then(|n| n.to_str()) else {
                    tracing::warn!("Skipping cache file with non-UTF-8 name: {:?}", path);
                    continue;
                };

                if piece_key == CACHE_METADATA_FILE || is_metadata_scratch_name(piece_key) {
                    continue;
                }

                // `.incomplete` files are markers, not piece data.
                // They are consumed below to tag their sibling piece as
                // incomplete; never register them as pieces themselves.
                if piece_key.ends_with(INCOMPLETE_SUFFIX) {
                    continue;
                }

                if !is_valid_piece_key(piece_key) {
                    tracing::warn!("Skipping cache file with invalid piece key: {:?}", path);
                    continue;
                }

                // The layout is `pieces/<info_hash>/<piece_key>`: a piece
                // file sitting under another hash's directory cannot be
                // reached through `piece_path`, so registering it would only
                // inflate the accounting.
                let parent_name = path
                    .parent()
                    .and_then(|p| p.file_name())
                    .and_then(|n| n.to_str());
                if parent_name != Some(piece_key_info_hash(piece_key)) {
                    tracing::warn!(
                        "Skipping cache file outside its info-hash directory: {:?}",
                        path
                    );
                    continue;
                }

                let piece_key = piece_key.to_string();

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

                // a sibling `<piece_key>.incomplete` marker means
                // the C++ write path is still filling this piece. Keep it in
                // metadata (so has_piece/size stay consistent) but exclude it
                // from background SHA-1 verification.
                if self.piece_marker_path(&piece_key).is_some_and(|p| p.exists()) {
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
                on_disk.insert(piece_key);
            }
        }

        Ok(())
    }

    /// Persist the metadata file atomically from the current in-memory map.
    /// Startup and shutdown call this while they already hold the cache lock;
    /// the periodic path takes a snapshot instead so its write runs outside it.
    fn save_metadata_file(&mut self) -> TorrentResult<()> {
        self.next_snapshot().write()
    }

    pub fn record_access(&mut self, piece_key: &str) -> TorrentResult<()> {
        let now = current_timestamp_ms();

        if let Some(meta) = self.metadata.get_mut(piece_key) {
            self.lru.remove(&(meta.last_accessed, piece_key.to_string()));
            meta.last_accessed = now;
            meta.hit_count += 1;
            self.hit_count += 1;
            self.lru.insert((now, piece_key.to_string()));
        } else {
            self.miss_count += 1;
            return Err(TorrentError::IoError(format!(
                "Piece not found in cache: {}",
                piece_key
            )));
        }

        // do not fsync on every read — mark dirty and let
        // the engine loop flush periodically / on shutdown.
        self.metadata_dirty = true;
        Ok(())
    }

    pub fn add_piece(&mut self, piece_key: &str, size: u64) -> TorrentResult<()> {
        if !is_valid_piece_key(piece_key) {
            return Err(TorrentError::IoError(format!(
                "Refusing to register piece with invalid key: {:?}",
                piece_key
            )));
        }
        let now = current_timestamp_ms();

        // insert returns the previous metadata if the piece_key
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

        if let Some(prev) = &prev {
            self.lru.remove(&(prev.last_accessed, piece_key.to_string()));
        }
        self.lru.insert((now, piece_key.to_string()));

        let old_size = prev.map(|m| m.size).unwrap_or(0);

        // mark as verified — add_piece is only called after
        // a successful libtorrent read_piece, which is the authoritative
        // proof of piece completeness.
        self.verified_piece_keys.insert(piece_key.to_string());

        // the piece is now provably complete — clear any
        // in-progress marker left by the C++ write path.
        self.incomplete_piece_keys.remove(piece_key);
        if let Some(marker) = self.piece_marker_path(piece_key) {
            if marker.exists() {
                if let Err(e) = fs::remove_file(&marker) {
                    tracing::warn!(
                        "Failed to remove incomplete marker {}: {}",
                        marker.display(),
                        e
                    );
                }
            }
        }

        self.current_size += size.saturating_sub(old_size);

        if self.current_size > self.max_cache_size {
            self.evict_lru()?;
        }

        // mark dirty — the engine loop flushes periodically
        // and on shutdown instead of fsyncing on every add_piece.
        self.metadata_dirty = true;
        Ok(())
    }

    /// Create (or truncate) the `.incomplete` marker for a piece and fsync it.
    ///
    /// The marker is the *only* on-disk signal `scan_pieces_subdirectory`
    /// reads on restart to classify a piece as incomplete; without it a
    /// partial piece is treated as an unverified SHA-1 verification candidate
    /// and purged.  Writing and fsyncing it before the metadata entry is
    /// inserted makes the registration atomic in the direction that matters:
    /// metadata is only registered once the incomplete classification is
    /// durably on disk.
    fn ensure_incomplete_marker(&self, marker: &Path) -> TorrentResult<()> {
        let file = File::create(marker).map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to create incomplete marker {}: {}",
                marker.display(),
                e
            ))
        })?;
        file.sync_all().map_err(|e| {
            TorrentError::IoError(format!(
                "Failed to fsync incomplete marker {}: {}",
                marker.display(),
                e
            ))
        })?;
        Ok(())
    }

    /// Register a piece discovered on disk as *incomplete*.
    ///
    /// The C++ `PieceStorage::write_piece` writes blocks to a piece file and
    /// drops a `<piece_key>.incomplete` marker alongside it while the piece is
    /// still being filled.  When a read's piece-wait window times out before
    /// libtorrent finishes the piece, the partially-downloaded bytes already
    /// sit on disk but the Rust cache metadata has no entry for them — so
    /// `.stats` reports zero cached pieces and the failed read leaves no
    /// trace of the download progress it actually made.
    ///
    /// This records that on-disk state the same way
    /// [`Self::scan_pieces_subdirectory`] does on restart: the piece enters
    /// metadata at its *actual* on-disk size (so `.stats` reflects the real
    /// download progress), it is tagged incomplete (excluded from background
    /// SHA-1 verification), and it is explicitly NOT marked verified (so the
    /// read fast-path never serves a partial piece).  A later successful
    /// `add_piece` upgrades it to verified and clears the marker.
    pub fn register_incomplete_piece(&mut self, piece_key: &str, size: u64) -> TorrentResult<()> {
        if !is_valid_piece_key(piece_key) {
            return Err(TorrentError::IoError(format!(
                "Refusing to register piece with invalid key: {:?}",
                piece_key
            )));
        }
        // Write the marker durably *before* mutating
        // metadata.  If the marker cannot be persisted, returning Err without
        // registering is the safe fallback — a partial piece left unregistered
        // degrades to the pre-fix behaviour instead of being mis-classified as
        // a verification candidate on restart and purged.
        let marker = self
            .piece_marker_path(piece_key)
            .ok_or_else(|| TorrentError::IoError(format!("Invalid piece key: {:?}", piece_key)))?;
        self.ensure_incomplete_marker(&marker)?;

        let now = current_timestamp_ms();
        let prev_hit_count = self
            .metadata
            .get(piece_key)
            .map(|m| m.hit_count)
            .unwrap_or(0);
        let prev = self.metadata.insert(
            piece_key.to_string(),
            PieceMetadata {
                last_accessed: now,
                size,
                hit_count: prev_hit_count,
            },
        );
        if prev.is_none() {
            self.miss_count += 1;
        }
        if let Some(prev) = &prev {
            self.lru.remove(&(prev.last_accessed, piece_key.to_string()));
        }
        self.lru.insert((now, piece_key.to_string()));
        let old_size = prev.map(|m| m.size).unwrap_or(0);

        // The piece is NOT verified — it is incomplete.
        self.verified_piece_keys.remove(piece_key);

        // Tag it incomplete so it is excluded from background SHA-1
        // verification and from the read fast-path (both key off the
        // verified set, not just metadata presence).
        self.incomplete_piece_keys.insert(piece_key.to_string());

        self.current_size += size.saturating_sub(old_size);
        if self.current_size > self.max_cache_size {
            self.evict_lru()?;
        }

        // Mark dirty — the engine loop flushes periodically and on
        // shutdown instead of fsyncing on every registration.
        self.metadata_dirty = true;
        Ok(())
    }

    /// Register a callback that will be invoked when a piece is evicted.
    /// The callback receives the info_hash of the affected torrent and the piece_index.
    pub fn on_evict(&mut self, callback: Box<dyn Fn(String, i32) + Send + Sync>) {
        self.evict_callbacks.push(callback);
    }

    /// Evict least-recently-accessed pieces until the accounted size fits the
    /// budget (C-5.1.2).
    ///
    /// Two kinds of piece are skipped: an incomplete one whose file libtorrent
    /// is still writing (`remove_piece` unlinks without the per-info-hash write
    /// lock, so evicting it would orphan in-flight blocks), and one whose file
    /// could not be deleted (a read-only mount, a permission error).  Neither
    /// aborts the pass: one undeletable piece must not pin the whole cache at
    /// the same victim forever (`C-5.4.4`: a failed removal is a warning).
    pub fn evict_lru(&mut self) -> TorrentResult<()> {
        let mut skipped: HashSet<String> = HashSet::new();

        while self.current_size > self.max_cache_size && !self.metadata.is_empty() {
            // `lru` is ordered oldest first, so the victim is taken from its
            // head instead of scanning every piece of the map.
            let oldest = self
                .lru
                .iter()
                .find(|(_, key)| {
                    !self.incomplete_piece_keys.contains(key) && !skipped.contains(key)
                })
                .map(|(_, key)| key.clone());

            let Some(piece_key) = oldest else {
                break;
            };

            match self.remove_piece(&piece_key) {
                Ok(()) => {
                    self.eviction_count += 1;
                    let info_hash = piece_key_info_hash(&piece_key).to_string();
                    let piece_index = self.extract_piece_index(&piece_key);
                    for callback in &self.evict_callbacks {
                        callback(info_hash.clone(), piece_index);
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "Failed to evict piece {}: {}; skipping it and continuing",
                        piece_key,
                        e
                    );
                    skipped.insert(piece_key);
                }
            }
        }

        Ok(())
    }

    fn remove_piece(&mut self, piece_key: &str) -> TorrentResult<()> {
        let Some(piece_path) = self.piece_path(piece_key) else {
            // An invalid key never names a file (C-5.4.6), so there is
            // nothing to unlink; forget whatever the map still holds.
            self.forget_piece(piece_key);
            return Ok(());
        };

        // Debit the registered `PieceMetadata.size` — the same value credited
        // by add_piece / scan_pieces_subdirectory — so current_size stays
        // symmetric. Using the on-disk file length here drifted upward on
        // partial writes: the full registered size was credited on add but only
        // the disk length debited on remove, so current_size never returned to
        // zero and the cache over-evicted.
        let registered_size = self.metadata.get(piece_key).map(|m| m.size).unwrap_or(0);

        // Attempt the unlink unconditionally: `Path::exists` also reports
        // false when the lookup itself fails, which would silently turn an
        // undeletable piece into a "successful" eviction and lose the file.
        match fs::remove_file(&piece_path) {
            Ok(()) => {}
            // Already gone: the goal ("file removed") is met.
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => {
                return Err(TorrentError::IoError(format!(
                    "Failed to remove piece file {}: {}",
                    piece_path.display(),
                    e
                )))
            }
        }

        // drop the in-progress marker alongside the piece file so
        // a stale marker never tags a future re-download of the same piece.
        if let Some(marker_path) = self.piece_marker_path(piece_key) {
            if let Err(e) = fs::remove_file(&marker_path) {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(
                        "Failed to remove incomplete marker {}: {}",
                        marker_path.display(),
                        e
                    );
                }
            }
        }
        self.current_size = self.current_size.saturating_sub(registered_size);

        self.forget_piece(piece_key);

        // mark dirty instead of fsyncing on every removal.
        self.metadata_dirty = true;
        Ok(())
    }

    /// Drop every in-memory record of `piece_key` (metadata, access order,
    /// verified/incomplete tags) without touching the disk or the size
    /// accounting.
    fn forget_piece(&mut self, piece_key: &str) {
        if let Some(meta) = self.metadata.remove(piece_key) {
            self.lru.remove(&(meta.last_accessed, piece_key.to_string()));
        }
        self.verified_piece_keys.remove(piece_key);
        self.incomplete_piece_keys.remove(piece_key);
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

    /// On-disk path of a piece, or `None` when the key is not one the write
    /// side could have produced (C-5.4.6).
    ///
    /// Building the path from a validated key is what keeps it a leaf below
    /// `pieces/<info_hash>/`: the whitelisted alphabet has no separator and no
    /// `.`, so a key can never resolve to an absolute path or a parent step.
    pub fn piece_path(&self, piece_key: &str) -> Option<PathBuf> {
        if !is_valid_piece_key(piece_key) {
            tracing::warn!("Refusing path lookup for invalid piece key {:?}", piece_key);
            return None;
        }
        let pieces_dir = self
            .cache_dir
            .join("pieces")
            .join(piece_key_info_hash(piece_key));
        Some(pieces_dir.join(piece_key))
    }

    /// Path of the `.incomplete` marker for a piece.
    fn piece_marker_path(&self, piece_key: &str) -> Option<PathBuf> {
        let mut os = self.piece_path(piece_key)?.into_os_string();
        os.push(INCOMPLETE_SUFFIX);
        Some(PathBuf::from(os))
    }

    pub fn ensure_piece_dir(&self, piece_key: &str) -> TorrentResult<PathBuf> {
        let piece_path = self
            .piece_path(piece_key)
            .ok_or_else(|| TorrentError::IoError(format!("Invalid piece key: {:?}", piece_key)))?;

        if let Some(pieces_dir) = piece_path.parent() {
            if !pieces_dir.exists() {
                fs::create_dir_all(pieces_dir).map_err(|e| {
                    TorrentError::IoError(format!("Failed to create pieces directory: {}", e))
                })?;
            }
        }

        Ok(piece_path)
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
        use std::io::{Seek, SeekFrom};
        let piece_path = self
            .piece_path(piece_key)
            .ok_or_else(|| TorrentError::IoError(format!("Invalid piece key: {:?}", piece_key)))?;
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

    /// whether a piece's metadata was registered via add_piece
    /// (verified by a successful libtorrent read_piece).  Pieces
    /// registered only by scan_pieces_subdirectory at startup are NOT
    /// verified — they could be incomplete sparse files from a crash.
    pub fn is_piece_verified(&self, piece_key: &str) -> bool {
        self.verified_piece_keys.contains(piece_key)
    }

    /// All piece keys present in cache metadata but not yet marked verified.
    /// These are candidates for background SHA-1 verification after a restart
    /// pieces discovered by `scan_pieces_subdirectory` that may be
    /// complete, or may be incomplete/corrupt files left by a crash.
    /// pieces tagged with an `.incomplete` marker are excluded —
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
    /// verification against the torrent's expected piece hash.
    ///
    /// The verified set is in-memory only (C-5.3.3): it is not written to the
    /// metadata file, so a restart re-verifies every scanned piece rather than
    /// trusting a persisted flag.
    pub fn mark_verified(&mut self, piece_key: &str) {
        self.verified_piece_keys.insert(piece_key.to_string());
        self.incomplete_piece_keys.remove(piece_key);
    }

    /// Delete a piece from the cache (metadata + on-disk file + verified set).
    /// Used to purge pieces that failed SHA-1 verification so they can be
    /// re-downloaded on demand.
    pub fn delete_piece(&mut self, piece_key: &str) -> TorrentResult<()> {
        self.remove_piece(piece_key)
    }
    /// Compute the on-disk pieces directory for `info_hash`, or `None` when
    /// the hash is empty or would escape the pieces root.  Pure path
    /// computation + validation — no filesystem I/O.
    pub(crate) fn pieces_dir_for(&self, info_hash: &str) -> Option<PathBuf> {
        // Defensive guard: only ever touch a leaf directory whose name is
        // literally this info_hash.  A path with separators (e.g. `../x`)
        // would change the leaf name and is refused, so this can never escape
        // the pieces directory.
        if info_hash.is_empty() {
            tracing::warn!("Refusing to purge pieces for empty info_hash");
            return None;
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
            return None;
        }
        Some(pieces_dir)
    }

    /// Drop the in-memory state for `info_hash`: metadata entries,
    /// verified/incomplete keys, and the current-size debit.  No filesystem
    /// I/O — the caller must have already removed the on-disk directory.
    pub(crate) fn drop_infohash_state(&mut self, info_hash: &str) {
        let removed_size = self.infohash_total_size(info_hash);
        self.current_size = self.current_size.saturating_sub(removed_size);
        self.remove_infohash_metadata(info_hash);
        // mark dirty instead of fsyncing on every purge.
        self.metadata_dirty = true;
    }

    /// Drop `info_hash`'s in-memory state only if its pieces directory is
    /// absent, returning `true` when the state was actually dropped.
    ///
    /// The on-disk directory removal runs lock-free (and may retry against a
    /// concurrent writer), so a same-info_hash re-add can recreate the
    /// directory in that window.  When the directory is back, keep the
    /// re-add's registration and skip the drop — the stale entries re-heal via
    /// re-download.
    pub(crate) fn drop_infohash_state_if_absent(&mut self, info_hash: &str) -> bool {
        let Some(pieces_dir) = self.pieces_dir_for(info_hash) else {
            return false;
        };
        if pieces_dir.exists() {
            return false;
        }
        self.drop_infohash_state(info_hash);
        true
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

    /// Drop metadata + access order + verified/incomplete-set entries for a
    /// given info_hash.
    fn remove_infohash_metadata(&mut self, info_hash: &str) {
        let prefix = format!("{}:", info_hash);
        let dropped: Vec<(u64, String)> = self
            .lru
            .iter()
            .filter(|(_, key)| key.starts_with(&prefix))
            .cloned()
            .collect();
        for entry in dropped {
            self.lru.remove(&entry);
        }
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
        self.piece_path(piece_key).is_some_and(|path| path.exists())
    }

    /// Check if a piece file exists on disk with a real length reaching
    /// `expected`.  `Path::exists` alone would pass a truncated file, so
    /// completeness/staleness decisions must compare the actual length — a
    /// truncated piece is as unusable as a missing one.
    pub fn piece_on_disk_at_least(&self, piece_key: &str, expected: u64) -> bool {
        let Some(path) = self.piece_path(piece_key) else {
            return false;
        };
        match fs::metadata(path) {
            Ok(m) => m.len() >= expected,
            Err(_) => false,
        }
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

/// flush dirty metadata when the CacheManager is dropped, so
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
    use std::os::unix::fs::MetadataExt;
    use tempfile::TempDir;

    /// 40-hex info hash for tests.  Cache keys are whitelisted to the shape
    /// libtorrent produces, so a readable tag has to be hashed into that shape
    /// instead of standing in for one.
    fn hash(tag: &str) -> String {
        // FNV-1a over the tag, expanded to sha1 length.
        let mut state: u64 = 0xcbf2_9ce4_8422_2325;
        let mut hex = String::with_capacity(48);
        for round in 0..3 {
            state ^= round;
            for byte in tag.as_bytes() {
                state ^= u64::from(*byte);
                state = state.wrapping_mul(0x0000_0100_0000_01b3);
            }
            hex.push_str(&format!("{state:016x}"));
        }
        hex.truncate(40);
        hex
    }

    /// A well-formed piece key for `tag`/`index`.
    fn key(tag: &str, index: usize) -> String {
        format!("{}:piece:{}", hash(tag), index)
    }

    /// Scratch files the metadata rewrite left in `cache_dir`.
    fn scratch_files(cache_dir: &Path) -> Vec<PathBuf> {
        fs::read_dir(cache_dir)
            .expect("read cache dir")
            .flatten()
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(is_metadata_scratch_name)
            })
            .collect()
    }

    /// A pid no live process owns: the writer of a scratch file that a crash
    /// left behind.
    #[cfg(unix)]
    fn dead_pid() -> u32 {
        let mut child = std::process::Command::new("true")
            .spawn()
            .expect("spawn a short-lived child");
        let pid = child.id();
        child.wait().expect("reap the child");
        assert!(!process_is_alive(pid), "the kernel recycled the child's pid");
        pid
    }

    #[test]
    fn test_cache_manager_basic() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert_eq!(cache.piece_count(), 0);

        let test_key = &key("abc123", 0);
        let piece_path = cache.ensure_piece_dir(test_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(test_key, 100)?;

        assert!(cache.has_piece(test_key));
        assert_eq!(cache.piece_count(), 1);

        Ok(())
    }

    /// The metadata file must never be observable half-written: the save writes
    /// a scratch file and renames it over the authoritative name, so a reader
    /// (or a restart after an interrupted shutdown) sees a complete map.
    ///
    /// Atomicity is asserted three ways, because "the scratch is gone" alone is
    /// also true of an in-place truncating write: the published file is a new
    /// inode after a rewrite (a rename, not a truncate), a save that cannot
    /// publish leaves the previously written map byte-identical, and a
    /// crash-leftover scratch is removed by the next start instead of being
    /// read as a piece.
    #[test]
    fn metadata_save_is_atomic_and_leaves_no_scratch() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        let metadata_path = temp_dir.path().join(CACHE_METADATA_FILE);
        let piece_key = key("hash1", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(&piece_key, 100)?;
        cache.flush()?;

        let saved = std::fs::read_to_string(&metadata_path).unwrap();
        assert!(
            saved.lines().any(|line| line.starts_with(piece_key.as_str())),
            "the flushed metadata does not carry the registered piece:\n{saved}"
        );
        assert!(
            scratch_files(temp_dir.path()).is_empty(),
            "the rename must consume the scratch file"
        );

        // A rewrite publishes a *new* file.  Reusing the published inode means
        // the save truncated it in place, which is exactly the half-written
        // window the rename exists to remove.
        let published = fs::metadata(&metadata_path).unwrap();
        let second_path = cache.ensure_piece_dir(&key("hash1", 1))?;
        std::fs::write(&second_path, vec![0u8; 200])?;
        cache.add_piece(&key("hash1", 1), 200)?;
        cache.flush()?;
        let replaced = fs::metadata(&metadata_path).unwrap();
        assert_ne!(
            published.ino(),
            replaced.ino(),
            "the metadata rewrite reused the published file's inode: the save \
             wrote the target in place instead of renaming a scratch over it"
        );

        // A save that cannot publish must not touch the previously written
        // file at all; a direct write would leave a truncated target behind.
        let before_failure = fs::read_to_string(&metadata_path).unwrap();
        let backup = temp_dir.path().join("published.backup");
        fs::rename(&metadata_path, &backup)?;
        fs::create_dir(&metadata_path)?;
        let third_path = cache.ensure_piece_dir(&key("hash1", 2))?;
        std::fs::write(&third_path, vec![0u8; 300])?;
        cache.add_piece(&key("hash1", 2), 300)?;
        assert!(
            cache.flush().is_err(),
            "a save whose scratch cannot be published must report the failure"
        );
        assert_eq!(
            fs::read_to_string(&backup)?,
            before_failure,
            "a failed save changed the previously written metadata file"
        );
        assert!(
            scratch_files(temp_dir.path()).is_empty(),
            "a failed save left its scratch file behind"
        );
        fs::remove_dir(&metadata_path)?;
        fs::rename(&backup, &metadata_path)?;

        // A crash-leftover scratch is removed at startup, not registered as a
        // piece.
        #[cfg(unix)]
        let leftover = {
            let leftover = temp_dir.path().join(format!(
                "{CACHE_METADATA_SCRATCH_PREFIX}{}.0{CACHE_METADATA_SCRATCH_SUFFIX}",
                dead_pid()
            ));
            std::fs::write(&leftover, "half-written").unwrap();
            leftover
        };
        let reopened = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        #[cfg(unix)]
        assert!(
            !leftover.exists(),
            "startup must clear a crash-leftover scratch file"
        );
        assert!(reopened.has_piece(&piece_key));
        assert_eq!(reopened.piece_count(), 3);
        Ok(())
    }

    /// Startup cleanup must spare a live writer's scratch: two processes can
    /// share a cache directory, and deleting that file would turn the writer's
    /// `rename` into `ENOENT`, losing its flush.
    #[cfg(unix)]
    #[test]
    fn test_stale_scratch_cleanup_spares_a_live_writer() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let scratch = |pid: u32| {
            temp_dir.path().join(format!(
                "{CACHE_METADATA_SCRATCH_PREFIX}{pid}.0{CACHE_METADATA_SCRATCH_SUFFIX}"
            ))
        };
        let mut writer = std::process::Command::new("sleep")
            .arg("30")
            .spawn()
            .expect("spawn a live writer process");
        let live = scratch(writer.id());
        std::fs::write(&live, "in flight")?;
        let dead = scratch(dead_pid());
        std::fs::write(&dead, "leftover")?;

        let _reopened = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(live.exists(), "cleanup deleted a live writer's scratch file");
        assert!(!dead.exists(), "cleanup kept a scratch whose writer is gone");
        let _ = writer.kill();
        let _ = writer.wait();
        Ok(())
    }

    #[test]
    fn test_lru_eviction() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;

        let piece_1_key = key("hash1", 0);
        let piece_1_path = cache.ensure_piece_dir(&piece_1_key)?;
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece(&piece_1_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_2_key = key("hash1", 1);
        let piece_2_path = cache.ensure_piece_dir(&piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece(&piece_2_key, 100)?;

        assert!(cache.has_piece(&piece_1_key));
        assert!(cache.has_piece(&piece_2_key));
        assert_eq!(cache.current_size(), 200);

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_3_key = key("hash1", 2);
        let piece_3_path = cache.ensure_piece_dir(&piece_3_key)?;
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece(&piece_3_key, 100)?;

        assert!(
            !cache.has_piece(&piece_1_key),
            "piece_1 should be evicted (oldest)"
        );
        assert!(cache.has_piece(&piece_2_key), "piece_2 should remain");
        assert!(cache.has_piece(&piece_3_key), "piece_3 should remain");
        assert_eq!(cache.current_size(), 200);

        Ok(())
    }

    #[test]
    fn test_persistence_across_restart() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_key = key("def456", 0);
            let piece_path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 50])?;
            cache.add_piece(&piece_key, 50)?;
        }

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(cache.has_piece(&key("def456", 0)));

        Ok(())
    }

    #[test]
    fn test_unverified_pieces_mark_verified_and_delete() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        // Simulate a piece file left on disk from a previous run (no add_piece,
        // so it is discovered by scan_pieces_subdirectory on restart).
        let piece_key = key("cafe1234", 3);
        {
            let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&piece_path, vec![0xABu8; 100])?;
        }

        // Restart: the scanned piece is registered but NOT verified.
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));
        assert_eq!(cache.unverified_pieces(), vec![piece_key.clone()]);

        // Mark verified after a (hypothetical) successful SHA-1 check.
        cache.mark_verified(&piece_key);
        assert!(cache.is_piece_verified(&piece_key));
        assert!(cache.unverified_pieces().is_empty());

        // A corrupted piece is purged: metadata, file and verified flag all go.
        cache.delete_piece(&piece_key)?;
        assert!(!cache.has_piece(&piece_key));
        assert!(!cache.has_piece_on_disk(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        Ok(())
    }

    #[test]
    fn test_delete_piece_removes_file_and_metadata() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("beef5678", 1);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 10])?;
        cache.add_piece(&piece_key, 10)?;
        assert!(cache.is_piece_verified(&piece_key));

        cache.delete_piece(&piece_key)?;

        assert!(!cache.has_piece(&piece_key));
        assert!(!piece_path.exists());
        assert!(!cache.is_piece_verified(&piece_key));

        Ok(())
    }

    #[test]
    fn test_remove_infohash_pieces_purges_dir_and_metadata() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let hash_a = hash("aaaa1111");
        let key_a = key("aaaa1111", 0);
        let key_b = key("bbbb2222", 0);
        let path_a = cache.ensure_piece_dir(&key_a)?;
        let path_b = cache.ensure_piece_dir(&key_b)?;
        std::fs::write(&path_a, vec![0u8; 100])?;
        std::fs::write(&path_b, vec![0u8; 50])?;
        cache.add_piece(&key_a, 100)?;
        cache.add_piece(&key_b, 50)?;
        assert_eq!(cache.current_size(), 150);

        let pieces_dir = cache.pieces_dir_for(&hash_a).unwrap();
        remove_dir_all_tolerating_writers(&pieces_dir)?;
        cache.drop_infohash_state(&hash_a);

        assert!(!cache.has_piece(&key_a));
        assert!(!path_a.exists());
        assert!(!path_a.parent().unwrap().exists());
        assert!(!cache.is_piece_verified(&key_a));

        // A different info_hash must be left untouched.
        assert!(cache.has_piece(&key_b));
        assert!(path_b.exists());
        assert!(cache.is_piece_verified(&key_b));
        assert_eq!(cache.current_size(), 50);

        Ok(())
    }

    #[test]
    fn test_remove_infohash_pieces_idempotent_and_refuses_escape() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        // No pieces for this hash: removing an absent directory is a no-op,
        // not an error (`remove_dir_all`'s NotFound is treated as success).
        let pieces_dir = cache.pieces_dir_for("missing_hash").unwrap();
        remove_dir_all_tolerating_writers(&pieces_dir)?;

        // A path with separators must be refused and never resolved against a
        // parent directory.
        let outside = temp_dir.path().join("outside.txt");
        std::fs::write(&outside, b"keep me")?;

        assert!(cache.pieces_dir_for("../outside.txt").is_none());
        assert!(outside.exists());
        assert!(!temp_dir.path().join("outside").exists());

        Ok(())
    }

    /// The on-disk directory is gone → the state must be dropped and `true`
    /// returned.
    #[test]
    fn test_drop_infohash_state_if_absent_drops_when_dir_absent() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let info_hash = hash("dddd4444");
        let piece_key = key("dddd4444", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(&piece_key, 100)?;
        assert!(cache.has_piece(&piece_key));

        // Simulate the lock-free removal having already taken the directory.
        let pieces_dir = cache.pieces_dir_for(&info_hash).unwrap();
        fs::remove_dir_all(&pieces_dir)?;

        assert!(cache.drop_infohash_state_if_absent(&info_hash));
        assert!(!cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        Ok(())
    }

    /// The directory reappeared (a concurrent same-info_hash re-add) → skip
    /// the drop, keep the registration, and return `false`.
    #[test]
    fn test_drop_infohash_state_if_absent_skips_when_dir_present() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let info_hash = hash("eeee5555");
        let piece_key = key("eeee5555", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 100])?;
        cache.add_piece(&piece_key, 100)?;
        assert!(cache.has_piece(&piece_key));

        assert!(!cache.drop_infohash_state_if_absent(&info_hash));
        assert!(cache.has_piece(&piece_key));
        assert!(cache.is_piece_verified(&piece_key));

        Ok(())
    }

    /// The retry loop must re-attempt a transient ENOTEMPTY and succeed once
    /// the injected removal stops failing — this is the exact sequence a
    /// concurrent writer produces.
    #[test]
    fn test_remove_dir_all_retries_transient_directory_not_empty() {
        let path = Path::new("/unused");
        let mut calls = 0u32;
        let result = remove_dir_all_with_retry(path, 6, Duration::from_millis(1), |_| {
            calls += 1;
            if calls <= 2 {
                Err(std::io::Error::from(std::io::ErrorKind::DirectoryNotEmpty))
            } else {
                Ok(())
            }
        });

        assert!(result.is_ok());
        assert_eq!(calls, 3, "two transient failures then success");
    }

    /// A directory already removed by a racer is not an error.
    #[test]
    fn test_remove_dir_all_treats_not_found_as_success() {
        let result =
            remove_dir_all_with_retry(Path::new("/unused"), 6, Duration::from_millis(1), |_| {
                Err(std::io::Error::from(std::io::ErrorKind::NotFound))
            });

        assert!(result.is_ok());
    }

    /// A persistent ENOTEMPTY (a writer that never stops) exhausts the budget
    /// and surfaces the last error rather than looping forever.
    #[test]
    fn test_remove_dir_all_gives_up_after_budget() {
        let mut calls = 0u32;
        let result =
            remove_dir_all_with_retry(Path::new("/unused"), 3, Duration::from_millis(1), |_| {
                calls += 1;
                Err(std::io::Error::from(std::io::ErrorKind::DirectoryNotEmpty))
            });

        let err = result.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::DirectoryNotEmpty);
        assert_eq!(calls, 3, "bounded attempts, no infinite loop");
    }

    /// Purging a stale info_hash directory races libtorrent's custom storage,
    /// which keeps writing piece files right after a restart.  The writer
    /// proves it is writing via a handshake before the purge starts, then
    /// keeps adding files while the (pre-filled, slow-to-walk) directory is
    /// removed; the retry must win once the writer finishes.
    #[test]
    fn test_remove_dir_all_tolerates_racing_writer() {
        let temp_dir = TempDir::new().unwrap();
        let dir = temp_dir.path().join("pieces").join("cccc3333");
        std::fs::create_dir_all(&dir).unwrap();

        // Pre-fill so `remove_dir_all`'s walk is long enough for a writer to
        // land a file mid-walk; otherwise the purge wins in microseconds and
        // the retry path is never exercised.
        for i in 0..3000 {
            std::fs::write(dir.join(format!("f{:04}", i)), b"x").unwrap();
        }

        let (tx, rx) = std::sync::mpsc::channel();
        let writer_dir = dir.clone();
        let writer = std::thread::spawn(move || {
            let mut wrote = 0u32;
            let mut signaled = false;
            for i in 0..20 {
                match std::fs::write(writer_dir.join(format!("racer{:04}", i)), b"y") {
                    Ok(()) => {
                        wrote += 1;
                        if !signaled {
                            signaled = true;
                            let _ = tx.send(());
                        }
                    }
                    // Directory removed: the purge won, stop racing.
                    Err(_) => break,
                }
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            wrote
        });

        // Handshake: wait until the writer has actually landed a file.
        rx.recv().unwrap();

        // Drive the retry with a short injectable delay; the writer's bounded
        // window is well under this budget, so the purge wins afterward.
        remove_dir_all_with_retry(&dir, 20, Duration::from_millis(5), |p| {
            fs::remove_dir_all(p)
        })
        .unwrap();

        let wrote = writer.join().expect("writer thread panicked");
        assert!(wrote >= 1, "writer must have written at least one file");
        assert!(!dir.exists());
    }

    #[test]
    fn test_add_piece_marks_verified() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("deadbeef", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0u8; 20])?;
        cache.add_piece(&piece_key, 20)?;

        assert!(cache.is_piece_verified(&piece_key));
        assert!(cache.unverified_pieces().is_empty());

        Ok(())
    }

    #[test]
    fn test_record_access_updates_lru() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 250)?;

        let piece_1_key = key("hash2", 0);
        let piece_1_path = cache.ensure_piece_dir(&piece_1_key)?;
        std::fs::write(&piece_1_path, vec![0u8; 100])?;
        cache.add_piece(&piece_1_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_2_key = key("hash2", 1);
        let piece_2_path = cache.ensure_piece_dir(&piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 100])?;
        cache.add_piece(&piece_2_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));
        cache.record_access(&piece_1_key)?;

        std::thread::sleep(std::time::Duration::from_millis(5));

        let piece_3_key = key("hash2", 2);
        let piece_3_path = cache.ensure_piece_dir(&piece_3_key)?;
        std::fs::write(&piece_3_path, vec![0u8; 100])?;
        cache.add_piece(&piece_3_key, 100)?;

        assert!(
            cache.has_piece(&piece_1_key),
            "piece_1 should remain (accessed recently)"
        );
        assert!(
            !cache.has_piece(&piece_2_key),
            "piece_2 should be evicted (oldest after piece_1 access)"
        );
        assert!(cache.has_piece(&piece_3_key), "piece_3 should remain");

        Ok(())
    }

    #[test]
    fn test_piece_directory_structure() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let info_hash = hash("abc123def456");
        let piece_key = key("abc123def456", 0);

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

        let hash1 = hash("torrent_hash_1");
        let hash2 = hash("torrent_hash_2");

        let piece1_key = key("torrent_hash_1", 0);
        let piece1_path = cache.ensure_piece_dir(&piece1_key)?;
        std::fs::write(&piece1_path, vec![0u8; 100])?;
        cache.add_piece(&piece1_key, 100)?;

        let piece2_key = key("torrent_hash_2", 0);
        let piece2_path = cache.ensure_piece_dir(&piece2_key)?;
        std::fs::write(&piece2_path, vec![0u8; 100])?;
        cache.add_piece(&piece2_key, 100)?;

        let dir1 = temp_dir.path().join("pieces").join(&hash1);
        let dir2 = temp_dir.path().join("pieces").join(&hash2);

        assert!(dir1.exists(), "directory for torrent 1 should exist");
        assert!(dir2.exists(), "directory for torrent 2 should exist");
        assert!(dir1.join(&piece1_key).exists());
        assert!(dir2.join(&piece2_key).exists());

        Ok(())
    }

    #[test]
    fn test_backward_compatibility_migration() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        let info_hash = hash("oldhash123");
        let piece_key = key("oldhash123", 0);
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
    // for the premature-EOF bug.  When metadata says a piece
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

        let piece_key = key("hash_empty", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;

        // Register metadata with non-zero size…
        cache.add_piece(&piece_key, 4096)?;
        // …but write only 0 bytes to disk (simulates partial write /
        // crash before fsync).
        std::fs::write(&piece_path, &[])?;

        // Metadata exists — has_piece is true.
        assert!(cache.has_piece(&piece_key));

        // But the file on disk is empty, so reading it would yield
        // empty data.  This is the inconsistency that causes the
        // assembly loop to skip this piece.
        let disk_data = std::fs::read(&piece_path)?;
        assert!(
            disk_data.is_empty(),
            "disk file is empty despite metadata claiming 4096 bytes"
        );
        assert!(
            cache.has_piece_on_disk(&piece_key),
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

        let piece_key = key("hash_partial", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;

        // Write 32 KiB of a "256 KiB" piece — partial write scenario.
        let partial_data = vec![0xABu8; 32768];
        std::fs::write(&piece_path, &partial_data)?;

        // has_piece_on_disk returns true — file exists.
        assert!(cache.has_piece_on_disk(&piece_key));
        // has_piece returns false — metadata was never registered.
        assert!(!cache.has_piece(&piece_key));

        // When add_piece is called later with the partial file size,
        // metadata will match the partial file — but the true piece
        // length might be much larger.
        cache.add_piece(&piece_key, partial_data.len() as u64)?;
        assert!(cache.has_piece(&piece_key));
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

        let piece_key = key("hash_mismatch", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;

        let real_data = vec![0xCDu8; 1024]; // 1 KiB
        let claimed_size: u64 = 262144; // 256 KiB
        std::fs::write(&piece_path, &real_data)?;
        cache.add_piece(&piece_key, claimed_size)?;

        assert!(cache.has_piece(&piece_key));
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

        let piece_key = key("hash_empty_disk", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, &[])?;
        // Don't register in metadata — simulate pure disk-only scenario.
        assert!(
            cache.has_piece_on_disk(&piece_key),
            "empty file on disk is still detected as existing"
        );
        assert!(!cache.has_piece(&piece_key), "no metadata registered");

        // the fast-path no longer consults has_piece_on_disk;
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

        let info_hash = hash("orphaned_hash");
        let piece_key = key("orphaned_hash", 0);

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

    // ── restart with partial piece files ─────────
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

        let info_hash = hash("tsi2048_partial_hash");
        let piece_key = key("tsi2048_partial_hash", 0);

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

        let info_hash = hash("tsi2048_complete_hash");
        let piece_key = key("tsi2048_complete_hash", 0);

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

        // scanned pieces are NOT verified — only add_piece
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

        let piece_key = key("tsi2048_addpiece", 0);
        let _piece_path = cache.ensure_piece_dir(&piece_key)?;

        // Before add_piece: no metadata, not verified.
        assert!(!cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        // add_piece (called after read_piece succeeds).
        cache.add_piece(&piece_key, 262144)?;

        // After add_piece: has metadata AND verified.
        assert!(cache.has_piece(&piece_key));
        assert!(
            cache.is_piece_verified(&piece_key),
            "piece registered via add_piece should be verified"
        );
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(262144));

        Ok(())
    }

    #[test]
    fn test_add_piece_re_registration_no_double_count() -> TorrentResult<()> {
        // after restart scan_pieces_subdirectory already
        // counted an on-disk piece into current_size.  When the engine
        // poll loop re-registers the same piece via add_piece (have ==
        // true), current_size must not re-add the full size.
        let temp_dir = TempDir::new().unwrap();

        let piece_key = key("tsi2231_redup", 0);

        // Phase 1: write the piece file and register it so it lands on
        // disk + in the metadata file, then drop the manager.
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 512])?;
            cache.add_piece(&piece_key, 512)?;
        }

        // Phase 2: restart.  scan_pieces_subdirectory re-counts the
        // on-disk piece into current_size.
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert_eq!(cache.current_size(), 512);

        // Phase 3: engine poll loop calls add_piece again for the same
        // have==true piece.  current_size must stay at 512, not 1024.
        cache.add_piece(&piece_key, 512)?;
        assert_eq!(
            cache.current_size(),
            512,
            "re-registering an existing piece must not double-count"
        );

        Ok(())
    }

    // ── current_size add/remove symmetry ─────────
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

        let piece_key = key("tsi2242_short", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;

        // Register a 4096-byte piece…
        cache.add_piece(&piece_key, 4096)?;
        assert_eq!(cache.current_size(), 4096);

        // …but the disk file is empty (crash before fsync).
        std::fs::write(&piece_path, &[])?;
        assert_eq!(std::fs::metadata(&piece_path)?.len(), 0);

        cache.delete_piece(&piece_key)?;

        assert_eq!(
            cache.current_size(),
            0,
            "current_size must return to 0 after removing a piece whose disk file was shorter than its registered size"
        );
        assert!(!cache.has_piece(&piece_key));

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

        let piece_key = key("tsi2242_evict", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;

        cache.add_piece(&piece_key, 4096)?;
        assert_eq!(cache.current_size(), 4096);

        // Disk file is shorter than registered size.
        std::fs::write(&piece_path, vec![0u8; 100])?;
        assert_eq!(std::fs::metadata(&piece_path)?.len(), 100);

        // Sleep so piece_2 gets a strictly newer last_accessed, making piece_1
        // the unambiguous LRU victim.
        std::thread::sleep(std::time::Duration::from_millis(5));

        // Adding a second piece triggers eviction of the first.
        let piece_2_key = key("tsi2242_evict", 1);
        let piece_2_path = cache.ensure_piece_dir(&piece_2_key)?;
        std::fs::write(&piece_2_path, vec![0u8; 4096])?;
        cache.add_piece(&piece_2_key, 4096)?;

        assert!(
            !cache.has_piece(&piece_key),
            "first piece should be evicted"
        );
        assert_eq!(
            cache.current_size(),
            4096,
            "after evicting the partial piece, current_size should reflect only the surviving piece"
        );

        Ok(())
    }

    // ── cache flush durability ──────────────────────────────

    #[test]
    fn test_flush_persists_metadata_for_restart() -> TorrentResult<()> {
        // flush() must persist the current metadata to disk so that a
        // restart discovers all pieces at their registered sizes.  This
        // is the explicit shutdown flush that prevents the "cache piece
        // cleaned after restart" bug.
        let temp_dir = TempDir::new().unwrap();

        let piece_key = key("tsi2263_flush", 0);
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let piece_path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&piece_path, vec![0u8; 256])?;
            cache.add_piece(&piece_key, 256)?;
            assert!(cache.is_piece_verified(&piece_key));

            // Explicit flush — simulates the shutdown path.
            cache.flush()?;
        }

        // Restart: metadata must reflect the flushed state.
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(cache.has_piece(&piece_key));
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(256));

        Ok(())
    }

    #[test]
    fn test_flush_preserves_multiple_pieces_across_restart() -> TorrentResult<()> {
        // Multiple pieces from different torrents, each flushed before
        // shutdown, must all survive a restart at their correct sizes.
        let temp_dir = TempDir::new().unwrap();

        let key_a = key("tsi2263_hash_a", 0);
        let key_b = key("tsi2263_hash_b", 1);
        let key_c = key("tsi2263_hash_a", 2);
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            for (piece_key, sz) in [(&key_a, 128u64), (&key_b, 512u64), (&key_c, 256u64)] {
                let path = cache.ensure_piece_dir(piece_key)?;
                std::fs::write(&path, vec![0u8; sz as usize])?;
                cache.add_piece(piece_key, sz)?;
            }
            cache.flush()?;
        }

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert_eq!(cache.piece_metadata_size(&key_a), Some(128));
        assert_eq!(cache.piece_metadata_size(&key_b), Some(512));
        assert_eq!(cache.piece_metadata_size(&key_c), Some(256));
        assert_eq!(cache.piece_count(), 3);
        assert_eq!(cache.current_size(), 896);

        Ok(())
    }

    // ── record_access must not fsync on every read ───────────

    #[test]
    fn test_record_access_does_not_persist_until_flush() -> TorrentResult<()> {
        // record_access updates the in-memory hit_count + last_accessed
        // but must NOT fsync the metadata file on every call (the old
        // behaviour limited read throughput to ~1.2 MB/s).  The metadata
        // file is only persisted on an explicit flush or Drop.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = key("tsi2274", 0);
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&path, vec![0u8; 100])?;
            cache.add_piece(&piece_key, 100)?;
            // flush so the initial add_piece state is on disk.
            cache.flush()?;

            // Record 50 accesses — none should touch the metadata file.
            let meta_path = temp_dir.path().join("cache_metadata.txt");
            let mtime_before = std::fs::metadata(&meta_path)?.modified()?;
            for _ in 0..50 {
                cache.record_access(&piece_key)?;
            }
            let mtime_after = std::fs::metadata(&meta_path)?.modified()?;
            assert_eq!(
                mtime_before, mtime_after,
                "metadata file must not be touched by record_access (no fsync per read)"
            );
            // In-memory hit_count is still updated.
            assert_eq!(cache.piece_hit_count(&piece_key), 50);
        }
        Ok(())
    }

    #[test]
    fn test_drop_flushes_dirty_metadata() -> TorrentResult<()> {
        // CacheManager::drop flushes dirty metadata so tests and other
        // short-lived callers persist mutations without an explicit flush.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = key("tsi2274_drop", 0);
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            let path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&path, vec![0u8; 64])?;
            cache.add_piece(&piece_key, 64)?;
            // No explicit flush — drop must persist.
        }
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(
            cache.has_piece(&piece_key),
            "drop must have flushed dirty metadata so restart sees the piece"
        );
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(64));
        Ok(())
    }

    // ── incomplete-marker exclusion ─────────────
    //
    // The C++ write path drops `<piece_key>.incomplete` next to the piece
    // file while blocks are still being written.  On restart, those pieces
    // must stay in metadata (so `has_piece`/size stay consistent) but must
    // NOT be candidates for background SHA-1 verification — verifying a
    // half-written piece always fails and wrongly purges it.

    #[test]
    fn test_incomplete_marker_excludes_piece_from_verification() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();

        let info_hash = hash("tsi2491_incomplete");
        let piece_key = key("tsi2491_incomplete", 0);

        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
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

        let piece_key = key("tsi2491_addpiece", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        let marker_path = piece_path.with_extension("incomplete");
        std::fs::write(&piece_path, vec![0xCDu8; 262144])?;
        std::fs::write(&marker_path, b"")?;

        // add_piece is the authoritative complete-piece registration point.
        cache.add_piece(&piece_key, 262144)?;

        assert!(!marker_path.exists(), "marker must be cleared by add_piece");
        assert!(cache.is_piece_verified(&piece_key));
        Ok(())
    }

    #[test]
    fn test_register_incomplete_piece_not_verified() -> TorrentResult<()> {
        // register_incomplete_piece records a partially-downloaded
        // piece at its actual on-disk size, tags it incomplete (so it is not
        // a verification candidate) and leaves it unverified (so the read
        // fast-path never serves a partial piece).  A later add_piece must
        // upgrade it to verified and clear the marker.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("tsi3130", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0xABu8; 32768])?;

        cache.register_incomplete_piece(&piece_key, 32768)?;

        // Registered at the partial size, visible to `.stats`.
        assert!(cache.has_piece(&piece_key));
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(32768));

        // Not verified, and excluded from background verification.
        assert!(!cache.is_piece_verified(&piece_key));
        assert!(
            cache.unverified_pieces().is_empty(),
            "incomplete-registered piece must not be a verification candidate"
        );

        // Marker written so a restart re-classifies it as incomplete.
        let marker_path = piece_path.with_extension("incomplete");
        assert!(marker_path.exists(), "marker must be created");

        // A later successful download upgrades it to verified.
        cache.add_piece(&piece_key, 32768)?;
        assert!(cache.is_piece_verified(&piece_key));
        assert!(!marker_path.exists(), "marker must be cleared by add_piece");
        Ok(())
    }

    #[test]
    fn test_register_incomplete_piece_rolls_back_on_marker_failure() -> TorrentResult<()> {
        // A marker write failure must surface as Err and
        // leave the piece unregistered — a partial piece registered without a
        // durable marker would be mis-classified as a verification candidate
        // on restart and purged.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("tsi3130_markerfail", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0xABu8; 32768])?;

        // Occupy the marker path with a directory so File::create fails.
        let marker_path = piece_path.with_extension("incomplete");
        std::fs::create_dir(&marker_path)?;

        let result = cache.register_incomplete_piece(&piece_key, 32768);
        assert!(
            result.is_err(),
            "marker write failure must surface as Err, not be swallowed"
        );
        assert!(
            !cache.has_piece(&piece_key),
            "metadata must not be registered when the marker cannot be persisted"
        );

        Ok(())
    }

    #[test]
    fn test_register_incomplete_piece_preserves_hit_count() -> TorrentResult<()> {
        // Re-registering an existing entry must not reset
        // its access count.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("tsi3130_hitcount", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&piece_path, vec![0xABu8; 32768])?;
        cache.add_piece(&piece_key, 32768)?;
        for _ in 0..5 {
            cache.record_access(&piece_key)?;
        }
        assert_eq!(cache.piece_hit_count(&piece_key), 5);

        cache.register_incomplete_piece(&piece_key, 32768)?;
        assert_eq!(
            cache.piece_hit_count(&piece_key),
            5,
            "hit_count must survive re-registration"
        );
        Ok(())
    }

    #[test]
    fn test_evict_lru_skips_incomplete_pieces() -> TorrentResult<()> {
        // An over-budget eviction must evict a complete
        // piece, never an incomplete one whose file libtorrent is still
        // writing.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 64 * 1024)?;

        let verified_key = key("tsi3130_evict", 0);
        let incomplete_key = key("tsi3130_evict", 1);

        let v_path = cache.ensure_piece_dir(&verified_key)?;
        std::fs::write(&v_path, vec![0x11u8; 32_768])?;
        cache.add_piece(&verified_key, 32_768)?;

        let i_path = cache.ensure_piece_dir(&incomplete_key)?;
        std::fs::write(&i_path, vec![0x22u8; 40_960])?;
        // Pushes over budget (72 KiB > 64 KiB); eviction must skip the
        // incomplete piece.
        cache.register_incomplete_piece(&incomplete_key, 40_960)?;

        assert!(
            !cache.has_piece(&verified_key),
            "the complete LRU piece must be evicted"
        );
        assert!(
            cache.has_piece(&incomplete_key),
            "the incomplete piece must survive eviction"
        );
        assert_eq!(
            cache.eviction_count, 1,
            "evict_lru must count each evicted piece"
        );
        Ok(())
    }

    #[test]
    fn test_delete_piece_clears_incomplete_marker() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let piece_key = key("tsi2491_delete", 0);
        let piece_path = cache.ensure_piece_dir(&piece_key)?;
        let marker_path = piece_path.with_extension("incomplete");
        std::fs::write(&piece_path, vec![0xEFu8; 64])?;
        std::fs::write(&marker_path, b"")?;

        cache.add_piece(&piece_key, 64)?;
        cache.delete_piece(&piece_key)?;

        assert!(
            !marker_path.exists(),
            "marker must be removed with the piece"
        );
        assert!(!cache.has_piece(&piece_key));
        Ok(())
    }

    #[test]
    fn test_unmarked_scan_piece_still_verification_candidate() -> TorrentResult<()> {
        // Regression guard for the pre-existing behavior: a scanned piece
        // WITHOUT a marker remains an unverified verification candidate.
        let temp_dir = TempDir::new().unwrap();

        let info_hash = hash("tsi2491_unmarked");
        let piece_key = key("tsi2491_unmarked", 0);

        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir)?;
        std::fs::write(pieces_dir.join(&piece_key), vec![0x11u8; 100])?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert_eq!(
            cache.unverified_pieces(),
            vec![piece_key],
            "unmarked scanned piece must still be verified"
        );
        Ok(())
    }

    #[test]
    fn test_take_dirty_snapshot_clears_dirty() -> TorrentResult<()> {
        // The periodic flush copies the map under the cache lock and writes
        // the copy outside it, so the snapshot must both carry the current
        // entries and clear the dirty flag; an unchanged map yields nothing.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        let piece_key = key("tsi2274_clean", 0);
        let path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&path, vec![0u8; 32])?;
        cache.add_piece(&piece_key, 32)?;

        assert!(
            cache.take_dirty_snapshot().is_some(),
            "a registration must make the map dirty"
        );
        assert!(
            cache.take_dirty_snapshot().is_none(),
            "a clean map must not produce another snapshot"
        );

        // A mutation after the snapshot re-dirties the map for the next tick.
        cache.record_access(&piece_key)?;
        assert!(cache.take_dirty_snapshot().is_some());
        Ok(())
    }

    #[test]
    fn test_snapshot_write_is_retried_after_failure() -> TorrentResult<()> {
        // A snapshot whose write fails must not lose the change: the caller
        // re-dirties the map and the next tick writes it.  The failure is
        // injected by occupying the authoritative name with a directory, so
        // the rename cannot land — the scratch itself is written successfully
        // and must be cleaned up by the error path.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        let piece_key = key("tsi2274_retry", 0);
        let path = cache.ensure_piece_dir(&piece_key)?;
        std::fs::write(&path, vec![0u8; 32])?;
        cache.add_piece(&piece_key, 32)?;

        let snapshot = cache.take_dirty_snapshot().expect("dirty map");
        let published = temp_dir.path().join(CACHE_METADATA_FILE);
        let backup = temp_dir.path().join("published.backup");
        std::fs::rename(&published, &backup)?;
        std::fs::create_dir(&published)?;

        assert!(
            snapshot.write().is_err(),
            "a snapshot that cannot be published must report the failure"
        );
        assert!(
            scratch_files(temp_dir.path()).is_empty(),
            "a failed write must not leave its scratch file behind"
        );
        cache.mark_metadata_dirty();

        std::fs::remove_dir(&published)?;
        std::fs::rename(&backup, &published)?;

        let retry = cache.take_dirty_snapshot().expect("re-dirtied map");
        retry.write()?;

        let reopened = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
        assert!(reopened.has_piece(&piece_key));
        Ok(())
    }

    /// A snapshot is published only when no newer one has been: the periodic
    /// flush runs outside the cache lock, so an abandoned engine thread's
    /// final write can land after the shutdown flush and must not roll the
    /// map back.
    #[test]
    fn test_older_snapshot_cannot_overwrite_a_newer_one() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let first = key("snap_order", 0);
        let path = cache.ensure_piece_dir(&first)?;
        std::fs::write(&path, vec![0u8; 10])?;
        cache.add_piece(&first, 10)?;
        let older = cache.take_dirty_snapshot().expect("dirty map");

        // The newer map no longer holds `first`, so the published file
        // identifies which snapshot won.
        let second = key("snap_order", 1);
        let path = cache.ensure_piece_dir(&second)?;
        std::fs::write(&path, vec![0u8; 10])?;
        cache.add_piece(&second, 10)?;
        cache.delete_piece(&first)?;
        let newer = cache.take_dirty_snapshot().expect("dirty map");

        // Publish out of order: the newer snapshot first, then the older one.
        newer.write()?;
        older.write()?;

        let stored = std::fs::read_to_string(temp_dir.path().join(CACHE_METADATA_FILE))?;
        assert!(
            stored.contains(second.as_str()),
            "the newer snapshot's piece is missing:\n{stored}"
        );
        assert!(
            !stored.contains(first.as_str()),
            "an older snapshot overwrote a newer one:\n{stored}"
        );
        Ok(())
    }

    /// Writers that publish concurrently must leave one complete file, with
    /// the newest snapshot as its content: scratch names are unique per write
    /// and the writer mutex serializes the renames.
    #[test]
    fn test_concurrent_writers_publish_the_newest_snapshot() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        let mut snapshots = Vec::new();
        for index in 0..4 {
            let piece_key = key("snap_race", index);
            let path = cache.ensure_piece_dir(&piece_key)?;
            std::fs::write(&path, vec![0u8; 10])?;
            cache.add_piece(&piece_key, 10)?;
            snapshots.push(cache.take_dirty_snapshot().expect("dirty map"));
        }

        let writers: Vec<_> = snapshots
            .into_iter()
            .map(|snapshot| std::thread::spawn(move || snapshot.write()))
            .collect();
        for writer in writers {
            writer.join().expect("writer thread panicked")?;
        }

        assert!(
            scratch_files(temp_dir.path()).is_empty(),
            "a writer left its scratch file behind"
        );

        // Only the newest snapshot holds `key("snap_race", 3)`.  The file must
        // be read directly: reopening a `CacheManager` rescans `pieces/` and
        // reports every piece even when an older snapshot won or the file was
        // discarded as malformed.
        let newest = key("snap_race", 3);
        let stored = std::fs::read_to_string(temp_dir.path().join(CACHE_METADATA_FILE))?;
        assert!(
            stored.lines().any(|line| line.starts_with(newest.as_str())),
            "the published metadata is not the newest snapshot:\n{stored}"
        );
        Ok(())
    }

    /// The periodic write must not hold the cache lock: serialization and
    /// fsync under it would stall every reader (`.stats`, the read path) for
    /// the whole rewrite at the engine's cadence.  Probe: while one thread
    /// writes a 100k-piece snapshot, another takes the cache lock in a loop
    /// and keeps the worst wait.
    #[test]
    fn test_snapshot_write_holds_no_cache_lock() -> TorrentResult<()> {
        use std::sync::Mutex;
        use std::time::Instant;

        let temp_dir = TempDir::new().unwrap();
        let cache = std::sync::Arc::new(Mutex::new(CacheManager::new(temp_dir.path(), u64::MAX)?));
        {
            let mut guard = cache.lock().unwrap();
            for index in 0..100_000 {
                guard.add_piece(&key("lockprobe", index), 1)?;
            }
        }

        let snapshot = cache
            .lock()
            .unwrap()
            .take_dirty_snapshot()
            .expect("dirty map");
        assert_eq!(snapshot.len(), 100_000);

        let started = Instant::now();
        let writer = std::thread::spawn(move || {
            let result = snapshot.write();
            (result, started.elapsed())
        });

        let mut worst_wait = Duration::ZERO;
        let mut probes = 0usize;
        while !writer.is_finished() && probes < 2_000_000 {
            let wait_start = Instant::now();
            let guard = cache.lock().unwrap();
            worst_wait = worst_wait.max(wait_start.elapsed());
            assert_eq!(guard.current_size(), 100_000);
            drop(guard);
            probes += 1;
            std::thread::yield_now();
        }

        let (result, write_time) = writer.join().expect("writer thread panicked");
        result?;
        assert!(probes > 0, "the probe never ran");
        // A write that held the cache lock would make one probe wait for the
        // whole rewrite; keep a wide margin for scheduler noise.
        assert!(
            worst_wait * 4 < write_time,
            "cache lock wait {worst_wait:?} is not far below the {write_time:?} write: \
             the rewrite is holding the cache lock"
        );
        Ok(())
    }

    // ── cache key whitelist (C-5.4.6) ─────────────────────────
    //
    // `cache_metadata.txt` is a plain text file an outside actor can edit, and
    // every key it holds becomes a path the daemon (often root) unlinks.  The
    // whitelist must hold on both sides: at load, and on every path
    // resolution.

    #[test]
    fn test_piece_key_whitelist_shape() {
        let v1 = hash("v1_hash");
        let v2 = "ab".repeat(32);
        assert!(is_valid_piece_key(&format!("{v1}:piece:0")));
        assert!(is_valid_piece_key(&format!("{v2}:piece:12345")));

        // Anything that is not exactly `{40|64 lowercase hex}:piece:{digits}`
        // is refused: escaping keys, uppercase hex, a missing or textual
        // index, and trailing content.
        for rejected in [
            "",
            "/etc/passwd",
            "../../etc/passwd",
            &format!("{v1}:piece:0/../../x"),
            &format!("{v1}:piece:"),
            &format!("{v1}:piece:-1"),
            &format!("{v1}:piece:0x10"),
            &format!("{v1}:piece:0:piece:1"),
            &format!("{v1}:piece:99999999999"),
            &format!("{}:piece:0", "AB".repeat(20)),
            &format!("{}:piece:0", "ab".repeat(19)),
            &format!("{}:piece:0", "ab".repeat(33)),
            &format!("{}:piece:0", "zz".repeat(20)),
            "hash1:piece:0",
        ] {
            assert!(!is_valid_piece_key(rejected), "accepted {rejected:?}");
        }
    }

    #[test]
    fn test_piece_path_refuses_escaping_keys() -> TorrentResult<()> {
        let temp_dir = TempDir::new().unwrap();
        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        for escaping in ["/etc/passwd", "../escape:piece:0", "hash1:piece:0"] {
            assert!(
                cache.piece_path(escaping).is_none(),
                "piece_path resolved {escaping:?}"
            );
            assert!(
                cache.ensure_piece_dir(escaping).is_err(),
                "ensure_piece_dir resolved {escaping:?}"
            );
            assert!(!cache.has_piece_on_disk(escaping));
        }
        Ok(())
    }

    #[test]
    fn test_injected_metadata_key_never_reaches_the_filesystem() -> TorrentResult<()> {
        // A row whose key is an absolute path must be dropped at load: it is
        // never registered, so neither the eviction pass nor a purge can
        // unlink the file it names.
        let cache_dir = TempDir::new().unwrap();
        let victim_dir = TempDir::new().unwrap();
        let victim = victim_dir.path().join("victim.txt");
        std::fs::write(&victim, b"do not delete me")?;

        let metadata_path = cache_dir.path().join(CACHE_METADATA_FILE);
        std::fs::write(
            &metadata_path,
            format!("{}\t0\t1\t0\n", victim.display()),
        )?;

        let mut cache = CacheManager::new(cache_dir.path(), 1)?;
        assert!(
            !cache.has_piece(&victim.display().to_string()),
            "an absolute-path key must not be registered"
        );

        // Force an eviction pass over the whole map and re-run a load, the two
        // places the injected key used to be honoured.
        cache.evict_lru()?;
        drop(cache);
        let _reopened = CacheManager::new(cache_dir.path(), 1)?;

        assert!(
            victim.exists(),
            "a key from the metadata file escaped the pieces directory"
        );

        // The rewritten file no longer carries the injected row.
        let stored = std::fs::read_to_string(&metadata_path)?;
        assert!(
            !stored.contains(&victim.display().to_string()),
            "the invalid row survived the rewrite:\n{stored}"
        );
        Ok(())
    }

    #[test]
    fn test_metadata_load_tolerates_bad_and_non_utf8_lines() -> TorrentResult<()> {
        // One unreadable line must not keep the engine from starting, and a
        // line that is not UTF-8 must be dropped like any other bad line
        // rather than failing the whole load.
        let temp_dir = TempDir::new().unwrap();
        let piece_key = key("tsi_loadtolerance", 0);
        let pieces_dir = temp_dir.path().join("pieces").join(hash("tsi_loadtolerance"));
        std::fs::create_dir_all(&pieces_dir)?;
        std::fs::write(pieces_dir.join(&piece_key), vec![0u8; 64])?;

        let mut raw = Vec::new();
        raw.extend_from_slice(b"not-a-pieces-line\n");
        raw.extend_from_slice(b"\xff\xfe\t0\t1\t0\n");
        raw.extend_from_slice(format!("{piece_key}\t1234\t64\t7\n").as_bytes());
        raw.extend_from_slice(b"\n");
        std::fs::write(temp_dir.path().join(CACHE_METADATA_FILE), &raw)?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert_eq!(
            cache.piece_count(),
            1,
            "only the well-formed row may be registered"
        );
        assert_eq!(cache.piece_metadata_size(&piece_key), Some(64));
        assert_eq!(cache.current_size(), 64);
        assert_eq!(cache.piece_hit_count(&piece_key), 7);
        Ok(())
    }

    #[test]
    fn test_scan_skips_files_outside_the_key_space() -> TorrentResult<()> {
        // The disk scan is the other place a key enters the cache: a file
        // whose name is not a piece key, or that sits in another info hash's
        // directory, is not a cache piece.
        let temp_dir = TempDir::new().unwrap();
        let own = key("tsi_scan_own", 0);
        let foreign = key("tsi_scan_foreign", 0);
        let own_dir = temp_dir.path().join("pieces").join(hash("tsi_scan_own"));
        let other_dir = temp_dir.path().join("pieces").join(hash("tsi_scan_other"));
        std::fs::create_dir_all(&own_dir)?;
        std::fs::create_dir_all(&other_dir)?;
        std::fs::write(own_dir.join(&own), vec![0u8; 10])?;
        std::fs::write(own_dir.join("garbage-name"), vec![0u8; 10])?;
        std::fs::write(other_dir.join(&foreign), vec![0u8; 10])?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(cache.has_piece(&own));
        assert!(!cache.has_piece(&foreign), "foreign file was registered");
        assert!(!cache.has_piece("garbage-name"));
        assert_eq!(cache.current_size(), 10);
        Ok(())
    }

    // ── startup reconciliation and eviction resilience ────────

    #[test]
    fn test_startup_drops_metadata_entries_without_files() -> TorrentResult<()> {
        // Metadata referencing a piece that is no longer on disk (an external
        // `rm`, a failed eviction, a crash) is dropped at startup: `has_piece`
        // must not claim it and the size accounting must follow the disk.
        let temp_dir = TempDir::new().unwrap();
        let gone_key = key("tsi_gone", 0);
        let kept_key = key("tsi_kept", 0);
        let kept_dir = temp_dir.path().join("pieces").join(hash("tsi_kept"));
        std::fs::create_dir_all(&kept_dir)?;
        std::fs::write(kept_dir.join(&kept_key), vec![0u8; 77])?;

        let metadata_path = temp_dir.path().join(CACHE_METADATA_FILE);
        std::fs::write(
            &metadata_path,
            format!("{gone_key}\t0\t100\t3\n{kept_key}\t1\t77\t1\n"),
        )?;

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;

        assert!(!cache.has_piece(&gone_key));
        assert!(cache.has_piece(&kept_key));
        assert_eq!(
            cache.current_size(),
            77,
            "current_size must count only what is still on disk"
        );
        assert_eq!(cache.piece_count(), 1);
        Ok(())
    }

    #[test]
    fn test_eviction_continues_past_a_delete_failure() -> TorrentResult<()> {
        // One undeletable piece must not pin eviction on the same victim
        // forever: the pass logs it, skips it and keeps reclaiming the rest.
        let temp_dir = TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 200)?;

        let stuck_key = key("tsi_stuck", 0);
        let stuck_path = cache.ensure_piece_dir(&stuck_key)?;
        // A directory at the piece path makes `remove_file` fail with EISDIR,
        // the same shape as a read-only mount or a permission error.
        std::fs::create_dir(&stuck_path)?;
        cache.add_piece(&stuck_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));
        let second_key = key("tsi_stuck", 1);
        let second_path = cache.ensure_piece_dir(&second_key)?;
        std::fs::write(&second_path, vec![0u8; 100])?;
        cache.add_piece(&second_key, 100)?;

        std::thread::sleep(std::time::Duration::from_millis(5));
        let third_key = key("tsi_stuck", 2);
        let third_path = cache.ensure_piece_dir(&third_key)?;
        std::fs::write(&third_path, vec![0u8; 100])?;
        // Pushes the accounted size to 300 > 200 with `stuck_key` oldest.
        cache.add_piece(&third_key, 100)?;

        assert!(
            cache.has_piece(&stuck_key),
            "the undeletable piece stays registered"
        );
        assert!(
            !cache.has_piece(&second_key),
            "eviction must move past the failed victim to the next oldest piece"
        );
        assert!(cache.has_piece(&third_key));
        assert!(cache.current_size() <= 200, "size must fall back under budget");
        assert_eq!(cache.eviction_count, 1);
        Ok(())
    }

    #[test]
    fn test_eviction_after_restart_uses_disk_state() -> TorrentResult<()> {
        // Restart with a piece file deleted behind the cache's back: the
        // reconciled map evicts from what is actually there instead of
        // selecting a piece that no longer exists (M9 + M10 together).
        let temp_dir = TempDir::new().unwrap();
        {
            let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024)?;
            for index in 0..2 {
                let piece_key = key("tsi_restart_evict", index);
                let path = cache.ensure_piece_dir(&piece_key)?;
                std::fs::write(&path, vec![0u8; 100])?;
                cache.add_piece(&piece_key, 100)?;
                std::thread::sleep(std::time::Duration::from_millis(5));
            }
            cache.flush()?;
        }

        std::fs::remove_file(
            temp_dir
                .path()
                .join("pieces")
                .join(hash("tsi_restart_evict"))
                .join(key("tsi_restart_evict", 0)),
        )?;

        let mut cache = CacheManager::new(temp_dir.path(), 150)?;
        assert_eq!(cache.current_size(), 100);
        assert!(!cache.has_piece(&key("tsi_restart_evict", 0)));

        let surviving = key("tsi_restart_evict", 1);
        let fresh_key = key("tsi_restart_evict", 2);
        let fresh_path = cache.ensure_piece_dir(&fresh_key)?;
        std::fs::write(&fresh_path, vec![0u8; 100])?;
        cache.add_piece(&fresh_key, 100)?;

        assert!(
            !cache.has_piece(&surviving),
            "the eviction must pick the real oldest piece, not the phantom entry"
        );
        assert!(cache.has_piece(&fresh_key));
        assert_eq!(cache.eviction_count, 1);
        assert!(cache.current_size() <= 150);
        Ok(())
    }
}

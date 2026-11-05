//! InodeManager — manages the in-memory inode tree for metadata/ and data/ subtrees.
//! Extracted from TorrentFs to separate inode management from FUSE protocol handling.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::db;
use tracing::info;

use super::fs_types::{Attr, FileKind};

// ── Inode constants ──
pub const ROOT_INO: u64 = 1;
pub const METADATA_INO: u64 = 2;
pub const DATA_INO: u64 = 3;
pub const STATS_INO: u64 = 4;
pub const MAX_TORRENT_SIZE: usize = 10 * 1024 * 1024;

// ── Data inode base addresses ──
//
// The inode space is split into disjoint bands, and every derived inode must
// land in exactly one of them:
//
//   [DATA_TORRENT_INO_BASE, DATA_TORRENT_INO_BASE + MAX_DATA_ROWS)  torrent roots
//   [DATA_DIR_INO_BASE,     ... + MAX_DATA_ROWS)                    torrent dirs
//   [DATA_FILE_INO_BASE,    ... + MAX_DATA_ROWS)                    torrent files
//   [SOURCE_PATH_DIR_INO_BASE, + HASH_INO_SLOTS)                    source-path dirs
//   [PENDING_TORRENT_INO_BASE, + HASH_INO_SLOTS)                    pending roots
//   [PENDING_TORRENT_DIR_INO_BASE, + HASH_INO_SLOTS)                pending dirs
//   [PENDING_TORRENT_FILE_INO_BASE, + HASH_INO_SLOTS)               pending files
//   [STATS_INO_OFFSET + data_ino, same span as the data bands)      `.stats` files
//
// The first three bands are encoded as `base + row_id` and are therefore bounded
// by the row cap: a row id at or beyond [`MAX_DATA_ROWS`] cannot be encoded and
// yields [`INVALID_INO`]. The next four are hash-derived and use a 2^48-slot
// band each — large enough that two real paths colliding (the birthday bound for
// thousands of directories is below 1e-8) is not a practical concern. `.stats`
// inodes are derived as `dir_ino + STATS_INO_OFFSET`, so the offset sits above
// every data band and its window spans the whole data-inode range.
pub const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
pub const DATA_DIR_INO_BASE: u64 = 2_000_000;
pub const DATA_FILE_INO_BASE: u64 = 3_000_000;
pub const SOURCE_PATH_DIR_INO_BASE: u64 = 1 << 48;
pub const PENDING_TORRENT_INO_BASE: u64 = SOURCE_PATH_DIR_INO_BASE + HASH_INO_SLOTS;
pub const PENDING_TORRENT_DIR_INO_BASE: u64 = PENDING_TORRENT_INO_BASE + HASH_INO_SLOTS;
pub const PENDING_TORRENT_FILE_INO_BASE: u64 = PENDING_TORRENT_DIR_INO_BASE + HASH_INO_SLOTS;

/// Base of the `.stats` band. Every stats inode is `dir_ino + STATS_INO_OFFSET`,
/// so the offset must exceed the largest data inode by at least the data span.
pub const STATS_INO_OFFSET: u64 = 1 << 62;

/// Width of the `.stats` band: it covers every possible `dir_ino` (all data
/// inodes are below it), so a derived stats inode never leaves the band.
pub const STATS_INO_SPAN: u64 = 1 << 51;

/// Returned instead of an out-of-range derived inode. `0` is not a valid FUSE
/// node id: `lookup` reports the entry as absent, and every other operation on
/// it ends in `ENOENT`. Rejecting at the derivation point keeps an oversized DB
/// row id from silently aliasing an unrelated inode (a `.stats` file, a
/// source-path directory, or a pending torrent).
pub const INVALID_INO: u64 = 0;

/// Row cap for the ID-derived data inode bands (`DATA_TORRENT` / `DATA_DIR` /
/// `DATA_FILE`), each encoded as `base + id` — one inode slot per DB row.
pub const MAX_DATA_ROWS: u64 = 1_000_000;

/// Slot count of every hash-derived band.
const HASH_INO_SLOTS: u64 = 1 << 48;

// ── Compile-time invariants ──
// The three row-encoded bands must not overlap each other, and the hash-derived
// bands must sit completely above the `.stats` window so a hash-derived inode
// can never be mistaken for a stats inode (`setattr`/`write` classify stats
// first). A future boundary crossing becomes a compile error.
const _: () = assert!(DATA_TORRENT_INO_BASE + MAX_DATA_ROWS <= DATA_DIR_INO_BASE);
const _: () = assert!(DATA_DIR_INO_BASE + MAX_DATA_ROWS <= DATA_FILE_INO_BASE);
const _: () = assert!(DATA_FILE_INO_BASE + MAX_DATA_ROWS <= SOURCE_PATH_DIR_INO_BASE);
const _: () = assert!(PENDING_TORRENT_FILE_INO_BASE + HASH_INO_SLOTS <= STATS_INO_SPAN);
const _: () = assert!(STATS_INO_OFFSET >= STATS_INO_SPAN);
const _: () = assert!(STATS_INO_OFFSET + STATS_INO_SPAN <= u64::MAX);
const _: () = assert!(PENDING_TORRENT_INO_BASE == SOURCE_PATH_DIR_INO_BASE + HASH_INO_SLOTS);
const _: () = assert!(PENDING_TORRENT_DIR_INO_BASE == PENDING_TORRENT_INO_BASE + HASH_INO_SLOTS);
const _: () = assert!(
    PENDING_TORRENT_FILE_INO_BASE == PENDING_TORRENT_DIR_INO_BASE + HASH_INO_SLOTS
);

pub static NEXT_INO: AtomicU64 = AtomicU64::new(5);
pub static NEXT_FH: AtomicU64 = AtomicU64::new(1);

// ── Inode data types ──

#[derive(Clone, Debug)]
pub enum InodeData {
    Directory {
        parent: u64,
        name: String,
    },
    File {
        parent: u64,
        name: String,
        data: Vec<u8>,
        /// `true` once `unlink` removed the directory entry.
        /// The inode + buffered data stay alive (readable/writable via
        /// still-open handles) until the last handle is `release`d, then
        /// the inode is destroyed. `find_child_by_name` / `readdir` skip
        /// unlinked inodes so the name is gone even though the inode
        /// lingers — matching FUSE "open-but-unlinked" semantics.
        unlinked: bool,
    },
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum DataInode {
    SourcePathDir {
        path: String,
    },
    TorrentRoot {
        torrent_id: i64,
        source_path: String,
        name: String,
        filename: String,
    },
    TorrentDir {
        torrent_id: i64,
        dir_id: i64,
        name: String,
        /// full path of this directory relative to the torrent
        /// root (e.g. `subdir/deeper`).  Empty for DB-backed dirs (which
        /// resolve via `dir_id`); set for pending torrent dirs so the
        /// bencode file list can be filtered by path.
        dir_path: String,
        /// the owning torrent's `(source_path, filename)`.
        /// For DB-backed dirs these are empty (resolved via
        /// `torrent_id` → DB).  For pending torrent dirs (`torrent_id ==
        /// 0`), they identify the `.torrent` metadata inode so the
        /// bencode file list can be re-parsed on lookup/readdir.
        torrent_source_path: String,
        torrent_filename: String,
    },
    TorrentFile {
        torrent_id: i64,
        file_id: i64,
        name: String,
        size: i64,
        /// the owning torrent's `(source_path,
        /// filename)`.  For DB-backed files these are empty (resolved
        /// via `torrent_id` → DB).  For pending torrent files
        /// (`torrent_id == 0`), they identify the `.torrent` metadata
        /// inode so stale entries can be evicted when the DB row lands.
        torrent_source_path: String,
        torrent_filename: String,
    },
}

// ── InodeManager ──

pub struct InodeManager {
    pub inodes: HashMap<u64, InodeData>,
    pub data_inodes: HashMap<u64, DataInode>,
    pub open_files: HashMap<u64, u64>,
    pub creation_time: Duration,
}

impl InodeManager {
    pub fn new(creation_time: Duration) -> Self {
        let mut inodes = HashMap::new();
        inodes.insert(
            ROOT_INO,
            InodeData::Directory {
                parent: 0,
                name: String::new(),
            },
        );
        inodes.insert(
            METADATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "metadata".to_string(),
            },
        );
        inodes.insert(
            DATA_INO,
            InodeData::Directory {
                parent: ROOT_INO,
                name: "data".to_string(),
            },
        );
        inodes.insert(
            STATS_INO,
            InodeData::File {
                parent: ROOT_INO,
                name: ".stats".to_string(),
                data: Vec::new(),
                unlinked: false,
            },
        );

        Self {
            inodes,
            data_inodes: HashMap::new(),
            open_files: HashMap::new(),
            creation_time,
        }
    }

    // ── Inode ID generators ──

    pub fn make_torrent_root_ino(torrent_id: i64) -> u64 {
        Self::row_inode(DATA_TORRENT_INO_BASE, "torrent root", torrent_id)
    }

    pub fn make_torrent_dir_ino(dir_id: i64) -> u64 {
        Self::row_inode(DATA_DIR_INO_BASE, "torrent directory", dir_id)
    }

    pub fn make_torrent_file_ino(file_id: i64) -> u64 {
        Self::row_inode(DATA_FILE_INO_BASE, "torrent file", file_id)
    }

    /// Encode a DB row id into its inode band, or [`INVALID_INO`] when the id
    /// cannot be represented.
    ///
    /// The guard is unconditional — unlike a `debug_assert!` it also runs in
    /// release builds, where an oversized id would otherwise be added to the
    /// base and land in an unrelated band: `DATA_FILE_INO_BASE + 4_000_000`
    /// renders a pending-torrent file, `+ 7_000_000` renders `.stats`.
    fn row_inode(base: u64, kind: &str, row_id: i64) -> u64 {
        if Self::is_valid_row_id(row_id) {
            base + row_id as u64
        } else {
            tracing::error!(
                "Cannot derive a {} inode from row id {} (valid range 0..{}); \
                 the row is not addressable and is reported as absent",
                kind,
                row_id,
                MAX_DATA_ROWS
            );
            INVALID_INO
        }
    }

    /// `true` if `id` is a valid DB row id for an ID-derived data inode
    /// (`base + id` stays inside its band).
    fn is_valid_row_id(id: i64) -> bool {
        id >= 0 && (id as u64) < MAX_DATA_ROWS
    }

    fn hash_inode(base: u64, parts: &[&str]) -> u64 {
        let mut hasher = DefaultHasher::new();
        for part in parts {
            part.hash(&mut hasher);
        }
        base + (hasher.finish() % HASH_INO_SLOTS)
    }

    pub fn make_source_path_dir_ino(path: &str) -> u64 {
        Self::hash_inode(SOURCE_PATH_DIR_INO_BASE, &[path])
    }

    /// deterministic inode for a pending torrent (background
    /// add_torrent in-flight, no DB row yet).  Hashes `(source_path,
    /// filename)` into the `PENDING_TORRENT_INO_BASE` band so each
    /// pending torrent gets a unique inode — two simultaneous cp's of
    /// different .torrent files must not collide.
    pub fn make_pending_torrent_ino(source_path: &str, filename: &str) -> u64 {
        Self::hash_inode(PENDING_TORRENT_INO_BASE, &[source_path, filename])
    }

    /// deterministic inode for a directory *inside* a pending
    /// torrent (e.g. `data/seed.torrent/subdir/`).  Hashes
    /// `(source_path, filename, dir_path)` into the
    /// `PENDING_TORRENT_DIR_INO_BASE` band so each pending directory
    /// gets a unique, stable inode across lookups and readdir.
    pub fn make_pending_torrent_dir_ino(source_path: &str, filename: &str, dir_path: &str) -> u64 {
        Self::hash_inode(
            PENDING_TORRENT_DIR_INO_BASE,
            &[source_path, filename, dir_path],
        )
    }

    /// deterministic inode for a file *inside* a pending
    /// torrent (e.g. `data/seed.torrent/subdir/file.txt`).  Hashes
    /// `(source_path, filename, file_path)` into the
    /// `PENDING_TORRENT_FILE_INO_BASE` band so each pending file gets a
    /// unique, stable inode.
    pub fn make_pending_torrent_file_ino(
        source_path: &str,
        filename: &str,
        file_path: &str,
    ) -> u64 {
        Self::hash_inode(
            PENDING_TORRENT_FILE_INO_BASE,
            &[source_path, filename, file_path],
        )
    }

    /// `true` for a derived `data/` inode.
    ///
    /// The `.stats` window is excluded: stats inodes are numerically above
    /// every data band but belong to the stats namespace, and callers classify
    /// them first (`setattr`, `write`). Keeping them out of `is_data_ino` makes
    /// `is_data_namespace` describe the read-only projection only.
    pub fn is_data_ino(ino: u64) -> bool {
        ino >= DATA_TORRENT_INO_BASE && !Self::is_stats_ino(ino)
    }

    /// Check if an inode belongs to the read-only `data/` namespace.
    ///
    /// This includes the `data/` root itself (DATA_INO) and all derived
    /// data inodes (torrent roots, directories, files, source-path dirs).
    /// Mutating operations on these inodes must return `EROFS` ("read-only
    /// file system"), not `ENOENT` or `EACCES` — the inode exists, it is
    /// just not writable.
    pub fn is_data_namespace(ino: u64) -> bool {
        ino == DATA_INO || Self::is_data_ino(ino)
    }

    /// Derive a stats inode from a directory inode.
    pub fn make_stats_ino(dir_ino: u64) -> u64 {
        dir_ino + STATS_INO_OFFSET
    }

    /// Check if an inode is a stats inode (derived from a directory inode).
    pub fn is_stats_ino(ino: u64) -> bool {
        ino >= STATS_INO_OFFSET && ino < STATS_INO_OFFSET + STATS_INO_SPAN
    }

    /// Get the parent directory inode from a stats inode.
    pub fn stats_ino_to_dir_ino(ino: u64) -> Option<u64> {
        if Self::is_stats_ino(ino) {
            Some(ino - STATS_INO_OFFSET)
        } else {
            None
        }
    }

    // ── Path resolution ──

    pub fn get_full_path(&self, ino: u64) -> String {
        let mut path_parts = Vec::new();
        let mut current_ino = ino;

        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, name } => {
                        if !name.is_empty() {
                            path_parts.push(name.clone());
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, name, .. } => {
                        path_parts.push(name.clone());
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }

        path_parts.reverse();
        path_parts.join("/")
    }

    pub fn is_metadata_child(&self, ino: u64) -> bool {
        if ino == METADATA_INO {
            return true;
        }

        let mut current_ino = ino;
        while current_ino != ROOT_INO && current_ino != 0 {
            if let Some(data) = self.inodes.get(&current_ino) {
                match data {
                    InodeData::Directory { parent, .. } => {
                        if *parent == METADATA_INO || current_ino == METADATA_INO {
                            return true;
                        }
                        current_ino = *parent;
                    }
                    InodeData::File { parent, .. } => {
                        current_ino = *parent;
                    }
                }
            } else {
                break;
            }
        }
        false
    }

    pub fn find_child_by_name(&self, parent: u64, name: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            match data {
                InodeData::Directory { parent: p, name: n } if *p == parent && n == name => {
                    return Some(*ino);
                }
                InodeData::File {
                    parent: p,
                    name: n,
                    unlinked,
                    ..
                } if *p == parent && n == name && !*unlinked => {
                    return Some(*ino);
                }
                _ => {}
            }
        }
        None
    }

    pub fn find_ino_by_full_path(&self, target_path: &str) -> Option<u64> {
        for (ino, data) in &self.inodes {
            let full_path = match data {
                InodeData::Directory { .. } | InodeData::File { .. } => self.get_full_path(*ino),
            };
            if full_path == target_path {
                return Some(*ino);
            }
        }
        None
    }

    pub fn extract_source_path(&self, parent: u64) -> String {
        if parent == METADATA_INO {
            return String::new();
        }

        let full_path = self.get_full_path(parent);
        if let Some(stripped) = full_path.strip_prefix("metadata/") {
            stripped.to_string()
        } else {
            full_path
        }
    }

    // ── Inode restoration from DB ──

    /// Restore metadata/ subdirectory inodes from the database on startup.
    pub fn restore_metadata_inodes(
        &mut self,
        dirs: Vec<(i64, Option<i64>, String, String)>,
        torrents: Vec<db::Torrent>,
    ) {
        let mut sorted_dirs = dirs;
        sorted_dirs.sort_by(|a, b| {
            let depth_a = a.3.matches('/').count();
            let depth_b = b.3.matches('/').count();
            depth_a.cmp(&depth_b)
        });

        let mut dir_id_to_ino: HashMap<i64, u64> = HashMap::new();

        for (db_id, parent_db_id, name, path) in &sorted_dirs {
            let parent_ino = if let Some(pid) = parent_db_id {
                *dir_id_to_ino.get(pid).unwrap_or(&METADATA_INO)
            } else {
                METADATA_INO
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            dir_id_to_ino.insert(*db_id, new_ino);

            self.inodes.insert(
                new_ino,
                InodeData::Directory {
                    parent: parent_ino,
                    name: name.clone(),
                },
            );

            info!(
                "Restored metadata inode {} for path '{}' (db_id={}, parent_ino={})",
                new_ino, path, db_id, parent_ino
            );
        }

        for torrent in &torrents {
            let parent_ino = if torrent.source_path.is_empty() {
                METADATA_INO
            } else {
                let full_source = format!("metadata/{}", torrent.source_path);
                self.find_ino_by_full_path(&full_source)
                    .unwrap_or(METADATA_INO)
            };

            let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
            let filename = if !torrent.filename.is_empty() {
                if torrent.filename.ends_with(".torrent") {
                    torrent.filename.clone()
                } else {
                    format!("{}.torrent", torrent.filename)
                }
            } else {
                if torrent.name.ends_with(".torrent") {
                    torrent.name.clone()
                } else {
                    format!("{}.torrent", torrent.name)
                }
            };
            self.inodes.insert(
                new_ino,
                InodeData::File {
                    parent: parent_ino,
                    name: filename,
                    data: torrent.torrent_data.clone().unwrap_or_default(),
                    unlinked: false,
                },
            );
        }
    }
    // ── Attribute helpers ──

    /// Directory attributes (uid/gid and timestamps are filled by the adapter).
    pub fn attr_for_dir(&self, ino: u64, writable: bool) -> Attr {
        Attr {
            ino,
            size: 0,
            kind: FileKind::Directory,
            perm: if writable { 0o755 } else { 0o555 },
            nlink: 2,
        }
    }

    /// File attributes (uid/gid and timestamps are filled by the adapter).
    pub fn attr_for_file(&self, ino: u64, size: u64) -> Attr {
        Attr {
            ino,
            size,
            kind: FileKind::RegularFile,
            perm: 0o444,
            nlink: 1,
        }
    }

    // ── Open-handle / unlink lifecycle helpers ──

    /// Count how many open file handles currently reference `ino`.
    /// `open_files` maps `fh -> ino`; this is the live reference count
    /// used to decide when an unlinked inode can finally be destroyed.
    pub fn open_handle_count(&self, ino: u64) -> usize {
        self.open_files
            .values()
            .filter(|open_ino| **open_ino == ino)
            .count()
    }

    /// `true` if `ino` is a `File` inode already unlinked but kept alive
    /// only by still-open handles.
    pub fn is_unlinked_file(&self, ino: u64) -> bool {
        matches!(
            self.inodes.get(&ino),
            Some(InodeData::File { unlinked: true, .. })
        )
    }

    /// Retire a `File` inode's directory entry.
    ///
    /// If no open handle references the inode, the inode is destroyed
    /// immediately. Otherwise it is marked `unlinked`: it stays alive
    /// (readable/writable) via its open handles, and the last `release`
    /// destroys it. The directory name disappears either way because
    /// [`find_child_by_name`](Self::find_child_by_name) skips unlinked
    /// inodes. Returns `true` if the inode was destroyed now.
    pub fn unlink_file(&mut self, ino: u64) -> bool {
        debug_assert!(
            matches!(self.inodes.get(&ino), Some(InodeData::File { .. })),
            "unlink_file called on non-file or unknown inode: {}",
            ino
        );
        let unlinked_already = self.is_unlinked_file(ino);
        if unlinked_already {
            // Already retired; nothing to do.
            return false;
        }
        let has_open = self.open_handle_count(ino) > 0;
        if has_open {
            if let Some(InodeData::File { unlinked, .. }) = self.inodes.get_mut(&ino) {
                *unlinked = true;
            }
            false
        } else {
            self.inodes.remove(&ino).is_some()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── stats ino derivation round-trip ──

    #[test]
    fn test_make_stats_ino_derives_from_dir_ino() {
        assert_eq!(
            InodeManager::make_stats_ino(DATA_INO),
            DATA_INO + STATS_INO_OFFSET
        );
        assert_eq!(
            InodeManager::make_stats_ino(ROOT_INO),
            ROOT_INO + STATS_INO_OFFSET
        );
        assert_eq!(
            InodeManager::make_stats_ino(METADATA_INO),
            METADATA_INO + STATS_INO_OFFSET
        );
    }

    #[test]
    fn test_is_stats_ino_true_for_derived_inos() {
        assert!(InodeManager::is_stats_ino(InodeManager::make_stats_ino(
            DATA_INO
        )));
        assert!(InodeManager::is_stats_ino(InodeManager::make_stats_ino(
            ROOT_INO
        )));
    }

    #[test]
    fn test_is_stats_ino_false_for_regular_inos() {
        assert!(!InodeManager::is_stats_ino(DATA_INO));
        assert!(!InodeManager::is_stats_ino(ROOT_INO));
        assert!(!InodeManager::is_stats_ino(METADATA_INO));
        assert!(!InodeManager::is_stats_ino(0));
    }

    #[test]
    fn test_is_stats_ino_boundaries() {
        // The window starts at STATS_INO_OFFSET and is STATS_INO_SPAN wide.
        assert!(InodeManager::is_stats_ino(STATS_INO_OFFSET));
        assert!(InodeManager::is_stats_ino(STATS_INO_OFFSET + STATS_INO_SPAN - 1));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET + STATS_INO_SPAN));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET - 1));
    }

    // ── ID-derived inode row-id bound ──

    #[test]
    fn test_is_valid_row_id_boundaries() {
        assert!(InodeManager::is_valid_row_id(0));
        assert!(InodeManager::is_valid_row_id(MAX_DATA_ROWS as i64 - 1));
        assert!(!InodeManager::is_valid_row_id(MAX_DATA_ROWS as i64));
        assert!(!InodeManager::is_valid_row_id(-1));
    }

    #[test]
    fn test_id_derived_inos_stay_inside_their_bands_at_row_cap() {
        // At the row cap each band still ends before the next one starts —
        // this is the invariant the compile-time asserts express.
        let max_id = MAX_DATA_ROWS as i64 - 1;
        let root = InodeManager::make_torrent_root_ino(max_id);
        let dir = InodeManager::make_torrent_dir_ino(max_id);
        let file = InodeManager::make_torrent_file_ino(max_id);
        assert!((DATA_TORRENT_INO_BASE..DATA_DIR_INO_BASE).contains(&root));
        assert!((DATA_DIR_INO_BASE..DATA_FILE_INO_BASE).contains(&dir));
        assert!((DATA_FILE_INO_BASE..SOURCE_PATH_DIR_INO_BASE).contains(&file));
        assert!(!InodeManager::is_stats_ino(file));
    }

    /// An out-of-range row id must not alias another namespace even in a
    /// release build (where the old `debug_assert!` was compiled out). Each
    /// derivation returns `INVALID_INO`, which no real inode occupies and
    /// which every lookup path reports as absent.
    #[test]
    fn test_row_derived_inodes_reject_out_of_range_ids() {
        for bad in [MAX_DATA_ROWS as i64, MAX_DATA_ROWS as i64 + 4_000_000, i64::MAX, -1] {
            assert_eq!(InodeManager::make_torrent_root_ino(bad), INVALID_INO);
            assert_eq!(InodeManager::make_torrent_dir_ino(bad), INVALID_INO);
            assert_eq!(InodeManager::make_torrent_file_ino(bad), INVALID_INO);
        }
        assert!(!InodeManager::is_data_ino(INVALID_INO));
        assert!(!InodeManager::is_stats_ino(INVALID_INO));
    }

    /// The source-path band must be wide enough that distinct real directory
    /// paths do not share an inode: the old 1_000_000-slot hash collided for
    /// about half the trees once ~1_200 directories existed, and a collision
    /// makes one directory resolve as another (or as `..`).
    #[test]
    fn test_source_path_inodes_are_distinct_for_many_paths() {
        let mut seen = std::collections::HashSet::new();
        for i in 0..10_000 {
            let path = format!("library/collection-{i}");
            let ino = InodeManager::make_source_path_dir_ino(&path);
            assert!(
                (SOURCE_PATH_DIR_INO_BASE
                    ..SOURCE_PATH_DIR_INO_BASE + HASH_INO_SLOTS)
                    .contains(&ino),
                "source-path inode {ino} escaped its band"
            );
            assert!(!InodeManager::is_stats_ino(ino));
            assert!(InodeManager::is_data_ino(ino));
            assert!(seen.insert(ino), "source-path inode collision at {path}");
        }
    }

    #[test]
    fn test_stats_ino_to_dir_ino_round_trip() {
        let dir_ino = DATA_INO;
        let stats_ino = InodeManager::make_stats_ino(dir_ino);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(stats_ino), Some(dir_ino));
    }

    #[test]
    fn test_stats_ino_to_dir_ino_none_for_regular_ino() {
        assert_eq!(InodeManager::stats_ino_to_dir_ino(DATA_INO), None);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(ROOT_INO), None);
        assert_eq!(InodeManager::stats_ino_to_dir_ino(0), None);
    }
}

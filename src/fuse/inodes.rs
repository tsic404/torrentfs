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
pub const DATA_TORRENT_INO_BASE: u64 = 1_000_000;
pub const DATA_DIR_INO_BASE: u64 = 2_000_000;
pub const DATA_FILE_INO_BASE: u64 = 3_000_000;
pub const SOURCE_PATH_DIR_INO_BASE: u64 = 4_000_000;
pub const PENDING_TORRENT_INO_BASE: u64 = 5_000_000;
pub const PENDING_TORRENT_DIR_INO_BASE: u64 = 6_000_000;
pub const PENDING_TORRENT_FILE_INO_BASE: u64 = 7_000_000;
pub const STATS_INO_OFFSET: u64 = 10_000_000;

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
        /// TSI-2234: `true` once `unlink` removed the directory entry.
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
        /// TSI-2448: full path of this directory relative to the torrent
        /// root (e.g. `subdir/deeper`).  Empty for DB-backed dirs (which
        /// resolve via `dir_id`); set for pending torrent dirs so the
        /// bencode file list can be filtered by path.
        dir_path: String,
        /// TSI-2448: the owning torrent's `(source_path, filename)`.
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
        /// TSI-2448 (review): the owning torrent's `(source_path,
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
        DATA_TORRENT_INO_BASE + (torrent_id as u64)
    }

    pub fn make_torrent_dir_ino(dir_id: i64) -> u64 {
        DATA_DIR_INO_BASE + (dir_id as u64)
    }

    pub fn make_torrent_file_ino(file_id: i64) -> u64 {
        DATA_FILE_INO_BASE + (file_id as u64)
    }

    pub fn make_source_path_dir_ino(path: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        path.hash(&mut hasher);
        SOURCE_PATH_DIR_INO_BASE + (hasher.finish() % 1_000_000)
    }

    /// TSI-2443: deterministic inode for a pending torrent (background
    /// add_torrent in-flight, no DB row yet).  Hashes `(source_path,
    /// filename)` into the `PENDING_TORRENT_INO_BASE` range so each
    /// pending torrent gets a unique inode — two simultaneous cp's of
    /// different .torrent files must not collide.
    pub fn make_pending_torrent_ino(source_path: &str, filename: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        source_path.hash(&mut hasher);
        filename.hash(&mut hasher);
        PENDING_TORRENT_INO_BASE + (hasher.finish() % 1_000_000)
    }

    /// TSI-2448: deterministic inode for a directory *inside* a pending
    /// torrent (e.g. `data/seed.torrent/subdir/`).  Hashes
    /// `(source_path, filename, dir_path)` into the
    /// `PENDING_TORRENT_DIR_INO_BASE` range so each pending directory
    /// gets a unique, stable inode across lookups and readdir.
    pub fn make_pending_torrent_dir_ino(source_path: &str, filename: &str, dir_path: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        source_path.hash(&mut hasher);
        filename.hash(&mut hasher);
        dir_path.hash(&mut hasher);
        PENDING_TORRENT_DIR_INO_BASE + (hasher.finish() % 1_000_000)
    }

    /// TSI-2448: deterministic inode for a file *inside* a pending
    /// torrent (e.g. `data/seed.torrent/subdir/file.txt`).  Hashes
    /// `(source_path, filename, file_path)` into the
    /// `PENDING_TORRENT_FILE_INO_BASE` range so each pending file gets a
    /// unique, stable inode.
    pub fn make_pending_torrent_file_ino(
        source_path: &str,
        filename: &str,
        file_path: &str,
    ) -> u64 {
        let mut hasher = DefaultHasher::new();
        source_path.hash(&mut hasher);
        filename.hash(&mut hasher);
        file_path.hash(&mut hasher);
        PENDING_TORRENT_FILE_INO_BASE + (hasher.finish() % 1_000_000)
    }

    pub fn is_data_ino(ino: u64) -> bool {
        ino >= DATA_TORRENT_INO_BASE
    }

    /// Check if an inode belongs to the read-only `data/` namespace.
    ///
    /// This includes the `data/` root itself (DATA_INO) and all derived
    /// data inodes (torrent roots, directories, files, source-path dirs).
    /// Mutating operations on these inodes must return `EROFS` ("read-only
    /// file system"), not `ENOENT` or `EACCES` — the inode exists, it is
    /// just not writable (TSI-2228).
    pub fn is_data_namespace(ino: u64) -> bool {
        ino == DATA_INO || Self::is_data_ino(ino)
    }

    /// Derive a stats inode from a directory inode.
    pub fn make_stats_ino(dir_ino: u64) -> u64 {
        dir_ino + STATS_INO_OFFSET
    }

    /// Check if an inode is a stats inode (derived from a directory inode).
    pub fn is_stats_ino(ino: u64) -> bool {
        ino >= STATS_INO_OFFSET && ino < STATS_INO_OFFSET + 10_000_000
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

    // ── Open-handle / unlink lifecycle helpers (TSI-2234) ──

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

    /// Retire a `File` inode's directory entry (TSI-2234).
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
        // STATS_INO_OFFSET (10_000_000) is included, STATS_INO_OFFSET + 10_000_000 is excluded
        assert!(InodeManager::is_stats_ino(STATS_INO_OFFSET));
        assert!(InodeManager::is_stats_ino(
            STATS_INO_OFFSET + 10_000_000 - 1
        ));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET + 10_000_000));
        assert!(!InodeManager::is_stats_ino(STATS_INO_OFFSET - 1));
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

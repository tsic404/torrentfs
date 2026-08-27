//! DataResolver — resolves FUSE lookup operations for the data/ subtree.
//! Extracted from TorrentFs to separate data tree resolution from the rest of FUSE handling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::db::Database;
use crate::domain::fs_error::{FsError, FsResult};
use crate::fuse::inodes::{DataInode, InodeManager};
use tracing::error;

use super::fs_types::FileKind;

pub struct DataResolver;

/// TSI-2448: alias for the readdir entry tuple returned by
/// `readdir_pending_torrent` — avoids clippy `type_complexity` lint.
type ReaddirEntries = (Vec<(u64, i64, FileKind, String)>, Vec<(u64, DataInode)>);

impl DataResolver {
    /// Resolve a child lookup within the data/ subtree.
    /// Returns (ino, DataInode) if found.
    pub fn resolve_data_lookup(
        inode_mgr: &InodeManager,
        db: &Arc<Mutex<Database>>,
        parent: u64,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        if parent == super::inodes::DATA_INO {
            return Self::resolve_data_root_lookup(db, name);
        }

        let data_inode = inode_mgr.data_inodes.get(&parent)?.clone();
        match data_inode {
            DataInode::SourcePathDir { path } => {
                Self::resolve_source_path_dir_lookup(db, &path, name)
            }
            DataInode::TorrentRoot {
                torrent_id,
                source_path,
                filename,
                ..
            } => {
                // TSI-2448: when torrent_id is 0 (pending — background
                // add_torrent in-flight, no DB row yet), resolve internal
                // files/dirs from the .torrent bencode instead of the DB.
                if torrent_id == 0 {
                    Self::resolve_pending_torrent_children(
                        inode_mgr,
                        &source_path,
                        &filename,
                        "",
                        name,
                    )
                } else {
                    Self::resolve_torrent_root_lookup(db, torrent_id, name)
                }
            }
            DataInode::TorrentDir {
                torrent_id,
                dir_id,
                dir_path,
                torrent_source_path,
                torrent_filename,
                ..
            } => {
                // TSI-2448: same pending fallback for subdirectories.
                if torrent_id == 0 {
                    Self::resolve_pending_torrent_children(
                        inode_mgr,
                        &torrent_source_path,
                        &torrent_filename,
                        &dir_path,
                        name,
                    )
                } else {
                    Self::resolve_torrent_dir_lookup(db, torrent_id, Some(dir_id), name)
                }
            }
            DataInode::TorrentFile { .. } => None,
        }
    }

    fn resolve_data_root_lookup(db: &Arc<Mutex<Database>>, name: &str) -> Option<(u64, DataInode)> {
        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes("").ok()?;
        if prefixes.contains(&name.to_string()) {
            let full_path = name.to_string();
            let ino = InodeManager::make_source_path_dir_ino(&full_path);
            return Some((ino, DataInode::SourcePathDir { path: full_path }));
        }

        let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
        for torrent in root_torrents {
            if torrent.filename == name {
                let ino = InodeManager::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    fn resolve_source_path_dir_lookup(
        db: &Arc<Mutex<Database>>,
        prefix: &str,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let new_path = if prefix.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", prefix, name)
        };

        let db_guard = db.lock().ok()?;

        let prefixes = db_guard.get_source_path_prefixes(prefix).ok()?;
        if prefixes.contains(&name.to_string()) {
            let ino = InodeManager::make_source_path_dir_ino(&new_path);
            return Some((ino, DataInode::SourcePathDir { path: new_path }));
        }

        let torrents = db_guard.get_torrents_by_source_path(prefix).ok()?;
        for torrent in torrents {
            if torrent.filename == name {
                let ino = InodeManager::make_torrent_root_ino(torrent.id);
                return Some((
                    ino,
                    DataInode::TorrentRoot {
                        torrent_id: torrent.id,
                        source_path: torrent.source_path.clone(),
                        name: torrent.name.clone(),
                        filename: torrent.filename.clone(),
                    },
                ));
            }
        }

        None
    }

    pub fn resolve_torrent_root_lookup(
        db: &Arc<Mutex<Database>>,
        torrent_id: i64,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        Self::resolve_torrent_dir_lookup(db, torrent_id, None, name)
    }

    pub fn resolve_torrent_dir_lookup(
        db: &Arc<Mutex<Database>>,
        torrent_id: i64,
        parent_dir_id: Option<i64>,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let db_guard = db.lock().ok()?;

        if let Some(dir) = db_guard
            .get_torrent_directory(torrent_id, parent_dir_id, name)
            .ok()?
        {
            let ino = InodeManager::make_torrent_dir_ino(dir.id);
            return Some((
                ino,
                DataInode::TorrentDir {
                    torrent_id,
                    dir_id: dir.id,
                    name: dir.name,
                    dir_path: String::new(),
                    torrent_source_path: String::new(),
                    torrent_filename: String::new(),
                },
            ));
        }

        let files = if let Some(pid) = parent_dir_id {
            db_guard.get_files_in_directory(pid).ok()?
        } else {
            db_guard.get_root_files(torrent_id).ok()?
        };

        for file in files {
            if file.name == name {
                let ino = InodeManager::make_torrent_file_ino(file.id);
                return Some((
                    ino,
                    DataInode::TorrentFile {
                        torrent_id,
                        file_id: file.id,
                        name: file.name,
                        size: file.size,
                        torrent_source_path: String::new(),
                        torrent_filename: String::new(),
                    },
                ));
            }
        }

        None
    }

    /// Lookup a data inode and cache it. Returns (ino, FileType, size).
    pub fn lookup_data_inode(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        parent: u64,
        name: &str,
    ) -> Option<(u64, FileKind, u64)> {
        // TSI-2443: try the DB first.  If it misses but a background
        // add_torrent is in-flight (processing_torrents has the key),
        // resolve a provisional TorrentRoot from the metadata inode so
        // the data/ mirror doesn't show ENOENT during the ~0.1-0.5s
        // window between release() returning and the DB insert landing.
        if let Some((ino, data_inode)) = Self::resolve_data_lookup(inode_mgr, db, parent, name) {
            inode_mgr.data_inodes.insert(ino, data_inode.clone());
            let kind_size = match &data_inode {
                DataInode::SourcePathDir { .. }
                | DataInode::TorrentRoot { .. }
                | DataInode::TorrentDir { .. } => (FileKind::Directory, 0u64),
                DataInode::TorrentFile { size, .. } => (FileKind::RegularFile, *size as u64),
            };
            return Some((ino, kind_size.0, kind_size.1));
        }

        // DB miss — check if a background add_torrent is pending for this
        // (source_path, filename).  Only torrent-root lookups (data/ root
        // and SourcePathDir parents) can be pending here; TorrentRoot /
        // TorrentDir / TorrentFile parents with torrent_id == 0 are
        // already handled by `resolve_data_lookup` above via the bencode
        // file-list fallback (TSI-2448).
        let source_path = if parent == super::inodes::DATA_INO {
            String::new()
        } else {
            match inode_mgr.data_inodes.get(&parent) {
                Some(DataInode::SourcePathDir { path }) => path.clone(),
                _ => return None,
            }
        };

        let key = (source_path.clone(), name.to_string());
        let pending = processing_torrents
            .lock()
            .is_ok_and(|g| g.contains_key(&key));
        if !pending {
            return None;
        }

        // The metadata .torrent file is still in the inode table; parse it
        // to get the torrent name for the TorrentRoot DataInode.
        let torrent_name = Self::parse_pending_torrent_name(inode_mgr, &source_path, name);
        let ino = InodeManager::make_pending_torrent_ino(&source_path, name);
        let data_inode = DataInode::TorrentRoot {
            torrent_id: 0,
            source_path,
            name: torrent_name,
            filename: name.to_string(),
        };
        inode_mgr.data_inodes.insert(ino, data_inode);
        Some((ino, FileKind::Directory, 0))
    }

    /// TSI-2443: scan the metadata inode table for a `.torrent` file at
    /// `(source_path, filename)` whose background add_torrent is pending.
    /// Returns the torrent's display name if found and parseable.
    ///
    /// Uses a lightweight bencode name extractor (`extract_bencode_name`)
    /// instead of `TorrentInfo::from_bytes` to avoid cloning the full
    /// torrent buffer (up to 10 MB) on every lookup/readdir.
    fn parse_pending_torrent_name(
        inode_mgr: &InodeManager,
        source_path: &str,
        filename: &str,
    ) -> String {
        for data in inode_mgr.inodes.values() {
            if let crate::fuse::inodes::InodeData::File {
                name,
                data: file_data,
                unlinked,
                parent,
            } = data
            {
                if *unlinked || name != filename || file_data.is_empty() {
                    continue;
                }
                // Verify the inode's source_path matches — the same
                // filename can exist in different metadata subdirectories.
                let inode_sp = inode_mgr.extract_source_path(*parent);
                if inode_sp != source_path {
                    continue;
                }
                if let Some(torrent_name) = extract_bencode_name(file_data) {
                    return torrent_name;
                }
            }
        }
        // Fallback: use the filename stem if the inode vanished or is
        // unparseable (e.g. the background thread already consumed it).
        filename.trim_end_matches(".torrent").to_string()
    }

    /// TSI-2448: scan the metadata inode table for the `.torrent` file
    /// at `(source_path, filename)` whose `add_torrent` is pending.
    /// Returns a reference to the raw torrent bytes if found.
    ///
    /// This is the same inode scan used by `parse_pending_torrent_name`
    /// but returns the full buffer so the bencode file list can be
    /// extracted for internal file/directory resolution.
    fn find_pending_torrent_data<'a>(
        inode_mgr: &'a InodeManager,
        source_path: &str,
        filename: &str,
    ) -> Option<&'a [u8]> {
        for data in inode_mgr.inodes.values() {
            if let crate::fuse::inodes::InodeData::File {
                name,
                data: file_data,
                unlinked,
                parent,
            } = data
            {
                if *unlinked || name != filename || file_data.is_empty() {
                    continue;
                }
                let inode_sp = inode_mgr.extract_source_path(*parent);
                if inode_sp != source_path {
                    continue;
                }
                return Some(file_data);
            }
        }
        None
    }

    /// TSI-2448: resolve a child (directory or file) inside a pending
    /// torrent root by parsing the bencode file list.
    ///
    /// `dir_path` is the directory path relative to the torrent root
    /// (empty for root-level lookups).  `name` is the child being
    /// looked up.  Returns a `DataInode` with a pending inode (stable
    /// hash of `(source_path, filename, child_path)`) if the child
    /// exists in the torrent's file structure.
    fn resolve_pending_torrent_children(
        inode_mgr: &InodeManager,
        source_path: &str,
        filename: &str,
        dir_path: &str,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let torrent_data = Self::find_pending_torrent_data(inode_mgr, source_path, filename)?;
        let (_torrent_name, files) = extract_bencode_files(torrent_data)?;

        // Build the full path prefix for this directory.
        let prefix = if dir_path.is_empty() {
            String::new()
        } else {
            format!("{}/", dir_path)
        };

        // Check if `name` is a subdirectory: any file whose path starts
        // with `prefix + name + "/"`.
        let sub_prefix = format!("{}{}/", prefix, name);
        let has_subdir = files.iter().any(|f| f.path.starts_with(&sub_prefix));
        if has_subdir {
            let child_path = if dir_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", dir_path, name)
            };
            let ino =
                InodeManager::make_pending_torrent_dir_ino(source_path, filename, &child_path);
            return Some((
                ino,
                DataInode::TorrentDir {
                    torrent_id: 0,
                    dir_id: 0,
                    name: name.to_string(),
                    dir_path: child_path,
                    torrent_source_path: source_path.to_string(),
                    torrent_filename: filename.to_string(),
                },
            ));
        }

        // Check if `name` is a file in this directory.
        let file_path = format!("{}{}", prefix, name);
        for file in &files {
            if file.path == file_path {
                let ino =
                    InodeManager::make_pending_torrent_file_ino(source_path, filename, &file_path);
                return Some((
                    ino,
                    DataInode::TorrentFile {
                        torrent_id: 0,
                        file_id: 0,
                        name: name.to_string(),
                        size: file.size as i64,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ));
            }
        }

        None
    }

    /// TSI-2443: collect pending torrent entries for a given `source_path`
    /// that have a background `add_torrent` in-flight but no DB row yet.
    /// Returns `(ino, DataInode, filename)` triples for injection into
    /// `readdir_data` listings alongside DB-sourced torrents.
    fn collect_pending_entries(
        inode_mgr: &InodeManager,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        source_path: &str,
        existing_filenames: &[&str],
    ) -> Vec<(u64, DataInode, String)> {
        let Ok(guard) = processing_torrents.lock() else {
            return Vec::new();
        };
        let mut result = Vec::new();
        for ((sp, filename), _) in guard.iter() {
            if sp != source_path {
                continue;
            }
            // Skip if the DB already has this filename — the DB row is
            // authoritative and already in the listing.
            if existing_filenames.contains(&filename.as_str()) {
                continue;
            }
            let torrent_name = Self::parse_pending_torrent_name(inode_mgr, sp, filename);
            let ino = InodeManager::make_pending_torrent_ino(sp, filename);
            let data_inode = DataInode::TorrentRoot {
                torrent_id: 0,
                source_path: sp.clone(),
                name: torrent_name,
                filename: filename.clone(),
            };
            result.push((ino, data_inode, filename.clone()));
        }
        result
    }

    /// TSI-2443 (review): evict stale pending entries from `data_inodes`
    /// whose `(source_path, filename)` now has a DB row.  Called after
    /// `readdir_data` lists DB-sourced torrents so the stale pending
    /// inodes don't linger past the 1s FUSE TTL.
    ///
    /// TSI-2448 (review): also evicts pending `TorrentDir` (6M range)
    /// and `TorrentFile` (7M range) entries cached during the pending
    /// window.  All three pending types (`TorrentRoot`, `TorrentDir`,
    /// `TorrentFile`) store `torrent_source_path`/`torrent_filename`,
    /// so eviction matches on those fields uniformly.
    fn evict_stale_pending(inode_mgr: &mut InodeManager, source_path: &str, db_filenames: &[&str]) {
        let db_fn_set: std::collections::HashSet<&str> = db_filenames.iter().copied().collect();

        // Collect all stale pending inodes in one pass: TorrentRoot (5M),
        // TorrentDir (6M), and TorrentFile (7M) whose torrent identity
        // matches a DB-landed (source_path, filename).
        let stale_inos: Vec<u64> = inode_mgr
            .data_inodes
            .iter()
            .filter_map(|(ino, data)| {
                let (tsp, tfn) = match data {
                    DataInode::TorrentRoot {
                        torrent_id: 0,
                        source_path,
                        filename,
                        ..
                    } => (source_path.as_str(), filename.as_str()),
                    DataInode::TorrentDir {
                        torrent_id: 0,
                        torrent_source_path,
                        torrent_filename,
                        ..
                    } => (torrent_source_path.as_str(), torrent_filename.as_str()),
                    DataInode::TorrentFile {
                        torrent_id: 0,
                        torrent_source_path,
                        torrent_filename,
                        ..
                    } => (torrent_source_path.as_str(), torrent_filename.as_str()),
                    _ => return None,
                };

                if tsp == source_path && db_fn_set.contains(tfn) {
                    Some(*ino)
                } else {
                    None
                }
            })
            .collect();

        for ino in stale_inos {
            inode_mgr.data_inodes.remove(&ino);
        }
    }

    /// TSI-2448: generate readdir entries for a pending torrent root or
    /// subdirectory by parsing the bencode file list instead of querying
    /// the DB.  `dir_path` is the directory path relative to the torrent
    /// root (empty string = root).  Returns `(entries, cache_entries)`
    /// so the caller can merge them into the `readdir_data` flow.
    ///
    /// The entry set includes `.` and `..` plus one entry per immediate
    /// child directory and file.  Directories are detected by scanning
    /// for files whose path has the directory as a prefix.
    fn readdir_pending_torrent(
        inode_mgr: &mut InodeManager,
        source_path: &str,
        filename: &str,
        dir_path: &str,
        _self_ino: u64,
    ) -> Option<ReaddirEntries> {
        let torrent_data = Self::find_pending_torrent_data(inode_mgr, source_path, filename)?;
        let (_name, files) = extract_bencode_files(torrent_data)?;

        let prefix = if dir_path.is_empty() {
            String::new()
        } else {
            format!("{}/", dir_path)
        };

        let mut entries: Vec<(u64, i64, FileKind, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();

        // Collect immediate children: directories (unique first-level
        // path components after the prefix) and files (exact prefix
        // match with no remaining `/`).
        let mut seen_dirs: std::collections::HashSet<String> = std::collections::HashSet::new();

        let mut offset_counter = 3i64;

        for file in &files {
            if !file.path.starts_with(&prefix) {
                continue;
            }
            let remainder = &file.path[prefix.len()..];
            if remainder.is_empty() {
                continue;
            }

            if let Some(slash_pos) = remainder.find('/') {
                // This file is inside a subdirectory — extract the dir name.
                let dir_name = &remainder[..slash_pos];
                if seen_dirs.insert(dir_name.to_string()) {
                    let child_dir_path = if dir_path.is_empty() {
                        dir_name.to_string()
                    } else {
                        format!("{}/{}", dir_path, dir_name)
                    };
                    let dir_ino = InodeManager::make_pending_torrent_dir_ino(
                        source_path,
                        filename,
                        &child_dir_path,
                    );
                    cache_entries.push((
                        dir_ino,
                        DataInode::TorrentDir {
                            torrent_id: 0,
                            dir_id: 0,
                            name: dir_name.to_string(),
                            dir_path: child_dir_path,
                            torrent_source_path: source_path.to_string(),
                            torrent_filename: filename.to_string(),
                        },
                    ));
                    entries.push((
                        dir_ino,
                        offset_counter,
                        FileKind::Directory,
                        dir_name.to_string(),
                    ));
                    offset_counter += 1;
                }
            } else {
                // A file directly in this directory.
                let file_name = remainder;
                let file_ino =
                    InodeManager::make_pending_torrent_file_ino(source_path, filename, &file.path);
                cache_entries.push((
                    file_ino,
                    DataInode::TorrentFile {
                        torrent_id: 0,
                        file_id: 0,
                        name: file_name.to_string(),
                        size: file.size as i64,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ));
                entries.push((
                    file_ino,
                    offset_counter,
                    FileKind::RegularFile,
                    file_name.to_string(),
                ));
                offset_counter += 1;
            }
        }

        Some((entries, cache_entries))
    }

    /// Generate readdir entries for a data/ inode.
    pub fn readdir_data(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        ino: u64,
        offset: i64,
    ) -> Option<Vec<(u64, i64, FileKind, String)>> {
        use super::inodes::{DATA_INO, ROOT_INO};

        let mut entries: Vec<(u64, i64, FileKind, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();

        if ino == DATA_INO {
            entries.push((DATA_INO, 1, FileKind::Directory, ".".to_string()));
            entries.push((ROOT_INO, 2, FileKind::Directory, "..".to_string()));

            {
                let db_guard = db.lock().ok()?;

                let mut offset_counter = 3i64;

                let root_torrents = db_guard.get_torrents_by_source_path("").ok()?;
                for torrent in root_torrents {
                    let torrent_ino = InodeManager::make_torrent_root_ino(torrent.id);
                    let name = torrent.filename.clone();
                    cache_entries.push((
                        torrent_ino,
                        DataInode::TorrentRoot {
                            torrent_id: torrent.id,
                            source_path: torrent.source_path.clone(),
                            name: torrent.name.clone(),
                            filename: torrent.filename.clone(),
                        },
                    ));
                    entries.push((torrent_ino, offset_counter, FileKind::Directory, name));
                    offset_counter += 1;
                }

                let prefixes = db_guard.get_source_path_prefixes("").ok()?;

                for prefix in prefixes {
                    let child_ino = InodeManager::make_source_path_dir_ino(&prefix);
                    cache_entries.push((
                        child_ino,
                        DataInode::SourcePathDir {
                            path: prefix.clone(),
                        },
                    ));
                    entries.push((child_ino, offset_counter, FileKind::Directory, prefix));
                    offset_counter += 1;
                }

                // Inject .stats virtual file for data/ directory
                let stats_ino = InodeManager::make_stats_ino(DATA_INO);
                entries.push((
                    stats_ino,
                    offset_counter,
                    FileKind::RegularFile,
                    ".stats".to_string(),
                ));
                // TSI-2443: inject pending torrents (background add_torrent
                // in-flight, no DB row yet) so readdir doesn't miss them.
                let existing_names: Vec<String> = entries
                    .iter()
                    .filter(|(_, _, k, _)| *k == FileKind::Directory)
                    .skip(2)
                    .map(|(_, _, _, n)| n.clone())
                    .collect();
                let existing_refs: Vec<&str> = existing_names.iter().map(|s| s.as_str()).collect();
                let pending = Self::collect_pending_entries(
                    inode_mgr,
                    processing_torrents,
                    "",
                    &existing_refs,
                );
                for (p_ino, p_inode, p_name) in pending {
                    cache_entries.push((p_ino, p_inode));
                    offset_counter += 1;
                    entries.push((p_ino, offset_counter, FileKind::Directory, p_name));
                }
                // TSI-2443 (review): evict stale pending inodes whose
                // DB rows have now landed.
                Self::evict_stale_pending(inode_mgr, "", &existing_refs);
            }

            for (cache_ino, cache_inode) in cache_entries {
                inode_mgr.data_inodes.insert(cache_ino, cache_inode);
            }

            return Some(
                entries
                    .into_iter()
                    .filter(|(_, o, _, _)| *o > offset)
                    .collect(),
            );
        }

        let data_inode = inode_mgr.data_inodes.get(&ino)?.clone();

        match data_inode {
            DataInode::SourcePathDir { path } => {
                entries.push((ino, 1, FileKind::Directory, ".".to_string()));

                let parent_ino = if path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        InodeManager::make_source_path_dir_ino(&parent_path)
                    }
                };
                entries.push((parent_ino, 2, FileKind::Directory, "..".to_string()));

                {
                    let db_guard = db.lock().ok()?;

                    let mut offset_counter = 3i64;

                    let sub_prefixes = db_guard.get_source_path_prefixes(&path).ok()?;
                    for sub in sub_prefixes {
                        let new_path = if path.is_empty() {
                            sub.clone()
                        } else {
                            format!("{}/{}", path, sub)
                        };
                        let child_ino = InodeManager::make_source_path_dir_ino(&new_path);
                        cache_entries.push((
                            child_ino,
                            DataInode::SourcePathDir {
                                path: new_path.clone(),
                            },
                        ));
                        entries.push((child_ino, offset_counter, FileKind::Directory, sub));
                        offset_counter += 1;
                    }

                    let direct_torrents = db_guard.get_torrents_by_source_path(&path).ok()?;
                    for torrent in direct_torrents {
                        let torrent_ino = InodeManager::make_torrent_root_ino(torrent.id);
                        let name = torrent.filename.clone();
                        cache_entries.push((
                            torrent_ino,
                            DataInode::TorrentRoot {
                                torrent_id: torrent.id,
                                source_path: torrent.source_path.clone(),
                                name: torrent.name.clone(),
                                filename: torrent.filename.clone(),
                            },
                        ));
                        entries.push((torrent_ino, offset_counter, FileKind::Directory, name));
                        offset_counter += 1;
                    }

                    // Inject .stats virtual file for source_path directory
                    let stats_ino = InodeManager::make_stats_ino(ino);
                    entries.push((
                        stats_ino,
                        offset_counter,
                        FileKind::RegularFile,
                        ".stats".to_string(),
                    ));
                    // TSI-2443: inject pending torrents for this source_path.
                    let existing_names: Vec<String> = entries
                        .iter()
                        .filter(|(_, _, k, _)| *k == FileKind::Directory)
                        .skip(2)
                        .map(|(_, _, _, n)| n.clone())
                        .collect();
                    let existing_refs: Vec<&str> =
                        existing_names.iter().map(|s| s.as_str()).collect();
                    let pending = Self::collect_pending_entries(
                        inode_mgr,
                        processing_torrents,
                        &path,
                        &existing_refs,
                    );
                    for (p_ino, p_inode, p_name) in pending {
                        cache_entries.push((p_ino, p_inode));
                        offset_counter += 1;
                        entries.push((p_ino, offset_counter, FileKind::Directory, p_name));
                    }
                    // TSI-2443 (review): evict stale pending inodes whose
                    // DB rows have now landed.
                    Self::evict_stale_pending(inode_mgr, &path, &existing_refs);
                }

                for (cache_ino, cache_inode) in cache_entries {
                    inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                }
            }
            DataInode::TorrentRoot {
                torrent_id,
                source_path,
                filename,
                ..
            } => {
                entries.push((ino, 1, FileKind::Directory, ".".to_string()));

                let parent_ino = if source_path.is_empty() {
                    DATA_INO
                } else {
                    let path_parts: Vec<&str> = source_path.split('/').collect();
                    if path_parts.len() == 1 {
                        DATA_INO
                    } else {
                        let parent_path = path_parts[..path_parts.len() - 1].join("/");
                        InodeManager::make_source_path_dir_ino(&parent_path)
                    }
                };
                entries.push((parent_ino, 2, FileKind::Directory, "..".to_string()));

                // TSI-2448: for pending torrents (torrent_id == 0),
                // list internal files/dirs from the bencode instead of
                // the DB.
                if torrent_id == 0 {
                    if let Some((mut p_entries, p_cache)) =
                        Self::readdir_pending_torrent(inode_mgr, &source_path, &filename, "", ino)
                    {
                        entries.append(&mut p_entries);

                        // Inject .stats virtual file for torrent root
                        let stats_ino = InodeManager::make_stats_ino(ino);
                        let next_offset = entries.last().map(|(_, o, _, _)| *o).unwrap_or(2) + 1;
                        entries.push((
                            stats_ino,
                            next_offset,
                            FileKind::RegularFile,
                            ".stats".to_string(),
                        ));

                        for (cache_ino, cache_inode) in p_cache {
                            inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                        }
                    }
                } else {
                    {
                        let db_guard = db.lock().ok()?;

                        let mut offset_counter = 3i64;

                        let root_dirs = db_guard
                            .get_torrent_directories_by_parent(None, torrent_id)
                            .ok()?;
                        for dir in root_dirs {
                            let dir_ino = InodeManager::make_torrent_dir_ino(dir.id);
                            cache_entries.push((
                                dir_ino,
                                DataInode::TorrentDir {
                                    torrent_id,
                                    dir_id: dir.id,
                                    name: dir.name.clone(),
                                    dir_path: String::new(),
                                    torrent_source_path: String::new(),
                                    torrent_filename: String::new(),
                                },
                            ));
                            entries.push((dir_ino, offset_counter, FileKind::Directory, dir.name));
                            offset_counter += 1;
                        }

                        let root_files = db_guard.get_root_files(torrent_id).ok()?;
                        for file in root_files {
                            let file_ino = InodeManager::make_torrent_file_ino(file.id);
                            cache_entries.push((
                                file_ino,
                                DataInode::TorrentFile {
                                    torrent_id,
                                    file_id: file.id,
                                    name: file.name.clone(),
                                    size: file.size,
                                    torrent_source_path: String::new(),
                                    torrent_filename: String::new(),
                                },
                            ));
                            entries.push((
                                file_ino,
                                offset_counter,
                                FileKind::RegularFile,
                                file.name,
                            ));
                            offset_counter += 1;
                        }

                        // Inject .stats virtual file for torrent root
                        let stats_ino = InodeManager::make_stats_ino(ino);
                        entries.push((
                            stats_ino,
                            offset_counter,
                            FileKind::RegularFile,
                            ".stats".to_string(),
                        ));
                    }

                    for (cache_ino, cache_inode) in cache_entries {
                        inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                    }
                }
            }
            DataInode::TorrentDir {
                torrent_id,
                dir_id,
                dir_path,
                torrent_source_path,
                torrent_filename,
                ..
            } => {
                entries.push((ino, 1, FileKind::Directory, ".".to_string()));

                // TSI-2448: for pending torrent dirs (torrent_id == 0),
                // list internal files/dirs from the bencode instead of
                // the DB.
                if torrent_id == 0 {
                    // For pending dirs, parent is the torrent root or
                    // a parent pending dir.  Compute `..` from the
                    // dir_path: if dir_path has no `/`, parent is the
                    // torrent root ino; otherwise it's the parent
                    // pending dir ino.
                    let parent_ino = if dir_path.is_empty() {
                        // Shouldn't happen — root is a TorrentRoot, not
                        // a TorrentDir — but handle defensively.
                        InodeManager::make_pending_torrent_ino(
                            &torrent_source_path,
                            &torrent_filename,
                        )
                    } else {
                        let parts: Vec<&str> = dir_path.rsplitn(2, '/').collect();
                        if parts.len() == 2 {
                            InodeManager::make_pending_torrent_dir_ino(
                                &torrent_source_path,
                                &torrent_filename,
                                parts[1],
                            )
                        } else {
                            InodeManager::make_pending_torrent_ino(
                                &torrent_source_path,
                                &torrent_filename,
                            )
                        }
                    };
                    entries.push((parent_ino, 2, FileKind::Directory, "..".to_string()));

                    if let Some((mut p_entries, p_cache)) = Self::readdir_pending_torrent(
                        inode_mgr,
                        &torrent_source_path,
                        &torrent_filename,
                        &dir_path,
                        ino,
                    ) {
                        entries.append(&mut p_entries);

                        for (cache_ino, cache_inode) in p_cache {
                            inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                        }
                    }
                } else {
                    {
                        let db_guard = db.lock().ok()?;

                        let parent_ino = db_guard
                            .get_torrent_directory_by_id(dir_id)
                            .ok()
                            .flatten()
                            .and_then(|d| d.parent_id)
                            .map(InodeManager::make_torrent_dir_ino)
                            .unwrap_or_else(|| InodeManager::make_torrent_root_ino(torrent_id));
                        entries.push((parent_ino, 2, FileKind::Directory, "..".to_string()));

                        let mut offset_counter = 3i64;

                        let sub_dirs = db_guard
                            .get_torrent_directories_by_parent(Some(dir_id), torrent_id)
                            .ok()?;
                        for dir in sub_dirs {
                            let sub_dir_ino = InodeManager::make_torrent_dir_ino(dir.id);
                            cache_entries.push((
                                sub_dir_ino,
                                DataInode::TorrentDir {
                                    torrent_id,
                                    dir_id: dir.id,
                                    name: dir.name.clone(),
                                    dir_path: String::new(),
                                    torrent_source_path: String::new(),
                                    torrent_filename: String::new(),
                                },
                            ));
                            entries.push((
                                sub_dir_ino,
                                offset_counter,
                                FileKind::Directory,
                                dir.name,
                            ));
                            offset_counter += 1;
                        }

                        let dir_files = db_guard.get_files_in_directory(dir_id).ok()?;
                        for file in dir_files {
                            let file_ino = InodeManager::make_torrent_file_ino(file.id);
                            cache_entries.push((
                                file_ino,
                                DataInode::TorrentFile {
                                    torrent_id,
                                    file_id: file.id,
                                    name: file.name.clone(),
                                    size: file.size,
                                    torrent_source_path: String::new(),
                                    torrent_filename: String::new(),
                                },
                            ));
                            entries.push((
                                file_ino,
                                offset_counter,
                                FileKind::RegularFile,
                                file.name,
                            ));
                            offset_counter += 1;
                        }
                    }

                    for (cache_ino, cache_inode) in cache_entries {
                        inode_mgr.data_inodes.insert(cache_ino, cache_inode);
                    }
                }
            }
            DataInode::TorrentFile { .. } => {
                return None;
            }
        }

        Some(
            entries
                .into_iter()
                .filter(|(_, o, _, _)| *o > offset)
                .collect(),
        )
    }

    /// Get the DB reference, returning a domain error on failure.
    pub fn get_db(db: &Option<Arc<Mutex<Database>>>) -> FsResult<&Arc<Mutex<Database>>> {
        db.as_ref().ok_or_else(|| {
            error!("Database not available");
            FsError::Internal("database not available".to_string())
        })
    }
}

/// TSI-2443 (review): extract the `name` field from a bencoded torrent
/// file without constructing a full `TorrentInfo`.  Avoids cloning the
/// entire torrent buffer (up to 10 MB) just to read the display name.
///
/// TSI-2448 (review): reimplemented with `BencodeParser` instead of
/// substring scanning — the old approach could match `4:name` inside the
/// `pieces` blob (raw SHA-1 hashes) and return garbage.  The recursive
/// parser navigates the `info` dict correctly.
fn extract_bencode_name(data: &[u8]) -> Option<String> {
    let (root, _) = BencodeParser::parse(data)?;
    let info = root.get_dict(b"info")?;
    info.get_str(b"name")
}

/// TSI-2448: a file entry extracted from the bencode of a pending
/// `.torrent` file.  `path` uses `/` as the path separator (matching
/// `FileInfo.path` from `TorrentInfo::files()`), and `size` is the file
/// size in bytes.  This is the lightweight pendant to `FileInfo` — no
/// FFI, no buffer ownership — used to populate the `data/` tree while
/// the background `add_torrent` DB insert is in-flight.
#[derive(Debug, Clone)]
struct PendingFile {
    path: String,
    size: u64,
}

/// TSI-2448: minimal bencode parser for extracting the file list from a
/// `.torrent` file without constructing a full `TorrentInfo` (which
/// requires FFI + buffer ownership).  The parser is intentionally
/// limited to what the data/ tree needs: the `info` dict's `name`,
/// `files` (multi-file), and `length` (single-file) keys.
///
/// Returns `(name, files)` where `files` is a list of `(path, size)`
/// pairs.  For a single-file torrent, `files` contains one entry whose
/// path is the torrent name.  Returns `None` if the bencode is
/// malformed.
fn extract_bencode_files(data: &[u8]) -> Option<(String, Vec<PendingFile>)> {
    let (root, _) = BencodeParser::parse(data)?;
    let info = root.get_dict(b"info")?;
    let name = info.get_str(b"name")?;

    if let Some(files) = info.get_list(b"files") {
        // Multi-file torrent
        let mut result = Vec::with_capacity(files.len());
        for entry in files {
            let length = entry.get_int(b"length")?;
            let path_list = entry.get_list(b"path")?;
            let mut parts = Vec::with_capacity(path_list.len());
            for part in path_list {
                parts.push(part.as_str()?);
            }
            result.push(PendingFile {
                path: parts.join("/"),
                size: length,
            });
        }
        Some((name, result))
    } else {
        // Single-file torrent: the file path is the torrent name itself
        info.get_int(b"length").map(|length| {
            (
                name.clone(),
                vec![PendingFile {
                    path: name,
                    size: length,
                }],
            )
        })
    }
}

/// Minimal bencode value for extracting torrent file lists.
enum Bencode {
    Str(Vec<u8>),
    Int(u64),
    List(Vec<Bencode>),
    Dict(Vec<(Vec<u8>, Bencode)>),
}

impl Bencode {
    fn as_str(&self) -> Option<String> {
        match self {
            Bencode::Str(v) => String::from_utf8(v.clone()).ok(),
            _ => None,
        }
    }

    fn as_dict(&self) -> Option<&Vec<(Vec<u8>, Bencode)>> {
        match self {
            Bencode::Dict(d) => Some(d),
            _ => None,
        }
    }

    fn get_dict(&self, key: &[u8]) -> Option<&Bencode> {
        let d = self.as_dict()?;
        d.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    fn get_str(&self, key: &[u8]) -> Option<String> {
        self.get_dict(key)?.as_str()
    }

    fn get_int(&self, key: &[u8]) -> Option<u64> {
        match self.get_dict(key)? {
            Bencode::Int(n) => Some(*n),
            _ => None,
        }
    }

    fn get_list(&self, key: &[u8]) -> Option<&Vec<Bencode>> {
        match self.get_dict(key)? {
            Bencode::List(l) => Some(l),
            _ => None,
        }
    }
}

/// Minimal recursive bencode parser.  Returns the parsed value and the
/// number of bytes consumed.  Only supports the four bencode types used
/// in torrent files: strings, integers, lists, and dictionaries.
struct BencodeParser;

impl BencodeParser {
    fn parse(data: &[u8]) -> Option<(Bencode, usize)> {
        if data.is_empty() {
            return None;
        }
        match data[0] {
            b'd' => Self::parse_dict(data),
            b'l' => Self::parse_list(data),
            b'i' => Self::parse_int(data),
            b'0'..=b'9' => Self::parse_str(data),
            _ => None,
        }
    }

    fn parse_dict(data: &[u8]) -> Option<(Bencode, usize)> {
        let mut pos = 1; // skip 'd'
        let mut entries = Vec::new();
        while pos < data.len() {
            if data[pos] == b'e' {
                return Some((Bencode::Dict(entries), pos + 1));
            }
            // Key must be a string
            let (key, consumed) = Self::parse_str(&data[pos..])?;
            pos += consumed;
            let key_bytes = match key {
                Bencode::Str(v) => v,
                _ => return None,
            };
            let (val, consumed) = Self::parse(&data[pos..])?;
            pos += consumed;
            entries.push((key_bytes, val));
        }
        None
    }

    fn parse_list(data: &[u8]) -> Option<(Bencode, usize)> {
        let mut pos = 1; // skip 'l'
        let mut items = Vec::new();
        while pos < data.len() {
            if data[pos] == b'e' {
                return Some((Bencode::List(items), pos + 1));
            }
            let (val, consumed) = Self::parse(&data[pos..])?;
            pos += consumed;
            items.push(val);
        }
        None
    }

    /// Parse a bencode integer (`i<digits>e`).
    ///
    /// Only non-negative integers (`u64`) are supported.  Bencode
    /// permits negative integers (e.g. `i-1e`), but torrent metadata
    /// only uses non-negative values (file sizes, piece lengths, etc.),
    /// so a negative integer will fail to parse and return `None` —
    /// which is the correct behavior for malformed torrent data.
    fn parse_int(data: &[u8]) -> Option<(Bencode, usize)> {
        let end = data[1..].iter().position(|&b| b == b'e')?;
        let s = std::str::from_utf8(&data[1..1 + end]).ok()?;
        let n: u64 = s.parse().ok()?;
        Some((Bencode::Int(n), 1 + end + 1))
    }

    fn parse_str(data: &[u8]) -> Option<(Bencode, usize)> {
        let colon = data.iter().position(|&b| b == b':')?;
        let len_str = std::str::from_utf8(&data[..colon]).ok()?;
        let len: usize = len_str.parse().ok()?;
        let start = colon + 1;
        let end = start.checked_add(len)?;
        if end > data.len() {
            return None;
        }
        Some((Bencode::Str(data[start..end].to_vec()), end))
    }
}

#[cfg(test)]
mod tests {
    use super::{DataInode, DataResolver, FileKind};
    use crate::db::{Database, InsertTorrentResult};
    use crate::fuse::inodes::{InodeManager, DATA_INO};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// Empty processing_torrents map for tests that don't exercise the
    /// pending-add fallback (TSI-2443).
    fn empty_pending() -> Arc<Mutex<HashMap<(String, String), ()>>> {
        Arc::new(Mutex::new(HashMap::new()))
    }

    /// Build a DB with a single torrent at the given source_path.
    fn db_with_torrent(source_path: &str) -> Arc<Mutex<Database>> {
        let mut db = Database::open_in_memory().unwrap();
        let result = db
            .insert_torrent(
                source_path,
                "test-torrent",
                "test.torrent",
                1024,
                "abc123",
                1,
            )
            .unwrap();
        assert!(matches!(result, InsertTorrentResult::Inserted(_)));
        Arc::new(Mutex::new(db))
    }

    /// Find the `..` entry's ino from a readdir result.
    fn dotdot_ino(entries: &[(u64, i64, FileKind, String)]) -> u64 {
        entries
            .iter()
            .find(|(_, _, _, name)| name == "..")
            .map(|(ino, _, _, _)| *ino)
            .expect("`..` entry not found")
    }

    /// TSI-2237: TorrentRoot `..` must point to the parent source-path
    /// directory inode, not an arbitrary sibling torrent root.
    ///
    /// source_path = "a/b" → `..` should be `make_source_path_dir_ino("a")`,
    /// matching the `..` returned by readdir on `SourcePathDir { path: "a" }`.
    #[test]
    fn torrent_root_dotdot_points_to_parent_source_path_dir() {
        let db = db_with_torrent("a/b");

        let torrent_ino = InodeManager::make_torrent_root_ino(1);

        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        inode_mgr.data_inodes.insert(
            torrent_ino,
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "a/b".to_string(),
                name: "test-torrent".to_string(),
                filename: "test.torrent".to_string(),
            },
        );

        let entries =
            DataResolver::readdir_data(&mut inode_mgr, &db, &empty_pending(), torrent_ino, 0)
                .expect("readdir TorrentRoot returned entries");

        let dotdot = dotdot_ino(&entries);
        let expected = InodeManager::make_source_path_dir_ino("a");

        assert_eq!(
            dotdot, expected,
            "TorrentRoot `..` should point to parent source-path dir inode"
        );

        // Cross-check: readdir on the parent SourcePathDir { path: "a" }
        // must yield the same inode for its `.` entry, proving the tree
        // is consistent in both directions.
        let parent_ino = InodeManager::make_source_path_dir_ino("a");
        let mut inode_mgr2 = InodeManager::new(Duration::from_secs(0));
        inode_mgr2.data_inodes.insert(
            parent_ino,
            DataInode::SourcePathDir {
                path: "a".to_string(),
            },
        );
        let parent_entries =
            DataResolver::readdir_data(&mut inode_mgr2, &db, &empty_pending(), parent_ino, 0)
                .expect("readdir SourcePathDir returned entries");
        let dot = parent_entries
            .iter()
            .find(|(_, _, _, name)| name == ".")
            .map(|(ino, _, _, _)| *ino)
            .expect("`.` entry not found");

        assert_eq!(dot, parent_ino, "SourcePathDir `.` is its own inode");
    }

    /// TSI-2237: when source_path is a single path segment (e.g. "a"),
    /// `..` should point to DATA_INO (the data/ root), not skip to a
    /// sibling torrent.
    #[test]
    fn torrent_root_dotdot_single_segment_points_to_data_ino() {
        let db = db_with_torrent("a");

        let torrent_ino = InodeManager::make_torrent_root_ino(1);
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        inode_mgr.data_inodes.insert(
            torrent_ino,
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "a".to_string(),
                name: "test-torrent".to_string(),
                filename: "test.torrent".to_string(),
            },
        );

        let entries =
            DataResolver::readdir_data(&mut inode_mgr, &db, &empty_pending(), torrent_ino, 0)
                .expect("readdir returned entries");

        assert_eq!(
            dotdot_ino(&entries),
            DATA_INO,
            "single-segment source_path `..` should be DATA_INO"
        );
    }

    /// TSI-2237: empty source_path (torrent at data/ root) → `..` is DATA_INO.
    #[test]
    fn torrent_root_dotdot_empty_source_path_points_to_data_ino() {
        let db = db_with_torrent("");

        let torrent_ino = InodeManager::make_torrent_root_ino(1);
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        inode_mgr.data_inodes.insert(
            torrent_ino,
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: String::new(),
                name: "test-torrent".to_string(),
                filename: "test.torrent".to_string(),
            },
        );

        let entries =
            DataResolver::readdir_data(&mut inode_mgr, &db, &empty_pending(), torrent_ino, 0)
                .expect("readdir returned entries");

        assert_eq!(
            dotdot_ino(&entries),
            DATA_INO,
            "empty source_path `..` should be DATA_INO"
        );
    }

    /// TSI-2237: multiple torrents sharing a parent source_path — `..`
    /// from any of their roots must resolve to the same parent dir
    /// inode (the source-path dir), not to torrents[0]'s root.
    #[test]
    fn torrent_root_dotdot_shared_parent_is_stable_across_torrents() {
        let mut db = Database::open_in_memory().unwrap();
        db.insert_torrent("a/b", "t1", "t1.torrent", 1024, "h1", 1)
            .unwrap();
        db.insert_torrent("a/b", "t2", "t2.torrent", 2048, "h2", 1)
            .unwrap();
        let db = Arc::new(Mutex::new(db));

        let expected_parent = InodeManager::make_source_path_dir_ino("a");

        for (tid, name) in [(1, "t1"), (2, "t2")] {
            let torrent_ino = InodeManager::make_torrent_root_ino(tid);
            let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
            inode_mgr.data_inodes.insert(
                torrent_ino,
                DataInode::TorrentRoot {
                    torrent_id: tid,
                    source_path: "a/b".to_string(),
                    name: name.to_string(),
                    filename: format!("{}.torrent", name),
                },
            );

            let entries =
                DataResolver::readdir_data(&mut inode_mgr, &db, &empty_pending(), torrent_ino, 0)
                    .expect("readdir returned entries");

            assert_eq!(
                dotdot_ino(&entries),
                expected_parent,
                "torrent {} `..` should point to parent source-path dir, not a sibling",
                name
            );
        }
    }

    // ── TSI-2443: pending add_torrent ENOENT window ──────────────────────

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

    /// TSI-2443: when a background `add_torrent` is pending (the `.torrent`
    /// file was written and `release` spawned the insert), `data/` lookup
    /// must NOT return ENOENT.  It should resolve a provisional TorrentRoot
    /// from the metadata inode table so the mirror is visible during the
    /// ~0.1-0.5s window before the DB row lands.
    #[test]
    fn lookup_data_root_returns_pending_torrent_instead_of_enoent() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Simulate a `.torrent` file written to metadata/ (root, source_path="")
        // but not yet in the DB — exactly the window between release()
        // spawning add_torrent and the DB insert committing.
        let ino = crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            ino,
            crate::fuse::inodes::InodeData::File {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "pending.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Mark the torrent as pending in processing_torrents.
        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert((String::new(), "pending.torrent".to_string()), ());

        // lookup on data/ for "pending.torrent" must return a TorrentRoot,
        // not None (ENOENT).
        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            DATA_INO,
            "pending.torrent",
        );
        let (ino, kind, size) = result.expect("pending torrent should resolve, not ENOENT");
        assert_eq!(kind, FileKind::Directory);
        assert_eq!(size, 0);

        // The data_inodes cache should have a TorrentRoot entry.
        let cached = inode_mgr.data_inodes.get(&ino).expect("cached TorrentRoot");
        match cached {
            DataInode::TorrentRoot {
                torrent_id,
                filename,
                name,
                ..
            } => {
                assert_eq!(*torrent_id, 0, "pending torrent uses sentinel id 0");
                assert_eq!(filename, "pending.torrent");
                assert_eq!(name, "foo", "name parsed from torrent metadata");
            }
            other => panic!("expected TorrentRoot, got {:?}", other),
        }
    }

    /// TSI-2443: when no background `add_torrent` is pending, the data/
    /// lookup still returns None (ENOENT) for a non-existent torrent —
    /// the fallback only applies when `processing_torrents` has the key.
    #[test]
    fn lookup_data_root_returns_none_when_not_pending() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        let pending = empty_pending();

        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            DATA_INO,
            "nonexistent.torrent",
        );
        assert!(
            result.is_none(),
            "non-pending missing torrent should be ENOENT"
        );
    }

    /// TSI-2443: `readdir` on `data/` must include pending torrents so
    /// `ls data/` shows them during the add_torrent window.
    #[test]
    fn readdir_data_root_includes_pending_torrent() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write a .torrent file to the metadata inode table.
        let ino = crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            ino,
            crate::fuse::inodes::InodeData::File {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "pending.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert((String::new(), "pending.torrent".to_string()), ());

        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, DATA_INO, 0)
            .expect("readdir data/ returned entries");

        // Find "pending.torrent" in the listing.
        let found = entries
            .iter()
            .any(|(_, _, _, name)| name == "pending.torrent");
        assert!(found, "readdir data/ must include pending torrent");
    }

    /// TSI-2443 (review): two pending torrents with different filenames in
    /// the same source_path must get distinct inodes — no collision.
    #[test]
    fn multiple_pending_torrents_get_distinct_inodes() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write two .torrent files to metadata/ root.
        for fname in ["alpha.torrent", "beta.torrent"] {
            let ino =
                crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            inode_mgr.inodes.insert(
                ino,
                crate::fuse::inodes::InodeData::File {
                    parent: crate::fuse::inodes::METADATA_INO,
                    name: fname.to_string(),
                    data: minimal_torrent_bytes(),
                    unlinked: false,
                },
            );
        }

        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert((String::new(), "alpha.torrent".to_string()), ());
        pending
            .lock()
            .unwrap()
            .insert((String::new(), "beta.torrent".to_string()), ());

        // lookup both — must return different inos.
        let (ino_a, _, _) = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            DATA_INO,
            "alpha.torrent",
        )
        .expect("alpha pending should resolve");
        let (ino_b, _, _) = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            DATA_INO,
            "beta.torrent",
        )
        .expect("beta pending should resolve");
        assert_ne!(ino_a, ino_b, "pending torrents must have distinct inodes");

        // readdir must show both, with distinct inos.
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, DATA_INO, 0)
            .expect("readdir data/ returned entries");
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(names.contains(&"alpha.torrent"), "readdir includes alpha");
        assert!(names.contains(&"beta.torrent"), "readdir includes beta");

        // No duplicate inos in the listing.
        let mut inos: Vec<u64> = entries.iter().map(|(i, _, _, _)| *i).collect();
        inos.sort_unstable();
        let before = inos.len();
        inos.dedup();
        assert_eq!(inos.len(), before, "readdir must not have duplicate inodes");
    }

    /// TSI-2443 (review): pending torrents under a SourcePathDir (not the
    /// data/ root) must be injected into that directory's readdir listing
    /// and resolve via lookup — the same ENOENT fix must work for
    /// `data/subdir/pending.torrent`, not just `data/pending.torrent`.
    #[test]
    fn source_path_dir_readdir_and_lookup_include_pending_torrent() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Create a metadata subdirectory "sub" so source_path = "sub".
        let dir_ino =
            crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            dir_ino,
            crate::fuse::inodes::InodeData::Directory {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "sub".to_string(),
            },
        );

        // Write a .torrent file inside metadata/sub/.
        let file_ino =
            crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            file_ino,
            crate::fuse::inodes::InodeData::File {
                parent: dir_ino,
                name: "pending.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Set up the SourcePathDir in data_inodes so readdir_data can
        // resolve the parent.
        let sp_ino = InodeManager::make_source_path_dir_ino("sub");
        inode_mgr.data_inodes.insert(
            sp_ino,
            DataInode::SourcePathDir {
                path: "sub".to_string(),
            },
        );

        // Mark the torrent as pending.
        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert(("sub".to_string(), "pending.torrent".to_string()), ());

        // lookup on data/sub/ for "pending.torrent" must resolve.
        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            sp_ino,
            "pending.torrent",
        );
        let (lookup_ino, kind, _) = result.expect("SourcePathDir pending should resolve");
        assert_eq!(kind, FileKind::Directory);

        // readdir on the SourcePathDir must include "pending.torrent".
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, sp_ino, 0)
            .expect("readdir SourcePathDir returned entries");
        let found = entries.iter().any(|(_, _, _, n)| n == "pending.torrent");
        assert!(found, "readdir SourcePathDir must include pending torrent");

        // The ino from readdir must match the ino from lookup.
        let readdir_ino = entries
            .iter()
            .find(|(_, _, _, n)| n == "pending.torrent")
            .map(|(i, _, _, _)| *i)
            .expect("pending entry in readdir");
        assert_eq!(
            lookup_ino, readdir_ino,
            "lookup and readdir inos must match"
        );
    }

    /// TSI-2443 (review): stale pending TorrentRoot(id=0) entries are
    /// evicted from data_inodes once the DB row lands.  After the DB
    /// insert completes, a readdir must not leave the stale pending ino
    /// in the cache.
    #[test]
    fn stale_pending_torrent_evicted_after_db_row_lands() {
        // DB with a torrent that already landed (simulating add_torrent
        // having completed).
        let db = db_with_torrent("");
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write the .torrent file to the metadata inode table.
        let ino = crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            ino,
            crate::fuse::inodes::InodeData::File {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "test.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Simulate a stale pending entry: the processing_torrents map is
        // now empty (add_torrent completed), but a stale TorrentRoot(id=0)
        // is still cached from the previous lookup.
        let pending = empty_pending();
        let stale_ino = InodeManager::make_pending_torrent_ino("", "test.torrent");
        inode_mgr.data_inodes.insert(
            stale_ino,
            DataInode::TorrentRoot {
                torrent_id: 0,
                source_path: String::new(),
                name: "test-torrent".to_string(),
                filename: "test.torrent".to_string(),
            },
        );

        // readdir on data/ — must list the DB torrent and evict the stale
        // pending entry.
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, DATA_INO, 0)
            .expect("readdir data/ returned entries");
        assert!(
            entries.iter().any(|(_, _, _, n)| n == "test.torrent"),
            "DB torrent must be in listing"
        );

        // The stale pending ino must have been evicted from data_inodes.
        assert!(
            !inode_mgr.data_inodes.contains_key(&stale_ino),
            "stale pending entry must be evicted after DB row lands"
        );
    }

    // ── TSI-2448: pending torrent internal file resolution ─────────────

    /// Minimal multi-file bencode with a directory structure:
    /// `foo` (root name) containing `dir/sub.txt` and `dir2/readme.txt`
    /// and a root-level `top.txt`.
    fn multifile_torrent_bytes() -> Vec<u8> {
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        // files list
        t.extend_from_slice(b"5:filesl");

        // dir/sub.txt — size 10
        t.push(b'd');
        t.extend_from_slice(b"6:lengthi10e");
        t.extend_from_slice(b"4:pathl3:dir7:sub.txte");
        t.push(b'e');

        // dir2/readme.txt — size 5
        t.push(b'd');
        t.extend_from_slice(b"6:lengthi5e");
        t.extend_from_slice(b"4:pathl4:dir210:readme.txte");
        t.push(b'e');

        // top.txt — size 8
        t.push(b'd');
        t.extend_from_slice(b"6:lengthi8e");
        t.extend_from_slice(b"4:pathl7:top.txte");
        t.push(b'e');

        t.extend_from_slice(b"e"); // end files list

        t.extend_from_slice(b"4:name3:foo");
        t.extend_from_slice(b"12:piece lengthi16384e");
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&[0u8; 20]);
        t.extend_from_slice(b"ee"); // end info dict + root dict
        t
    }

    /// Set up a pending torrent: write the .torrent to the metadata inode
    /// table, insert a pending TorrentRoot in data_inodes, and mark the
    /// key as pending in processing_torrents.
    fn setup_pending_multifile(
        source_path: &str,
        filename: &str,
    ) -> (
        InodeManager,
        Arc<Mutex<Database>>,
        Arc<Mutex<HashMap<(String, String), ()>>>,
        u64,
    ) {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write the .torrent file to the metadata inode table.
        let file_ino =
            crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let parent_ino = if source_path.is_empty() {
            crate::fuse::inodes::METADATA_INO
        } else {
            // Create the metadata subdirectory chain.
            let parts: Vec<&str> = source_path.split('/').collect();
            let mut current_parent = crate::fuse::inodes::METADATA_INO;
            for part in parts {
                let dir_ino =
                    crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                inode_mgr.inodes.insert(
                    dir_ino,
                    crate::fuse::inodes::InodeData::Directory {
                        parent: current_parent,
                        name: part.to_string(),
                    },
                );
                current_parent = dir_ino;
            }
            current_parent
        };
        inode_mgr.inodes.insert(
            file_ino,
            crate::fuse::inodes::InodeData::File {
                parent: parent_ino,
                name: filename.to_string(),
                data: multifile_torrent_bytes(),
                unlinked: false,
            },
        );

        // Mark as pending.
        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert((source_path.to_string(), filename.to_string()), ());

        // Resolve the pending torrent root via lookup_data_inode.
        let parent_data_ino = if source_path.is_empty() {
            DATA_INO
        } else {
            InodeManager::make_source_path_dir_ino(source_path)
        };
        // If there's a source_path, insert the SourcePathDir in data_inodes.
        if !source_path.is_empty() {
            inode_mgr.data_inodes.insert(
                parent_data_ino,
                DataInode::SourcePathDir {
                    path: source_path.to_string(),
                },
            );
        }
        let (root_ino, _, _) = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            parent_data_ino,
            filename,
        )
        .expect("pending torrent root should resolve");

        (inode_mgr, db, pending, root_ino)
    }

    /// TSI-2448: readdir on a pending torrent root must list internal
    /// files and directories from the bencode — not return empty.
    #[test]
    fn pending_torrent_root_readdir_lists_internal_files() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, root_ino, 0)
            .expect("readdir pending root returned entries");

        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(
            names.contains(&"dir"),
            "readdir should list directory 'dir'"
        );
        assert!(
            names.contains(&"dir2"),
            "readdir should list directory 'dir2'"
        );
        assert!(
            names.contains(&"top.txt"),
            "readdir should list root-level file 'top.txt'"
        );
    }

    /// TSI-2448: lookup on a pending torrent root for an internal file
    /// must resolve to a TorrentFile, not ENOENT.
    #[test]
    fn pending_torrent_root_lookup_resolves_internal_file() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "top.txt");
        let (ino, kind, size) = result.expect("pending root lookup for top.txt should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 8, "top.txt size should be 8");

        // Verify the cached DataInode is a TorrentFile.
        let cached = inode_mgr.data_inodes.get(&ino).expect("cached TorrentFile");
        match cached {
            DataInode::TorrentFile { name, size, .. } => {
                assert_eq!(name, "top.txt");
                assert_eq!(*size, 8);
            }
            other => panic!("expected TorrentFile, got {:?}", other),
        }
    }

    /// TSI-2448: lookup on a pending torrent root for an internal
    /// directory must resolve to a TorrentDir, not ENOENT.
    #[test]
    fn pending_torrent_root_lookup_resolves_internal_dir() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "dir");
        let (ino, kind, _) = result.expect("pending root lookup for 'dir' should resolve");
        assert_eq!(kind, FileKind::Directory);

        // Verify the cached DataInode is a pending TorrentDir.
        let cached = inode_mgr.data_inodes.get(&ino).expect("cached TorrentDir");
        match cached {
            DataInode::TorrentDir {
                dir_path,
                torrent_source_path,
                torrent_filename,
                ..
            } => {
                assert_eq!(dir_path, "dir");
                assert_eq!(torrent_source_path, "");
                assert_eq!(torrent_filename, "multi.torrent");
            }
            other => panic!("expected TorrentDir, got {:?}", other),
        }
    }

    /// TSI-2448: readdir on a pending torrent subdirectory must list its
    /// contents from the bencode.
    #[test]
    fn pending_torrent_dir_readdir_lists_contents() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        // Lookup the 'dir' subdirectory.
        let (dir_ino, _, _) =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "dir")
                .expect("lookup 'dir' should resolve");

        // readdir on the subdirectory should list 'sub.txt'.
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, dir_ino, 0)
            .expect("readdir pending dir returned entries");
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(
            names.contains(&"sub.txt"),
            "readdir on 'dir' should list 'sub.txt'"
        );
    }

    /// TSI-2448: lookup on a pending torrent subdirectory for a file
    /// must resolve, not ENOENT.
    #[test]
    fn pending_torrent_dir_lookup_resolves_file() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        // Lookup the 'dir' subdirectory.
        let (dir_ino, _, _) =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "dir")
                .expect("lookup 'dir' should resolve");

        // Lookup 'sub.txt' inside 'dir'.
        let result =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, dir_ino, "sub.txt");
        let (_, kind, size) = result.expect("lookup 'sub.txt' in 'dir' should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 10, "sub.txt size should be 10");
    }

    /// TSI-2448: pending internal file resolution works under a
    /// SourcePathDir (not just data/ root).
    #[test]
    fn pending_torrent_under_source_path_dir_resolves_internal() {
        let (mut inode_mgr, db, pending, root_ino) =
            setup_pending_multifile("sub", "multi.torrent");

        // readdir on the pending root should list internal entries.
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, root_ino, 0)
            .expect("readdir pending root under subdir returned entries");
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(
            names.contains(&"dir"),
            "readdir should list directory 'dir' under source_path"
        );
        assert!(
            names.contains(&"top.txt"),
            "readdir should list 'top.txt' under source_path"
        );

        // lookup for an internal file should resolve.
        let result =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "top.txt");
        let (_, kind, size) = result.expect("lookup 'top.txt' under source_path should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 8);
    }

    /// TSI-2448: single-file pending torrent — lookup for the single
    /// file inside the root should resolve from bencode.
    #[test]
    fn pending_single_file_torrent_resolves_internal_file() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write a single-file .torrent (name=foo, length=16).
        let file_ino =
            crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            file_ino,
            crate::fuse::inodes::InodeData::File {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "single.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        let pending = Arc::new(Mutex::new(HashMap::new()));
        pending
            .lock()
            .unwrap()
            .insert((String::new(), "single.torrent".to_string()), ());

        // Resolve the pending root.
        let (root_ino, _, _) = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            DATA_INO,
            "single.torrent",
        )
        .expect("pending root should resolve");

        // Lookup the internal file 'foo' (same as torrent name for
        // single-file torrents).
        let result =
            DataResolver::lookup_data_inode(&mut inode_mgr, &db, &pending, root_ino, "foo");
        let (_, kind, size) = result.expect("lookup 'foo' should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 16, "single file size should be 16");

        // readdir should list 'foo'.
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, root_ino, 0)
            .expect("readdir single-file pending root returned entries");
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(names.contains(&"foo"), "readdir should list 'foo'");
    }

    /// TSI-2448: non-existent file inside a pending torrent root
    /// should still return ENOENT (None) — the fallback only resolves
    /// files that actually exist in the torrent.
    #[test]
    fn pending_torrent_lookup_nonexistent_file_returns_none() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            root_ino,
            "nonexistent.txt",
        );
        assert!(
            result.is_none(),
            "non-existent file in pending torrent should be ENOENT"
        );
    }

    /// TSI-2448: bencode file-list extractor parses multi-file torrents.
    #[test]
    fn extract_bencode_files_multifile() {
        let data = multifile_torrent_bytes();
        let (name, files) =
            super::extract_bencode_files(&data).expect("should parse multi-file torrent");
        assert_eq!(name, "foo");
        assert_eq!(files.len(), 3, "should have 3 files");

        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert!(paths.contains(&"dir/sub.txt"));
        assert!(paths.contains(&"dir2/readme.txt"));
        assert!(paths.contains(&"top.txt"));

        let sizes: std::collections::HashMap<&str, u64> =
            files.iter().map(|f| (f.path.as_str(), f.size)).collect();
        assert_eq!(sizes["dir/sub.txt"], 10);
        assert_eq!(sizes["dir2/readme.txt"], 5);
        assert_eq!(sizes["top.txt"], 8);
    }

    /// TSI-2448: bencode file-list extractor parses single-file torrents.
    #[test]
    fn extract_bencode_files_singlefile() {
        let data = minimal_torrent_bytes();
        let (name, files) =
            super::extract_bencode_files(&data).expect("should parse single-file torrent");
        assert_eq!(name, "foo");
        assert_eq!(files.len(), 1, "single-file torrent should have 1 file");
        assert_eq!(files[0].path, "foo");
        assert_eq!(files[0].size, 16);
    }

    /// TSI-2448: bencode file-list extractor returns None for malformed data.
    #[test]
    fn extract_bencode_files_malformed_returns_none() {
        assert!(super::extract_bencode_files(b"not bencode").is_none());
        assert!(super::extract_bencode_files(b"").is_none());
    }

    /// TSI-2448 (review): stale pending TorrentDir (6M) and TorrentFile
    /// (7M) entries are evicted from data_inodes when the DB row lands,
    /// not just the TorrentRoot (5M).
    #[test]
    fn stale_pending_dir_and_file_evicted_after_db_row_lands() {
        let db = db_with_torrent("");
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));

        // Write the .torrent file to the metadata inode table.
        let ino = crate::fuse::inodes::NEXT_INO.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        inode_mgr.inodes.insert(
            ino,
            crate::fuse::inodes::InodeData::File {
                parent: crate::fuse::inodes::METADATA_INO,
                name: "test.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        let pending = empty_pending();

        // Insert stale pending entries: root, dir, and file.
        let stale_root = InodeManager::make_pending_torrent_ino("", "test.torrent");
        let stale_dir = InodeManager::make_pending_torrent_dir_ino("", "test.torrent", "subdir");
        let stale_file =
            InodeManager::make_pending_torrent_file_ino("", "test.torrent", "subdir/file.txt");

        inode_mgr.data_inodes.insert(
            stale_root,
            DataInode::TorrentRoot {
                torrent_id: 0,
                source_path: String::new(),
                name: "test-torrent".to_string(),
                filename: "test.torrent".to_string(),
            },
        );
        inode_mgr.data_inodes.insert(
            stale_dir,
            DataInode::TorrentDir {
                torrent_id: 0,
                dir_id: 0,
                name: "subdir".to_string(),
                dir_path: "subdir".to_string(),
                torrent_source_path: String::new(),
                torrent_filename: "test.torrent".to_string(),
            },
        );
        inode_mgr.data_inodes.insert(
            stale_file,
            DataInode::TorrentFile {
                torrent_id: 0,
                file_id: 0,
                name: "file.txt".to_string(),
                size: 16,
                torrent_source_path: String::new(),
                torrent_filename: "test.torrent".to_string(),
            },
        );

        // readdir on data/ — must list the DB torrent and evict ALL stale
        // pending entries (root + dir + file).
        let entries = DataResolver::readdir_data(&mut inode_mgr, &db, &pending, DATA_INO, 0)
            .expect("readdir data/ returned entries");
        assert!(
            entries.iter().any(|(_, _, _, n)| n == "test.torrent"),
            "DB torrent must be in listing"
        );

        assert!(
            !inode_mgr.data_inodes.contains_key(&stale_root),
            "stale pending root must be evicted"
        );
        assert!(
            !inode_mgr.data_inodes.contains_key(&stale_dir),
            "stale pending dir must be evicted"
        );
        assert!(
            !inode_mgr.data_inodes.contains_key(&stale_file),
            "stale pending file must be evicted"
        );
    }
}

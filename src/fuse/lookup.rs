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

        let data_inode = inode_mgr.data_inodes.get(&parent)?;
        match data_inode {
            DataInode::SourcePathDir { path } => {
                Self::resolve_source_path_dir_lookup(db, path, name)
            }
            DataInode::TorrentRoot { torrent_id, .. } => {
                Self::resolve_torrent_root_lookup(db, *torrent_id, name)
            }
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => Self::resolve_torrent_dir_lookup(db, *torrent_id, Some(*dir_id), name),
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
        // and SourcePathDir parents) can be pending; TorrentRoot /
        // TorrentDir / TorrentFile parents query the DB by id and have no
        // pending fallback.
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

    /// TSI-2443 (review): evict stale pending `TorrentRoot(id=0)` entries
    /// from `data_inodes` whose `(source_path, filename)` now has a DB
    /// row.  Called after `readdir_data` lists DB-sourced torrents so the
    /// stale pending inodes don't linger past the 1s FUSE TTL.
    fn evict_stale_pending(inode_mgr: &mut InodeManager, source_path: &str, db_filenames: &[&str]) {
        // If a filename is in the DB, its pending inode (if any) is stale.
        let stale_inos: Vec<u64> = db_filenames
            .iter()
            .filter_map(|fname| {
                let ino = InodeManager::make_pending_torrent_ino(source_path, fname);
                // Only evict if the entry actually exists and is a pending root.
                if matches!(
                    inode_mgr.data_inodes.get(&ino),
                    Some(DataInode::TorrentRoot { torrent_id: 0, .. })
                ) {
                    Some(ino)
                } else {
                    None
                }
            })
            .collect();
        for ino in stale_inos {
            inode_mgr.data_inodes.remove(&ino);
        }
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
                            },
                        ));
                        entries.push((file_ino, offset_counter, FileKind::RegularFile, file.name));
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
            DataInode::TorrentDir {
                torrent_id, dir_id, ..
            } => {
                entries.push((ino, 1, FileKind::Directory, ".".to_string()));

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
                            },
                        ));
                        entries.push((sub_dir_ino, offset_counter, FileKind::Directory, dir.name));
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
                            },
                        ));
                        entries.push((file_ino, offset_counter, FileKind::RegularFile, file.name));
                        offset_counter += 1;
                    }
                }

                for (cache_ino, cache_inode) in cache_entries {
                    inode_mgr.data_inodes.insert(cache_ino, cache_inode);
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
/// Searches for the `4:name<len>:<value>` pattern in the bencode.  The
/// `name` key lives inside the `info` dict but bencode is a flat byte
/// stream, so a targeted substring scan is safe: `4:name` is unambiguous
/// as a bencode key, and the following `<len>:<value>` is parsed to
/// extract the name.  Returns `None` if not found or malformed.
fn extract_bencode_name(data: &[u8]) -> Option<String> {
    let needle = b"4:name";
    let pos = data.windows(needle.len()).position(|w| w == needle)?;
    let rest = &data[pos + needle.len()..];

    // Read the length prefix: digits up to ':'.
    let colon = rest.iter().position(|&b| b == b':')?;
    let len_str = std::str::from_utf8(&rest[..colon]).ok()?;
    let name_len: usize = len_str.parse().ok()?;
    let name_start = colon + 1;
    let name_end = name_start.checked_add(name_len)?;
    if name_end > rest.len() {
        return None;
    }
    let name_bytes = &rest[name_start..name_end];
    String::from_utf8(name_bytes.to_vec()).ok()
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
}

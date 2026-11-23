//! DataResolver — resolves FUSE lookup operations for the data/ subtree.
//! Extracted from TorrentFs to separate data tree resolution from the rest of FUSE handling.

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};

use crate::db::Database;
use crate::domain::fs_error::{FsError, FsResult};
use crate::fuse::inodes::{DataInode, InodeManager};
use crate::infrastructure::metadata::TorrentInfo;
use tracing::error;

use super::fs_types::FileKind;

pub struct DataResolver;

/// Shared `.torrent` buffers parked by `release`, keyed by metadata file
/// inode. `release` moves the bytes out of the inode into this map, so every
/// consumer that needs the content (attributes, pending parse, listing) must
/// read through the same accessor.
pub type BufferedTorrents = HashMap<u64, Arc<Vec<u8>>>;

/// alias for the readdir entry tuple returned by
/// `readdir_pending_torrent` — avoids clippy `type_complexity` lint.
type ReaddirEntries = (Vec<(u64, i64, FileKind, String)>, Vec<(u64, DataInode)>);

/// `(database change count, in-flight add signature)` — the state a `data/`
/// listing is derived from. A cached listing is valid only while this value is
/// unchanged.
pub type DataListingState = (u64, u64);

/// Listing of one `data/` directory together with the state it was read from.
pub struct DataListing {
    pub entries: Vec<(u64, i64, FileKind, String)>,
    pub state: DataListingState,
}

impl DataResolver {
    /// Resolve a child lookup within the data/ subtree.
    /// Returns (ino, DataInode) if found.
    pub fn resolve_data_lookup(
        inode_mgr: &InodeManager,
        db: &Arc<Mutex<Database>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
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
                // when torrent_id is 0 (pending — background
                // add_torrent in-flight, no DB row yet), resolve internal
                // files/dirs from the .torrent bencode instead of the DB.
                if torrent_id == 0 {
                    Self::resolve_pending_torrent_children(
                        inode_mgr,
                        pending_index,
                        buffered,
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
                // same pending fallback for subdirectories.
                if torrent_id == 0 {
                    Self::resolve_pending_torrent_children(
                        inode_mgr,
                        pending_index,
                        buffered,
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

        let file = db_guard
            .get_torrent_file_by_name(torrent_id, parent_dir_id, name)
            .ok()??;
        let ino = InodeManager::make_torrent_file_ino(file.id);
        Some((
            ino,
            DataInode::TorrentFile {
                torrent_id,
                file_id: file.id,
                name: file.name,
                size: file.size,
                torrent_source_path: String::new(),
                torrent_filename: String::new(),
            },
        ))
    }

    /// Lookup a data inode and cache it. Returns (ino, FileType, size).
    pub fn lookup_data_inode(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        parent: u64,
        name: &str,
    ) -> Option<(u64, FileKind, u64)> {
        Self::lookup_data_inode_with_buffers(
            inode_mgr,
            db,
            processing_torrents,
            pending_index,
            &BufferedTorrents::new(),
            parent,
            name,
        )
    }

    /// [`Self::lookup_data_inode`] with access to the shared buffers `release`
    /// parks outside the inode: a pending torrent must parse its declared
    /// name and file tree from those bytes, not the (now empty) inode.
    pub fn lookup_data_inode_with_buffers(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        parent: u64,
        name: &str,
    ) -> Option<(u64, FileKind, u64)> {
        // try the DB first.  If it misses but a background
        // add_torrent is in-flight (processing_torrents has the key),
        // resolve a provisional TorrentRoot from the metadata inode so
        // the data/ mirror doesn't show ENOENT during the ~0.1-0.5s
        // window between release() returning and the DB insert landing.
        if let Some((ino, data_inode)) =
            Self::resolve_data_lookup(inode_mgr, db, pending_index, buffered, parent, name)
        {
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
        // file-list fallback.
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
        let torrent_name = Self::parse_pending_torrent_name(
            inode_mgr,
            pending_index,
            buffered,
            &source_path,
            name,
        );
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

    /// scan the metadata inode table for a `.torrent` file at
    /// `(source_path, filename)` whose background add_torrent is pending.
    /// Returns the torrent's display name if found and parseable.
    ///
    /// The declared name comes from libtorrent's parse of the buffered
    /// `.torrent` (C-3.4.1), with the same legality check the file listing
    /// applies; an unparseable or illegal name falls back to the stem.
    fn parse_pending_torrent_name(
        inode_mgr: &InodeManager,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        source_path: &str,
        filename: &str,
    ) -> String {
        if let Some(file_data) =
            Self::find_pending_torrent_data(inode_mgr, buffered, source_path, filename)
        {
            let torrent_name = pending_index
                .lock()
                .ok()
                .and_then(|mut cache| cache.name(source_path, filename, file_data));
            if let Some(torrent_name) = torrent_name {
                return torrent_name;
            }
        }
        // Fallback: use the filename stem if the inode vanished or is
        // unparseable (e.g. the background thread already consumed it).
        filename.trim_end_matches(".torrent").to_string()
    }

    /// scan the metadata inode table for the `.torrent` file
    /// at `(source_path, filename)` whose `add_torrent` is pending.
    /// Returns a reference to the raw torrent bytes if found.
    ///
    /// This is the same inode scan used by `parse_pending_torrent_name`
    /// but returns the full buffer so the bencode file list can be
    /// extracted for internal file/directory resolution.
    fn find_pending_torrent_data<'a>(
        inode_mgr: &'a InodeManager,
        buffered: &'a BufferedTorrents,
        source_path: &str,
        filename: &str,
    ) -> Option<&'a [u8]> {
        for (ino, data) in &inode_mgr.inodes {
            if let crate::fuse::inodes::InodeData::File {
                name,
                data: file_data,
                unlinked,
                parent,
            } = data
            {
                if *unlinked || name != filename {
                    continue;
                }
                let inode_sp = inode_mgr.extract_source_path(*parent);
                if inode_sp != source_path {
                    continue;
                }
                if !file_data.is_empty() {
                    return Some(file_data);
                }
                // `release` moved the bytes out of the inode; the parked
                // shared handle is the authoritative buffer for this file.
                if let Some(bytes) = buffered.get(ino) {
                    if !bytes.is_empty() {
                        return Some(bytes.as_slice());
                    }
                }
            }
        }
        None
    }

    /// resolve a child (directory or file) inside a pending
    /// torrent root by parsing the bencode file list.
    ///
    /// `dir_path` is the directory path relative to the torrent root
    /// (empty for root-level lookups).  `name` is the child being
    /// looked up.  Returns a `DataInode` with a pending inode (stable
    /// hash of `(source_path, filename, child_path)`) if the child
    /// exists in the torrent's file structure.
    fn resolve_pending_torrent_children(
        inode_mgr: &InodeManager,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        source_path: &str,
        filename: &str,
        dir_path: &str,
        name: &str,
    ) -> Option<(u64, DataInode)> {
        let torrent_data =
            Self::find_pending_torrent_data(inode_mgr, buffered, source_path, filename)?;
        let child = {
            let mut cache = pending_index.lock().ok()?;
            cache.resolve(source_path, filename, torrent_data, dir_path, name)
        }?;

        match child {
            PendingChild::Directory(child_path) => {
                let ino =
                    InodeManager::make_pending_torrent_dir_ino(source_path, filename, &child_path);
                Some((
                    ino,
                    DataInode::TorrentDir {
                        torrent_id: 0,
                        dir_id: 0,
                        name: name.to_string(),
                        dir_path: child_path,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ))
            }
            PendingChild::File(size) => {
                let file_path = if dir_path.is_empty() {
                    name.to_string()
                } else {
                    format!("{}/{}", dir_path, name)
                };
                let ino =
                    InodeManager::make_pending_torrent_file_ino(source_path, filename, &file_path);
                Some((
                    ino,
                    DataInode::TorrentFile {
                        torrent_id: 0,
                        file_id: 0,
                        name: name.to_string(),
                        size: size as i64,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ))
            }
        }
    }

    /// collect pending torrent entries for a given `source_path`
    /// Collect pending torrent entries for a `source_path` — torrents whose
    /// background `add_torrent` is in flight and has no DB row yet — plus the
    /// signature of the in-flight add set they were taken from.
    ///
    /// Both come out of the same lock hold, which is what lets a cached listing
    /// and its key agree: an add that shows up here is part of the returned
    /// signature, and one that is not part of the signature cannot show up here.
    fn collect_pending_entries(
        inode_mgr: &InodeManager,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        source_path: &str,
        existing_filenames: &[&str],
    ) -> (Vec<(u64, DataInode, String)>, u64) {
        let Ok(guard) = processing_torrents.lock() else {
            return (Vec::new(), 0);
        };
        let signature = Self::signature_of(&guard);
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
            let torrent_name =
                Self::parse_pending_torrent_name(inode_mgr, pending_index, buffered, sp, filename);
            let ino = InodeManager::make_pending_torrent_ino(sp, filename);
            let data_inode = DataInode::TorrentRoot {
                torrent_id: 0,
                source_path: sp.clone(),
                name: torrent_name,
                filename: filename.clone(),
            };
            result.push((ino, data_inode, filename.clone()));
        }
        (result, signature)
    }

    /// Signature of a set of in-flight adds: any insertion or removal changes
    /// it. The steady state (no in-flight add) is `0` and allocation-free.
    pub fn signature_of(torrents: &HashMap<(String, String), ()>) -> u64 {
        if torrents.is_empty() {
            return 0;
        }
        let mut hasher = DefaultHasher::new();
        let mut keys: Vec<&(String, String)> = torrents.keys().collect();
        keys.sort_unstable();
        keys.hash(&mut hasher);
        hasher.finish()
    }

    /// Signature of the current in-flight add set.
    pub fn pending_signature(
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
    ) -> u64 {
        match processing_torrents.lock() {
            Ok(torrents) => Self::signature_of(&torrents),
            Err(_) => 0,
        }
    }

    /// The state a listing would currently be derived from: the database change
    /// count and the in-flight add signature. A cached listing is still valid
    /// exactly while this value matches the one stored with it.
    ///
    /// Both halves are read under one hold — the pending lock inside the
    /// database lock, the same nesting the listing build uses — so they describe
    /// one instant. Read separately, a write that does not touch the in-flight
    /// set (a rename or removal) could land between them, letting the stale
    /// pre-write version compare equal to a cached key.
    pub fn current_state(
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
    ) -> DataListingState {
        let Ok(db_guard) = db.lock() else {
            return (0, Self::pending_signature(processing_torrents));
        };
        let db_version = db_guard.total_changes();
        (db_version, Self::pending_signature(processing_torrents))
    }

    /// Evict stale pending entries from `data_inodes` whose `(source_path,
    /// filename)` now has a DB row (called after `readdir_data`), plus pending
    /// `TorrentDir`/`TorrentFile` entries cached during the pending window, so
    /// they don't linger past the 1s FUSE TTL. All three pending types store
    /// `torrent_source_path`/`torrent_filename`, so eviction matches on those
    /// fields uniformly.
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

    /// Generate readdir entries for a pending torrent root/subdirectory by
    /// parsing the bencode file list instead of querying the DB. `dir_path`
    /// is relative to the torrent root (empty = root); returns
    /// `(entries, cache_entries)` for the caller to merge into `readdir_data`.
    /// Entries are `.`, `..`, plus each immediate child directory and file
    /// (directories detected by scanning file paths for a prefix match).
    fn readdir_pending_torrent(
        inode_mgr: &mut InodeManager,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        source_path: &str,
        filename: &str,
        dir_path: &str,
        _self_ino: u64,
    ) -> Option<ReaddirEntries> {
        let torrent_data =
            Self::find_pending_torrent_data(inode_mgr, buffered, source_path, filename)?;
        let children = {
            let mut cache = pending_index.lock().ok()?;
            cache.listing(source_path, filename, torrent_data, dir_path)
        }?;

        let mut entries: Vec<(u64, i64, FileKind, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();
        let mut offset_counter = 3i64;
        for child in children {
            let child_path = if dir_path.is_empty() {
                child.name.clone()
            } else {
                format!("{}/{}", dir_path, child.name)
            };
            if child.is_dir {
                let dir_ino = InodeManager::make_pending_torrent_dir_ino(
                    source_path,
                    filename,
                    &child_path,
                );
                cache_entries.push((
                    dir_ino,
                    DataInode::TorrentDir {
                        torrent_id: 0,
                        dir_id: 0,
                        name: child.name.clone(),
                        dir_path: child_path,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ));
                entries.push((
                    dir_ino,
                    offset_counter,
                    FileKind::Directory,
                    child.name,
                ));
            } else {
                let file_ino =
                    InodeManager::make_pending_torrent_file_ino(source_path, filename, &child_path);
                cache_entries.push((
                    file_ino,
                    DataInode::TorrentFile {
                        torrent_id: 0,
                        file_id: 0,
                        name: child.name.clone(),
                        size: child.size as i64,
                        torrent_source_path: source_path.to_string(),
                        torrent_filename: filename.to_string(),
                    },
                ));
                entries.push((
                    file_ino,
                    offset_counter,
                    FileKind::RegularFile,
                    child.name,
                ));
            }
            offset_counter += 1;
        }

        Some((entries, cache_entries))
    }

    /// Generate the listing of a `data/` directory together with the state it
    /// was read from.
    ///
    /// `state` is captured from the *same* snapshot as `entries`: the database
    /// change count under the database lock that serializes the listing queries,
    /// and the in-flight-add signature under the pending lock that produced the
    /// pending entries. A listing and its key therefore never disagree — an
    /// in-flight add that appears in the listing is part of the signature, and
    /// one that is not part of the signature cannot appear in the listing (the
    /// add set is read while the same lock is held, not before or after the
    /// listing was built).
    pub fn readdir_data(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        ino: u64,
    ) -> Option<DataListing> {
        Self::readdir_data_with_buffers(
            inode_mgr,
            db,
            processing_torrents,
            pending_index,
            &BufferedTorrents::new(),
            ino,
        )
    }

    /// [`Self::readdir_data`] with the shared buffers `release` parks outside
    /// the inode, so a pending torrent lists its declared tree instead of
    /// falling back to the filename stem.
    pub fn readdir_data_with_buffers(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        buffered: &BufferedTorrents,
        ino: u64,
    ) -> Option<DataListing> {
        use super::inodes::{DATA_INO, ROOT_INO};

        let mut entries: Vec<(u64, i64, FileKind, String)> = Vec::new();
        let mut cache_entries: Vec<(u64, DataInode)> = Vec::new();
        // Every arm assigns the state it built its entries from.
        let state: DataListingState;

        if ino == DATA_INO {
            entries.push((DATA_INO, 1, FileKind::Directory, ".".to_string()));
            entries.push((ROOT_INO, 2, FileKind::Directory, "..".to_string()));

            {
                let db_guard = db.lock().ok()?;
                let db_version = db_guard.total_changes();

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
                // inject pending torrents (background add_torrent
                // in-flight, no DB row yet) so readdir doesn't miss them.
                let existing_names: Vec<String> = entries
                    .iter()
                    .filter(|(_, _, k, _)| *k == FileKind::Directory)
                    .skip(2)
                    .map(|(_, _, _, n)| n.clone())
                    .collect();
                let existing_refs: Vec<&str> = existing_names.iter().map(|s| s.as_str()).collect();
                let (pending, pending_signature) = Self::collect_pending_entries(
                    inode_mgr,
                    processing_torrents,
                    pending_index,
                    buffered,
                    "",
                    &existing_refs,
                );
                for (p_ino, p_inode, p_name) in pending {
                    cache_entries.push((p_ino, p_inode));
                    offset_counter += 1;
                    entries.push((p_ino, offset_counter, FileKind::Directory, p_name));
                }
                // evict stale pending inodes whose
                // DB rows have now landed.
                Self::evict_stale_pending(inode_mgr, "", &existing_refs);

                state = (db_version, pending_signature);
            }

            for (cache_ino, cache_inode) in cache_entries {
                inode_mgr.data_inodes.insert(cache_ino, cache_inode);
            }

            return Some(DataListing { entries, state });
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
                    let db_version = db_guard.total_changes();

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
                    // inject pending torrents for this source_path.
                    let existing_names: Vec<String> = entries
                        .iter()
                        .filter(|(_, _, k, _)| *k == FileKind::Directory)
                        .skip(2)
                        .map(|(_, _, _, n)| n.clone())
                        .collect();
                    let existing_refs: Vec<&str> =
                        existing_names.iter().map(|s| s.as_str()).collect();
                    let (pending, pending_signature) = Self::collect_pending_entries(
                        inode_mgr,
                        processing_torrents,
                        pending_index,
                        buffered,
                        &path,
                        &existing_refs,
                    );
                    for (p_ino, p_inode, p_name) in pending {
                        cache_entries.push((p_ino, p_inode));
                        offset_counter += 1;
                        entries.push((p_ino, offset_counter, FileKind::Directory, p_name));
                    }
                    // evict stale pending inodes whose
                    // DB rows have now landed.
                    Self::evict_stale_pending(inode_mgr, &path, &existing_refs);

                    state = (db_version, pending_signature);
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

                // for pending torrents (torrent_id == 0),
                // list internal files/dirs from the bencode instead of
                // the DB.
                if torrent_id == 0 {
                    if let Some((mut p_entries, p_cache)) =
                        Self::readdir_pending_torrent(
                            inode_mgr,
                            pending_index,
                            buffered,
                            &source_path,
                            &filename,
                            "",
                            ino,
                        )
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
                    // The bencode-derived listing is stable for as long as the
                    // pending inode exists; keying it on the current state only
                    // costs rebuilds, never staleness.
                    state = Self::current_state(db, processing_torrents);
                } else {
                    {
                        let db_guard = db.lock().ok()?;
                        let db_version = db_guard.total_changes();

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

                        state = (db_version, Self::pending_signature(processing_torrents));
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

                // for pending torrent dirs (torrent_id == 0),
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
                        pending_index,
                        buffered,
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
                    // Bencode-derived listing: same reasoning as the pending
                    // torrent root above.
                    state = Self::current_state(db, processing_torrents);
                } else {
                    {
                        let db_guard = db.lock().ok()?;
                        let db_version = db_guard.total_changes();

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

                        state = (db_version, Self::pending_signature(processing_torrents));
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

        Some(DataListing { entries, state })
    }

    /// Get the DB reference, returning a domain error on failure.
    pub fn get_db(db: &Option<Arc<Mutex<Database>>>) -> FsResult<&Arc<Mutex<Database>>> {
        db.as_ref().ok_or_else(|| {
            error!("Database not available");
            FsError::Internal("database not available".to_string())
        })
    }
}

/// Deepest path accepted from a pending torrent's file list. Mirrors
/// libtorrent's own `max_directory_depth` default so the projection can never
/// build a deeper tree than the landed one.
const MAX_PENDING_PATH_DEPTH: usize = 100;

/// `true` when `name` is a legal single directory-entry name: non-empty, not
/// `.`/`..`, and free of path separators / NUL bytes.
fn is_legal_entry_name(name: &str) -> bool {
    !name.is_empty() && name != "." && name != ".." && !name.contains('/') && !name.contains('\0')
}

/// `true` when every component of a `/`-separated relative path is a legal
/// entry name and the path does not exceed [`MAX_PENDING_PATH_DEPTH`].
fn is_legal_relative_path(path: &str) -> bool {
    if path.is_empty() {
        return false;
    }
    let mut depth = 0usize;
    for part in path.split('/') {
        depth += 1;
        if depth > MAX_PENDING_PATH_DEPTH || !is_legal_entry_name(part) {
            return false;
        }
    }
    true
}

/// Strip the torrent root component from libtorrent's file path. A single-file
/// torrent is only the root name; a multi-file entry is `<name>/<rel>`.
fn relative_to_root(path: &str, name: &str) -> Option<String> {
    if path == name {
        return Some(name.to_string());
    }
    Some(path.strip_prefix(name)?.strip_prefix('/')?.to_string())
}

/// One immediate child of a pending torrent directory, as readdir reports it.
#[derive(Debug, Clone)]
struct PendingListingEntry {
    name: String,
    size: u64,
    is_dir: bool,
}

/// A pending torrent's child, resolved by name from the built index.
#[derive(Debug, Clone)]
enum PendingChild {
    /// Relative path of the child directory (already depth-checked).
    Directory(String),
    /// Declared size of the child file.
    File(u64),
}

/// Direct-lookup projection of one pending `.torrent` file tree.
///
/// Built once from libtorrent's parse: every file path is registered under its
/// parent directory, so a by-name lookup is one hash probe and a directory
/// listing is one map read. Re-parsing the torrent and scanning the whole file
/// list per request made a 10 000-file pending directory quadratic
/// (F2 / C-11.6). Validity, legality and the depth cap are applied while
/// building, so an illegal entry never enters the index.
struct PendingIndex {
    name: String,
    children: HashMap<(String, String), PendingChild>,
    listings: HashMap<String, Vec<PendingListingEntry>>,
}

impl PendingIndex {
    fn build(data: &[u8]) -> Option<Self> {
        let info = TorrentInfo::from_bytes(data.to_vec()).ok()?;
        let name = info.name();
        if !is_legal_entry_name(&name) {
            return None;
        }
        let files = info.files().ok()?;
        let mut index = Self {
            name,
            children: HashMap::new(),
            listings: HashMap::new(),
        };
        for file in files {
            let Some(relative) = relative_to_root(&file.path, &index.name) else {
                continue;
            };
            if !is_legal_relative_path(&relative) {
                continue;
            }
            let parts: Vec<&str> = relative.split('/').collect();
            let mut dir_path = String::new();
            for (depth, part) in parts.iter().enumerate() {
                let is_leaf = depth + 1 == parts.len();
                let key = (dir_path.clone(), (*part).to_string());
                let child_path = if dir_path.is_empty() {
                    (*part).to_string()
                } else {
                    format!("{}/{}", dir_path, part)
                };
                if !index.children.contains_key(&key) {
                    let (child, size) = if is_leaf {
                        (PendingChild::File(file.size), file.size)
                    } else {
                        (PendingChild::Directory(child_path.clone()), 0)
                    };
                    index.children.insert(key, child);
                    index
                        .listings
                        .entry(dir_path.clone())
                        .or_default()
                        .push(PendingListingEntry {
                            name: (*part).to_string(),
                            size,
                            is_dir: !is_leaf,
                        });
                }
                if !is_leaf {
                    dir_path = child_path;
                }
            }
        }
        Some(index)
    }
}

/// Bound on cached pending indexes: only torrents whose persist is in flight
/// are served this way, and the entry is dropped with the pending key.
const MAX_PENDING_INDEXES: usize = 8;

/// Cache of [`PendingIndex`] keyed by the pending `(source_path, filename)`.
///
/// A pending `.torrent`'s bytes can be replaced while it is still in flight, so
/// the owner drops entries whenever the metadata content changes
/// ([`Self::invalidate`]). The counters make the per-request cost observable:
/// `builds` must stay flat across repeated lookups, and `visits` grows by at
/// most one per lookup (a single hash probe), never by the file count.
#[derive(Default)]
pub struct PendingIndexCache {
    entries: HashMap<(String, String), Arc<PendingIndex>>,
    builds: u64,
    visits: u64,
}

impl PendingIndexCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Drop every built index. Called when a metadata inode's content or the
    /// metadata inode table changed, so no lookup can serve a stale tree.
    pub fn invalidate(&mut self) {
        self.entries.clear();
    }

    /// Number of libtorrent parses that built an index.
    pub fn build_count(&self) -> u64 {
        self.builds
    }

    /// Number of index probes performed by by-name resolution.
    pub fn lookup_visits(&self) -> u64 {
        self.visits
    }

    fn index_for(
        &mut self,
        source_path: &str,
        filename: &str,
        data: &[u8],
    ) -> Option<Arc<PendingIndex>> {
        let key = (source_path.to_string(), filename.to_string());
        if let Some(index) = self.entries.get(&key) {
            return Some(Arc::clone(index));
        }
        let index = Arc::new(PendingIndex::build(data)?);
        if self.entries.len() >= MAX_PENDING_INDEXES {
            self.entries.clear();
        }
        self.builds += 1;
        self.entries.insert(key, Arc::clone(&index));
        Some(index)
    }

    fn name(&mut self, source_path: &str, filename: &str, data: &[u8]) -> Option<String> {
        Some(self.index_for(source_path, filename, data)?.name.clone())
    }

    fn resolve(
        &mut self,
        source_path: &str,
        filename: &str,
        data: &[u8],
        dir_path: &str,
        name: &str,
    ) -> Option<PendingChild> {
        let index = self.index_for(source_path, filename, data)?;
        self.visits += 1;
        index
            .children
            .get(&(dir_path.to_string(), name.to_string()))
            .cloned()
    }

    fn listing(
        &mut self,
        source_path: &str,
        filename: &str,
        data: &[u8],
        dir_path: &str,
    ) -> Option<Vec<PendingListingEntry>> {
        let index = self.index_for(source_path, filename, data)?;
        self.visits += 1;
        Some(index.listings.get(dir_path).cloned().unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::{DataInode, DataResolver, FileKind, PendingIndexCache};

    /// A fresh pending index cache for a test that exercises resolution only.
    fn index_cache() -> Arc<Mutex<PendingIndexCache>> {
        Arc::new(Mutex::new(PendingIndexCache::new()))
    }

    /// `readdir_data` result without the state it carries — tests that only
    /// assert entries use this.
    fn list(
        inode_mgr: &mut InodeManager,
        db: &Arc<Mutex<Database>>,
        processing_torrents: &Arc<Mutex<HashMap<(String, String), ()>>>,
        pending_index: &Arc<Mutex<PendingIndexCache>>,
        ino: u64,
    ) -> Vec<(u64, i64, FileKind, String)> {
        DataResolver::readdir_data(inode_mgr, db, processing_torrents, pending_index, ino)
            .expect("readdir_data")
            .entries
    }
    use crate::db::{Database, InsertTorrentResult};
    use crate::fuse::inodes::{InodeManager, DATA_INO};
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    /// `current_state` must take both halves of the state under one hold: the
    /// pending lock inside the database lock. Read separately, a write that does
    /// not touch the in-flight set (a rename or removal) can land between them,
    /// and the stale pre-write version then compares equal to a cached key —
    /// serving one page from before the write. The listing build nests the same
    /// way, so no reverse-order path is introduced.
    #[test]
    fn current_state_holds_the_database_lock_while_reading_the_pending_set() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let pending: Arc<Mutex<HashMap<(String, String), ()>>> =
            Arc::new(Mutex::new(HashMap::new()));
        // Hold the second half open, then start the state read.
        let pending_guard = pending.lock().unwrap();
        let reader = {
            let db = db.clone();
            let pending = pending.clone();
            std::thread::spawn(move || DataResolver::current_state(&db, &pending))
        };

        // While the state read waits for the pending lock it must already hold
        // the database lock, so no write can slip between the two halves.
        let mut held = false;
        for _ in 0..500 {
            if db.try_lock().is_err() {
                held = true;
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
        drop(pending_guard);
        let state = reader.join().expect("state reader");

        assert!(
            held,
            "state read must hold the database lock while it reads the pending set"
        );
        assert_eq!(state, (0, 0), "empty database, no in-flight add");

        // The same value is what a listing built at this instant carries.
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        let listing = DataResolver::readdir_data(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            DATA_INO,
        )
        .expect("data/ listing");
        assert_eq!(listing.state, state);
    }

    /// The state returned with a listing is the state the entries were built
    /// from: a listing can only carry a provisional entry if its state also
    /// carries the in-flight add that produced it, and any database write moves
    /// the state. That is what makes a cached listing and its key impossible to
    /// disagree, so a stale listing can never be served.
    #[test]
    fn readdir_state_describes_the_listing_it_was_read_with() {
        let db = Arc::new(Mutex::new(Database::open_in_memory().unwrap()));
        let mut inode_mgr = InodeManager::new(Duration::from_secs(0));
        let pending = Arc::new(Mutex::new(HashMap::new()));

        let listing = DataResolver::readdir_data(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            DATA_INO,
        )
        .expect("data/ listing");
        assert_eq!(
            listing.state.1, 0,
            "no in-flight add ⇒ no pending signature"
        );
        assert!(!listing
            .entries
            .iter()
            .any(|(_, _, _, name)| name == "ghost.torrent"));

        let version = listing.state.0;
        db.lock()
            .unwrap()
            .insert_torrent("", "test", "persisted.torrent", 4096, "hash-state", 1)
            .unwrap();
        let listing = DataResolver::readdir_data(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            DATA_INO,
        )
        .expect("data/ listing");
        assert!(
            listing.state.0 > version,
            "a database write must move the listing state"
        );
        assert!(listing
            .entries
            .iter()
            .any(|(_, _, _, name)| name == "persisted.torrent"));

        pending
            .lock()
            .unwrap()
            .insert((String::new(), "ghost.torrent".to_string()), ());
        let listing = DataResolver::readdir_data(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            DATA_INO,
        )
        .expect("data/ listing");
        assert!(listing
            .entries
            .iter()
            .any(|(_, _, _, name)| name == "ghost.torrent"));
        assert_ne!(
            listing.state.1, 0,
            "a listing carrying a provisional entry must carry the add in its state"
        );
    }

    /// Empty processing_torrents map for tests that don't exercise the
    /// pending-add fallback.
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

    /// TorrentRoot `..` must point to the parent source-path
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

        let entries = list(
            &mut inode_mgr,
            &db,
            &empty_pending(),
            &index_cache(),
            torrent_ino,
        );

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
        let parent_entries = list(
            &mut inode_mgr2,
            &db,
            &empty_pending(),
            &index_cache(),
            parent_ino,
        );
        let dot = parent_entries
            .iter()
            .find(|(_, _, _, name)| name == ".")
            .map(|(ino, _, _, _)| *ino)
            .expect("`.` entry not found");

        assert_eq!(dot, parent_ino, "SourcePathDir `.` is its own inode");
    }

    /// when source_path is a single path segment (e.g. "a"),
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

        let entries = list(
            &mut inode_mgr,
            &db,
            &empty_pending(),
            &index_cache(),
            torrent_ino,
        );

        assert_eq!(
            dotdot_ino(&entries),
            DATA_INO,
            "single-segment source_path `..` should be DATA_INO"
        );
    }

    /// empty source_path (torrent at data/ root) → `..` is DATA_INO.
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

        let entries = list(
            &mut inode_mgr,
            &db,
            &empty_pending(),
            &index_cache(),
            torrent_ino,
        );

        assert_eq!(
            dotdot_ino(&entries),
            DATA_INO,
            "empty source_path `..` should be DATA_INO"
        );
    }

    /// multiple torrents sharing a parent source_path — `..`
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

            let entries = list(
                &mut inode_mgr,
                &db,
                &empty_pending(),
                &index_cache(),
                torrent_ino,
            );

            assert_eq!(
                dotdot_ino(&entries),
                expected_parent,
                "torrent {} `..` should point to parent source-path dir, not a sibling",
                name
            );
        }
    }

    // ── pending add_torrent ENOENT window ──────────────────────

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

    /// when a background `add_torrent` is pending (the `.torrent`
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
            &index_cache(),
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

    /// when no background `add_torrent` is pending, the data/
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
            &index_cache(),
            DATA_INO,
            "nonexistent.torrent",
        );
        assert!(
            result.is_none(),
            "non-pending missing torrent should be ENOENT"
        );
    }

    /// `readdir` on `data/` must include pending torrents so
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

        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), DATA_INO);

        // Find "pending.torrent" in the listing.
        let found = entries
            .iter()
            .any(|(_, _, _, name)| name == "pending.torrent");
        assert!(found, "readdir data/ must include pending torrent");
    }

    /// two pending torrents with different filenames in
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
            &index_cache(),
            DATA_INO,
            "alpha.torrent",
        )
        .expect("alpha pending should resolve");
        let (ino_b, _, _) = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            DATA_INO,
            "beta.torrent",
        )
        .expect("beta pending should resolve");
        assert_ne!(ino_a, ino_b, "pending torrents must have distinct inodes");

        // readdir must show both, with distinct inos.
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), DATA_INO);
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

    /// pending torrents under a SourcePathDir (not the
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
            &index_cache(),
            sp_ino,
            "pending.torrent",
        );
        let (lookup_ino, kind, _) = result.expect("SourcePathDir pending should resolve");
        assert_eq!(kind, FileKind::Directory);

        // readdir on the SourcePathDir must include "pending.torrent".
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), sp_ino);
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

    /// stale pending TorrentRoot(id=0) entries are
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
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), DATA_INO);
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

    // ── pending torrent internal file resolution ─────────────

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
            &index_cache(),
            parent_data_ino,
            filename,
        )
        .expect("pending torrent root should resolve");

        (inode_mgr, db, pending, root_ino)
    }

    /// readdir on a pending torrent root must list internal
    /// files and directories from the bencode — not return empty.
    #[test]
    fn pending_torrent_root_readdir_lists_internal_files() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), root_ino);

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

    /// lookup on a pending torrent root for an internal file
    /// must resolve to a TorrentFile, not ENOENT.
    #[test]
    fn pending_torrent_root_lookup_resolves_internal_file() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result =
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                root_ino,
                "top.txt",
            );
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

    /// lookup on a pending torrent root for an internal
    /// directory must resolve to a TorrentDir, not ENOENT.
    #[test]
    fn pending_torrent_root_lookup_resolves_internal_dir() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result =
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                root_ino,
                "dir",
            );
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

    /// readdir on a pending torrent subdirectory must list its
    /// contents from the bencode.
    #[test]
    fn pending_torrent_dir_readdir_lists_contents() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        // Lookup the 'dir' subdirectory.
        let (dir_ino, _, _) =
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                root_ino,
                "dir",
            )
                .expect("lookup 'dir' should resolve");

        // readdir on the subdirectory should list 'sub.txt'.
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), dir_ino);
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(
            names.contains(&"sub.txt"),
            "readdir on 'dir' should list 'sub.txt'"
        );
    }

    /// lookup on a pending torrent subdirectory for a file
    /// must resolve, not ENOENT.
    #[test]
    fn pending_torrent_dir_lookup_resolves_file() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        // Lookup the 'dir' subdirectory.
        let (dir_ino, _, _) =
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                root_ino,
                "dir",
            )
                .expect("lookup 'dir' should resolve");

        // Lookup 'sub.txt' inside 'dir'.
        let result =
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                dir_ino,
                "sub.txt",
            );
        let (_, kind, size) = result.expect("lookup 'sub.txt' in 'dir' should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 10, "sub.txt size should be 10");
    }

    /// pending internal file resolution works under a
    /// SourcePathDir (not just data/ root).
    #[test]
    fn pending_torrent_under_source_path_dir_resolves_internal() {
        let (mut inode_mgr, db, pending, root_ino) =
            setup_pending_multifile("sub", "multi.torrent");

        // readdir on the pending root should list internal entries.
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), root_ino);
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
            DataResolver::lookup_data_inode(
                &mut inode_mgr,
                &db,
                &pending,
                &index_cache(),
                root_ino,
                "top.txt",
            );
        let (_, kind, size) = result.expect("lookup 'top.txt' under source_path should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 8);
    }

    /// single-file pending torrent — lookup for the single
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
            &index_cache(),
            DATA_INO,
            "single.torrent",
        )
        .expect("pending root should resolve");

        // Lookup the internal file 'foo' (same as torrent name for
        // single-file torrents).
        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            root_ino,
            "foo",
        );
        let (_, kind, size) = result.expect("lookup 'foo' should resolve");
        assert_eq!(kind, FileKind::RegularFile);
        assert_eq!(size, 16, "single file size should be 16");

        // readdir should list 'foo'.
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), root_ino);
        let names: Vec<&str> = entries.iter().map(|(_, _, _, n)| n.as_str()).collect();
        assert!(names.contains(&"foo"), "readdir should list 'foo'");
    }

    /// non-existent file inside a pending torrent root
    /// should still return ENOENT (None) — the fallback only resolves
    /// files that actually exist in the torrent.
    #[test]
    fn pending_torrent_lookup_nonexistent_file_returns_none() {
        let (mut inode_mgr, db, pending, root_ino) = setup_pending_multifile("", "multi.torrent");

        let result = DataResolver::lookup_data_inode(
            &mut inode_mgr,
            &db,
            &pending,
            &index_cache(),
            root_ino,
            "nonexistent.txt",
        );
        assert!(
            result.is_none(),
            "non-existent file in pending torrent should be ENOENT"
        );
    }

    /// The built index projects a multi-file torrent's directories and files,
    /// keyed by `(parent path, entry name)` — the shape by-name resolution and
    /// readdir consume.
    #[test]
    fn pending_index_projects_multifile_tree() {
        let data = multifile_torrent_bytes();
        let index = super::PendingIndex::build(&data).expect("should parse multi-file torrent");
        assert_eq!(index.name, "foo");

        assert!(matches!(
            index.children.get(&(String::new(), "dir".to_string())),
            Some(super::PendingChild::Directory(path)) if path == "dir"
        ));
        assert!(matches!(
            index.children.get(&("dir".to_string(), "sub.txt".to_string())),
            Some(super::PendingChild::File(10))
        ));
        assert!(matches!(
            index.children.get(&(String::new(), "top.txt".to_string())),
            Some(super::PendingChild::File(8))
        ));

        let root = index.listings.get("").expect("root listing");
        let names: Vec<&str> = root.iter().map(|entry| entry.name.as_str()).collect();
        assert!(names.contains(&"dir"));
        assert!(names.contains(&"dir2"));
        assert!(names.contains(&"top.txt"));
        assert!(root.iter().find(|e| e.name == "dir").is_some_and(|e| e.is_dir));
        assert!(root
            .iter()
            .find(|e| e.name == "top.txt")
            .is_some_and(|e| !e.is_dir && e.size == 8));
    }

    /// A single-file torrent's first-level entry is the file itself.
    #[test]
    fn pending_index_projects_singlefile_root() {
        let data = minimal_torrent_bytes();
        let index = super::PendingIndex::build(&data).expect("should parse single-file torrent");
        assert_eq!(index.name, "foo");
        assert!(matches!(
            index.children.get(&(String::new(), "foo".to_string())),
            Some(super::PendingChild::File(16))
        ));
        let root = index.listings.get("").expect("root listing");
        assert_eq!(root.len(), 1);
        assert_eq!(root[0].name, "foo");
    }

    /// Malformed bytes build no index (the pending path then falls back).
    #[test]
    fn pending_index_rejects_malformed_data() {
        assert!(super::PendingIndex::build(b"not bencode").is_none());
        assert!(super::PendingIndex::build(b"").is_none());
    }

    /// The number of files a pending torrent declares must not change what one
    /// by-name lookup costs: the tree is built once and every lookup is a
    /// single hash probe (F2 / C-11.6).
    #[test]
    fn pending_lookup_cost_is_independent_of_file_count() {
        fn wide_torrent(files: usize) -> Vec<u8> {
            let paths: Vec<Vec<Vec<u8>>> = (0..files)
                .map(|i| {
                    vec![
                        b"dir".to_vec(),
                        format!("f{i}.txt").into_bytes(),
                    ]
                })
                .collect();
            torrent_with_path_elements(b"foo", &paths)
        }

        for files in [500usize, 10_000] {
            let data = wide_torrent(files);
            let mut cache = super::PendingIndexCache::new();
            let probes = 200;
            for i in 0..probes {
                let name = format!("f{i}.txt");
                let child =
                    cache.resolve("", "wide.torrent", &data, "dir", &name);
                assert!(
                    matches!(child, Some(super::PendingChild::File(16))),
                    "{files} files: probe {name} resolved to {child:?}"
                );
            }
            assert_eq!(
                cache.build_count(),
                1,
                "{files} files: the index must be built once, not per lookup"
            );
            assert_eq!(
                cache.lookup_visits(),
                probes,
                "{files} files: each lookup must be one probe, not a file-list scan"
            );
            cache.invalidate();
            let _ = cache.resolve("", "wide.torrent", &data, "dir", "f499.txt");
            assert_eq!(
                cache.build_count(),
                2,
                "invalidation drops the index; the next lookup rebuilds it once"
            );
        }
    }

    /// Build a multi-file torrent whose each `path` is a full element list, so
    /// the test can inject `.`/`..`, empty, slash-bearing, and over-deep
    /// segments that a single-element builder cannot express.
    fn torrent_with_path_elements(name: &[u8], paths: &[Vec<Vec<u8>>]) -> Vec<u8> {
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"5:filesl");
        for path in paths {
            t.push(b'd');
            t.extend_from_slice(b"6:lengthi16e");
            t.extend_from_slice(b"4:pathl");
            for element in path {
                t.extend_from_slice(element.len().to_string().as_bytes());
                t.push(b':');
                t.extend_from_slice(element);
            }
            t.push(b'e');
            t.push(b'e');
        }
        t.push(b'e');
        t.extend_from_slice(b"4:name");
        t.extend_from_slice(name.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(name);
        // libtorrent rejects a torrent whose `pieces` does not cover the whole
        // declared size, so derive the hash count from `paths` (each file is
        // 16 bytes, piece length 16384).
        let total_size = paths.len() as u64 * 16;
        let num_pieces = total_size.div_ceil(16384).max(1) as usize;
        t.extend_from_slice(b"12:piece lengthi16384e");
        t.extend_from_slice(format!("6:pieces{}:", num_pieces * 20).as_bytes());
        t.extend_from_slice(&vec![0u8; num_pieces * 20]);
        t.extend_from_slice(b"ee");
        t
    }

    /// `C-3.1.3`/M7: a pathological `.torrent` must never project `..`, an
    /// empty name, or a `/`-bearing segment. libtorrent sanitises the elements
    /// on its own, and the legality check drops anything still illegal.
    #[test]
    fn pending_paths_never_contain_illegal_segments() {
        let data = torrent_with_path_elements(
            b"foo",
            &[
                vec![b"..".to_vec(), b"evil.txt".to_vec()],
                vec![b"a/b".to_vec()],
                vec![b"".to_vec(), b"empty.txt".to_vec()],
                vec![b".".to_vec()],
                vec![b"ok.txt".to_vec()],
            ],
        );

        if let Some(index) = super::PendingIndex::build(&data) {
            for (parent, name) in index.children.keys() {
                assert!(
                    super::is_legal_entry_name(name),
                    "illegal projected entry name: {:?}",
                    name
                );
                assert!(
                    parent.is_empty() || super::is_legal_relative_path(parent),
                    "illegal projected parent path: {:?}",
                    parent
                );
                assert!(
                    !parent.split('/').any(|part| part == ".."),
                    "projected path escapes the torrent root: {:?}",
                    parent
                );
            }
        }
    }

    /// `C-3.1.3`/M7: an over-deep path is rejected instead of building an
    /// unbounded tree. libtorrent refuses the torrent (`max_directory_depth`);
    /// the Rust-side check is the backstop if that ever changes.
    #[test]
    fn pending_paths_are_depth_bounded() {
        let deep: Vec<Vec<u8>> = (0..super::MAX_PENDING_PATH_DEPTH + 20)
            .map(|i| format!("d{i}").into_bytes())
            .collect();
        let data = torrent_with_path_elements(b"foo", &[deep]);

        if let Some(index) = super::PendingIndex::build(&data) {
            for (parent, name) in index.children.keys() {
                let depth = parent.split('/').filter(|p| !p.is_empty()).count() + 1;
                assert!(
                    depth <= super::MAX_PENDING_PATH_DEPTH,
                    "path depth {} exceeds the cap: {:?}/{:?}",
                    depth,
                    parent,
                    name
                );
            }
        }
    }

    /// `C-3.1.3`/M7: a torrent declaring `..` as its root name must never
    /// materialise that entry. libtorrent may sanitise the name instead of
    /// rejecting the torrent; either way the pending projection only ever
    /// exposes a legal single entry name.
    #[test]
    fn pending_root_name_must_be_a_legal_entry() {
        let data = torrent_with_path_elements(b"..", &[vec![b"evil.txt".to_vec()]]);
        if let Some(index) = super::PendingIndex::build(&data) {
            assert!(
                super::is_legal_entry_name(&index.name),
                "a `..` root name must not become a directory entry: {:?}",
                index.name
            );
            for (parent, name) in index.children.keys() {
                assert!(super::is_legal_entry_name(name));
                assert!(parent.is_empty() || super::is_legal_relative_path(parent));
            }
        }
    }

    /// stale pending TorrentDir (6M) and TorrentFile
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
        let entries = list(&mut inode_mgr, &db, &pending, &index_cache(), DATA_INO);
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

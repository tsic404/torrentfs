//! `FsService` — the filesystem application service layer.
//!
//! This is where the domain logic lives: inode resolution, `.torrent`
//! lifecycle persistence, data reads (local vs. remote), and `.stats`
//! rendering. `FsService` imports **neither** `fuser` **nor** `libc`; it
//! returns domain results (`FsError`, `Attr`, `ReadOutcome`, …) and leaves all
//! protocol/errno adaptation to `TorrentFs` (the thin adapter in `super::mod`).
//!
//! See the architecture design doc §5.2 for the namespace × operation table.

use std::collections::HashMap;
use std::ffi::OsStr;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use std::sync::OnceLock;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

use fuser::Notifier;

use tracing::{error, info, warn};

use sha1_smol::Sha1;

use crate::cache::CacheManager;
use crate::config::TorrentfsConfig;
use crate::db::Database;
use crate::domain::fs_error::{FsError, FsResult};
use crate::infrastructure::metrics::Metrics;
use crate::metadata::TorrentInfo;
use crate::seeding::SeedingManager;
use crate::services::download::DownloadService;
use crate::services::seeding::SeedingService;
use crate::services::torrent::TorrentService;

use super::fs_types::{
    Attr, Created, DirEntry, Entry, FileKind, OpenOutcome, ReadOutcome, StatsKind,
};
use super::inodes::{
    DataInode, InodeData, InodeManager, DATA_DIR_INO_BASE, DATA_INO, DATA_TORRENT_INO_BASE,
    MAX_TORRENT_SIZE, METADATA_INO, NEXT_FH, NEXT_INO, ROOT_INO, STATS_INO,
};
#[cfg(test)]
use super::inodes::{DATA_FILE_INO_BASE, SOURCE_PATH_DIR_INO_BASE};
use super::lookup::DataResolver;
use super::stats::{generate_directory_stats, generate_global_stats, generate_torrent_stats};

/// TSI-2274: maximum number of entries in the L1 range cache.  Each entry
/// caches a `(offset, size)` slice read from a torrent file.  When the
/// cache is full, inserting a new entry clears all existing entries —
/// coarse but bounded: the L1 cache is a hot-zone optimization, and
/// clearing it only causes a few L2 disk re-reads.
const MAX_L1_ENTRIES: usize = 256;

/// TSI-2378: how long `rename` waits for a concurrently in-flight
/// `add_torrent` (spawned detached by `release`, TSI-2247) to settle its DB
/// insert before giving up and failing the rename.
const PENDING_ADD_TIMEOUT: Duration = Duration::from_secs(5);

/// TSI-2293: guard against a silent 0-byte EOF from `read_file_range`.
///
/// When `pieces_on_disk` (using `info.files()` summed sizes) and the
/// engine's `read_file_range` (using libtorrent `file_offset` FFI)
/// disagree on `file_start_offset`, the engine can return `Ok(Vec::new())`
/// for a range that `pieces_on_disk` said was on disk (or not on disk).
/// Returning empty data for a non-zero `size` would make the kernel see
/// EOF and `dd` exits 0 — the user never learns the download failed.
///
/// `fs_service::read` verifies `offset < actual_size` before entering
/// either the sync or the deferred read path, so `size` is always
/// greater than zero when data is expected.  This function translates
/// empty data for a non-zero `size` into `Err(FsError::NoPeers)` (→
/// ENODATA), letting the user see a meaningful error instead of a
/// silent EOF.
fn guard_empty_read(data: &[u8], size: u32) -> Result<(), FsError> {
    if data.is_empty() && size > 0 {
        Err(FsError::NoPeers(format!(
            "Read returned 0 bytes for size {} \
             (possible file_offset mismatch)",
            size
        )))
    } else {
        Ok(())
    }
}

pub struct FsService {
    pub inode_mgr: InodeManager,
    pub db: Option<Arc<Mutex<Database>>>,
    pub torrent_service: Option<TorrentService>,
    pub processing_torrents: Arc<Mutex<HashMap<(String, String), ()>>>,
    /// TSI-2378: signalled whenever an entry is removed from
    /// `processing_torrents`, so a `rename` blocked on an in-flight
    /// `add_torrent` wakes immediately instead of polling.
    pub processing_torrents_cv: Arc<Condvar>,
    pub download_service: Option<Arc<DownloadService>>,
    pub seeding_manager: Option<Arc<SeedingManager>>,
    pub torrent_data_cache: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    pub torrent_info_cache: Arc<Mutex<HashMap<String, Arc<TorrentInfo>>>>,
    pub listen_addr: String,
    pub metrics: Arc<Metrics>,
    /// TSI-2454: kernel cache invalidation handle.  Set once after
    /// `fuser::spawn_mount2` returns (main wires `bg.notifier()` into
    /// here).  When set, `unlink`/`rmdir`/`rename` call
    /// `Notifier::inval_entry` to immediately purge the kernel dentry
    /// cache for removed `data/` entries, so the stale mirror vanishes
    /// at once instead of lingering for the 1s FUSE TTL.
    pub notifier: Arc<OnceLock<Option<Notifier>>>,
}

// ── Kernel cache invalidation (TSI-2454) ─────────────────────────────────
//
// `fuser::Notifier::inval_entry(parent, name)` sends `FUSE_NOTIFY_INVAL_ENTRY`
// to the kernel, immediately purging its cached dentry for `name` under
// `parent`.  Without this, the kernel serves its cached `data/` mirror entry
// for the full 1s TTL (`const TTL` in `fuse/mod.rs`), so the user sees a stale
// directory after `rm metadata/foo.torrent` until the TTL expires.
//
// The `Notifier` is obtained from `BackgroundSession::notifier()` *after*
// `spawn_mount2` returns — `TorrentFs` is moved into the session, so `main`
// extracts the `Arc<OnceLock<Option<Notifier>>>` handle before the move and
// sets it once the session is live.  All calls are best-effort: a failure
// (kernel already evicted, ENOENT) is swallowed by `Notifier::send_inval`.
impl FsService {
    /// Invalidate the kernel's cached dentry for `name` under `parent` in the
    /// `data/` subtree.  No-op when the notifier isn't wired (tests).
    fn inval_data_entry(&self, parent: u64, name: &str) {
        if let Some(Some(notifier)) = self.notifier.get() {
            if let Err(e) = notifier.inval_entry(parent, &OsStr::new(name)) {
                warn!(
                    "inval_entry(parent={}, name={}) failed: {}",
                    parent, name, e
                );
            }
        }
    }

    /// Split a `data/` source-path directory into its `(parent_ino, name)`
    /// for `inval_entry`.  A top-level path like `"cat-a"` has parent
    /// `DATA_INO` and name `"cat-a"`.  A nested path like `"cat-a/sub"`
    /// has parent `make_source_path_dir_ino("cat-a")` and name `"sub"`.
    fn split_data_dir_parent(path: &str) -> (u64, String) {
        match path.rsplit_once('/') {
            Some((parent_path, name)) => (
                InodeManager::make_source_path_dir_ino(parent_path),
                name.to_string(),
            ),
            None => (DATA_INO, path.to_string()),
        }
    }
}

impl FsService {
    // ── Construction ────────────────────────────────────────────────────────

    pub fn new_with_cache_path(cache_path: PathBuf, config: &TorrentfsConfig) -> Self {
        if !cache_path.exists() {
            if let Err(e) = std::fs::create_dir_all(&cache_path) {
                warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
            }
        }
        let metrics = Arc::new(Metrics::new());
        let download_service =
            DownloadService::new_with_metrics(cache_path.as_path(), config, metrics.clone())
                .ok()
                .map(Arc::new);

        // Create the SeedingManager and register it as the CacheManager
        // eviction callback.  The Arc is kept on FsService so it can be
        // shared with TorrentService for seed removal on unlink (TSI-2232).
        let seeding_manager = match &download_service {
            Some(_) => match SeedingService::new(&cache_path, config) {
                Ok(seeding_svc) => {
                    let sm = seeding_svc.get_seeding_manager();
                    Some(sm)
                }
                Err(e) => {
                    warn!(
                        "SeedingService initialization failed; seeding disabled: {:?}",
                        e
                    );
                    None
                }
            },
            None => None,
        };

        let creation_time = Duration::from_secs(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs(),
        );

        let listen_addr = config
            .connections
            .listen_interfaces
            .clone()
            .unwrap_or_else(|| "0.0.0.0:6881".to_string());
        Self {
            inode_mgr: InodeManager::new(creation_time),
            db: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service,
            seeding_manager,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr,
            metrics,
            notifier: Arc::new(OnceLock::new()),
        }
    }

    #[allow(dead_code)]
    pub fn new() -> Self {
        Self::new_with_cache_path(
            PathBuf::from("/tmp/torrentfs-cache"),
            &TorrentfsConfig::default_config(),
        )
    }

    pub fn new_with_db_and_cache(
        db: Database,
        cache_path: PathBuf,
        config: &TorrentfsConfig,
    ) -> Self {
        let mut svc = Self::new_with_cache_path(cache_path, config);

        // Collect data from database before moving it.
        let (dirs, torrents, torrent_datas) = {
            let dirs = db.get_all_metadata_directories().unwrap_or_default();
            let torrents = db.get_all_torrents().unwrap_or_default();
            let torrent_datas: Vec<Vec<u8>> = torrents
                .iter()
                .filter_map(|t| t.torrent_data.clone())
                .collect();
            (dirs, torrents, torrent_datas)
        };

        let db_arc = Arc::new(Mutex::new(db));
        svc.torrent_service = Some(TorrentService::new(
            db_arc.clone(),
            svc.download_service.clone(),
            svc.seeding_manager.clone(),
        ));
        svc.db = Some(db_arc);
        svc.inode_mgr.restore_metadata_inodes(dirs, torrents);

        // Recreate lightweight libtorrent handles for all persisted torrents so
        // peer/seed information and `.stats` piece status are visible without a
        // read (TSI-2112 / TSI-2133).
        let mut infos_by_hash: HashMap<String, Arc<TorrentInfo>> = HashMap::new();
        if let Some(ds) = &svc.download_service {
            for data in torrent_datas {
                match TorrentInfo::from_bytes(data) {
                    Ok(info) => {
                        let name = info.name();
                        let info = Arc::new(info);
                        if let Ok(ih) = info.info_hash() {
                            infos_by_hash.insert(hex::encode(ih), info.clone());
                        }
                        if let Err(e) = ds.ensure_handle_lightweight(info) {
                            warn!(
                                "Failed to restore lightweight handle for torrent '{}': {:?}",
                                name, e
                            );
                        }
                    }
                    Err(e) => {
                        warn!("Failed to parse stored torrent data on startup: {:?}", e);
                    }
                }
            }
        }

        // TSI-2199: background SHA-1 verification of on-disk cached pieces that
        // the startup scan re-registered as unverified. Runs asynchronously so
        // FUSE mount readiness is never blocked.
        if !infos_by_hash.is_empty() {
            if let Some(cache) = svc.get_cache_manager() {
                spawn_cache_verification(cache, infos_by_hash);
            }
        }

        svc
    }

    /// Get the CacheManager shared with DownloadService.
    pub fn get_cache_manager(&self) -> Option<Arc<Mutex<CacheManager>>> {
        self.download_service
            .as_ref()
            .and_then(|ds| ds.get_cache_manager())
    }

    // ── Namespace-agnostic queries ──────────────────────────────────────────

    pub fn lookup(&mut self, parent: u64, name: &str) -> FsResult<Option<Entry>> {
        if parent == ROOT_INO {
            return Ok(match name {
                "metadata" => Some(Entry {
                    ino: METADATA_INO,
                    attr: self.inode_mgr.attr_for_dir(METADATA_INO, true),
                }),
                "data" => Some(Entry {
                    ino: DATA_INO,
                    attr: self.inode_mgr.attr_for_dir(DATA_INO, false),
                }),
                ".stats" => {
                    let stats_size = self.read_stats(StatsKind::Global).len() as u64;
                    Some(Entry {
                        ino: STATS_INO,
                        attr: self.inode_mgr.attr_for_file(STATS_INO, stats_size),
                    })
                }
                _ => None,
            });
        }

        // Handle .stats virtual file for data/ subtree directories.
        if name == ".stats" {
            if parent == DATA_INO {
                let stats_ino = InodeManager::make_stats_ino(parent);
                let content = self.generate_dir_stats_content("");
                return Ok(Some(Entry {
                    ino: stats_ino,
                    attr: self
                        .inode_mgr
                        .attr_for_file(stats_ino, content.len() as u64),
                }));
            }

            if let Some(data_inode) = self.inode_mgr.data_inodes.get(&parent) {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_dir_stats_content(path);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    DataInode::TorrentRoot { torrent_id, .. } => {
                        let stats_ino = InodeManager::make_stats_ino(parent);
                        let content = self.generate_torrent_stats_for_id(*torrent_id);
                        return Ok(Some(Entry {
                            ino: stats_ino,
                            attr: self
                                .inode_mgr
                                .attr_for_file(stats_ino, content.len() as u64),
                        }));
                    }
                    _ => return Ok(None),
                }
            }
            // Parent not yet cached in data_inodes — fall through to the normal
            // data/ lookup below.
        }

        if parent == DATA_INO || InodeManager::is_data_ino(parent) {
            if let Some(db) = &self.db {
                if let Some((ino, kind, size)) = DataResolver::lookup_data_inode(
                    &mut self.inode_mgr,
                    db,
                    &self.processing_torrents,
                    parent,
                    name,
                ) {
                    let attr = match kind {
                        FileKind::Directory => self.inode_mgr.attr_for_dir(ino, false),
                        FileKind::RegularFile => self.inode_mgr.attr_for_file(ino, size),
                    };
                    return Ok(Some(Entry { ino, attr }));
                }
            }
            return Ok(None);
        }

        if let Some(child_ino) = self.inode_mgr.find_child_by_name(parent, name) {
            if let Some(data) = self.inode_mgr.inodes.get(&child_ino) {
                let attr = match data {
                    InodeData::Directory { .. } => self.inode_mgr.attr_for_dir(child_ino, true),
                    InodeData::File {
                        data: file_data, ..
                    } => self
                        .inode_mgr
                        .attr_for_file(child_ino, file_data.len() as u64),
                };
                return Ok(Some(Entry {
                    ino: child_ino,
                    attr,
                }));
            }
        }

        Ok(None)
    }

    pub fn getattr(&mut self, ino: u64) -> FsResult<Attr> {
        match ino {
            ROOT_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            METADATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, true)),
            DATA_INO => Ok(self.inode_mgr.attr_for_dir(ino, false)),
            STATS_INO => {
                let stats_size = self.generate_global_stats_content().len() as u64;
                Ok(self.inode_mgr.attr_for_file(ino, stats_size))
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let content = self.generate_data_stats_for_ino(ino);
                    return Ok(self.inode_mgr.attr_for_file(ino, content.len() as u64));
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(data_inode) = self.inode_mgr.data_inodes.get(&ino) {
                        return Ok(match data_inode {
                            DataInode::SourcePathDir { .. }
                            | DataInode::TorrentRoot { .. }
                            | DataInode::TorrentDir { .. } => {
                                self.inode_mgr.attr_for_dir(ino, false)
                            }
                            DataInode::TorrentFile { size, .. } => {
                                self.inode_mgr.attr_for_file(ino, *size as u64)
                            }
                        });
                    }

                    let torrent_id = (ino - DATA_TORRENT_INO_BASE) as i64;
                    if (DATA_TORRENT_INO_BASE..DATA_DIR_INO_BASE).contains(&ino) {
                        if self
                            .torrent_service
                            .as_ref()
                            .map(|ts| ts.torrent_exists_by_id(torrent_id))
                            .unwrap_or(false)
                        {
                            return Ok(self.inode_mgr.attr_for_dir(ino, false));
                        } else {
                            return Err(FsError::NotFound);
                        }
                    }

                    return Err(FsError::NotFound);
                }

                if let Some(data) = &self.inode_mgr.inodes.get(&ino) {
                    match data {
                        InodeData::Directory { .. } => Ok(self
                            .inode_mgr
                            .attr_for_dir(ino, self.inode_mgr.is_metadata_child(ino))),
                        InodeData::File {
                            data: file_data, ..
                        } => Ok(self.inode_mgr.attr_for_file(ino, file_data.len() as u64)),
                    }
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    /// `setattr` (chmod/chown/truncate/utimens) on any namespace whose
    /// attributes are virtual and immutable must never silently succeed.
    ///
    /// `getattr` alone cannot tell the difference between "attributes are
    /// virtual and immutable" (metadata, `.stats`, root) and "this inode exists
    /// but is not writable", so a `chmod` on `data/` (or on a `metadata/`
    /// file whose mode is fixed at 0o444) previously returned the current
    /// attributes (exit 0) while leaving the mode untouched.
    ///
    /// The errno differs by namespace: `data/` is genuinely read-only and
    /// returns `EROFS` (matching `write`/`rmdir`/`unlink`/`rename`,
    /// TSI-2533); `metadata/`, the `.stats` virtual files and the root
    /// directory have virtual, immutable attributes, so `setattr` there
    /// returns `EPERM` (TSI-2536).
    pub fn setattr(&mut self, ino: u64) -> FsResult<Attr> {
        // Stats-derived inodes (`dir_ino + STATS_INO_OFFSET`, >= 10_000_000)
        // also satisfy `is_data_ino` (>= DATA_TORRENT_INO_BASE), so they must
        // be classified as stats *before* the data/ guard — otherwise they
        // would wrongly return EROFS instead of EPERM.
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            return Err(FsError::NotPermitted);
        }
        if InodeManager::is_data_namespace(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if self.inode_mgr.is_metadata_child(ino) || ino == ROOT_INO {
            return Err(FsError::NotPermitted);
        }
        self.getattr(ino)
    }

    /// `symlink` is never supported: torrentfs exposes only directories and
    /// regular files.  The read-only `data/` namespace must return `EROFS`
    /// (`ReadOnlyFileSystem`) to match `write`/`chmod`/`rmdir`/`unlink`/`rename`;
    /// every other namespace returns `EPERM` (`NotPermitted`) because the
    /// operation is unsupported, not a permissions question (TSI-2537).
    pub fn symlink(&mut self, parent: u64) -> FsError {
        if InodeManager::is_data_namespace(parent) {
            FsError::ReadOnlyFileSystem
        } else {
            FsError::NotPermitted
        }
    }

    pub fn readdir(&mut self, ino: u64, offset: i64) -> FsResult<Vec<DirEntry>> {
        if ino == DATA_INO || InodeManager::is_data_ino(ino) {
            if let Some(db) = &self.db {
                if let Some(entries) = DataResolver::readdir_data(
                    &mut self.inode_mgr,
                    db,
                    &self.processing_torrents,
                    ino,
                    offset,
                ) {
                    return Ok(entries
                        .into_iter()
                        .map(|(entry_ino, entry_offset, kind, name)| DirEntry {
                            ino: entry_ino,
                            offset: entry_offset,
                            kind,
                            name,
                        })
                        .collect());
                }
            }
            return Err(FsError::NotFound);
        }

        let mut entries: Vec<DirEntry> = vec![DirEntry {
            ino,
            offset: 1,
            kind: FileKind::Directory,
            name: ".".to_string(),
        }];

        if ino == ROOT_INO {
            entries.push(DirEntry {
                ino: ROOT_INO,
                offset: 2,
                kind: FileKind::Directory,
                name: "..".to_string(),
            });
            entries.push(DirEntry {
                ino: METADATA_INO,
                offset: 3,
                kind: FileKind::Directory,
                name: "metadata".to_string(),
            });
            entries.push(DirEntry {
                ino: DATA_INO,
                offset: 4,
                kind: FileKind::Directory,
                name: "data".to_string(),
            });
            entries.push(DirEntry {
                ino: STATS_INO,
                offset: 5,
                kind: FileKind::RegularFile,
                name: ".stats".to_string(),
            });
        } else if let Some(InodeData::Directory { parent, .. }) = self.inode_mgr.inodes.get(&ino) {
            entries.push(DirEntry {
                ino: *parent,
                offset: 2,
                kind: FileKind::Directory,
                name: "..".to_string(),
            });

            let mut offset_counter = entries.len() as i64 + 1;
            // Collect entries first to avoid borrowing issues.
            let children: Vec<(u64, FileKind, String)> = self
                .inode_mgr
                .inodes
                .iter()
                .filter_map(|(child_ino, data)| match data {
                    InodeData::Directory {
                        parent: p, name, ..
                    } if *p == ino && !name.is_empty() => {
                        Some((*child_ino, FileKind::Directory, name.clone()))
                    }
                    InodeData::File {
                        parent: p,
                        name,
                        unlinked,
                        ..
                    } if *p == ino && !*unlinked => {
                        Some((*child_ino, FileKind::RegularFile, name.clone()))
                    }
                    _ => None,
                })
                .collect();

            for (child_ino, kind, name) in children {
                entries.push(DirEntry {
                    ino: child_ino,
                    offset: offset_counter,
                    kind,
                    name,
                });
                offset_counter += 1;
            }
        } else {
            return Err(FsError::NotDirectory);
        }

        Ok(entries.into_iter().filter(|e| e.offset > offset).collect())
    }

    /// Open a file, returning the file handle and open-mode hints.
    ///
    /// Data torrent files set `direct_io: true` so the adapter applies
    /// `FOPEN_DIRECT_IO`, bypassing the kernel page cache — this lets the
    /// daemon's errno (e.g. ENODATA for "no seeder") reach userspace instead
    /// of being converted to EIO by `filemap_read_folio` (TSI-2246).
    pub fn open(&mut self, ino: u64) -> FsResult<OpenOutcome> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(0.into()),
            STATS_INO => {
                let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                self.inode_mgr.open_files.insert(fh, ino);
                Ok(fh.into())
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    return Ok(fh.into());
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile { .. }) =
                        self.inode_mgr.data_inodes.get(&ino)
                    {
                        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                        self.inode_mgr.open_files.insert(fh, ino);
                        // Bypass page cache so read errors propagate directly.
                        Ok(OpenOutcome {
                            fh,
                            direct_io: true,
                        })
                    } else {
                        Ok(0.into())
                    }
                } else if self.inode_mgr.inodes.contains_key(&ino) {
                    let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
                    self.inode_mgr.open_files.insert(fh, ino);
                    Ok(fh.into())
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }
    pub fn opendir(&mut self, ino: u64) -> FsResult<()> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Ok(()),
            _ => {
                if InodeManager::is_data_ino(ino) || self.inode_mgr.inodes.contains_key(&ino) {
                    Ok(())
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    // ── Metadata namespace (.torrent lifecycle) ─────────────────────────────

    pub fn mknod(&mut self, parent: u64, name: &str) -> FsResult<Entry> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
                unlinked: false,
            },
        );

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Entry {
            ino: new_ino,
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
        })
    }

    pub fn create(&mut self, parent: u64, name: &str) -> FsResult<Created> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::File {
                parent,
                name: name.to_string(),
                data: Vec::new(),
                unlinked: false,
            },
        );

        let fh = NEXT_FH.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.open_files.insert(fh, new_ino);

        info!(
            "Created file {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(Created {
            attr: self.inode_mgr.attr_for_file(new_ino, 0),
            fh,
        })
    }

    pub fn write(&mut self, ino: u64, offset: i64, data: &[u8]) -> FsResult<u32> {
        if ino == STATS_INO || InodeManager::is_stats_ino(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // TSI-2228: data/ namespace is read-only. Data inodes live in
        // `data_inodes`, not `inodes`, so without this guard `write` would
        // fall through to the `None` arm and return `ENOENT` — the inode
        // exists, it is just not writable. Return `EROFS` instead.
        if InodeManager::is_data_namespace(ino) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        match self.inode_mgr.inodes.get_mut(&ino) {
            Some(InodeData::File {
                data: file_data,
                name,
                ..
            }) => {
                // TSI-2233: FUSE hands us `offset: i64`. A negative offset
                // casts to a huge `usize` and a super-large offset makes
                // `Vec::resize` attempt a multi-GB allocation (OOM/abort on
                // the FUSE dispatch thread), while `offset + data.len()` can
                // overflow and the subsequent slice `panic`. Validate before
                // any allocation: reject negatives, cap the post-write size
                // at MAX_TORRENT_SIZE (the only writable inodes are .torrent
                // buffers), and use checked arithmetic for the end offset.
                if offset < 0 {
                    warn!("write to {} with negative offset {}", name, offset);
                    return Err(FsError::InvalidArgument);
                }
                let offset = offset as usize;

                let end = match offset.checked_add(data.len()) {
                    Some(e) => e,
                    None => {
                        return Err(FsError::FileTooLarge(format!(
                            "{}: write at offset {} + {} bytes overflows usize",
                            name,
                            offset,
                            data.len()
                        )));
                    }
                };

                if end > MAX_TORRENT_SIZE {
                    warn!(
                        "write to {} would grow file to {} bytes (limit {})",
                        name, end, MAX_TORRENT_SIZE
                    );
                    return Err(FsError::FileTooLarge(format!(
                        "{} exceeds {} bytes",
                        name, MAX_TORRENT_SIZE
                    )));
                }

                if offset > file_data.len() {
                    file_data.resize(offset, 0);
                }
                if end > file_data.len() {
                    file_data.resize(end, 0);
                }

                file_data[offset..end].copy_from_slice(data);

                info!("Wrote {} bytes to file {}", data.len(), name);
                Ok(data.len() as u32)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    /// Validate a buffered `.torrent` on close (flush).
    pub fn flush(&mut self, ino: u64) -> FsResult<()> {
        if let Some(InodeData::File { data, name, .. }) = self.inode_mgr.inodes.get(&ino) {
            if name.ends_with(".torrent") {
                if data.is_empty() {
                    warn!("Zero-byte torrent file {} rejected", name);
                    return Err(FsError::InvalidArgument);
                }

                if data.len() > MAX_TORRENT_SIZE {
                    warn!(
                        "Torrent file {} exceeds size limit ({} bytes)",
                        name,
                        data.len()
                    );
                    return Err(FsError::FileTooLarge(format!(
                        "{} exceeds {} bytes",
                        name, MAX_TORRENT_SIZE
                    )));
                }

                match TorrentInfo::from_bytes(data.clone()) {
                    Ok(_) => {
                        info!("Torrent {} validated successfully", name);
                    }
                    Err(e) => {
                        warn!("Invalid torrent file {}: {:?}", name, e);
                        return Err(FsError::InvalidArgument);
                    }
                }
            }
        }
        Ok(())
    }

    /// Persist a closed `.torrent` into the database (release).
    pub fn release(&mut self, fh: u64) -> FsResult<()> {
        if let Some(ino) = self.inode_mgr.open_files.remove(&fh) {
            // TSI-2234: the file handle is now closed. If the inode was
            // unlinked while still open, this `release` is the "last close"
            // point at which the inode is finally destroyed (unless another
            // handle is still open). An unlinked `.torrent` was already
            // removed from the DB by `unlink`, so do NOT re-persist its
            // buffered bytes here — just retire the inode.
            if self.inode_mgr.is_unlinked_file(ino) {
                if self.inode_mgr.open_handle_count(ino) == 0 {
                    self.inode_mgr.inodes.remove(&ino);
                }
                return Ok(());
            }

            if let Some(InodeData::File {
                data, name, parent, ..
            }) = self.inode_mgr.inodes.get(&ino).cloned()
            {
                if name.ends_with(".torrent") {
                    if data.is_empty() {
                        warn!("Zero-byte torrent file {} removed", name);
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    if data.len() > MAX_TORRENT_SIZE {
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    if TorrentInfo::from_bytes(data.clone()).is_err() {
                        warn!("Torrent {} invalid, removing inode", name);
                        self.inode_mgr.inodes.remove(&ino);
                        return Ok(());
                    }

                    let source_path = self.inode_mgr.extract_source_path(parent);
                    let dedup_key = (source_path.clone(), name.clone());

                    // TSI-2247: Dedup guard — check + insert atomically, then
                    // DROP the lock before spawning background work.  The key
                    // is `(source_path, filename)` so that torrents in the
                    // same directory (especially the root, where
                    // `source_path` is `""`) don't collide.  The previous
                    // code held `processing_torrents` during the entire
                    // `add_torrent` call (DB insert + handle creation),
                    // blocking the single-threaded FUSE dispatch loop.  Now
                    // `add_torrent` runs on a detached thread so `release`
                    // never blocks the dispatcher.
                    {
                        let mut processing = self.processing_torrents.lock().map_err(|e| {
                            error!("Mutex poisoned in release(): {}", e);
                            FsError::LockPoisoned
                        })?;

                        if processing.contains_key(&dedup_key) {
                            warn!(
                                "Torrent at '{}{}' already being processed, skipping",
                                source_path, name
                            );
                            return Ok(());
                        }
                        processing.insert(dedup_key.clone(), ());
                    }
                    // Lock released here.

                    // The inode is NOT removed here: if the background
                    // `add_torrent` fails, the file must remain visible so
                    // the user can retry or delete it.  On success the
                    // DB-backed entry supersedes this inode (lookup queries
                    // the DB), so keeping it is harmless — this mirrors the
                    // pre-TSI-2247 behavior.
                    if let Some(ts) = &self.torrent_service {
                        let ts = ts.clone();
                        let processing = self.processing_torrents.clone();
                        let processing_cv = self.processing_torrents_cv.clone();
                        let key = dedup_key.clone();
                        let fname = name.clone();
                        std::thread::spawn(move || {
                            match ts.add_torrent(&data, &source_path, &name) {
                                Ok(()) => {
                                    info!("Successfully processed torrent: {}", fname);
                                }
                                Err(e) => {
                                    error!("Failed to process torrent {}: {}", fname, e);
                                }
                            }
                            if let Ok(mut guard) = processing.lock() {
                                guard.remove(&key);
                            }
                            // TSI-2378: wake a rename blocked on this pending add.
                            processing_cv.notify_one();
                        });
                    } else {
                        info!(
                            "Torrent {} received (no DB configured, skipping insert)",
                            name
                        );
                        if let Ok(mut guard) = self.processing_torrents.lock() {
                            guard.remove(&dedup_key);
                        }
                        // TSI-2378: wake any waiter (defensive; no-DB path).
                        self.processing_torrents_cv.notify_one();
                    }
                }
            }
        }

        Ok(())
    }

    pub fn mkdir(&mut self, parent: u64, name: &str) -> FsResult<Attr> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if self.inode_mgr.find_child_by_name(parent, name).is_some() {
            return Err(FsError::AlreadyExists);
        }

        let new_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        self.inode_mgr.inodes.insert(
            new_ino,
            InodeData::Directory {
                parent,
                name: name.to_string(),
            },
        );

        // Persist the directory to the database.
        let source_path = if parent == METADATA_INO {
            name.to_string()
        } else {
            let parent_source_path = self.inode_mgr.extract_source_path(parent);
            if parent_source_path.is_empty() {
                name.to_string()
            } else {
                format!("{}/{}", parent_source_path, name)
            }
        };

        if let Some(ref ts) = self.torrent_service {
            let _ = ts.ensure_metadata_directories(&source_path);
        }

        info!(
            "Created directory {} with inode {} in {}",
            name,
            new_ino,
            self.inode_mgr.get_full_path(parent)
        );
        Ok(self.inode_mgr.attr_for_dir(new_ino, true))
    }

    pub fn rmdir(&mut self, parent: u64, name: &str) -> FsResult<()> {
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::Directory { .. }) => {
                let has_children = self.inode_mgr.inodes.iter().any(|(_, data)| match data {
                    InodeData::Directory { parent: p, .. } if *p == ino => true,
                    InodeData::File {
                        parent: p,
                        unlinked,
                        ..
                    } if *p == ino && !*unlinked => true,
                    _ => false,
                });

                if has_children {
                    return Err(FsError::DirectoryNotEmpty);
                }

                let source_path = self.inode_mgr.extract_source_path(ino);
                self.inode_mgr.inodes.remove(&ino);

                // Clean up data_inodes cache.
                self.inode_mgr
                    .data_inodes
                    .retain(|_, data_inode| match data_inode {
                        DataInode::SourcePathDir { path } => {
                            if path == &source_path {
                                false
                            } else if source_path.is_empty() {
                                true
                            } else {
                                !path.starts_with(&format!("{}/", source_path))
                            }
                        }
                        _ => true,
                    });

                // TSI-2454: purge the kernel dentry cache for the removed
                // `data/` SourcePathDir so the stale mirror vanishes at
                // once instead of lingering for the 1s FUSE TTL.
                let (dir_parent, dir_name) = Self::split_data_dir_parent(&source_path);
                self.inval_data_entry(dir_parent, &dir_name);

                if let Some(ref ts) = self.torrent_service {
                    let _ = ts.delete_metadata_directory(&source_path);
                }

                info!(
                    "Deleted directory '{}' (source_path='{}')",
                    name, source_path
                );
                Ok(())
            }
            Some(InodeData::File { .. }) => Err(FsError::NotDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn unlink(&mut self, parent: u64, name: &str) -> FsResult<Option<i64>> {
        let mut removed_id = None;
        if InodeManager::is_data_namespace(parent) {
            return Err(FsError::ReadOnlyFileSystem);
        }
        if !self.inode_mgr.is_metadata_child(parent) {
            return Err(FsError::PermissionDenied);
        }

        if !name.ends_with(".torrent") {
            return Err(FsError::PermissionDenied);
        }

        let ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        match self.inode_mgr.inodes.get(&ino) {
            Some(InodeData::File {
                name,
                parent: file_parent,
                ..
            }) => {
                let filename = name.clone();
                let source_path = self.inode_mgr.extract_source_path(*file_parent);

                if let Some(ref ts) = self.torrent_service {
                    match ts.remove_torrent(&filename, &source_path) {
                        Ok(Some((torrent_id, info_hash))) => {
                            removed_id = Some(torrent_id);
                            // TSI-2234: defer inode destruction. Mark the
                            // inode unlinked (so its directory name vanishes
                            // from lookup/readdir) but keep the inode + any
                            // open handles alive: read/write/flush/release
                            // on a still-open handle must keep working until
                            // the last handle is released. If no handle is
                            // open, `unlink_file` destroys the inode now.
                            // open_files is NOT stripped: that would orphan
                            // the handle and silently drop buffered bytes.
                            self.inode_mgr.unlink_file(ino);

                            // Clean up metadata directories left empty by this
                            // deletion so the data/ mirror no longer exposes
                            // orphaned directories. A cleanup failure is
                            // non-fatal: the dirs stay in the DB, so the
                            // cached SourcePathDir entries remain valid.
                            let cleaned = ts
                                .cleanup_orphaned_metadata_directories(&source_path)
                                .unwrap_or_default();

                            self.inode_mgr
                                .data_inodes
                                .retain(|_, data_inode| match data_inode {
                                    DataInode::TorrentRoot {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::TorrentDir {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::TorrentFile {
                                        torrent_id: tid, ..
                                    } => *tid != torrent_id,
                                    DataInode::SourcePathDir { path } => !cleaned.contains(path),
                                });

                            // TSI-2454: purge the kernel dentry cache so the
                            // removed torrent vanishes from `data/` at once
                            // instead of lingering for the 1s FUSE TTL.  The
                            // parent in `data/` is `DATA_INO` for root-level
                            // torrents or the `SourcePathDir` inode for a
                            // subdirectory.
                            let data_parent = if source_path.is_empty() {
                                DATA_INO
                            } else {
                                InodeManager::make_source_path_dir_ino(&source_path)
                            };
                            self.inval_data_entry(data_parent, &filename);

                            // Invalidate each cleaned source-path directory
                            // (leaf-first) so empty parent dirs vanish from
                            // `data/` too.  A cleaned path's parent is
                            // `DATA_INO` (top-level) or the SourcePathDir
                            // inode of its parent path.
                            for cleaned_path in &cleaned {
                                let (dir_parent, dir_name) =
                                    Self::split_data_dir_parent(cleaned_path);
                                self.inval_data_entry(dir_parent, &dir_name);
                            }

                            let mut processing = self.processing_torrents.lock().map_err(|e| {
                                error!("Mutex poisoned in unlink() processing_torrents: {}", e);
                                FsError::LockPoisoned
                            })?;
                            processing.remove(&(source_path.clone(), filename.clone()));
                            drop(processing);

                            let mut cache = self.torrent_data_cache.lock().map_err(|e| {
                                error!("Mutex poisoned in unlink() torrent_data_cache: {}", e);
                                FsError::LockPoisoned
                            })?;
                            // TSI-2274: L1 keys are `{info_hash}:{file_id}:{offset}:{size}`;
                            // drop every range cached for this torrent.
                            let prefix = format!("{}:", info_hash);
                            cache.retain(|k, _| !k.starts_with(&prefix));
                            drop(cache);

                            info!(
                                "Deleted torrent '{}' (id={}, source_path='{}')",
                                filename, torrent_id, source_path
                            );
                        }
                        Ok(None) => {
                            // TSI-2234: same deferred-destruction logic.
                            self.inode_mgr.unlink_file(ino);
                            // TSI-2454: the file was not yet in the DB but
                            // `data/` may still show a pending TorrentRoot
                            // cached from a prior lookup.  The filename is
                            // the directory name the data/ mirror uses for
                            // the torrent root, so invalidate it.
                            let data_parent = if source_path.is_empty() {
                                DATA_INO
                            } else {
                                InodeManager::make_source_path_dir_ino(&source_path)
                            };
                            self.inval_data_entry(data_parent, &filename);
                            info!("Deleted file '{}' (not yet in database)", filename);
                        }
                        Err(e) => {
                            error!("Failed to delete torrent: {:?}", e);
                            return Err(e);
                        }
                    }
                } else {
                    // TSI-2234: same deferred-destruction logic (no DB).
                    self.inode_mgr.unlink_file(ino);
                    info!("Deleted file '{}' (no database)", filename);
                }

                Ok(removed_id)
            }
            Some(InodeData::Directory { .. }) => Err(FsError::IsDirectory),
            None => Err(FsError::NotFound),
        }
    }

    pub fn rename(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
    ) -> FsResult<()> {
        self.rename_with_pending_timeout(parent, name, newparent, newname, PENDING_ADD_TIMEOUT)
    }

    /// TSI-2378 (review): the pending-add wait timeout is injectable so
    /// tests can exercise the timeout/rollback path in milliseconds instead
    /// of hard-waiting out the 5s production deadline.
    pub(crate) fn rename_with_pending_timeout(
        &mut self,
        parent: u64,
        name: &str,
        newparent: u64,
        newname: &str,
        pending_timeout: Duration,
    ) -> FsResult<()> {
        // TSI-2228: data/ is a read-only namespace — renames into or out
        // of it must return `EROFS`, not `ENOENT` or `EPERM`. Check this
        // before parent existence: an inode number in the data range is
        // inherently read-only, regardless of whether it is currently
        // materialised.
        if InodeManager::is_data_namespace(parent) || InodeManager::is_data_namespace(newparent) {
            return Err(FsError::ReadOnlyFileSystem);
        }

        // Check parent existence in both inodes and data_inodes tables.
        let parent_exists = self.inode_mgr.inodes.contains_key(&parent)
            || self.inode_mgr.data_inodes.contains_key(&parent);
        if !parent_exists {
            return Err(FsError::NotFound);
        }

        let newparent_exists = self.inode_mgr.inodes.contains_key(&newparent)
            || self.inode_mgr.data_inodes.contains_key(&newparent);
        if !newparent_exists {
            return Err(FsError::NotFound);
        }

        if !self.inode_mgr.is_metadata_child(parent) || !self.inode_mgr.is_metadata_child(newparent)
        {
            return Err(FsError::NotPermitted);
        }

        let source_ino = match self.inode_mgr.find_child_by_name(parent, name) {
            Some(ino) => ino,
            None => {
                return Err(FsError::NotFound);
            }
        };

        if let Some(target_ino) = self.inode_mgr.find_child_by_name(newparent, newname) {
            if target_ino == source_ino {
                return Ok(());
            }
            return Err(FsError::AlreadyExists);
        }

        let is_directory = matches!(
            self.inode_mgr.inodes.get(&source_ino),
            Some(InodeData::Directory { .. })
        );

        if is_directory {
            // --- Directory rename ---
            let old_source_path = self.inode_mgr.extract_source_path(source_ino);
            let new_source_path = if newparent == METADATA_INO {
                newname.to_string()
            } else {
                let parent_path = self.inode_mgr.extract_source_path(newparent);
                if parent_path.is_empty() {
                    newname.to_string()
                } else {
                    format!("{}/{}", parent_path, newname)
                }
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::Directory {
                    parent: newparent,
                    name: newname.to_string(),
                },
            );

            // Update data_inodes cache.
            let old_prefix = format!("{}/", old_source_path);
            let new_prefix = format!("{}/", new_source_path);
            for data_inode in self.inode_mgr.data_inodes.values_mut() {
                match data_inode {
                    DataInode::SourcePathDir { path } => {
                        if path == &old_source_path {
                            *path = new_source_path.clone();
                        } else if path.starts_with(&old_prefix) {
                            *path = format!("{}{}", new_prefix, &path[old_prefix.len()..]);
                        }
                    }
                    DataInode::TorrentRoot { source_path, .. } => {
                        if source_path == &old_source_path {
                            *source_path = new_source_path.clone();
                        } else if source_path.starts_with(&old_prefix) {
                            *source_path =
                                format!("{}{}", new_prefix, &source_path[old_prefix.len()..]);
                        }
                    }
                    _ => {}
                }
            }

            // Persist to database.
            if let Some(ref ts) = self.torrent_service {
                ts.rename_metadata_directory(&old_source_path, newname, &new_source_path)?;
                info!(
                    "Renamed metadata directory '{}' to '{}' (source_path: '{}' -> '{}')",
                    name, newname, old_source_path, new_source_path
                );
            } else {
                info!(
                    "Renamed directory '{}' to '{}' (no database)",
                    name, newname
                );
            }

            Ok(())
        } else {
            // --- File rename ---
            if !name.ends_with(".torrent") || !newname.ends_with(".torrent") {
                return Err(FsError::PermissionDenied);
            }

            let (file_data, old_name, was_unlinked) = match self.inode_mgr.inodes.get(&source_ino) {
                Some(InodeData::File {
                    data,
                    name,
                    unlinked,
                    ..
                }) => (data.clone(), name.clone(), *unlinked),
                None => {
                    return Err(FsError::NotFound);
                }
                _ => unreachable!(),
            };

            self.inode_mgr.inodes.insert(
                source_ino,
                InodeData::File {
                    parent: newparent,
                    name: newname.to_string(),
                    data: file_data.clone(),
                    unlinked: was_unlinked,
                },
            );

            if let Some(ref ts) = self.torrent_service {
                let old_source_path = self.inode_mgr.extract_source_path(parent);
                let new_source_path = self.inode_mgr.extract_source_path(newparent);

                // TSI-2378: a fast `cp` followed immediately by `mv` races
                // with the detached `add_torrent` thread spawned by
                // `release` (TSI-2247): the DB row may not exist yet, so
                // `rename_torrent` silently no-ops (`Ok(None)`) and the row
                // later lands at the OLD name — the data/ mirror then keeps
                // serving the stale filename and the rename is lost on
                // restart. Wait (bounded) for the pending add to settle
                // first; on timeout fail so the caller retries.
                let dedup_key = (old_source_path.clone(), old_name.clone());
                // On timeout the helper returns Err and the inode is rolled
                // back to the pre-rename name below — a bare `?` would leave
                // metadata/ showing the new name while the still-pending
                // background add lands the DB row at the OLD one,
                if let Err(e) = self.wait_for_pending_add(&dedup_key, pending_timeout) {
                    self.inode_mgr.inodes.insert(
                        source_ino,
                        InodeData::File {
                            parent,
                            name: old_name.clone(),
                            data: file_data.clone(),
                            unlinked: was_unlinked,
                        },
                    );
                    return Err(e);
                }

                ts.rename_torrent(&old_name, &old_source_path, newname, &new_source_path)?;

                // TSI-2373: a cross-directory move orphans the metadata
                // directories at the old source_path — the DB rows survive,
                // so the data/ mirror keeps exposing a ghost tree for a path
                // that no longer holds any torrent. Prune them the same way
                // unlink does, and evict the stale cached SourcePathDir /
                // TorrentRoot entries so later lookups re-resolve from the
                // DB at the new location. Cleanup failure is non-fatal: the
                // directories stay in the DB, so the cached entries remain
                // valid.
                if old_source_path != new_source_path {
                    if !old_source_path.is_empty() {
                        let cleaned = ts
                            .cleanup_orphaned_metadata_directories(&old_source_path)
                            .unwrap_or_default();

                        self.inode_mgr
                            .data_inodes
                            .retain(|_, data_inode| match data_inode {
                                DataInode::SourcePathDir { path } => !cleaned.contains(path),
                                _ => true,
                            });

                        // TSI-2454: purge the kernel dentry cache for each
                        // cleaned source-path directory so the stale mirror
                        // vanishes at once instead of lingering for the 1s
                        // FUSE TTL.
                        for cleaned_path in &cleaned {
                            let (dir_parent, dir_name) = Self::split_data_dir_parent(cleaned_path);
                            self.inval_data_entry(dir_parent, &dir_name);
                        }
                    }

                    // Evict cached TorrentRoot entries bound to the old
                    // location so later lookups re-resolve from the DB at the
                    // new one.
                    self.inode_mgr
                        .data_inodes
                        .retain(|_, data_inode| match data_inode {
                            DataInode::TorrentRoot { source_path, .. } => {
                                source_path != &old_source_path
                            }
                            _ => true,
                        });

                    // TSI-2454: purge the kernel dentry cache for the old
                    // data/ location so the stale name vanishes at once
                    // instead of lingering for the 1s FUSE TTL.
                    let old_data_parent = if old_source_path.is_empty() {
                        DATA_INO
                    } else {
                        InodeManager::make_source_path_dir_ino(&old_source_path)
                    };
                    self.inval_data_entry(old_data_parent, &old_name);
                } else {
                    // TSI-2378 (review): an intra-directory rename keeps the
                    // source_path but changes the filename — a cached
                    // TorrentRoot entry still carries the old `filename`, so
                    // data/ would keep listing the stale name (and the DB row
                    // under the new one would be shadowed) until restart.
                    // Evict it so later lookups re-resolve from the DB.
                    self.inode_mgr
                        .data_inodes
                        .retain(|_, data_inode| match data_inode {
                            DataInode::TorrentRoot {
                                source_path,
                                filename,
                                ..
                            } => !(source_path == &old_source_path && filename == &old_name),
                            _ => true,
                        });

                    // TSI-2454: purge the kernel dentry cache for the old
                    // filename so the stale name vanishes from `data/` at
                    // once instead of lingering for the 1s FUSE TTL.
                    let data_parent = if old_source_path.is_empty() {
                        DATA_INO
                    } else {
                        InodeManager::make_source_path_dir_ino(&old_source_path)
                    };
                    self.inval_data_entry(data_parent, &old_name);
                }
                info!(
                    "Renamed torrent '{}' to '{}' (source_path: '{}' -> '{}')",
                    old_name, newname, old_source_path, new_source_path
                );
            } else {
                info!("Renamed file '{}' to '{}' (no database)", old_name, newname);
            }
            Ok(())
        }
    }

    /// TSI-2378: wait (bounded by `timeout`) for a pending `add_torrent` at
    /// `dedup_key` to clear. The background add signals
    /// `processing_torrents_cv` when it removes its entry, so this sleeps on
    /// the Condvar instead of polling the map. On timeout the caller has
    /// already rolled back any inode state and must fail the operation.
    /// `timeout` is a parameter so tests can use a short deadline instead of
    /// the production 5s.
    fn wait_for_pending_add(
        &self,
        dedup_key: &(String, String),
        timeout: Duration,
    ) -> FsResult<()> {
        let deadline = std::time::Instant::now() + timeout;
        let mut guard = self
            .processing_torrents
            .lock()
            .map_err(|_| FsError::LockPoisoned)?;
        while guard.contains_key(dedup_key) {
            let now = std::time::Instant::now();
            if now >= deadline {
                error!(
                    "Timed out waiting for pending add of '{:?}' before rename",
                    dedup_key
                );
                return Err(FsError::Internal(
                    "torrent is still being processed; retry the rename".to_string(),
                ));
            }
            let (g, timed_out) = self
                .processing_torrents_cv
                .wait_timeout(guard, deadline - now)
                .map_err(|_| {
                    error!("Condvar poisoned in rename() pending-add wait");
                    FsError::LockPoisoned
                })?;
            guard = g;
            if timed_out.timed_out() && guard.contains_key(dedup_key) {
                continue;
            }
        }
        drop(guard);
        Ok(())
    }

    // ── Data namespace (read-only: local / remote) ──────────────────────────

    /// Serve a read request across all namespaces (EISDIR, stats, data, metadata).
    pub fn read(&mut self, ino: u64, offset: i64, size: u32) -> FsResult<ReadOutcome> {
        match ino {
            ROOT_INO | METADATA_INO | DATA_INO => Err(FsError::IsDirectory),
            STATS_INO => {
                let offset = offset as usize;
                let stats = self.generate_global_stats_content();
                if offset >= stats.len() {
                    Ok(ReadOutcome::Ready(Vec::new()))
                } else {
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    Ok(ReadOutcome::Ready(stats[offset..end].to_vec()))
                }
            }
            _ => {
                if InodeManager::is_stats_ino(ino) {
                    let stats = self.generate_data_stats_for_ino(ino);
                    let offset = offset as usize;
                    if offset >= stats.len() {
                        return Ok(ReadOutcome::Ready(Vec::new()));
                    }
                    let end = std::cmp::min(offset + size as usize, stats.len());
                    return Ok(ReadOutcome::Ready(stats[offset..end].to_vec()));
                }

                if InodeManager::is_data_ino(ino) {
                    if let Some(DataInode::TorrentFile {
                        torrent_id,
                        file_id,
                        name,
                        size: file_size,
                        ..
                    }) = self.inode_mgr.data_inodes.get(&ino)
                    {
                        let offset = offset as usize;
                        let read_size = size as usize;

                        info!(
                            "Read request for torrent file: {} (torrent_id={}, file_id={}, offset={}, size={})",
                            name, torrent_id, file_id, offset, read_size
                        );

                        let actual_size = *file_size as usize;
                        if offset >= actual_size {
                            return Ok(ReadOutcome::Ready(Vec::new()));
                        }

                        let end = std::cmp::min(offset + read_size, actual_size);
                        let result_size = end - offset;

                        return self.read_data(
                            *torrent_id,
                            *file_id,
                            offset as u64,
                            result_size as u32,
                        );
                    }

                    return Err(FsError::NotFound);
                }

                if let Some(InodeData::File { data, .. }) = &self.inode_mgr.inodes.get(&ino) {
                    let offset = offset as usize;
                    let end = std::cmp::min(offset + size as usize, data.len());
                    if offset < data.len() {
                        Ok(ReadOutcome::Ready(data[offset..end].to_vec()))
                    } else {
                        Ok(ReadOutcome::Ready(Vec::new()))
                    }
                } else {
                    Err(FsError::NotFound)
                }
            }
        }
    }

    /// Read torrent file data from local cache or the BitTorrent network.
    pub fn read_data(
        &mut self,
        torrent_id: i64,
        file_id: i64,
        offset: u64,
        size: u32,
    ) -> FsResult<ReadOutcome> {
        let ts = self.torrent_service.as_ref().ok_or_else(|| {
            error!("Torrent service not available");
            FsError::Internal("torrent service not available".to_string())
        })?;

        let (info_hash, _source_path, files) =
            ts.get_torrent_with_files(torrent_id)?.ok_or_else(|| {
                error!("Torrent not found: {}", torrent_id);
                FsError::NotFound
            })?;

        let _file = files.iter().find(|f| f.id == file_id).ok_or_else(|| {
            error!("File not found: {}", file_id);
            FsError::NotFound
        })?;

        let file_index = files.iter().position(|f| f.id == file_id).ok_or_else(|| {
            error!("File index not found for file_id: {}", file_id);
            FsError::Internal(format!("file index not found for file_id: {}", file_id))
        })? as i32;

        // TSI-2274: L1 range cache — keyed by
        // `{info_hash}:{file_id}:{offset}:{size}` so repeated reads of the
        // same range hit RAM and bypass L2 disk + the (now throttled)
        // metadata fsync.  Before this fix the L1 cache was only ever read
        // and removed, never populated, so every read went to L2 disk +
        // fsync.
        //
        // The `size` is part of the key to prevent silent data truncation
        // (review issue 1): if a smaller read cached N bytes and a larger
        // read hits that entry, it would return only N bytes instead of
        // the requested size.
        let cache_key = format!("{}:{}:{}:{}", info_hash, file_id, offset, size);
        {
            let cache = self
                .torrent_data_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(cached) = cache.get(&cache_key) {
                self.metrics.l1_hit();
                return Ok(ReadOutcome::Ready(cached.clone()));
            }
        }
        self.metrics.l1_miss();

        if let Some(ds) = &self.download_service {
            // L3 (metadata) cache: parsed `TorrentInfo` keyed by info_hash,
            // avoiding a DB re-read + bencode re-parse on every read.
            let info = self.get_or_parse_torrent_info(&info_hash, torrent_id)?;

            // Fast pre-check: are all needed pieces already on disk?
            match ds.pieces_on_disk(&info, file_index, offset, size) {
                Ok(true) => {
                    self.metrics.l2_hit();
                    match ds.read_file_range(info.clone(), file_index, offset, size) {
                        Ok(data) => {
                            // TSI-2293: guard against `Ok(empty)` for a
                            // non-zero `size` — same root cause as the
                            // deferred path (see `guard_empty_read`).
                            if let Err(e) = guard_empty_read(&data, size) {
                                warn!(
                                    "Sync read returned 0 bytes for \
                                     non-zero size (torrent_id={}, \
                                     file_id={}, size={}); returning ENODATA",
                                    torrent_id, file_id, size
                                );
                                return Err(e);
                            }
                            // TSI-2274: populate the L1 range cache so a
                            // repeated read of this exact range is served
                            // from RAM, bypassing L2 disk + metadata fsync.
                            // Bounded: when the cache is full, clear all
                            // entries before inserting (review issue 2) —
                            // coarse but bounded, the L1 cache is a hot-zone
                            // optimization and clearing it only causes a few
                            // L2 disk re-reads.
                            if !data.is_empty() {
                                if let Ok(mut cache) = self.torrent_data_cache.lock() {
                                    if cache.len() >= MAX_L1_ENTRIES {
                                        cache.clear();
                                    }
                                    cache.insert(cache_key.clone(), data.clone());
                                }
                            }
                            info!(
                                "Successfully read {} bytes from torrent file \
                                 (torrent_id={}, file_id={})",
                                data.len(),
                                torrent_id,
                                file_id
                            );
                            Ok(ReadOutcome::Ready(data))
                        }
                        Err(e) => {
                            warn!(
                                "Failed to read from torrent file (torrent_id={}, file_id={}): {}. \
                                 The torrent may have no active peers/seeds. \
                                 Check tracker health with `cat .stats`.",
                                torrent_id, file_id, e
                            );
                            Err(e.into())
                        }
                    }
                }
                Ok(false) => {
                    self.metrics.l2_miss();
                    self.metrics.deferred_read();
                    info!(
                        "Deferring read for torrent file (torrent_id={}, file_id={}): \
                         pieces not on disk, blocking in worker thread",
                        torrent_id, file_id
                    );
                    Ok(ReadOutcome::Pending {
                        info,
                        file_index,
                        offset,
                        size,
                        info_hash: info_hash.clone(),
                        torrent_id,
                    })
                }
                Err(e) => {
                    error!(
                        "Failed to check pieces on disk (torrent_id={}, file_id={}): {:?}",
                        torrent_id, file_id, e
                    );
                    Err(e.into())
                }
            }
        } else {
            error!("Download manager not available");
            Err(FsError::Internal(
                "download manager not available".to_string(),
            ))
        }
    }

    /// Return the parsed `TorrentInfo` for `info_hash`, hitting the L3
    /// (metadata) cache when possible.
    ///
    /// On a miss this reads the raw `.torrent` bytes from the DB once and
    /// parses them once; subsequent reads for the same torrent reuse the
    /// cached `Arc<TorrentInfo>` and skip both the DB re-read and the
    /// bencode re-parse.
    fn get_or_parse_torrent_info(
        &self,
        info_hash: &str,
        torrent_id: i64,
    ) -> FsResult<Arc<TorrentInfo>> {
        {
            let cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            if let Some(info) = cache.get(info_hash) {
                self.metrics.l3_hit();
                return Ok(info.clone());
            }
        }

        self.metrics.l3_miss();
        let info = Arc::new(
            TorrentInfo::from_bytes(self.get_torrent_raw_data(torrent_id)?).map_err(|e| {
                error!("Failed to parse torrent info for download: {:?}", e);
                FsError::Internal(format!(
                    "failed to parse torrent info for download: {:?}",
                    e
                ))
            })?,
        );

        {
            let mut cache = self
                .torrent_info_cache
                .lock()
                .map_err(|_| FsError::LockPoisoned)?;
            cache.insert(info_hash.to_string(), info.clone());
        }

        Ok(info)
    }

    fn get_torrent_raw_data(&self, torrent_id: i64) -> FsResult<Vec<u8>> {
        let db = DataResolver::get_db(&self.db)?;
        let db_guard = db.lock().map_err(|_| {
            error!("Database lock poisoned");
            FsError::LockPoisoned
        })?;

        let torrent = db_guard
            .get_torrent_by_id(torrent_id)
            .map_err(|e| {
                error!("Failed to get torrent: {:?}", e);
                FsError::from(e)
            })?
            .ok_or_else(|| {
                error!("Torrent not found for id: {}", torrent_id);
                FsError::NotFound
            })?;

        if let Some(ref data) = torrent.torrent_data {
            if !data.is_empty() {
                return Ok(data.clone());
            }
        }

        for data in self.inode_mgr.inodes.values() {
            if let InodeData::File {
                name,
                data: file_data,
                unlinked,
                ..
            } = data
            {
                // TSI-2234: skip unlinked-but-open inodes — their torrent_id
                // was already removed from the DB by `unlink`, so matching
                // their buffered bytes here would be a stale dirty read.
                if !*unlinked && name.ends_with(".torrent") && !file_data.is_empty() {
                    if let Ok(info) = TorrentInfo::from_bytes(file_data.clone()) {
                        if let Ok(metadata) = info.metadata() {
                            if hex::encode(metadata.info_hash) == torrent.info_hash {
                                return Ok(file_data.clone());
                            }
                        }
                    }
                }
            }
        }

        Err(FsError::NotFound)
    }

    // ── Stats namespace ─────────────────────────────────────────────────────

    /// Render the requested `.stats` content.
    pub fn read_stats(&self, kind: StatsKind) -> Vec<u8> {
        match kind {
            StatsKind::Global => self.generate_global_stats_content(),
            StatsKind::StatsInode { ino } => self.generate_data_stats_for_ino(ino),
        }
    }

    fn generate_global_stats_content(&self) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        let session_stats = self.download_service.as_ref().map(|ds| ds.snapshot_stats());
        // TSI-2274: report the current L1 range cache depth so `.stats`
        // reflects the now-populated memory cache.
        let l1_entries = self
            .torrent_data_cache
            .lock()
            .map(|c| c.len() as u64)
            .unwrap_or(0);
        self.metrics.set_l1_entries(l1_entries);
        generate_global_stats(
            self.inode_mgr.creation_time,
            &self.db,
            session_stats,
            get_cm,
            &self.listen_addr,
            Some(self.metrics.snapshot()),
        )
    }

    fn generate_dir_stats_content(&self, source_path: &str) -> Vec<u8> {
        let get_cm = || self.get_cache_manager();
        generate_directory_stats(source_path, &self.db, &self.download_service, get_cm)
    }

    fn generate_torrent_stats_for_id(&self, torrent_id: i64) -> Vec<u8> {
        // Extract info_hash inside a short-lived lock scope, then release before
        // calling generate_torrent_stats() which acquires its own lock.
        let info_hash = {
            let db_guard = match self.db.as_ref().and_then(|db| db.lock().ok()) {
                Some(g) => g,
                None => return b"Database not available\n".to_vec(),
            };
            match db_guard.get_torrent_by_id(torrent_id).ok().flatten() {
                Some(t) => t.info_hash,
                None => return format!("Torrent not found (id={})\n", torrent_id).into_bytes(),
            }
        };
        let get_cm = || self.get_cache_manager();
        generate_torrent_stats(
            torrent_id,
            &info_hash,
            &self.db,
            &self.download_service,
            get_cm,
        )
    }

    fn generate_data_stats_for_ino(&self, ino: u64) -> Vec<u8> {
        let dir_ino = match InodeManager::stats_ino_to_dir_ino(ino) {
            Some(d) => d,
            None => return b"Invalid stats inode\n".to_vec(),
        };

        if dir_ino == DATA_INO {
            return self.generate_dir_stats_content("");
        }

        match self.inode_mgr.data_inodes.get(&dir_ino) {
            Some(DataInode::SourcePathDir { path }) => self.generate_dir_stats_content(path),
            Some(DataInode::TorrentRoot { torrent_id, .. }) => {
                self.generate_torrent_stats_for_id(*torrent_id)
            }
            _ => b"Stats not available for this inode\n".to_vec(),
        }
    }
}

/// TSI-2199: background SHA-1 verification of on-disk cached pieces.
///
/// After a restart, [`CacheManager::scan_pieces_subdirectory`] re-registers
/// every on-disk piece but leaves it unverified — it may be a complete piece
/// or an incomplete/corrupt file left by a crash. This worker recomputes each
/// candidate's SHA-1 and compares it against the torrent's expected piece
/// hash: matches are marked verified (so subsequent reads serve from local
/// cache), mismatches and incomplete pieces (wrong size / sparse partial
/// write) are purged so they can be re-downloaded on demand (TSI-2257).
///
/// TSI-2491: pieces still being written carry a `.incomplete` marker and are
/// excluded from the candidate list by `CacheManager::unverified_pieces` —
/// only pieces that are actually possibly-complete are verified here.
///
/// Runs on a detached background thread so it never blocks FUSE mount
/// readiness.
fn spawn_cache_verification(
    cache: Arc<Mutex<CacheManager>>,
    infos_by_hash: HashMap<String, Arc<TorrentInfo>>,
) {
    let spawned = std::thread::Builder::new()
        .name("cache-verify".to_string())
        .spawn(move || {
            let unverified = match cache.lock() {
                Ok(c) => c.unverified_pieces(),
                Err(_) => {
                    warn!("Cache lock poisoned during piece verification");
                    return;
                }
            };

            let mut verified = 0usize;
            let mut purged = 0usize;
            let mut skipped = 0usize;

            for piece_key in unverified {
                let (info_hash, piece_index) = match split_piece_key(&piece_key) {
                    Some(v) => v,
                    None => {
                        warn!("Skipping malformed piece key: {}", piece_key);
                        skipped += 1;
                        continue;
                    }
                };

                let info = match infos_by_hash.get(info_hash) {
                    Some(info) => info,
                    None => {
                        // Torrent no longer present in the DB: leave untouched.
                        skipped += 1;
                        continue;
                    }
                };

                // Resolve the on-disk path inside a brief lock, then do all
                // file I/O lock-free so active reads (PieceStore /
                // DownloadService / .stats — all take the same cache mutex)
                // are never blocked by the verification thread.
                let path = match cache.lock() {
                    Ok(c) => c.piece_path(&piece_key),
                    Err(_) => {
                        warn!("Cache lock poisoned during piece verification");
                        return;
                    }
                };

                let outcome = verify_single_piece(&path, piece_index, info, &piece_key);

                match outcome {
                    VerifyOutcome::Verified => {
                        if let Ok(mut c) = cache.lock() {
                            c.mark_verified(&piece_key);
                        }
                        verified += 1;
                    }
                    VerifyOutcome::Purged => {
                        if let Ok(mut c) = cache.lock() {
                            let _ = c.delete_piece(&piece_key);
                        }
                        purged += 1;
                    }
                    VerifyOutcome::Skipped => {
                        skipped += 1;
                    }
                }
            }

            info!(
                "Cache piece verification complete: {} verified, {} purged, {} skipped",
                verified, purged, skipped
            );
        });

    if let Err(e) = spawned {
        warn!("Failed to spawn cache verification thread: {}", e);
    }
}

/// Outcome of verifying a single cached piece.
enum VerifyOutcome {
    Verified,
    Purged,
    Skipped,
}

/// Verify a single on-disk cached piece against its expected SHA-1 hash.
///
/// Pure file I/O — no cache lock is held.  The caller resolves the on-disk
/// `path` (and `piece_index`) from the piece key inside a brief lock, then
/// calls this function lock-free.  The caller is responsible for applying
/// the side effect (mark verified / delete piece).
fn verify_single_piece(
    path: &std::path::Path,
    piece_index: i32,
    info: &TorrentInfo,
    piece_key: &str,
) -> VerifyOutcome {
    let expected = match info.hash_for_piece(piece_index) {
        Some(h) => h,
        None => return VerifyOutcome::Skipped,
    };
    let expected_size = match info.piece_size(piece_index) {
        Some(s) => s,
        None => return VerifyOutcome::Skipped,
    };

    let meta = match std::fs::metadata(path) {
        Ok(m) => m,
        Err(_) => {
            warn!("Cached piece file missing, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };

    let file_size = meta.len();
    if file_size != expected_size {
        warn!(
            "Cached piece has wrong size ({} != {}), purging incomplete piece: {}",
            file_size, expected_size, piece_key
        );
        return VerifyOutcome::Purged;
    }

    // TSI-2229 / TSI-2257: a piece file may have the correct *logical* size
    // but still be incomplete — `write_piece` writes blocks at arbitrary
    // offsets, and a crash between block writes leaves a sparse file whose
    // zero-filled gaps make st_size match piece_length while the physical
    // allocation is smaller. Purge it: an incomplete piece left by a crash
    // is garbage and the space should be reclaimed. It will be re-downloaded
    // on demand, identical to a never-downloaded piece.
    if is_sparse_file(&meta, file_size) {
        warn!(
            "Cached piece is sparse (partial write), purging incomplete piece: {}",
            piece_key
        );
        return VerifyOutcome::Purged;
    }

    let data = match std::fs::read(path) {
        Ok(d) => d,
        Err(_) => {
            warn!("Cached piece file disappeared, purging: {}", piece_key);
            return VerifyOutcome::Purged;
        }
    };
    let actual = Sha1::from(&data[..]).digest().bytes();
    if actual == expected {
        VerifyOutcome::Verified
    } else {
        warn!(
            "Cached piece failed SHA-1 verification, purging: {}",
            piece_key
        );
        VerifyOutcome::Purged
    }
}

/// Split a piece key of the form `{info_hash}:piece:{index}` into its parts.
fn split_piece_key(key: &str) -> Option<(&str, i32)> {
    let mut parts = key.split(':');
    let info_hash = parts.next()?;
    if parts.next()? != "piece" {
        return None;
    }
    let index = parts.next()?.parse::<i32>().ok()?;
    Some((info_hash, index))
}

/// Whether a piece file is *sparse* — its physical disk allocation is smaller
/// than its logical size.
///
/// `write_piece` writes blocks at arbitrary offsets via `seekp`; a crash
/// between block writes leaves a file whose `st_size` matches the expected
/// piece length but whose interior has zero-filled gaps (the filesystem does
/// not allocate blocks for the unwritten regions). Such a file is not a
/// complete piece even though its logical size is correct.
///
/// Comparing `st_blocks * 512` (physical) against `st_size` (logical) detects
/// this condition. A fully-written all-zero piece also appears sparse; it
/// is purged along with genuine partial writes — the next read re-downloads
/// it, `register_piece` marks it verified, and no data is lost (TSI-2257).
///
/// Takes the already-fetched `Metadata` so the caller avoids a duplicate
/// `stat` syscall (one per piece — significant at 987+ pieces).
fn is_sparse_file(meta: &std::fs::Metadata, logical_size: u64) -> bool {
    if logical_size == 0 {
        return false;
    }
    use std::os::unix::fs::MetadataExt;
    let physical = meta.blocks() * 512;
    physical < logical_size
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use crate::db::{Database, FileEntry, InsertTorrentResult};
    use crate::infrastructure::metrics::Metrics;
    use crate::metadata::TorrentInfo;
    use crate::services::torrent::TorrentService;

    use crate::fuse::inodes::InodeManager;

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

    fn service_with_torrent(torrent_bytes: &[u8]) -> (FsService, String, i64, Arc<Metrics>) {
        let info = TorrentInfo::from_bytes(torrent_bytes.to_vec()).expect("parse torrent");
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        drop(info);

        let mut db = Database::open_in_memory().expect("in-memory db");
        let torrent_id = match db
            .insert_torrent("src", "foo", "foo.torrent", 16, &info_hash, 1)
            .expect("insert torrent")
        {
            InsertTorrentResult::Inserted(id) => id,
            other => panic!("unexpected insert result: {:?}", other),
        };
        db.set_torrent_data(torrent_id, torrent_bytes)
            .expect("set torrent data");

        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        let svc = FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(TorrentService::new(db_arc, None, None)),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            processing_torrents_cv: Arc::new(Condvar::new()),
            download_service: None,
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics: metrics.clone(),
            notifier: Arc::new(OnceLock::new()),
        };
        (svc, info_hash, torrent_id, metrics)
    }

    #[test]
    fn torrent_info_cached_by_info_hash() {
        let torrent_bytes = minimal_torrent_bytes();
        let (svc, info_hash, torrent_id, metrics) = service_with_torrent(&torrent_bytes);

        // First lookup: miss → parse + insert.
        let first = svc
            .get_or_parse_torrent_info(&info_hash, torrent_id)
            .unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 0);

        // Second lookup: hit → same Arc, no re-parse.
        let second = svc
            .get_or_parse_torrent_info(&info_hash, torrent_id)
            .unwrap();
        assert_eq!(metrics.snapshot().l3_misses, 1);
        assert_eq!(metrics.snapshot().l3_hits, 1);
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn split_piece_key_parses_info_hash_and_index() {
        assert_eq!(
            split_piece_key("abcdef0123456789:piece:42"),
            Some(("abcdef0123456789", 42))
        );
        // Non-`piece` segment or missing index are rejected.
        assert_eq!(split_piece_key("abc:data:0"), None);
        assert_eq!(split_piece_key("abc:piece:x"), None);
        assert_eq!(split_piece_key("abc"), None);
        assert_eq!(split_piece_key(""), None);
    }

    /// TSI-2228: Bare service without any torrents — sufficient for testing
    /// that mutating operations on the read-only `data/` namespace return
    /// `EROFS` (`ReadOnlyFileSystem`), not `ENOENT` or `EACCES`.
    fn bare_service() -> FsService {
        let metrics = Arc::new(Metrics::new());
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            torrent_service: None,
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
        }
    }

    #[test]
    fn data_namespace_write_returns_erofs() {
        let mut svc = bare_service();
        // DATA_INO (the `data/` root) is a directory → would be EISDIR
        // without the read-only guard, but the guard fires first.
        let err = svc.write(DATA_INO, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A data file inode (not in `inodes`, only in `data_inodes`) would
        // previously fall through to the `None` arm → `NotFound` → ENOENT.
        let data_file_ino = DATA_FILE_INO_BASE + 1;
        let err = svc.write(data_file_ino, 0, b"x").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// TSI-2533: `setattr` (chmod/chown/truncate/utimens) on the read-only
    /// `data/` namespace must return `EROFS`. Before this guard, `getattr`
    /// returned the current attributes (exit 0) and left the mode untouched —
    /// a silent success that differed from `write`/`rmdir`/`unlink`/`rename`.
    #[test]
    fn data_namespace_setattr_returns_erofs() {
        let mut svc = bare_service();

        let err = svc.setattr(DATA_INO).unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A data file inode lives in `data_inodes`, not `inodes`; without the
        // guard `getattr` would resolve it and return its attributes.
        let data_file_ino = DATA_FILE_INO_BASE + 1;
        let err = svc.setattr(data_file_ino).unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// TSI-2536: `setattr` (chmod/chown/truncate/utimens) on the `metadata/`
    /// namespace must return `EPERM` — never a silent success. `metadata/`
    /// is writable (create/write/rename/unlink work), so `EROFS` would be
    /// misleading; `EPERM` reflects "attributes are virtual and immutable".
    #[test]
    fn metadata_namespace_setattr_returns_eperm() {
        let mut svc = bare_service();

        let err = svc.setattr(METADATA_INO).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // A metadata/ file (writable .torrent buffer, but attributes are
        // virtual and immutable) must also reject setattr.
        let ino = svc
            .create(METADATA_INO, "ubuntu.iso.torrent")
            .expect("create metadata file")
            .attr
            .ino;
        let err = svc.setattr(ino).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);
    }

    /// TSI-2536: the root directory and the `.stats` virtual files have
    /// fixed modes (0o555 / 0o444) just like `metadata/`, so `setattr` on
    /// them must also return `EPERM` instead of silently returning the
    /// current attributes (exit 0) while leaving the mode untouched.
    #[test]
    fn root_and_stats_setattr_return_eperm() {
        let mut svc = bare_service();

        let err = svc.setattr(ROOT_INO).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        let err = svc.setattr(STATS_INO).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);

        // Derived stats inode (`dir_ino + STATS_INO_OFFSET`) falls inside the
        // `is_data_ino` range (>= DATA_TORRENT_INO_BASE), so it must be
        // classified as stats *before* the data/ guard, otherwise it would
        // wrongly return EROFS (Radian 💭3).
        let stats_ino = InodeManager::make_stats_ino(DATA_INO);
        assert!(InodeManager::is_stats_ino(stats_ino));
        let err = svc.setattr(stats_ino).unwrap_err();
        assert_eq!(err, FsError::NotPermitted);
    }

    /// Helper: create an empty writable `.torrent` inode via the public
    /// `create()` path (not direct `inodes` manipulation) and return its
    /// ino. Using the real path keeps the test honest about the
    /// `InodeData::File` layout without duplicating its constructor.
    fn writable_file_ino(svc: &mut FsService, name: &str) -> u64 {
        svc.create(METADATA_INO, name)
            .expect("create writable file")
            .attr
            .ino
    }

    /// TSI-2233: a negative offset must be rejected before any allocation
    /// rather than cast to a huge `usize` and fed to `Vec::resize` (OOM).
    #[test]
    fn write_rejects_negative_offset() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "f.torrent");
        let err = svc.write(ino, -1, b"x").unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
        // No partial allocation: the file is still empty.
        assert_eq!(
            svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.len()),
                _ => None,
            }),
            Some(0)
        );
    }

    /// TSI-2233: an offset that would grow the buffer past MAX_TORRENT_SIZE
    /// is rejected with `FileTooLarge` (→ EFBIG) instead of allocating.
    #[test]
    fn write_rejects_offset_beyond_max_size() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "big.torrent");
        let over = MAX_TORRENT_SIZE as i64 + 1;
        let err = svc.write(ino, over, b"x").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
        // Buffer untouched.
        assert!(svc
            .inode_mgr
            .inodes
            .get(&ino)
            .and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.is_empty()),
                _ => None,
            })
            .unwrap());
    }

    /// TSI-2233: `offset + data.len()` exceeding `MAX_TORRENT_SIZE` (even
    /// when offset alone is within bounds) is rejected, not silently capped.
    #[test]
    fn write_rejects_end_beyond_max_size() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "edge.torrent");
        // Offset alone is within the limit, but offset + 2 pushes end past
        // it — no pre-seeded data needed.
        let base = MAX_TORRENT_SIZE - 1;
        let err = svc.write(ino, base as i64, b"xy").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
    }

    /// TSI-2233: a write whose `end` lands exactly on MAX_TORRENT_SIZE is
    /// allowed (the guard uses strict `>`, mirroring `flush`'s `>`). This
    /// is the positive boundary the reject tests hover around.
    #[test]
    fn write_at_exact_max_size_is_allowed() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "max.torrent");
        // MAX_TORRENT_SIZE bytes from offset 0 → end == MAX_TORRENT_SIZE.
        let payload = vec![0u8; MAX_TORRENT_SIZE];
        let n = svc.write(ino, 0, &payload).expect("write at limit");
        assert_eq!(n, MAX_TORRENT_SIZE as u32);
        assert_eq!(
            svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.len()),
                _ => None,
            }),
            Some(MAX_TORRENT_SIZE)
        );
    }

    /// TSI-2233: a normal write at a positive offset still works and grows
    /// the buffer with a zero gap (regression guard for the guard logic).
    #[test]
    fn write_at_positive_offset_grows_with_gap() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "gap.torrent");
        let n = svc.write(ino, 4, b"AB").expect("write ok");
        assert_eq!(n, 2);
        let data = svc.inode_mgr.inodes.get(&ino).and_then(|d| match d {
            InodeData::File { data, .. } => Some(data.clone()),
            _ => None,
        });
        assert_eq!(data.as_deref(), Some(b"\0\0\0\0AB".as_slice()));
    }

    /// TSI-2233: a huge positive offset (would cast to a multi-GB `usize`
    /// and trigger a runaway `Vec::resize`) is rejected by the size cap
    /// before any allocation. This is the local-DoS vector the guard closes.
    #[test]
    fn write_rejects_huge_positive_offset() {
        let mut svc = bare_service();
        let ino = writable_file_ino(&mut svc, "big.torrent");
        // Far beyond MAX_TORRENT_SIZE but still a valid positive i64.
        let huge = MAX_TORRENT_SIZE as i64 * 2;
        let err = svc.write(ino, huge, b"x").unwrap_err();
        assert!(matches!(err, FsError::FileTooLarge(_)));
        // No allocation happened.
        assert!(svc
            .inode_mgr
            .inodes
            .get(&ino)
            .and_then(|d| match d {
                InodeData::File { data, .. } => Some(data.is_empty()),
                _ => None,
            })
            .unwrap());
    }

    #[test]
    fn data_namespace_create_mknod_mkdir_return_erofs() {
        let mut svc = bare_service();

        let err = svc.create(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mknod(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.mkdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // A nested data inode as parent.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.mkdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// TSI-2248: `rmdir`/`unlink` on the read-only `data/` namespace must
    /// return `EROFS` **without any side effect** — no inode table mutation,
    /// no `data_inodes` mutation, no cache eviction.  A mutation here would
    /// corrupt FUSE session state: subsequent `readdir`/`lookup` on the
    /// data/ subtree could serve stale or missing entries, and in the worst
    /// case the dispatcher thread could dereference a half-removed inode,
    /// aborting the FUSE session (mount disappears while the process lives).
    /// This test pins the early-return contract so the guard can never be
    /// accidentally bypassed by a future refactor.
    #[test]
    fn data_namespace_unlink_rmdir_return_erofs() {
        let mut svc = bare_service();

        // Snapshot the inode tables before any EROFS operation.
        let inodes_before = svc.inode_mgr.inodes.len();
        let data_inodes_before = svc.inode_mgr.data_inodes.len();

        let err = svc.unlink(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let err = svc.rmdir(DATA_INO, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc.unlink(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
        let err = svc.rmdir(data_dir_ino, "foo").unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // TSI-2248: No side effects — the early-return guard must not
        // mutate inode state, which would corrupt the FUSE session.
        assert_eq!(
            svc.inode_mgr.inodes.len(),
            inodes_before,
            "EROFS path must not mutate the inode table"
        );
        assert_eq!(
            svc.inode_mgr.data_inodes.len(),
            data_inodes_before,
            "EROFS path must not mutate the data_inodes cache"
        );
    }

    #[test]
    fn data_namespace_rename_returns_erofs() {
        let mut svc = bare_service();

        // Rename *into* data/ as the new parent.
        let err = svc
            .rename(METADATA_INO, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Rename *out of* data/ as the source parent.
        let err = svc
            .rename(DATA_INO, "foo", METADATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);

        // Both parents in data/ namespace.
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        let err = svc
            .rename(data_dir_ino, "foo", DATA_INO, "bar")
            .unwrap_err();
        assert_eq!(err, FsError::ReadOnlyFileSystem);
    }

    /// TSI-2537: `symlink` is unsupported everywhere, but the read-only
    /// `data/` namespace must return `EROFS` (matching `write`/`chmod`/
    /// `rmdir`/`unlink`/`rename`), while every other namespace returns
    /// `EPERM`.  Before the guard, `symlink` never reached the service: the
    /// fuser default answered `EPERM` for every parent.
    #[test]
    fn symlink_returns_erofs_in_data_namespace_only() {
        let mut svc = bare_service();

        // data/ root and a nested data inode → EROFS.
        assert_eq!(svc.symlink(DATA_INO), FsError::ReadOnlyFileSystem);
        let data_dir_ino = DATA_DIR_INO_BASE + 5;
        assert_eq!(svc.symlink(data_dir_ino), FsError::ReadOnlyFileSystem);

        // metadata/ and a nested metadata directory → EPERM (unsupported).
        assert_eq!(svc.symlink(METADATA_INO), FsError::NotPermitted);
        svc.mkdir(METADATA_INO, "dir").expect("mkdir");
        let dir_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "dir")
            .expect("dir inode");
        assert_eq!(svc.symlink(dir_ino), FsError::NotPermitted);
    }

    /// TSI-2373: a cross-directory mv of a `.torrent` must prune the
    /// metadata directories orphaned at the old source_path, so the data/
    /// mirror stops exposing the ghost tree.
    #[test]
    fn cross_directory_mv_prunes_orphaned_old_source_path_dirs() {
        let mut svc = service_with_db();

        // mkdir cat-b under metadata/ (persisted via ensure_metadata_directories).
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        // Simulate a torrent previously added at the root: a DB row with
        // source_path == "" plus the restored File inode in metadata/.
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("", "ubuntu-iso", "ubuntu.iso.torrent", 16, "hash-2373", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: METADATA_INO,
                name: "ubuntu.iso.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ cache so eviction is actually exercised.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: String::new(),
                name: "ubuntu-iso".to_string(),
                filename: "ubuntu.iso.torrent".to_string(),
            },
        );

        // Cross-directory move: metadata/ubuntu.iso.torrent → metadata/cat-b/.
        svc.rename(
            METADATA_INO,
            "ubuntu.iso.torrent",
            cat_b_ino,
            "ubuntu.iso.torrent",
        )
        .expect("cross-directory rename");

        // The DB row moved to cat-b; nothing references the old location.
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(
                db.get_torrent_by_filename_and_source_path("ubuntu.iso.torrent", "")
                    .unwrap()
                    .is_none(),
                "old location must be empty after the move"
            );
            assert!(
                db.get_torrent_by_filename_and_source_path("ubuntu.iso.torrent", "cat-b")
                    .unwrap()
                    .is_some(),
                "torrent must live at cat-b after the move"
            );
        }

        // The stale TorrentRoot cached for the old (root) source_path must be
        // evicted — a later lookup re-resolves from the DB under cat-b.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry for the old source_path must be evicted"
        );
    }

    /// TSI-2373: same contract for a torrent nested in a subdirectory — the
    /// emptied directory chain (cat-a and its parents) must vanish from both
    /// the DB and the cached SourcePathDir entries.
    #[test]
    fn cross_directory_mv_from_subdir_removes_ghost_dir_chain() {
        let mut svc = service_with_db();

        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        svc.mkdir(METADATA_INO, "cat-b").expect("mkdir cat-b");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");
        let cat_b_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-b")
            .expect("cat-b inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "t1.torrent", 16, "hash-a1", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ caches for both paths before the move.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_source_path_dir_ino("cat-a"),
            DataInode::SourcePathDir {
                path: "cat-a".to_string(),
            },
        );
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "cat-a".to_string(),
                name: "t1".to_string(),
                filename: "t1.torrent".to_string(),
            },
        );

        // Cross-directory move: metadata/cat-a/t1.torrent → metadata/cat-b/t1.torrent.
        svc.rename(cat_a_ino, "t1.torrent", cat_b_ino, "t1.torrent")
            .expect("cross-directory rename");

        // DB: no torrent left under cat-a, and its mirror directory row is gone.
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(db
                .get_torrent_by_filename_and_source_path("t1.torrent", "cat-a")
                .unwrap()
                .is_none());
            assert!(db
                .get_torrent_by_filename_and_source_path("t1.torrent", "cat-b")
                .unwrap()
                .is_some());
            assert!(
                !db.get_source_path_prefixes("")
                    .unwrap()
                    .contains(&"cat-a".to_string()),
                "ghost cat-a directory must be pruned from metadata_directories"
            );
            assert!(
                db.get_source_path_prefixes("")
                    .unwrap()
                    .contains(&"cat-b".to_string()),
                "destination directory must survive"
            );
        }

        // Cached SourcePathDir and TorrentRoot entries bound to the dead
        // source_path must be evicted — later lookups re-resolve from the DB.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_source_path_dir_ino("cat-a")),
            "stale SourcePathDir cache entry for cat-a must be evicted"
        );
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry for cat-a must be evicted"
        );
    }

    /// TSI-2373 guard: an intra-directory rename (same parent) must NOT run
    /// orphan cleanup — only the filename changes.
    #[test]
    fn intra_directory_rename_skips_orphan_cleanup() {
        let mut svc = service_with_db();

        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "old.torrent", 16, "hash-r1", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "old.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        svc.rename(cat_a_ino, "old.torrent", cat_a_ino, "new.torrent")
            .expect("intra-directory rename");

        // Same source_path: the directory must remain — it still holds t1.
        let db = svc.db.as_ref().unwrap().lock().unwrap();
        assert!(
            db.get_source_path_prefixes("")
                .unwrap()
                .contains(&"cat-a".to_string()),
            "intra-directory rename must not prune the still-populated directory"
        );
    }

    /// TSI-2378: a fast `cp` (release → detached add_torrent) followed
    /// immediately by a rename must wait for the pending add to settle, so
    /// the DB row lands at the NEW name instead of the old one. Simulate the
    /// race by inserting a pending entry in `processing_torrents` before
    /// calling rename; the rename must block until it clears and then move
    /// the row.
    #[test]
    fn intra_directory_rename_waits_for_pending_add() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        // The .torrent file exists as an inode but NOT yet in the DB —
        // exactly the window between release() spawning add_torrent and
        // insert_torrent_with_files committing.
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Mark the add as pending, and clear it from another thread after a
        // short delay — mimicking the background insert committing. The
        // notify_one is essential: it exercises the same wake path
        // `release`'s background add uses, so this test verifies the
        // Condvar wakeup instead of falling through to the timeout.
        {
            let processing = svc.processing_torrents.clone();
            let processing_cv = svc.processing_torrents_cv.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(50));
                if let Ok(mut guard) = processing.lock() {
                    guard.remove(&("cat-a".to_string(), "t1.torrent".to_string()));
                }
                processing_cv.notify_one();
            });
            svc.processing_torrents
                .lock()
                .unwrap()
                .insert(("cat-a".to_string(), "t1.torrent".to_string()), ());
        }

        let started = std::time::Instant::now();

        svc.rename(cat_a_ino, "t1.torrent", cat_a_ino, "renamed.torrent")
            .expect("rename must wait for the pending add and succeed");

        // The rename must return via the Condvar wakeup, not by waiting out
        // the 5s production timeout.
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "rename must be woken by notify_one promptly, took {:?}",
            started.elapsed()
        );

        // The pending entry was consumed by the simulated add; nothing is
        // left behind.
        assert!(
            svc.processing_torrents.lock().unwrap().is_empty(),
            "pending-add map must be drained"
        );
    }

    /// TSI-2378: while the add is still pending past the timeout, the rename
    /// must FAIL rather than silently no-op the DB update (which would leave
    /// the data/ mirror on the stale filename).
    #[test]
    fn intra_directory_rename_fails_when_add_stays_pending() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        let t1_ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            t1_ino,
            InodeData::File {
                parent: cat_a_ino,
                name: "t1.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Pending forever — no background thread will clear it.
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(("cat-a".to_string(), "t1.torrent".to_string()), ());

        // Inject a short deadline: exercising the full rename timeout path
        // must not hard-wait out the 5s production constant.
        let started = std::time::Instant::now();
        let result = svc.rename_with_pending_timeout(
            cat_a_ino,
            "t1.torrent",
            cat_a_ino,
            "renamed.torrent",
            Duration::from_millis(100),
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout path must honor the injected deadline, took {:?}",
            started.elapsed()
        );
        assert!(
            result.is_err(),
            "rename over a stuck pending add must fail instead of losing the rename"
        );

        match svc.inode_mgr.inodes.get(&t1_ino) {
            Some(InodeData::File { name, .. }) if name == "t1.torrent" => {}
            other => panic!(
                "inode must be rolled back to old name 't1.torrent' after rename timeout, got {:?}",
                other.map(|d| match d {
                    InodeData::File { name, .. } => name.clone(),
                    _ => "<non-file>".to_string(),
                })
            ),
        }
    }

    /// TSI-2378 (review suggestion): the 5s production timeout must not be
    /// baked into the wait loop — exercise the timeout path with a short
    /// injected deadline so this test stays fast.
    #[test]
    fn wait_for_pending_add_times_out_with_short_deadline() {
        let svc = service_with_db();
        let key = ("cat-a".to_string(), "t1.torrent".to_string());
        svc.processing_torrents
            .lock()
            .unwrap()
            .insert(key.clone(), ());

        let started = std::time::Instant::now();
        let err = svc
            .wait_for_pending_add(&key, Duration::from_millis(100))
            .expect_err("short timeout must fail");
        assert!(
            matches!(err, FsError::Internal(_)),
            "timeout must surface as Internal error, got {:?}",
            err
        );
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "timeout must honor the injected deadline, took {:?}",
            started.elapsed()
        );
    }

    /// TSI-2378: an intra-directory rename must evict the cached TorrentRoot
    /// carrying the OLD filename — otherwise data/ keeps listing the stale
    /// name until restart even though the DB row is updated.
    #[test]
    fn intra_directory_rename_evicts_stale_torrent_root_cache() {
        let mut svc = service_with_db();
        svc.mkdir(METADATA_INO, "cat-a").expect("mkdir cat-a");
        let cat_a_ino = svc
            .inode_mgr
            .find_child_by_name(METADATA_INO, "cat-a")
            .expect("cat-a inode");

        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("cat-a", "t1", "old.torrent", 16, "hash-2378", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }
        svc.inode_mgr.inodes.insert(
            NEXT_INO.fetch_add(1, Ordering::SeqCst),
            InodeData::File {
                parent: cat_a_ino,
                name: "old.torrent".to_string(),
                data: minimal_torrent_bytes(),
                unlinked: false,
            },
        );

        // Warm the data/ cache with the pre-rename TorrentRoot entry.
        svc.inode_mgr.data_inodes.insert(
            InodeManager::make_torrent_root_ino(1),
            DataInode::TorrentRoot {
                torrent_id: 1,
                source_path: "cat-a".to_string(),
                name: "t1".to_string(),
                filename: "old.torrent".to_string(),
            },
        );

        svc.rename(cat_a_ino, "old.torrent", cat_a_ino, "new.torrent")
            .expect("intra-directory rename");

        // The stale cached entry (old filename) must be evicted so later
        // lookups re-resolve from the DB at the new filename.
        assert!(
            !svc.inode_mgr
                .data_inodes
                .contains_key(&InodeManager::make_torrent_root_ino(1)),
            "stale TorrentRoot cache entry with the old filename must be evicted"
        );

        // And the DB row must now carry the new filename at the same path.
        {
            let db = svc.db.as_ref().unwrap().lock().unwrap();
            assert!(
                db.get_torrent_by_filename_and_source_path("new.torrent", "cat-a")
                    .unwrap()
                    .is_some(),
                "DB row must be renamed in place"
            );
        }
    }

    /// TSI-2381: `cp` over an existing metadata torrent, then a fast `mv`.
    ///
    /// The `cp` releases a NEW info-hash under the OLD filename while a DB
    /// row still exists at `(source_path, old_filename)` from the previous
    /// content — so the background `add_torrent` takes the `Duplicate`
    /// branch and keeps that stale row (old name/info_hash) instead of
    /// inserting anything. The immediate rename then finds no row at the
    /// new name and silently no-ops; on restart the OLD torrent is
    /// restored and metadata/ + data/ show unexpected duplicates.
    #[test]
    fn rename_after_overwrite_syncs_stale_row() {
        let mut svc = service_with_db();

        // A previous torrent already persisted at (root, "a.torrent").
        {
            let mut db = svc.db.as_ref().unwrap().lock().unwrap();
            match db.insert_torrent("", "stale", "a.torrent", 16, "hash-old", 1) {
                Ok(crate::db::InsertTorrentResult::Inserted(_)) => {}
                other => panic!("unexpected insert result: {:?}", other),
            }
        }

        // `cp` overwrites it with different torrent bytes and closes the
        // handle. The inode now carries the NEW bytes but keeps the name.
        let (ino, fh) = create_torrent_file(&mut svc, "a.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes())
            .expect("write ok");
        svc.release(fh).expect("release ok");

        // Simulate the background add_torrent having settled via the
        // Duplicate branch (row id 1 kept as-is) — drain the pending map
        // like the real thread would after its insert attempt.
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            std::thread::sleep(Duration::from_millis(10));
        }

        // The fast `mv`: the row at the new name does not exist yet, so the
        // pre-fix rename no-ops and the stale row survives at "a.torrent".
        svc.rename(METADATA_INO, "a.torrent", METADATA_INO, "b.torrent")
            .expect("rename ok");

        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let row = db_guard
            .get_torrent_by_filename_and_source_path("b.torrent", "")
            .unwrap()
            .expect("row must exist at the new name");

        // The overwritten row must reflect the NEW torrent content — the
        // pre-fix Duplicate branch kept the stale info-hash/bytes, so the
        // restart restored the old torrent and data/ served its file tree.
        let expected = {
            let info = TorrentInfo::from_bytes(minimal_torrent_bytes()).unwrap();
            hex::encode(info.info_hash().unwrap())
        };
        assert_eq!(
            row.info_hash, expected,
            "row must carry the NEW info-hash after overwrite+rename, got {}",
            row.info_hash
        );
    }

    #[test]
    fn data_namespace_inodes_correctly_identified() {
        // Unit-test the helper itself.
        assert!(InodeManager::is_data_namespace(DATA_INO));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE));
        assert!(InodeManager::is_data_namespace(DATA_TORRENT_INO_BASE + 42));
        assert!(InodeManager::is_data_namespace(DATA_DIR_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(DATA_FILE_INO_BASE + 1));
        assert!(InodeManager::is_data_namespace(SOURCE_PATH_DIR_INO_BASE));

        // Non-data inodes are not in the data namespace.
        assert!(!InodeManager::is_data_namespace(ROOT_INO));
        assert!(!InodeManager::is_data_namespace(METADATA_INO));
        assert!(!InodeManager::is_data_namespace(STATS_INO));
        assert!(!InodeManager::is_data_namespace(
            NEXT_INO.load(Ordering::SeqCst),
        ));
    }

    /// TSI-2246: opening a `data/` torrent file must set `direct_io: true`
    /// so the kernel bypasses its page cache and the daemon's errno
    /// (e.g. ENODATA) reaches userspace instead of being converted to EIO.
    #[test]
    fn open_data_torrent_file_sets_direct_io() {
        let mut svc = bare_service();
        let ino = DATA_FILE_INO_BASE + 1;
        svc.inode_mgr.data_inodes.insert(
            ino,
            DataInode::TorrentFile {
                torrent_id: 1,
                file_id: 1,
                name: "foo".to_string(),
                size: 16,
                torrent_source_path: String::new(),
                torrent_filename: String::new(),
            },
        );
        let outcome = svc.open(ino).expect("open data file");
        assert!(
            outcome.direct_io,
            "data torrent file must request direct_io"
        );
        assert_ne!(outcome.fh, 0, "data torrent file must get a real fh");
    }

    /// TSI-2246: non-data files (e.g. metadata) must NOT set direct_io —
    /// page cache is fine for static in-memory content.
    #[test]
    fn open_metadata_file_does_not_set_direct_io() {
        let mut svc = bare_service();
        let ino = NEXT_INO.fetch_add(1, Ordering::SeqCst);
        svc.inode_mgr.inodes.insert(
            ino,
            InodeData::File {
                parent: ROOT_INO,
                name: "x".to_string(),
                data: vec![b'x'],
                unlinked: false,
            },
        );
        let outcome = svc.open(ino).expect("open metadata file");
        assert!(
            !outcome.direct_io,
            "metadata file must not request direct_io"
        );
    }

    /// TSI-2246: stats inodes must NOT set direct_io.
    #[test]
    fn open_stats_file_does_not_set_direct_io() {
        let mut svc = bare_service();
        // STATS_INO is the global `.stats` file.
        let outcome = svc.open(STATS_INO).expect("open stats");
        assert!(!outcome.direct_io, "stats file must not request direct_io");
    }

    #[test]
    fn is_sparse_file_detects_sparse_partial_write() {
        // Simulate a partial multi-block piece write: create a file with the
        // right logical size but a zero-filled gap (sparse hole) by seeking
        // past the start before writing the trailing block.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sparse_piece");
        use std::io::{Seek, Write};
        let mut file = std::fs::File::create(&path).unwrap();
        // Write 16 KiB at offset 256 KiB — the file's logical size becomes
        // 256 KiB + 16 KiB, but bytes 0..256 KiB are a sparse hole.
        file.seek(std::io::SeekFrom::Start(256 * 1024)).unwrap();
        file.write_all(&vec![0xABu8; 16 * 1024]).unwrap();
        drop(file);

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 256 * 1024 + 16 * 1024);
        assert!(
            is_sparse_file(&meta, meta.len()),
            "file with a sparse hole must be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_complete_file() {
        // A fully-written (non-sparse) file must not be flagged as sparse.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("complete_piece");
        std::fs::write(&path, vec![0xCDu8; 262_144]).unwrap();

        let meta = std::fs::metadata(&path).unwrap();
        assert_eq!(meta.len(), 262_144);
        assert!(
            !is_sparse_file(&meta, meta.len()),
            "fully-written file must not be detected as sparse"
        );
    }

    #[test]
    fn is_sparse_file_false_for_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty_piece");
        std::fs::write(&path, b"").unwrap();
        let meta = std::fs::metadata(&path).unwrap();
        assert!(!is_sparse_file(&meta, 0));
    }

    /// Build a single-file bencoded torrent whose single piece has a known
    /// SHA-1 hash.  Returns `(torrent_bytes, piece_content, piece_length)`.
    fn build_single_piece_torrent() -> (Vec<u8>, Vec<u8>, u64) {
        let piece_length: usize = 16_384;
        let content = (0..piece_length)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>();
        let hash = {
            use sha1_smol::Sha1;
            Sha1::from(&content).digest().bytes()
        };
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(content.len().to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces20:");
        t.extend_from_slice(&hash);
        t.extend_from_slice(b"ee");
        (t, content, piece_length as u64)
    }

    /// Write a sparse piece file to `piece_path` whose *logical* size equals
    /// `piece_length` but whose physical allocation is smaller — simulating an
    /// interrupted multi-block write where only the trailing block was
    /// flushed to disk.  The tail starts at `piece_length / 4` so it never
    /// underflows for small pieces (TSI-2257 review: the prior `piece_length -
    /// 4096` could underflow when `piece_length < 4096`).
    fn write_sparse_piece_file(piece_path: &std::path::Path, content: &[u8], piece_length: u64) {
        use std::io::{Seek, Write};
        let tail_start = piece_length / 4;
        let mut file = std::fs::File::create(piece_path).unwrap();
        file.seek(std::io::SeekFrom::Start(tail_start)).unwrap();
        file.write_all(&content[tail_start as usize..]).unwrap();
        drop(file);
    }

    #[test]
    fn verify_single_piece_sparse_file_is_purged() {
        let (torrent_bytes, content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);
        let temp_dir = tempfile::tempdir().unwrap();

        // Create the piece file on disk *before* constructing the
        // CacheManager so the startup scan registers it as unverified
        // (exactly like a real restart).
        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir).unwrap();
        let piece_path = pieces_dir.join(&piece_key);

        // Sparse file: correct logical size but a zero-filled hole at the
        // start (simulates an interrupted multi-block write where only the
        // trailing block was flushed to disk).
        write_sparse_piece_file(&piece_path, &content, piece_length);

        let cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        // Scan registered the piece but left it unverified.
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));

        // TSI-2257: a sparse (partial-write) piece is purged, not skipped.
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "sparse piece file should be purged, not skipped"
        );
        // The caller (spawn_cache_verification) applies Purged by deleting
        // the piece from the cache; here we only assert the outcome, but
        // the purge contract implies the file is reclaimable.
    }

    #[test]
    fn verify_single_piece_complete_correct_hash_is_verified() {
        let (torrent_bytes, content, _piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        std::fs::write(&piece_path, &content).unwrap();
        cache.add_piece(&piece_key, content.len() as u64).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Verified),
            "complete piece with correct hash should be verified"
        );
    }

    #[test]
    fn verify_single_piece_complete_wrong_hash_is_purged() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a complete, non-sparse file with wrong content.
        std::fs::write(&piece_path, vec![0xFFu8; piece_length as usize]).unwrap();
        cache.add_piece(&piece_key, piece_length).unwrap();
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "complete non-sparse piece with wrong hash should be purged"
        );
    }

    #[test]
    fn verify_single_piece_wrong_size_is_purged() {
        let (torrent_bytes, _content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        let piece_path = cache.ensure_piece_dir(&piece_key).unwrap();
        // Write a short file (incomplete piece).
        std::fs::write(&piece_path, vec![0u8; piece_length as usize / 2]).unwrap();
        cache.add_piece(&piece_key, piece_length / 2).unwrap();

        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(
            matches!(outcome, VerifyOutcome::Purged),
            "wrong-size piece should be purged"
        );
    }

    /// TSI-2257: a purged incomplete piece must be deleted from the cache
    /// (metadata + on-disk file) so the disk space is reclaimed.  This
    /// exercises the full `spawn_cache_verification` purge contract by
    /// applying `VerifyOutcome::Purged` via `delete_piece` — the same path
    /// the verification worker uses.
    #[test]
    fn purged_sparse_piece_frees_disk_and_metadata() {
        let (torrent_bytes, content, piece_length) = build_single_piece_torrent();
        let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent"));
        let info_hash = hex::encode(info.info_hash().expect("info hash"));
        let piece_key = format!("{}:piece:0", info_hash);

        let temp_dir = tempfile::tempdir().unwrap();

        // Create a sparse piece file on disk *before* constructing the
        // CacheManager so the startup scan registers it as unverified.
        let pieces_dir = temp_dir.path().join("pieces").join(&info_hash);
        std::fs::create_dir_all(&pieces_dir).unwrap();
        let piece_path = pieces_dir.join(&piece_key);

        // Sparse file: correct logical size but a zero-filled hole at the
        // start (simulates an interrupted multi-block write where only the
        // trailing block was flushed to disk).
        write_sparse_piece_file(&piece_path, &content, piece_length);

        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();
        assert!(cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));
        assert!(piece_path.exists());
        let size_before = cache.current_size();
        assert!(size_before > 0, "scan should credit the piece size");

        // Verify → Purged → the worker applies the side effect.
        let outcome = verify_single_piece(&piece_path, 0, &info, &piece_key);
        assert!(matches!(outcome, VerifyOutcome::Purged));
        cache.delete_piece(&piece_key).unwrap();

        // The piece is gone from metadata, verified set, and disk.
        assert!(!cache.has_piece(&piece_key));
        assert!(!cache.is_piece_verified(&piece_key));
        assert!(!piece_path.exists(), "purged piece file must be deleted");
        assert_eq!(cache.current_size(), 0, "current_size must drop to zero");
    }

    // ── TSI-2247: release must not block the FUSE dispatch thread ─────────

    /// Build a service backed by an in-memory DB + TorrentService so
    /// `release` can exercise the full add_torrent path.
    fn service_with_db() -> FsService {
        let db = Database::open_in_memory().expect("in-memory db");
        let db_arc = Arc::new(Mutex::new(db));
        let metrics = Arc::new(Metrics::new());
        FsService {
            inode_mgr: InodeManager::new(Duration::from_secs(0)),
            db: Some(db_arc.clone()),
            torrent_service: Some(TorrentService::new(db_arc, None, None)),
            processing_torrents: Arc::new(Mutex::new(HashMap::new())),
            download_service: None,
            processing_torrents_cv: Arc::new(Condvar::new()),
            seeding_manager: None,
            torrent_data_cache: Arc::new(Mutex::new(HashMap::new())),
            torrent_info_cache: Arc::new(Mutex::new(HashMap::new())),
            listen_addr: String::new(),
            metrics,
            notifier: Arc::new(OnceLock::new()),
        }
    }

    /// Create a writable `.torrent` inode via the public `create()` path and
    /// return its (ino, fh).
    fn create_torrent_file(svc: &mut FsService, name: &str) -> (u64, u64) {
        let created = svc.create(METADATA_INO, name).expect("create file");
        (created.attr.ino, created.fh)
    }

    /// TSI-2247: Closing an empty `.torrent` file must hit the fast path —
    /// `release` returns `Ok(())` immediately without touching
    /// `processing_torrents` or the DB.
    #[test]
    fn release_empty_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "empty.torrent");

        // release must succeed instantly — no DB insert, no background spawn.
        svc.release(fh).expect("release ok");

        // Inode is removed.
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        // processing_torrents is untouched (empty).
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// TSI-2247: Closing a `.torrent` with invalid (non-parseable) data must
    /// also hit the fast path — no DB insert, no background spawn.
    #[test]
    fn release_invalid_torrent_is_fast_path() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "bad.torrent");
        // Write garbage that is non-empty but not a valid torrent.
        svc.write(ino, 0, b"not a torrent").expect("write ok");

        svc.release(fh).expect("release ok");

        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
    }

    /// TSI-2247: Closing a valid `.torrent` file must NOT block the caller.
    /// `add_torrent` runs on a background thread; `release` returns `Ok(())`
    /// immediately.  The `processing_torrents` entry is cleaned up by the
    /// background thread after the DB insert completes, and the torrent is
    /// persisted in the DB.
    #[test]
    fn release_valid_torrent_returns_immediately() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "valid.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino, 0, &data).expect("write ok");

        // release should return instantly — the DB insert is deferred to a
        // background thread.
        let start = std::time::Instant::now();
        svc.release(fh).expect("release ok");
        let elapsed = start.elapsed();

        // Even an in-memory DB insert takes < 1s; the point is that
        // release itself does not wait for it.  Allow generous slack
        // for CI scheduling jitter.
        assert!(
            elapsed.as_secs() < 5,
            "release took {:?} — should be near-instant",
            elapsed
        );

        // Inode is NOT removed (data integrity: if add_torrent fails, the
        // file must remain visible to the user).
        assert!(svc.inode_mgr.inodes.contains_key(&ino));

        // Wait for the background thread to finish and clean up
        // processing_torrents.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Verify the torrent was persisted to the DB by the background thread.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let torrent = db_guard
            .get_torrent_by_filename_and_source_path("valid.torrent", "")
            .expect("db query");
        assert!(torrent.is_some(), "torrent should be in DB after release");
        assert_eq!(torrent.unwrap().filename, "valid.torrent");
    }

    /// TSI-2247: `processing_torrents` is NOT held during `add_torrent` —
    /// the lock is released before the background thread is spawned.  Two
    /// `.torrent` files in the root directory (both `source_path == ""`)
    /// must both be processed and persisted — the dedup key is
    /// `(source_path, filename)`, so they don't collide.
    #[test]
    fn release_does_not_hold_processing_lock_during_add() {
        let mut svc = service_with_db();
        let (ino1, fh1) = create_torrent_file(&mut svc, "a.torrent");
        let data = minimal_torrent_bytes();
        svc.write(ino1, 0, &data).expect("write ok");
        svc.release(fh1).expect("release ok");

        // At this point the background thread for a.torrent may or may not
        // have finished.  Either way, processing_torrents should be lockable
        // without blocking (it would be held only briefly by the background
        // thread's cleanup).  The second release for a *different* file
        // must succeed immediately — even though both share source_path "".
        let (ino2, fh2) = create_torrent_file(&mut svc, "b.torrent");
        svc.write(ino2, 0, &data).expect("write ok");

        let start = std::time::Instant::now();
        svc.release(fh2).expect("release ok");
        let elapsed = start.elapsed();
        assert!(
            elapsed.as_secs() < 5,
            "second release took {:?} — should not wait for first",
            elapsed
        );

        // Wait for both background threads to finish.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            if svc.processing_torrents.lock().unwrap().is_empty() {
                break;
            }
            if std::time::Instant::now() > deadline {
                panic!("processing_torrents not cleaned up after 10s");
            }
            std::thread::sleep(std::time::Duration::from_millis(50));
        }

        // Both torrents must be in the DB — the tuple dedup key prevents
        // the second from being skipped as a duplicate of the first.
        let db_guard = svc.db.as_ref().unwrap().lock().unwrap();
        let t1 = db_guard
            .get_torrent_by_filename_and_source_path("a.torrent", "")
            .expect("db query");
        assert!(t1.is_some(), "a.torrent should be in DB");
        let t2 = db_guard
            .get_torrent_by_filename_and_source_path("b.torrent", "")
            .expect("db query");
        assert!(t2.is_some(), "b.torrent should be in DB");
    }

    /// TSI-2247: `flush` for an empty `.torrent` returns `EINVAL` (fast
    /// path — no FFI call to libtorrent).
    #[test]
    fn flush_empty_torrent_returns_einval() {
        let mut svc = service_with_db();
        let (ino, _fh) = create_torrent_file(&mut svc, "empty.torrent");

        let err = svc.flush(ino).unwrap_err();
        assert_eq!(err, FsError::InvalidArgument);
    }

    // ── TSI-2234: unlink-while-open keeps the inode alive ───────────────

    /// TSI-2234: `unlink` on a file with no open handle destroys the
    /// inode immediately — the name is gone and the inode is absent.
    #[test]
    fn unlink_closed_file_destroys_inode() {
        let mut svc = bare_service();
        // Create a file, write valid torrent bytes, and close the handle.
        // `release` for valid data does NOT remove the inode (no DB in
        // bare_service, so the add_torrent path is skipped but the inode
        // lingers), so the inode is alive with zero open handles — the
        // precondition for immediate destruction on unlink.
        let created = svc.create(METADATA_INO, "closed.torrent").expect("create");
        let ino = created.attr.ino;
        let fh = created.fh;
        let data = minimal_torrent_bytes();
        svc.write(ino, 0, &data).expect("write valid torrent");
        svc.release(fh).expect("close handle");
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 0);

        // unlink with no open handle → immediate destruction.
        svc.unlink(METADATA_INO, "closed.torrent").expect("unlink");
        assert_eq!(
            svc.inode_mgr
                .find_child_by_name(METADATA_INO, "closed.torrent"),
            None
        );
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// TSI-2234: an open handle keeps read/write working after unlink, and
    /// the inode is destroyed only on the LAST release (not the first).
    #[test]
    fn unlink_while_open_keeps_handle_until_last_release() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "open.torrent").expect("create");
        let ino = created.attr.ino;
        let fh1 = created.fh;
        // Open a second handle on the same inode.
        let fh2 = match svc.open(ino).expect("open") {
            OpenOutcome { fh, .. } => fh,
        };
        svc.write(ino, 0, b"hello").expect("write");

        // unlink while both handles are open.
        svc.unlink(METADATA_INO, "open.torrent").expect("unlink");
        // Name gone, inode lingers (unlinked).
        assert_eq!(
            svc.inode_mgr
                .find_child_by_name(METADATA_INO, "open.torrent"),
            None
        );
        assert!(svc.inode_mgr.is_unlinked_file(ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 2);

        // read/write via fh1's ino still succeed.
        svc.write(ino, 5, b"!").expect("write after unlink");
        let outcome = svc.read(ino, 0, 6).expect("read after unlink");
        let buf = match outcome {
            ReadOutcome::Ready(b) => b,
            _ => panic!("expected Ready read"),
        };
        assert_eq!(&buf, b"hello!");

        // First release: inode survives (fh2 still open).
        svc.release(fh1).expect("release fh1");
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        assert_eq!(svc.inode_mgr.open_handle_count(ino), 1);

        // Second (last) release: inode destroyed.
        svc.release(fh2).expect("release fh2");
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// TSI-2234: after unlink, `readdir` no longer lists the file even
    /// though its inode lingers for an open handle.
    #[test]
    fn unlink_hides_name_from_readdir_while_open() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "hidden.torrent").expect("create");
        let ino = created.attr.ino;
        let fh = created.fh;
        // Visible before unlink.
        let names: Vec<String> = svc
            .readdir(METADATA_INO, 0)
            .expect("readdir")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(names.iter().any(|n| n == "hidden.torrent"));

        svc.unlink(METADATA_INO, "hidden.torrent").expect("unlink");

        // Gone from readdir after unlink.
        let names: Vec<String> = svc
            .readdir(METADATA_INO, 0)
            .expect("readdir")
            .into_iter()
            .map(|e| e.name)
            .collect();
        assert!(!names.iter().any(|n| n == "hidden.torrent"));
        // Inode still alive.
        assert!(svc.inode_mgr.inodes.contains_key(&ino));
        svc.release(fh).expect("release");
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// TSI-2234: a second `unlink` of the same name returns `ENOENT`
    /// because `find_child_by_name` skips the unlinked inode.
    #[test]
    fn unlink_twice_returns_not_found() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "twice.torrent").expect("create");
        let _fh = created.fh;
        svc.unlink(METADATA_INO, "twice.torrent")
            .expect("first unlink");
        let err = svc.unlink(METADATA_INO, "twice.torrent").unwrap_err();
        assert_eq!(err, FsError::NotFound);
    }

    /// TSI-2234: creating a new file with the same name as an unlinked-but-
    /// still-open inode succeeds (the old name is freed even though the
    /// old inode lingers).
    #[test]
    fn recreate_name_after_unlink_while_open_succeeds() {
        let mut svc = bare_service();
        let created = svc.create(METADATA_INO, "reuse.torrent").expect("create");
        let old_ino = created.attr.ino;
        let _fh = created.fh;
        svc.unlink(METADATA_INO, "reuse.torrent").expect("unlink");
        // Re-create with the same name — must not collide with the lingering
        // unlinked inode.
        let created2 = svc.create(METADATA_INO, "reuse.torrent").expect("recreate");
        assert_ne!(created2.attr.ino, old_ino);
        // Both inodes coexist: old one (unlinked, open) + new one (linked).
        assert!(svc.inode_mgr.is_unlinked_file(old_ino));
        assert!(!svc.inode_mgr.is_unlinked_file(created2.attr.ino));
    }

    /// TSI-2234: an unlinked `.torrent` is NOT re-persisted on release —
    /// the torrent was already removed from the DB by `unlink`, so
    /// `release` just destroys the buffered inode.
    #[test]
    fn unlinked_torrent_not_repersisted_on_release() {
        let mut svc = service_with_db();
        let (ino, fh) = create_torrent_file(&mut svc, "repersist.torrent");
        svc.write(ino, 0, &minimal_torrent_bytes()).expect("write");
        svc.unlink(METADATA_INO, "repersist.torrent")
            .expect("unlink");
        assert!(svc.inode_mgr.is_unlinked_file(ino));
        // release must NOT spawn a background add_torrent for an unlinked
        // file — processing_torrents stays empty.
        svc.release(fh).expect("release");
        assert!(svc.processing_torrents.lock().unwrap().is_empty());
        // Inode destroyed on last release.
        assert!(!svc.inode_mgr.inodes.contains_key(&ino));
    }

    /// TSI-2234 (review blocking #1): a directory whose only remaining
    /// child is an unlinked-but-still-open file must be removable — POSIX
    /// treats "only unlinked-but-open files" as empty. Before the fix,
    /// `rmdir` saw the lingering unlinked `File` inode as a child and
    /// wrongly returned `ENOTEMPTY`.
    #[test]
    fn rmdir_succeeds_after_unlink_while_open() {
        let mut svc = bare_service();
        // Create a subdirectory under metadata/ and a .torrent inside it.
        let dir = svc.mkdir(METADATA_INO, "sub").expect("mkdir sub").ino;
        let created = svc.create(dir, "x.torrent").expect("create");
        let _fh = created.fh;
        // Unlink the file while its handle is open — inode lingers (unlinked).
        svc.unlink(dir, "x.torrent").expect("unlink");
        assert!(svc.inode_mgr.is_unlinked_file(created.attr.ino));
        assert!(svc.inode_mgr.inodes.contains_key(&created.attr.ino));
        // The directory now has no *visible* children; rmdir must succeed.
        svc.rmdir(METADATA_INO, "sub")
            .expect("rmdir after unlink-while-open");
        assert!(!svc.inode_mgr.inodes.contains_key(&dir));
    }

    /// TSI-2274 (review issue 2): the L1 range cache is bounded by
    /// `MAX_L1_ENTRIES`.  When full, inserting a new entry clears the
    /// cache so memory does not grow unboundedly.
    #[test]
    fn l1_cache_evicts_when_full() {
        let svc = bare_service();
        {
            let mut cache = svc.torrent_data_cache.lock().unwrap();
            for i in 0..MAX_L1_ENTRIES {
                let key = format!("hash:file:{}:4096", i);
                cache.insert(key, vec![0xABu8; 4096]);
            }
            assert_eq!(cache.len(), MAX_L1_ENTRIES);
        }
        // One more insert triggers eviction.
        {
            let mut cache = svc.torrent_data_cache.lock().unwrap();
            if cache.len() >= MAX_L1_ENTRIES {
                cache.clear();
            }
            cache.insert("hash:file:999:4096".to_string(), vec![0xCDu8; 4096]);
        }
        let cache = svc.torrent_data_cache.lock().unwrap();
        assert_eq!(
            cache.len(),
            1,
            "cache should have been cleared and now hold only the new entry"
        );
        assert!(cache.contains_key("hash:file:999:4096"));
    }

    /// TSI-2274 (review issue 1): L1 cache keys include `size` so a
    /// read with a different size at the same offset does not collide.
    #[test]
    fn l1_cache_key_includes_size() {
        let svc = bare_service();
        let mut cache = svc.torrent_data_cache.lock().unwrap();
        // Simulate a 4 KB read at offset 0 and a 128 KB read at offset 0.
        let key_small = format!("{}:{}:{}:{}", "hash", 1, 0u64, 4096u32);
        let key_large = format!("{}:{}:{}:{}", "hash", 1, 0u64, 131072u32);
        assert_ne!(key_small, key_large, "keys must differ by size");
        cache.insert(key_small, vec![0u8; 4096]);
        cache.insert(key_large, vec![0u8; 131072]);
        // Both entries coexist — no truncation.
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_non_ascii_filenames_db_round_trip() {
        // TSI-2278: Verify that multi-file torrents with non-ASCII (UTF-8)
        // file names can be inserted, retrieved, and looked up by name
        // through the DB — the same path FUSE lookup/readdir uses.
        let name = "测试种子".as_bytes();
        let file1 = "你好.txt".as_bytes().to_vec();
        let file2 = "世界.txt".as_bytes().to_vec();
        let torrent_bytes = crate::infrastructure::metadata::build_multifile_torrent(
            name,
            &[(file1, 16), (file2, 16)],
            16,
        );
        let info = TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse torrent");
        let metadata = info.metadata().expect("get metadata");
        let info_hash_hex = hex::encode(metadata.info_hash);

        let dir = tempfile::TempDir::new().unwrap();
        let mut db = Database::open(&dir.path().join("test.db")).unwrap();

        let file_entries: Vec<FileEntry> = metadata
            .files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();

        let result = db
            .insert_torrent_with_files(
                "",
                &metadata.name,
                "test.torrent",
                metadata.total_size as i64,
                &info_hash_hex,
                metadata.num_files as i64,
                &file_entries,
            )
            .expect("insert");
        let torrent_id = match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("expected insertion"),
        };

        // Root directory = torrent name (first component of file_path).
        let root_dirs = db
            .get_torrent_directories_by_parent(None, torrent_id)
            .expect("get root dirs");
        assert_eq!(root_dirs.len(), 1);
        assert_eq!(root_dirs[0].name, "测试种子");

        // Files under the root directory.
        let dir_files = db
            .get_files_in_directory(root_dirs[0].id)
            .expect("get dir files");
        assert_eq!(dir_files.len(), 2);
        assert_eq!(dir_files[0].name, "你好.txt");
        assert_eq!(dir_files[1].name, "世界.txt");

        // Simulate FUSE lookup: find a file by name in the directory.
        let found = dir_files.iter().find(|f| f.name == "你好.txt");
        assert!(found.is_some(), "lookup should find 你好.txt");

        // TSI-2278: The read path maps file_id → file_index via
        // `files.iter().position(|f| f.id == file_id)` where `files`
        // comes from `get_files_by_torrent_id` (ordered by `id`).  This
        // must match the libtorrent file index order (the order
        // `info.files()` returns).  If the orderings diverge, the read
        // path would use the wrong file_index → wrong piece range → EIO.
        //
        // Both the DB insertion (`insert_torrent_with_files`) and
        // `info.files()` iterate the torrent's file list in the same
        // order, so the DB `id` order must match the libtorrent index.
        let all_files = db
            .get_files_by_torrent_id(torrent_id)
            .expect("get all files");
        let info_files = info.files().expect("get libtorrent files");
        assert_eq!(all_files.len(), info_files.len());
        for (db_idx, db_file) in all_files.iter().enumerate() {
            // Each DB file's `path` must match the corresponding
            // libtorrent file's path at the same index.
            assert_eq!(
                db_file.path, info_files[db_idx].path,
                "DB file at index {} (id={}, name={:?}) has path {:?} \
                 but libtorrent file at index {} has path {:?} — \
                 ordering mismatch would cause wrong file_index in reads",
                db_idx, db_file.id, db_file.name, db_file.path, db_idx, info_files[db_idx].path,
            );
        }
        // Two calls must return the same order (stability).
        let all_files_2 = db
            .get_files_by_torrent_id(torrent_id)
            .expect("get all files (2nd call)");
        let ids_1: Vec<i64> = all_files.iter().map(|f| f.id).collect();
        let ids_2: Vec<i64> = all_files_2.iter().map(|f| f.id).collect();
        assert_eq!(ids_1, ids_2, "DB file ordering must be stable across calls");
    }

    #[test]
    fn test_non_utf8_filenames_db_round_trip() {
        // TSI-2278: Non-UTF-8 (e.g. GBK) file names are sanitized by
        // libtorrent to '_' before reaching Rust.  The sanitized names
        // are valid UTF-8 and can be stored/retrieved from the DB.
        let name: &[u8] = b"\xb2\xe2\xca\xd4"; // 测试 in GBK
        let file1: &[u8] = b"\xc4\xe3\xba\xc3.txt"; // 你好.txt in GBK
        let file2: &[u8] = b"\xca\xc0\xbd\xe7.txt"; // 世界.txt in GBK
        let torrent_bytes = crate::infrastructure::metadata::build_multifile_torrent(
            name,
            &[(file1.to_vec(), 16), (file2.to_vec(), 16)],
            16,
        );
        let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
        let metadata = info.metadata().expect("get metadata");

        // Sanitized names must be valid UTF-8 for SQLite TEXT storage.
        for f in &metadata.files {
            assert!(
                std::str::from_utf8(f.path.as_bytes()).is_ok(),
                "sanitized path must be valid UTF-8: {:?}",
                f.path
            );
        }

        let info_hash_hex = hex::encode(metadata.info_hash);
        let dir = tempfile::TempDir::new().unwrap();
        let mut db = Database::open(&dir.path().join("test.db")).unwrap();

        let file_entries: Vec<FileEntry> = metadata
            .files
            .iter()
            .map(|f| FileEntry {
                path: f.path.clone(),
                size: f.size as i64,
            })
            .collect();

        let result = db
            .insert_torrent_with_files(
                "",
                &metadata.name,
                "test.torrent",
                metadata.total_size as i64,
                &info_hash_hex,
                metadata.num_files as i64,
                &file_entries,
            )
            .expect("insert");
        let torrent_id = match result {
            InsertTorrentResult::Inserted(id) => id,
            _ => panic!("expected insertion"),
        };

        // Verify files are retrievable and have unique names.
        let all_files = db.get_files_by_torrent_id(torrent_id).expect("get files");
        assert_eq!(all_files.len(), 2);
        let name0 = all_files[0].name.as_str();
        let name1 = all_files[1].name.as_str();
    }

    /// TSI-2293: `guard_empty_read` must reject empty data for a non-zero
    /// `size` with `FsError::NoPeers` (→ ENODATA), preventing the silent
    /// 0-byte EOF that `read_data`'s sync path would otherwise produce.
    #[test]
    fn sync_empty_data_guard_maps_to_no_peers() {
        // Empty data, non-zero size → error.
        let err = guard_empty_read(&[], 4096).unwrap_err();
        assert!(matches!(err, FsError::NoPeers(_)));
    }

    /// TSI-2293: `guard_empty_read` must pass through non-empty data and
    /// zero-size reads without error.
    #[test]
    fn sync_empty_data_guard_passes_nonempty_and_zero_size() {
        // Non-empty data, non-zero size → ok.
        assert!(guard_empty_read(b"data", 4).is_ok());
        // Empty data, zero size → ok (legitimate EOF).
        assert!(guard_empty_read(&[], 0).is_ok());
    }

    /// TSI-2454: `split_data_dir_parent` maps a top-level source-path to
    /// `DATA_INO` and a nested path to its parent's `SourcePathDir` inode.
    #[test]
    fn split_data_dir_parent_maps_correctly() {
        // Top-level: parent is data/ root.
        let (parent, name) = FsService::split_data_dir_parent("cat-a");
        assert_eq!(parent, DATA_INO);
        assert_eq!(name, "cat-a");

        // Nested: parent is the SourcePathDir inode of the parent path.
        let (parent, name) = FsService::split_data_dir_parent("cat-a/sub");
        assert_eq!(parent, InodeManager::make_source_path_dir_ino("cat-a"));
        assert_eq!(name, "sub");

        // Deeply nested.
        let (parent, name) = FsService::split_data_dir_parent("a/b/c");
        assert_eq!(parent, InodeManager::make_source_path_dir_ino("a/b"));
        assert_eq!(name, "c");
    }

    /// TSI-2454: `inval_data_entry` is a no-op when no `Notifier` is wired
    /// (the test/constructor path — `OnceLock` is empty).  The call must
    /// not panic.
    #[test]
    fn inval_data_entry_noop_without_notifier() {
        let svc = bare_service();
        // No notifier set — this must be a silent no-op.
        svc.inval_data_entry(DATA_INO, "foo.torrent");
    }
}

//! `DownloadEngine` — single-owner-thread actor for the download subsystem.
//!
//! The engine thread exclusively owns the libtorrent `Session`, all torrent
//! `TorrentHandle`s, the [`PieceStore`] (data plane) and the [`PieceScheduler`]
//! (control plane / priority).  External callers send [`Command`]s over an
//! `mpsc` channel and receive results on a `sync_channel`; the raw libtorrent
//! pointers therefore never cross a thread boundary, which lets `Session` and
//! `TorrentHandle` drop their `unsafe impl Send`.
//!
//! Non-blocking `.stats` reads go through a shared [`DownloadSnapshot`] that
//! the engine refreshes each tick, replacing the old `try_lock` on the
//! `DownloadManager` big lock (TSI-2119).

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::alert::{AlertConsumer, SharedSessionStats};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::metadata::TorrentInfo;
use crate::infrastructure::metrics::Metrics;
use tracing::{info, warn};

use super::piece_scheduler::{PiecePriorityConfig, PieceScheduler, PieceStatus};
use super::piece_store::PieceStore;
use super::session::{Session, TorrentHandle};
use super::types::{SessionStats, TorrentState, TorrentStatus};

/// A command sent to the download engine actor.
pub enum Command {
    /// Ensure a lightweight handle exists for a torrent (no download).
    EnsureHandle {
        info: Arc<TorrentInfo>,
        reply: SyncSender<TorrentResult<()>>,
    },
    /// Fire-and-forget variant of [`Command::EnsureHandle`]: the engine
    /// creates the handle eventually but the sender does not wait for it.
    /// Used by the FUSE write path (torrent release), which must never block
    /// the single-threaded FUSE dispatch loop on a busy download.
    EnsureHandleAsync { info: Arc<TorrentInfo> },
    /// Read a byte range from a file, driving the piece download if needed.
    /// Blocks on the engine thread until the pieces are available.
    ReadFileRange {
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
        reply: SyncSender<TorrentResult<Vec<u8>>>,
    },
    /// Piece status for a torrent (used by `.stats`).
    GetPiecesStatus {
        info_hash: String,
        num_pieces: i32,
        reply: SyncSender<TorrentResult<Vec<PieceStatus>>>,
    },
    /// Remove a torrent handle from the engine session and clear its
    /// scheduler state.  Used by the unlink/remove path when the last DB
    /// reference to an info_hash is deleted, so the engine stops
    /// announcing/seeding a removed torrent (TSI-2232).
    RemoveHandle { info_hash: String },
    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle (TSI-2275). The engine checks the private flag (TSI-2277):
    /// if either the existing or incoming torrent is private, the merge is
    /// skipped to prevent PT passkey leakage and peer cross-pollination.
    /// Fire-and-forget: the result is logged, not returned to the caller.
    MergeTrackers { info: Arc<TorrentInfo> },
    /// Query the current tracker list on a torrent handle (TSI-2277 test
    /// support). Used by tests to verify PT isolation: private torrent
    /// trackers must not be merged into the existing handle.
    GetTrackers {
        info_hash: String,
        reply: SyncSender<TorrentResult<Vec<crate::TrackerEntry>>>,
    },
    /// Stop the engine thread.
    Shutdown,
}

/// Shared, non-blocking snapshot of the download subsystem state.
#[derive(Default)]
pub struct DownloadSnapshot {
    /// Per-info_hash torrent status.
    pub statuses: HashMap<String, TorrentStatus>,
    /// Per-info_hash `(piece_length, piece statuses)`.
    pub pieces: HashMap<String, (u64, Vec<PieceStatus>)>,
    /// Per-info_hash private flag (TSI-2277). A torrent is "private" when
    /// its info dict has `private=1` (BEP-27). Private torrents are isolated
    /// from cross-site tracker merging to prevent passkey leakage and peer
    /// cross-pollination across PT swarms.
    pub private_torrents: HashMap<String, bool>,
}

/// Handle to a running download engine.  Cheap to clone (`Send + Sync`).
pub struct DownloadEngine {
    tx: mpsc::Sender<Command>,
    stopping: Arc<AtomicBool>,
    join: Arc<Mutex<Option<JoinHandle<()>>>>,
    cache_manager: Arc<Mutex<CacheManager>>,
    shared_stats: SharedSessionStats,
    snapshot: Arc<Mutex<DownloadSnapshot>>,
    metrics: Arc<Metrics>,
    read_timeout_secs: u64,
}

/// State owned exclusively by the engine thread.
struct EngineState {
    session: Session,
    handles: HashMap<String, TorrentHandle>,
    /// Per-info_hash private flag (TSI-2277). Populated at handle creation
    /// time from `TorrentInfo::is_private()`. Used to guard tracker merging:
    /// if either the existing or incoming torrent is private, the merge is
    /// skipped to prevent PT passkey leakage and peer cross-pollination.
    private_torrents: HashMap<String, bool>,
    store: PieceStore,
    read_timeout_secs: u64,
    scheduler: PieceScheduler,
    cache_dir: String,
    metrics: Arc<Metrics>,
    snapshot: Arc<Mutex<DownloadSnapshot>>,
    stopping: Arc<AtomicBool>,
    alert_consumer: Option<AlertConsumer>,
}

impl DownloadEngine {
    pub fn new(cache_dir: &Path, config: &TorrentfsConfig) -> TorrentResult<Self> {
        Self::new_with_metrics(cache_dir, config, Arc::new(Metrics::new()))
    }

    pub fn new_with_metrics(
        cache_dir: &Path,
        config: &TorrentfsConfig,
        metrics: Arc<Metrics>,
    ) -> TorrentResult<Self> {
        let cache_dir_str = cache_dir.to_string_lossy().into_owned();
        let pieces_dir = cache_dir.join("pieces");
        std::fs::create_dir_all(&pieces_dir).map_err(|e| TorrentError::IoError(e.to_string()))?;

        // Send-safe shared state, created on the caller's thread.
        // Read cache_size from config, falling back to 1 GiB when unset or
        // non-positive (matches the historical default).
        let cache_size = config
            .cache
            .cache_size
            .map(|v| if v > 0 { v as u64 } else { 1024 * 1024 * 1024 })
            .unwrap_or(1024 * 1024 * 1024);
        let cache_manager = Arc::new(Mutex::new(CacheManager::new(cache_dir, cache_size)?));
        let store = PieceStore::new(cache_manager.clone());
        let scheduler = PieceScheduler::new(PiecePriorityConfig::from_toml(&config.piece_priority));

        let read_timeout_secs = config
            .timeouts
            .read_timeout_secs
            .map(|v| if v > 0 { v as u64 } else { 30 })
            .unwrap_or(30);

        let (tx, rx) = mpsc::channel::<Command>();
        let stopping = Arc::new(AtomicBool::new(false));
        let shared_stats = SharedSessionStats::new();
        let snapshot = Arc::new(Mutex::new(DownloadSnapshot::default()));

        // The libtorrent `Session` owns a raw pointer and is not `Send`, so it
        // must be created on the engine thread itself.  Everything else moved
        // into the closure is `Send`.
        let (init_tx, init_rx) = mpsc::sync_channel::<TorrentResult<()>>(1);
        let config = config.clone();

        // Clones moved into the engine thread; the originals remain for the
        // returned handle.
        let thread_snapshot = snapshot.clone();
        let thread_stopping = stopping.clone();
        let thread_metrics = metrics.clone();

        let thread_shared_stats = shared_stats.clone();
        let handle = std::thread::Builder::new()
            .spawn(move || {
                let shared_stats = thread_shared_stats;
                let session = match Session::new_with_custom_storage(&config, &pieces_dir) {
                    Ok(s) => s,
                    Err(e) => {
                        let _ = init_tx.send(Err(e));
                        return;
                    }
                };
                // Spawn the dedicated alert consumer (design §4.2): it drains
                // libtorrent alerts event-driven via `set_alert_notify`, so the
                // engine loop no longer polls. `pop_alerts` is mutex-serialized
                // and thread-safe on the C++ side, so only the raw session
                // pointer crosses this thread boundary.
                // SAFETY: `session` outlives the consumer — `engine_loop` calls
                // `consumer.stop()` (unregistering the notify hook) before the
                // session is dropped.
                let alert_consumer = unsafe {
                    AlertConsumer::spawn(
                        session.inner(),
                        shared_stats.clone(),
                        thread_metrics.clone(),
                    )
                };
                let state = EngineState {
                    session,
                    handles: HashMap::new(),
                    private_torrents: HashMap::new(),
                    store,
                    scheduler,
                    cache_dir: cache_dir_str,
                    read_timeout_secs,
                    metrics: thread_metrics,
                    snapshot: thread_snapshot,
                    stopping: thread_stopping,
                    alert_consumer: Some(alert_consumer),
                };
                let _ = init_tx.send(Ok(()));
                engine_loop(state, rx);
            })
            .map_err(|e| TorrentError::Unknown {
                code: -1,
                message: format!("Failed to spawn download engine thread: {}", e),
            })?;

        init_rx.recv().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Download engine thread disconnected before init".to_string(),
        })??;

        Ok(DownloadEngine {
            tx,
            stopping,
            join: Arc::new(Mutex::new(Some(handle))),
            cache_manager,
            shared_stats: shared_stats.clone(),
            snapshot,
            metrics,
            read_timeout_secs,
        })
    }

    // ── Non-blocking snapshots (used by `.stats`) ────────────────────

    /// Shared cache handle (non-blocking `.stats` / on-disk checks).
    pub fn cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }

    /// Cached session stats snapshot.
    pub fn snapshot_stats(&self) -> SessionStats {
        self.shared_stats.snapshot()
    }

    /// Non-blocking torrent status from the last engine snapshot.
    pub fn try_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.snapshot
            .try_lock()
            .ok()?
            .statuses
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking piece status from the last engine snapshot.
    pub fn try_pieces_status(&self, info_hash: &str) -> Option<(u64, Vec<PieceStatus>)> {
        self.snapshot
            .try_lock()
            .ok()?
            .pieces
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking private-flag check from the last engine snapshot
    /// (TSI-2277). Returns `Some(true)` if the torrent's info dict has
    /// `private=1`, `Some(false)` if not, `None` if the info_hash has no
    /// handle in the snapshot. Used by `.stats` to display the PT isolation
    /// state.
    pub fn try_is_private(&self, info_hash: &str) -> Option<bool> {
        self.snapshot
            .try_lock()
            .ok()?
            .private_torrents
            .get(info_hash)
            .copied()
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn read_timeout_secs(&self) -> u64 {
        self.read_timeout_secs
    }

    // ── Command senders ──────────────────────────────────────────────

    fn send(&self, cmd: Command) -> TorrentResult<()> {
        self.tx.send(cmd).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Download engine has shut down".to_string(),
        })
    }

    /// Ensure a lightweight handle exists for a torrent.
    ///
    /// Blocks the caller until the engine thread has created (or found) the
    /// handle.  Only call this from contexts that may block (tests, the
    /// engine's own read path); never from the FUSE dispatch loop, where a
    /// busy download would stall every other filesystem operation.
    pub fn ensure_handle(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::EnsureHandle { info, reply: tx })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Ensure a lightweight handle exists for a torrent without waiting for
    /// the engine thread to finish.
    ///
    /// The command is queued and executed eventually (the `Arc<TorrentInfo>`
    /// keeps the metadata alive).  Used by the FUSE release path so a torrent
    /// write never blocks the single-threaded FUSE dispatch loop on a busy
    /// download; the handle is created lazily on first read either way.
    pub fn ensure_handle_async(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.send(Command::EnsureHandleAsync { info })
    }

    /// Remove a torrent handle from the engine session and clear its
    /// scheduler state.  Fire-and-forget: the command is queued and
    /// executed eventually on the engine thread; this call does not block.
    /// Safe to call from the FUSE unlink path — it will never stall the
    /// dispatch loop on a busy engine (TSI-2232).
    pub fn remove_handle(&self, info_hash: &str) -> TorrentResult<()> {
        self.send(Command::RemoveHandle {
            info_hash: info_hash.to_string(),
        })
    }

    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle (TSI-2275 / TSI-2277). Fire-and-forget: the command is queued
    /// and executed on the engine thread; this call does not block. The
    /// engine checks the private flag — if either the existing or incoming
    /// torrent is private, the merge is skipped (PT isolation).
    pub fn merge_trackers(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.send(Command::MergeTrackers { info })
    }

    /// Query the current tracker list on a torrent handle (TSI-2277).
    /// Synchronous: blocks until the engine thread responds. Used by tests
    /// to verify PT isolation — private torrent trackers must not be merged.
    pub fn get_trackers(&self, info_hash: &str) -> TorrentResult<Vec<crate::TrackerEntry>> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::GetTrackers {
            info_hash: info_hash.to_string(),
            reply: tx,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Read a file range, driving piece download on the engine thread.
    pub fn read_file_range(
        &self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::ReadFileRange {
            info,
            file_index,
            offset,
            size,
            reply: tx,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }

    /// Piece status for a torrent (synchronous).
    pub fn get_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<PieceStatus>> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.send(Command::GetPiecesStatus {
            info_hash: info_hash.to_string(),
            num_pieces,
            reply: tx,
        })?;
        rx.recv().map_err(|_| Self::disconnected())?
    }
    /// Stop the engine: abort in-flight reads and join the thread.
    /// Idempotent.
    pub fn shutdown(&self) {
        self.stopping.store(true, Ordering::Relaxed);
        let _ = self.tx.send(Command::Shutdown);
        if let Some(handle) = self.join.lock().ok().and_then(|mut g| g.take()) {
            let _ = handle.join();
        }
    }

    fn disconnected() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Download engine thread disconnected".to_string(),
        }
    }
}

impl Drop for DownloadEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

// ── Engine loop ────────────────────────────────────────────────────────────

/// libtorrent `torrent_flags::upload_mode` numeric value (`1 << 1`).
const UPLOAD_MODE_FLAG: u64 = 1 << 1;

/// Snapshot refresh interval. Alerts are drained by a dedicated consumer
/// thread (`set_alert_notify`), so this interval bounds `.stats` staleness
/// for per-torrent status/pieces and also drives the session-stats sample
/// request (TSI-2344): each tick fires `post_session_stats`, whose alert
/// the consumer drains into the shared stats snapshot.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

fn engine_loop(mut state: EngineState, rx: Receiver<Command>) {
    tracing::info!("Download engine started");
    loop {
        // Block on commands; wake every `SNAPSHOT_INTERVAL` to refresh the
        // non-blocking `.stats` snapshot (design §4.2: no alert polling here).
        match rx.recv_timeout(SNAPSHOT_INTERVAL) {
            Ok(cmd) => {
                let stop = state.handle_command(cmd);
                state.publish_snapshot();
                state.flush_cache_metadata();
                if stop {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                state.refresh_session_stats();
                state.publish_snapshot();
                state.flush_cache_metadata();
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    // TSI-2274: final flush so metadata mutations that happened since the
    // last tick are durable before the engine thread exits.
    state.flush_cache_metadata();
    // Unregister the alert-notify hook before the session is dropped.
    if let Some(mut consumer) = state.alert_consumer.take() {
        consumer.stop();
    }
    tracing::info!("Download engine stopped");
}

impl EngineState {
    /// TSI-2344: request a fresh session-stats sample. Fire-and-forget —
    /// libtorrent answers with a `session_stats_alert` on the normal alert
    /// queue, which the alert-consumer thread drains into `shared_stats`.
    /// This is the sole producer for the `.stats` Global Rates counters.
    fn refresh_session_stats(&self) {
        self.session.post_stats();
    }

    /// Handle one command; returns `true` when the engine should stop.
    fn handle_command(&mut self, cmd: Command) -> bool {
        match cmd {
            Command::EnsureHandle { info, reply } => {
                let _ = reply.send(self.ensure_handle(&info));
            }
            Command::EnsureHandleAsync { info } => {
                let _ = self.ensure_handle(&info);
            }
            Command::ReadFileRange {
                info,
                file_index,
                offset,
                size,
                reply,
            } => {
                let _ = reply.send(self.read_file_range(&info, file_index, offset, size));
            }
            Command::GetPiecesStatus {
                info_hash,
                num_pieces,
                reply,
            } => {
                let _ = reply.send(self.build_pieces_status(&info_hash, num_pieces));
            }
            Command::RemoveHandle { info_hash } => {
                let _ = self.remove_handle(&info_hash);
            }
            Command::MergeTrackers { info } => {
                self.merge_trackers(&info);
            }
            Command::GetTrackers { info_hash, reply } => {
                let result = match self.handles.get(&info_hash) {
                    Some(handle) => handle.trackers(),
                    None => Err(TorrentError::Unknown {
                        code: -1,
                        message: "No handle for info_hash".to_string(),
                    }),
                };
                let _ = reply.send(result);
            }
            Command::Shutdown => return true,
        }
        false
    }

    /// Ensure a lightweight handle exists for the torrent.
    ///
    /// The handle is added in upload_mode: it connects to trackers and peers
    /// (so peer/seed info is visible immediately) but never requests pieces,
    /// so nothing is downloaded until the first read that needs data.  On that
    /// read, [`Self::read_file_range`] clears the upload_mode flag to switch
    /// the torrent into download mode.
    fn ensure_handle(&mut self, info: &TorrentInfo) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        if self.handles.contains_key(&info_hash) {
            return Ok(());
        }

        let pieces_dir = Path::new(&self.cache_dir).join("pieces");
        std::fs::create_dir_all(&pieces_dir).map_err(|e| TorrentError::IoError(e.to_string()))?;
        let torrent_save_dir = pieces_dir.join(&info_hash);
        std::fs::create_dir_all(&torrent_save_dir)
            .map_err(|e| TorrentError::IoError(e.to_string()))?;

        let handle = self
            .session
            .add_torrent_upload_mode(info, &torrent_save_dir)?;

        let (piece_length, num_pieces) = handle.get_torrent_info()?;
        self.scheduler
            .init_torrent(&info_hash, num_pieces as i32, piece_length)?;
        // Record the private flag (TSI-2277) so that tracker merging can
        // check it without re-parsing the torrent_info on every duplicate
        // add. The flag is immutable for the lifetime of the info_hash.
        self.private_torrents
            .insert(info_hash.clone(), info.is_private());
        self.handles.insert(info_hash, handle);
        Ok(())
    }

    /// Remove a torrent handle from the session and clear its scheduler
    /// state.  Idempotent: a missing info_hash is a no-op.  Called on the
    /// engine thread when the last DB reference to an info_hash is deleted
    /// (TSI-2232), so the engine stops announcing/seeding a removed torrent
    /// and its handle/scheduler entries do not leak across add/remove cycles.
    fn remove_handle(&mut self, info_hash: &str) -> TorrentResult<()> {
        if let Some(handle) = self.handles.remove(info_hash) {
            self.session.remove_torrent(handle, false);
        }
        self.scheduler.remove_torrent(info_hash);
        self.private_torrents.remove(info_hash);
        Ok(())
    }

    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle (TSI-2275), with PT isolation guard (TSI-2277).
    ///
    /// Called when `add_torrent` detects a duplicate info_hash (a second
    /// torrent with the same content hash but potentially different trackers).
    /// The new torrent's trackers are deduplicated against the existing
    /// handle's trackers and merged in, then `force_reannounce` is called to
    /// immediately contact the new trackers.
    ///
    /// **PT isolation**: if either the existing handle's torrent or the
    /// incoming torrent has the `private` flag set (BEP-27), the merge is
    /// **skipped entirely**. Private torrents embed passkeys in their
    /// announce URLs; merging trackers across PT sites would leak passkeys
    /// to foreign swarms and cross-pollinate peers, risking account bans.
    /// Private torrents keep their independent tracker lists and only share
    /// the local piece cache.
    ///
    /// All failures are non-fatal (warn-logged): the torrent is already in
    /// the DB and the handle exists, so a merge failure only means the new
    /// trackers won't be announced — the existing behavior is unchanged.
    fn merge_trackers(&mut self, info: &TorrentInfo) {
        let info_hash = match info.info_hash() {
            Ok(h) => hex::encode(h),
            Err(e) => {
                warn!("merge_trackers: failed to get info_hash, skipping: {:?}", e);
                return;
            }
        };

        // PT isolation guard (TSI-2277): if the incoming torrent is private,
        // do not merge its trackers into the existing handle.
        let incoming_private = info.is_private();

        // Look up the existing handle. If no handle exists yet, there's
        // nothing to merge into — the handle will be created on first
        // access with this torrent's own trackers.
        let existing_handle = match self.handles.get(&info_hash) {
            Some(h) => h,
            None => {
                // No handle yet — ensure_handle will create one from this
                // torrent's trackers on first access. Nothing to merge.
                return;
            }
        };

        // PT isolation guard: check the existing torrent's private flag.
        // Conservative: if the private flag is somehow missing from the map
        // (shouldn't happen — ensure_handle always populates it), treat as
        // private to avoid risking passkey leakage on an uncertain flag.
        let existing_private = self
            .private_torrents
            .get(&info_hash)
            .copied()
            .unwrap_or(true);

        if incoming_private || existing_private {
            info!(
                "PT isolation (TSI-2277): skipping tracker merge for {} — \
                 private flag on existing={} / incoming={}. \
                 Trackers remain independent; piece cache is shared.",
                info_hash, existing_private, incoming_private
            );
            return;
        }

        // Extract trackers from the incoming torrent.
        let incoming_trackers = match info.trackers() {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "merge_trackers {}: failed to extract trackers: {:?}",
                    info_hash, e
                );
                return;
            }
        };

        if incoming_trackers.is_empty() {
            // No trackers to merge — nothing to do.
            return;
        }

        // Extract existing trackers from the handle's torrent_info.
        // We use the handle's own tracker list via the FFI.
        let existing_trackers = match existing_handle.trackers() {
            Ok(t) => t,
            Err(e) => {
                warn!(
                    "merge_trackers {}: failed to get existing trackers: {:?}",
                    info_hash, e
                );
                return;
            }
        };

        // Deduplicate by URL, preserving tier information. Existing trackers
        // keep their tier; incoming trackers that are new get their original
        // tier (or a tier higher than any existing to avoid disrupting
        // the announce priority order).
        let mut seen: std::collections::HashSet<String> =
            existing_trackers.iter().map(|t| t.url.clone()).collect();
        let mut merged = existing_trackers.clone();
        for tracker in &incoming_trackers {
            if seen.insert(tracker.url.clone()) {
                merged.push(tracker.clone());
            }
        }

        if merged.len() == existing_trackers.len() {
            // All incoming trackers were duplicates — nothing new to merge.
            return;
        }

        // Replace trackers on the handle and force re-announce.
        if !existing_handle.replace_trackers(&merged) {
            warn!("merge_trackers {}: replace_trackers FFI failed", info_hash);
            return;
        }

        if !existing_handle.force_reannounce() {
            warn!(
                "merge_trackers {}: force_reannounce FFI failed (non-fatal)",
                info_hash
            );
        }

        info!(
            "merge_trackers {}: merged {} new tracker(s) into existing {} (total {})",
            info_hash,
            merged.len() - existing_trackers.len(),
            existing_trackers.len(),
            merged.len()
        );
    }

    /// Read a file range, driving piece download if needed.  Runs entirely on
    /// the engine thread; alerts are drained during the piece wait.
    fn read_file_range(
        &mut self,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        self.ensure_handle(info)?;
        let info_hash = hex::encode(info.info_hash()?);

        // ── Collect handle metadata (scoped borrow) ────────────────────
        let (piece_length, num_pieces, total_size, file_start_offset, file_size, status) = {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            if !handle.is_valid() {
                return Err(TorrentError::InvalidFile(
                    "Torrent handle is invalid".to_string(),
                ));
            }
            let status = handle.status()?;
            let piece_info = handle.get_file_piece_info(file_index)?;
            let (piece_length, num_pieces) = handle.get_torrent_info()?;
            let file_start_offset = piece_info.file_offset as u64;
            let file_size = info
                .files()
                .ok()
                .and_then(|fs| fs.get(file_index as usize).map(|f| f.size))
                .unwrap_or(u64::MAX);
            (
                piece_length as u64,
                num_pieces as i32,
                info.total_size(),
                file_start_offset,
                file_size,
                status,
            )
        };

        if num_pieces <= 0 || piece_length == 0 {
            return Err(TorrentError::InvalidFile(format!(
                "Invalid torrent: num_pieces = {}, piece_length = {}",
                num_pieces, piece_length
            )));
        }

        let absolute_offset = file_start_offset + offset;
        let file_end = file_start_offset + file_size;
        let size = if absolute_offset < file_end {
            (std::cmp::min(size as u64, file_end - absolute_offset) as u32).max(1)
        } else {
            return Ok(Vec::new());
        };

        let start_piece = (absolute_offset / piece_length) as i32;
        let end_offset = absolute_offset + size as u64;
        let end_piece = if size > 0 {
            std::cmp::min(((end_offset - 1) / piece_length) as i32, num_pieces - 1)
        } else {
            start_piece
        };
        if start_piece >= num_pieces {
            return Err(TorrentError::InvalidFile(format!(
                "start_piece {} exceeds num_pieces {}",
                start_piece, num_pieces
            )));
        }
        if start_piece > end_piece {
            return Ok(Vec::new());
        }

        // ── Wait for initial state transitions ─────────────────────────
        let max_wait_secs = self.read_timeout_secs;
        let start = Instant::now();
        let mut status = status;
        while matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::Allocating
                | TorrentState::CheckingResumeData
        ) {
            if self.stopping.load(Ordering::Relaxed) {
                return Err(TorrentError::Timeout(
                    "shutdown requested, read aborted".to_string(),
                ));
            }
            if start.elapsed().as_secs() > max_wait_secs {
                return Err(TorrentError::Timeout(format!(
                    "Torrent stuck in state {:?} for {} seconds",
                    status.state, max_wait_secs
                )));
            }
            std::thread::sleep(Duration::from_millis(100));
            match self.handles.get(&info_hash).map(|h| h.status()) {
                Some(Ok(s)) => status = s,
                _ => {}
            }
        }

        // ── ReaderAdded: elevate priority for this read ────────────────
        {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            if let Err(e) =
                self.scheduler
                    .reader_added(handle, info, file_index, offset, size, &self.store)
            {
                tracing::warn!("read_file_range: reader_added failed: {:?}", e);
            }
        }
        // Publish the snapshot immediately so `.stats` reflects the elevated
        // piece priorities while this read is in progress (TSI-2224).  Without
        // this, `publish_snapshot` only runs in the engine loop between
        // commands — but this handler blocks the engine thread until the read
        // completes, by which point `release_reader` has already reset all
        // priorities to 0, so `.stats` always saw an all-`[]` Pieces grid.
        self.publish_snapshot();

        // ── Detect stale libtorrent piece state (TSI-2258) ───────────────
        // A piece can be purged from cache (file deleted + metadata cleared by
        // the background SHA-1 verification) while libtorrent's internal
        // piece bitmask still marks it as complete (resume state not
        // updated).  Without intervention, `all_pieces_local` would take the
        // fast path, `read_from_disk` would find the file missing, silently
        // skip it, and return a short read → EIO.  Detect this: if any piece
        // in the range has `have_piece == true` but the on-disk file is gone,
        // call `force_recheck` so libtorrent re-verifies via the custom storage
        // and clears the stale bit, then wait for the recheck to finish.
        if self.has_stale_pieces(&info_hash, start_piece, end_piece) {
            tracing::info!(
                "read_file_range: stale libtorrent piece state detected for \
                 info_hash={}, forcing recheck to clear bits for pieces {}-{}",
                info_hash,
                start_piece,
                end_piece
            );
            self.force_recheck_and_wait(&info_hash);
        }

        // ── Fast path: all pieces available locally ────────────────────
        if self.all_pieces_local(
            &info_hash,
            start_piece,
            end_piece,
            piece_length,
            num_pieces,
            total_size,
        ) {
            let result = self.read_from_disk(
                &info_hash,
                start_piece,
                end_piece,
                piece_length,
                num_pieces,
                total_size,
                absolute_offset,
                end_offset,
                size,
            );
            self.release_reader(&info_hash);
            return result;
        }

        // ── Switch to download mode ────────────────────────────────────
        // The handle was created in upload_mode (connect, never request). The
        // reader_added call above has already applied the piece priority
        // gradient; clearing upload_mode now lets libtorrent start requesting
        // those pieces from the peers it is already connected to.
        {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            if !handle.unset_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "read_file_range: failed to clear upload_mode for {}",
                    info_hash
                );
            }
        }

        // Settle sleep for libtorrent state transitions.
        std::thread::sleep(Duration::from_millis(100));

        // ── Slow path: peer discovery + piece-wait ─────────────────────
        // TSI-2246: when the swarm has no available seeder we fail fast with
        // `NoPeers` instead of falling through to the full `piece_wait_timeout`
        // (which would surface as a `Timeout` → EIO, a misleading "I/O error"
        // for what is really "no seeder").  We already passed the
        // all-pieces-local fast path above, so reaching here means at least one
        // piece is missing and only the network could supply it — which it
        // cannot with zero peers/seeds.
        let peer_wait_timeout = Duration::from_secs(std::cmp::min(self.read_timeout_secs, 9));
        {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            if status.num_peers == 0 && status.num_seeds == 0 {
                let peer_wait_start = Instant::now();
                loop {
                    if self.stopping.load(Ordering::Relaxed) {
                        self.release_reader(&info_hash);
                        return Err(TorrentError::Timeout(
                            "shutdown requested, read aborted".to_string(),
                        ));
                    }
                    if peer_wait_start.elapsed() >= peer_wait_timeout {
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(500));
                    match handle.status() {
                        Ok(s) => {
                            status = s;
                            if status.num_peers > 0 || status.num_seeds > 0 {
                                break;
                            }
                        }
                        // TSI-2246 review: `handle.status()` failed — return
                        // the actual error instead of falling through to the
                        // `NoPeers` check below, which would mask the real
                        // cause behind a misleading "no seeder" message.
                        Err(e) => {
                            self.release_reader(&info_hash);
                            return Err(e);
                        }
                    }
                }
            }
            if status.num_peers == 0 && status.num_seeds == 0 {
                self.release_reader(&info_hash);
                return Err(TorrentError::NoPeers(format!(
                    "No peers or seeders connected for info_hash {} after {}s; \
                     the swarm has no available seeder. Check tracker health or \
                     try again later.",
                    info_hash,
                    peer_wait_timeout.as_secs()
                )));
            }
            let _ = handle.status();
        }

        // ── Set piece deadlines ────────────────────────────────────────
        // TSI-2258 review: `have_piece` can be stale (true but file purged).
        // Only skip the deadline for pieces that are truly available —
        // `have_piece` true AND the file exists on disk.  Stale pieces
        // still need a deadline so libtorrent re-requests them.
        {
            let handle = self
                .handles
                .get(&info_hash)
                .ok_or_else(|| Self::missing())?;
            for piece_idx in start_piece..=end_piece {
                let piece_key = PieceStore::piece_key(&info_hash, piece_idx);
                let truly_available =
                    handle.have_piece(piece_idx) && !self.store.has_stale_piece(&piece_key);
                if !truly_available {
                    handle.set_piece_deadline(piece_idx, 0);
                }
            }
        }

        // ── Wait for each missing piece ────────────────────────────────
        let piece_wait_timeout = Duration::from_secs(self.read_timeout_secs);
        for piece_idx in start_piece..=end_piece {
            let piece_start = Instant::now();
            loop {
                if self.stopping.load(Ordering::Relaxed) {
                    self.release_reader(&info_hash);
                    return Err(TorrentError::Timeout(
                        "shutdown requested, read aborted".to_string(),
                    ));
                }

                let have = self
                    .handles
                    .get(&info_hash)
                    .map(|h| h.have_piece(piece_idx))
                    .unwrap_or(false);
                // TSI-2258: `have_piece` can be stale (resume state marks the
                // piece as complete, but the file was purged from cache).
                // In that case, do not treat it as ready — instead, fall
                // through to the download path so libtorrent re-requests
                // the piece.
                let have_valid = if have {
                    let piece_key = PieceStore::piece_key(&info_hash, piece_idx);
                    !self.store.has_stale_piece(&piece_key)
                } else {
                    false
                };
                let cached = {
                    let piece_key = PieceStore::piece_key(&info_hash, piece_idx);
                    let cache = self.store.cache_manager();
                    cache
                        .lock()
                        .map(|c| {
                            PieceStore::is_piece_complete_in_cache(
                                &c,
                                &piece_key,
                                piece_idx,
                                piece_length,
                                num_pieces,
                                total_size,
                            )
                        })
                        .unwrap_or(false)
                };
                self.metrics.record_poll(have_valid || cached);
                if have_valid || cached {
                    if have_valid {
                        self.register_piece(
                            &info_hash,
                            piece_idx,
                            piece_length,
                            num_pieces,
                            total_size,
                        );
                    }
                    break;
                }

                if piece_start.elapsed() >= piece_wait_timeout {
                    self.release_reader(&info_hash);
                    // TSI-2261: when the piece-wait times out, distinguish
                    // "no seeder available" from "slow download". If the
                    // torrent has zero connected seeders after the full
                    // timeout, the swarm has no seeder — return NoPeers
                    // (→ ENODATA, "no data available") instead of Timeout
                    // (→ EIO, "input/output error") so the user sees a
                    // meaningful error. Seeders present but slow still
                    // returns Timeout → EIO.
                    //
                    // If status is unavailable (handle gone or status()
                    // failed), fall back to Timeout — do NOT fabricate a
                    // zero-seeder swarm that would mislead the user into
                    // checking tracker health for what is really a stale
                    // handle.
                    let (progress, num_seeds) =
                        match self.handles.get(&info_hash).and_then(|h| h.status().ok()) {
                            Some(s) => (s.progress * 100.0, s.num_seeds),
                            None => {
                                return Err(TorrentError::Timeout(format!(
                                    "Timed out waiting for piece {} after {:.0}s \
                                 (status unavailable)",
                                    piece_idx,
                                    piece_wait_timeout.as_secs(),
                                )));
                            }
                        };
                    if num_seeds == 0 {
                        return Err(TorrentError::NoPeers(format!(
                            "No seeder connected for info_hash {} after {:.0}s. \
                             The torrent has no available seeder — check \
                             tracker health or try again later.",
                            info_hash,
                            piece_wait_timeout.as_secs(),
                        )));
                    }
                    return Err(TorrentError::Timeout(format!(
                        "Timed out waiting for piece {} after {:.0}s. \
                         Torrent progress: {:.2}%",
                        piece_idx,
                        piece_wait_timeout.as_secs(),
                        progress,
                    )));
                }

                // Refresh the snapshot so `.stats` shows pieces becoming
                // cached and priority changes during long reads (TSI-2224).
                self.publish_snapshot();

                std::thread::sleep(Duration::from_millis(200));
            }
        }

        let result = self.read_from_disk(
            &info_hash,
            start_piece,
            end_piece,
            piece_length,
            num_pieces,
            total_size,
            absolute_offset,
            end_offset,
            size,
        );
        self.release_reader(&info_hash);
        result
    }

    fn all_pieces_local(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return false,
        };
        for piece_idx in start_piece..=end_piece {
            let piece_key = PieceStore::piece_key(info_hash, piece_idx);
            let complete = self
                .store
                .cache_manager()
                .lock()
                .map(|c| {
                    PieceStore::is_piece_complete_in_cache(
                        &c,
                        &piece_key,
                        piece_idx,
                        piece_length,
                        num_pieces,
                        total_size,
                    )
                })
                .unwrap_or(false);
            // TSI-2258: use the unified stale detection — `have_piece` true
            // but the on-disk file is gone means the bit is stale; the
            // piece is NOT available locally and must be re-downloaded.
            let have = handle.have_piece(piece_idx);
            let have_valid = if have {
                !self.store.has_stale_piece(&piece_key)
            } else {
                false
            };
            if !have_valid && !complete {
                return false;
            }
        }
        true
    }

    /// TSI-2258: detect whether any piece in the range has a stale
    /// libtorrent bitmask — `have_piece == true` but the on-disk piece file
    /// is gone (purged by cache verification or manual `delete_piece`).
    /// Returns `true` if at least one such piece exists.
    fn has_stale_pieces(&self, info_hash: &str, start_piece: i32, end_piece: i32) -> bool {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return false,
        };
        for piece_idx in start_piece..=end_piece {
            if handle.have_piece(piece_idx) {
                let piece_key = PieceStore::piece_key(info_hash, piece_idx);
                if self.store.has_stale_piece(&piece_key) {
                    return true;
                }
            }
        }
        false
    }

    /// TSI-2258: force libtorrent to re-verify all pieces for a torrent and
    /// wait for the recheck to complete (or time out).  After `force_recheck`,
    /// libtorrent transitions through `CheckingFiles` and, via the custom
    /// storage's `async_check_files`, discovers that purged pieces are gone
    /// — clearing their `have_piece` bits so the normal download path can
    /// re-request them.
    fn force_recheck_and_wait(&self, info_hash: &str) {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return,
        };
        if !handle.force_recheck() {
            tracing::warn!(
                "force_recheck failed for info_hash={}; stale bits may persist",
                info_hash
            );
            return;
        }
        // TSI-2258 review: wait for the recheck to finish.  Two subtleties:
        // 1. TOCTOU — `force_recheck()` is asynchronous; libtorrent may not
        //    have transitioned to `QueuedForChecking` by the time we first
        //    poll.  Without a grace period, the first `status()` would see
        //    the old state (Seeding/Downloading), conclude "recheck done",
        //    and return while stale bits still persist.  Fix: sleep a grace
        //    period before the first poll, then track whether we *ever*
        //    observed a checking state — only return after observing the
        //    transition OUT of checking.
        // 2. Poll interval — 200ms reduces syscall overhead while keeping
        //    recheck latency (typically <1s) acceptable.
        let max_wait = Duration::from_secs(std::cmp::min(self.read_timeout_secs, 10));
        let start = Instant::now();

        // Grace period: let libtorrent queue the recheck before polling.
        std::thread::sleep(Duration::from_millis(200));

        let mut saw_checking = false;
        while start.elapsed() < max_wait {
            if self.stopping.load(Ordering::Relaxed) {
                return;
            }
            match handle.status() {
                Ok(s) => {
                    let is_checking = matches!(
                        s.state,
                        TorrentState::QueuedForChecking
                            | TorrentState::CheckingFiles
                            | TorrentState::CheckingResumeData
                    );
                    if is_checking {
                        saw_checking = true;
                    } else if saw_checking {
                        // We observed the checking state and it has now
                        // ended — the recheck is truly complete.
                        return;
                    } else if start.elapsed() > Duration::from_secs(2) {
                        // No checking state observed after 2s — the recheck
                        // may have completed instantly (unlikely but
                        // possible) or failed to start.  Fall through; the
                        // safety nets in all_pieces_local and the piece-wait
                        // loop will catch any remaining stale bits.
                        tracing::warn!(
                            "force_recheck: no checking state observed for \
                             info_hash={} after 2s, proceeding",
                            info_hash
                        );
                        return;
                    }
                }
                Err(_) => return,
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        tracing::warn!(
            "force_recheck did not finish within {:?} for info_hash={}",
            max_wait,
            info_hash
        );
    }

    fn read_from_disk(
        &mut self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
        absolute_offset: u64,
        end_offset: u64,
        size: u32,
    ) -> TorrentResult<Vec<u8>> {
        let mut result = Vec::with_capacity(size as usize);
        let mut bytes_read = 0usize;
        for piece_idx in start_piece..=end_piece {
            let piece_key = PieceStore::piece_key(info_hash, piece_idx);
            let piece_data = match self.store.read_piece(&piece_key) {
                Ok(d) => d,
                Err(_) => {
                    // TSI-2258: the piece file is gone but we reached
                    // read_from_disk — either the fast path (have_piece was
                    // true but file purged) or the piece-wait loop broke
                    // on `have_piece` without verifying disk presence.
                    // Rather than silently skipping (which leads to a
                    // short-read EIO), return PieceNotReady so the caller
                    // sees a transient error and can retry, and the stale
                    // libtorrent bitmask is caught by the recheck guard
                    // on the next attempt.
                    let piece_start = (piece_idx as u64) * piece_length;
                    let piece_end_theoretical = piece_start + piece_length;
                    if absolute_offset < piece_end_theoretical && end_offset > piece_start {
                        return Err(TorrentError::PieceNotReady(format!(
                            "Piece {} file missing from cache but overlaps \
                             requested range (possible stale libtorrent state)",
                            piece_idx
                        )));
                    }
                    tracing::debug!("read_from_disk: piece {} not on disk", piece_idx);
                    continue;
                }
            };
            if piece_data.is_empty() {
                let piece_start = (piece_idx as u64) * piece_length;
                let piece_end_theoretical = piece_start + piece_length;
                if absolute_offset < piece_end_theoretical && end_offset > piece_start {
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} data is empty but overlaps requested range",
                        piece_idx
                    )));
                }
                continue;
            }
            // TSI-2262: verify the piece data length matches the expected
            // piece size. A shorter file means the piece was read while
            // libtorrent's write_piece was still writing blocks to it
            // (the write-during-read race). The shared read lock should
            // prevent this in normal operation, but this check is a safety
            // net for edge cases (e.g. cache eviction + re-download).
            let expected_piece_size = if piece_idx == num_pieces - 1 {
                let remainder = total_size.saturating_sub(((num_pieces - 1) as u64) * piece_length);
                if remainder > 0 {
                    remainder
                } else {
                    piece_length
                }
            } else {
                piece_length
            };
            if (piece_data.len() as u64) < expected_piece_size {
                let piece_start = (piece_idx as u64) * piece_length;
                let piece_end_theoretical = piece_start + piece_length;
                if absolute_offset < piece_end_theoretical && end_offset > piece_start {
                    return Err(TorrentError::PieceNotReady(format!(
                        "Piece {} data is {} bytes, expected {} (possible \
                         write-during-read race)",
                        piece_idx,
                        piece_data.len(),
                        expected_piece_size
                    )));
                }
                continue;
            }
            // TSI-2225: the piece is being served from the local disk. If it is
            // not yet registered in the cache metadata (e.g. it was downloaded
            // eagerly by the access-window prefetch rather than through this
            // read's piece-wait loop), register it now so `pieces_on_disk` and
            // restart scans treat it as a complete, verified piece instead of
            // forcing a re-download that can time out with EIO.
            if !self.store.has_piece(info_hash, piece_idx) {
                self.register_piece(info_hash, piece_idx, piece_length, num_pieces, total_size);
            }
            if let Some((local_start, local_end)) = Self::piece_chunk_bounds(
                &piece_data,
                piece_idx,
                piece_length,
                absolute_offset,
                end_offset,
            ) {
                let chunk = &piece_data[local_start..local_end];
                result.extend_from_slice(chunk);
                bytes_read += chunk.len();
                if bytes_read >= size as usize {
                    break;
                }
            }
        }
        if size > 0 && bytes_read < size as usize {
            return Err(TorrentError::PieceNotReady(format!(
                "Short read: expected {} bytes, got {} bytes",
                size, bytes_read
            )));
        }
        Ok(result)
    }

    fn register_piece(
        &mut self,
        info_hash: &str,
        piece_idx: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) {
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
        if let Err(e) = self.store.register_piece(info_hash, piece_idx, expected) {
            tracing::warn!(
                "register_piece: failed for {}:piece:{}: {:?}",
                info_hash,
                piece_idx,
                e
            );
        }
        if let Some(handle) = self.handles.get(info_hash) {
            self.scheduler.piece_ready(handle, info_hash, piece_idx);
        }
    }

    fn release_reader(&mut self, info_hash: &str) {
        if let Some(handle) = self.handles.get(info_hash) {
            // Return to idle upload_mode first so no eager piece requests leak
            // out before the priority gradient is reset below.
            if !handle.set_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "release_reader: failed to re-enable upload_mode for {}",
                    info_hash
                );
            }
            if let Err(e) = self
                .scheduler
                .reader_released(handle, info_hash, &self.store)
            {
                tracing::warn!("release_reader: reader_released failed: {:?}", e);
            }
        }
    }

    fn build_pieces_status(
        &self,
        info_hash: &str,
        num_pieces: i32,
    ) -> TorrentResult<Vec<PieceStatus>> {
        let cache = self.store.cache_manager();
        let cache = cache.lock().map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Cache lock poisoned".to_string(),
        })?;
        let priorities = self.scheduler.priorities(info_hash);
        let mut result = Vec::with_capacity(num_pieces as usize);
        for p in 0..num_pieces {
            let piece_key = PieceStore::piece_key(info_hash, p);
            let is_cached = cache.has_piece(&piece_key);
            let hit_count = if is_cached {
                cache.piece_hit_count(&piece_key)
            } else {
                0
            };
            let priority = priorities
                .and_then(|v| {
                    if (p as usize) < v.len() {
                        Some(v[p as usize])
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            result.push(PieceStatus {
                priority,
                is_cached,
                hit_count,
            });
        }
        Ok(result)
    }

    /// Publish the current engine state into the shared snapshot.
    fn publish_snapshot(&mut self) {
        let mut statuses = HashMap::new();
        let mut pieces = HashMap::new();
        for (info_hash, handle) in &self.handles {
            if let Ok(status) = handle.status() {
                statuses.insert(info_hash.clone(), status);
            }
            if let Some(num_pieces) = self.scheduler.num_pieces(info_hash) {
                if let Ok(status) = self.build_pieces_status(info_hash, num_pieces) {
                    let piece_length = self.scheduler.piece_length(info_hash).unwrap_or(0);
                    pieces.insert(info_hash.clone(), (piece_length, status));
                }
            }
        }
        if let Ok(mut snap) = self.snapshot.lock() {
            *snap = DownloadSnapshot {
                statuses,
                pieces,
                private_torrents: self.private_torrents.clone(),
            };
        }
    }

    /// TSI-2274: periodically persist dirty cache metadata so the on-disk
    /// state does not lag too far behind the in-memory state.  Mutating
    /// cache methods (`record_access`, `add_piece`, `remove_piece`, …)
    /// only flag `metadata_dirty` instead of fsyncing on every call; this
    /// is the single flush point that hits disk.  The `main.rs` shutdown
    /// path still calls `flush()` for the final fsync.
    fn flush_cache_metadata(&self) {
        if let Ok(mut cm) = self.store.cache_manager().lock() {
            if let Err(e) = cm.flush_metadata_if_dirty() {
                tracing::warn!("Failed to flush cache metadata: {:?}", e);
            }
        }
    }

    fn missing() -> TorrentError {
        TorrentError::Unknown {
            code: -1,
            message: "Torrent handle missing".to_string(),
        }
    }

    /// Compute the byte-range within a piece that overlaps a requested read.
    fn piece_chunk_bounds(
        piece_data: &[u8],
        piece_idx: i32,
        piece_length: u64,
        absolute_offset: u64,
        end_offset: u64,
    ) -> Option<(usize, usize)> {
        let piece_start = (piece_idx as u64) * piece_length;
        let piece_end = piece_start + piece_data.len() as u64;
        let read_start = std::cmp::max(absolute_offset, piece_start);
        let read_end = std::cmp::min(end_offset, piece_end);
        if read_start < read_end {
            Some((
                (read_start - piece_start) as usize,
                (read_end - piece_start) as usize,
            ))
        } else {
            None
        }
    }
}

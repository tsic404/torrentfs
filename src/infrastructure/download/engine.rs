//! `DownloadEngine` — single-owner-thread actor for the download subsystem.
//! The engine thread exclusively owns the libtorrent `Session`, all
//! `TorrentHandle`s, [`PieceStore`] (data plane) and [`PieceScheduler`]
//! (control plane). Callers send [`Command`]s over `mpsc` and receive on a
//! `sync_channel`, so raw libtorrent pointers never cross a thread boundary
//! (hence the dropped `unsafe impl Send`). Non-blocking `.stats` reads use a
//! shared [`DownloadSnapshot`], republished each tick only while a `.stats`
//! reader is looking (see [`READER_DEMAND_TTL`]) and refreshed immediately by
//! mutating commands; an unread daemon does no periodic FFI work.  This
//! replaces the old big-lock `try_lock`.
//!
//! A read that has to wait on the swarm never blocks the engine thread: it is
//! parked on [`EngineState::pending_reads`] and advanced one step per loop
//! iteration, so its peer/piece-wait window cannot serialize other commands.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use crate::error::{TorrentError, TorrentResult};
use crate::infrastructure::alert::{AlertConsumer, SharedSessionStats};
use crate::infrastructure::cache::CacheManager;
use crate::infrastructure::config::TorrentfsConfig;
use crate::infrastructure::metadata::{TorrentInfo, TrackerEntry};
use crate::infrastructure::metrics::Metrics;
use tracing::{info, warn};

use super::piece_scheduler::{PiecePriorityConfig, PieceScheduler, PieceStatus, ReadId};
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
    /// Blocks the caller until the pieces are available; the engine thread
    /// itself stays free (the read parks on the pending-read queue).
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
    /// announcing/seeding a removed torrent.
    RemoveHandle { info_hash: String },
    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle. The engine checks the private flag:
    /// if either the existing or incoming torrent is private, the merge is
    /// skipped to prevent PT passkey leakage and peer cross-pollination.
    /// Fire-and-forget: the result is logged, not returned to the caller.
    MergeTrackers { info: Arc<TorrentInfo> },
    /// Query the current tracker list on a torrent handle (test
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
    /// Per-info_hash announce targets (`url` + `tier`) of the live handle,
    /// including trackers merged from a duplicate info_hash. Absent when the
    /// info_hash has no handle or its tracker list could not be read — an
    /// absent entry never means "no trackers".
    pub trackers: HashMap<String, Vec<TrackerEntry>>,
    /// Per-info_hash private flag. A torrent is "private" when
    /// its info dict has `private=1` (BEP-27). Private torrents are isolated
    /// from cross-site tracker merging to prevent passkey leakage and peer
    /// cross-pollination across PT swarms.
    pub private_torrents: HashMap<String, bool>,
    /// Per-info_hash seconds the swarm has been *continuously* empty (no peers
    /// and no seeds).  Absent for a swarm that currently has a peer/seed and
    /// for an info_hash whose status could not be read — so `None` never means
    /// "empty for zero seconds", it means "not observed empty".
    pub empty_swarm_secs: HashMap<String, u64>,
    /// Per-info_hash state of the reads parked in the engine for it, from the
    /// same `pending_reads` pass.  An absent info_hash means no read is parked
    /// (nobody is waiting), which `.stats` renders differently from a wait of
    /// zero seconds; both fields of an entry therefore always agree.
    pub waiting_reads: HashMap<String, WaitingReads>,
    /// Per-info_hash seconds a seeder has been connected with a zero download
    /// rate while a read waits — the sustained form of the `.stats` slow-swarm
    /// signal.  Absent for a torrent that is not in that state.
    pub slow_swarm_secs: HashMap<String, u64>,
}

/// Reads parked waiting for one torrent, aggregated for `.stats`.
///
/// A parked read is a read the engine is holding until data arrives, whichever
/// phase it is in (`WaitState`, `Recheck`, peer wait, piece wait) — so the
/// count and the age always describe the same set, and `.stats` can never
/// report a wait with zero readers beside it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WaitingReads {
    /// Whole seconds the oldest parked read has been waiting.  One continuous
    /// window per read: a phase switch does not restart it.
    pub oldest_secs: u64,
    /// Reads currently parked for this torrent.
    pub count: u32,
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
    /// Configured peer-discovery wait; [`peer_discovery_window_secs`] applies
    /// the `read_timeout_secs` cap and the optional `peer_wait_cap_secs` on top
    /// of it.
    peer_discovery_wait_secs: u64,
    /// Optional hard ceiling on the peer-discovery window; `None` leaves the
    /// window to `read_timeout_secs` and `peer_discovery_wait_secs`.
    peer_wait_cap_secs: Option<u64>,
    /// Configured no-seeder piece-wait window, capped by `read_timeout_secs`
    /// like the other waits.
    no_seeder_read_timeout_secs: u64,
    /// Counter bumped by every `.stats` accessor; the engine thread opens a
    /// fresh [`READER_DEMAND_TTL`] window whenever it observes a change, which
    /// is what keeps the periodic snapshot publish and session-stats sample
    /// running while someone is looking at `.stats`.
    reader_demand: Arc<AtomicU64>,
}

/// State owned exclusively by the engine thread.
struct EngineState {
    session: Session,
    handles: HashMap<String, TorrentHandle>,
    /// Per-info_hash private flag. Populated at handle creation
    /// time from `TorrentInfo::is_private()`. Used to guard tracker merging:
    /// if either the existing or incoming torrent is private, the merge is
    /// skipped to prevent PT passkey leakage and peer cross-pollination.
    private_torrents: HashMap<String, bool>,
    store: PieceStore,
    read_timeout_secs: u64,
    /// Configured peer-discovery wait; [`peer_discovery_window_secs`] applies
    /// the `read_timeout_secs` cap and the optional `peer_wait_cap_secs` on top
    /// of it.
    peer_discovery_wait_secs: u64,
    /// Optional hard ceiling on the peer-discovery window; `None` leaves the
    /// window to `read_timeout_secs` and `peer_discovery_wait_secs`.
    peer_wait_cap_secs: Option<u64>,
    /// Configured no-seeder piece-wait window, capped by `read_timeout_secs`
    /// like the other waits.
    no_seeder_read_timeout_secs: u64,
    scheduler: PieceScheduler,
    cache_dir: String,
    metrics: Arc<Metrics>,
    snapshot: Arc<Mutex<DownloadSnapshot>>,
    stopping: Arc<AtomicBool>,
    alert_consumer: Option<AlertConsumer>,
    /// Piece-completion events forwarded by the alert consumer thread,
    /// drained by the engine loop and turned into piece registration.
    piece_finished_rx: mpsc::Receiver<(String, i32)>,
    /// Reads parked while they wait on the swarm.  The engine loop polls them
    /// once per iteration instead of blocking the engine thread inside a read,
    /// so one sourceless read cannot serialize every other command behind its
    /// peer/piece-wait window.
    pending_reads: Vec<PendingRead>,
    /// Per-info_hash cold-start flight, shared by every read parked in the
    /// peer-discovery phase.  Concurrent cold reads of one torrent therefore
    /// run a single probe and share its outcome instead of each starting its
    /// own window and making its own "no seeder" decision.
    cold_flights: HashMap<String, ColdFlight>,
    /// Per-info_hash verdict of the last probe that found the swarm empty,
    /// inherited by the reads that follow it (see [`SourcelessSwarm`]).
    sourceless: HashMap<String, SourcelessSwarm>,
    /// Per-info_hash instant the swarm last became empty (no peers, no seeds).
    /// Kept across ticks so the published age measures one continuous empty
    /// window instead of the time since the last snapshot.
    empty_swarm_since: HashMap<String, Instant>,
    /// Accessor counter shared with the engine handle (see
    /// [`DownloadEngine::mark_reader_demand`]).
    reader_demand: Arc<AtomicU64>,
    /// Counter value observed on the last iteration; a change opens a fresh
    /// reader-demand window.
    seen_reader_demand: u64,
    /// End of the current reader-demand window; `None` until the first
    /// `.stats` read.
    reader_demand_until: Option<Instant>,
    /// Per-info_hash instant a seeder was last connected with a zero download
    /// rate while a read waited.  Same continuous-window treatment as
    /// [`Self::empty_swarm_since`], so the `.stats` slow-swarm alert fires on a
    /// sustained stall rather than on the zero rate of a just-connected seeder.
    slow_swarm_since: HashMap<String, Instant>,
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
        let scheduler = PieceScheduler::new(
            PiecePriorityConfig::from_toml(&config.piece_priority),
            cache_size,
        );

        let read_timeout_secs = config.timeouts.resolved_read_timeout_secs();
        let peer_discovery_wait_secs = config.timeouts.resolved_peer_discovery_wait_secs();
        let peer_wait_cap_secs = config.timeouts.resolved_peer_wait_cap_secs();
        let no_seeder_read_timeout_secs = config.timeouts.resolved_no_seeder_read_timeout_secs();

        let (tx, rx) = mpsc::channel::<Command>();
        let stopping = Arc::new(AtomicBool::new(false));
        let shared_stats = SharedSessionStats::new();
        let snapshot = Arc::new(Mutex::new(DownloadSnapshot::default()));
        let reader_demand = Arc::new(AtomicU64::new(0));

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
        let thread_reader_demand = reader_demand.clone();

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
                // Piece-completion events flow from the consumer thread back to
                // this engine loop over a dedicated channel; the engine drains
                // them and registers finished pieces (background/prefetch
                // downloads never go through a read's piece-wait loop).
                let (piece_finished_tx, piece_finished_rx) = mpsc::channel::<(String, i32)>();
                let alert_consumer = unsafe {
                    AlertConsumer::spawn(
                        session.inner(),
                        shared_stats.clone(),
                        thread_metrics.clone(),
                        piece_finished_tx,
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
                    peer_discovery_wait_secs,
                    peer_wait_cap_secs,
                    no_seeder_read_timeout_secs,
                    metrics: thread_metrics,
                    snapshot: thread_snapshot,
                    stopping: thread_stopping,
                    alert_consumer: Some(alert_consumer),
                    piece_finished_rx,
                    pending_reads: Vec::new(),
                    cold_flights: HashMap::new(),
                    sourceless: HashMap::new(),
                    empty_swarm_since: HashMap::new(),
                    reader_demand: thread_reader_demand,
                    seen_reader_demand: 0,
                    reader_demand_until: None,
                    slow_swarm_since: HashMap::new(),
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
            peer_discovery_wait_secs,
            peer_wait_cap_secs,
            no_seeder_read_timeout_secs,
            reader_demand,
        })
    }

    // ── Non-blocking snapshots (used by `.stats`) ────────────────────

    /// Shared cache handle (non-blocking `.stats` / on-disk checks).
    pub fn cache_manager(&self) -> Arc<Mutex<CacheManager>> {
        self.cache_manager.clone()
    }

    /// Cached session stats snapshot.
    pub fn snapshot_stats(&self) -> SessionStats {
        self.mark_reader_demand();
        self.shared_stats.snapshot()
    }

    /// Note that a `.stats` reader just consumed the shared snapshot.
    ///
    /// The engine publishes the snapshot and samples session stats only while
    /// a reader is looking (see [`READER_DEMAND_TTL`]); a counter rather than a
    /// flag so a read that lands while the engine is publishing still opens a
    /// fresh window instead of being swallowed by the publish in flight.
    fn mark_reader_demand(&self) {
        self.reader_demand.fetch_add(1, Ordering::Relaxed);
    }

    /// Non-blocking torrent status from the last engine snapshot.
    pub fn try_torrent_status(&self, info_hash: &str) -> Option<TorrentStatus> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .statuses
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking piece status from the last engine snapshot.
    pub fn try_pieces_status(&self, info_hash: &str) -> Option<(u64, Vec<PieceStatus>)> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .pieces
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking announce targets from the last engine snapshot: the
    /// handle's tracker list (`url` + `tier`), after any tracker merge.  `None`
    /// when the snapshot was locked, or holds no readable list for the
    /// info_hash — never "zero trackers", which is `Some(vec![])`.  Used by
    /// `.stats` to show which trackers the torrent announces to.
    pub fn try_trackers(&self, info_hash: &str) -> Option<Vec<TrackerEntry>> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .trackers
            .get(info_hash)
            .cloned()
    }

    /// Non-blocking private-flag check from the last engine snapshot.
    /// Returns `Some(true)` if the torrent's info dict has
    /// `private=1`, `Some(false)` if not, `None` if the info_hash has no
    /// handle in the snapshot. Used by `.stats` to display the PT isolation
    /// state.
    pub fn try_is_private(&self, info_hash: &str) -> Option<bool> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .private_torrents
            .get(info_hash)
            .copied()
    }

    /// Non-blocking "swarm has been empty for this many seconds" check from
    /// the last engine snapshot.  `None` when the snapshot has no entry for
    /// the info_hash — the handle is gone, its status was unreadable, or the
    /// swarm currently has a peer/seed.  Used by `.stats` to keep the health
    /// alert off a transient empty sample.
    pub fn try_empty_swarm_secs(&self, info_hash: &str) -> Option<u64> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .empty_swarm_secs
            .get(info_hash)
            .copied()
    }

    /// Non-blocking parked-read state for this info_hash from the last engine
    /// snapshot, or `None` when no read of that torrent is parked.  Both facts
    /// `.stats` shows about waiting — the oldest read's age and the number of
    /// parked reads — come from this one entry, so they can never contradict
    /// each other.  Used by `.stats` without blocking on the engine command
    /// channel.
    pub fn try_waiting_reads(&self, info_hash: &str) -> Option<WaitingReads> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .waiting_reads
            .get(info_hash)
            .copied()
    }

    /// Non-blocking "a connected seeder has delivered nothing for this many
    /// seconds while a read waited" check from the last engine snapshot.
    /// `None` when the snapshot has no entry — the torrent is not in that
    /// state.  Used by `.stats` to keep the slow-swarm alert off a zero rate
    /// sampled before the seeder's first block arrives.
    pub fn try_slow_swarm_secs(&self, info_hash: &str) -> Option<u64> {
        self.mark_reader_demand();
        self.snapshot
            .try_lock()
            .ok()?
            .slow_swarm_secs
            .get(info_hash)
            .copied()
    }

    pub fn metrics(&self) -> Arc<Metrics> {
        self.metrics.clone()
    }

    pub fn read_timeout_secs(&self) -> u64 {
        self.read_timeout_secs
    }

    /// Configured no-seeder piece-wait window (seconds).  `.stats` sizes its
    /// empty-swarm health grace from this so the alert cannot fire while a
    /// no-seeder read is still inside the window the engine gave it.
    pub fn no_seeder_read_timeout_secs(&self) -> u64 {
        self.no_seeder_read_timeout_secs
    }

    /// Worst-case seconds a single `read_file_range` call may block its caller
    /// before returning its own result.  The FUSE deferred-read deadline must
    /// cover this budget (plus dispatch margin) so a ticket is never expired
    /// with ENODATA while the read is still legitimately waiting for a slow
    /// seeder.
    pub fn read_wait_budget_secs(&self) -> u64 {
        read_wait_budget_secs(
            self.read_timeout_secs,
            self.peer_discovery_wait_secs,
            self.peer_wait_cap_secs,
            self.no_seeder_read_timeout_secs,
        )
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
    /// dispatch loop on a busy engine.
    pub fn remove_handle(&self, info_hash: &str) -> TorrentResult<()> {
        self.send(Command::RemoveHandle {
            info_hash: info_hash.to_string(),
        })
    }

    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle. Fire-and-forget: the command is queued
    /// and executed on the engine thread; this call does not block. The
    /// engine checks the private flag — if either the existing or incoming
    /// torrent is private, the merge is skipped (PT isolation).
    pub fn merge_trackers(&self, info: Arc<TorrentInfo>) -> TorrentResult<()> {
        self.send(Command::MergeTrackers { info })
    }

    /// Query the current tracker list on a torrent handle.
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

    /// Read a file range, driving the piece download if needed.
    ///
    /// Blocks the calling thread until the range is served or the wait windows
    /// elapse; the download itself is driven cooperatively on the engine thread,
    /// which stays free to serve other commands while this read waits.
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
///
/// Set once at add time (`add_torrent_upload_mode`) and cleared once by the
/// first read that needs the swarm — never re-armed.  libtorrent 2.1.1's
/// `torrent::set_upload_mode(true)` reaches `peer_connection::cancel_all_requests()`
/// for every connected peer, and that dereferences the piece picker
/// unconditionally; a torrent that finished every piece is a complete seed
/// whose picker libtorrent has already released, so re-arming it there aborts
/// the process.  The idle "request nothing" state is carried by the
/// piece-priority vector instead (see [`PieceScheduler::recompute`]).
const UPLOAD_MODE_FLAG: u64 = 1 << 1;

/// Upper bound (seconds) on the `force_recheck` wait in the stale-piece path.
/// It runs before peer discovery in the same read, so it adds to the read
/// budget.
pub(crate) const RECHECK_WAIT_CAP_SECS: u64 = 10;

/// Piece-wait window (seconds) for a read whose peer-discovery wait already
/// elapsed without a seeder.  Zero: the swarm probe (`force_reannounce` + up to
/// [`peer_discovery_window_secs`]) already gave a seeder time to appear;
/// finding none means the read is sourceless, so the piece-wait loop fails on
/// its first iteration rather than spending the no-seeder window again.  A
/// whole-file `cat` fans out into one 128 KiB chunk read per command, each
/// previously paying its own no-seeder window.
pub(crate) const NO_SEEDER_FAST_FAIL_SECS: u64 = 0;

/// Peer-discovery window (seconds) for a read that finds an empty swarm: how
/// long it waits for a peer or seeder to appear before the swarm counts as
/// sourceless.
///
/// The window is `min(read_timeout_secs, peer_discovery_wait_secs)` — the
/// configured discovery wait covers a cold mount's first read (tracker announce
/// plus peer connect) while a short read timeout still bounds the whole read —
/// further capped by `peer_wait_cap_secs` when that optional fail-fast ceiling
/// is set.  Pure so the effective window is unit-testable without a running
/// engine.
fn peer_discovery_window_secs(
    read_timeout_secs: u64,
    peer_discovery_wait_secs: u64,
    peer_wait_cap_secs: Option<u64>,
) -> u64 {
    let window = std::cmp::min(read_timeout_secs, peer_discovery_wait_secs);
    peer_wait_cap_secs.map_or(window, |cap| std::cmp::min(window, cap))
}

/// Worst-case seconds a single `read_file_range` call may block its caller (a
/// FUSE deferred-read worker) before returning its own result: the
/// state-transition wait (`read_timeout_secs`), the recheck wait (≤
/// [`RECHECK_WAIT_CAP_SECS`]), the peer-discovery window (≤
/// [`peer_discovery_window_secs`]) and the piece-wait window.  The piece-wait
/// worst case is a seeder connecting at the very end of the short no-seeder
/// window (`no_seeder_read_timeout_secs`) and then getting a full
/// `read_timeout_secs` window from its connect time (the window resets on
/// seeder connect — see [`EngineState::poll_piece_wait`]).
///
/// This is the budget the FUSE deferred-read deadline must cover.  Pure so it
/// is unit-testable without a running engine.
pub(crate) fn read_wait_budget_secs(
    read_timeout_secs: u64,
    peer_discovery_wait_secs: u64,
    peer_wait_cap_secs: Option<u64>,
    no_seeder_read_timeout_secs: u64,
) -> u64 {
    let recheck_wait = std::cmp::min(read_timeout_secs, RECHECK_WAIT_CAP_SECS);
    let peer_wait = peer_discovery_window_secs(
        read_timeout_secs,
        peer_discovery_wait_secs,
        peer_wait_cap_secs,
    );
    let no_seeder_wait = std::cmp::min(read_timeout_secs, no_seeder_read_timeout_secs);
    read_timeout_secs
        .saturating_add(recheck_wait)
        .saturating_add(peer_wait)
        .saturating_add(no_seeder_wait)
        .saturating_add(read_timeout_secs)
}

/// Piece-wait window (seconds) for a single read: the full `read_timeout_secs`
/// when a seeder is connected (a slow-but-present seeder may still finish); the
/// no-seeder window (`no_seeder_read_timeout_secs`, itself capped by
/// `read_timeout_secs`) when no seeder is connected and peer discovery has not
/// elapsed (leechers may still serve, or a seeder may still connect); or zero
/// ([`NO_SEEDER_FAST_FAIL_SECS`]) once peer discovery elapsed with no seeder —
/// the read is sourceless and fails fast instead of tying up the caller and its
/// FUSE deadline budget for a seeder the probe proved cannot arrive.  Pure so
/// it is unit-testable without a running engine.
fn piece_wait_window_secs(
    has_seeder: bool,
    is_peer_wait_exhausted: bool,
    read_timeout_secs: u64,
    no_seeder_read_timeout_secs: u64,
) -> u64 {
    if has_seeder {
        read_timeout_secs
    } else if is_peer_wait_exhausted {
        NO_SEEDER_FAST_FAIL_SECS
    } else {
        std::cmp::min(read_timeout_secs, no_seeder_read_timeout_secs)
    }
}

/// Whether a swarm-status snapshot is completely empty — no peers and no
/// seeds.  The no-seeder fast-fail (`is_peer_wait_exhausted`) may only be set for
/// an empty swarm: a leecher (`num_peers > 0`) is still a potential source (or
/// a future seeder), so it keeps the regular no-seeder piece-wait window
/// rather than failing in zero seconds.  Pure so the exact condition is
/// unit-testable without a running engine.
fn swarm_is_empty(num_peers: i32, num_seeds: i32) -> bool {
    num_peers == 0 && num_seeds == 0
}

/// Advance a per-info_hash "condition has held since" clock.
///
/// `samples` holds one `(info_hash, holds)` entry per handle.  A condition that
/// keeps holding keeps its original start instant — the published age is
/// therefore one *continuous* window, not the time since the last sample.  A
/// condition that stops drops its entry, so its next window starts a fresh
/// clock; entries for handles that disappeared are dropped with it (the
/// returned map only covers `samples`).  Pure so the clock semantics are
/// unit-testable without a running engine.
///
/// Used for the empty-swarm clock (`.stats` health alert) and the slow-swarm
/// clock (seeder connected, zero download rate, a read waiting): both are
/// instantaneous samples that flap, so both alerts need a sustained window.
fn advance_condition_since(
    previous: &HashMap<String, Instant>,
    samples: &[(String, bool)],
    now: Instant,
) -> HashMap<String, Instant> {
    samples
        .iter()
        .filter(|(_, holds)| *holds)
        .map(|(info_hash, _)| {
            let since = previous.get(info_hash).copied().unwrap_or(now);
            (info_hash.clone(), since)
        })
        .collect()
}

/// Which window ended a read that timed out with no seeder connected, with the
/// numbers and the advice the operator-facing texts need.
///
/// Peer discovery and the no-seeder piece wait are the only two waits that can
/// end such a read, and only the discovery one is operator-tunable — so both the
/// stderr hint and the `NoPeers` message must name the window that actually
/// elapsed, and must not recommend a setting that cannot change the outcome.
/// Pure so the mapping is unit-testable without a running engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NoSeederWait {
    /// The read spent its peer-discovery window on an empty swarm and never
    /// saw a seeder, so that window is what ended it.
    PeerDiscoveryWindow {
        /// Effective window: the `min` of the configured timeouts (see
        /// [`DiscoveryWindowAdvice`]).
        secs: u64,
        /// Which timeouts the window is a `min` of (only the configured ones).
        formula: &'static str,
        /// Which of those timeouts has to move to widen it — naming a
        /// non-binding one would not widen it.
        advice: &'static str,
    },
    /// The read ended in the no-seeder piece wait, which is
    /// `[timeouts] peer_discovery_wait_secs`-independent (it derives from
    /// `read_timeout_secs`, with `no_seeder_read_timeout_secs` capping it while
    /// no seeder is connected): either the discovery phase never ran out its
    /// window (a leecher-only swarm, or a peer that appeared during it), or a
    /// seeder connected only after the window elapsed and the piece wait
    /// restarted with the full `read_timeout_secs` window, so that longer wait
    /// is the one that elapsed.
    NoSeederPieceWait {
        /// The piece-wait window that elapsed.
        secs: u64,
        /// The discovery window the read did not run out, so both waits are
        /// visible in the message.
        discovery_window_secs: u64,
    },
}

/// How the effective peer-discovery window is composed, and which setting has
/// to move to widen it.
///
/// The window is a `min` of the configured timeouts, so a hint that names a
/// term above the minimum sends the operator to a knob that cannot change the
/// outcome — the criterion both texts in this family follow.  `formula` shows
/// the composition (listing only the terms actually configured), `advice` names
/// the term(s) currently binding it.
struct DiscoveryWindowAdvice {
    /// The window's `min` formula, e.g.
    /// `min(read_timeout_secs, peer_discovery_wait_secs)`.
    formula: &'static str,
    /// The timeout(s) that have to be raised for a retry to wait longer.
    advice: &'static str,
}

/// Which setting caps the effective peer-discovery window and therefore has to
/// move to widen it.
///
/// Every term at the window's minimum is named: the window is a `min`, so
/// raising only one of several tied terms leaves it exactly where it is.  Pure
/// so the mapping is unit-testable without a running engine.
fn discovery_window_advice(
    read_timeout_secs: u64,
    peer_discovery_wait_secs: u64,
    peer_wait_cap_secs: Option<u64>,
) -> DiscoveryWindowAdvice {
    let window = peer_discovery_window_secs(
        read_timeout_secs,
        peer_discovery_wait_secs,
        peer_wait_cap_secs,
    );
    // `window` is one of the terms below, so the final `else` is the
    // discovery-wait term: every other branch already claimed its own case.
    let read_timeout_binds = read_timeout_secs == window;
    let discovery_wait_binds = peer_discovery_wait_secs == window;
    let cap_binds = peer_wait_cap_secs == Some(window);
    let advice = if read_timeout_binds && discovery_wait_binds && cap_binds {
        "raise [timeouts] read_timeout_secs, peer_discovery_wait_secs and \
         peer_wait_cap_secs together (any one alone leaves the window \
         unchanged) and retry"
    } else if read_timeout_binds && discovery_wait_binds {
        "raise [timeouts] read_timeout_secs and peer_discovery_wait_secs \
         together (either alone leaves the window unchanged) and retry"
    } else if read_timeout_binds && cap_binds {
        "raise [timeouts] read_timeout_secs and peer_wait_cap_secs together \
         (either alone leaves the window unchanged) and retry"
    } else if discovery_wait_binds && cap_binds {
        "raise [timeouts] peer_discovery_wait_secs and peer_wait_cap_secs \
         together (either alone leaves the window unchanged) and retry"
    } else if read_timeout_binds {
        // The read timeout is the minimum: it caps the window, so raising the
        // discovery knob alone cannot widen it.
        "raise [timeouts] read_timeout_secs (it caps the window) and retry"
    } else if cap_binds {
        // The optional cap is the minimum: fail-fast is doing what it was set
        // to do, and only the cap can lift it.
        "raise [timeouts] peer_wait_cap_secs (it caps the window) and retry"
    } else {
        "raise [timeouts] peer_discovery_wait_secs and retry"
    };
    DiscoveryWindowAdvice {
        formula: if peer_wait_cap_secs.is_some() {
            "min(read_timeout_secs, peer_discovery_wait_secs, peer_wait_cap_secs)"
        } else {
            "min(read_timeout_secs, peer_discovery_wait_secs)"
        },
        advice,
    }
}

/// Which of the two waits ended this read, carrying the advice for it.
///
/// The discovery window ended the read only when the read exhausted that window
/// on an empty swarm *and* no seeder ever connected: that leaves the piece wait
/// at zero ([`NO_SEEDER_FAST_FAIL_SECS`]), so the discovery window is the only
/// wait the read spent.  A seeder that connects after the window elapsed does
/// not rescue the read — it restarts the piece wait at the full
/// `read_timeout_secs` — but it does end the discovery window's claim on the
/// outcome, so `has_seeder` must veto the discovery attribution: that read is
/// ended by the piece wait it went on to spend, and no `[timeouts]` value
/// widens it.  Pure so the mapping is unit-testable without a running engine.
fn no_seeder_wait(
    is_peer_wait_exhausted: bool,
    has_seeder: bool,
    discovery_window_secs: u64,
    piece_wait_secs: u64,
    discovery: DiscoveryWindowAdvice,
) -> NoSeederWait {
    if is_peer_wait_exhausted && !has_seeder {
        NoSeederWait::PeerDiscoveryWindow {
            secs: discovery_window_secs,
            formula: discovery.formula,
            advice: discovery.advice,
        }
    } else {
        NoSeederWait::NoSeederPieceWait {
            secs: piece_wait_secs,
            discovery_window_secs,
        }
    }
}

/// Advance the reader-demand window from the accessor counter.
///
/// A counter change opens a fresh [`READER_DEMAND_TTL`] window; an unchanged
/// counter leaves it where it is, so a reader that stops lets it lapse.
/// Returns the counter value to remember and the window end.  Pure so the
/// window semantics are unit-testable without a running engine.
fn advance_reader_demand(
    observed: u64,
    seen: u64,
    until: Option<Instant>,
    now: Instant,
) -> (u64, Option<Instant>) {
    if observed == seen {
        (seen, until)
    } else {
        (observed, Some(now + READER_DEMAND_TTL))
    }
}

/// Whether a reader-demand window is still open at `now`.  Pure so the
/// expiry boundary is unit-testable without a running engine.
fn reader_demand_is_live(until: Option<Instant>, now: Instant) -> bool {
    until.is_some_and(|deadline| now < deadline)
}

/// Whether a command should trigger a snapshot publish.
///
/// A mutating command (handle add/remove, tracker merge) changes what `.stats`
/// reports, so it publishes immediately.  A read command changes nothing
/// observable: it refreshes the rate-limited snapshot only for a watching
/// reader, which is what keeps that reader's status fresh during a read burst.
/// Pure so the gating is unit-testable without a running engine.
fn should_publish_after_command(
    is_read: bool,
    is_watched: bool,
    since_last_publish: Duration,
) -> bool {
    !is_read || (is_watched && since_last_publish >= SNAPSHOT_INTERVAL)
}

/// Age in whole seconds of every clock in `since`, sampled at `now`.
fn elapsed_secs_since(since: &HashMap<String, Instant>, now: Instant) -> HashMap<String, u64> {
    since
        .iter()
        .map(|(info_hash, since)| {
            (
                info_hash.clone(),
                now.saturating_duration_since(*since).as_secs(),
            )
        })
        .collect()
}

/// Aggregate the engine's parked reads into the per-info_hash `.stats` wait
/// state.
///
/// `waits` holds one `(info_hash, waiting_since)` entry per parked read, so one
/// pass yields both facts `.stats` shows about a torrent's waiting: how long its
/// *oldest* read has waited — a second reader joining a torrent already waiting
/// for a minute must not reset the report to zero — and how many reads are
/// parked.  An info_hash with no parked read gets no entry, which is how the
/// renderer tells "nobody waiting" from "waited zero seconds".  Pure so the
/// aggregation rule is unit-testable without a running engine.
fn aggregate_waiting_reads<'a>(
    waits: impl Iterator<Item = (&'a str, Instant)>,
    now: Instant,
) -> HashMap<String, WaitingReads> {
    let mut waiting_reads: HashMap<String, WaitingReads> = HashMap::new();
    for (info_hash, waiting_since) in waits {
        let waited = now.saturating_duration_since(waiting_since).as_secs();
        waiting_reads
            .entry(info_hash.to_string())
            .and_modify(|waits| {
                waits.oldest_secs = waits.oldest_secs.max(waited);
                waits.count += 1;
            })
            .or_insert(WaitingReads {
                oldest_secs: waited,
                count: 1,
            });
    }
    waiting_reads
}

/// Format the stderr hint emitted when a read times out with zero connected
/// seeders.  The message describes the *current* swarm state at
/// timeout — a seeder that connected and left during the wait also lands here,
/// so it says "no seeder connected", not "no seeder ever connected".  The
/// daemon writes the line to its own stderr (operator-facing; a FUSE daemon
/// has no channel into the reading client's stderr), letting the operator
/// tell "no seeder" apart from "seeder slow" (`DownloadTimeout`) and from a
/// cache stall ([`cache_stall_stderr_hint`]).
///
/// [`NoSeederWait`] carries which window actually ended the read and, for the
/// discovery one, which timeout has to move to widen it: pointing a read whose
/// discovery window was not what ended it — a leecher-only swarm, a peer that
/// arrived mid-discovery, or a seeder that connected only after the window
/// elapsed and then left — at `[timeouts] peer_discovery_wait_secs` would send
/// the operator to a knob that cannot change the outcome.  Pure so the exact
/// message is unit-testable without a running engine.
pub(crate) fn no_seeder_stderr_hint(num_peers: i32, num_seeds: i32, wait: NoSeederWait) -> String {
    let counts = format!("no seeder connected (Peers:{num_peers} Seeds:{num_seeds})");
    match wait {
        NoSeederWait::PeerDiscoveryWindow { secs, advice, .. } => {
            format!("{counts} within the {secs}s peer-discovery window; {advice}")
        }
        NoSeederWait::NoSeederPieceWait { secs, .. } => format!(
            "{counts} after the {secs}s no-seeder piece wait; no seeder is \
             present in the swarm"
        ),
    }
}

/// Format the `NoPeers` error returned when a piece-wait expires with zero
/// connected seeders and no cache-side evidence.  A read waiting on data the
/// on-disk cache no longer holds is a cache stall instead (see
/// [`cache_stall_message`]): telling the operator to check the tracker for a
/// `cache_size` problem is exactly the misattribution this split avoids.
///
/// The advice follows [`no_seeder_stderr_hint`]'s criterion — never recommend a
/// setting that cannot change this read's outcome.  The discovery branch states
/// the effective window's formula (`min` of the configured timeouts) and names
/// the one that caps it; the piece-wait branch states the bounds its window
/// derives from (`read_timeout_secs`, capped by the no-seeder setting while no
/// seeder is connected) and that `[timeouts] peer_discovery_wait_secs` does not
/// size it, so retrying with a larger discovery window would change nothing
/// while the swarm still has a peer.  Pure so the exact message is
/// unit-testable without a running engine.
pub(crate) fn no_peers_message(info_hash: &str, peer_wait_secs: u64, wait: NoSeederWait) -> String {
    match wait {
        NoSeederWait::PeerDiscoveryWindow {
            secs,
            formula,
            advice,
        } => format!(
            "No seeder connected for info_hash {info_hash} after {peer_wait_secs}s \
             of peer discovery (window {secs}s = {formula}). The torrent has no \
             available seeder — check tracker health, or {advice}."
        ),
        NoSeederWait::NoSeederPieceWait {
            secs,
            discovery_window_secs,
        } => format!(
            "No seeder connected for info_hash {info_hash} after {peer_wait_secs}s \
             of peer discovery (window {discovery_window_secs}s) + {secs}s \
             no-seeder piece wait. That wait derives from read_timeout_secs \
             (capped by [timeouts] no_seeder_read_timeout_secs while no seeder \
             is connected), not from [timeouts] peer_discovery_wait_secs — \
             check tracker health, or retry once a seeder joins."
        ),
    }
}

/// What a read's piece-wait window actually ran out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReadStallCause {
    /// No seeder is connected and no cache-side evidence explains the stall,
    /// so nothing in the swarm can serve the read.
    NoSeeder,
    /// The read is waiting on data the on-disk cache does not hold: the piece
    /// it waits on was there and is gone, or its range cannot fit the cache at
    /// all.  Either way the read cannot be served from the cache and depends
    /// on a re-download.
    CacheStall,
    /// A seeder is connected and the cache holds the read, but the pieces did
    /// not arrive inside the window.
    SlowSwarm,
}

/// Classify a read whose piece-wait window elapsed.
///
/// `stale_blockers` holds every piece this read found stale — libtorrent's bit
/// set but no usable on-disk data, i.e. the cache no longer holds data it once
/// held.  It counts only when it still contains the piece the read is blocked
/// on, so a stale observation for an already-served piece cannot colour a later
/// swarm stall; it must hold *all* of them, because a `force_recheck` clears the
/// bits of every missing piece at once, leaving no way to re-detect a piece
/// whose bit was already cleared by the time the read reaches it.  A read range
/// larger than the whole cache is the other cache-side evidence: its head
/// pieces are evicted by its tail's download before the read captures them, and
/// that is only reported with a seeder present (otherwise "no seeder" is the
/// cause the operator must act on first).  Pure so the classification is
/// unit-testable without a running engine.
pub(crate) fn classify_read_stall(
    num_seeds: i32,
    stale_blockers: &[i32],
    blocking_piece: i32,
    read_span_bytes: u64,
    cache_capacity_bytes: u64,
) -> ReadStallCause {
    if stale_blockers.contains(&blocking_piece) {
        ReadStallCause::CacheStall
    } else if num_seeds == 0 {
        ReadStallCause::NoSeeder
    } else if read_span_bytes > cache_capacity_bytes {
        ReadStallCause::CacheStall
    } else {
        ReadStallCause::SlowSwarm
    }
}

/// Byte count as MiB, so a read-failure message carries numbers the operator
/// can compare against `[cache] cache_size` (and the file size) directly.
fn format_mib(bytes: u64) -> String {
    format!("{:.2} MiB", bytes as f64 / (1024.0 * 1024.0))
}

/// Format the stderr hint for a read stalled on the on-disk cache, mirroring
/// [`no_seeder_stderr_hint`] so the operator can grep the two causes apart.
/// `blocking_piece_stale` selects the evidence: the piece the read waits on is
/// gone from the cache, vs. a read range larger than the whole cache.  Pure so
/// the exact message is unit-testable without a running engine.
pub(crate) fn cache_stall_stderr_hint(
    read_span_bytes: u64,
    cache_capacity_bytes: u64,
    blocking_piece_stale: bool,
) -> String {
    let evidence = if blocking_piece_stale {
        "piece the read waits on is gone from cache"
    } else {
        "read span exceeds cache"
    };
    format!(
        "read stalled on the on-disk cache \
         (cache_size={}, read span={}, {evidence}); raise [cache] cache_size if \
         the cache is evicting data the read needs",
        format_mib(cache_capacity_bytes),
        format_mib(read_span_bytes)
    )
}

/// Format the `Timeout` error for a read stalled on the on-disk cache rather
/// than on the swarm.  It states the evidence without claiming *why* the data
/// left the cache (eviction, a failed check, or removal outside the cache are
/// indistinguishable here), gives both numbers (`cache_size` against the read
/// span), both actions (size the cache to the file; the re-download needs a
/// reachable seeder), and the swarm state — the bare `ENODATA` the FUSE layer
/// returns cannot tell this apart from "no seeder" ([`no_peers_message`]) or
/// "seeder slow" (the plain `Timeout` message).  Pure so the exact message is
/// unit-testable without a running engine.
pub(crate) fn cache_stall_message(
    info_hash: &str,
    piece_wait_secs: u64,
    cache_capacity_bytes: u64,
    read_span_bytes: u64,
    blocking_piece_stale: bool,
    num_seeds: i32,
) -> String {
    let cause = if blocking_piece_stale {
        "the piece this read waits on was in the on-disk cache and is no \
         longer there (evicted, purged after a failed check, or removed \
         outside the cache), so it has to be re-downloaded"
    } else {
        "this read's range is larger than the whole on-disk cache, so its head \
         pieces are evicted by its tail's download before the read captures \
         them"
    };
    let action = if blocking_piece_stale {
        "raise [cache] cache_size to at least the size of the file being read \
         if the cache is evicting data the read still needs"
    } else {
        "raise [cache] cache_size to at least the size of the file being read \
         so the read's pieces stay resident"
    };
    let swarm = if num_seeds > 0 {
        "A seeder is connected, so the re-download itself is not blocked on \
         the swarm"
    } else {
        "No seeder is connected, so the re-download cannot start until one \
         connects"
    };
    format!(
        "Read timed out for info_hash {info_hash} after {piece_wait_secs:.0}s \
         piece wait: {cause}. cache_size = {capacity}, this read spans {span} — \
         {action}. {swarm}.",
        capacity = format_mib(cache_capacity_bytes),
        span = format_mib(read_span_bytes),
    )
}

/// Compute the partial-read bounds when the piece-wait window elapses. The
/// loop advances `piece_idx` in order, so on timeout every piece before it is
/// complete; return that contiguous prefix `[start_piece, last_complete]` as a
/// short read (visible progress) instead of 0 bytes the client can't tell from
/// EOF. Returns `(partial_end, partial_size)` covering `[absolute_offset,
/// partial_end)`, or `None` when the first requested piece is missing.
/// `partial_end` is the min of the request end and the last complete piece's
/// boundary (on the timeout path always a full piece boundary).
fn partial_read_bounds(
    start_piece: i32,
    last_complete: i32,
    piece_length: u64,
    absolute_offset: u64,
    end_offset: u64,
) -> Option<(u64, u32)> {
    if last_complete < start_piece {
        return None;
    }
    let partial_end = std::cmp::min(end_offset, (last_complete as u64 + 1) * piece_length);
    if partial_end <= absolute_offset {
        return None;
    }
    Some((partial_end, (partial_end - absolute_offset) as u32))
}

/// Snapshot refresh interval. Alerts are drained by a dedicated consumer
/// thread (`set_alert_notify`), so this interval bounds `.stats` staleness
/// for per-torrent status/pieces, and it paces the session-stats sample
/// request (`post_session_stats`, drained by the consumer into the shared
/// stats snapshot).  Both are published only while a `.stats` reader is
/// watching — see [`READER_DEMAND_TTL`].
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

/// How long a `.stats` read keeps the periodic snapshot publish and
/// session-stats sample alive (`.stats` is their only consumer).  Each new
/// read reopens the window, so staleness is bounded by the reader's own
/// polling interval: a reader polling no slower than this sees the publish
/// cadence unchanged, a slower one sees the snapshot from the end of its
/// previous window, and a one-off read after an idle spell sees the previous
/// publish — the read right after it is fresh.
const READER_DEMAND_TTL: Duration = Duration::from_secs(2);

/// Poll cadence for parked reads while at least one is waiting on the swarm.
/// Matches the old inline piece-wait poll so a parked read observes a newly
/// available piece or a connecting seeder at the same granularity it used to
/// while it held the engine thread.
const PENDING_READ_POLL_INTERVAL: Duration = Duration::from_millis(200);

/// Phase of a parked read's wait machine.  Each engine-loop iteration advances
/// every parked read by one step, so no phase ever sleeps on the engine
/// thread.
enum ReadPhase {
    /// The torrent is still checking/allocating; wait for it to settle.
    WaitState,
    /// Stale libtorrent piece bits were detected; a recheck is in flight.
    Recheck,
    /// The swarm looks empty; probe for a peer or seeder to connect.
    PeerWait,
    /// Piece deadlines are set; wait for the pieces to arrive.
    PieceWait,
}

/// One cold-start flight per info_hash: the shared peer-discovery window for
/// every read of that torrent that finds the swarm empty.
///
/// The first such read starts the flight (a single `force_reannounce` plus one
/// bounded window); concurrent reads of the same info_hash attach to it and
/// observe the same outcome, so the probe and the "no seeder" decision are per
/// torrent instead of per reader.  The flight retires when its window ends —
/// either because a peer/seeder finally connected or because the probe
/// exhausted on an empty swarm — so a later read starts a fresh probe and can
/// discover a seeder that came online after the previous probe.  Concurrent
/// dedup holds only within the window; recovery from a transiently-empty swarm
/// is unchanged from the pre-flight behaviour.
struct ColdFlight {
    /// When the window started — shared by every attached read.
    started: Instant,
    /// When the probe gives up.
    deadline: Instant,
    /// The mid-window re-announce already fired for this flight.
    reannounced: bool,
}

impl ColdFlight {
    fn new(now: Instant, window: Duration) -> Self {
        Self {
            started: now,
            deadline: now + window,
            reannounced: false,
        }
    }

    /// Whether the window has ended.  A reader attaching to an expired flight
    /// would fail on its first poll with no chance to probe again, so the
    /// caller retires it and starts a fresh one.
    fn is_expired(&self, now: Instant) -> bool {
        now >= self.deadline
    }

    /// Whether the single mid-window re-announce is due and not yet taken.
    /// Marks it taken, so concurrent attached reads cannot each re-announce.
    fn take_reannounce(&mut self, now: Instant) -> bool {
        if self.reannounced {
            return false;
        }
        let window = self.deadline.saturating_duration_since(self.started);
        if now.saturating_duration_since(self.started) < window / 2 {
            return false;
        }
        self.reannounced = true;
        true
    }
}

/// A finished probe's verdict that a swarm has neither a peer nor a seed, kept
/// past the end of the probe's window so the reads that follow it do not each
/// re-open a cold flight.
///
/// A whole-file `cat` reaches the engine as one read per FUSE chunk, and with
/// an empty swarm every chunk used to pay a full discovery window of its own:
/// three chunks spent three windows (3 × 12s measured) waiting for a seeder
/// none of those windows could produce, while the first window's verdict
/// already spoke for all of them.  The verdict is evidence with a shelf life:
/// it speaks only for a swarm that is still empty, so it is dropped the moment
/// a peer or seed connects (checked against a live status read before the
/// verdict is applied) and it lapses one window after the probe gave up — the
/// next read then probes again, which is what keeps a seeder that came online
/// after the probe reachable.
struct SourcelessSwarm {
    /// Instant the probe gave up: the verdict's epoch and its TTL anchor.
    at: Instant,
    /// The probe's discovery window, reused as the verdict's TTL and its
    /// re-announce cadence.
    window: Duration,
    /// How long the probe's discovery wait actually ran, so an inheriting read
    /// reports the same elapsed window a fresh probe would have reported.
    elapsed: Duration,
    /// Instant of the last announce this verdict drove.  It starts at the
    /// probe's end — the probe announced at its own half-window mark and then
    /// waited the rest out — so the first inherited read re-announces only a
    /// half-window later.
    last_announce: Instant,
}

impl SourcelessSwarm {
    fn new(at: Instant, window: Duration, elapsed: Duration) -> Self {
        Self {
            at,
            window,
            elapsed,
            last_announce: at,
        }
    }

    /// Whether the verdict still speaks for the swarm: less than one window has
    /// passed since the probe gave up.  At the boundary the verdict is spent —
    /// a read arriving there probes again instead of inheriting stale evidence.
    fn is_live(&self, now: Instant) -> bool {
        now.saturating_duration_since(self.at) < self.window
    }

    /// Whether an inheriting read owes the swarm a re-announce, marking it
    /// taken.  One announce per half-window keeps the probe's cadence alive —
    /// the inheriting reads do not wait, so nothing else would drive discovery
    /// — without letting a burst of chunk reads announce once per read.
    fn take_reannounce(&mut self, now: Instant) -> bool {
        if now.saturating_duration_since(self.last_announce) < self.window / 2 {
            return false;
        }
        self.last_announce = now;
        true
    }
}

/// A read that passed validation and is ready to be served or parked.
struct PreparedRead {
    info: Arc<TorrentInfo>,
    file_index: i32,
    offset: u64,
    size: u32,
    info_hash: String,
    piece_length: u64,
    num_pieces: i32,
    total_size: u64,
    absolute_offset: u64,
    end_offset: u64,
    start_piece: i32,
    end_piece: i32,
    status: TorrentStatus,
}

/// A read waiting on the swarm, parked on the engine's queue instead of
/// blocking the engine thread.  The caller is still blocked on `reply`; only
/// the engine thread is freed.
struct PendingRead {
    info: Arc<TorrentInfo>,
    file_index: i32,
    offset: u64,
    size: u32,
    info_hash: String,
    piece_length: u64,
    num_pieces: i32,
    total_size: u64,
    absolute_offset: u64,
    end_offset: u64,
    start_piece: i32,
    end_piece: i32,
    /// First piece not yet known to be available.
    current_piece: i32,
    /// Reply channel to the caller blocked in `DownloadEngine::read_file_range`.
    reply: SyncSender<TorrentResult<Vec<u8>>>,
    phase: ReadPhase,
    /// When the current phase started (drives the recheck grace check).
    phase_start: Instant,
    /// When the current phase gives up.
    phase_deadline: Instant,
    /// Last observed torrent status — the peer/seed counts drive the windows.
    status: TorrentStatus,
    /// Peer discovery ran its full window on an empty swarm, so the piece wait
    /// collapses to zero seconds.
    is_peer_wait_exhausted: bool,
    /// A seeder is connected, so the piece wait uses the full read timeout.
    has_seeder: bool,
    /// Start of the piece-wait window; resets when a seeder connects.
    piece_wait_start: Instant,
    /// Actual peer-discovery wait once the peer-wait phase completes, bounded
    /// by [`peer_discovery_window_secs`] (the `[timeouts]
    /// peer_discovery_wait_secs` window capped by `read_timeout_secs` and by
    /// the optional `peer_wait_cap_secs`), so the `NoPeers` message reports it
    /// even when the piece wait then fast-fails at zero seconds.
    peer_wait_elapsed: Duration,
    /// Every piece of this read's range whose data was found missing from the
    /// on-disk cache although libtorrent's bit for it was set — i.e. the cache
    /// no longer holds data it once held (evicted, purged after a failed
    /// check, or removed outside the cache), so the read must re-download it.
    /// *All* such pieces are kept, not just the first: a `force_recheck` clears
    /// the bits of every missing piece at once, so a piece the read has not
    /// reached yet can never be re-detected once its bit is cleared.  The
    /// evidence counts at timeout only for the piece the read is blocked on
    /// (see `classify_read_stall`); a stale observation for an already-served
    /// piece cannot colour a later swarm stall because the read only advances
    /// past a piece once it is usable again.
    stale_blockers: Vec<i32>,
    /// The recheck has been observed in a checking state (TOCTOU guard).
    saw_checking: bool,
    /// Id of the reader this read registered with the scheduler, once the
    /// priority gradient has been applied.  Released by id so a concurrent read
    /// on the same torrent never releases the wrong reader.
    reader_id: Option<ReadId>,
    /// When this read began waiting.  Never reset by a phase transition, so
    /// `.stats` reports one continuous wait per read instead of restarting the
    /// clock at every peer-wait → piece-wait switch.
    waiting_since: Instant,
}

impl PendingRead {
    fn new(prepared: PreparedRead, reply: SyncSender<TorrentResult<Vec<u8>>>) -> Self {
        let now = Instant::now();
        Self {
            info: prepared.info,
            file_index: prepared.file_index,
            offset: prepared.offset,
            size: prepared.size,
            info_hash: prepared.info_hash,
            piece_length: prepared.piece_length,
            num_pieces: prepared.num_pieces,
            total_size: prepared.total_size,
            absolute_offset: prepared.absolute_offset,
            end_offset: prepared.end_offset,
            start_piece: prepared.start_piece,
            end_piece: prepared.end_piece,
            current_piece: prepared.start_piece,
            reply,
            phase: ReadPhase::PieceWait,
            phase_start: now,
            phase_deadline: now,
            status: prepared.status,
            is_peer_wait_exhausted: false,
            has_seeder: false,
            piece_wait_start: now,
            peer_wait_elapsed: Duration::ZERO,
            stale_blockers: Vec::new(),
            saw_checking: false,
            reader_id: None,
            waiting_since: now,
        }
    }

    /// Record `piece_idx` as a piece of this read whose data the on-disk cache
    /// no longer holds.  Idempotent: the read-start scan and the piece-wait
    /// loop can both observe the same piece, and a recheck can leave it stale
    /// for several polls.
    fn record_stale_piece(&mut self, piece_idx: i32) {
        if !self.stale_blockers.contains(&piece_idx) {
            self.stale_blockers.push(piece_idx);
        }
    }
}

/// Whether a libtorrent state means the torrent is still verifying or
/// allocating its storage and cannot serve a read yet.
fn is_settling_state(state: TorrentState) -> bool {
    matches!(
        state,
        TorrentState::QueuedForChecking
            | TorrentState::CheckingFiles
            | TorrentState::Allocating
            | TorrentState::CheckingResumeData
    )
}

fn engine_loop(mut state: EngineState, rx: Receiver<Command>) {
    tracing::info!("Download engine started");
    // `publish_snapshot` rebuilds the per-torrent piece grid — O(num_pieces)
    // cache lookups plus a `post_torrent_updates` FFI round-trip — so running
    // it after *every* command made byte-granular reads (`dd bs=1 count=4096`)
    // pathological on large torrents.  Non-read commands (handle add/remove,
    // tracker merge) mutate observable state, so they still publish
    // immediately.  `ReadFileRange` is the high-frequency command: a cached
    // read mutates nothing, so publishing per read is pure waste — instead
    // refresh on a wall-clock threshold.  The threshold must not gate on the
    // timeout branch: `recv_timeout` returns immediately while commands are
    // queued, so an idle-only publish would freeze `.stats` (per-torrent
    // status / num_peers) for the whole duration of a sustained read burst
    // (`dd bs=1 count=N`).
    let mut last_publish = Instant::now();
    // Cache-metadata flush cadence.  Tracked separately from `last_publish`
    // because the parked-read poll shortens the loop's wait to
    // `PENDING_READ_POLL_INTERVAL`: gating housekeeping on the publish timer
    // would then starve the flush for the whole duration of a parked read.
    let mut last_housekeeping = Instant::now();
    // Session-stats sample cadence for a watching `.stats` reader.  Also
    // separate from `last_publish`: a burst of read commands keeps the loop
    // out of its timeout branch, and each one would otherwise fire its own
    // `post_session_stats`.
    let mut last_stats_sample = Instant::now();
    loop {
        // Poll parked reads on the short cadence so a waiting read is checked
        // at roughly the old inline piece-wait granularity; with none parked,
        // fall back to the snapshot interval.
        let wait = if state.pending_reads.is_empty() {
            SNAPSHOT_INTERVAL
        } else {
            PENDING_READ_POLL_INTERVAL
        };
        // Open a fresh window when a `.stats` accessor has fired since the
        // last look; `is_watched` then gates every periodic FFI call below.
        let is_watched = state.refresh_reader_demand();
        match rx.recv_timeout(wait) {
            Ok(cmd) => {
                let is_read = matches!(cmd, Command::ReadFileRange { .. });
                let stop = state.handle_command(cmd);
                state.drain_piece_finished();
                state.poll_pending_reads();
                let has_parked_read = !state.pending_reads.is_empty();
                if has_parked_read
                    || (is_watched && last_stats_sample.elapsed() >= SNAPSHOT_INTERVAL)
                {
                    state.refresh_session_stats();
                    last_stats_sample = Instant::now();
                }
                if should_publish_after_command(is_read, is_watched, last_publish.elapsed()) {
                    state.publish_snapshot();
                    last_publish = Instant::now();
                }
                // do NOT flush cache metadata per command.  Every
                // cached read marks the piece metadata dirty
                // (`record_access`), so a per-command flush fsync'd
                // `cache_metadata.txt` once per 1-byte read — the dominant
                // cost that made `dd bs=1 count=4096` hang on a cached file.
                // Flushing on the periodic tick (timeout branch) and on
                // shutdown is the "periodic flush" intent.
                if last_housekeeping.elapsed() >= SNAPSHOT_INTERVAL {
                    state.flush_cache_metadata();
                    last_housekeeping = Instant::now();
                }
                if stop {
                    break;
                }
            }
            Err(RecvTimeoutError::Timeout) => {
                let housekeeping = last_housekeeping.elapsed() >= SNAPSHOT_INTERVAL;
                if housekeeping {
                    state.flush_cache_metadata();
                    last_housekeeping = Instant::now();
                }
                state.drain_piece_finished();
                state.poll_pending_reads();
                let has_parked_read = !state.pending_reads.is_empty();
                if has_parked_read || (housekeeping && is_watched) {
                    state.refresh_session_stats();
                    last_stats_sample = Instant::now();
                }
                if is_watched && last_publish.elapsed() >= SNAPSHOT_INTERVAL {
                    state.publish_snapshot();
                    last_publish = Instant::now();
                }
            }
            Err(RecvTimeoutError::Disconnected) => break,
        }
    }
    // Resolve every parked read before the session goes away: their callers
    // are blocked on the reply channel and would otherwise see a bare
    // disconnect error instead of the shutdown abort.
    state.abort_pending_reads();
    // final flush so metadata mutations that happened since the
    // last tick are durable before the engine thread exits.
    state.flush_cache_metadata();
    // Unregister the alert-notify hook before the session is dropped.
    if let Some(mut consumer) = state.alert_consumer.take() {
        consumer.stop();
    }
    tracing::info!("Download engine stopped");
}

impl EngineState {
    /// request a fresh session-stats sample. Fire-and-forget —
    /// libtorrent answers with a `session_stats_alert` on the normal alert
    /// queue, which the alert-consumer thread drains into `shared_stats`.
    /// This is the sole producer for the `.stats` Global Rates counters.
    fn refresh_session_stats(&self) {
        self.session.post_stats();
    }

    /// Open a fresh reader-demand window if a `.stats` accessor has fired since
    /// the last look, and report whether one is live now.  `false` means no
    /// consumer for the snapshot: the periodic publish and session-stats
    /// sample are then skipped entirely.
    fn refresh_reader_demand(&mut self) -> bool {
        let observed = self.reader_demand.load(Ordering::Relaxed);
        let now = Instant::now();
        let (seen, until) = advance_reader_demand(
            observed,
            self.seen_reader_demand,
            self.reader_demand_until,
            now,
        );
        self.seen_reader_demand = seen;
        self.reader_demand_until = until;
        reader_demand_is_live(until, now)
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
                self.start_read(info, file_index, offset, size, reply);
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

        // Kick the tracker immediately instead of waiting for libtorrent's
        // scheduled first announce.  An idle (upload_mode) handle otherwise
        // announces only when libtorrent next ticks the tracker, so `.stats`
        // can report `Peers: 0 Seeds: 0` after a torrent is added until a
        // read drives the announce through the slow path.  Forcing it here
        // makes peer/seed counts observable right away, with no read.
        if !handle.force_reannounce() {
            tracing::debug!(
                "ensure_handle {}: force_reannounce rejected (non-fatal)",
                info_hash
            );
        }

        let (piece_length, num_pieces) = handle.get_torrent_info()?;
        self.scheduler
            .init_torrent(&info_hash, num_pieces as i32, piece_length)?;
        // Zero every piece priority in libtorrent.  A freshly added torrent
        // defaults all pieces to libtorrent's `default_priority` (4 = "want"),
        // so the moment the first read clears `upload_mode` the torrent would
        // request *every* piece — a full-file download for a large torrent the
        // instant any byte is read.  `init_torrent` above only records an
        // all-zero baseline in the scheduler; mirror it here so libtorrent
        // requests only the pieces `reader_added` actually elevates (the
        // selective, on-demand download the scheduler is designed to drive).
        // One bulk FFI call, not a per-piece loop: this runs on the single
        // engine command thread, so N round-trips would stall every other
        // torrent's command while a large torrent is added.
        if !handle.set_all_piece_priorities(0) {
            tracing::warn!(
                "ensure_handle {}: failed to zero piece priorities; the first \
                 read may re-request the whole file",
                info_hash
            );
        }
        // The fresh handle announces on its own schedule again, so a verdict
        // left by an earlier handle must not suppress its reads.
        self.sourceless.remove(&info_hash);
        // Record the private flag so that tracker merging can
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
    /// so the engine stops announcing/seeding a removed torrent
    /// and its handle/scheduler entries do not leak across add/remove cycles.
    fn remove_handle(&mut self, info_hash: &str) -> TorrentResult<()> {
        if let Some(handle) = self.handles.remove(info_hash) {
            self.session.remove_torrent(handle, false);
        }
        self.scheduler.remove_torrent(info_hash);
        self.private_torrents.remove(info_hash);
        self.cold_flights.remove(info_hash);
        self.sourceless.remove(info_hash);
        Ok(())
    }

    /// Merge trackers from a duplicate-info_hash torrent into the existing
    /// handle, with PT isolation guard. Called when `add_torrent` sees a
    /// duplicate info_hash: the new trackers are deduplicated and merged, then
    /// `force_reannounce` contacts them. **PT isolation**: if either torrent is
    /// `private` (BEP-27), the merge is skipped entirely — private announce URLs
    /// embed passkeys, and merging would leak them across swarms and
    /// cross-pollinate peers. Failures are non-fatal (warn-logged): the row and
    /// handle already exist.
    fn merge_trackers(&mut self, info: &TorrentInfo) {
        let info_hash = match info.info_hash() {
            Ok(h) => hex::encode(h),
            Err(e) => {
                warn!("merge_trackers: failed to get info_hash, skipping: {:?}", e);
                return;
            }
        };

        // PT isolation guard: if the incoming torrent is private,
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

    /// Begin a read, running only the non-blocking setup here.  A range whose
    /// pieces are already local is served inline; anything that has to wait on
    /// the swarm is parked on [`EngineState::pending_reads`] and polled by the
    /// engine loop, so the engine thread never blocks inside a read and one
    /// sourceless read cannot serialize healthy commands behind its wait
    /// window.
    fn start_read(
        &mut self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
        reply: SyncSender<TorrentResult<Vec<u8>>>,
    ) {
        let prepared = match self.prepare_read(info, file_index, offset, size) {
            Ok(Some(prepared)) => prepared,
            // An empty range (offset past EOF, or a zero-span slice) is a
            // legitimate empty read, not a download failure.
            Ok(None) => {
                let _ = reply.send(Ok(Vec::new()));
                return;
            }
            Err(e) => {
                let _ = reply.send(Err(e));
                return;
            }
        };

        let mut read = PendingRead::new(prepared, reply);
        if is_settling_state(read.status.state) {
            // Wait for the torrent to finish checking/allocating first: both
            // the stale-piece recheck and the local-pieces fast path need a
            // settled bitmask.
            read.phase = ReadPhase::WaitState;
            read.phase_start = Instant::now();
            self.pending_reads.push(read);
            return;
        }

        if let Some(result) = self.after_settling(&mut read) {
            let _ = read.reply.send(result);
            return;
        }
        self.pending_reads.push(read);
    }

    /// Validate a read and collect everything the wait machine needs.  Errors
    /// here mean the read never touched the swarm, so the caller is answered
    /// immediately.  `Ok(None)` is an empty read, also served without touching
    /// the swarm.
    fn prepare_read(
        &mut self,
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> TorrentResult<Option<PreparedRead>> {
        self.ensure_handle(&info)?;
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
            return Ok(None);
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
            return Ok(None);
        }

        Ok(Some(PreparedRead {
            info,
            file_index,
            offset,
            size,
            info_hash,
            piece_length,
            num_pieces,
            total_size,
            absolute_offset,
            end_offset,
            start_piece,
            end_piece,
            status,
        }))
    }

    /// Continue a read whose torrent has settled: recheck stale piece bits when
    /// any exist, otherwise serve from local pieces or park it in a swarm-wait
    /// phase.  Returns `Some(result)` when the read finishes here.
    fn after_settling(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        // Record every stale piece in the read's range as this read's cache
        // evidence: their data was in the cache and is gone, so the read has to
        // re-download them.  All of them, not just the first — the recheck
        // below clears every missing piece's bit at once, so a piece the read
        // has not reached yet could never be re-detected afterwards.  The
        // evidence only counts at timeout for the piece the read is blocked on
        // (see `classify_read_stall`).
        let stale_pieces = self.stale_pieces_in_range(
            &read.info_hash,
            read.start_piece,
            read.end_piece,
            read.piece_length,
            read.num_pieces,
            read.total_size,
        );
        if !stale_pieces.is_empty() {
            for piece_idx in stale_pieces {
                read.record_stale_piece(piece_idx);
            }
            tracing::info!(
                "read_file_range: stale libtorrent piece state detected for \
                 info_hash={}, forcing recheck to clear bits for pieces {}-{}",
                read.info_hash,
                read.start_piece,
                read.end_piece
            );
            if !self.start_recheck(read) {
                return self.begin_waiting(read);
            }
            return None;
        }
        self.begin_waiting(read)
    }

    /// Start a `force_recheck` to clear stale `have_piece` bits (set but the
    /// piece file is missing or truncated) and park the read in the recheck
    /// phase.  Returns false when the recheck could not be started.
    fn start_recheck(&mut self, read: &mut PendingRead) -> bool {
        let recheck_started = self
            .handles
            .get(&read.info_hash)
            .map(|h| h.force_recheck())
            .unwrap_or(false);
        if !recheck_started {
            tracing::warn!(
                "force_recheck failed for info_hash={}; stale bits may persist",
                read.info_hash
            );
            return false;
        }
        read.phase = ReadPhase::Recheck;
        read.phase_start = Instant::now();
        read.phase_deadline = read.phase_start
            + Duration::from_secs(std::cmp::min(self.read_timeout_secs, RECHECK_WAIT_CAP_SECS));
        read.saw_checking = false;
        true
    }

    /// Serve a read from local pieces when possible, or against a recent
    /// probe's sourceless verdict when the swarm is still empty; otherwise
    /// apply the reader priority gradient and park it in a swarm-wait phase.
    /// Returns `Some(result)` when the read finished here.
    ///
    /// The verdict is applied before the gradient is registered: an inheriting
    /// read fails fast, and `reader_added` scales with torrent size and exists
    /// only to drive a download it would never start.
    fn begin_waiting(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        // A fully-cached read must not run the download machinery:
        // `reader_added`/`publish_snapshot`/`release_reader` scale with torrent
        // size and exist only to drive a download.  For a cached range their
        // per-read FFI overhead made byte-granular reads (`dd bs=1`) slow, so
        // read straight from the piece store.
        if self.all_pieces_local(
            &read.info_hash,
            read.start_piece,
            read.end_piece,
            read.piece_length,
            read.num_pieces,
            read.total_size,
        ) {
            return Some(self.read_from_disk(
                &read.info_hash,
                read.start_piece,
                read.end_piece,
                read.piece_length,
                read.num_pieces,
                read.total_size,
                read.absolute_offset,
                read.end_offset,
                read.size,
            ));
        }

        // A recent probe's verdict (see [`SourcelessSwarm`]) stands in for this
        // read's own discovery window: the read inherits the probe's outcome
        // instead of re-spending the window, which is what keeps a whole-file
        // `cat` — one read per FUSE chunk — from paying a window per chunk.
        if self.sourceless.contains_key(&read.info_hash) {
            // Only a live status read can tell whether the verdict still speaks
            // for the swarm: the read may have sat in the settle/recheck phases
            // while a peer or seed connected, which spends the verdict.
            if let Some(status) = self
                .handles
                .get(&read.info_hash)
                .and_then(|h| h.status().ok())
            {
                read.status = status;
            }
            let now = Instant::now();
            let inherited = match self.sourceless.get_mut(&read.info_hash) {
                Some(verdict)
                    if swarm_is_empty(read.status.num_peers, read.status.num_seeds)
                        && verdict.is_live(now) =>
                {
                    Some((verdict.elapsed, verdict.take_reannounce(now)))
                }
                _ => None,
            };
            if let Some((elapsed, is_reannounce_due)) = inherited {
                // This read does not wait, so nothing else drives discovery
                // while the verdict suppresses it: keep the probe's own
                // announce cadence, which is what lets a seeder that joined
                // after the probe be found by a later read.
                if is_reannounce_due {
                    if let Some(handle) = self.handles.get(&read.info_hash) {
                        if !handle.force_reannounce() {
                            tracing::debug!(
                                "read_file_range {}: force_reannounce rejected (non-fatal)",
                                read.info_hash
                            );
                        }
                    }
                }
                // The verdict stands in for the discovery window, not for the
                // data: pieces this read already holds are still served, as the
                // short read `read_timed_out` builds from the prefix.  The
                // range-wide check above is false as soon as one piece of the
                // range is missing, so advance `current_piece` past the locally
                // available prefix first — otherwise a read that spans a cached
                // prefix and missing pieces reports zero bytes for data it has.
                while read.current_piece <= read.end_piece
                    && self.piece_is_local(
                        &read.info_hash,
                        read.current_piece,
                        read.piece_length,
                        read.num_pieces,
                        read.total_size,
                    )
                {
                    read.current_piece += 1;
                }
                read.is_peer_wait_exhausted = true;
                read.peer_wait_elapsed = elapsed;
                return Some(self.read_timed_out(read, read.current_piece, Duration::ZERO));
            }
            // Spent — a peer or seed is present, or the verdict aged out — so
            // this read runs its own discovery or download from here.  The
            // status refresh above already routed it (see the swarm check
            // below).
            self.sourceless.remove(&read.info_hash);
        }

        // ReaderAdded: elevate priority for this read.  Publish immediately so
        // `.stats` reflects the elevated priorities while the read is parked —
        // the old inline path published here because the engine blocked until
        // `release_reader` reset them, leaving `.stats` permanently all-`[]`.
        // The returned id is what releases *this* reader: a concurrent read on
        // the same torrent holds its own, so neither can release the other's.
        let reader_id = {
            let handle = match self.handles.get(&read.info_hash) {
                Some(h) => h,
                None => return Some(Err(Self::missing())),
            };
            match self.scheduler.reader_added(
                handle,
                &read.info,
                read.file_index,
                read.offset,
                read.size,
                &self.store,
            ) {
                Ok(id) => Some(id),
                Err(e) => {
                    tracing::warn!("read_file_range: reader_added failed: {:?}", e);
                    None
                }
            }
        };
        read.reader_id = reader_id;
        self.publish_snapshot();

        // Switch to download mode: the gradient above is already applied, so
        // clearing upload_mode lets libtorrent start requesting those pieces
        // from the peers it is already connected to.  This is the only clear —
        // the flag is never re-armed, see `UPLOAD_MODE_FLAG`.
        {
            let handle = match self.handles.get(&read.info_hash) {
                Some(h) => h,
                None => return Some(Err(Self::missing())),
            };
            if !handle.unset_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "read_file_range: failed to clear upload_mode for {}",
                    read.info_hash
                );
            }
        }
        self.publish_snapshot();

        // With zero connected peers/seeds, kick the swarm immediately: after a
        // delete + re-add the fresh handle's first announce can land outside
        // the tracker's min-interval, timing out an otherwise-healthy read.
        // (A live verdict would have ended this read above, so it probes.)
        if swarm_is_empty(read.status.num_peers, read.status.num_seeds) {
            self.enter_peer_wait(read);
        } else {
            self.enter_piece_wait(read);
        }
        None
    }

    /// Effective peer-discovery window for this engine:
    /// [`peer_discovery_window_secs`] over the waits and cap this engine was
    /// built with.
    fn peer_discovery_window_secs(&self) -> u64 {
        peer_discovery_window_secs(
            self.read_timeout_secs,
            self.peer_discovery_wait_secs,
            self.peer_wait_cap_secs,
        )
    }

    /// Park a read in the peer-discovery phase: the swarm looked empty, so
    /// attach it to the info_hash's shared cold flight (creating the flight and
    /// kicking one re-announce when it is the first reader of this cold
    /// period) and give peers a bounded window to appear.
    fn enter_peer_wait(&mut self, read: &mut PendingRead) {
        let window_secs = self.peer_discovery_window_secs();
        let window = Duration::from_secs(window_secs);
        // A flight whose window already elapsed is stale — attaching to it
        // would fail this read on its first poll, and only libtorrent's own
        // announce schedule could ever refresh the swarm.  Retire it so this
        // read probes again (a fresh `force_reannounce` + a fresh window); the
        // window's own expiry retires flights on the normal path, so this
        // guards a flight left behind by any other exit.
        if self
            .cold_flights
            .get(&read.info_hash)
            .map(|flight| flight.is_expired(Instant::now()))
            .unwrap_or(false)
        {
            self.cold_flights.remove(&read.info_hash);
        }
        if !self.cold_flights.contains_key(&read.info_hash) {
            // One line per cold flight (not per read, not per piece): the
            // wait itself is otherwise silent for its whole window, which reads
            // as a hang in the daemon log.
            tracing::info!(
                "read_file_range {}: swarm empty — waiting up to {}s for a \
                 peer to appear (peer discovery)",
                read.info_hash,
                window_secs
            );
            // First cold reader of this info_hash: kick the swarm once.  A
            // concurrent reader attaching below must not re-announce — the
            // flight already covers discovery for the whole torrent.
            if let Some(handle) = self.handles.get(&read.info_hash) {
                if !handle.force_reannounce() {
                    tracing::debug!(
                        "read_file_range {}: force_reannounce rejected (non-fatal)",
                        read.info_hash
                    );
                }
            }
            self.cold_flights.insert(
                read.info_hash.clone(),
                ColdFlight::new(Instant::now(), window),
            );
        }
        let flight = self
            .cold_flights
            .get(&read.info_hash)
            .expect("cold flight was just created");
        read.phase = ReadPhase::PeerWait;
        read.phase_start = flight.started;
        read.phase_deadline = flight.deadline;
    }

    /// Park a read in the piece-wait phase: set piece deadlines for every piece
    /// not truly available, then wait for them to arrive.
    fn enter_piece_wait(&mut self, read: &mut PendingRead) {
        // `have_piece` can be stale (true but file purged or truncated).  Only
        // skip the deadline for pieces that are truly available — `have_piece`
        // true AND the file on disk reaches the expected size.  Stale pieces
        // still need a deadline so libtorrent re-requests them.
        if let Some(handle) = self.handles.get(&read.info_hash) {
            for piece_idx in read.start_piece..=read.end_piece {
                let piece_key = PieceStore::piece_key(&read.info_hash, piece_idx);
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    read.piece_length,
                    read.num_pieces,
                    read.total_size,
                );
                let truly_available = handle.have_piece(piece_idx)
                    && !self.store.has_stale_piece(&piece_key, expected);
                if !truly_available {
                    handle.set_piece_deadline(piece_idx, 0);
                }
            }
        }
        read.has_seeder = read.status.num_seeds > 0;
        read.piece_wait_start = Instant::now();
        read.phase = ReadPhase::PieceWait;
    }

    /// Poll one parked read by a single step.  Returns `Some(result)` when the
    /// read terminates here (success, short read, or error); `None` keeps it
    /// parked for the next iteration.
    fn poll_read(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        if !self.handles.contains_key(&read.info_hash) {
            return Some(Err(Self::missing()));
        }
        match read.phase {
            ReadPhase::WaitState => self.poll_wait_state(read),
            ReadPhase::Recheck => self.poll_recheck(read),
            ReadPhase::PeerWait => self.poll_peer_wait(read),
            ReadPhase::PieceWait => self.poll_piece_wait(read),
        }
    }

    /// Poll a read waiting for the torrent to leave a checking/allocating
    /// state.
    fn poll_wait_state(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        if self.stopping.load(Ordering::Relaxed) {
            return Some(Err(TorrentError::Timeout(
                "shutdown requested, read aborted".to_string(),
            )));
        }
        let max_wait_secs = self.read_timeout_secs;
        if read.phase_start.elapsed().as_secs() > max_wait_secs {
            return Some(Err(TorrentError::Timeout(format!(
                "Torrent stuck in state {:?} for {} seconds",
                read.status.state, max_wait_secs
            ))));
        }
        let status = self.handles.get(&read.info_hash).map(|h| h.status());
        match status {
            Some(Ok(s)) => {
                let is_settling = is_settling_state(s.state);
                read.status = s;
                if is_settling {
                    None
                } else {
                    self.after_settling(read)
                }
            }
            // A failed status read keeps the last known state, as the inline
            // wait loop did.
            _ => None,
        }
    }

    /// Poll a read waiting for a `force_recheck` to finish.  Falls through to
    /// the normal slow path once the recheck completes (or its cap elapses) —
    /// the recheck only clears stale bits; the read still has to fetch the
    /// piece.
    fn poll_recheck(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        if self.stopping.load(Ordering::Relaxed) {
            return Some(Err(TorrentError::Timeout(
                "shutdown requested, read aborted".to_string(),
            )));
        }
        if Instant::now() >= read.phase_deadline {
            tracing::warn!(
                "force_recheck did not finish within {:?} for info_hash={}",
                read.phase_deadline
                    .saturating_duration_since(read.phase_start),
                read.info_hash
            );
            return self.finish_recheck(read);
        }
        let status = self.handles.get(&read.info_hash).map(|h| h.status());
        match status {
            Some(Ok(s)) => {
                if is_settling_state(s.state) {
                    read.saw_checking = true;
                    None
                } else if read.saw_checking {
                    // We observed the checking state and it has now ended — the
                    // recheck is truly complete.
                    self.finish_recheck(read)
                } else if self
                    .stale_pieces_in_range(
                        &read.info_hash,
                        read.start_piece,
                        read.end_piece,
                        read.piece_length,
                        read.num_pieces,
                        read.total_size,
                    )
                    .is_empty()
                {
                    // The recheck cleared the stale bits even though the poll
                    // never saw a settling state (a small torrent rechecks
                    // faster than the poll).  This is the condition the recheck
                    // actually exists for, so it avoids the fixed 2s fallback.
                    self.finish_recheck(read)
                } else if read.phase_start.elapsed() > Duration::from_secs(2) {
                    // No checking state observed after 2s — the recheck may
                    // have completed instantly (unlikely) or failed to start.
                    // Fall through; the safety nets in `all_pieces_local` and
                    // the piece wait catch any remaining stale bits.
                    tracing::warn!(
                        "force_recheck: no checking state observed for \
                         info_hash={} after 2s, proceeding",
                        read.info_hash
                    );
                    self.finish_recheck(read)
                } else {
                    None
                }
            }
            // `status()` failed — proceed, as the inline wait did.
            _ => self.finish_recheck(read),
        }
    }

    /// Transition a read out of the recheck phase: mark the torrent's piece
    /// priorities dirty (the recheck reset libtorrent's priorities out-of-band,
    /// so the next `recompute` must rewrite them once) and fall through to the
    /// normal download path.
    fn finish_recheck(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        self.scheduler.mark_priorities_dirty(&read.info_hash);
        if read.reader_id.is_some() {
            // The read registered its reader before the recheck (a stale piece
            // surfaced mid-wait).  Recompute — the dirty flag forces a full
            // rewrite, re-elevating the cleared pieces — and re-enter the
            // piece wait without double-adding a reader.
            if let Some(handle) = self.handles.get(&read.info_hash) {
                self.scheduler
                    .recompute(handle, &read.info_hash, &self.store);
            }
            self.enter_piece_wait(read);
            return None;
        }
        self.begin_waiting(read)
    }

    /// Poll a read probing an empty-looking swarm for a peer or seeder.
    fn poll_peer_wait(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        if self.stopping.load(Ordering::Relaxed) {
            return Some(Err(TorrentError::Timeout(
                "shutdown requested, read aborted".to_string(),
            )));
        }

        if Instant::now() >= read.phase_deadline {
            // Final status refresh before declaring the swarm empty: a peer or
            // seeder that connected during the last poll window must not be
            // treated as absent.  A late leecher (`num_peers > 0, num_seeds ==
            // 0`) still gets the regular no-seeder piece-wait window instead of
            // a zero-second `NoPeers`.
            match self.handles.get(&read.info_hash).map(|h| h.status()) {
                Some(Ok(s)) => {
                    read.is_peer_wait_exhausted = swarm_is_empty(s.num_peers, s.num_seeds);
                    read.status = s;
                    self.publish_snapshot();
                }
                Some(Err(e)) => return Some(Err(e)),
                None => return Some(Err(Self::missing())),
            }
            // The window elapsed: retire the flight so the next read of this
            // info_hash starts a fresh one — a new `force_reannounce` and a new
            // window.  A seeder that came online after the probe is then
            // discoverable by a retry; retaining the expired flight instead
            // would fail every later read instantly and leave discovery to
            // libtorrent's own (possibly minutes-long) announce schedule.
            self.cold_flights.remove(&read.info_hash);
            let now = Instant::now();
            read.peer_wait_elapsed = now.saturating_duration_since(read.phase_start);
            if read.is_peer_wait_exhausted {
                // Record the probe's verdict for the reads that follow it: with
                // the swarm still empty they inherit it instead of each
                // re-opening a cold flight and re-spending this window (see
                // [`SourcelessSwarm`]).
                self.sourceless.insert(
                    read.info_hash.clone(),
                    SourcelessSwarm::new(
                        now,
                        Duration::from_secs(self.peer_discovery_window_secs()),
                        read.peer_wait_elapsed,
                    ),
                );
            } else {
                // A peer or seed arrived inside the window, so no verdict.
                self.sourceless.remove(&read.info_hash);
            }
            self.enter_piece_wait(read);
            return None;
        }

        // Keep the global `Connected:` counter fresh while the read is parked.
        self.refresh_session_stats();
        let status = self.handles.get(&read.info_hash).map(|h| h.status());
        match status {
            Some(Ok(s)) => {
                let has_swarm = s.num_peers > 0 || s.num_seeds > 0;
                read.status = s;
                // Refresh the shared snapshot so `.stats` shows peers/seeds as
                // they connect during the peer-wait phase.
                self.publish_snapshot();
                if has_swarm {
                    self.cold_flights.remove(&read.info_hash);
                    self.sourceless.remove(&read.info_hash);
                    read.peer_wait_elapsed =
                        Instant::now().saturating_duration_since(read.phase_start);
                    self.enter_piece_wait(read);
                    return None;
                }
            }
            Some(Err(e)) => return Some(Err(e)),
            None => return Some(Err(Self::missing())),
        }

        // Half the peer-wait budget gone and still nobody connected — force one
        // more announce before the piece deadline path takes over.  The flight
        // owns that re-announce, so the reads sharing it produce one, not one
        // each.
        let due = self
            .cold_flights
            .get_mut(&read.info_hash)
            .map(|flight| flight.take_reannounce(Instant::now()))
            .unwrap_or(false);
        if due {
            if let Some(handle) = self.handles.get(&read.info_hash) {
                handle.force_reannounce();
            }
        }
        None
    }

    /// Poll a read waiting for its pieces to arrive.
    fn poll_piece_wait(&mut self, read: &mut PendingRead) -> Option<TorrentResult<Vec<u8>>> {
        if self.stopping.load(Ordering::Relaxed) {
            return Some(Err(TorrentError::Timeout(
                "shutdown requested, read aborted".to_string(),
            )));
        }

        // Consume every piece that has become available, then wait on the first
        // one that has not — the old inline inner loop, minus the sleep on the
        // engine thread.
        loop {
            let piece_idx = read.current_piece;
            if piece_idx > read.end_piece {
                return Some(self.read_from_disk(
                    &read.info_hash,
                    read.start_piece,
                    read.end_piece,
                    read.piece_length,
                    read.num_pieces,
                    read.total_size,
                    read.absolute_offset,
                    read.end_offset,
                    read.size,
                ));
            }

            let have = self
                .handles
                .get(&read.info_hash)
                .map(|h| h.have_piece(piece_idx))
                .unwrap_or(false);
            // `have_piece` can be stale: the bit says complete but the file
            // was purged or truncated.  Such a piece cannot be re-downloaded
            // until a recheck clears the bit (libtorrent skips pieces it
            // already has), so detect it here and recheck instead of stalling
            // the piece-wait window.
            let stale = if have {
                let piece_key = PieceStore::piece_key(&read.info_hash, piece_idx);
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    read.piece_length,
                    read.num_pieces,
                    read.total_size,
                );
                self.store.has_stale_piece(&piece_key, expected)
            } else {
                false
            };
            let have_valid = have && !stale;
            let cached = {
                let piece_key = PieceStore::piece_key(&read.info_hash, piece_idx);
                let cache = self.store.cache_manager();
                cache
                    .lock()
                    .map(|c| {
                        PieceStore::is_piece_complete_in_cache(
                            &c,
                            &piece_key,
                            piece_idx,
                            read.piece_length,
                            read.num_pieces,
                            read.total_size,
                        )
                    })
                    .unwrap_or(false)
            };
            self.metrics.record_poll(have_valid || cached);
            if have_valid || cached {
                // Defer registration to `read_from_disk`: eager `register_piece`
                // credits the cache size and can LRU-evict this just-downloaded
                // piece before `read_from_disk` reads it, re-triggering the
                // stale-bit recheck.
                read.current_piece += 1;
                continue;
            }
            if stale {
                // The bit is set but the file is gone.  A recheck clears the
                // bit so libtorrent re-requests the piece; detected here (not
                // only in `after_settling`) because a whole-file read evicts
                // earlier pieces as later ones download, turning them stale
                // mid-wait — without this the wait runs out its 60s window.
                // The piece blocking the read is the one whose data left the
                // cache, so record it as the read's cache evidence.
                read.record_stale_piece(piece_idx);
                if self.start_recheck(read) {
                    return None;
                }
                // Recheck failed to start: fall through to the normal wait
                // (the window will time out rather than loop).
            }

            // A late-connecting seeder upgrades the wait window from the short
            // no-seeder timeout to the full read timeout.  The window restarts
            // from the connect time, so a seeder that arrives at the end of the
            // short window still gets a full `read_timeout_secs` rather than
            // its leftover sliver.
            if !read.has_seeder {
                if let Some(s) = self
                    .handles
                    .get(&read.info_hash)
                    .and_then(|h| h.status().ok())
                {
                    if s.num_seeds > 0 {
                        read.has_seeder = true;
                        read.piece_wait_start = Instant::now();
                        read.status = s;
                    }
                }
            }
            let window = Duration::from_secs(piece_wait_window_secs(
                read.has_seeder,
                read.is_peer_wait_exhausted,
                self.read_timeout_secs,
                self.no_seeder_read_timeout_secs,
            ));
            if read.piece_wait_start.elapsed() >= window {
                return Some(self.read_timed_out(read, piece_idx, window));
            }
            return None;
        }
    }

    /// Resolve a read whose piece-wait window elapsed: return the contiguous
    /// prefix of completed pieces when there is one, otherwise the
    /// `NoPeers`/`Timeout` error that distinguishes "no seeder" from "slow" and
    /// from a read stalled on the on-disk cache.
    fn read_timed_out(
        &mut self,
        read: &PendingRead,
        piece_idx: i32,
        window: Duration,
    ) -> TorrentResult<Vec<u8>> {
        // libtorrent's custom storage may have already written partial piece
        // data to disk for the requested range.  Record it as incomplete
        // metadata so `.stats` reflects the download progress the failed read
        // actually made.
        self.register_incomplete_on_disk_pieces(&read.info_hash, read.start_piece, read.end_piece);

        // Instead of empty (ENODATA) after the window, return the contiguous
        // prefix of completed pieces.  `current_piece` advances in order, so
        // every piece before `piece_idx` is complete (it is the first missing
        // one); returning that prefix as a short read gives visible progress
        // instead of 0 bytes the client can't tell from EOF.
        if let Some((partial_end, partial_size)) = partial_read_bounds(
            read.start_piece,
            piece_idx - 1,
            read.piece_length,
            read.absolute_offset,
            read.end_offset,
        ) {
            return self.read_from_disk(
                &read.info_hash,
                read.start_piece,
                piece_idx - 1,
                read.piece_length,
                read.num_pieces,
                read.total_size,
                read.absolute_offset,
                partial_end,
                partial_size,
            );
        }

        // Tell the three stalls apart: no seeder (`NoPeers`), a read the
        // on-disk cache window cannot cover (`Timeout` naming the cache), and
        // a slow-but-healthy swarm (`Timeout`).  If status is unavailable
        // (handle gone, `status()` failed), fall back to `Timeout` — don't
        // fabricate a zero-seeder swarm and mislead the user into checking
        // tracker health for a stale handle.
        let (progress, num_peers, num_seeds) = match self
            .handles
            .get(&read.info_hash)
            .and_then(|h| h.status().ok())
        {
            Some(s) => (s.progress * 100.0, s.num_peers, s.num_seeds),
            None => {
                return Err(TorrentError::Timeout(format!(
                    "Timed out waiting for piece {} after {:.0}s \
                         (status unavailable)",
                    piece_idx,
                    window.as_secs(),
                )));
            }
        };
        let read_span_bytes = read.end_offset.saturating_sub(read.absolute_offset);
        // An unreadable capacity must not read as "the cache is too small":
        // `u64::MAX` leaves only the stale-blocker evidence to classify a stall
        // as a cache one.
        let cache_capacity_bytes = self
            .store
            .cache_manager()
            .lock()
            .map(|c| c.max_cache_size())
            .unwrap_or(u64::MAX);
        let blocking_piece_stale = read.stale_blockers.contains(&piece_idx);
        // Write the one-line hint to the daemon's own stderr (operator-facing;
        // FUSE has no channel into the client's stderr) via a direct,
        // non-panicking write: `eprintln!` panics on a broken stderr (aborting
        // this thread), and `tracing` writes to stdout, not stderr.
        match classify_read_stall(
            num_seeds,
            &read.stale_blockers,
            piece_idx,
            read_span_bytes,
            cache_capacity_bytes,
        ) {
            ReadStallCause::NoSeeder => {
                let wait = no_seeder_wait(
                    read.is_peer_wait_exhausted,
                    read.has_seeder,
                    self.peer_discovery_window_secs(),
                    window.as_secs(),
                    discovery_window_advice(
                        self.read_timeout_secs,
                        self.peer_discovery_wait_secs,
                        self.peer_wait_cap_secs,
                    ),
                );
                let _ = writeln!(
                    std::io::stderr(),
                    "{}",
                    no_seeder_stderr_hint(num_peers, num_seeds, wait)
                );
                Err(TorrentError::NoPeers(no_peers_message(
                    &read.info_hash,
                    read.peer_wait_elapsed.as_secs(),
                    wait,
                )))
            }
            ReadStallCause::CacheStall => {
                let _ = writeln!(
                    std::io::stderr(),
                    "{}",
                    cache_stall_stderr_hint(
                        read_span_bytes,
                        cache_capacity_bytes,
                        blocking_piece_stale
                    )
                );
                Err(TorrentError::Timeout(cache_stall_message(
                    &read.info_hash,
                    window.as_secs(),
                    cache_capacity_bytes,
                    read_span_bytes,
                    blocking_piece_stale,
                    num_seeds,
                )))
            }
            ReadStallCause::SlowSwarm => Err(TorrentError::Timeout(format!(
                "Timed out waiting for piece {} after {:.0}s. \
                 Torrent progress: {:.2}%",
                piece_idx,
                window.as_secs(),
                progress,
            ))),
        }
    }

    /// Advance every parked read by one step, resolving (and removing) the ones
    /// that terminated.
    fn poll_pending_reads(&mut self) {
        if self.pending_reads.is_empty() {
            return;
        }
        let parked = std::mem::take(&mut self.pending_reads);
        let mut remaining = Vec::with_capacity(parked.len());
        for mut read in parked {
            match self.poll_read(&mut read) {
                Some(result) => self.finish_read(&read, result),
                None => remaining.push(read),
            }
        }
        self.pending_reads = remaining;
    }

    /// Deliver a parked read's final result and, when the reader priority
    /// gradient was applied, release exactly that reader.
    fn finish_read(&mut self, read: &PendingRead, result: TorrentResult<Vec<u8>>) {
        if let Some(id) = read.reader_id {
            self.release_reader(&read.info_hash, id);
        }
        let _ = read.reply.send(result);
    }

    /// Resolve every parked read with a shutdown abort.  Called as the engine
    /// loop exits so a blocked caller sees a timeout, not a bare channel
    /// disconnect.
    fn abort_pending_reads(&mut self) {
        for read in std::mem::take(&mut self.pending_reads) {
            self.finish_read(
                &read,
                Err(TorrentError::Timeout(
                    "shutdown requested, read aborted".to_string(),
                )),
            );
        }
    }

    /// Whether one piece of a read range is available locally right now: the
    /// handle has it and its on-disk data is present, or the piece store holds
    /// it complete.  A stale bit — `have_piece` true but the file gone or
    /// truncated — is *not* local: that piece has to be re-downloaded.
    fn piece_is_local(
        &self,
        info_hash: &str,
        piece_idx: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return false,
        };
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
        // use the unified stale detection — `have_piece` true
        // but the on-disk file is gone or truncated means the bit is
        // stale; the piece is NOT available locally and must be
        // re-downloaded.
        let have_valid = handle.have_piece(piece_idx)
            && !self.store.has_stale_piece(
                &piece_key,
                PieceStore::expected_piece_size(piece_idx, piece_length, num_pieces, total_size),
            );
        have_valid || complete
    }

    /// Whether every piece of a read range is available locally
    /// (see [`Self::piece_is_local`]).
    fn all_pieces_local(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> bool {
        (start_piece..=end_piece).all(|piece_idx| {
            self.piece_is_local(info_hash, piece_idx, piece_length, num_pieces, total_size)
        })
    }

    /// Every piece in the range whose libtorrent bitmask is stale —
    /// `have_piece == true` but the on-disk piece is missing or shorter than
    /// its expected size.  The data was in the cache and is no longer there;
    /// `has_stale_piece` cannot tell eviction from a verification purge from a
    /// file removed or truncated outside the cache, and all three leave the
    /// piece needing a re-download.  Empty when no such piece exists.  Returns
    /// *all* of them because the caller rechecks, and a `force_recheck` clears
    /// every missing piece's bit at once — a piece skipped here could never be
    /// re-detected.  The range is one read's span, so the vector stays small.
    fn stale_pieces_in_range(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
        piece_length: u64,
        num_pieces: i32,
        total_size: u64,
    ) -> Vec<i32> {
        let handle = match self.handles.get(info_hash) {
            Some(h) => h,
            None => return Vec::new(),
        };
        let mut stale = Vec::new();
        for piece_idx in start_piece..=end_piece {
            if handle.have_piece(piece_idx) {
                let piece_key = PieceStore::piece_key(info_hash, piece_idx);
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    piece_length,
                    num_pieces,
                    total_size,
                );
                if self.store.has_stale_piece(&piece_key, expected) {
                    stale.push(piece_idx);
                }
            }
        }
        stale
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
                    // The piece file is gone but we reached read_from_disk
                    // (fast path or piece-wait loop broke on `have_piece`
                    // without checking disk). Return `PieceNotReady` rather
                    // than silently skipping (a short-read EIO): the caller
                    // sees a transient error and can retry, and the stale
                    // bitmask is caught by the recheck guard next attempt.
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
            // verify the piece data length matches the expected
            // piece size. A shorter file means the piece was read while
            // libtorrent's write_piece was still writing blocks to it
            // (the write-during-read race). The shared read lock should
            // prevent this in normal operation, but this check is a safety
            // net for edge cases (e.g. cache eviction + re-download).
            let expected_piece_size =
                PieceStore::expected_piece_size(piece_idx, piece_length, num_pieces, total_size);
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
        // Register every piece AFTER the whole range has been read.  A
        // per-piece register during the loop evicts the oldest cache entry on
        // each `add_piece`; on a warm read whose tail pieces are already
        // cached but whose head must be re-downloaded, the head's registrations
        // would evict the still-unread tail pieces and re-trigger the recheck
        // spin.  Deferring means all bytes are captured before any eviction.
        for piece_idx in start_piece..=end_piece {
            if !self.store.has_piece(info_hash, piece_idx) {
                self.register_piece(info_hash, piece_idx, piece_length, num_pieces, total_size);
            }
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
        let expected =
            PieceStore::expected_piece_size(piece_idx, piece_length, num_pieces, total_size);
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

    /// After a read's piece-wait window times out, record any
    /// partial piece data that libtorrent's custom storage already wrote to
    /// disk for the requested range as *incomplete* cache metadata.
    ///
    /// The bytes on disk are real download progress; without this, `.stats`
    /// reports zero cached pieces and the failed read leaves no trace of the
    /// work it actually did.  Verified pieces are left untouched, and a later
    /// successful `register_piece` upgrades the incomplete entries to
    /// verified.
    fn register_incomplete_on_disk_pieces(
        &self,
        info_hash: &str,
        start_piece: i32,
        end_piece: i32,
    ) {
        self.store
            .register_incomplete_pieces_in_range(info_hash, start_piece, end_piece);
    }

    fn release_reader(&mut self, info_hash: &str, id: ReadId) {
        if let Some(handle) = self.handles.get(info_hash) {
            self.scheduler
                .reader_released(handle, info_hash, id, &self.store);
        }
    }

    /// Drain all queued piece-completion events forwarded by the alert
    /// consumer thread and register each finished piece.  This is the
    /// convergence path for background/prefetch downloads: they never go
    /// through a read's piece-wait loop, so without it `piece_ready` would
    /// never fire and the retained prefetch window could not clean up.
    fn drain_piece_finished(&mut self) {
        while let Ok((info_hash, piece_index)) = self.piece_finished_rx.try_recv() {
            self.handle_piece_finished(&info_hash, piece_index);
        }
    }

    /// Register a piece reported complete by the `piece_finished` alert and
    /// clear it from the scheduler's wanted set.  The piece is complete on
    /// disk (libtorrent has written and hash-checked it), so its on-disk size
    /// is the authoritative registered size.
    fn handle_piece_finished(&mut self, info_hash: &str, piece_index: i32) {
        if let Err(e) = self.store.register_finished_piece(info_hash, piece_index) {
            tracing::warn!(
                "handle_piece_finished: failed to register {}:piece:{}: {:?}",
                info_hash,
                piece_index,
                e
            );
        }
        if let Some(handle) = self.handles.get(info_hash) {
            self.scheduler.piece_ready(handle, info_hash, piece_index);
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
    ///
    /// Costs one `post_torrent_updates` FFI round trip (libtorrent rebuilds a
    /// `torrent_status` per handle), a status/tracker read per handle and one
    /// cache lookup per piece, so the loop calls it only while a `.stats`
    /// reader is looking (see [`READER_DEMAND_TTL`]).
    fn publish_snapshot(&mut self) {
        let mut statuses = HashMap::new();
        let mut pieces = HashMap::new();
        let mut trackers = HashMap::new();
        let mut swarms: Vec<(String, bool)> = Vec::new();
        // request libtorrent to refresh per-torrent statistics
        // before reading status. Without this, `status().num_peers` can
        // return 0 even when peers are connected — the internal peer list
        // is only refreshed on session tick or post_torrent_updates.
        self.session.post_torrent_updates();
        for (info_hash, handle) in &self.handles {
            if let Ok(status) = handle.status() {
                swarms.push((
                    info_hash.clone(),
                    swarm_is_empty(status.num_peers, status.num_seeds),
                ));
                statuses.insert(info_hash.clone(), status);
            }
            if let Ok(list) = handle.trackers() {
                trackers.insert(info_hash.clone(), list);
            }
            if let Some(num_pieces) = self.scheduler.num_pieces(info_hash) {
                if let Ok(status) = self.build_pieces_status(info_hash, num_pieces) {
                    let piece_length = self.scheduler.piece_length(info_hash).unwrap_or(0);
                    pieces.insert(info_hash.clone(), (piece_length, status));
                }
            }
        }
        let now = Instant::now();
        self.empty_swarm_since = advance_condition_since(&self.empty_swarm_since, &swarms, now);
        let empty_swarm_secs = elapsed_secs_since(&self.empty_swarm_since, now);
        let waiting_reads = aggregate_waiting_reads(
            self.pending_reads
                .iter()
                .map(|read| (read.info_hash.as_str(), read.waiting_since)),
            now,
        );
        // A seeder with a zero download rate is only a stall once it has held
        // for a while: the sample right after a connection lands before the
        // first block, so a single zero rate is not evidence of no progress.
        let slow_swarms: Vec<(String, bool)> = statuses
            .iter()
            .map(|(info_hash, status)| {
                (
                    info_hash.clone(),
                    status.num_seeds >= 1
                        && status.download_rate == 0
                        && waiting_reads.contains_key(info_hash),
                )
            })
            .collect();
        self.slow_swarm_since = advance_condition_since(&self.slow_swarm_since, &slow_swarms, now);
        let slow_swarm_secs = elapsed_secs_since(&self.slow_swarm_since, now);
        if let Ok(mut snap) = self.snapshot.lock() {
            *snap = DownloadSnapshot {
                statuses,
                pieces,
                trackers,
                private_torrents: self.private_torrents.clone(),
                empty_swarm_secs,
                waiting_reads,
                slow_swarm_secs,
            };
        }
    }

    /// periodically persist dirty cache metadata so the on-disk
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

#[cfg(test)]
mod tests {
    use super::{
        advance_condition_since, advance_reader_demand, aggregate_waiting_reads,
        cache_stall_message, cache_stall_stderr_hint, classify_read_stall, discovery_window_advice,
        no_peers_message, no_seeder_stderr_hint, no_seeder_wait, partial_read_bounds,
        peer_discovery_window_secs, piece_wait_window_secs, read_wait_budget_secs,
        reader_demand_is_live, should_publish_after_command, swarm_is_empty, ColdFlight,
        DiscoveryWindowAdvice, NoSeederWait, ReadStallCause, SourcelessSwarm, WaitingReads,
        NO_SEEDER_FAST_FAIL_SECS, READER_DEMAND_TTL, SNAPSHOT_INTERVAL,
    };
    use crate::infrastructure::config::{
        DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS, DEFAULT_PEER_DISCOVERY_WAIT_SECS,
        DEFAULT_READ_TIMEOUT_SECS,
    };
    use std::time::{Duration, Instant};

    /// The advice the mapping hands the formatters for the shipped defaults
    /// (`read_timeout_secs` 60, discovery wait 30, no cap): the discovery wait
    /// is the binding term.
    const DEFAULT_DISCOVERY: DiscoveryWindowAdvice = DiscoveryWindowAdvice {
        formula: "min(read_timeout_secs, peer_discovery_wait_secs)",
        advice: "raise [timeouts] peer_discovery_wait_secs and retry",
    };

    /// A cold flight is the shared peer-discovery window of one info_hash: its
    /// deadline is fixed when the first reader creates it, so every reader that
    /// attaches later observes the same expiry instead of getting a fresh
    /// window of its own.
    #[test]
    fn cold_flight_deadline_is_fixed_at_creation() {
        let start = Instant::now();
        // The window `enter_peer_wait` computes for the shipped config: the
        // configured discovery wait, uncapped by read_timeout_secs (60) or by
        // the optional peer-wait cap (unset).
        let window = Duration::from_secs(DEFAULT_PEER_DISCOVERY_WAIT_SECS);
        let flight = ColdFlight::new(start, window);
        assert_eq!(flight.deadline, start + window);
        // Attachment copies `started`/`deadline` verbatim; a later reader must
        // not extend the window.
        assert_eq!(flight.started, start);
    }

    /// A flight is expired exactly when its window ended.  A reader attaching
    /// to an expired flight must start a fresh one (see `enter_peer_wait`):
    /// otherwise it would fail on its first poll and leave swarm discovery to
    /// libtorrent's own announce schedule.
    #[test]
    fn cold_flight_expires_at_its_deadline() {
        let start = Instant::now();
        let window = Duration::from_secs(DEFAULT_PEER_DISCOVERY_WAIT_SECS);
        let flight = ColdFlight::new(start, window);
        assert!(!flight.is_expired(start + window - Duration::from_secs(1)));
        assert!(flight.is_expired(start + window));
        assert!(flight.is_expired(start + window * 2));
    }

    /// The mid-window re-announce belongs to the flight, not the reader: it
    /// fires once, after half the window, however many readers are attached.
    /// Pre-fix each parked read carried its own flag, so N concurrent cold
    /// readers produced N re-announces on one torrent.
    #[test]
    fn cold_flight_reannounces_once_after_half_the_window() {
        let start = Instant::now();
        let window = Duration::from_secs(10);
        let mut flight = ColdFlight::new(start, window);

        assert!(
            !flight.take_reannounce(start + Duration::from_secs(4)),
            "re-announce must not fire before half the window elapsed"
        );
        assert!(
            flight.take_reannounce(start + Duration::from_secs(5)),
            "re-announce must fire once half the window elapsed"
        );
        assert!(
            !flight.take_reannounce(start + Duration::from_secs(9)),
            "a second attached reader must not re-announce again"
        );
    }

    /// A sourceless verdict speaks for exactly one window of wall-clock after
    /// the probe gave up: a read arriving inside that window inherits the
    /// probe's outcome instead of re-spending the window, and a read arriving
    /// at the boundary probes again — that lapse is what keeps a seeder that
    /// came online after the probe reachable by a retry.
    #[test]
    fn sourceless_verdict_lapses_one_window_after_the_probe() {
        let at = Instant::now();
        let window = Duration::from_secs(12);
        let verdict = SourcelessSwarm::new(at, window, window);

        assert!(verdict.is_live(at), "a fresh verdict is live");
        assert!(verdict.is_live(at + Duration::from_millis(11_999)));
        assert!(
            !verdict.is_live(at + window),
            "the verdict must be spent exactly one window after the probe"
        );
        assert!(!verdict.is_live(at + Duration::from_secs(60)));
    }

    /// The verdict's re-announce cadence matches the probe's own: one announce
    /// per half-window, repeated for as long as the verdict lives — the reads
    /// inheriting it do not wait, so nothing else drives discovery, and a
    /// seeder that joined after the probe must still be found.  A burst of
    /// chunk reads inside one half-window must not each announce.
    #[test]
    fn sourceless_verdict_reannounces_once_per_half_window() {
        let at = Instant::now();
        let window = Duration::from_secs(10);
        let mut verdict = SourcelessSwarm::new(at, window, window);

        assert!(
            !verdict.take_reannounce(at),
            "the probe announced at its own half-window mark, so an inheriting \
             read must not announce immediately after it"
        );
        assert!(!verdict.take_reannounce(at + Duration::from_secs(4)));
        assert!(
            verdict.take_reannounce(at + Duration::from_secs(5)),
            "an inheriting read must announce once half the window elapsed"
        );
        assert!(
            !verdict.take_reannounce(at + Duration::from_secs(9)),
            "a later read inside the same half-window must not announce again"
        );
        assert!(
            verdict.take_reannounce(at + Duration::from_secs(10)),
            "the cadence keeps running while the verdict is live"
        );
    }

    /// the no-seeder stderr hint must use the exact message the
    /// operator greps for — `no seeder connected (Peers:N Seeds:M)` with the
    /// live peer/seed counts — so a currently-empty swarm is distinguishable
    /// from "seeder slow" and from a cache stall (which surface as different
    /// hints).  The advice is selected by which window actually ended the read,
    /// because only the discovery one is operator-tunable.
    #[test]
    fn no_seeder_hint_reports_live_counts() {
        assert_eq!(
            no_seeder_stderr_hint(
                0,
                0,
                NoSeederWait::PeerDiscoveryWindow {
                    secs: 30,
                    formula: DEFAULT_DISCOVERY.formula,
                    advice: DEFAULT_DISCOVERY.advice,
                }
            ),
            "no seeder connected (Peers:0 Seeds:0) within the 30s peer-discovery \
             window; raise [timeouts] peer_discovery_wait_secs and retry"
        );
        // Peers may be non-zero (leechers without the piece) while seeds stay 0.
        // This read ended in the piece wait, so the hint names that window
        // instead of the discovery knob.
        assert_eq!(
            no_seeder_stderr_hint(
                3,
                0,
                NoSeederWait::NoSeederPieceWait {
                    secs: 12,
                    discovery_window_secs: 30,
                }
            ),
            "no seeder connected (Peers:3 Seeds:0) after the 12s no-seeder piece \
             wait; no seeder is present in the swarm"
        );
        assert!(!no_seeder_stderr_hint(
            0,
            0,
            NoSeederWait::PeerDiscoveryWindow {
                secs: 30,
                formula: DEFAULT_DISCOVERY.formula,
                advice: DEFAULT_DISCOVERY.advice,
            }
        )
        .contains("cache"));
    }

    /// The no-seeder hint must name the window that actually ended the read:
    /// the discovery window only when the read exhausted it on an empty swarm
    /// and never saw a seeder, otherwise the piece wait it ended in.  Naming
    /// the discovery knob in the latter cases would point the operator at a
    /// knob that cannot change this read's outcome.
    #[test]
    fn no_seeder_wait_names_the_window_that_elapsed() {
        // Peer discovery ran out on an empty swarm and no seeder ever
        // connected: the piece wait stays at zero, so the discovery window is
        // the only wait the read spent.
        assert_eq!(
            no_seeder_wait(true, false, 30, 0, DEFAULT_DISCOVERY),
            NoSeederWait::PeerDiscoveryWindow {
                secs: 30,
                formula: DEFAULT_DISCOVERY.formula,
                advice: DEFAULT_DISCOVERY.advice,
            }
        );
        // A peer was already there (leecher-only swarm) or connected during
        // discovery: the piece wait ended the read, not the discovery window.
        assert_eq!(
            no_seeder_wait(false, false, 30, 12, DEFAULT_DISCOVERY),
            NoSeederWait::NoSeederPieceWait {
                secs: 12,
                discovery_window_secs: 30,
            }
        );
        // A seeder connected only after the discovery window elapsed, so the
        // piece wait restarted at the full read timeout and then ended the
        // read; the discovery window is not what the read ran out.
        assert_eq!(
            no_seeder_wait(true, true, 30, 60, DEFAULT_DISCOVERY),
            NoSeederWait::NoSeederPieceWait {
                secs: 60,
                discovery_window_secs: 30,
            }
        );
    }

    /// The discovery window is `min(read_timeout_secs, peer_discovery_wait_secs)`
    /// (and the optional `peer_wait_cap_secs` when set), so the advice must name
    /// whichever terms cap it — recommending a term above the minimum would
    /// send the operator to a knob that cannot widen the window.  Equal values
    /// need all of them, since raising one alone leaves the `min` as it is.
    #[test]
    fn discovery_window_advice_names_the_binding_knob() {
        // Shipped defaults (read_timeout 60, discovery wait 30, no cap): the
        // discovery wait is the smaller one.
        let discovery_binds = discovery_window_advice(60, 30, None);
        assert!(discovery_binds
            .advice
            .contains("raise [timeouts] peer_discovery_wait_secs"));
        assert!(!discovery_binds.advice.contains("read_timeout_secs"));
        // A short read timeout caps the window: the discovery knob alone cannot
        // widen it.
        let timeout_binds = discovery_window_advice(4, 30, None);
        assert!(timeout_binds
            .advice
            .contains("raise [timeouts] read_timeout_secs"));
        assert!(timeout_binds.advice.contains("caps the window"));
        // Both equal: either alone leaves the window unchanged.
        let both_bind = discovery_window_advice(30, 30, None);
        assert!(both_bind
            .advice
            .contains("read_timeout_secs and peer_discovery_wait_secs together"));
        // A configured cap below both waits is the binding term, and the
        // formula shows the cap's part in the `min`.
        let cap_binds = discovery_window_advice(60, 120, Some(5));
        assert_eq!(
            cap_binds.advice,
            "raise [timeouts] peer_wait_cap_secs (it caps the window) and retry"
        );
        assert!(cap_binds.formula.contains("peer_wait_cap_secs"));
        // A cap tied with the discovery wait: only raising both widens the
        // `min`.
        let cap_ties = discovery_window_advice(60, 30, Some(30));
        assert!(cap_ties
            .advice
            .contains("peer_discovery_wait_secs and peer_wait_cap_secs together"));
        // A cap above the window does not bind, so it must not be advised —
        // but it still appears in the formula, because it is what the read
        // timeout and discovery wait are `min`'d against.
        let cap_loose = discovery_window_advice(60, 30, Some(300));
        assert_eq!(
            cap_loose.advice,
            "raise [timeouts] peer_discovery_wait_secs and retry"
        );
        assert!(cap_loose.formula.contains("peer_wait_cap_secs"));
    }

    /// the `NoPeers` message describes an empty swarm only.  A read stalled on
    /// the cache is reported by `cache_stall_message` — the bare `ENODATA` the
    /// FUSE layer returns cannot tell the two apart, so the texts must differ.
    /// Each branch must also name the window the read ran out of and, for the
    /// discovery window, the formula and the knob that actually widen it — a
    /// read that ended in the piece wait must not be pointed at the discovery
    /// knob, which cannot change its outcome.
    #[test]
    fn no_peers_message_points_at_the_tracker_only() {
        let discovery = no_peers_message(
            "abc",
            9,
            NoSeederWait::PeerDiscoveryWindow {
                secs: 30,
                formula: DEFAULT_DISCOVERY.formula,
                advice: DEFAULT_DISCOVERY.advice,
            },
        );
        assert!(discovery.contains("check tracker health"));
        assert!(discovery.contains("raise [timeouts] peer_discovery_wait_secs"));
        assert!(
            discovery.contains("(window 30s = min(read_timeout_secs, peer_discovery_wait_secs))")
        );
        assert!(!discovery.contains("cache_size"));
        assert_ne!(
            discovery,
            cache_stall_message("abc", 15, 1 << 20, 1 << 20, true, 0)
        );

        // A configured cap is part of the window's formula, so the message
        // shows the read's actual bounds rather than the uncapped pair.
        let capped = no_peers_message(
            "abc",
            9,
            NoSeederWait::PeerDiscoveryWindow {
                secs: 5,
                formula: "min(read_timeout_secs, peer_discovery_wait_secs, peer_wait_cap_secs)",
                advice: "raise [timeouts] peer_wait_cap_secs (it caps the window) and retry",
            },
        );
        assert!(capped.contains(
            "(window 5s = min(read_timeout_secs, peer_discovery_wait_secs, peer_wait_cap_secs))"
        ));
        assert!(capped.contains("raise [timeouts] peer_wait_cap_secs"));

        // Piece-wait branch: states the fact and names the window's source; it
        // must not hand out the discovery knob as advice.
        let piece_wait = no_peers_message(
            "abc",
            9,
            NoSeederWait::NoSeederPieceWait {
                secs: 12,
                discovery_window_secs: 30,
            },
        );
        assert!(piece_wait.contains("check tracker health"));
        assert!(piece_wait.contains("derives from read_timeout_secs"));
        assert!(piece_wait.contains("not from [timeouts] peer_discovery_wait_secs"));
        assert!(
            !piece_wait.contains("raise [timeouts]"),
            "the piece-wait branch must not advise a timeout that cannot widen its window: {piece_wait}"
        );
        assert!(!piece_wait.contains("cache_size"));
    }

    /// The discovery window is the smallest of the configured wait, the
    /// `read_timeout_secs` cap that bounds the whole read, and the optional
    /// `peer_wait_cap_secs` fail-fast ceiling — each of them narrows the window
    /// and none of them widens another's.
    #[test]
    fn peer_discovery_window_is_capped_by_the_read_timeout() {
        assert_eq!(
            peer_discovery_window_secs(60, DEFAULT_PEER_DISCOVERY_WAIT_SECS, None),
            DEFAULT_PEER_DISCOVERY_WAIT_SECS
        );
        assert_eq!(peer_discovery_window_secs(4, 30, None), 4);
        assert_eq!(peer_discovery_window_secs(60, 120, None), 60);
        assert_eq!(peer_discovery_window_secs(120, 120, None), 120);
        // The optional cap narrows the window whenever it is below both.
        assert_eq!(peer_discovery_window_secs(60, 120, Some(20)), 20);
        assert_eq!(peer_discovery_window_secs(60, 30, Some(30)), 30);
        // A cap above the other bounds leaves them to decide the window.
        assert_eq!(peer_discovery_window_secs(60, 120, Some(300)), 60);
        assert_eq!(peer_discovery_window_secs(4, 30, Some(300)), 4);
    }

    /// A read waiting on a piece the cache no longer holds — or a read range
    /// larger than the whole cache — is a cache stall, not a swarm problem.
    /// Only an empty swarm without cache-side evidence is "no seeder", and only
    /// a seeder-backed read that fits the cache is "slow swarm".
    #[test]
    fn read_stall_cause_separates_cache_from_swarm() {
        const CACHE: u64 = 1 << 20;
        // No cache evidence, no seeder: the swarm is the cause.
        assert_eq!(
            classify_read_stall(0, &[], 0, CACHE, CACHE),
            ReadStallCause::NoSeeder
        );
        // The read waits on a piece whose data left the cache, and no seeder
        // can re-fetch it: still a cache stall — the data was resident and a
        // larger cache would have served the read without any seeder.
        assert_eq!(
            classify_read_stall(0, &[0], 0, CACHE / 8, CACHE),
            ReadStallCause::CacheStall
        );
        // Same with a seeder connected: the re-download is not blocked on the
        // swarm, so the cache is what the read ran out of.
        assert_eq!(
            classify_read_stall(1, &[0], 0, CACHE / 8, CACHE),
            ReadStallCause::CacheStall
        );
        // A read range larger than the cache cannot stay resident: a cache
        // stall once a seeder is there to serve the re-download.
        assert_eq!(
            classify_read_stall(1, &[], 0, 4 * CACHE, CACHE),
            ReadStallCause::CacheStall
        );
        // Range exactly filling the cache, seeder present, no cache evidence:
        // the swarm was merely slow.
        assert_eq!(
            classify_read_stall(1, &[], 0, CACHE, CACHE),
            ReadStallCause::SlowSwarm
        );
        // Unknown capacity (u64::MAX) can never be proven too small.
        assert_eq!(
            classify_read_stall(1, &[], 0, u64::MAX - 1, u64::MAX),
            ReadStallCause::SlowSwarm
        );
    }

    /// Cache evidence belongs to the piece the read is waiting on *now*: a
    /// stale piece the read already re-downloaded and moved past must not
    /// colour a later stall, or a slow swarm would be reported as a cache
    /// problem (the read advanced past it, so the bit is no longer the
    /// blocker).
    #[test]
    fn read_stall_cause_ignores_cache_evidence_for_a_served_piece() {
        const CACHE: u64 = 1 << 20;
        // Piece 0 was stale, got re-downloaded, and the read now waits on
        // piece 1 with a connected seeder: a slow swarm, not a cache stall.
        assert_eq!(
            classify_read_stall(1, &[0], 1, CACHE / 8, CACHE),
            ReadStallCause::SlowSwarm
        );
        // Without a seeder the same read is a plain no-seeder stall.
        assert_eq!(
            classify_read_stall(0, &[0], 1, CACHE / 8, CACHE),
            ReadStallCause::NoSeeder
        );
    }

    /// Several pieces of one read can lose their data at once, and the recheck
    /// clears all their bits together: the evidence for a piece the read has
    /// not reached yet has to survive on the recorded list, or the read's
    /// eventual stall on it would be blamed on the swarm.
    #[test]
    fn read_stall_cause_keeps_evidence_for_pieces_not_yet_reached() {
        const CACHE: u64 = 1 << 20;
        // Pieces 0..=2 were all stale; the read re-downloaded 0, is waiting on
        // 1, and 2 is still ahead of it.
        assert_eq!(
            classify_read_stall(1, &[0, 1, 2], 1, CACHE / 8, CACHE),
            ReadStallCause::CacheStall
        );
        // Same read, no seeder connected.
        assert_eq!(
            classify_read_stall(0, &[0, 1, 2], 1, CACHE / 8, CACHE),
            ReadStallCause::CacheStall
        );
    }

    /// The cache-stall message must name the cache and both actions, and must
    /// state which swarm state the read failed in — a user seeing only
    /// `ENODATA` has no other way to tell a `cache_size` problem from a missing
    /// seeder.  It must not claim *why* the data left the cache: eviction, a
    /// verification purge and an external removal are indistinguishable here.
    #[test]
    fn cache_stall_message_names_cache_cause_and_swarm_state() {
        let stale = cache_stall_message("abc", 15, 1 << 20, 1 << 20, true, 1);
        assert!(stale.contains("was in the on-disk cache and is no longer there"));
        assert!(stale.contains("evicted, purged after a failed check, or removed"));
        assert!(stale.contains("cache_size = 1.00 MiB"));
        assert!(stale.contains("this read spans 1.00 MiB"));
        assert!(stale.contains("raise [cache] cache_size"));
        assert!(stale.contains("not blocked on the swarm"));

        // No stale evidence: the cause is the read range, not a removed piece,
        // so the message must not claim the data ever was in the cache.
        let oversized = cache_stall_message("abc", 15, 1 << 20, 4 << 20, false, 1);
        assert!(oversized.contains("larger than the whole on-disk cache"));
        assert!(!oversized.contains("is no longer there"));
        assert!(!oversized.contains("purged"));

        // No seeder connected: the re-download cannot start, and the message
        // must say so instead of implying a healthy swarm.
        let sourceless = cache_stall_message("abc", 15, 1 << 20, 1 << 20, true, 0);
        assert!(sourceless.contains("cannot start until one connects"));
        assert!(!sourceless.contains("not blocked on the swarm"));
    }

    /// The cache-stall hint is what the operator greps in the daemon's stderr:
    /// it must carry the two numbers (so `[cache] cache_size` can be sized) and
    /// the evidence marker, and must not read like the no-seeder hint.
    #[test]
    fn cache_stall_hint_reports_numbers_and_evidence() {
        assert_eq!(
            cache_stall_stderr_hint(1 << 20, 1 << 20, true),
            "read stalled on the on-disk cache \
             (cache_size=1.00 MiB, read span=1.00 MiB, piece the read waits on \
             is gone from cache); raise [cache] cache_size if the cache is \
             evicting data the read needs"
        );
        assert_eq!(
            cache_stall_stderr_hint(4 << 20, 1 << 20, false),
            "read stalled on the on-disk cache \
             (cache_size=1.00 MiB, read span=4.00 MiB, read span exceeds cache); \
             raise [cache] cache_size if the cache is evicting data the read \
             needs"
        );
    }

    /// the read budget must cover all five synchronous phases —
    /// state-transition wait + recheck wait (capped) + peer-discovery window
    /// (min of its bounds) + no-seeder wait (capped) + piece wait (full window,
    /// reset on seeder connect) — so the FUSE deferred-read deadline never
    /// expires a ticket while the engine is still legitimately waiting.
    #[test]
    fn budget_covers_all_slow_path_phases() {
        // Shipped defaults: read_timeout_secs = 60 (DEFAULT_READ_TIMEOUT_SECS),
        // peer_discovery_wait_secs = 30 (DEFAULT_PEER_DISCOVERY_WAIT_SECS) and
        // no-seeder window = 15 (DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS), no
        // peer-wait cap:
        //   60 (state) + 10 (recheck cap) + 30 (peer discovery) + 15 (no-seeder
        //   cap) + 60 (piece) = 175s.
        assert_eq!(
            read_wait_budget_secs(
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_PEER_DISCOVERY_WAIT_SECS,
                None,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ),
            60 + 10 + 30 + 15 + 60
        );
        // A discovery window below the other bounds contributes only its own
        // value, and one above the read timeout is capped by it.
        assert_eq!(
            read_wait_budget_secs(60, 9, None, 15),
            60 + 10 + 9 + 15 + 60
        );
        assert_eq!(
            read_wait_budget_secs(60, 300, None, 15),
            60 + 10 + 60 + 15 + 60
        );
        // A peer-wait cap shortens the discovery term it caps.
        assert_eq!(
            read_wait_budget_secs(60, 300, Some(20), 15),
            60 + 10 + 20 + 15 + 60
        );
        // A raised no-seeder window adds its own value to the budget.
        assert_eq!(
            read_wait_budget_secs(60, 30, None, 45),
            60 + 10 + 30 + 45 + 60
        );
        // A no-seeder window above the read timeout is capped by it.
        assert_eq!(
            read_wait_budget_secs(60, 30, None, 300),
            60 + 10 + 30 + 60 + 60
        );
        // Short timeout still caps every phase at the timeout itself.
        assert_eq!(read_wait_budget_secs(4, 30, None, 15), 4 + 4 + 4 + 4 + 4);
        // Timeout below every cap.
        assert_eq!(read_wait_budget_secs(2, 30, None, 15), 2 + 2 + 2 + 2 + 2);
    }

    #[test]
    fn budget_exceeds_legacy_deadline_for_default_timeout() {
        // The old FUSE deadline was `read_timeout_secs + 5` — shorter than
        // the engine's worst-case budget and even its peer-wait+piece-wait
        // path. At the default timeout the budget (175s) still exceeds the
        // legacy deadline (65s), so a slow seeder is never expired early.
        assert!(
            read_wait_budget_secs(
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_PEER_DISCOVERY_WAIT_SECS,
                None,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ) > DEFAULT_READ_TIMEOUT_SECS + 5
        );
    }

    /// a no-seeder read must never wait the full read timeout — it caps at the
    /// configured no-seeder window
    /// ([`DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS`] by default), so the engine
    /// thread is not blocked for the whole `read_timeout_secs` and concurrent
    /// healthy reads on the same mount are not serialized behind a dead
    /// torrent.
    #[test]
    fn no_seeder_piece_wait_caps_at_short_timeout() {
        assert_eq!(
            piece_wait_window_secs(
                false,
                false,
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ),
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
        );
        assert_eq!(
            piece_wait_window_secs(false, false, 120, DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS),
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
        );
        // A raised no-seeder window keeps the read waiting longer — the
        // low-peer tradeoff the setting exists for.
        assert_eq!(piece_wait_window_secs(false, false, 120, 60), 60);
        // A read_timeout below the cap still bounds the window at itself.
        assert_eq!(piece_wait_window_secs(false, false, 3, 60), 3);
    }

    /// a sourceless read (peer discovery already elapsed with no seeder) must
    /// not spend the no-seeder window again — it fails fast so the engine
    /// thread is freed for healthy reads instead of re-blocking per chunk.
    #[test]
    fn peer_wait_exhausted_no_seeder_fails_fast() {
        assert_eq!(
            piece_wait_window_secs(
                false,
                true,
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ),
            NO_SEEDER_FAST_FAIL_SECS
        );
        // A seeder connected mid-wait still gets the full timeout, even when
        // peer discovery had previously elapsed.
        assert_eq!(
            piece_wait_window_secs(
                true,
                true,
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ),
            DEFAULT_READ_TIMEOUT_SECS
        );
    }

    /// the no-seeder fast-fail may only trigger for a truly empty swarm — a
    /// leecher (`num_peers > 0, num_seeds == 0`) is not empty, so it keeps the
    /// regular no-seeder piece-wait window instead of failing in zero seconds.
    #[test]
    fn swarm_is_empty_requires_no_peers_and_no_seeds() {
        assert!(swarm_is_empty(0, 0));
        assert!(!swarm_is_empty(1, 0));
        assert!(!swarm_is_empty(0, 1));
        assert!(!swarm_is_empty(3, 2));
    }

    /// a "condition has held since" clock must measure one continuous window:
    /// a holding sample starts the clock, subsequent holding samples keep the
    /// original start, a sample that stops holding drops the entry, and a
    /// handle absent from the next sample set is dropped entirely.
    #[test]
    fn advance_condition_since_tracks_continuous_window() {
        use std::collections::HashMap;
        use std::time::{Duration, Instant};

        let t0 = Instant::now();
        let t1 = t0 + Duration::from_secs(5);
        let t2 = t0 + Duration::from_secs(9);

        let mut previous: HashMap<String, Instant> = HashMap::new();
        previous = advance_condition_since(&previous, &[("a".into(), true)], t0);
        assert_eq!(previous.get("a"), Some(&t0));

        // still empty at t1 → keeps the original start, so the published age
        // measures the continuous window (5s), not the last sample.
        previous =
            advance_condition_since(&previous, &[("a".into(), true), ("b".into(), true)], t1);
        assert_eq!(previous.get("a"), Some(&t0));
        assert_eq!(previous.get("b"), Some(&t1));

        // "a" gains a peer at t2 → dropped; "b" stays empty → keeps t1.
        previous =
            advance_condition_since(&previous, &[("a".into(), false), ("b".into(), true)], t2);
        assert!(!previous.contains_key("a"));
        assert_eq!(previous.get("b"), Some(&t1));

        // "b" gains a peer, then goes empty again → a fresh clock at t2, not
        // the pre-connection start it had before.
        previous = advance_condition_since(&previous, &[("b".into(), false)], t2);
        assert!(!previous.contains_key("b"));
        previous = advance_condition_since(&previous, &[("b".into(), true)], t2);
        assert_eq!(previous.get("b"), Some(&t2));
    }

    /// one pass must yield both facts `.stats` shows about a torrent's waiting:
    /// the oldest parked read's age and how many are parked.  The oldest wins
    /// (a late reader must not reset the report), and an info_hash with no
    /// parked read stays absent so `.stats` can drop its wait lines.
    #[test]
    fn waiting_reads_report_oldest_age_and_count() {
        use std::collections::HashMap;
        use std::time::{Duration, Instant};

        let t0 = Instant::now();
        let now = t0 + Duration::from_secs(12);
        let waits = [
            ("torrent-a", t0),
            // A reader that joined 7s ago must not reset torrent-a's wait,
            // but it must still be counted.
            ("torrent-a", t0 + Duration::from_secs(5)),
            ("torrent-b", t0 + Duration::from_secs(7)),
        ];
        let published = aggregate_waiting_reads(
            waits.iter().map(|(info_hash, since)| (*info_hash, *since)),
            now,
        );
        assert_eq!(
            published.get("torrent-a"),
            Some(&WaitingReads {
                oldest_secs: 12,
                count: 2
            })
        );
        assert_eq!(
            published.get("torrent-b"),
            Some(&WaitingReads {
                oldest_secs: 5,
                count: 1
            })
        );
        assert_eq!(published.get("torrent-c"), None);

        let empty: HashMap<String, WaitingReads> =
            aggregate_waiting_reads(std::iter::empty::<(&str, Instant)>(), now);
        assert!(empty.is_empty());
    }

    /// a read with a connected seeder uses the full window so a
    /// slow-but-present seeder is not failed fast.
    #[test]
    fn seeder_piece_wait_uses_full_timeout() {
        assert_eq!(
            piece_wait_window_secs(
                true,
                false,
                DEFAULT_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS
            ),
            DEFAULT_READ_TIMEOUT_SECS
        );
        assert_eq!(piece_wait_window_secs(true, false, 3, 60), 3);
    }

    /// With the first piece missing, the partial read is empty —
    /// the read must keep its existing NoPeers/Timeout error, not fabricate
    /// a short read out of nothing.
    #[test]
    fn partial_bounds_none_when_first_piece_missing() {
        // Timed out on start_piece itself → last_complete = start_piece - 1.
        assert_eq!(
            partial_read_bounds(4, 3, 262_144, 1_048_576, 2_097_152),
            None
        );
    }

    /// The completed prefix clamps to the requested end and to the
    /// piece boundary of the last completed piece — a full piece length.
    #[test]
    fn partial_bounds_cover_completed_prefix() {
        // 256 KiB pieces; read [1 MiB, 2 MiB) spans pieces 4..=7.  Timed out
        // on piece 6 → pieces 4-5 completed: return [1 MiB, 1.5 MiB).
        assert_eq!(
            partial_read_bounds(4, 5, 262_144, 1_048_576, 2_097_152),
            Some((1_572_864, 524_288))
        );
    }

    /// When the requested end lands before the piece boundary, the
    /// partial end must clamp to the request end, not overrun it into the
    /// still-missing piece's span.
    #[test]
    fn partial_bounds_clamp_to_request_end() {
        // end_offset (1_200_000) is inside piece 4's span and short of the
        // piece-5 boundary (1_310_720): the clamp must stop at the request.
        assert_eq!(
            partial_read_bounds(4, 4, 262_144, 1_048_576, 1_200_000),
            Some((1_200_000, 151_424))
        );
        // First piece missing → None, regardless of how the end clamps.
        assert_eq!(
            partial_read_bounds(4, 3, 262_144, 1_048_576, 1_200_000),
            None
        );
    }

    /// A `.stats` accessor that the engine has already seen must not move the
    /// demand window: only a new accessor (a changed counter) opens a fresh
    /// one, so the window measures "is someone still reading" rather than
    /// "has anyone ever read".
    #[test]
    fn reader_demand_window_opens_only_on_a_new_accessor() {
        let now = Instant::now();
        let window = Some(now + READER_DEMAND_TTL);

        assert_eq!(advance_reader_demand(1, 1, window, now), (1, window));

        let (seen, until) = advance_reader_demand(2, 1, window, now);
        assert_eq!(seen, 2);
        assert_eq!(until, Some(now + READER_DEMAND_TTL));
    }

    /// The window is live strictly before its deadline and dead at it — the
    /// boundary decides whether an idle daemon resumes publishing.
    #[test]
    fn reader_demand_window_expires_at_its_deadline() {
        let now = Instant::now();
        let deadline = now + READER_DEMAND_TTL;

        assert!(reader_demand_is_live(Some(deadline), now));
        assert!(reader_demand_is_live(
            Some(deadline),
            deadline - Duration::from_millis(1)
        ));
        assert!(!reader_demand_is_live(Some(deadline), deadline));
        assert!(!reader_demand_is_live(None, now));
    }

    /// The window must outlast the publish cadence: a reader polling `.stats`
    /// once per `SNAPSHOT_INTERVAL` has to keep the engine publishing between
    /// two of its reads, otherwise a monitoring reader would see the engine
    /// fall idle between polls and its second read would be stale.  Slower
    /// readers are outside that guarantee by design — their staleness is
    /// bounded by their own polling interval (see [`READER_DEMAND_TTL`]).
    #[test]
    fn reader_demand_window_covers_the_publish_cadence() {
        assert!(READER_DEMAND_TTL >= 2 * SNAPSHOT_INTERVAL);
    }

    /// A mutating command always publishes — it changes what `.stats` reports —
    /// while a read command only refreshes the rate-limited snapshot for a
    /// watching reader: an unwatched read burst (`dd bs=1 count=N`) must not
    /// rebuild the piece grid every tick.
    #[test]
    fn read_commands_publish_only_for_a_watching_reader() {
        let early = SNAPSHOT_INTERVAL - Duration::from_millis(1);

        assert!(should_publish_after_command(false, false, Duration::ZERO));
        assert!(should_publish_after_command(false, true, early));
        assert!(!should_publish_after_command(
            true,
            false,
            SNAPSHOT_INTERVAL
        ));
        assert!(!should_publish_after_command(true, true, early));
        assert!(should_publish_after_command(true, true, SNAPSHOT_INTERVAL));
    }
}

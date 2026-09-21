//! `DownloadEngine` — single-owner-thread actor for the download subsystem.
//! The engine thread exclusively owns the libtorrent `Session`, all
//! `TorrentHandle`s, [`PieceStore`] (data plane) and [`PieceScheduler`]
//! (control plane). Callers send [`Command`]s over `mpsc` and receive on a
//! `sync_channel`, so raw libtorrent pointers never cross a thread boundary
//! (hence the dropped `unsafe impl Send`). Non-blocking `.stats` reads use a
//! shared [`DownloadSnapshot`] refreshed each tick, replacing the old big-lock
//! `try_lock`.
//!
//! A read that has to wait on the swarm never blocks the engine thread: it is
//! parked on [`EngineState::pending_reads`] and advanced one step per loop
//! iteration, so its peer/piece-wait window cannot serialize other commands.

use std::collections::HashMap;
use std::io::Write;
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
    /// Per-info_hash private flag. A torrent is "private" when
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
    /// Per-info_hash private flag. Populated at handle creation
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
    /// Piece-completion events forwarded by the alert consumer thread,
    /// drained by the engine loop and turned into piece registration.
    piece_finished_rx: mpsc::Receiver<(String, i32)>,
    /// Reads parked while they wait on the swarm.  The engine loop polls them
    /// once per iteration instead of blocking the engine thread inside a read,
    /// so one sourceless read cannot serialize every other command behind its
    /// peer/piece-wait window.
    pending_reads: Vec<PendingRead>,
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

        let read_timeout_secs = config.timeouts.resolved_read_timeout_secs();

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
                    metrics: thread_metrics,
                    snapshot: thread_snapshot,
                    stopping: thread_stopping,
                    alert_consumer: Some(alert_consumer),
                    piece_finished_rx,
                    pending_reads: Vec::new(),
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

    /// Non-blocking private-flag check from the last engine snapshot.
    /// Returns `Some(true)` if the torrent's info dict has
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

    /// Worst-case seconds a single `read_file_range` call may block its caller
    /// before returning its own result.  The FUSE deferred-read deadline must
    /// cover this budget (plus dispatch margin) so a ticket is never expired
    /// with ENODATA while the read is still legitimately waiting for a slow
    /// seeder.
    pub fn read_wait_budget_secs(&self) -> u64 {
        read_wait_budget_secs(self.read_timeout_secs)
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
const UPLOAD_MODE_FLAG: u64 = 1 << 1;

/// Upper bound (seconds) on the peer-discovery wait in the slow read path.
/// The engine's worst-case read budget sums the state-transition
/// wait, the recheck wait, this peer-discovery wait and the piece-wait window —
/// the FUSE deferred-read deadline must exceed that sum.
pub(crate) const PEER_WAIT_CAP_SECS: u64 = 9;

/// Upper bound (seconds) on the `force_recheck` wait in the stale-piece path.
/// It runs before peer discovery in the same read, so it adds to the read
/// budget.
pub(crate) const RECHECK_WAIT_CAP_SECS: u64 = 10;

/// Upper bound (seconds) on the piece-wait window for a no-seeder read
/// (`num_seeds == 0`) that has not yet exhausted peer discovery.  A no-seeder
/// read can never be served by the swarm, so it fails fast with `NoPeers`
/// after this short window rather than the full `read_timeout_secs`, which
/// would only tie up the caller and its FUSE deferred-read deadline — unless a
/// seeder connects mid-wait and upgrades the window back to the full timeout.
pub(crate) const NO_SEEDER_READ_TIMEOUT_SECS: u64 = 15;

/// Piece-wait window (seconds) for a read whose peer-discovery wait already
/// elapsed without a seeder.  Zero: the swarm probe (`force_reannounce` + up to
/// [`PEER_WAIT_CAP_SECS`]) already gave a seeder time to appear; finding none
/// means the read is sourceless, so the piece-wait loop fails on its first
/// iteration rather than spending the no-seeder window again.  A whole-file
/// `cat` fans out into one 128 KiB chunk read per command, each previously
/// paying its own 15s window.
pub(crate) const NO_SEEDER_FAST_FAIL_SECS: u64 = 0;

/// Worst-case seconds a single `read_file_range` call may block its caller (a
/// FUSE deferred-read worker) before returning its own result: the
/// state-transition wait (`read_timeout_secs`), the recheck wait (≤
/// [`RECHECK_WAIT_CAP_SECS`]), the peer-discovery wait (≤
/// [`PEER_WAIT_CAP_SECS`]) and the piece-wait window.  The piece-wait worst
/// case is a seeder connecting at the very end of the short no-seeder window
/// (≤ [`NO_SEEDER_READ_TIMEOUT_SECS`]) and then getting a full
/// `read_timeout_secs` window from its connect time (the window resets on
/// seeder connect — see [`EngineState::poll_piece_wait`]).
///
/// This is the budget the FUSE deferred-read deadline must cover.  Pure so it
/// is unit-testable without a running engine.
pub(crate) fn read_wait_budget_secs(read_timeout_secs: u64) -> u64 {
    let recheck_wait = std::cmp::min(read_timeout_secs, RECHECK_WAIT_CAP_SECS);
    let peer_wait = std::cmp::min(read_timeout_secs, PEER_WAIT_CAP_SECS);
    let no_seeder_wait = std::cmp::min(read_timeout_secs, NO_SEEDER_READ_TIMEOUT_SECS);
    read_timeout_secs
        .saturating_add(recheck_wait)
        .saturating_add(peer_wait)
        .saturating_add(no_seeder_wait)
        .saturating_add(read_timeout_secs)
}

/// Piece-wait window (seconds) for a single read: the full `read_timeout_secs`
/// when a seeder is connected (a slow-but-present seeder may still finish); the
/// short [`NO_SEEDER_READ_TIMEOUT_SECS`] cap when no seeder is connected and
/// peer discovery has not elapsed (leechers may still serve, or a seeder may
/// still connect); or zero ([`NO_SEEDER_FAST_FAIL_SECS`]) once peer discovery
/// elapsed with no seeder — the read is sourceless and fails fast instead of
/// tying up the caller and its FUSE deadline budget for a seeder the probe
/// proved cannot arrive.  Pure so it is unit-testable without a running engine.
fn piece_wait_window_secs(
    has_seeder: bool,
    is_peer_wait_exhausted: bool,
    read_timeout_secs: u64,
) -> u64 {
    if has_seeder {
        read_timeout_secs
    } else if is_peer_wait_exhausted {
        NO_SEEDER_FAST_FAIL_SECS
    } else {
        std::cmp::min(read_timeout_secs, NO_SEEDER_READ_TIMEOUT_SECS)
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

/// Format the stderr hint emitted when a read times out with zero connected
/// seeders.  The message describes the *current* swarm state at
/// timeout — a seeder that connected and left during the wait also lands here,
/// so it says "no seeder connected", not "no seeder ever connected".  The
/// daemon writes the line to its own stderr (operator-facing; a FUSE daemon
/// has no channel into the reading client's stderr), letting the operator
/// tell "no seeder" apart from "seeder slow" (`DownloadTimeout`).  `truncated`
/// marks the stale-piece path (a cached piece was purged or truncated and
/// needed re-download), so "truncated + no seeder" is distinguishable from a
/// plain cold read with no seeder.  Pure so the exact message is
/// unit-testable without a running engine.
pub(crate) fn no_seeder_stderr_hint(num_peers: i32, num_seeds: i32, truncated: bool) -> String {
    if truncated {
        format!(
            "no seeder connected (Peers:{} Seeds:{}, truncated piece re-download)",
            num_peers, num_seeds
        )
    } else {
        format!(
            "no seeder connected (Peers:{} Seeds:{})",
            num_peers, num_seeds
        )
    }
}

/// Format the `NoPeers` error returned when a piece-wait expires with zero
/// connected seeders.  `truncated` distinguishes the two failure contexts the
/// operator must tell apart: a plain cold read that simply has no seeder, vs.
/// a read that began with a truncated/purged cached piece whose re-download
/// (`force_recheck`) needs a seeder that is absent — the file cannot self-heal.
/// Pure so the exact message is unit-testable without a running engine.
pub(crate) fn no_peers_message(
    info_hash: &str,
    peer_wait_secs: u64,
    piece_wait_secs: u64,
    truncated: bool,
) -> String {
    if truncated {
        format!(
            "No seeder connected for info_hash {info_hash} after {:.0}s \
             peer discovery + {:.0}s piece wait. A truncated piece needs \
             re-download, but the torrent has no available seeder — the file \
             cannot self-heal until a seeder connects.",
            peer_wait_secs, piece_wait_secs
        )
    } else {
        format!(
            "No seeder connected for info_hash {info_hash} after {:.0}s \
             peer discovery + {:.0}s piece wait. The torrent has no available \
             seeder — check tracker health or try again later.",
            peer_wait_secs, piece_wait_secs
        )
    }
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
/// for per-torrent status/pieces and also drives the session-stats sample
/// request: each tick fires `post_session_stats`, whose alert
/// the consumer drains into the shared stats snapshot.
const SNAPSHOT_INTERVAL: Duration = Duration::from_secs(1);

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
    /// The peer-wait probe already re-announced once mid-window.
    reannounced_mid_wait: bool,
    /// Start of the piece-wait window; resets when a seeder connects.
    piece_wait_start: Instant,
    /// Actual peer-discovery wait (≤ [`PEER_WAIT_CAP_SECS`]) once the peer-wait
    /// phase completes, so the `NoPeers` message reports it even when the
    /// piece wait then fast-fails at zero seconds.
    peer_wait_elapsed: Duration,
    /// A stale (purged/truncated) cached piece was detected at read start, so
    /// the read entered the recheck path.  Surfaces in the `NoPeers` message
    /// to tell "truncated + no seeder" apart from a plain cold read.
    had_truncated_piece: bool,
    /// The recheck has been observed in a checking state (TOCTOU guard).
    saw_checking: bool,
    /// Id of the reader this read registered with the scheduler, once the
    /// priority gradient has been applied.  Released by id so a concurrent read
    /// on the same torrent never releases the wrong reader.
    reader_id: Option<ReadId>,
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
            reannounced_mid_wait: false,
            piece_wait_start: now,
            peer_wait_elapsed: Duration::ZERO,
            had_truncated_piece: false,
            saw_checking: false,
            reader_id: None,
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
    // Cache-metadata flush / idle-torrent settle cadence.  Tracked separately
    // from `last_publish` because the parked-read poll shortens the loop's wait
    // to `PENDING_READ_POLL_INTERVAL`: gating housekeeping on the publish timer
    // would then starve the flush for the whole duration of a parked read.
    let mut last_housekeeping = Instant::now();
    loop {
        // Poll parked reads on the short cadence so a waiting read is checked
        // at roughly the old inline piece-wait granularity; with none parked,
        // fall back to the snapshot interval.
        let wait = if state.pending_reads.is_empty() {
            SNAPSHOT_INTERVAL
        } else {
            PENDING_READ_POLL_INTERVAL
        };
        match rx.recv_timeout(wait) {
            Ok(cmd) => {
                let is_read = matches!(cmd, Command::ReadFileRange { .. });
                let stop = state.handle_command(cmd);
                state.drain_piece_finished();
                state.poll_pending_reads();
                if !state.pending_reads.is_empty() {
                    state.refresh_session_stats();
                }
                if !is_read || last_publish.elapsed() >= SNAPSHOT_INTERVAL {
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
                    state.settle_idle_torrents();
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
                    state.settle_idle_torrents();
                    state.flush_cache_metadata();
                    last_housekeeping = Instant::now();
                }
                state.drain_piece_finished();
                state.poll_pending_reads();
                if housekeeping || !state.pending_reads.is_empty() {
                    state.refresh_session_stats();
                }
                if last_publish.elapsed() >= SNAPSHOT_INTERVAL {
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
        // Keep the flag so a later `NoPeers` failure can say "truncated piece
        // needs re-download" rather than a plain "no seeder" — the operator
        // must tell the two apart.
        let had_truncated_piece = self.has_stale_pieces(
            &read.info_hash,
            read.start_piece,
            read.end_piece,
            read.piece_length,
            read.num_pieces,
            read.total_size,
        );
        if had_truncated_piece {
            read.had_truncated_piece = true;
            tracing::info!(
                "read_file_range: stale libtorrent piece state detected for \
                 info_hash={}, forcing recheck to clear bits for pieces {}-{}",
                read.info_hash,
                read.start_piece,
                read.end_piece
            );
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
                return self.begin_waiting(read);
            }
            read.phase = ReadPhase::Recheck;
            read.phase_start = Instant::now();
            read.phase_deadline = read.phase_start
                + Duration::from_secs(std::cmp::min(self.read_timeout_secs, RECHECK_WAIT_CAP_SECS));
            read.saw_checking = false;
            return None;
        }
        self.begin_waiting(read)
    }

    /// Serve a read from local pieces when possible; otherwise apply the reader
    /// priority gradient and park it in a swarm-wait phase.  Returns
    /// `Some(result)` when the read finished here.
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
        // from the peers it is already connected to.
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
        if swarm_is_empty(read.status.num_peers, read.status.num_seeds) {
            self.enter_peer_wait(read);
        } else {
            self.enter_piece_wait(read);
        }
        None
    }

    /// Park a read in the peer-discovery phase: the swarm looked empty, so kick
    /// an immediate re-announce and give peers a bounded window to appear.
    fn enter_peer_wait(&mut self, read: &mut PendingRead) {
        if let Some(handle) = self.handles.get(&read.info_hash) {
            if !handle.force_reannounce() {
                tracing::debug!(
                    "read_file_range {}: force_reannounce rejected (non-fatal)",
                    read.info_hash
                );
            }
        }
        read.phase = ReadPhase::PeerWait;
        read.phase_start = Instant::now();
        read.phase_deadline = read.phase_start
            + Duration::from_secs(std::cmp::min(self.read_timeout_secs, PEER_WAIT_CAP_SECS));
        read.reannounced_mid_wait = false;
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
            return self.begin_waiting(read);
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
                    self.begin_waiting(read)
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
                    self.begin_waiting(read)
                } else {
                    None
                }
            }
            // `status()` failed — proceed, as the inline wait did.
            _ => self.begin_waiting(read),
        }
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
            read.peer_wait_elapsed = Instant::now().saturating_duration_since(read.phase_start);
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
        // more announce before the piece deadline path takes over.
        let window = read
            .phase_deadline
            .saturating_duration_since(read.phase_start);
        if !read.reannounced_mid_wait && read.phase_start.elapsed() >= window / 2 {
            read.reannounced_mid_wait = true;
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
            // `have_piece` can be stale (resume state marks the piece complete,
            // but the file was purged or truncated); in that case do not treat
            // it as ready — fall through to the download path so libtorrent
            // re-requests the piece.
            let have_valid = if have {
                let piece_key = PieceStore::piece_key(&read.info_hash, piece_idx);
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    read.piece_length,
                    read.num_pieces,
                    read.total_size,
                );
                !self.store.has_stale_piece(&piece_key, expected)
            } else {
                false
            };
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
                if have_valid {
                    self.register_piece(
                        &read.info_hash,
                        piece_idx,
                        read.piece_length,
                        read.num_pieces,
                        read.total_size,
                    );
                }
                read.current_piece += 1;
                continue;
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
            ));
            if read.piece_wait_start.elapsed() >= window {
                return Some(self.read_timed_out(read, piece_idx, window));
            }
            return None;
        }
    }

    /// Resolve a read whose piece-wait window elapsed: return the contiguous
    /// prefix of completed pieces when there is one, otherwise the
    /// `NoPeers`/`Timeout` error that distinguishes "no seeder" from "slow".
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

        // Distinguish "no seeder" from "slow download": zero connected seeders
        // → `NoPeers`; seeders present but slow → `Timeout`.  If status is
        // unavailable (handle gone, `status()` failed), fall back to `Timeout`
        // — don't fabricate a zero-seeder swarm and mislead the user into
        // checking tracker health for a stale handle.
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
        if num_seeds == 0 {
            // A zero-seeder read has no seeder to serve it.  Write a one-line
            // hint to the daemon's own stderr (operator-facing; FUSE has no
            // channel into the client's stderr) via a direct, non-panicking
            // write: `eprintln!` panics on a broken stderr (aborting this
            // thread), and `tracing` writes to stdout, not stderr.
            let _ = writeln!(
                std::io::stderr(),
                "{}",
                no_seeder_stderr_hint(num_peers, num_seeds, read.had_truncated_piece)
            );
            return Err(TorrentError::NoPeers(no_peers_message(
                &read.info_hash,
                read.peer_wait_elapsed.as_secs(),
                window.as_secs(),
                read.had_truncated_piece,
            )));
        }
        Err(TorrentError::Timeout(format!(
            "Timed out waiting for piece {} after {:.0}s. \
             Torrent progress: {:.2}%",
            piece_idx,
            window.as_secs(),
            progress,
        )))
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
            // use the unified stale detection — `have_piece` true
            // but the on-disk file is gone or truncated means the bit is
            // stale; the piece is NOT available locally and must be
            // re-downloaded.
            let have = handle.have_piece(piece_idx);
            let have_valid = if have {
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    piece_length,
                    num_pieces,
                    total_size,
                );
                !self.store.has_stale_piece(&piece_key, expected)
            } else {
                false
            };
            if !have_valid && !complete {
                return false;
            }
        }
        true
    }

    /// detect whether any piece in the range has a stale
    /// libtorrent bitmask — `have_piece == true` but the on-disk piece is
    /// missing or shorter than its expected size (purged by cache
    /// verification / manual `delete_piece`, or truncated externally).
    /// Returns `true` if at least one such piece exists.
    fn has_stale_pieces(
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
            if handle.have_piece(piece_idx) {
                let piece_key = PieceStore::piece_key(info_hash, piece_idx);
                let expected = PieceStore::expected_piece_size(
                    piece_idx,
                    piece_length,
                    num_pieces,
                    total_size,
                );
                if self.store.has_stale_piece(&piece_key, expected) {
                    return true;
                }
            }
        }
        false
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
            // the piece is being served from the local disk. If it is
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
        // Restore idle upload_mode when the reader count and the wanted set
        // both reach zero. `reader_released` is infallible, so this decision
        // does not depend on a recompute result.
        self.maybe_restore_upload_mode(info_hash);
    }

    /// Restore idle `upload_mode` once a torrent has converged (no reader, no
    /// wanted piece).  Called both on reader release and on the periodic
    /// engine tick so convergence is also caught when the last wanted piece
    /// becomes ready outside a reader release (e.g. a later cached read).
    fn maybe_restore_upload_mode(&mut self, info_hash: &str) {
        if !self.scheduler.is_idle(info_hash) {
            return;
        }
        if let Some(handle) = self.handles.get(info_hash) {
            if !handle.set_flags(UPLOAD_MODE_FLAG) {
                tracing::warn!(
                    "maybe_restore_upload_mode: failed to re-enable upload_mode for {}",
                    info_hash
                );
            }
        }
    }

    /// On each engine tick, restore `upload_mode` for any torrent that has
    /// converged to idle since its last read.  Without this, a read-ahead
    /// window completed entirely through later cached reads would leave the
    /// handle in download mode indefinitely (fast-path reads skip
    /// `release_reader`).
    fn settle_idle_torrents(&mut self) {
        let idle: Vec<String> = self
            .handles
            .keys()
            .filter(|ih| self.scheduler.is_idle(ih.as_str()))
            .cloned()
            .collect();
        for info_hash in idle {
            self.maybe_restore_upload_mode(&info_hash);
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
    fn publish_snapshot(&self) {
        let mut statuses = HashMap::new();
        let mut pieces = HashMap::new();
        // request libtorrent to refresh per-torrent statistics
        // before reading status. Without this, `status().num_peers` can
        // return 0 even when peers are connected — the internal peer list
        // is only refreshed on session tick or post_torrent_updates.
        self.session.post_torrent_updates();
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
        no_peers_message, no_seeder_stderr_hint, partial_read_bounds, piece_wait_window_secs,
        read_wait_budget_secs, swarm_is_empty, NO_SEEDER_FAST_FAIL_SECS,
        NO_SEEDER_READ_TIMEOUT_SECS,
    };
    use crate::infrastructure::config::DEFAULT_READ_TIMEOUT_SECS;

    /// the no-seeder stderr hint must use the exact message the
    /// operator greps for — `no seeder connected (Peers:N Seeds:M)` with the
    /// live peer/seed counts — so a currently-empty swarm is distinguishable
    /// from "seeder slow" (which surfaces as `DownloadTimeout`, not this hint).
    #[test]
    fn no_seeder_hint_reports_live_counts() {
        assert_eq!(
            no_seeder_stderr_hint(0, 0, false),
            "no seeder connected (Peers:0 Seeds:0)"
        );
        // Peers may be non-zero (leechers without the piece) while seeds stay 0.
        assert_eq!(
            no_seeder_stderr_hint(3, 0, false),
            "no seeder connected (Peers:3 Seeds:0)"
        );
        // The truncated stale-piece path appends a marker so the operator can
        // tell "truncated + no seeder" apart from a plain no-seeder read.
        assert_eq!(
            no_seeder_stderr_hint(0, 0, true),
            "no seeder connected (Peers:0 Seeds:0, truncated piece re-download)"
        );
    }

    /// the `NoPeers` message must distinguish a plain cold read with no seeder
    /// from a truncated/purged piece whose re-download needs a seeder that is
    /// absent — the two failure contexts a reader can't otherwise tell apart
    /// from the bare `ENODATA` the FUSE layer returns.
    #[test]
    fn no_peers_message_distinguishes_truncated_context() {
        let plain = no_peers_message("abc", 9, 0, false);
        let truncated = no_peers_message("abc", 9, 0, true);
        assert!(plain.contains("check tracker health"));
        assert!(!plain.contains("truncated"));
        assert!(truncated.contains("truncated"));
        assert!(truncated.contains("self-heal"));
        assert_ne!(plain, truncated);
    }

    /// the read budget must cover all five synchronous phases —
    /// state-transition wait + recheck wait (capped) + peer-discovery wait
    /// (capped) + no-seeder wait (capped) + piece wait (full window, reset on
    /// seeder connect) — so the FUSE deferred-read deadline never expires a
    /// ticket while the engine is still legitimately waiting.
    #[test]
    fn budget_covers_all_slow_path_phases() {
        // Default read_timeout_secs = 60 (DEFAULT_READ_TIMEOUT_SECS):
        //   60 (state) + 10 (recheck cap) + 9 (peer cap) + 15 (no-seeder cap)
        //   + 60 (piece) = 154s.
        assert_eq!(read_wait_budget_secs(DEFAULT_READ_TIMEOUT_SECS), 154);
        // Short timeout still caps every phase at the timeout itself.
        assert_eq!(read_wait_budget_secs(4), 4 + 4 + 4 + 4 + 4);
        // Timeout below every cap.
        assert_eq!(read_wait_budget_secs(2), 2 + 2 + 2 + 2 + 2);
    }

    #[test]
    fn budget_exceeds_legacy_deadline_for_default_timeout() {
        // The old FUSE deadline was `read_timeout_secs + 5` — shorter than
        // the engine's worst-case budget and even its peer-wait+piece-wait
        // path. At the default timeout the budget (154s) still exceeds the
        // legacy deadline (65s), so a slow seeder is never expired early.
        assert!(read_wait_budget_secs(DEFAULT_READ_TIMEOUT_SECS) > DEFAULT_READ_TIMEOUT_SECS + 5);
    }

    /// a no-seeder read must never wait the full read timeout — it caps at
    /// [`NO_SEEDER_READ_TIMEOUT_SECS`], so the engine thread is not blocked for
    /// the whole `read_timeout_secs` and concurrent healthy reads on the same
    /// mount are not serialized behind a dead torrent.
    #[test]
    fn no_seeder_piece_wait_caps_at_short_timeout() {
        assert_eq!(
            piece_wait_window_secs(false, false, DEFAULT_READ_TIMEOUT_SECS),
            NO_SEEDER_READ_TIMEOUT_SECS
        );
        assert_eq!(
            piece_wait_window_secs(false, false, 120),
            NO_SEEDER_READ_TIMEOUT_SECS
        );
        // A read_timeout below the cap still bounds the window at itself.
        assert_eq!(piece_wait_window_secs(false, false, 3), 3);
    }

    /// a sourceless read (peer discovery already elapsed with no seeder) must
    /// not spend the no-seeder window again — it fails fast so the engine
    /// thread is freed for healthy reads instead of re-blocking per chunk.
    #[test]
    fn peer_wait_exhausted_no_seeder_fails_fast() {
        assert_eq!(
            piece_wait_window_secs(false, true, DEFAULT_READ_TIMEOUT_SECS),
            NO_SEEDER_FAST_FAIL_SECS
        );
        // A seeder connected mid-wait still gets the full timeout, even when
        // peer discovery had previously elapsed.
        assert_eq!(
            piece_wait_window_secs(true, true, DEFAULT_READ_TIMEOUT_SECS),
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

    /// a read with a connected seeder uses the full window so a
    /// slow-but-present seeder is not failed fast.
    #[test]
    fn seeder_piece_wait_uses_full_timeout() {
        assert_eq!(
            piece_wait_window_secs(true, false, DEFAULT_READ_TIMEOUT_SECS),
            DEFAULT_READ_TIMEOUT_SECS
        );
        assert_eq!(piece_wait_window_secs(true, false, 3), 3);
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
}

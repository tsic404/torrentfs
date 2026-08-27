//! libtorrent alert dispatching for the download engine.
//!
//! A dedicated `AlertConsumer` thread drains libtorrent alerts event-driven
//! (design §4.2): it registers a `set_alert_notify` callback that signals a
//! condvar whenever the alert queue transitions 0→1, so the consumer blocks
//! instead of polling a fixed interval.  Alert processing is:
//! - `session_stats_alert` → updates the shared session-stats snapshot
//! - `torrent_finished_alert` / `torrent_removed_alert` → logged
//! - `read_piece_alert` and others → logged (piece data is read from disk)
//!
//! Only the consumer thread ever calls `pop_alerts`, so it is the single
//! owner of the alert queue and the `set_alert_notify` 0→1 semantics hold.
//! Shutdown ordering (TSI-2244): `stop()` joins the consumer thread *before*
//! unregistering the notify hook, so `set_alert_notify(None)` never races
//! with a concurrent `pop_alerts` on the same session.

use std::os::raw::c_void;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::infrastructure::download::SessionStats;
use crate::infrastructure::metrics::Metrics;

// ── Shared session stats ────────────────────────────────────

/// Shared session statistics updated by the alert consumer thread and read by
/// `.stats` generation without blocking on libtorrent FFI.
#[derive(Debug, Clone)]
pub struct SharedSessionStats {
    pub inner: Arc<Mutex<SessionStats>>,
}

impl SharedSessionStats {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(SessionStats::default())),
        }
    }

    pub fn snapshot(&self) -> SessionStats {
        self.inner.lock().map(|g| g.clone()).unwrap_or_default()
    }
}

impl Default for SharedSessionStats {
    fn default() -> Self {
        Self::new()
    }
}

// ── Alert types matching the C FFI enum ─────────────────────

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) enum AlertType {
    ReadPiece,
    SessionStats,
    TorrentFinished,
    TorrentRemoved,
    Other(i32),
}

impl From<i32> for AlertType {
    fn from(value: i32) -> Self {
        match value {
            // Patterns bind to the bindgen-generated constants of
            // `lt_alert_type_t` (libtorrent_wrapper.h), not hand-written
            // literals. TSI-2145 drifted this mapping to 0-based and every
            // alert was dispatched one slot early — `session_stats_alert`
            // never reached the `SessionStats` branch, leaving root
            // `.stats` Total DL/UL at zero (TSI-2344). Deriving from the
            // generated constants makes any future header enum change a
            // compile error here instead of silent runtime misdispatch.
            x if x == libtorrent_sys::lt_alert_type_t_LT_ALERT_READ_PIECE as i32 => {
                AlertType::ReadPiece
            }
            x if x == libtorrent_sys::lt_alert_type_t_LT_ALERT_SESSION_STATS as i32 => {
                AlertType::SessionStats
            }
            x if x == libtorrent_sys::lt_alert_type_t_LT_ALERT_TORRENT_FINISHED as i32 => {
                AlertType::TorrentFinished
            }
            x if x == libtorrent_sys::lt_alert_type_t_LT_ALERT_TORRENT_REMOVED as i32 => {
                AlertType::TorrentRemoved
            }
            other => AlertType::Other(other),
        }
    }
}

// ── Alert notify (wakeup) machinery ─────────────────────────

/// Safety-net timeout for the condvar wait. The notify callback is the
/// primary wakeup; this timeout only guards against a missed wakeup in the
/// narrow window between an empty `pop_alerts` and entering the wait. It is
/// deliberately generous (not a poll interval) so an idle consumer sleeps.
const SAFETY_NET_TIMEOUT: Duration = Duration::from_secs(1);

/// Shared state signaled by the C alert-notify callback and waited on by the
/// consumer thread. `flag` closes the missed-wakeup race: the callback sets
/// it before `notify_one`, and the consumer clears it under the mutex before
/// deciding whether to block.
struct NotifyState {
    flag: AtomicBool,
    cv: Condvar,
    mutex: Mutex<()>,
}

impl NotifyState {
    /// Block until a notify arrives or `timeout` elapses.
    ///
    /// Returns `true` if a notify was observed (the flag was set by the
    /// callback, either before this call or via `notify_one` while blocked),
    /// `false` if the wait timed out.
    fn wait(&self, timeout: Duration) -> bool {
        let guard = self
            .mutex
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if self.flag.swap(false, Ordering::SeqCst) {
            // A callback already fired before we blocked — don't block.
            return true;
        }
        let (_guard, timed_out) = self
            .cv
            .wait_timeout(guard, timeout)
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        self.flag.store(false, Ordering::SeqCst);
        !timed_out.timed_out()
    }
}

/// libtorrent alert-notify callback. Invoked on libtorrent's internal
/// thread(s) whenever the alert queue goes 0→1. Must be non-blocking and must
/// not touch the session — it only flags and notifies the consumer.
unsafe extern "C" fn notify_cb(user_data: *mut c_void) {
    // SAFETY: `user_data` is `Arc::as_ptr(&notify)` and remains valid for as
    // long as the session is alive (the callback is unregistered before the
    // shared state is freed; see `AlertConsumer::stop`).
    let state = &*(user_data as *const NotifyState);
    state.flag.store(true, Ordering::SeqCst);
    state.cv.notify_one();
}

// ── AlertConsumer ───────────────────────────────────────────

/// Background thread that continuously pops and dispatches libtorrent alerts.
/// Woken by the libtorrent `set_alert_notify` callback instead of a fixed
/// poll interval.
pub struct AlertConsumer {
    handle: Option<JoinHandle<()>>,
    stop_flag: Arc<AtomicBool>,
    /// Raw session pointer — kept only to unregister the notify callback in
    /// [`AlertConsumer::stop`], which is always called while the session is
    /// still alive.
    session: libtorrent_sys::lt_session_t,
    notify: Arc<NotifyState>,
}

impl AlertConsumer {
    /// Spawn a background alert consumer thread.
    ///
    /// # Safety
    ///
    /// - `session` must be a valid libtorrent session pointer that remains
    ///   alive until [`AlertConsumer::stop`] is called (which unregisters the
    ///   notify callback).
    /// - `stats`: shared session-stats snapshot updated on `session_stats_alert`.
    /// - `metrics`: shared observability counters.
    pub unsafe fn spawn(
        session: libtorrent_sys::lt_session_t,
        stats: SharedSessionStats,
        metrics: Arc<Metrics>,
    ) -> Self {
        let notify = Arc::new(NotifyState {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            mutex: Mutex::new(()),
        });

        // Register the non-blocking notify hook. It fires on libtorrent's
        // thread(s) whenever the alert queue goes 0→1 and only signals this
        // consumer — all alert draining stays on the consumer thread.
        unsafe {
            libtorrent_sys::lt_session_set_alert_notify(
                session,
                Some(notify_cb),
                Arc::as_ptr(&notify) as *mut c_void,
            );
        }

        let stop_flag = Arc::new(AtomicBool::new(false));
        let stop = stop_flag.clone();
        // Cast to usize to work around `*mut c_void: !Send`. The pointer is
        // valid for the session's lifetime, and `stop()` (called before the
        // session is dropped) unregisters the notify hook.
        let session_ptr = session as usize;
        let notify_thread = notify.clone();

        let handle = thread::Builder::new()
            .name("alert-consumer".into())
            .spawn(move || {
                tracing::info!("Alert consumer started (event-driven via set_alert_notify)");
                let session = session_ptr as libtorrent_sys::lt_session_t;
                loop {
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }

                    if drain_alerts(session, &stats, &metrics) {
                        // Drained some alerts; loop again to drain any that
                        // arrived while we were dispatching before blocking.
                        continue;
                    }

                    // Queue empty — block until the notify callback fires or
                    // the safety-net timeout elapses.
                    let _ = notify_thread.wait(SAFETY_NET_TIMEOUT);
                }
                tracing::info!("Alert consumer stopping");
            })
            .expect("Failed to spawn alert-consumer thread");

        Self {
            handle: Some(handle),
            stop_flag,
            session,
            notify,
        }
    }

    /// Stop the alert consumer and unregister the notify callback.
    ///
    /// Must be called while the session is still alive. Ordering is critical
    /// to eliminate the FFI race with `drain_alerts` (issue TSI-2244):
    /// 1. Set the stop flag and notify the condvar — wakes the consumer.
    /// 2. Join the consumer thread — once joined, no `pop_alerts` FFI call is
    ///    in flight, so `set_alert_notify(None)` cannot race with it.
    /// 3. Unregister the notify hook — safe because the consumer is gone and
    ///    the session is still alive.
    ///
    /// `Drop` only does steps 1–2 (defensive); it cannot do step 3 because the
    /// session may already be destroyed by the time `Drop` runs.
    pub fn stop(&mut self) {
        // 1. Signal the consumer thread to exit.
        self.stop_flag.store(true, Ordering::Relaxed);
        self.notify.cv.notify_all();

        // 2. Join the consumer thread so no `pop_alerts` FFI call is in flight.
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }

        // 3. Unregister the notify hook. No concurrent `pop_alerts` is possible
        //    because the consumer thread has exited. The session is still alive
        //    (caller's contract), so the FFI call is safe.
        unsafe {
            libtorrent_sys::lt_session_set_alert_notify(self.session, None, std::ptr::null_mut());
        }
    }
}

impl Drop for AlertConsumer {
    fn drop(&mut self) {
        // Do not unregister the notify hook here: the session may already be
        // destroyed by the time this runs — `stop()` unregisters it while the
        // session is still alive (the caller must invoke `stop()` first). Just
        // wake the consumer thread and join it.
        self.stop_flag.store(true, Ordering::Relaxed);
        self.notify.cv.notify_all();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

// ── Drain + dispatch ────────────────────────────────────────

/// Drain all pending libtorrent alerts and dispatch them.
///
/// Returns `true` when at least one alert was drained, `false` when the queue
/// was empty. Must only be called from the alert consumer thread.
fn drain_alerts(
    session: libtorrent_sys::lt_session_t,
    stats: &SharedSessionStats,
    metrics: &Metrics,
) -> bool {
    let list = unsafe { libtorrent_sys::lt_session_pop_alerts(session) };
    if list.is_null() {
        return false;
    }

    let count = unsafe { (*list).count };
    let alerts = unsafe { (*list).alerts };

    if !alerts.is_null() && count > 0 {
        for i in 0..count {
            let alert = unsafe { &*alerts.add(i as usize) };
            let alert_type = AlertType::from(alert.type_);
            dispatch(alert_type, alert, stats, metrics);
        }
    }

    unsafe { libtorrent_sys::lt_alert_list_destroy(list) };
    true
}

pub(crate) fn dispatch(
    alert_type: AlertType,
    alert: &libtorrent_sys::lt_alert_data_t,
    stats: &SharedSessionStats,
    _metrics: &Metrics,
) {
    match alert_type {
        AlertType::ReadPiece => {
            let info_hash = cstr(&alert.info_hash);
            tracing::debug!(
                "alert: read_piece (info_hash={}, piece={}, size={}, error={})",
                info_hash,
                alert.piece_index,
                alert.piece_data_size,
                alert.error_code
            );
        }
        AlertType::SessionStats => {
            if let Ok(mut guard) = stats.inner.lock() {
                guard.download_rate = alert.download_rate;
                guard.upload_rate = alert.upload_rate;
                guard.total_downloaded = alert.total_downloaded;
                guard.total_uploaded = alert.total_uploaded;
                guard.dht_nodes = alert.dht_nodes;
                guard.peers_connected = alert.peers_connected;
                guard.half_open_connections = alert.half_open_connections;
            }
            tracing::trace!(
                "alert: session_stats (dl={}, ul={}, peers={}, dht={})",
                alert.download_rate,
                alert.upload_rate,
                alert.peers_connected,
                alert.dht_nodes
            );
        }
        AlertType::TorrentFinished => {
            let info_hash = cstr(&alert.info_hash);
            // TSI-2467: With selective piece priorities (priority 0 for
            // non-read-range pieces), libtorrent fires torrent_finished
            // prematurely when all non-filtered pieces in the current read
            // range have passed hash check. This is expected behavior —
            // close_redundant_connections is disabled to prevent seed peer
            // disconnection, and the next read's priority changes trigger
            // resume_download() to return to downloading state.
            tracing::debug!(
                "alert: torrent_finished (info_hash={}, \
                 likely premature — selective download with filtered pieces)",
                info_hash
            );
        }
        AlertType::TorrentRemoved => {
            let info_hash = cstr(&alert.info_hash);
            tracing::info!("alert: torrent_removed (info_hash={})", info_hash);
        }
        AlertType::Other(category) => {
            let msg = cstr_ptr(alert.message);
            tracing::debug!("alert: category={}, message={}", category, msg);
        }
    }
}

/// Read a null-terminated fixed-size char array as a &str.
/// Generic over T to support both `[i8; 41]` (amd64) and `[u8; 41]` (arm64)
/// due to platform-specific C `char` signedness.
fn cstr<T>(buf: &[T; 41]) -> &str {
    let bytes: &[u8] = unsafe { std::slice::from_raw_parts(buf.as_ptr() as *const u8, 41) };
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(41);
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// Read a nullable C string pointer, returning an owned `String`.
fn cstr_ptr(ptr: *const std::os::raw::c_char) -> String {
    if ptr.is_null() {
        return String::new();
    }
    unsafe { std::ffi::CStr::from_ptr(ptr) }
        .to_str()
        .unwrap_or("")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    fn new_state() -> Arc<NotifyState> {
        Arc::new(NotifyState {
            flag: AtomicBool::new(false),
            cv: Condvar::new(),
            mutex: Mutex::new(()),
        })
    }

    #[test]
    fn notify_cb_sets_flag() {
        let state = new_state();
        unsafe { notify_cb(Arc::as_ptr(&state) as *mut c_void) };
        assert!(state.flag.load(Ordering::SeqCst));
    }

    #[test]
    fn wait_returns_immediately_when_already_flagged() {
        // Simulate a callback firing before the consumer blocks (the
        // missed-wakeup race). The flag must make `wait` return at once
        // instead of blocking for the full timeout.
        let state = new_state();
        state.flag.store(true, Ordering::SeqCst);
        let start = Instant::now();
        assert!(state.wait(Duration::from_secs(60)));
        assert!(
            start.elapsed() < Duration::from_millis(500),
            "wait should return immediately when the flag is already set"
        );
    }

    #[test]
    fn wait_times_out_when_not_notified() {
        let state = new_state();
        let start = Instant::now();
        assert!(!state.wait(Duration::from_millis(50)));
        assert!(
            start.elapsed() >= Duration::from_millis(50),
            "wait should block for at least the timeout when not notified"
        );
    }

    #[test]
    fn alert_type_mapping_matches_ffi_enum() {
        // TSI-2344: the mapping must dispatch on the bindgen-generated
        // `lt_alert_type_t` constants (libtorrent_wrapper.h). A previous
        // off-by-one sent `session_stats_alert` to `TorrentFinished`, so
        // the shared session-stats snapshot never updated and root `.stats`
        // showed Total DL/UL = 0 B.
        assert!(matches!(
            AlertType::from(libtorrent_sys::lt_alert_type_t_LT_ALERT_READ_PIECE as i32),
            AlertType::ReadPiece
        ));
        assert!(matches!(
            AlertType::from(libtorrent_sys::lt_alert_type_t_LT_ALERT_SESSION_STATS as i32),
            AlertType::SessionStats
        ));
        assert!(matches!(
            AlertType::from(libtorrent_sys::lt_alert_type_t_LT_ALERT_TORRENT_FINISHED as i32),
            AlertType::TorrentFinished
        ));
        assert!(matches!(
            AlertType::from(libtorrent_sys::lt_alert_type_t_LT_ALERT_TORRENT_REMOVED as i32),
            AlertType::TorrentRemoved
        ));
    }

    #[test]
    fn alert_type_other_fallback_preserves_value() {
        // Unmapped values (e.g. LT_ALERT_OTHER = 99) must round-trip into
        // `Other` with their raw value intact.
        let other = libtorrent_sys::lt_alert_type_t_LT_ALERT_OTHER as i32;
        match AlertType::from(other) {
            AlertType::Other(v) => assert_eq!(v, other),
            other_variant => panic!("expected Other({other}), got {other_variant:?}"),
        }
    }

    #[test]
    fn session_stats_alert_updates_shared_snapshot() {
        // TSI-2344 integration-level assertion: a `session_stats_alert`
        // (type = LT_ALERT_SESSION_STATS) flowing through dispatch must make
        // the shared snapshot readable with non-zero values — this is what
        // root `.stats` Global Rates renders. The engine's per-tick
        // `post_session_stats` request is the alert's producer.
        let shared = SharedSessionStats::new();
        let metrics = Metrics::new();
        let alert = libtorrent_sys::lt_alert_data_t {
            type_: libtorrent_sys::lt_alert_type_t_LT_ALERT_SESSION_STATS as i32,
            info_hash: [0; 41],
            piece_index: 0,
            error_code: 0,
            piece_data: std::ptr::null_mut(),
            piece_data_size: 0,
            download_rate: 2048,
            upload_rate: 1024,
            total_downloaded: 439_871_233, // ≈ 419 MB, as observed in QA
            total_uploaded: 4096,
            dht_nodes: 300,
            peers_connected: 7,
            half_open_connections: 2,
            message: std::ptr::null(),
            category: 0,
        };
        dispatch(AlertType::from(alert.type_), &alert, &shared, &metrics);

        let ss = shared.snapshot();
        assert_eq!(ss.total_downloaded, 439_871_233);
        assert!(ss.total_downloaded > 0, "snapshot must read non-zero DL");
        assert_eq!(ss.download_rate, 2048);
        assert_eq!(ss.upload_rate, 1024);
        assert_eq!(ss.dht_nodes, 300);
        assert_eq!(ss.peers_connected, 7);
    }
}

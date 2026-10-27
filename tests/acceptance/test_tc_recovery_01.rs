//! Design acceptance case TC-recovery-01: unmount → remount with the same
//! state database keeps the previously ingested torrent visible with unchanged
//! structure, permissions and sizes (`docs/03-acceptance.md` §I AC-80).
//!
//! The stop half is the point of this carrier: the daemon is signalled while a
//! read is still in flight — the torrent's swarm is empty, so the read is
//! parked in the peer-discovery wait — and must still finish C-9.2.2's order
//! (`docs/02-contracts.md`) inside the stop window a container runtime allows
//! (`docker stop` defaults to 10s) with exit code 0 and no FUSE mount left
//! behind.  A daemon that blocks there is SIGKILLed, and the severed mount is
//! what makes the next `docker start` fail — i.e. the remount half of AC-80
//! never gets to run.
//!
//! The in-flight read alone does not bound the stop: the engine parks reads on
//! its own loop, so it returns to the stop flag and exits quickly.  The
//! incident's thread was wedged inside a libtorrent call that never reaches the
//! flag, so the second case freezes the engine thread with `ptrace` and asserts
//! the teardown still gives up on it at its bound, unmounts and exits 0.  That
//! is the observation an unbounded join cannot make.

use crate::common::{
    daemon_config, fuse_mount_blocker, ingest_torrent_through_mount, mountinfo_fuse_count,
    unique_torrent, wait_for_path, wait_for_projection, wave_slot, Daemon, MountGate, TestHarness,
    DAEMON_MOUNT_TIMEOUT,
};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};
use torrentfs::infrastructure::download::ENGINE_THREAD_NAME;

pub const TC_ID: &str = "TC-recovery-01";

/// Container stop window (`docker stop` default).  The graceful shutdown must
/// complete inside it, or the runtime escalates to SIGKILL.
const STOP_WINDOW: Duration = Duration::from_secs(10);
/// The teardown's report that it gave up on the engine thread at its bound
/// (`ENGINE_STOP_BUDGET` in `src/main.rs`, which is a binary-private constant;
/// the log line is the observable).
const ENGINE_ABANDON_MARKER: &str = "download engine thread still running";
/// Parsing, persisting and projecting one fixture torrent.
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// Waiting for a read to be parked in the engine (the log line that proves it).
const PARK_TIMEOUT: Duration = Duration::from_secs(60);

/// The projection's observable facts, taken by direct lookup.
///
/// The fixture is a single-file torrent, so its whole projected tree is the
/// torrent directory plus one file — direct lookups cover the entry set AC-80
/// freezes, where a READDIR walk would also race the temporary listing of
/// C-2.5.3 (a directory listing can still answer with the pre-persist view
/// while a lookup already resolves the settled tree).
#[derive(Debug, PartialEq, Eq)]
struct ProjectedTree {
    dir_mode: u32,
    file_mode: u32,
    file_size: u64,
}

fn projected_tree(torrent_dir: &Path, data_file: &Path) -> Option<ProjectedTree> {
    let dir = fs::symlink_metadata(torrent_dir).ok()?;
    let file = fs::symlink_metadata(data_file).ok()?;
    Some(ProjectedTree {
        dir_mode: dir.permissions().mode() & 0o777,
        file_mode: file.permissions().mode() & 0o777,
        file_size: file.len(),
    })
}

/// Wait until `dir`'s READDIR lists `name`.
///
/// A freshly ingested torrent is listed in a temporary projection before the
/// background persist lands (C-2.5.3): a direct LOOKUP can already resolve the
/// file while READDIR still serves the empty temporary listing.  The snapshot
/// and the in-flight read below both need the settled tree, so the case waits
/// for the listing itself instead of inferring it from `exists()`.
fn wait_for_listed_entry(dir: &Path, name: &str, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .any(|entry| entry.file_name().to_string_lossy() == name)
            })
            .unwrap_or(false)
        {
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    false
}

/// Wait until the engine reports one more parked read than `seen_so_far` (the
/// in-flight request this case needs at signal time), or panic with the
/// daemon's output.  Counting rather than matching keeps an earlier deferral
/// (from the mount's own setup reads) from being mistaken for this one.
fn wait_for_new_parked_read(
    daemon: &Daemon,
    seen_so_far: usize,
    reader_done: &std::sync::mpsc::Receiver<std::io::Result<Vec<u8>>>,
) {
    let deadline = Instant::now() + PARK_TIMEOUT;
    while Instant::now() < deadline {
        if daemon.output().matches("Deferred read queued").count() > seen_so_far {
            return;
        }
        // A read that finishes instead of parking means the swarm was not empty
        // or the range was served locally: the case's premise did not hold, and
        // waiting out the timeout would hide the reason.
        if let Ok(outcome) = reader_done.try_recv() {
            panic!(
                "the read returned before it could be in flight: {:?}\n{}",
                outcome.map(|data| data.len()),
                daemon.output()
            );
        }
        thread::sleep(Duration::from_millis(50));
    }
    let log_meta = fs::metadata(daemon.log_path())
        .map(|m| format!("{} bytes", m.len()))
        .unwrap_or_else(|e| format!("unreadable: {e}"));
    panic!(
        "no new read became in flight within {PARK_TIMEOUT:?} (log {}: {log_meta})\n{}",
        daemon.log_path().display(),
        daemon.output()
    );
}

#[test]
fn tc_recovery_01_sigterm_with_inflight_read_exits_bounded_and_remount_keeps_projection() {
    if let Some(blocker) = fuse_mount_blocker() {
        eprintln!("SKIP TC-recovery-01: this environment cannot mount FUSE ({blocker})");
        return;
    }
    let _slot = wave_slot();
    let mut harness = TestHarness::with_torrent(unique_torrent("recovery-01-inflight.txt"));

    // Windows long enough that the read is still waiting when SIGTERM lands.
    let config = daemon_config(30, 30, 30);
    let mut daemon = Daemon::start(Some(&config), "debug");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!("SKIP TC-recovery-01: the environment refused the FUSE mount\n{detail}");
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not mount:\n{detail}"),
    }
    let mountpoint = daemon.mountpoint();

    ingest_torrent_through_mount(&mountpoint, "recovery-01.torrent", &harness.torrent_data);
    assert!(
        wait_for_projection(&mountpoint, "recovery-01.torrent", PROJECTION_TIMEOUT),
        "projection never appeared\n{}",
        daemon.output()
    );

    let data_file = mountpoint
        .join("data")
        .join("recovery-01.torrent")
        .join(harness.info.name());
    assert!(
        wait_for_path(&data_file, PROJECTION_TIMEOUT),
        "projected file {} never appeared\n{}",
        data_file.display(),
        daemon.output()
    );

    // Empty the swarm before the read: with the seeder gone the read has no
    // source and parks in peer discovery instead of completing, and no piece is
    // on disk yet (nothing was read), so it cannot be served locally either.
    harness.stop_seeder();

    let torrent_dir = mountpoint.join("data").join("recovery-01.torrent");
    assert!(
        wait_for_listed_entry(&torrent_dir, &harness.info.name(), PROJECTION_TIMEOUT),
        "{} never listed {} (the settled projection never replaced the temporary \
         listing)\n{}",
        torrent_dir.display(),
        harness.info.name(),
        daemon.output()
    );

    let before = projected_tree(&torrent_dir, &data_file);
    assert!(
        before.is_some(),
        "the projection was not resolvable before the stop\n{}",
        daemon.output()
    );

    // ── in-flight read ───────────────────────────────────────────────────────
    let parked_before = daemon.output().matches("Deferred read queued").count();
    let read_path = data_file.clone();
    let (reader_done_tx, reader_done_rx) = std::sync::mpsc::channel();
    let reader = thread::spawn(move || {
        // A freshly ingested torrent stays listed in a temporary projection
        // until the background persist lands (C-2.5.3), and reads of that
        // projection can answer ENOENT.  Retry that window; any outcome after
        // it is reported so a non-parksing read fails the case with its reason.
        let deadline = Instant::now() + PROJECTION_TIMEOUT;
        let outcome = loop {
            match fs::read(&read_path) {
                Err(_) if Instant::now() < deadline => thread::sleep(Duration::from_millis(50)),
                other => break other,
            }
        };
        let _ = reader_done_tx.send(outcome);
    });
    wait_for_new_parked_read(&daemon, parked_before, &reader_done_rx);

    // ── SIGTERM ──────────────────────────────────────────────────────────────
    assert!(
        daemon.signal(libc::SIGTERM),
        "daemon already exited before SIGTERM"
    );
    let signalled = Instant::now();
    let status = daemon.wait_for_exit(STOP_WINDOW).unwrap_or_else(|| {
        panic!(
            "daemon did not exit within the {STOP_WINDOW:?} container stop window after \
             SIGTERM with a read in flight\n{}",
            daemon.output()
        )
    });
    let stopped_in = signalled.elapsed();
    assert_eq!(
        status.code(),
        Some(0),
        "a graceful SIGTERM shutdown must exit 0, got {status}\n{}",
        daemon.output()
    );
    assert!(
        stopped_in <= STOP_WINDOW,
        "shutdown took {stopped_in:?}, past the {STOP_WINDOW:?} container stop window\n{}",
        daemon.output()
    );
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        0,
        "SIGTERM left the FUSE mount in the mount table\n{}",
        daemon.output()
    );
    // The parked read is answered by the engine's shutdown abort inside the
    // daemon, but the kernel may keep the request pending on the now-detached
    // connection for a while after the device closes.  The criterion is the
    // daemon's exit, not the reader's wake-up, so give it a short grace and
    // leave the thread to the process exit instead of blocking the case on it.
    let _ = reader_done_rx.recv_timeout(Duration::from_secs(2));
    drop(reader);

    // ── remount on the same state, projection unchanged ──────────────────────
    daemon.restart(Some(&config), "info");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!("SKIP TC-recovery-01: the environment refused the remount\n{detail}");
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not remount:\n{detail}"),
    }
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        1,
        "the remount did not appear in the mount table\n{}",
        daemon.output()
    );
    // The remounted instance rebuilds the projection from the state database;
    // wait for it to settle before measuring, exactly as on the first mount.
    assert!(
        wait_for_listed_entry(&torrent_dir, &harness.info.name(), PROJECTION_TIMEOUT),
        "the remount never listed {} again\n{}",
        harness.info.name(),
        daemon.output()
    );
    let after = projected_tree(&torrent_dir, &data_file);
    assert_eq!(
        before,
        after,
        "the projection changed across the stop/remount (entry set, permission bits \
         or size)\n{}",
        daemon.output()
    );

    // Leave the second instance on the graceful path too, so the case does not
    // depend on the harness killing it.
    assert!(
        daemon.signal(libc::SIGTERM),
        "remounted daemon already exited before SIGTERM"
    );
    let status = daemon.wait_for_exit(STOP_WINDOW).unwrap_or_else(|| {
        panic!(
            "remounted daemon did not exit after SIGTERM\n{}",
            daemon.output()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "the remounted instance's graceful shutdown must exit 0, got {status}\n{}",
        daemon.output()
    );
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        0,
        "the remounted instance left its FUSE mount behind\n{}",
        daemon.output()
    );
}

/// The engine thread's TID inside `pid`, found by the name the engine spawns it
/// with (the crate constant, so a rename cannot leave this case looking for a
/// thread that no longer exists).
fn engine_thread_id(pid: u32) -> Option<libc::pid_t> {
    fs::read_dir(format!("/proc/{pid}/task"))
        .ok()?
        .filter_map(Result::ok)
        .find_map(|task| {
            let tid = task
                .file_name()
                .to_string_lossy()
                .parse::<libc::pid_t>()
                .ok()?;
            let comm = fs::read_to_string(task.path().join("comm")).ok()?;
            (comm.trim() == ENGINE_THREAD_NAME).then_some(tid)
        })
}

/// The daemon's engine thread, frozen in place with `PTRACE_ATTACH`.
///
/// The stop flag can only end the engine loop once the thread returns to it, so
/// a thread parked inside a libtorrent call never observes the shutdown
/// command — the state the container incident was in, and the reason the stop
/// has to be bounded.  Freezing the thread reproduces it without changing the
/// daemon: the process stays live, the stop request is queued, and the thread
/// never answers it.
///
/// The freeze also holds the process's exit status: Linux does not report a
/// thread group's exit while one of its traced threads is still stopped, so the
/// case must release — and reap — this handle before reading that status.  That
/// is part of the expected sequence, not only cleanup.
struct FrozenThread {
    tid: libc::pid_t,
}

impl FrozenThread {
    /// Freeze the thread `tid` in place.
    ///
    /// An `EPERM` from `PTRACE_ATTACH` is the environment refusing the attach
    /// (a seccomp profile or a ptrace scope that excludes the daemon); every
    /// other error means the freeze did not happen and the case cannot rely on
    /// the thread staying unresponsive.
    fn attach(tid: libc::pid_t) -> Result<Self, std::io::Error> {
        let attached = unsafe {
            libc::ptrace(
                libc::PTRACE_ATTACH,
                tid,
                std::ptr::null_mut::<libc::c_void>(),
                std::ptr::null_mut::<libc::c_void>(),
            )
        };
        if attached != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut status = 0;
        if unsafe { libc::waitpid(tid, &mut status, libc::__WALL) } != tid
            || !libc::WIFSTOPPED(status)
        {
            unsafe {
                libc::ptrace(
                    libc::PTRACE_DETACH,
                    tid,
                    std::ptr::null_mut::<libc::c_void>(),
                    std::ptr::null_mut::<libc::c_void>(),
                );
            }
            return Err(std::io::Error::other(format!(
                "the engine thread did not stop (status {status:#x})"
            )));
        }
        Ok(Self { tid })
    }
}

impl Drop for FrozenThread {
    fn drop(&mut self) {
        // Resume the tracee (a pending SIGKILL from the group exit lands first)
        // and reap it: an unreaped tracee keeps its thread group from reporting
        // its exit status to this process.
        unsafe {
            libc::ptrace(
                libc::PTRACE_DETACH,
                self.tid,
                std::ptr::null_mut::<libc::c_void>(),
                std::ptr::null_mut::<libc::c_void>(),
            );
        }
        let mut status = 0;
        for _ in 0..100 {
            let reaped =
                unsafe { libc::waitpid(self.tid, &mut status, libc::__WALL | libc::WNOHANG) };
            if reaped == self.tid || reaped < 0 {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
    }
}

/// The bound on the engine join, made discriminating.
///
/// The in-flight-read case above cannot fail an unbounded teardown in this
/// harness: the engine parks its reads on its own loop, so it reaches the stop
/// flag and exits quickly whether or not the teardown waits without a bound.
/// The incident's thread was wedged inside a libtorrent call that never reaches
/// the flag, so this case freezes the engine thread and asserts what the
/// contract requires in that state: the teardown gives up on the thread at the
/// bound (the log records it), unmounts, and exits 0 inside the stop window.
/// An unbounded join reaches none of those — in the container it is SIGKILLed,
/// and here it never logs, never unmounts, and this case times out.
#[test]
fn tc_recovery_01_bounded_exit_when_the_engine_thread_ignores_the_stop() {
    if let Some(blocker) = fuse_mount_blocker() {
        eprintln!(
            "SKIP TC-recovery-01 (bounded exit): this environment cannot mount FUSE ({blocker})"
        );
        return;
    }
    let _slot = wave_slot();
    let config = daemon_config(30, 30, 30);
    let mut daemon = Daemon::start(Some(&config), "debug");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!(
                "SKIP TC-recovery-01 (bounded exit): the environment refused the FUSE mount\n{detail}"
            );
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not mount:\n{detail}"),
    }
    let mountpoint = daemon.mountpoint();
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        1,
        "the case needs a live mount before the signal\n{}",
        daemon.output()
    );

    let pid = daemon.pid().expect("the daemon is running");
    // A build whose stop path can abandon the engine thread has to make that
    // thread findable; without it the case cannot isolate the thread the bound
    // is about, and passing silently is the defect this carrier was added for.
    let engine_tid = engine_thread_id(pid).unwrap_or_else(|| {
        panic!(
            "the daemon exposes no thread named {ENGINE_THREAD_NAME}, so the bounded stop \
             cannot be exercised\n{}",
            daemon.output()
        )
    });
    let frozen = match FrozenThread::attach(engine_tid) {
        Ok(frozen) => frozen,
        Err(error) if error.raw_os_error() == Some(libc::EPERM) => {
            eprintln!(
                "SKIP TC-recovery-01 (bounded exit): this environment refuses ptrace ({error})"
            );
            return;
        }
        Err(error) => panic!(
            "could not freeze the engine thread: {error}, so the thread is not known to \
             ignore the stop\n{}",
            daemon.output()
        ),
    };

    assert!(
        daemon.signal(libc::SIGTERM),
        "daemon already exited before SIGTERM"
    );
    let signalled = Instant::now();
    let deadline = signalled + STOP_WINDOW;
    let mut abandoned_after = None;
    let mut unmounted_after = None;
    while Instant::now() < deadline {
        let elapsed = signalled.elapsed();
        if abandoned_after.is_none() && daemon.output().contains(ENGINE_ABANDON_MARKER) {
            abandoned_after = Some(elapsed);
        }
        if abandoned_after.is_some() && mountinfo_fuse_count(&mountpoint) == 0 {
            unmounted_after = Some(elapsed);
            break;
        }
        thread::sleep(Duration::from_millis(50));
    }

    let abandoned_after = abandoned_after.unwrap_or_else(|| {
        panic!(
            "the teardown never reported abandoning the engine thread within {STOP_WINDOW:?} of \
             SIGTERM: the stop waited for a thread that cannot observe it\n{}",
            daemon.output()
        )
    });
    assert!(
        abandoned_after <= STOP_WINDOW,
        "the teardown gave up on the engine thread only after {abandoned_after:?}, past the \
         {STOP_WINDOW:?} container stop window"
    );
    let unmounted_after = unmounted_after.unwrap_or_else(|| {
        panic!(
            "the daemon never unmounted within {STOP_WINDOW:?} of SIGTERM while its engine \
             thread was unresponsive\n{}",
            daemon.output()
        )
    });
    assert!(
        unmounted_after <= STOP_WINDOW,
        "the teardown unmounted only after {unmounted_after:?}, past the {STOP_WINDOW:?} \
         container stop window"
    );

    // Releasing the frozen thread is what lets the finished teardown report its
    // exit status (see [`FrozenThread`]).
    drop(frozen);
    let status = daemon.wait_for_exit(STOP_WINDOW).unwrap_or_else(|| {
        panic!(
            "the daemon did not exit after its frozen engine thread was released\n{}",
            daemon.output()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "a bounded SIGTERM teardown must exit 0, got {status}\n{}",
        daemon.output()
    );
}

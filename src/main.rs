//! torrentfs — A FUSE filesystem for BitTorrent management.
//! Thin binary entry point. All logic lives in the library crate.

use clap::Parser;
use fuser::MountOption;
use std::fs::File;
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, OnceLock};
use std::thread::Thread;
use std::time::{Duration, Instant};
use tracing::{error, info, warn, Level};
use tracing_subscriber::FmtSubscriber;

use torrentfs::config::TorrentfsConfig;
use torrentfs::db::Database;
use torrentfs::fuse::{TorrentFs, WorkerPool};
use torrentfs::DownloadService;

/// Set by the SIGINT/SIGTERM handler to request graceful shutdown; the handler
/// also unparks the main thread so it can run the teardown sequence.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);
static MAIN_THREAD: OnceLock<Thread> = OnceLock::new();

/// Async-signal-safe handler: only stores an atomic flag and unparks the main
/// thread. All teardown runs on the main thread after `park` returns.
extern "C" fn handle_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
    if let Some(main) = MAIN_THREAD.get() {
        main.unpark();
    }
}

/// Install SIGINT/SIGTERM handlers so the main thread shuts down cleanly
/// (drain workers, stop session) instead of terminating abruptly mid-read.
fn install_shutdown_signal_handlers() {
    let _ = MAIN_THREAD.set(std::thread::current());
    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_shutdown_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_shutdown_signal as *const () as libc::sighandler_t,
        );
    }
}

#[derive(Parser, Debug)]
#[command(name = "torrentfs", version)]
#[command(about = "A FUSE filesystem for torrent management")]
struct Args {
    #[arg(
        required_unless_present = "config_check",
        help = "Mount point path (not required with --config-check)"
    )]
    mountpoint: Option<PathBuf>,
    #[arg(long, help = "Database path")]
    db: Option<PathBuf>,
    #[arg(long, help = "Cache directory for downloaded pieces")]
    cache: Option<PathBuf>,
    #[arg(long, help = "Configuration file path (TOML)")]
    config: Option<PathBuf>,
    /// Validate the config file and exit (0 = valid, non-zero = invalid).
    #[arg(long, conflicts_with_all = ["mountpoint", "db", "cache"], requires = "config",
          help = "Validate a configuration file and exit")]
    config_check: bool,
}

fn fuse_allow_other_enabled() -> io::Result<bool> {
    let file = File::open("/etc/fuse.conf")?;
    for line in BufReader::new(file).lines() {
        let line = line?;
        if line.trim() == "user_allow_other" {
            return Ok(true);
        }
    }
    Ok(false)
}

fn user_in_fuse_group() -> bool {
    use std::fs;
    if let Ok(group_file) = fs::read_to_string("/etc/group") {
        for line in group_file.lines() {
            let parts: Vec<&str> = line.split(':').collect();
            if parts.len() >= 4 && parts[0] == "fuse" {
                let members = parts[3];
                if let Ok(current_user) = std::env::var("USER") {
                    if members.split(',').any(|m| m.trim() == current_user) {
                        return true;
                    }
                }
            }
        }
    }

    if let Ok(output) = std::process::Command::new("groups").output() {
        let groups = String::from_utf8_lossy(&output.stdout);
        if groups.split_whitespace().any(|g| g == "fuse") {
            return true;
        }
    }

    false
}

/// Explicitly unmount the FUSE filesystem before joining the session.
///
/// In `AutoUnmount` mode, `BackgroundSession::join()` only tears down the
/// fusermount control socket when it drops the mount — the actual unmount is
/// deferred to process exit.  The FUSE session thread therefore stays blocked
/// in `fuse_dev_do_read` (the device is still open) and `guard.join()` never
/// returns.  A lazy detach makes the device read return `ENODEV` so the
/// session thread exits.
///
/// Strategy mirrors fuser's own `fuse_unmount_pure()`: try `umount2(MNT_DETACH)`
/// first (root / rootful container), then fall back to the setuid `fusermount`
/// helper when that returns `EPERM` (non-root mount owner).
///
/// Returns `true` when the mount was detached so clean shutdown can proceed,
/// `false` when every attempt failed and the mount is still live.
fn unmount_fuse(mountpoint: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let c_path = match std::ffi::CString::new(mountpoint.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            warn!(
                "mountpoint {:?} contains a NUL byte; cannot unmount",
                mountpoint
            );
            return false;
        }
    };

    let ret = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) };
    if ret == 0 {
        info!("unmounted {} (umount2 MNT_DETACH)", mountpoint.display());
        return true;
    }
    warn!(
        "umount2({}) failed ({}), falling back to fusermount",
        mountpoint.display(),
        std::io::Error::last_os_error()
    );

    // Non-root fallback: torrentfs mounts via the setuid fusermount helper
    // (auto_unmount + allow_other), so unmount must go through `fusermount -u`.
    for bin in ["fusermount3", "fusermount"] {
        match std::process::Command::new(bin)
            .arg("-u")
            .arg("-q")
            .arg("-z")
            .arg("--")
            .arg(mountpoint)
            .output()
        {
            Ok(output) if output.status.success() => {
                info!("unmounted {} ({bin} -u)", mountpoint.display());
                return true;
            }
            Ok(output) => {
                warn!(
                    "{bin} -u failed with status {:?}: {}",
                    output.status,
                    String::from_utf8_lossy(&output.stderr)
                );
            }
            Err(e) => {
                warn!("failed to run {bin}: {e}");
            }
        }
    }
    warn!("all unmount attempts failed for {}", mountpoint.display());
    false
}

/// Upper bound on the whole graceful-shutdown teardown: engine stop + cache
/// flush + worker drain + FUSE unmount + session join.
///
/// The session join receives only the part of this budget left after the
/// earlier steps (`SHUTDOWN_TIMEOUT.saturating_sub(elapsed_since_signal)`), so
/// the total time from SIGTERM to process exit is bounded.  The bound matters
/// in containers where the FUSE superblock can be held alive by an external
/// bind mount (entrypoint.sh's rootful path publishes `/mnt-inner` via
/// `mount --bind`): that reference keeps the kernel from aborting the
/// connection, so the session thread stays blocked in `read()` on `/dev/fuse`
/// and would otherwise hang until the container engine SIGKILLs.  Past this
/// window the thread is abandoned and process exit closes `/dev/fuse`,
/// leaving the now-stale bind mount for the entrypoint to unmount.
///
/// A container stop grace period should therefore exceed this budget (e.g.
/// `docker stop -t 10`) so teardown finishes before SIGKILL.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Outcome of waiting for the FUSE session thread to exit during shutdown.
#[derive(Debug, PartialEq, Eq)]
enum JoinOutcome {
    /// The session thread exited within the timeout — safe to join.
    Finished,
    /// The session thread did not exit — abandon it so the process can exit.
    TimedOut,
}

/// Poll `is_finished` until it reports true, or `timeout` elapses.
///
/// Split out of `wait_for_shutdown` so the bounded-wait policy is unit-testable
/// without mounting a real filesystem.
fn wait_bounded<F>(is_finished: F, timeout: Duration, poll_interval: Duration) -> JoinOutcome
where
    F: Fn() -> bool,
{
    let deadline = Instant::now() + timeout;
    loop {
        if is_finished() {
            return JoinOutcome::Finished;
        }
        if Instant::now() >= deadline {
            return JoinOutcome::TimedOut;
        }
        std::thread::sleep(poll_interval);
    }
}

/// Park the main thread until SIGINT/SIGTERM, then run graceful shutdown:
/// stop the download engine, drain the download worker queue, unmount the FUSE
/// session, and join the session thread (which drops the libtorrent session).
/// The whole teardown is bounded by [`SHUTDOWN_TIMEOUT`], measured from the
/// signal.  If the unmount fails, the process exits non-zero.  If the unmount
/// succeeds but the session thread does not exit within the remaining budget —
/// possible when an external bind mount keeps the superblock alive — the
/// thread is abandoned and process exit closes `/dev/fuse`.
fn wait_for_shutdown(
    worker_pool: Arc<WorkerPool>,
    download_service: Option<Arc<DownloadService>>,
    bg: fuser::BackgroundSession,
    mountpoint: &Path,
) {
    while !SHUTDOWN.load(Ordering::SeqCst) {
        std::thread::park();
    }
    // The shutdown deadline starts when the signal arrives; every subsequent
    // teardown step (engine stop, flush, drain, unmount) consumes part of it.
    let shutdown_started = Instant::now();
    info!("shutdown requested — stopping download engine");
    if let Some(ds) = &download_service {
        ds.shutdown();
    }
    // TSI-2263: flush the cache metadata to disk (with fsync) before
    // unmounting.  The download engine has already stopped, so no new
    // pieces are being registered.  Without this explicit flush, a
    // container restart can leave cache_metadata.txt stale, causing
    // scan_pieces_subdirectory to register pieces at wrong sizes and
    // the verifier to purge them ("cache piece cleaned" after restart).
    if let Some(ds) = &download_service {
        if let Some(cache) = ds.get_cache_manager() {
            match cache.lock() {
                Ok(mut cm) => {
                    if let Err(e) = cm.flush() {
                        warn!("Failed to flush cache metadata on shutdown: {:?}", e);
                    } else {
                        info!("cache metadata flushed to disk");
                    }
                }
                Err(_) => warn!("Cache lock poisoned on shutdown — metadata not flushed"),
            }
        }
    }
    info!("draining download worker queue");
    worker_pool.shutdown();
    info!("unmounting FUSE filesystem");
    if !unmount_fuse(mountpoint) {
        error!("FUSE unmount failed; the mountpoint is left in an inconsistent state");
        std::process::exit(1);
    }
    info!("joining FUSE session");
    // The join gets only the shutdown budget left after the steps above, so
    // the total teardown time is bounded.  Normal path: the unmount aborts the
    // connection and the session thread exits immediately.  With an external
    // bind mount holding the superblock alive it never exits — abandon it so
    // the process can terminate (see SHUTDOWN_TIMEOUT).
    let join_budget = SHUTDOWN_TIMEOUT.saturating_sub(shutdown_started.elapsed());
    match wait_bounded(
        || bg.guard.is_finished(),
        join_budget,
        Duration::from_millis(20),
    ) {
        JoinOutcome::Finished => {
            bg.join();
            info!("torrentfs unmounted successfully");
        }
        JoinOutcome::TimedOut => {
            warn!(
                "FUSE session thread did not exit within the {}s shutdown window; abandoning it so the process can exit",
                SHUTDOWN_TIMEOUT.as_secs()
            );
        }
    }
}

fn main() {
    let log_level = std::env::var("RUST_LOG")
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "trace" => Some(Level::TRACE),
            "debug" => Some(Level::DEBUG),
            "info" => Some(Level::INFO),
            "warn" => Some(Level::WARN),
            "error" => Some(Level::ERROR),
            _ => None,
        })
        .unwrap_or(Level::INFO);

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    let args = Args::parse();

    // --config-check: validate the TOML file and exit. No FUSE, no DB, no mount.
    if args.config_check {
        let path = args
            .config
            .as_ref()
            .expect("config-check requires --config");
        match TorrentfsConfig::from_file(path) {
            Ok(_) => {
                info!("Configuration file {:?} is valid", path);
                std::process::exit(0);
            }
            Err(e) => {
                error!("Invalid configuration file {:?}: {}", path, e);
                std::process::exit(1);
            }
        }
    }
    install_shutdown_signal_handlers();

    // Load configuration from TOML file if provided
    let config = match &args.config {
        Some(config_path) => match TorrentfsConfig::from_file(config_path) {
            Ok(cfg) => {
                info!("Loaded configuration from {:?}", config_path);
                cfg
            }
            Err(e) => {
                error!("Failed to load config from {:?}: {}", config_path, e);
                std::process::exit(1);
            }
        },
        None => TorrentfsConfig::default_config(),
    };

    // Early check: /dev/fuse must exist for FUSE mounts to work.
    // On rootless containers this is the most common failure point.
    if !std::path::Path::new("/dev/fuse").exists() {
        error!(
            "/dev/fuse not found. torrentfs requires the FUSE kernel module.\n\
             Container users: pass --device /dev/fuse --cap-add SYS_ADMIN to podman/docker.\n\
             Host users: ensure the fuse kernel module is loaded (modprobe fuse)."
        );
        std::process::exit(3);
    }

    // Unreachable as Option::None: clap enforces `required_unless_present`
    // so mountpoint is guaranteed present on this path.
    let mountpoint = args
        .mountpoint
        .unwrap_or_else(|| unreachable!("clap: mountpoint required without --config-check"));

    if !mountpoint.exists() {
        std::fs::create_dir_all(&mountpoint).expect("Failed to create mountpoint");
    }

    let cache_path = args.cache.clone().unwrap_or_else(|| {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/cache")
    });
    if !cache_path.exists() {
        if let Err(e) = std::fs::create_dir_all(&cache_path) {
            warn!("Failed to create cache directory {:?}: {:?}", cache_path, e);
        }
    }

    let db_path = if let Some(db_path) = &args.db {
        db_path.clone()
    } else {
        dirs::data_local_dir()
            .unwrap_or_else(|| PathBuf::from("/tmp"))
            .join("torrentfs/db/metadata.db")
    };

    if let Some(parent) = db_path.parent() {
        if !parent.exists() {
            if let Err(e) = std::fs::create_dir_all(parent) {
                warn!("Failed to create database directory: {:?}", e);
            }
        }
    }

    let allow_other_enabled = fuse_allow_other_enabled().unwrap_or(false);

    // No owner-only fallback: on kernels that gate every unprivileged FUSE
    // mount on `user_allow_other` in /etc/fuse.conf (not on the `allow_other`
    // option itself), a retry without AllowOther fails with the same EPERM.
    // The EPERM path below reuses the /etc/fuse.conf diagnostic instead of
    // claiming a degraded owner-only mount.
    let mut options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];
    if allow_other_enabled {
        options.push(MountOption::AllowOther);
    } else {
        warn!(
            "'user_allow_other' is not set in /etc/fuse.conf; non-root mount will fail with EPERM"
        );
    }

    let db = match Database::open(&db_path) {
        Ok(db) => {
            info!("Database opened at {:?}", db_path);
            Some(db)
        }
        Err(e) => {
            if args.db.is_some() {
                error!("Failed to open database: {:?}", e);
                std::process::exit(1);
            }
            warn!(
                "Failed to open database at {:?}: {:?}, running without persistence",
                db_path, e
            );
            None
        }
    };

    let fs = match db {
        Some(d) => TorrentFs::new_with_db_and_cache(d, cache_path.clone(), &config),
        None => TorrentFs::new_with_cache_path(cache_path.clone(), &config),
    };
    let worker_pool = fs.worker_pool();
    let download_service = fs.download_service().cloned();
    let notifier = fs.notifier_handle();

    // The download engine is essential: without it every data read returns
    // EIO and no torrent can be seeded. `None` here means initialization
    // failed (e.g. the cache directory is owned by a different user left over
    // from a previous container run). Fail loudly instead of mounting a
    // filesystem that can only browse metadata.
    if download_service.is_none() {
        error!(
            "Download engine failed to initialize (cache dir {:?}). torrentfs \
             cannot download or seed file content. Ensure the cache directory \
             is writable by the current user — a state directory left over from \
             a previous container run under a different user is the usual cause.",
            cache_path
        );
        std::process::exit(1);
    }

    match fuser::spawn_mount2(fs, &mountpoint, &options) {
        Ok(bg) => {
            // TSI-2454: wire the kernel cache invalidation channel so
            // `unlink`/`rmdir`/`rename` can immediately purge stale
            // `data/` dentries instead of waiting for the 1s TTL.
            notifier.set(Some(bg.notifier())).ok();
            info!("torrentfs mounted");
            wait_for_shutdown(worker_pool, download_service, bg, &mountpoint);
        }
        Err(e) => {
            let error_msg = e.to_string();
            if e.kind() == io::ErrorKind::PermissionDenied {
                let mut hints = Vec::new();
                if !allow_other_enabled {
                    hints.push("'user_allow_other' is not set in /etc/fuse.conf");
                }
                if !user_in_fuse_group() {
                    hints.push("user may not be in the 'fuse' group (some systems require this)");
                }
                hints.push("running in a container or restricted environment");
                hints.push("SELinux/AppArmor restrictions");
                hints.push("/dev/fuse device permissions");
                error!(
                    "Mount failed: Operation not permitted. Possible causes:\n  - {}",
                    hints.join("\n  - ")
                );
                std::process::exit(2);
            }
            error!("Failed to mount filesystem: {}", error_msg);
            std::process::exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wait_bounded_returns_finished_when_thread_exits() {
        assert_eq!(
            wait_bounded(|| true, Duration::from_secs(5), Duration::from_millis(1)),
            JoinOutcome::Finished
        );
    }
    #[test]
    fn wait_bounded_returns_timed_out_when_thread_never_exits() {
        assert_eq!(
            wait_bounded(
                || false,
                Duration::from_millis(10),
                Duration::from_millis(1)
            ),
            JoinOutcome::TimedOut
        );
    }
}

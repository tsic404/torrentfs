//! torrentfs — A FUSE filesystem for BitTorrent management.
//! Thin binary entry point. All logic lives in the library crate.

use clap::{Parser, ValueEnum};
use fuser::MountOption;
use std::fs::{File, OpenOptions};
use std::io::{self, BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
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
    #[arg(
        long,
        value_name = "LEVEL",
        help = "Log verbosity (error|warn|info|debug|trace); overrides RUST_LOG"
    )]
    log_level: Option<LogLevelArg>,
    #[arg(
        long,
        value_name = "PATH",
        help = "Append logs to a file instead of stdout"
    )]
    log_file: Option<PathBuf>,
    /// Validate the config file and exit (0 = valid, non-zero = invalid).
    #[arg(long, conflicts_with_all = ["mountpoint"], requires = "config",
          help = "Validate a configuration file and exit")]
    config_check: bool,
}

/// CLI log verbosity. Maps 1:1 onto `tracing::Level`; clap derives the flag's
/// accepted values from the variant names (error/warn/info/debug/trace).
#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum LogLevelArg {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevelArg {
    fn to_tracing(self) -> Level {
        match self {
            LogLevelArg::Error => Level::ERROR,
            LogLevelArg::Warn => Level::WARN,
            LogLevelArg::Info => Level::INFO,
            LogLevelArg::Debug => Level::DEBUG,
            LogLevelArg::Trace => Level::TRACE,
        }
    }
}

/// Resolve the tracing verbosity: an explicit `--log-level` wins over the
/// `RUST_LOG` environment variable, falling back to `INFO` when neither names
/// a recognized level.
fn resolve_log_level(cli_level: Option<LogLevelArg>) -> Level {
    match cli_level {
        Some(level) => level.to_tracing(),
        None => std::env::var("RUST_LOG")
            .ok()
            .and_then(|v| parse_rust_log(&v))
            .unwrap_or(Level::INFO),
    }
}

/// Parse a `RUST_LOG` value into a `tracing::Level`. Unknown values return
/// `None` so the caller can apply its own default rather than mis-tagging a
/// typo as a specific level.
fn parse_rust_log(value: &str) -> Option<Level> {
    match value.to_lowercase().as_str() {
        "trace" => Some(Level::TRACE),
        "debug" => Some(Level::DEBUG),
        "info" => Some(Level::INFO),
        "warn" => Some(Level::WARN),
        "error" => Some(Level::ERROR),
        _ => None,
    }
}

/// Open the `--log-file` target in append mode, creating parent directories so
/// a mounted-but-empty log directory works on first run. torrentfs itself never
/// drops privileges: when launched through the container entrypoint it already
/// runs as the daemon user (UID 1000), so the path must be writable by that
/// identity — the entrypoint creates and re-owns the log directory before its
/// `setpriv` drop (see `fix_state_dir_ownership`).
fn open_log_file(path: &Path) -> io::Result<File> {
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent)?;
        }
    }
    OpenOptions::new().create(true).append(true).open(path)
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
/// Strategy adapts fuser's `fuse_unmount_pure()` with a deliberate divergence:
/// root (rootful container, or a user namespace with CAP_SYS_ADMIN) detaches
/// directly via `umount2(MNT_DETACH)`, while non-root owners skip the
/// guaranteed-`EPERM` syscall and unmount through the setuid `fusermount`
/// helper directly — fuser itself always tries `umount2` first for everyone.
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

    // SAFETY: `geteuid()` has no preconditions.
    if should_attempt_direct_unmount(unsafe { libc::geteuid() }) {
        // Root detaches the mount directly. Non-root mounts always go through
        // the setuid fusermount helper — AutoUnmount is only supported via the
        // helper (see fuser's `fuse_mount_pure`) — so `umount2` would fail with
        // EPERM on every shutdown: skip the doomed syscall and its spurious
        // WARN, and unmount via the helper below.
        // SAFETY: `c_path` is a valid NUL-terminated string owned by this frame.
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
    }

    // Unmount via the setuid fusermount helper: the correct path for non-root
    // owners, and the fallback for root when `umount2` failed above.
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

/// Decide whether a direct `umount2(MNT_DETACH)` is worth attempting before
/// falling back to the `fusermount` helper. Only root (or a user namespace with
/// CAP_SYS_ADMIN) can detach a mount it does not own; non-root owners mount
/// through the helper and must unmount through it too.
fn should_attempt_direct_unmount(euid: u32) -> bool {
    euid == 0
}

/// Grace period for the FUSE session thread to exit on its own after the
/// mount is detached.
///
/// A lazy detach aborts the FUSE connection, so in the direct-mount path the
/// session thread exits within milliseconds and can be joined immediately.  In
/// the rootful path entrypoint.sh publishes the internal mount at `/mnt` with
/// `mount --bind`, which keeps the FUSE superblock alive after the detach: the
/// session thread stays blocked in `read()` on `/dev/fuse` until the
/// entrypoint releases that reference after this process exits.  The download
/// engine and cache have already been shut down by then, so neither case needs
/// a long wait — this grace period only has to outlast the immediate exit.
const SESSION_DRAIN_GRACE: Duration = Duration::from_secs(1);

/// Outcome of waiting for the FUSE session thread to exit during shutdown.
#[derive(Debug, PartialEq, Eq)]
enum JoinOutcome {
    /// The session thread exited within the grace period — safe to join.
    Finished,
    /// The session thread is still blocked — the external bind mount keeps the
    /// superblock alive, so the process exits and the entrypoint unmounts it.
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
/// If the unmount fails, the process exits non-zero.  If the unmount succeeds
/// but the session thread does not exit within [`SESSION_DRAIN_GRACE`] —
/// expected when an external bind mount keeps the superblock alive — the
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
    info!("shutdown requested — stopping download engine");
    if let Some(ds) = &download_service {
        ds.shutdown();
    }
    // flush the cache metadata to disk (with fsync) before
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
    // Normal path: the unmount aborts the connection and the session thread
    // exits immediately.  With an external bind mount holding the superblock
    // alive it never exits until the entrypoint releases that reference after
    // the process exits — so only drain briefly, then proceed regardless.
    match wait_bounded(
        || bg.guard.is_finished(),
        SESSION_DRAIN_GRACE,
        Duration::from_millis(20),
    ) {
        JoinOutcome::Finished => {
            bg.join();
            info!("torrentfs unmounted successfully");
        }
        JoinOutcome::TimedOut => {
            // Expected in the rootful bind-mount path: the engine and cache are
            // already shut down, so the process can exit and the entrypoint will
            // release the bind mount afterwards.
            info!(
                "FUSE session thread still blocked after unmount (external bind mount keeping the superblock alive); exiting and letting the entrypoint release it"
            );
        }
    }
}

fn main() {
    let args = Args::parse();

    let log_level = resolve_log_level(args.log_level);
    match &args.log_file {
        Some(path) => {
            let file = open_log_file(path).unwrap_or_else(|e| {
                eprintln!("Failed to open log file {:?}: {}", path, e);
                std::process::exit(1);
            });
            let subscriber = FmtSubscriber::builder()
                .with_max_level(log_level)
                .with_writer(Mutex::new(file))
                .finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set tracing subscriber");
        }
        None => {
            let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();
            tracing::subscriber::set_global_default(subscriber)
                .expect("Failed to set tracing subscriber");
        }
    }

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
            // wire the kernel cache invalidation channel so
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
    fn should_attempt_direct_unmount_only_for_root() {
        assert!(should_attempt_direct_unmount(0));
        assert!(!should_attempt_direct_unmount(1000));
        assert!(!should_attempt_direct_unmount(u32::MAX));
    }

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

    #[test]
    fn config_check_accepts_db_and_cache() {
        let args = Args::try_parse_from([
            "torrentfs",
            "--config",
            "/cfg.toml",
            "--config-check",
            "--db",
            "/state.db",
            "--cache",
            "/cache",
        ])
        .expect("--config-check must not conflict with --db/--cache");
        assert!(args.config_check);
        assert_eq!(args.db.as_deref(), Some(Path::new("/state.db")));
        assert_eq!(args.cache.as_deref(), Some(Path::new("/cache")));
    }

    #[test]
    fn config_check_still_conflicts_with_mountpoint() {
        let err = Args::try_parse_from([
            "torrentfs",
            "--config",
            "/cfg.toml",
            "--config-check",
            "/mnt",
        ])
        .expect_err("--config-check must still conflict with mountpoint");
        assert_eq!(err.kind(), clap::error::ErrorKind::ArgumentConflict);
    }

    #[test]
    fn config_check_requires_config() {
        let err = Args::try_parse_from(["torrentfs", "--config-check"])
            .expect_err("--config-check must require --config");
        assert_eq!(err.kind(), clap::error::ErrorKind::MissingRequiredArgument);
    }

    #[test]
    fn parse_rust_log_recognizes_known_levels_case_insensitively() {
        assert_eq!(parse_rust_log("debug"), Some(Level::DEBUG));
        assert_eq!(parse_rust_log("DEBUG"), Some(Level::DEBUG));
        assert_eq!(parse_rust_log("trace"), Some(Level::TRACE));
        assert_eq!(parse_rust_log("info"), Some(Level::INFO));
    }

    #[test]
    fn parse_rust_log_returns_none_for_unknown_level() {
        assert_eq!(parse_rust_log("bogus"), None);
        assert_eq!(parse_rust_log(""), None);
    }

    #[test]
    fn log_level_arg_maps_to_tracing_level() {
        assert_eq!(LogLevelArg::Error.to_tracing(), Level::ERROR);
        assert_eq!(LogLevelArg::Warn.to_tracing(), Level::WARN);
        assert_eq!(LogLevelArg::Info.to_tracing(), Level::INFO);
        assert_eq!(LogLevelArg::Debug.to_tracing(), Level::DEBUG);
        assert_eq!(LogLevelArg::Trace.to_tracing(), Level::TRACE);
    }

    #[test]
    fn log_flags_parse_with_mountpoint() {
        let args = Args::try_parse_from([
            "torrentfs",
            "--log-level",
            "debug",
            "--log-file",
            "/var/log/torrentfs.log",
            "/mnt",
        ])
        .expect("--log-level/--log-file must parse alongside the mountpoint");
        assert_eq!(args.log_level, Some(LogLevelArg::Debug));
        assert_eq!(
            args.log_file.as_deref(),
            Some(Path::new("/var/log/torrentfs.log"))
        );
        assert_eq!(args.mountpoint.as_deref(), Some(Path::new("/mnt")));
    }

    #[test]
    fn log_level_rejects_unknown_value() {
        let err = Args::try_parse_from(["torrentfs", "--log-level", "bogus", "/mnt"])
            .expect_err("--log-level must reject an unknown level");
        assert_eq!(err.kind(), clap::error::ErrorKind::InvalidValue);
    }
}

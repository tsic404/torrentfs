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
fn unmount_fuse(mountpoint: &Path) {
    use std::os::unix::ffi::OsStrExt;
    let c_path = match std::ffi::CString::new(mountpoint.as_os_str().as_bytes()) {
        Ok(c) => c,
        Err(_) => {
            warn!(
                "mountpoint {:?} contains a NUL byte; cannot unmount",
                mountpoint
            );
            return;
        }
    };

    let ret = unsafe { libc::umount2(c_path.as_ptr(), libc::MNT_DETACH) };
    if ret == 0 {
        info!("unmounted {} (umount2 MNT_DETACH)", mountpoint.display());
        return;
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
                return;
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
}

/// Park the main thread until SIGINT/SIGTERM, then run graceful shutdown:
/// stop the download engine, drain the download worker queue, unmount, and
/// join the FUSE session (which drops the libtorrent session).
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
    unmount_fuse(mountpoint);
    info!("joining FUSE session");
    bg.join();
    info!("torrentfs unmounted successfully");
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

    if allow_other_enabled {
        let options = vec![
            MountOption::FSName("torrentfs".to_string()),
            MountOption::AutoUnmount,
            MountOption::AllowOther,
        ];

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

        match fuser::spawn_mount2(fs, &mountpoint, &options) {
            Ok(bg) => {
                info!("torrentfs mounted");
                wait_for_shutdown(worker_pool, download_service, bg, &mountpoint);
                return;
            }
            Err(e) if e.kind() == io::ErrorKind::PermissionDenied => {
                warn!("Mount with AllowOther failed, falling back to owner-only mode");
            }
            Err(e) => {
                error!("Failed to mount filesystem: {}", e);
                std::process::exit(1);
            }
        }
    } else {
        warn!("user_allow_other not set in /etc/fuse.conf, mount will only be accessible by owner");
    }

    let options = vec![
        MountOption::FSName("torrentfs".to_string()),
        MountOption::AutoUnmount,
    ];

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

    match fuser::spawn_mount2(fs, &mountpoint, &options) {
        Ok(bg) => {
            info!("torrentfs mounted");
            wait_for_shutdown(worker_pool, download_service, bg, &mountpoint);
            info!("torrentfs unmounted successfully");
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

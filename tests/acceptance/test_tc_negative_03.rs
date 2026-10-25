//! Design acceptance case TC-negative-03: persistent state (the SQLite
//! database and the piece cache) defaults to a location outside the FUSE
//! mountpoint, and pointing it inside the mountpoint must not produce a
//! persistence illusion — "looks usable, then everything is gone after a
//! remount" (`docs/03-acceptance.md` §J AC-92; `docs/02-contracts.md` §C-12).
//!
//! The default half is observed on the real daemon: with `XDG_DATA_HOME`
//! pointed at a temporary tree and no `--db`/`--cache`, the process must create
//! its state under that tree and leave the mountpoint empty.  The daemon
//! resolves and creates state before attempting the mount, so the observation
//! needs the FUSE device to exist but not a successful mount.  The
//! forbidden-configuration half needs an actual mount; it runs wherever the
//! host can create one and records its reason when it cannot.

use std::fs::{File, OpenOptions};
use std::io::Read;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use torrentfs::infrastructure::config::default_state_paths;

use crate::common::base::create_single_piece_torrent;

pub const TC_ID: &str = "TC-negative-03";

/// The default layout is the documented XDG tree: one `torrentfs` state root
/// with `db/metadata.db` and `cache`.  This pins the resolver's composition;
/// the daemon-side observation below proves the process actually uses it.
#[test]
fn tc_negative_03_default_state_layout_is_the_documented_xdg_tree() {
    let (db_path, cache_path) = default_state_paths();
    let base = dirs::data_local_dir().unwrap_or_else(|| PathBuf::from("/tmp"));

    assert!(
        db_path.is_absolute(),
        "the default database path must be absolute: {}",
        db_path.display()
    );
    assert!(
        cache_path.is_absolute(),
        "the default cache path must be absolute: {}",
        cache_path.display()
    );
    assert_eq!(
        db_path,
        base.join("torrentfs/db/metadata.db"),
        "the default database must be <data dir>/torrentfs/db/metadata.db"
    );
    assert_eq!(
        cache_path,
        base.join("torrentfs/cache"),
        "the default cache must be <data dir>/torrentfs/cache"
    );

    println!(
        "TC-negative-03: default state layout — db={}, cache={}",
        db_path.display(),
        cache_path.display()
    );
}

/// The daemon itself must place its default state outside the mountpoint:
/// with `XDG_DATA_HOME=<tmp>/xdg` and no `--db`/`--cache`, the SQLite file and
/// the cache directory appear under `<tmp>/xdg/torrentfs`, and nothing
/// state-like appears under the mountpoint's underlying directory.
///
/// The daemon checks `/dev/fuse` before resolving state paths, so the
/// observation requires the device node to exist; a successful mount is not
/// required.  Where the mount succeeds, the daemon is stopped before the
/// underlying mountpoint directory is inspected.
#[test]
fn tc_negative_03_daemon_default_state_lands_outside_the_mountpoint() {
    if !Path::new("/dev/fuse").exists() {
        eprintln!(
            "TC-negative-03 SKIP (daemon default-state half): /dev/fuse is absent, \
             so the daemon exits before it resolves state paths"
        );
        return;
    }

    let root = tempfile::TempDir::new().expect("test root");
    let xdg = root.path().join("xdg");
    std::fs::create_dir_all(&xdg).expect("create XDG data home");
    let mountpoint = root.path().join("mnt");
    std::fs::create_dir_all(&mountpoint).expect("create mountpoint");

    let expected_db = xdg.join("torrentfs/db/metadata.db");
    let expected_cache = xdg.join("torrentfs/cache");

    let mut run = spawn_daemon(
        root.path(),
        &mountpoint,
        None,
        None,
        Some(("XDG_DATA_HOME", &xdg)),
    );
    match wait_for_state(&mut run, &expected_db, &expected_cache, Duration::from_secs(30)) {
        StateSetup::Created => {}
        StateSetup::Exited(status) => {
            panic!(
                "the daemon exited {:?} before creating the default state tree at {}; stderr:\n{}",
                status.code(),
                xdg.join("torrentfs").display(),
                read_log(&run.stderr)
            );
        }
    }

    assert!(
        expected_db.is_file(),
        "the default database must be created at {}",
        expected_db.display()
    );
    assert!(
        expected_cache.is_dir(),
        "the default cache directory must be created at {}",
        expected_cache.display()
    );

    // Let the mount attempt settle, then stop the daemon and inspect the
    // mountpoint's underlying directory.
    let mount_deadline = Instant::now() + Duration::from_secs(15);
    let mut mounted = false;
    while Instant::now() < mount_deadline {
        if is_mount(&mountpoint) {
            mounted = true;
            break;
        }
        if run.child.try_wait().expect("poll daemon").is_some() {
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    stop_daemon(&mut run, &mountpoint);
    assert!(
        !is_mount(&mountpoint),
        "the daemon left the mount attached at {}",
        mountpoint.display()
    );

    let mut leftovers: Vec<String> = std::fs::read_dir(&mountpoint)
        .expect("read the underlying mountpoint")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.file_name().to_string_lossy().into_owned())
        // The daemon's mountpoint lock lives on the underlying directory by
        // design; it is not persistent filesystem state (C-12).
        .filter(|name| name != ".torrentfs.lock")
        .collect();
    leftovers.sort();
    assert!(
        leftovers.is_empty(),
        "default state must not be created under the mountpoint; found {leftovers:?}"
    );

    // WAL mode can leave the schema in the sidecar until shutdown; check both
    // after the daemon is stopped so the file proves state was really written.
    let wal_path = PathBuf::from(format!("{}-wal", expected_db.display()));
    let db_bytes = expected_db.metadata().map(|m| m.len()).unwrap_or(0);
    let wal_bytes = wal_path.metadata().map(|m| m.len()).unwrap_or(0);
    assert!(
        db_bytes > 0 || wal_bytes > 0,
        "the default database must hold the daemon's schema ({} or its -wal)",
        expected_db.display()
    );

    println!(
        "TC-negative-03: daemon default state at {} (db) and {} (cache); \
         mounted={mounted}; mountpoint leftover entries: {leftovers:?}",
        expected_db.display(),
        expected_cache.display()
    );
}

/// Can this process create a FUSE mount at all?  Unprivileged mounts need
/// either root/`CAP_SYS_ADMIN` or a setuid `fusermount` helper; without those
/// the forbidden-configuration half cannot observe the shadowed path.
fn mount_capability() -> Result<(), String> {
    match OpenOptions::new().read(true).write(true).open("/dev/fuse") {
        Ok(_) => {}
        Err(e) => return Err(format!("/dev/fuse is not usable: {e}")),
    }
    // SAFETY: geteuid() has no preconditions.
    if unsafe { libc::geteuid() } == 0 {
        return Ok(());
    }
    if let Some(path) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&path) {
            for name in ["fusermount3", "fusermount"] {
                let candidate = directory.join(name);
                if let Ok(metadata) = std::fs::metadata(&candidate) {
                    if metadata.permissions().mode() & 0o4000 != 0 {
                        return Ok(());
                    }
                }
            }
        }
    }
    Err("not root, no CAP_SYS_ADMIN and no setuid fusermount helper".to_string())
}

fn is_mount(mountpoint: &Path) -> bool {
    let target = std::fs::canonicalize(mountpoint).unwrap_or_else(|_| mountpoint.to_path_buf());
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };
    mountinfo.lines().any(|line| {
        let mut fields = line.split_whitespace();
        fields
            .nth(4)
            .map(|raw| Path::new(raw) == target)
            .unwrap_or(false)
    })
}

/// Exit codes the daemon uses for "this host cannot mount": missing/unusable
/// FUSE device and mount permission denied.  Those are environment limits,
/// not the forbidden-configuration behaviour under test.
const EXIT_MISSING_DEVICE: i32 = 3;
const EXIT_MOUNT_PERMISSION_DENIED: i32 = 2;
const EXIT_FUSE_DEVICE_UNUSABLE: i32 = 100;

struct DaemonRun {
    child: Child,
    stdout: PathBuf,
    stderr: PathBuf,
}

static RUN_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Spawn the daemon at `mountpoint`.  `db`/`cache` are passed only for the
/// forbidden-configuration run; the default-state run passes `None` so the
/// daemon must resolve the XDG defaults itself.  `env` overrides one variable
/// in the child environment (used for `XDG_DATA_HOME`).
fn spawn_daemon(
    root: &Path,
    mountpoint: &Path,
    config: Option<&Path>,
    db_and_cache: Option<(&Path, &Path)>,
    env: Option<(&str, &Path)>,
) -> DaemonRun {
    let run = RUN_COUNTER.fetch_add(1, Ordering::SeqCst);
    let stdout = root.join(format!("daemon-{run}.stdout"));
    let stderr = root.join(format!("daemon-{run}.stderr"));

    let mut command = Command::new(env!("CARGO_BIN_EXE_torrentfs"));
    command.arg(mountpoint);
    if let Some(config) = config {
        command.arg("--config").arg(config);
    }
    if let Some((db, cache)) = db_and_cache {
        command.arg("--db").arg(db).arg("--cache").arg(cache);
    }
    if let Some((key, value)) = env {
        command.env(key, value);
    }
    let child = command
        .stdout(Stdio::from(
            File::create(&stdout).expect("create daemon stdout"),
        ))
        .stderr(Stdio::from(
            File::create(&stderr).expect("create daemon stderr"),
        ))
        .spawn()
        .expect("spawn torrentfs daemon");
    DaemonRun {
        child,
        stdout,
        stderr,
    }
}

/// State paths deliberately inside the mountpoint: both the SQLite tree and
/// the piece cache live under `state/`, which the FUSE mount shadows.
fn forbidden_state_paths(mountpoint: &Path) -> (PathBuf, PathBuf) {
    let state = mountpoint.join("state");
    (state.join("metadata.db"), state.join("cache"))
}

fn read_log(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap_or_default()
}

/// Wait until the mount appears or the daemon exits.
enum Setup {
    Mounted,
    Exited(ExitStatus),
}

/// Outcome of waiting for the daemon's default state tree.
enum StateSetup {
    Created,
    Exited(ExitStatus),
}

fn wait_for_startup(run: &mut DaemonRun, mountpoint: &Path, timeout: Duration) -> Setup {
    let deadline = Instant::now() + timeout;
    loop {
        if is_mount(mountpoint) {
            return Setup::Mounted;
        }
        if let Some(status) = run.child.try_wait().expect("poll daemon") {
            return Setup::Exited(status);
        }
        if Instant::now() >= deadline {
            let _ = run.child.kill();
            let _ = run.child.wait();
            panic!(
                "the daemon neither mounted nor exited within {timeout:?}; stderr:\n{}",
                read_log(&run.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until both default state paths exist or the daemon exits.
fn wait_for_state(
    run: &mut DaemonRun,
    db: &Path,
    cache: &Path,
    timeout: Duration,
) -> StateSetup {
    let deadline = Instant::now() + timeout;
    loop {
        if db.is_file() && cache.is_dir() {
            return StateSetup::Created;
        }
        if let Some(status) = run.child.try_wait().expect("poll daemon") {
            return StateSetup::Exited(status);
        }
        if Instant::now() >= deadline {
            let _ = run.child.kill();
            let _ = run.child.wait();
            panic!(
                "the daemon created no state within {timeout:?}; stderr:\n{}",
                read_log(&run.stderr)
            );
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// SIGTERM the daemon, give it the documented grace, then force it down.
fn stop_daemon(run: &mut DaemonRun, mountpoint: &Path) {
    let already_exited = run
        .child
        .try_wait()
        .expect("poll daemon before SIGTERM")
        .is_some();
    if !already_exited {
        // SAFETY: kill(2) with a live child pid; the signal number is a constant.
        unsafe {
            libc::kill(run.child.id() as libc::pid_t, libc::SIGTERM);
        }
    }
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        if run
            .child
            .try_wait()
            .expect("poll daemon after SIGTERM")
            .is_some()
        {
            break;
        }
        if Instant::now() >= deadline {
            let _ = run.child.kill();
            let _ = run.child.wait();
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    // AutoUnmount normally detaches; a killed daemon can leave the mount
    // behind, so release it through the same helpers the daemon uses.
    if is_mount(mountpoint) {
        for bin in ["fusermount3", "fusermount"] {
            let _ = Command::new(bin).arg("-u").arg("-z").arg(mountpoint).output();
        }
    }
}

/// The forbidden configuration's actual behaviour is recorded, and the one
/// outcome AC-92 forbids is asserted absent: state that reads worked against
/// must not disappear on the next mount.
#[test]
fn tc_negative_03_state_inside_the_mountpoint_records_actual_behaviour() {
    if let Err(reason) = mount_capability() {
        eprintln!(
            "TC-negative-03 SKIP (mount-dependent half): {reason}; the default-path \
             assertions above still carry AC-92's first clause"
        );
        return;
    }

    let root = tempfile::TempDir::new().expect("test root");
    let mountpoint = root.path().join("mnt");
    std::fs::create_dir_all(&mountpoint).expect("create mountpoint");
    let config = root.path().join("torrentfs.toml");
    std::fs::write(
        &config,
        "[timeouts]\nread_timeout_secs = 2\npeer_discovery_wait_secs = 1\n\
         no_seeder_read_timeout_secs = 1\n",
    )
    .expect("write config");

    let (db_path, cache_path) = forbidden_state_paths(&mountpoint);
    let mut first = spawn_daemon(
        root.path(),
        &mountpoint,
        Some(&config),
        Some((&db_path, &cache_path)),
        None,
    );
    match wait_for_startup(&mut first, &mountpoint, Duration::from_secs(30)) {
        Setup::Exited(status) => {
            let stderr = read_log(&first.stderr);
            let code = status.code().unwrap_or(-1);
            if code == EXIT_MOUNT_PERMISSION_DENIED
                || code == EXIT_FUSE_DEVICE_UNUSABLE
                || code == EXIT_MISSING_DEVICE
            {
                eprintln!(
                    "TC-negative-03 SKIP (mount-dependent half): the daemon exited {code} \
                     because this host cannot mount FUSE; recorded stderr tail: {}",
                    stderr.lines().last().unwrap_or("")
                );
                return;
            }
            // Any other startup refusal is itself a visible failure, so the
            // configuration does not present a persistence illusion.
            println!(
                "TC-negative-03: forbidden state path refused at startup (exit {code}); \
                 stderr tail: {}",
                stderr.lines().last().unwrap_or("")
            );
            return;
        }
        Setup::Mounted => {
            println!(
                "TC-negative-03: daemon mounted with db/cache inside the mountpoint; \
                 stdout={} stderr={}",
                first.stdout.display(),
                first.stderr.display()
            );
        }
    }

    // Ingest one torrent through the mounted metadata/ namespace and record
    // whether the projection appears.
    let (torrent, content) = create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "negative-03-seed.txt",
    );
    let metadata_file = mountpoint.join("metadata/seed.torrent");
    std::fs::write(&metadata_file, &torrent).expect("write torrent through metadata/");

    let seed_root = mountpoint.join("data/seed.torrent");
    let list_deadline = Instant::now() + Duration::from_secs(20);
    while !seed_root.exists() && Instant::now() < list_deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let listed = seed_root.exists();
    println!("TC-negative-03: projection visible while mounted = {listed}");

    // A read is the usability signal: bytes served here must survive the
    // remount.  With the piece cache shadowed by the mount, an error is the
    // expected (and visible) outcome.
    let mut read_ok = false;
    if listed {
        let file = seed_root.join("negative-03-seed.txt");
        match File::open(&file) {
            Ok(mut handle) => {
                let mut bytes = Vec::new();
                match handle.read_to_end(&mut bytes) {
                    Ok(_) if bytes.len() == content.len() => {
                        read_ok = true;
                        println!(
                            "TC-negative-03: read served {} bytes while mounted",
                            bytes.len()
                        );
                    }
                    Ok(_) => println!(
                        "TC-negative-03: read returned {} of {} bytes",
                        bytes.len(),
                        content.len()
                    ),
                    Err(e) => println!(
                        "TC-negative-03: read failed with {:?} (errno {:?})",
                        e.kind(),
                        e.raw_os_error()
                    ),
                }
            }
            Err(e) => println!(
                "TC-negative-03: opening the projected file failed with {:?} (errno {:?})",
                e.kind(),
                e.raw_os_error()
            ),
        }
    }

    stop_daemon(&mut first, &mountpoint);

    // Remount with the same forbidden configuration and record whether the
    // state survived.
    let mut second = spawn_daemon(
        root.path(),
        &mountpoint,
        Some(&config),
        Some((&db_path, &cache_path)),
        None,
    );
    let survived = match wait_for_startup(&mut second, &mountpoint, Duration::from_secs(30)) {
        Setup::Exited(status) => {
            println!(
                "TC-negative-03: remount with the forbidden configuration failed \
                 (exit {:?}); stderr tail: {}",
                status.code(),
                read_log(&second.stderr).lines().last().unwrap_or("")
            );
            false
        }
        Setup::Mounted => {
            let deadline = Instant::now() + Duration::from_secs(20);
            let root = mountpoint.join("data/seed.torrent");
            while !root.exists() && Instant::now() < deadline {
                std::thread::sleep(Duration::from_millis(100));
            }
            let survived = root.exists();
            println!("TC-negative-03: projection survived the remount = {survived}");
            stop_daemon(&mut second, &mountpoint);
            survived
        }
    };

    // AC-92's violation is the persistence illusion: content was served while
    // mounted, yet the state is gone after the remount.  A visible failure
    // (no projection, read error, or a refused restart) meets the criterion.
    assert!(
        !read_ok || survived,
        "the forbidden state layout served {} bytes, but the projection and state \
         vanished after the remount — a persistence illusion",
        content.len()
    );

    println!(
        "TC-negative-03: recorded forbidden-config behaviour — listed={listed}, \
         read_ok={read_ok}, survived_remount={survived}"
    );
}

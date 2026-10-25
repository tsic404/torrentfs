//! Design acceptance case TC-config-05: the offline `--config-check` only
//! validates — it creates no mount point, no state database and no cache
//! directory, and needs no FUSE device (`docs/03-acceptance.md` §G AC-64;
//! `docs/02-contracts.md` §C-1.1.4, §C-1.2, §C-8.1.6, §C-10.4).
//!
//! Each case asserts the binary's own exit code and the absence of every state
//! path under a sandboxed home; the FUSE case masks `/dev` in a private mount
//! namespace and reports SKIP where the environment cannot create one.

use std::ffi::OsStr;
use std::io::Write;
use std::path::PathBuf;
use std::process::{Command, Output};

pub const TC_ID: &str = "TC-config-05";

const VALID_CONFIG: &str = "[connections]\nmax_connections = 200\n";

/// One representative per rejected family (C-8.1.2/C-8.1.4/C-8.1.5): the
/// precheck must exit 1 for each, not merely "non-zero".
const INVALID_CONFIGS: &[(&str, &str)] = &[
    ("invalid TOML", "this is not valid toml {{{"),
    ("unknown section", "[bogus_section]\nfoo = 1\n"),
    ("unknown key", "[connections]\nnot_a_key = 1\n"),
    (
        "out-of-range value",
        "[connections]\nmax_connections = -1\n",
    ),
    (
        "out-of-range enum",
        "[algorithms]\nchoking_algorithm = 999\n",
    ),
];

/// A run's home, its config file, and every state path the daemon would create
/// on the way to a mount, all inside one temp directory.
struct Sandbox {
    _dir: tempfile::TempDir,
    root: PathBuf,
    config: PathBuf,
}

impl Sandbox {
    fn new(config_body: &str) -> Self {
        let dir = tempfile::TempDir::new().expect("create sandbox");
        let root = dir.path().to_path_buf();
        let config = root.join("config.toml");
        let mut file = std::fs::File::create(&config).expect("create config file");
        write!(file, "{config_body}").expect("write config file");
        Sandbox {
            _dir: dir,
            root,
            config,
        }
    }

    fn db_path(&self) -> PathBuf {
        self.root.join("db").join("metadata.db")
    }

    fn cache_path(&self) -> PathBuf {
        self.root.join("cache")
    }

    fn xdg_state_dir(&self) -> PathBuf {
        self.root.join("xdg").join("torrentfs")
    }

    fn home_state_dir(&self) -> PathBuf {
        self.root
            .join("home")
            .join(".local")
            .join("share")
            .join("torrentfs")
    }

    /// Invoke the shipped binary with the default user-data locations
    /// redirected under this sandbox.
    fn run(&self, extra: &[&OsStr]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_torrentfs"))
            .args(["--config-check", "--config"])
            .arg(&self.config)
            .args(extra)
            .env("XDG_DATA_HOME", self.root.join("xdg"))
            .env("HOME", self.root.join("home"))
            .env("TMPDIR", self.root.join("tmp"))
            .output()
            .expect("run torrentfs")
    }

    /// Every path the daemon would create for a mount or for persistence.
    fn state_paths(&self) -> Vec<PathBuf> {
        let db = self.db_path();
        let db_parent = db.parent().expect("db path has a parent").to_path_buf();
        vec![
            db,
            db_parent,
            self.cache_path(),
            self.xdg_state_dir(),
            self.home_state_dir(),
        ]
    }
}

fn assert_no_state(sandbox: &Sandbox, label: &str) {
    for path in sandbox.state_paths() {
        assert!(
            !path.exists(),
            "{label}: the offline precheck must not create {}",
            path.display()
        );
    }
}

fn describe(out: &Output) -> String {
    format!(
        "stdout={} stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

/// A legal file exits 0 and leaves the state database and the cache directory
/// untouched even when both are named on the command line — the precheck
/// validates the file and stops (C-1.1.4, C-1.2.1, C-8.1.6).
#[test]
fn tc_config_05_valid_config_exits_zero_without_creating_state() {
    let sandbox = Sandbox::new(VALID_CONFIG);
    let db = sandbox.db_path();
    let cache = sandbox.cache_path();
    let out = sandbox.run(&[
        OsStr::new("--db"),
        db.as_os_str(),
        OsStr::new("--cache"),
        cache.as_os_str(),
    ]);
    assert_eq!(
        out.status.code(),
        Some(0),
        "a legal config must pass the precheck with exit 0: {}",
        describe(&out)
    );
    assert_no_state(&sandbox, "valid config");
}

/// Each rejected family exits exactly 1 (C-1.2.2) and still leaves no state
/// behind: the failure happens in the loader, before any mount or persistence
/// setup.
#[test]
fn tc_config_05_invalid_config_exits_one_without_creating_state() {
    for (label, body) in INVALID_CONFIGS {
        let sandbox = Sandbox::new(body);
        let db = sandbox.db_path();
        let cache = sandbox.cache_path();
        let out = sandbox.run(&[
            OsStr::new("--db"),
            db.as_os_str(),
            OsStr::new("--cache"),
            cache.as_os_str(),
        ]);
        assert_eq!(
            out.status.code(),
            Some(1),
            "{label}: an illegal config must fail the precheck with exit 1: {}",
            describe(&out)
        );
        assert_no_state(&sandbox, label);
    }
}

/// A mount point and the offline precheck are mutually exclusive (C-1.2.3,
/// C-10.4): the combination is a usage error, so the mount point path is never
/// created and no mount appears for it.
#[test]
fn tc_config_05_precheck_never_creates_the_mountpoint() {
    let sandbox = Sandbox::new(VALID_CONFIG);
    let mountpoint = sandbox.root.join("mnt").join("target");
    let out = sandbox.run(&[mountpoint.as_os_str()]);
    assert_eq!(
        out.status.code(),
        Some(2),
        "precheck with a mount point is a usage error: {}",
        describe(&out)
    );
    assert!(
        !mountpoint.exists(),
        "the precheck must not create the mount point {}",
        mountpoint.display()
    );
    assert!(
        !mountpoint
            .parent()
            .expect("mount point has a parent")
            .exists(),
        "the precheck must not create any mount point parent directory"
    );
    let mountinfo = std::fs::read_to_string("/proc/self/mountinfo").expect("read mountinfo");
    assert!(
        !mountinfo.contains(&mountpoint.to_string_lossy().into_owned()),
        "no mount may appear for the precheck's mount point {}",
        mountpoint.display()
    );
    assert_no_state(&sandbox, "precheck with mount point");
}

fn private_mount_namespace_available() -> bool {
    Command::new("unshare")
        .args(["-m", "-r", "true"])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

/// A legal file exits 0 where `/dev/fuse` cannot exist: the FUSE-device
/// existence probe sits after the precheck's early exit (C-1.1.4), so the
/// offline path never depends on the device.
#[test]
fn tc_config_05_precheck_needs_no_fuse_device() {
    if !private_mount_namespace_available() {
        eprintln!(
            "SKIP TC-config-05 fuse-device case: this environment cannot create a private mount namespace"
        );
        return;
    }

    let sandbox = Sandbox::new(VALID_CONFIG);
    let script = "mount -t tmpfs none /dev || exit 8; \
                  test -e /dev/fuse && exit 9; \
                  exec \"$TC_TORRENTFS_BIN\" --config-check --config \"$TC_TORRENTFS_CONFIG\"";
    let out = Command::new("unshare")
        .args(["-m", "-r", "sh", "-c", script])
        .env("TC_TORRENTFS_BIN", env!("CARGO_BIN_EXE_torrentfs"))
        .env("TC_TORRENTFS_CONFIG", &sandbox.config)
        .env("XDG_DATA_HOME", sandbox.root.join("xdg"))
        .env("HOME", sandbox.root.join("home"))
        .env("TMPDIR", sandbox.root.join("tmp"))
        .output()
        .expect("run the precheck with /dev masked");
    match out.status.code() {
        Some(0) => {}
        Some(8) | Some(9) => {
            eprintln!(
                "SKIP TC-config-05 fuse-device case: the namespace could not mask /dev (exit {:?})",
                out.status.code()
            );
            return;
        }
        other => panic!(
            "the precheck must exit 0 with no FUSE device, got {other:?}: {}",
            describe(&out)
        ),
    }
    assert_no_state(&sandbox, "fuse device masked");
}

//! Exit status of the mountpoint-setup failure path.
//!
//! Creating the mountpoint used to be an unwinding `expect`, so a failure
//! exited with the runtime panic status — the same status the mountpoint lock
//! uses for "another instance is running" (C-1.2.6). A supervisor could not
//! tell a lost lock from a broken mountpoint. The failure now reports a
//! generic mount failure (C-1.2.2) with its own diagnostic.
//!
//! The entry probe is an existence check on the FUSE device, and the failure
//! happens before the device is ever opened, so a private mount namespace with
//! a plain file standing in for the device exercises the real binary on a
//! machine without FUSE. When this environment cannot create such a namespace
//! the case reports SKIP; the mapping itself is pinned by the `--bin torrentfs`
//! unit tests, which need no device.

use std::process::{Command, Output};

fn logs(out: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    )
}

fn private_mount_namespace_available() -> bool {
    Command::new("unshare")
        .args(["-m", "-r", "true"])
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

#[test]
fn uncreatable_mountpoint_exits_generic_mount_failure_not_the_panic_status() {
    if !private_mount_namespace_available() {
        eprintln!(
            "SKIP mountpoint exit case: this environment cannot create a private mount namespace"
        );
        return;
    }

    let dir = tempfile::TempDir::new().unwrap();
    let config = dir.path().join("config.toml");
    std::fs::write(&config, "").unwrap();

    // A regular file cannot become a parent directory, so creating the
    // mountpoint below it fails deterministically.
    let blocker = dir.path().join("blocker");
    std::fs::write(&blocker, b"not a directory").unwrap();
    let mountpoint = blocker.join("mnt");

    // The namespace gets a fresh `/dev` holding a regular file named `fuse`:
    // the entry only probes for existence before this branch, never opening
    // the device, and the branch fails before any mount attempt.
    let script = "mount -t tmpfs none /dev || exit 8; \
                  touch /dev/fuse || exit 9; \
                  exec \"$TC_TORRENTFS_BIN\" --config \"$TC_TORRENTFS_CONFIG\" \
                  \"$TC_TORRENTFS_MOUNTPOINT\"";
    let out = Command::new("unshare")
        .args(["-m", "-r", "sh", "-c", script])
        .env("TC_TORRENTFS_BIN", env!("CARGO_BIN_EXE_torrentfs"))
        .env("TC_TORRENTFS_CONFIG", &config)
        .env("TC_TORRENTFS_MOUNTPOINT", &mountpoint)
        .output()
        .expect("run torrentfs with a fabricated FUSE device");

    match out.status.code() {
        Some(8) | Some(9) => {
            eprintln!(
                "SKIP mountpoint exit case: the namespace could not fabricate /dev/fuse (exit {:?})",
                out.status.code()
            );
            return;
        }
        _ => {}
    }

    let logs = logs(&out);
    assert_eq!(
        out.status.code(),
        Some(1),
        "an uncreatable mountpoint is a generic mount failure, got {logs}"
    );
    assert_ne!(
        out.status.code(),
        Some(101),
        "the lock-contention status must not be reused for a mountpoint failure, got {logs}"
    );
    assert!(
        logs.contains("Failed to create mountpoint"),
        "the diagnostic must name the failed step, got {logs}"
    );
    assert!(
        !logs.contains("panicked"),
        "the failure must not unwind, got {logs}"
    );
}

//! Design acceptance case TC-recovery-03: SIGTERM stops the daemon with exit
//! code 0, drains the download engine, flushes the cache metadata and
//! unmounts FUSE (`docs/03-acceptance.md` §I AC-82).
//!
//! Observable criteria: exit code 0; no FUSE mount left in the mount table;
//! the cache metadata read *after* the process is gone records the piece the
//! mount served (C-5.2.2 makes shutdown the authoritative flush point, and
//! C-5.2.4 forbids judging the live daemon's metadata by a single read).

use crate::common::{
    daemon_config, fuse_mount_blocker, ingest_torrent_through_mount, mountinfo_fuse_count,
    read_when_ready, unique_torrent, wait_for_path, wait_for_projection, wave_slot, Daemon,
    MountGate, TestHarness, DAEMON_MOUNT_TIMEOUT,
};
use std::fs;
use std::time::Duration;

pub const TC_ID: &str = "TC-recovery-03";

/// Parsing and persisting one fixture torrent.
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// Reading the seeded file through the mount (peer discovery included).
const READ_TIMEOUT: Duration = Duration::from_secs(120);
/// A graceful stop on an idle mount.
const EXIT_TIMEOUT: Duration = Duration::from_secs(60);

#[test]
fn tc_recovery_03_sigterm_exits_zero_and_persists_cache_metadata() {
    if let Some(blocker) = fuse_mount_blocker() {
        eprintln!("SKIP TC-recovery-03: this environment cannot mount FUSE ({blocker})");
        return;
    }
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("recovery-03-graceful.txt"));

    let mut daemon = Daemon::start(Some(&daemon_config(30, 15, 30)), "info");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!("SKIP TC-recovery-03: the environment refused the FUSE mount\n{detail}");
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not mount:\n{detail}"),
    }
    let mountpoint = daemon.mountpoint();

    ingest_torrent_through_mount(&mountpoint, "graceful.torrent", &harness.torrent_data);
    assert!(
        wait_for_projection(&mountpoint, "graceful.torrent", PROJECTION_TIMEOUT),
        "projection never appeared\n{}",
        daemon.output()
    );

    // Serve one piece through the mount so the cache has metadata to flush.
    let data_file = mountpoint
        .join("data")
        .join("graceful.torrent")
        .join(harness.info.name());
    assert!(
        wait_for_path(&data_file, PROJECTION_TIMEOUT),
        "projected file {} never appeared\n{}",
        data_file.display(),
        daemon.output()
    );
    let content = read_when_ready(&data_file, READ_TIMEOUT).unwrap_or_else(|e| {
        panic!(
            "read {} before SIGTERM: {e}\n{}",
            data_file.display(),
            daemon.output()
        )
    });
    assert_eq!(
        content.len(),
        harness.file_content.len(),
        "read the wrong byte count before SIGTERM"
    );

    // ── SIGTERM ──────────────────────────────────────────────────────────────
    assert!(daemon.signal(libc::SIGTERM), "daemon already exited before SIGTERM");
    let status = daemon.wait_for_exit(EXIT_TIMEOUT).unwrap_or_else(|| {
        panic!(
            "daemon did not exit within {EXIT_TIMEOUT:?} after SIGTERM\n{}",
            daemon.output()
        )
    });
    assert_eq!(
        status.code(),
        Some(0),
        "a graceful SIGTERM shutdown must exit 0, got {status}\n{}",
        daemon.output()
    );

    // ── no mount residue ─────────────────────────────────────────────────────
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        0,
        "SIGTERM left the FUSE mount in the mount table\n{}",
        daemon.output()
    );

    // ── cache metadata is the shutdown flush ─────────────────────────────────
    let metadata_path = daemon.cache_dir().join("cache_metadata.txt");
    let metadata = fs::read_to_string(&metadata_path).unwrap_or_else(|e| {
        panic!(
            "cache metadata {} unreadable after shutdown: {e}\n{}",
            metadata_path.display(),
            daemon.output()
        )
    });
    let info_hash = hex::encode(
        harness
            .info
            .info_hash()
            .expect("fixture torrent has an info hash"),
    );
    let piece_key = format!("{info_hash}:piece:0");
    let recorded = metadata.lines().find_map(|line| {
        let mut fields = line.split('\t');
        (fields.next() == Some(piece_key.as_str())).then(|| {
            (
                fields.next().map(str::to_string),
                fields.next().map(str::to_string),
            )
        })
    });
    let (_, size) = recorded.unwrap_or_else(|| {
        panic!(
            "shutdown flush did not persist piece {piece_key}; cache metadata holds:\n{metadata}"
        )
    });
    assert_eq!(
        size.as_deref(),
        Some("16384"),
        "the persisted piece carries the wrong size; cache metadata holds:\n{metadata}"
    );
}

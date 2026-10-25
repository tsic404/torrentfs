//! Design acceptance case TC-recovery-02: a torrent directory holding
//! thousands of files lists, descends, recurses and resolves by name within
//! seconds, without hanging (`docs/03-acceptance.md` §I AC-81).
//!
//! The fixture is one multi-file torrent: 3000 files directly in the torrent
//! directory plus 20 subdirectories of 50 files each.  A listing that scans
//! the directory per entry — or materialises it before answering a lookup —
//! turns the root readdir into hours of work at this size (F2), so the
//! per-operation ceilings below separate "seconds" from "hung".
//!
//! The case drives the real binary through a FUSE mount: "enter, list,
//! recurse, find" are namespace operations, not engine calls.  Environments
//! that cannot mount FUSE report a skip with the reason.

use crate::common::{
    fuse_mount_blocker, ingest_torrent_through_mount, mountinfo_fuse_count, wait_for_projection,
    wave_slot, Daemon, MountGate, DAEMON_MOUNT_TIMEOUT,
};
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

pub const TC_ID: &str = "TC-recovery-02";

/// Files directly inside the torrent directory.
const FILES_AT_ROOT: usize = 3000;
/// Subdirectories inside the torrent directory.
const SUBDIRS: usize = 20;
/// Files inside each subdirectory.
const FILES_PER_SUBDIR: usize = 50;
/// Declared size of every fixture file.
const FILE_BYTES: usize = 32;
/// Piece length of the fixture torrent.
const PIECE_LEN: usize = 16 * 1024;
/// The `.torrent` file name dropped into `metadata/`.
const TORRENT_FILE: &str = "many-files.torrent";
/// The torrent's internal name; the projection appears as
/// `data/many-files.torrent/`.
const TORRENT_NAME: &str = "many-files";
/// Upper bound for parsing and persisting a 4000-file torrent.
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(180);

/// One `stat`-level namespace operation (entering the directory, resolving a
/// file by name) must finish in seconds.  The failure this excludes — a
/// per-entry linear scan — takes hours here, so the ceiling is generous by
/// design while still being seconds.
const SINGLE_OP_BUDGET: Duration = Duration::from_secs(2);
/// How long to wait before a timed namespace operation, so the kernel's
/// one-second entry and attribute cache (C-1.3.4) has certainly expired: a
/// measurement inside that window is answered from the dcache, not by the
/// daemon.  The 200 ms margin keeps the expiry from being observed a tick
/// early.
const CACHE_EXPIRY_WAIT: Duration = Duration::from_millis(1200);
/// Listing the 3000-entry torrent directory once.
const LIST_BUDGET: Duration = Duration::from_secs(5);
/// Walking the whole projection: 4000 files across 21 directories.
const WALK_BUDGET: Duration = Duration::from_secs(5);
/// Entries in the settled torrent directory: every file plus every
/// subdirectory.  (`.stats` sits on the projection root, not on the torrent's
/// own directory.)
const EXPECTED_ENTRIES: usize = FILES_AT_ROOT + SUBDIRS;

/// Deterministic content for fixture file `index` (hash-only fixture: the
/// daemon never downloads it, but a real piece hash keeps the metadata
/// honest).
fn file_content(index: usize) -> [u8; FILE_BYTES] {
    let mut content = [0u8; FILE_BYTES];
    for (offset, byte) in content.iter_mut().enumerate() {
        *byte = (index as u8)
            .wrapping_mul(31)
            .wrapping_add((offset as u8).wrapping_mul(7))
            .wrapping_add(11);
    }
    content
}

/// Build the multi-file fixture torrent: bencode v1, one `files` entry per
/// projected file, SHA-1 piece hashes over the concatenated content in the
/// canonical path order libtorrent concatenates a torrent's files.
fn build_many_files_torrent() -> Vec<u8> {
    let mut paths: Vec<(String, usize)> =
        Vec::with_capacity(FILES_AT_ROOT + SUBDIRS * FILES_PER_SUBDIR);
    for index in 0..FILES_AT_ROOT {
        paths.push((format!("f{index:04}.bin"), index));
    }
    for dir in 0..SUBDIRS {
        for file in 0..FILES_PER_SUBDIR {
            paths.push((
                format!("d{dir:02}/f{file:04}.bin"),
                FILES_AT_ROOT + dir * FILES_PER_SUBDIR + file,
            ));
        }
    }

    // `files` is path-ordered (BEP 3 names are byte-sorted) and the piece
    // hashes cover the concatenation in that same order, so the fixture is a
    // consistent torrent rather than merely a list of entries.
    paths.sort_by(|left, right| left.0.as_bytes().cmp(right.0.as_bytes()));

    let mut content = Vec::with_capacity(paths.len() * FILE_BYTES);
    for (_, index) in &paths {
        content.extend_from_slice(&file_content(*index));
    }
    let mut pieces = Vec::new();
    for chunk in content.chunks(PIECE_LEN) {
        use sha1_smol::Sha1;
        let mut hasher = Sha1::new();
        hasher.update(chunk);
        pieces.extend_from_slice(&hasher.digest().bytes());
    }

    let announce_url = "http://127.0.0.1:1/announce";
    let mut torrent = Vec::new();
    torrent.push(b'd');
    torrent.extend_from_slice(b"8:announce");
    torrent.extend_from_slice(announce_url.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(announce_url.as_bytes());
    torrent.extend_from_slice(b"4:infod");
    torrent.extend_from_slice(b"5:filesl");
    for (path, _) in &paths {
        torrent.extend_from_slice(b"d6:lengthi");
        torrent.extend_from_slice(FILE_BYTES.to_string().as_bytes());
        torrent.push(b'e');
        torrent.extend_from_slice(b"4:pathl");
        for component in path.split('/') {
            torrent.extend_from_slice(component.len().to_string().as_bytes());
            torrent.push(b':');
            torrent.extend_from_slice(component.as_bytes());
        }
        torrent.push(b'e');
        torrent.push(b'e');
    }
    torrent.push(b'e');
    torrent.extend_from_slice(format!("4:name{}:{}", TORRENT_NAME.len(), TORRENT_NAME).as_bytes());
    torrent.extend_from_slice(b"12:piece lengthi");
    torrent.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    torrent.push(b'e');
    torrent.extend_from_slice(b"6:pieces");
    torrent.extend_from_slice(pieces.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(&pieces);
    torrent.extend_from_slice(b"ee");
    torrent
}

/// Run `operation`, returning its duration.
fn timed<T>(operation: impl FnOnce() -> T) -> (T, Duration) {
    let started = Instant::now();
    let value = operation();
    (value, started.elapsed())
}

/// Wait until `directory` lists the torrent's own file table.
///
/// C-2.5.3 lists a freshly delivered torrent in a temporary state from close
/// to the background persist landing: the projection exists, but its entries
/// are not yet the torrent's file table.  AC-81 judges the settled namespace,
/// so this bounded wait skips only that contract-sanctioned window; the
/// measurements after it stay exact.
fn wait_for_file_table(directory: &Path, expected: usize, timeout: Duration) -> usize {
    let deadline = Instant::now() + timeout;
    let mut seen = 0;
    while Instant::now() < deadline {
        seen = fs::read_dir(&directory)
            .map(|entries| entries.count())
            .unwrap_or(0);
        if seen >= expected {
            return seen;
        }
        thread::sleep(Duration::from_millis(100));
    }
    seen
}

/// Directories under `root` plus every file name, walked breadth-first.
fn recursive_walk(root: &Path) -> (BTreeSet<String>, BTreeSet<String>, Duration) {
    let (result, elapsed) = timed(|| {
        let mut pending = vec![root.to_path_buf()];
        let mut directories = BTreeSet::new();
        let mut files = BTreeSet::new();
        while let Some(directory) = pending.pop() {
            for entry in fs::read_dir(&directory)
                .unwrap_or_else(|e| panic!("read_dir {}: {e}", directory.display()))
            {
                let entry = entry.expect("directory entry");
                let path = entry.path();
                let file_type = entry.file_type().expect("entry file type");
                let relative = path
                    .strip_prefix(root)
                    .expect("walked path under root")
                    .to_string_lossy()
                    .to_string();
                if file_type.is_dir() {
                    directories.insert(relative);
                    pending.push(path);
                } else if relative != ".stats" {
                    files.insert(relative);
                }
            }
        }
        (directories, files)
    });
    let (directories, files) = result;
    (directories, files, elapsed)
}

#[test]
fn tc_recovery_02_large_directory_answers_within_seconds() {
    if let Some(blocker) = fuse_mount_blocker() {
        eprintln!("SKIP TC-recovery-02: this environment cannot mount FUSE ({blocker})");
        return;
    }
    let _slot = wave_slot();

    let mut daemon = Daemon::start(Some(&crate::common::daemon_config(30, 15, 30)), "info");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!("SKIP TC-recovery-02: the environment refused the FUSE mount\n{detail}");
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not mount:\n{detail}"),
    }
    let mountpoint = daemon.mountpoint();

    ingest_torrent_through_mount(&mountpoint, TORRENT_FILE, &build_many_files_torrent());
    assert!(
        wait_for_projection(&mountpoint, TORRENT_FILE, PROJECTION_TIMEOUT),
        "projection of a {} file torrent never appeared\n{}",
        FILES_AT_ROOT + SUBDIRS * FILES_PER_SUBDIR,
        daemon.output()
    );
    // The projection root holds the torrent's own entry: `.stats` plus the
    // directory named after the torrent, which is where a multi-file torrent's
    // relative paths start (a single-file torrent puts the file there).
    let projection_root = mountpoint.join("data").join(TORRENT_FILE);
    let torrent_root = projection_root.join(TORRENT_NAME);
    // The projection exists before the background persist lands (C-2.5.3: it
    // is listed in a temporary state, then switches to the real tree).  The
    // measurements below judge the settled namespace, so wait for the file
    // table — the wait is bounded and changes no assertion.
    let settled = wait_for_file_table(&torrent_root, EXPECTED_ENTRIES, PROJECTION_TIMEOUT);
    assert!(
        settled >= EXPECTED_ENTRIES,
        "{} never listed the torrent's file table ({settled} of {EXPECTED_ENTRIES} entries; \
         the temporary listing of C-2.5.3 never gave way to the real tree)\n{}",
        torrent_root.display(),
        daemon.output()
    );

    // ── enter ────────────────────────────────────────────────────────────────
    // Every probe above resolved this dentry, and the kernel caches the answer
    // for one second (C-1.3.4): measured now, the stat below would be a cache
    // hit that says nothing about the filesystem.  Waiting the cache out makes
    // the entry cost a LOOKUP, which is the operation a per-entry scan would
    // make take hours.
    thread::sleep(CACHE_EXPIRY_WAIT);
    let (metadata, entered) = timed(|| fs::metadata(&torrent_root));
    let metadata = metadata.unwrap_or_else(|e| panic!("stat {}: {e}", torrent_root.display()));
    assert!(metadata.is_dir(), "{} is not a directory", torrent_root.display());
    assert!(
        entered <= SINGLE_OP_BUDGET,
        "entering the torrent directory took {entered:?} (budget {SINGLE_OP_BUDGET:?}, cold lookup)"
    );

    // ── list ─────────────────────────────────────────────────────────────────
    let (listed, listing) = timed(|| {
        fs::read_dir(&torrent_root)
            .unwrap_or_else(|e| panic!("read_dir {}: {e}", torrent_root.display()))
            .map(|entry| entry.expect("directory entry").file_name())
            .collect::<BTreeSet<_>>()
    });
    let mut expected: BTreeSet<std::ffi::OsString> = (0..FILES_AT_ROOT)
        .map(|index| format!("f{index:04}.bin").into())
        .collect();
    expected.extend((0..SUBDIRS).map(|dir| format!("d{dir:02}").into()));
    assert_eq!(
        listed.len(),
        expected.len(),
        "torrent directory listed {} entries, expected {} (complete output is required); \
         listed: {:?}",
        listed.len(),
        expected.len(),
        listed.iter().take(10).collect::<Vec<_>>()
    );
    assert_eq!(listed, expected, "torrent directory listing is incomplete or renamed");
    assert!(
        listing <= LIST_BUDGET,
        "listing {} entries took {listing:?} (budget {LIST_BUDGET:?})",
        listed.len()
    );

    // ── recursive listing ────────────────────────────────────────────────────
    let (directories, files, walked) = recursive_walk(&torrent_root);
    assert_eq!(
        directories.len(),
        SUBDIRS,
        "recursive listing found {} directories, expected {SUBDIRS}",
        directories.len()
    );
    assert_eq!(
        files.len(),
        FILES_AT_ROOT + SUBDIRS * FILES_PER_SUBDIR,
        "recursive listing found {} files, expected {} (output complete)",
        files.len(),
        FILES_AT_ROOT + SUBDIRS * FILES_PER_SUBDIR
    );
    assert!(
        walked <= WALK_BUDGET,
        "recursive listing took {walked:?} (budget {WALK_BUDGET:?})"
    );

    // ── find by name ─────────────────────────────────────────────────────────
    // The walk above listed d07 with plain READDIR, which does not instantiate
    // entries: this is the first name resolution of the file, so it is a real
    // LOOKUP rather than a dcache hit.
    let nested = torrent_root.join("d07").join("f0042.bin");
    let (nested_metadata, found) = timed(|| fs::metadata(&nested));
    let nested_metadata =
        nested_metadata.unwrap_or_else(|e| panic!("stat {}: {e}", nested.display()));
    assert!(nested_metadata.is_file(), "{} is not a regular file", nested.display());
    assert_eq!(
        nested_metadata.len(),
        FILE_BYTES as u64,
        "looked-up file reports the wrong size"
    );
    assert!(
        found <= SINGLE_OP_BUDGET,
        "resolving {} took {found:?} (budget {SINGLE_OP_BUDGET:?})",
        nested.display()
    );

    // Teardown is part of the criterion's "does not hang": the graceful stop
    // must still work on a mount carrying thousands of entries.
    assert!(daemon.signal(libc::SIGTERM), "daemon already exited before SIGTERM");
    let status = daemon
        .wait_for_exit(Duration::from_secs(60))
        .unwrap_or_else(|| panic!("daemon did not exit after SIGTERM\n{}", daemon.output()));
    assert_eq!(status.code(), Some(0), "daemon exited with {status}");
    assert_eq!(
        mountinfo_fuse_count(&mountpoint),
        0,
        "the large-directory mount was left behind\n{}",
        daemon.output()
    );
}

//! Design acceptance case TC-negative-02: directory listing and lookup are
//! not O(directory size) scans and never wait on the network, so a
//! multi-thousand-entry directory stays second-scale and listing stays
//! responsive while a read is parked on the swarm
//! (`docs/03-acceptance.md` §J AC-91; `docs/02-contracts.md` §C-11.2/§C-11.4/
//! §C-11.6/§C-2.5.6).
//!
//! The contract fixes the observable bounds, not an implementation shape, so
//! the assertions are wall-clock limits on the service's own lookup/readdir
//! calls plus the listing invariants (strictly increasing offsets, middle-offset
//! continuation with no repetition, no over-long page).  The fixture puts
//! 3 000 entries in one directory: a listing that materialises the directory
//! per page and ignores the kernel's resume offset cannot pass the
//! middle-offset assertion, and a scan-heavy path shows up in the wall clock.
//! The concurrent-read half mirrors C-11.5: FUSE dispatch is serial, therefore
//! a read parked on the swarm must leave the dispatcher free — a pending read
//! returns immediately and the following listing is not held behind it.

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::fs_types::{DirEntry, FileKind};
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::fuse::FsService;
use torrentfs::TorrentInfo;

use crate::common::{
    build_multi_file_torrent, ingest_torrent, quiet_config, readdir_from, service_with_config,
    wave_slot,
};

pub const TC_ID: &str = "TC-negative-02";

/// One directory carries the bulk of the tree; AC-91's "大目录" requires
/// thousands of entries in a single directory, not thousands spread thin.
const BULK_DIRECTORY_FILES: usize = 3_000;
const SMALL_DIRECTORY_COUNT: usize = 10;
const SMALL_DIRECTORY_FILES: usize = 100;
const TOTAL_FILES: usize = BULK_DIRECTORY_FILES + SMALL_DIRECTORY_COUNT * SMALL_DIRECTORY_FILES;
/// One directory's listing (including a middle-offset continuation) and one
/// name resolution must stay well inside "second-scale" on a loaded runner.
const PER_OPERATION_BUDGET: Duration = Duration::from_secs(2);
/// Walking the whole 4 000-entry tree (11 listings + 11 lookups).
const TREE_BUDGET: Duration = Duration::from_secs(10);

/// Time one operation and fail when it exceeds its budget.
fn timed<T>(label: &str, budget: Duration, operation: impl FnOnce() -> T) -> T {
    let start = Instant::now();
    let value = operation();
    let elapsed = start.elapsed();
    assert!(
        elapsed < budget,
        "{label} took {elapsed:?}, exceeding the {budget:?} budget"
    );
    value
}

/// The observable part of one entry, comparable across two listings.
type EntryKey = (u64, i64, FileKind, String);

fn entry_key(entry: &DirEntry) -> EntryKey {
    (
        entry.ino,
        entry.offset,
        entry.kind,
        entry.name.clone(),
    )
}

/// List a directory and assert the C-2.5.6 page invariants the kernel relies
/// on: every returned offset is strictly greater than the previous one; a
/// continuation from a middle offset starts exactly at the following entry and
/// paging on from there reaches the end with nothing repeated or skipped (that
/// is how the kernel pages, and a materialise-per-call implementation that
/// ignores the offset repeats rows here); and the page after the last offset is
/// empty.
fn list_page(service: &mut FsService, ino: u64) -> Vec<DirEntry> {
    let entries = readdir_from(service, ino, -1);
    let mut previous = 0;
    for entry in &entries {
        assert!(
            entry.offset > previous,
            "readdir offsets must be strictly increasing: {:?} after {}",
            entry,
            previous
        );
        previous = entry.offset;
    }

    // The kernel continues a listing from the last offset it accepted; take a
    // middle entry's offset so the continuation is a real second page, not a
    // single-call shortcut.
    let pivot = entries.len() / 2;
    let resume_from = entries[pivot].offset;
    let resumed = timed(
        &format!("resume inode {ino} from offset {resume_from}"),
        PER_OPERATION_BUDGET,
        || service.readdir(ino, resume_from),
    )
    .unwrap_or_else(|e| panic!("continue listing inode {ino} after {resume_from}: {e:?}"));
    if pivot + 1 < entries.len() {
        assert_eq!(
            resumed.first().map(entry_key),
            Some(entry_key(&entries[pivot + 1])),
            "a listing continued from a middle offset must start at the next \
             entry — no repetition, nothing skipped"
        );
    }
    let remainder: Vec<EntryKey> = readdir_from(service, ino, resume_from)
        .iter()
        .map(entry_key)
        .collect();
    let expected: Vec<EntryKey> = entries[pivot + 1..].iter().map(entry_key).collect();
    assert_eq!(
        remainder, expected,
        "paging on from a middle offset must cover exactly the remaining \
         entries — no repetition, nothing skipped"
    );

    let next = service
        .readdir(ino, previous)
        .unwrap_or_else(|e| panic!("list inode {ino} after {previous}: {e:?}"));
    assert!(
        next.is_empty(),
        "the page after the last accepted offset must be empty, got {} entries",
        next.len()
    );
    entries
}

/// The large-directory fixture: one directory with 3 000 files, ten more with
/// 100 each (4 000 entries total), plus a small cache-miss torrent whose read
/// parks on the (empty) swarm.
fn fixture() -> (FsService, tempfile::TempDir) {
    let mut config = quiet_config();
    config.timeouts.read_timeout_secs = Some(2);
    config.timeouts.peer_discovery_wait_secs = Some(1);
    config.timeouts.no_seeder_read_timeout_secs = Some(1);
    let (mut service, cache) = service_with_config(&config);

    let mut paths: Vec<(String, usize)> = (0..BULK_DIRECTORY_FILES)
        .map(|file| (format!("bulk/{file:04}.bin"), 1))
        .collect();
    for directory in 0..SMALL_DIRECTORY_COUNT {
        for file in 0..SMALL_DIRECTORY_FILES {
            paths.push((format!("dir{directory:02}/file{file:03}.bin"), 1));
        }
    }
    assert_eq!(paths.len(), TOTAL_FILES);
    let files: Vec<(&str, usize)> = paths
        .iter()
        .map(|(path, size)| (path.as_str(), *size))
        .collect();
    let large = build_multi_file_torrent(
        "http://127.0.0.1:1/announce",
        "negative-02-large",
        &files,
    );
    ingest_torrent(&mut service, METADATA_INO, "large.torrent", &large);

    let (stalled, _) = crate::common::base::create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "negative-02-stalled.txt",
    );
    ingest_torrent(&mut service, METADATA_INO, "stalled.torrent", &stalled);

    (service, cache)
}

/// Entering, listing (including a kernel-style middle-offset continuation),
/// recursively walking and resolving by name in a torrent with a 3 000-entry
/// directory all return inside their wall-clock budgets, while a read parked
/// on an empty swarm does not block the following listing.
#[test]
fn tc_negative_02_large_directory_listing_is_second_scale() {
    let _slot = wave_slot();
    let (mut service, _cache) = fixture();

    let root = service
        .lookup(DATA_INO, "large.torrent")
        .expect("lookup large torrent root")
        .expect("large torrent must be projected");

    // libtorrent's multi-file paths start with the torrent's `name`; entering
    // that directory is the first step of the recursive walk.
    let name_dir = service
        .lookup(root.ino, "negative-02-large")
        .expect("lookup the torrent's name directory")
        .expect("the name directory must be projected");

    // Enter the torrent and list its top-level directories.
    let top = timed("list torrent root", PER_OPERATION_BUDGET, || {
        list_page(&mut service, name_dir.ino)
    });
    let directories: Vec<_> = top
        .iter()
        .filter(|entry| {
            entry.kind == FileKind::Directory && entry.name != "." && entry.name != ".."
        })
        .collect();
    assert_eq!(
        directories.len(),
        SMALL_DIRECTORY_COUNT + 1,
        "the torrent root must list the bulk directory and the small directories"
    );

    // Recursive walk: every directory and every file resolves by name, and
    // listing each directory (with its middle-offset continuation) stays
    // inside the bound.
    let walk_start = Instant::now();
    let mut total_files = 0usize;
    for directory in &directories {
        let attrs = timed(
            &format!("stats for {}", directory.name),
            PER_OPERATION_BUDGET,
            || service.getattr(directory.ino),
        )
        .unwrap_or_else(|e| panic!("attrs for {}: {e:?}", directory.name));
        assert_eq!(
            attrs.perm, 0o555,
            "{} must stay a 0555 directory",
            directory.name
        );

        let entries = timed(
            &format!("list {}", directory.name),
            PER_OPERATION_BUDGET,
            || list_page(&mut service, directory.ino),
        );
        let files: Vec<_> = entries
            .iter()
            .filter(|entry| entry.kind == FileKind::RegularFile)
            .collect();
        let expected_files = if directory.name == "bulk" {
            BULK_DIRECTORY_FILES
        } else {
            SMALL_DIRECTORY_FILES
        };
        assert_eq!(
            files.len(),
            expected_files,
            "{} must list all {expected_files} files",
            directory.name
        );
        total_files += files.len();

        // Resolve one file by name inside each directory — a by-name lookup,
        // not a scan of the directory.
        let probe = &files[expected_files / 2];
        let entry = timed(
            &format!("resolve {}", probe.name),
            PER_OPERATION_BUDGET,
            || service.lookup(directory.ino, &probe.name),
        )
        .expect("lookup inside directory")
        .unwrap_or_else(|| panic!("{} must resolve inside {}", probe.name, directory.name));
        assert_eq!(
            entry.ino, probe.ino,
            "resolving {} by name must return the listed inode",
            probe.name
        );
        assert_eq!(
            entry.attr.perm, 0o444,
            "torrent files must stay 0444 regular files"
        );
    }
    assert_eq!(
        total_files, TOTAL_FILES,
        "the recursive walk must see every file"
    );
    let walk_elapsed = walk_start.elapsed();
    assert!(
        walk_elapsed < TREE_BUDGET,
        "the recursive walk took {walk_elapsed:?}, exceeding the {TREE_BUDGET:?} budget"
    );

    // A read whose pieces are not cached parks on the swarm instead of
    // running on the dispatcher: `read` returns `Pending` at once.
    let stalled_root = service
        .lookup(DATA_INO, "stalled.torrent")
        .expect("lookup stalled torrent root")
        .expect("stalled torrent must be projected");
    let stalled_file = service
        .lookup(stalled_root.ino, "negative-02-stalled.txt")
        .expect("lookup stalled file")
        .expect("stalled file must be projected");
    let outcome = timed("park a cache-miss read", Duration::from_secs(1), || {
        service.read(stalled_file.ino, 0, 512)
    })
    .unwrap_or_else(|e| panic!("parking a read must not error: {e:?}"));
    assert!(
        matches!(outcome, torrentfs::fuse::fs_types::ReadOutcome::Pending { .. }),
        "an uncached read must be parked (Pending), not served or failed"
    );

    // The same cache-miss read over the engine, running concurrently with the
    // listing below, must not slow the listing down.
    let download = Arc::clone(
        service
            .download_service
            .as_ref()
            .expect("the fixture service must own a download engine"),
    );
    let info = Arc::new(
        TorrentInfo::from_bytes(
            crate::common::base::create_single_piece_torrent(
                "http://127.0.0.1:1/announce",
                "negative-02-stalled.txt",
            )
            .0,
        )
        .expect("parse stalled torrent"),
    );
    let reader = thread::spawn(move || download.read_file_range(info, 0, 0, 512));

    // Give the reader a moment to park in the piece/peer wait, then list the
    // largest directory while it is still outstanding.
    thread::sleep(Duration::from_millis(300));
    let bulk = directories
        .iter()
        .find(|entry| entry.name == "bulk")
        .expect("the bulk directory");
    let entries = timed(
        "list during a parked read",
        PER_OPERATION_BUDGET,
        || list_page(&mut service, bulk.ino),
    );
    assert_eq!(
        entries
            .iter()
            .filter(|entry| entry.kind == FileKind::RegularFile)
            .count(),
        BULK_DIRECTORY_FILES,
        "the listing during a parked read must be complete"
    );
    assert!(
        !reader.is_finished(),
        "the swarm read finished before the listing did, so the listing was \
         never exercised while a read was outstanding"
    );

    let read_result = reader.join().expect("reader thread");
    assert!(
        read_result.is_err(),
        "a read with no seeder must fail rather than serve data"
    );

    if let Some(download) = &service.download_service {
        download.shutdown();
    }
}

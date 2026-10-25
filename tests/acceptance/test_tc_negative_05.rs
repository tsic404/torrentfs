//! Design acceptance case TC-negative-05: expanding a directory at one-entry
//! pagination granularity costs must grow with the entry count, not with its
//! square (`docs/03-acceptance.md` §J AC-91).
//!
//! The walk mirrors the kernel: one entry consumed per `readdir` call, the
//! next call resuming from that entry's offset.  A listing whose per-call
//! post-processing filters the whole directory and clones the tail costs
//! O(directory) per call, so the two scales — 4x apart and in the ten
//! thousands — come out roughly 16x apart.  A per-page listing comes out
//! roughly 4x apart, under the design's 8x bound.
//!
//! The fixture seeds the database directly with two torrents, each with all
//! its files in one `bulk/` directory, so the walk is the only thing measured.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use torrentfs::fuse::inodes::DATA_INO;
use torrentfs::{Database, FileEntry, FsService, InsertTorrentResult};

use crate::common::{service_with_state, wave_slot};

pub const TC_ID: &str = "TC-negative-05";

/// Smaller scale; the larger one is four times it and reaches the ten
/// thousands, as AC-91 words it.
const SMALL_ENTRIES: usize = 2_500;
const SCALE_FACTOR: usize = 4;
const LARGE_ENTRIES: usize = SMALL_ENTRIES * SCALE_FACTOR;

/// The design's bound: linear expansion is about `SCALE_FACTOR`, a per-call
/// full-directory scan about `SCALE_FACTOR²`.
const MAX_GROWTH: f64 = 8.0;

/// Every file of one torrent sits in a single directory, so one listing
/// carries `entries` rows.
fn seed_torrent(db: &mut Database, filename: &str, entries: usize) {
    let source_id = match db
        .insert_torrent(
            "",
            filename,
            filename,
            entries as i64,
            filename,
            entries as i64,
        )
        .expect("insert torrent row")
    {
        InsertTorrentResult::Inserted(id) | InsertTorrentResult::Duplicate(id) => id,
    };
    let files: Vec<FileEntry> = (0..entries)
        .map(|index| FileEntry {
            path: format!("bulk/{index:05}.bin"),
            size: 1,
        })
        .collect();
    db.insert_files(source_id, &files).expect("insert file tree");
}

/// Resolve the `bulk/` directory of one seeded torrent through the projection,
/// the way the kernel reaches it: `data/<filename>` then the torrent's tree.
fn bulk_directory(service: &mut FsService, filename: &str) -> u64 {
    let root = service
        .lookup(DATA_INO, filename)
        .unwrap_or_else(|e| panic!("lookup data/{filename}: {e:?}"))
        .unwrap_or_else(|| panic!("data/{filename} must be projected"));
    service
        .lookup(root.ino, "bulk")
        .unwrap_or_else(|e| panic!("lookup {filename}/bulk: {e:?}"))
        .unwrap_or_else(|| panic!("{filename}/bulk must be projected"))
        .ino
}

/// Expand a directory one entry per call, resuming from the offset of the
/// entry just consumed, and time the whole expansion.
fn expand_one_entry_per_call(service: &mut FsService, ino: u64) -> (Vec<String>, Duration) {
    let mut names = Vec::new();
    let mut offset = 0;
    let start = Instant::now();
    loop {
        let page = service
            .readdir(ino, offset)
            .unwrap_or_else(|e| panic!("readdir({ino}, {offset}): {e:?}"));
        let Some(entry) = page.first() else { break };
        assert!(
            entry.offset > offset,
            "readdir({ino}, {offset}) must start past the requested offset, got {entry:?}"
        );
        offset = entry.offset;
        names.push(entry.name.clone());
    }
    (names, start.elapsed())
}

/// The walk must hand out every entry of the directory exactly once.
fn assert_exact(names: &[String], files: usize, label: &str) {
    let mut seen: BTreeSet<&str> = BTreeSet::new();
    for name in names {
        assert!(
            seen.insert(name.as_str()),
            "{label}: entry '{name}' was listed twice"
        );
    }
    assert_eq!(
        names.len(),
        files + 2,
        "{label}: the walk must cover '.', '..' and all {files} files"
    );
    for anchor in [".", ".."] {
        assert!(
            seen.contains(anchor),
            "{label}: the walk must include '{anchor}'"
        );
    }
    assert_eq!(
        names.iter().filter(|name| name.ends_with(".bin")).count(),
        files,
        "{label}: every file must appear in the walk"
    );
}

#[test]
fn tc_negative_05_one_entry_pagination_does_not_scan_the_directory_per_call() {
    let _slot = wave_slot();
    let (mut service, _cache) = service_with_state();
    {
        let db = service
            .db
            .as_ref()
            .expect("service owns its database")
            .clone();
        let mut guard = db.lock().expect("lock database");
        seed_torrent(&mut guard, "small.torrent", SMALL_ENTRIES);
        seed_torrent(&mut guard, "large.torrent", LARGE_ENTRIES);
    }

    let small_dir = bulk_directory(&mut service, "small.torrent");
    let large_dir = bulk_directory(&mut service, "large.torrent");

    let (small_names, small_elapsed) = expand_one_entry_per_call(&mut service, small_dir);
    let (large_names, large_elapsed) = expand_one_entry_per_call(&mut service, large_dir);

    assert_exact(&small_names, SMALL_ENTRIES, "small directory");
    assert_exact(&large_names, LARGE_ENTRIES, "large directory");

    // A walk that finishes inside a microsecond would make the ratio
    // meaningless; treat it as the 1µs floor instead of dividing by zero.
    let baseline = small_elapsed.max(Duration::from_micros(1)).as_secs_f64();
    let growth = large_elapsed.as_secs_f64() / baseline;
    assert!(
        growth < MAX_GROWTH,
        "expanding {LARGE_ENTRIES} entries took {large_elapsed:?} against \
         {small_elapsed:?} for {SMALL_ENTRIES} ({growth:.2}x): a \
         {SCALE_FACTOR}x directory must not cost {MAX_GROWTH}x or more at \
         one-entry pagination granularity"
    );
}

//! Design acceptance case TC-stats-05: `.stats` never leaks into a torrent's
//! internal directories or into the `metadata/` tree
//! (`docs/03-acceptance.md` §E AC-44).
//!
//! Both trees are walked to the leaves over complete `readdir` listings — not
//! sampled one level deep — because the virtual file is injected during
//! listing, and an injection applied to every data directory would still pass
//! a root-only check.

use crate::common::{local_test_config, wave_slot};

use std::collections::BTreeSet;

use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::{Database, FileEntry, FsService, InsertTorrentResult};

pub const TC_ID: &str = "TC-stats-05";

/// A torrent with two levels of internal directories, so "internal directory"
/// is exercised below the torrent root and below an intermediate directory.
const FILES: &[(&str, i64)] = &[
    ("disc/track_1.flac", 2048),
    ("disc/inner/track_2.flac", 4096),
    ("cover.jpg", 512),
];

fn database() -> (Database, i64) {
    let mut db = Database::open_in_memory().expect("in-memory database");
    db.ensure_metadata_directories("cat")
        .expect("create metadata/cat");
    db.ensure_metadata_directories("cat/sub")
        .expect("create metadata/cat/sub");

    let source_id = match db
        .insert_torrent("cat", "duo", "duo.torrent", 6656, "hash-stats-05", 3)
        .expect("insert torrent row")
    {
        InsertTorrentResult::Inserted(id) => id,
        InsertTorrentResult::Duplicate(id) => id,
    };
    let files: Vec<FileEntry> = FILES
        .iter()
        .map(|(path, size)| FileEntry {
            path: (*path).to_string(),
            size: *size,
        })
        .collect();
    db.insert_files(source_id, &files).expect("insert file tree");
    (db, source_id)
}

struct Listing {
    names: BTreeSet<String>,
}

/// Walk one directory recursively.  Every `.stats` found under a directory
/// that must not expose one is recorded as a violation.  Returns the listing
/// of the walked directory so callers can prove the walk was not vacuous.
fn walk(
    svc: &mut FsService,
    ino: u64,
    path: String,
    allow_stats_here: bool,
    violations: &mut Vec<String>,
) -> Listing {
    let entries = svc
        .readdir(ino, 0)
        .unwrap_or_else(|e| panic!("readdir {path}: {e:?}"));
    let names: BTreeSet<String> = entries
        .iter()
        .map(|entry| entry.name.clone())
        .filter(|name| name != "." && name != "..")
        .collect();

    for entry in entries {
        if entry.name == "." || entry.name == ".." {
            continue;
        }
        if entry.name == ".stats" {
            if !allow_stats_here {
                violations.push(format!("{path}/.stats"));
            }
            continue;
        }
        if entry.kind == FileKind::Directory {
            walk(
                svc,
                entry.ino,
                format!("{path}/{}", entry.name),
                false,
                violations,
            );
        }
    }

    Listing { names }
}

fn child_ino(svc: &mut FsService, parent: u64, name: &str) -> (u64, FileKind) {
    svc.readdir(parent, 0)
        .expect("readdir parent")
        .into_iter()
        .find(|entry| entry.name == name)
        .map(|entry| (entry.ino, entry.kind))
        .unwrap_or_else(|| panic!("{name} must be listed under inode {parent}"))
}

#[test]
fn tc_stats_05_stats_absent_from_torrent_subdirs_and_metadata_tree() {
    let _slot = wave_slot();
    let (db, _source_id) = database();
    let cache = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let mut svc = FsService::new_with_db_and_cache(db, cache.path().to_path_buf(), &config);

    // ── The torrent tree under data/ ─────────────────────────────────────────
    let (cat, kind) = child_ino(&mut svc, DATA_INO, "cat");
    assert_eq!(kind, FileKind::Directory, "data/cat must be a directory");
    let (torrent_root, kind) = child_ino(&mut svc, cat, "duo.torrent");
    assert_eq!(
        kind,
        FileKind::Directory,
        "the projected torrent root must be a directory"
    );

    let mut violations = Vec::new();
    let root_listing = walk(
        &mut svc,
        torrent_root,
        "data/cat/duo.torrent".to_string(),
        true,
        &mut violations,
    );
    // Positive controls: the torrent root does carry `.stats` (AC-41) and the
    // walk reached the internal directories and root files.
    assert!(
        root_listing.names.contains(".stats"),
        "the torrent root must expose .stats; otherwise this walk proves nothing: {:?}",
        root_listing.names
    );
    for expected in ["disc", "cover.jpg"] {
        assert!(
            root_listing.names.contains(expected),
            "the torrent root must list {expected}: {:?}",
            root_listing.names
        );
    }

    let (disc, kind) = child_ino(&mut svc, torrent_root, "disc");
    assert_eq!(kind, FileKind::Directory);
    let disc_listing = svc.readdir(disc, 0).expect("readdir disc");
    let disc_names: BTreeSet<&str> = disc_listing.iter().map(|e| e.name.as_str()).collect();
    assert!(
        disc_names.contains("inner") && disc_names.contains("track_1.flac"),
        "the internal directory must list its child directory and file: {disc_names:?}"
    );

    assert!(
        violations.is_empty(),
        "the torrent's internal directories must not expose .stats: {violations:?}"
    );

    // ── The metadata tree ────────────────────────────────────────────────────
    let mut metadata_violations = Vec::new();
    let metadata_root = walk(
        &mut svc,
        METADATA_INO,
        "metadata".to_string(),
        false,
        &mut metadata_violations,
    );
    for expected in ["cat"] {
        assert!(
            metadata_root.names.contains(expected),
            "metadata/ root must list {expected}: {:?}",
            metadata_root.names
        );
    }

    let (cat_meta, kind) = child_ino(&mut svc, METADATA_INO, "cat");
    assert_eq!(kind, FileKind::Directory);
    let cat_meta_names: BTreeSet<String> = svc
        .readdir(cat_meta, 0)
        .expect("readdir metadata/cat")
        .into_iter()
        .map(|entry| entry.name)
        .collect();
    assert!(
        cat_meta_names.contains("duo.torrent") && cat_meta_names.contains("sub"),
        "metadata/cat must list the stored torrent and its subdirectory: {cat_meta_names:?}"
    );

    assert!(
        metadata_violations.is_empty(),
        "the metadata/ tree must not expose .stats: {metadata_violations:?}"
    );
}

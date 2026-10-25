//! Design acceptance case TC-stats-03: the directory `.stats` files exist and
//! aggregate every torrent under their `source_path` prefix
//! (`docs/03-acceptance.md` §E AC-42, contract C-6.1.2 / C-6.1.5).
//!
//! The case drives the FUSE service directly: `readdir` injects the virtual
//! `.stats` entry and `read_stats` renders the daemon's bytes; one nested
//! `source_path` makes an exact-match aggregation (two torrents, not three)
//! fail here.

use crate::common::{local_test_config, wave_slot};

use torrentfs::fuse::fs_types::{FileKind, StatsKind};
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::{Database, FsService, InsertTorrentResult};

pub const TC_ID: &str = "TC-stats-03";

/// `(source_path, name, filename, info_hash)` rows the aggregation must see.
const TORRENTS: &[(&str, &str, &str, &str)] = &[
    ("movies", "movie-alpha", "alpha.torrent", "hash-stats-03-alpha"),
    ("movies", "movie-beta", "beta.torrent", "hash-stats-03-beta"),
    (
        "movies/sub",
        "movie-nested",
        "nested.torrent",
        "hash-stats-03-nested",
    ),
    ("music", "song-one", "song-one.torrent", "hash-stats-03-song"),
];

/// The directories the write path would have created are what make a
/// `source_path` addressable in `data/`.
const SOURCE_DIRS: &[&str] = &["movies", "movies/sub", "music", "empty-dir"];

fn database() -> Database {
    let mut db = Database::open_in_memory().expect("in-memory database");
    for dir in SOURCE_DIRS {
        db.ensure_metadata_directories(dir)
            .expect("create metadata directory rows");
    }
    for (source_path, name, filename, info_hash) in TORRENTS {
        match db
            .insert_torrent(source_path, name, filename, 4096, info_hash, 1)
            .expect("insert torrent row")
        {
            InsertTorrentResult::Inserted(_) | InsertTorrentResult::Duplicate(_) => {}
        }
    }
    db
}

fn service(db: Database) -> (FsService, tempfile::TempDir) {
    let cache = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let svc = FsService::new_with_db_and_cache(db, cache.path().to_path_buf(), &config);
    (svc, cache)
}

/// Resolve a child inode the way the kernel does after a listing: the entry
/// must be present in the parent's `readdir` output.
fn child_ino(svc: &mut FsService, parent: u64, name: &str) -> u64 {
    let entries = svc.readdir(parent, 0).expect("readdir parent");
    entries
        .iter()
        .find(|entry| entry.name == name)
        .unwrap_or_else(|| panic!("{name} must be listed under inode {parent}"))
        .ino
}

/// Render a directory's `.stats` from the entry its own listing carries, so a
/// directory that fails to expose the virtual file fails here rather than
/// being papered over with a computed inode number.
fn directory_stats(svc: &mut FsService, dir_ino: u64) -> String {
    let entries = svc.readdir(dir_ino, 0).expect("readdir directory");
    let stats = entries
        .iter()
        .find(|entry| entry.name == ".stats")
        .unwrap_or_else(|| panic!("directory inode {dir_ino} must list .stats: {entries:?}"));
    assert_eq!(
        stats.kind,
        FileKind::RegularFile,
        "directory .stats must be a regular file"
    );
    let bytes = svc.read_stats(StatsKind::StatsInode { ino: stats.ino });
    String::from_utf8(bytes).expect("directory .stats is UTF-8")
}

/// The `Torrents: N` aggregate on the Summary line.
fn summary_count(stats: &str) -> usize {
    let value = stats
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("Torrents: "))
        .unwrap_or_else(|| panic!("directory .stats has no Summary count:\n{stats}"));
    value
        .split_whitespace()
        .next()
        .expect("Torrents count")
        .parse()
        .expect("Torrents count is numeric")
}

fn assert_sections(stats: &str, path: &str) {
    let title = stats.lines().next().unwrap_or_default();
    assert_eq!(
        title,
        format!("===== directory: {path} ====="),
        "directory .stats must open with its directory title"
    );
    for section in [
        "-- Summary --",
        "-- Torrents --",
        "-- Rates --",
        "-- Peers --",
        "-- Cache --",
    ] {
        assert!(
            stats.contains(section),
            "directory .stats for {path:?} is missing section {section:?}:\n{stats}"
        );
    }
}

#[test]
fn tc_stats_03_directory_stats_aggregate_their_prefix() {
    let _slot = wave_slot();
    let (mut svc, _cache) = service(database());

    // `/data/.stats` — the data root aggregates every source path.
    let data_root = directory_stats(&mut svc, DATA_INO);
    assert_sections(&data_root, "");
    assert_eq!(
        summary_count(&data_root),
        TORRENTS.len(),
        "/data/.stats must aggregate every torrent under the mount"
    );

    // `/data/movies/.stats` — exact matches plus the `movies/sub` descendant.
    let movies_ino = child_ino(&mut svc, DATA_INO, "movies");
    let movies = directory_stats(&mut svc, movies_ino);
    assert_sections(&movies, "movies");
    assert_eq!(
        summary_count(&movies),
        3,
        "the movies aggregate is the whole `movies` prefix, nested source paths included:\n{movies}"
    );
    for name in ["movie-alpha", "movie-beta", "movie-nested"] {
        assert!(
            movies.contains(name),
            "movies .stats must list {name}:\n{movies}"
        );
    }
    assert!(
        !movies.contains("song-one"),
        "movies .stats must not list a torrent from another prefix:\n{movies}"
    );

    // `/data/movies/sub/.stats` — a nested source path is its own aggregate
    // boundary: it sits at depth 2 and has both a torrent of its own and a
    // parent prefix, so the title must name the nested path and the listing
    // must stop at its own torrent.
    let sub_ino = child_ino(&mut svc, movies_ino, "sub");
    let sub = directory_stats(&mut svc, sub_ino);
    assert_sections(&sub, "movies/sub");
    assert_eq!(
        summary_count(&sub),
        1,
        "the nested source path aggregates exactly its own torrent:\n{sub}"
    );
    assert!(
        sub.contains("movie-nested"),
        "movies/sub .stats must list movie-nested:\n{sub}"
    );
    assert!(
        !sub.contains("movie-alpha") && !sub.contains("movie-beta"),
        "movies/sub .stats must not list its parent's torrents:\n{sub}"
    );

    // `/data/music/.stats` — a sibling prefix sees exactly its own torrent, so
    // the prefix comparison cannot be passing by matching everything.
    let music_ino = child_ino(&mut svc, DATA_INO, "music");
    let music = directory_stats(&mut svc, music_ino);
    assert_sections(&music, "music");
    assert_eq!(summary_count(&music), 1, "music prefix count:\n{music}");

    // A directory with no matching torrents renders the one-line notice only
    // (C-6.1.5): no aggregate sections, not a zeroed report.
    let empty_ino = child_ino(&mut svc, DATA_INO, "empty-dir");
    let empty = directory_stats(&mut svc, empty_ino);
    assert_eq!(
        empty.lines().next().unwrap_or_default(),
        "===== directory: empty-dir =====",
        "empty directory .stats keeps its title"
    );
    assert!(
        empty.contains("  No torrents found under this path."),
        "empty directory .stats must carry the no-match notice:\n{empty}"
    );
    for section in ["-- Summary --", "-- Torrents --", "-- Rates --"] {
        assert!(
            !empty.contains(section),
            "empty directory .stats must not render the {section:?} section:\n{empty}"
        );
    }
    let body_lines: Vec<&str> = empty
        .lines()
        .filter(|line| !line.trim().is_empty())
        .filter(|line| !line.starts_with("====="))
        .collect();
    assert_eq!(
        body_lines,
        vec!["  No torrents found under this path."],
        "the no-match render is one explanation line and nothing else:\n{empty}"
    );

    // The metadata tree root is not a data source path and carries no `.stats`.
    let metadata = svc.readdir(METADATA_INO, 0).expect("readdir metadata");
    assert!(
        metadata.iter().all(|entry| entry.name != ".stats"),
        "metadata/ root must not expose .stats: {metadata:?}"
    );
}

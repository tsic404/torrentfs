//! Design acceptance case TC-stats-04: writing a new torrent raises the root
//! `.stats` total and the directory `.stats` seed count, one write at a time
//! (`docs/03-acceptance.md` §E AC-43, contract C-6.5.2 polling cadence).
//!
//! The torrent goes through the shipped write path — `create` / `write` /
//! `release` on the metadata namespace — so the increment is a real ingest.
//! Counts are polled, never asserted on one instantaneous read: freshness is
//! bounded by the reader's own polling interval.

use crate::common::{local_test_config, wave_slot};

use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::fs_types::{FileKind, StatsKind};
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::{Database, FsService};

pub const TC_ID: &str = "TC-stats-04";

const TIMEOUT: Duration = Duration::from_secs(30);

/// A structurally valid single-file `.torrent` with no announce target — the
/// write path accepts it, and the DB row it produces carries the info hash the
/// projection needs.
fn single_file_torrent(name: &str) -> Vec<u8> {
    let mut content = format!("torrentfs acceptance stats-04 content: {name}").into_bytes();
    content.resize(16 * 1024, b'X');
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(&content);
    let piece_hash = hasher.digest().bytes();

    let mut torrent = Vec::new();
    torrent.push(b'd');
    torrent.extend_from_slice(b"4:infod");
    torrent.extend_from_slice(b"6:lengthi16384e");
    torrent.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    torrent.extend_from_slice(b"12:piece lengthi16384e");
    torrent.extend_from_slice(b"6:pieces20:");
    torrent.extend_from_slice(&piece_hash);
    torrent.extend_from_slice(b"ee");
    torrent
}

/// Find a child inode through the parent's listings, the way the kernel
/// resolves a path after `readdir`.
fn child_ino(svc: &mut FsService, parent: u64, name: &str) -> u64 {
    svc.readdir(parent, 0)
        .expect("readdir parent")
        .into_iter()
        .find(|entry| entry.name == name)
        .unwrap_or_else(|| panic!("{name} must be listed under inode {parent}"))
        .ino
}

/// Render one directory's `.stats` from the `.stats` entry its listing carries.
fn directory_stats(svc: &mut FsService, dir_ino: u64) -> String {
    let entries = svc.readdir(dir_ino, 0).expect("readdir directory");
    let stats = entries
        .iter()
        .find(|entry| entry.name == ".stats")
        .unwrap_or_else(|| panic!("directory inode {dir_ino} must list .stats"));
    assert_eq!(
        stats.kind,
        FileKind::RegularFile,
        "directory .stats must be a regular file"
    );
    String::from_utf8(svc.read_stats(StatsKind::StatsInode { ino: stats.ino }))
        .expect("directory .stats is UTF-8")
}

/// `Torrents: N` from a directory `.stats`, or 0 for the no-match notice.
fn directory_count(stats: &str) -> usize {
    if stats.contains("No torrents found under this path.") {
        return 0;
    }
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

/// `Total: N` from the mount-root `.stats` Torrents overview.
fn root_total(stats: &str) -> usize {
    let value = stats
        .lines()
        .find_map(|line| line.trim_start().strip_prefix("Total: "))
        .unwrap_or_else(|| panic!("root .stats has no Torrents total:\n{stats}"));
    value
        .split_whitespace()
        .next()
        .expect("Total count")
        .parse()
        .expect("Total count is numeric")
}

/// Write one `.torrent` into the metadata directory and close it, exactly as
/// the kernel's create / write / release sequence does.
fn write_torrent(svc: &mut FsService, parent: u64, filename: &str, name: &str) {
    let created = svc.create(parent, filename).expect("create .torrent");
    let bytes = single_file_torrent(name);
    svc.write(created.attr.ino, 0, &bytes)
        .expect("write .torrent bytes");
    svc.release(created.fh).expect("release .torrent");
}

/// `(root total, data-root count, directory count)` rendered from the stats
/// files themselves.
fn counts(svc: &mut FsService, directory_ino: u64) -> (usize, usize, usize) {
    let root = String::from_utf8(svc.read_stats(StatsKind::Global)).expect("root .stats is UTF-8");
    let data_root = directory_stats(svc, DATA_INO);
    let directory = directory_stats(svc, directory_ino);
    (
        root_total(&root),
        directory_count(&data_root),
        directory_count(&directory),
    )
}

/// Poll the three stats files until `expected` is observed.  The design makes
/// `.stats` a rendered file over a snapshot whose staleness is bounded by the
/// reader's polling interval; a single immediate assertion after the write
/// would test timing, not the contract.
fn wait_for_counts(svc: &mut FsService, directory_ino: u64, expected: (usize, usize, usize)) {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        let observed = counts(svc, directory_ino);
        if observed == expected {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected (root total, /data/.stats, directory .stats) = {expected:?}, \
             still {observed:?} after {TIMEOUT:?}"
        );
        thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn tc_stats_04_new_torrent_raises_root_and_directory_counts() {
    let _slot = wave_slot();
    let cache = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let db = Database::open_in_memory().expect("in-memory database");
    let mut svc = FsService::new_with_db_and_cache(db, cache.path().to_path_buf(), &config);

    // The directory the write targets, and the baseline all three stats files
    // report before any torrent lands in it.
    let music = svc
        .mkdir(METADATA_INO, "music")
        .expect("mkdir metadata/music")
        .ino;
    // The metadata directory is mirrored into `data/`; its `.stats` is the
    // directory view this case watches.
    let music_ino = child_ino(&mut svc, DATA_INO, "music");
    wait_for_counts(&mut svc, music_ino, (0, 0, 0));

    write_torrent(&mut svc, music, "first.torrent", "stats-04-first");
    wait_for_counts(&mut svc, music_ino, (1, 1, 1));

    write_torrent(&mut svc, music, "second.torrent", "stats-04-second");
    wait_for_counts(&mut svc, music_ino, (2, 2, 2));

    // The second write's delta is exactly one on every view of the count — a
    // render that double-counts a source row or drops the directory scope
    // cannot land on both endpoints.
    assert_eq!(
        counts(&mut svc, music_ino),
        (2, 2, 2),
        "each ingest raises every count by exactly one"
    );
}

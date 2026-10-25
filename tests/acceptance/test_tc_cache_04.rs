//! Design acceptance case TC-cache-04: deleting a torrent removes its
//! `info_hash` cache directory and leaves no orphaned cache metadata
//! (`docs/03-acceptance.md` §D AC-33; `docs/02-contracts.md` §C-5.2.3,
//! §C-5.2.4, §C-5.4.4).
//!
//! The deletion goes through `TorrentService::remove_torrent` — the exact call
//! the FUSE unlink of a `metadata/<file>.torrent` issues — so the purge path is
//! the product's, not a test-only reimplementation.  Cache metadata is a lazy
//! on-disk file: a live daemon shows a lagging view, so the assertion polls the
//! ≥5 s window the contract allows and falls back to the shutdown flush when
//! the periodic flush has not caught up.

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot,
    TestHarness,
};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::services::torrent::TorrentService;
use torrentfs::TorrentInfo;

pub const TC_ID: &str = "TC-cache-04";

const TORRENT_NAME: &str = "tc-cache-04-remove.bin";
const TORRENT_FILENAME: &str = "tc-cache-04-remove.bin.torrent";
const SOURCE_PATH: &str = "";
const READ_BUDGET: Duration = Duration::from_secs(120);
/// Full pieces of the 4 × 256 KiB fixture.
const PIECE_LEN: usize = 256 * 1024;
/// C-5.2.4: at least five seconds of polling before the lagging view is read as
/// a failure; the shutdown flush is the authoritative fallback.
const CONVERGENCE_WINDOW: Duration = Duration::from_secs(10);

#[test]
fn tc_cache_04_removing_a_torrent_purges_cache_and_metadata() {
    let _slot = wave_slot();
    let _session_guard = acquire_session_lock();

    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_named(announce_url, TORRENT_NAME)
    });
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.local_discovery.lsd_enabled = Some(false);

    let service = Arc::new(DownloadService::new(cache_dir.path(), &config).expect("engine"));
    let db = Arc::new(Mutex::new(Database::open_in_memory().expect("in-memory db")));
    let torrents = TorrentService::new(Arc::clone(&db), Some(Arc::clone(&service)));

    // Ingest exactly as the FUSE release of a written `.torrent` does.
    torrents
        .add_torrent(&harness.torrent_data, SOURCE_PATH, TORRENT_FILENAME)
        .expect("ingest torrent");
    let info = Arc::new(TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("parse"));
    let info_hash = hex::encode(info.info_hash().expect("info hash"));

    // Read the whole file so every piece is on disk before the deletion.
    let data = read_once(
        &service,
        &info,
        harness.file_content.len() as u32,
        READ_BUDGET,
    );
    assert_eq!(data, harness.file_content, "warm read returned the wrong bytes");

    let cache = service.get_cache_manager().expect("cache manager");
    let piece_count = harness.file_content.len().div_ceil(PIECE_LEN);
    wait_until("every piece to be registered in the cache", Duration::from_secs(30), || {
        (0..piece_count).all(|piece| {
            cache
                .lock()
                .expect("cache lock")
                .has_piece(&piece_key(&info_hash, piece))
        })
    });

    let pieces_dir = cache_dir.path().join("pieces").join(&info_hash);
    let metadata_path = cache_dir.path().join("cache_metadata.txt");
    assert!(pieces_dir.is_dir(), "precondition: the pieces directory must exist");

    // Make the "before" state durable so the convergence check below cannot
    // pass vacuously.
    cache.lock().expect("cache lock").flush().expect("flush cache metadata");
    let before = std::fs::read_to_string(&metadata_path).expect("read cache metadata");
    assert!(
        before.contains(&info_hash),
        "precondition: cache metadata must reference the torrent before removal:\n{before}"
    );

    // Delete the seed record — the service call behind a `metadata/` unlink.
    let removed = torrents
        .remove_torrent(TORRENT_FILENAME, SOURCE_PATH)
        .expect("remove torrent")
        .expect("the torrent row must exist");
    assert_eq!(removed.1, info_hash, "removal must report the deleted info_hash");

    // C-5.4.4: the info_hash cache directory is gone, with no residual data
    // left anywhere under pieces/.  This walks the whole tree — a top-level
    // name compare would be satisfied by the wait above alone — and any I/O
    // error fails the case instead of reading as "no orphans".
    wait_until("the info_hash cache directory to disappear", Duration::from_secs(5), || {
        !pieces_dir.exists()
    });
    let orphans = orphaned_paths(&cache_dir.path().join("pieces"), &info_hash)
        .expect("walk the pieces tree for residual data");
    assert!(
        orphans.is_empty(),
        "no residual data may remain for the removed info_hash: {orphans:?}"
    );

    // The in-memory view converges immediately; the on-disk metadata is a
    // lagging view that must converge within the polling window, or be read
    // after the daemon stopped (C-5.2.4).
    let stats = cache
        .lock()
        .expect("cache lock")
        .get_cache_stats_by_infohash(&info_hash);
    assert_eq!(
        stats.piece_count, 0,
        "in-memory cache must carry no pieces for the deleted torrent"
    );
    assert_eq!(
        stats.total_size, 0,
        "in-memory cache must carry no bytes for the deleted torrent"
    );

    let deadline = Instant::now() + CONVERGENCE_WINDOW;
    let mut converged = false;
    while Instant::now() < deadline {
        if !read_metadata(&metadata_path).contains(&info_hash) {
            converged = true;
            break;
        }
        thread::sleep(Duration::from_millis(100));
    }
    if !converged {
        // Second accepted witness: the shutdown flush is the authoritative
        // on-disk state.
        service.shutdown();
        let after_stop = read_metadata(&metadata_path);
        assert!(
            !after_stop.contains(&info_hash),
            "after shutdown the flushed cache metadata must not reference the deleted \
             info_hash:\n{after_stop}"
        );
    }

    service.shutdown();
}

/// The cache metadata file's content.  A vanished file is an empty cache
/// metadata — the converged state — while any other I/O error must surface
/// instead of passing the convergence witness.
fn read_metadata(path: &Path) -> String {
    match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(error) => panic!("read cache metadata {}: {error}", path.display()),
    }
}

/// A vanished metadata file is a converged (empty) view, not an I/O error.
#[test]
fn tc_cache_04_missing_metadata_reads_as_empty() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    assert_eq!(read_metadata(&dir.path().join("absent.txt")), "");
}

/// Any I/O error other than `NotFound` must surface, not read as empty: a
/// swallowed error would let the convergence witness above pass on a metadata
/// file the test cannot actually see.
#[test]
#[should_panic(expected = "read cache metadata")]
fn tc_cache_04_unreadable_metadata_is_not_swallowed() {
    // A directory cannot be read as text (EISDIR).  Unlike a `chmod 000`
    // fixture this still raises the error when the suite runs as root.
    let dir = tempfile::TempDir::new().expect("temp dir");
    let path = dir.path().join("metadata-as-directory");
    std::fs::create_dir(&path).expect("create a directory in place of the metadata file");
    let error = std::fs::read_to_string(&path).expect_err("a directory is not readable text");
    assert_ne!(
        error.kind(),
        std::io::ErrorKind::NotFound,
        "the fixture must raise an error other than NotFound: {error}"
    );
    read_metadata(&path);
}

/// Residual paths under `pieces/` that belong to `info_hash`: the piece
/// directory itself or a stray `{info_hash}:piece:{n}` file at any depth.
/// I/O errors propagate — an unreadable tree must fail, not read as empty.
fn orphaned_paths(pieces_root: &Path, info_hash: &str) -> std::io::Result<Vec<PathBuf>> {
    let piece_prefix = format!("{info_hash}:piece:");
    let mut orphans = Vec::new();
    let mut pending = vec![pieces_root.to_path_buf()];
    while let Some(dir) = pending.pop() {
        for entry in std::fs::read_dir(&dir)? {
            let entry = entry?;
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name == info_hash || name.starts_with(&piece_prefix) {
                orphans.push(path.clone());
            }
            if entry.file_type()?.is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(orphans)
}

/// The residual walk must find piece files left deeper in the tree and must
/// report an unreadable tree as an error, not as an empty result.
#[test]
fn tc_cache_04_residual_walk_catches_orphaned_pieces() {
    let dir = tempfile::TempDir::new().expect("temp dir");
    let pieces = dir.path().join("pieces");
    std::fs::create_dir_all(pieces.join("nested")).expect("create pieces tree");
    let stray = pieces.join("nested").join("abc:piece:3");
    std::fs::write(&stray, b"x").expect("write stray piece");
    assert_eq!(
        orphaned_paths(&pieces, "abc").expect("walk"),
        vec![stray.clone()],
        "a piece file left below the top level must be reported"
    );
    assert!(
        orphaned_paths(&pieces, "def").expect("walk").is_empty(),
        "another info_hash's tree is not residue"
    );
    let missing = dir.path().join("no-such-dir");
    assert!(
        orphaned_paths(&missing, "abc").is_err(),
        "an unreadable tree must not read as 'no orphans'"
    );
}

fn piece_key(info_hash: &str, piece: usize) -> String {
    format!("{info_hash}:piece:{piece}")
}

fn read_once(
    service: &DownloadService,
    info: &Arc<TorrentInfo>,
    size: u32,
    budget: Duration,
) -> Vec<u8> {
    let start = Instant::now();
    loop {
        match service.read_file_range(Arc::clone(info), 0, 0, size) {
            Ok(data) if data.len() == size as usize => return data,
            Ok(data) => {
                if start.elapsed() >= budget {
                    panic!("timed out reading {size} bytes (got {} bytes)", data.len());
                }
            }
            Err(error) => {
                if start.elapsed() >= budget {
                    panic!("timed out reading {size} bytes: {error:?}");
                }
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn wait_until(what: &str, budget: Duration, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + budget;
    loop {
        if condition() {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        thread::sleep(Duration::from_millis(100));
    }
}

//! Design acceptance case TC-ingest-05: two byte-identical `.torrent` files at
//! different `source_path` values share one content record and one progress
//! owner; two files with the same `info_hash` but different bytes stay
//! independent (`docs/03-acceptance.md` §A AC-05, contracts C-3.2.2 /
//! C-3.2.3 / C-3.2.5).

use crate::common::{create_single_piece_torrent, wave_slot, IngestHarness, TestHarness};
use std::thread;
use std::time::{Duration, Instant};
use torrentfs::fuse::fs_types::ReadOutcome;

pub const TC_ID: &str = "TC-ingest-05";

/// The `Status` / `Progress` / `DL` lines and the `-- Pieces` marker line —
/// the observable fields the design pins for this case.
fn pinned_stats_lines(stats: &str) -> Vec<String> {
    stats
        .lines()
        .filter(|line| {
            line.starts_with("  Status: ")
                || line.starts_with("  DL: ")
                || line.starts_with("  Pieces: ")
        })
        .map(str::to_string)
        .collect()
}

fn progress_percent(stats: &str) -> Option<f64> {
    let line = stats.lines().find(|line| line.starts_with("  Status: "))?;
    let start = line.find("Progress: ")? + "Progress: ".len();
    let rest = &line[start..];
    let end = rest.find('%')?;
    rest[..end].parse().ok()
}

/// Read the whole file through one projection's file inode.
///
/// `FsService::read` is the entry point the FUSE adapter calls; the first
/// attempt returns `Pending` while the pieces are missing, exactly as a real
/// read does.  The adapter resolves that pending read on a worker with the
/// blocking engine read, so this repeats the same step and then re-enters
/// `read`, which now serves the bytes through the projection's cache path.
fn read_through_projection(ingest: &mut IngestHarness, file_ino: u64, len: u32) -> Vec<u8> {
    let started = Instant::now();
    loop {
        match ingest.service().read(file_ino, 0, len) {
            Ok(ReadOutcome::Ready(data)) => return data,
            Ok(ReadOutcome::Pending {
                info,
                file_index,
                offset,
                size,
                ..
            }) => {
                let download = ingest
                    .service()
                    .download_service
                    .clone()
                    .expect("the ingest service must hold a download engine");
                match download.read_file_range_blocking(info, file_index, offset, size) {
                    Ok(_) => {}
                    Err(e) => assert!(
                        started.elapsed() < Duration::from_secs(60),
                        "timed out reading through the projection: {:?}",
                        e
                    ),
                }
            }
            Err(e) => panic!("read through the projection failed: {:?}", e),
        }
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "timed out waiting for the projection read to complete"
        );
    }
}

/// Identical bytes at two source paths: both projections list the content,
/// one content record carries the state, and progress produced through one
/// copy is visible — letter for letter — through the other.
#[test]
fn tc_ingest_05_identical_bytes_share_one_content_record() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(|announce_url| {
        create_single_piece_torrent(announce_url, "ingest-05-shared.bin")
    });
    // File-backed so the content layer (`torrents`) can be queried directly.
    let mut ingest = IngestHarness::with_file_db();

    let a = ingest.mkdir(&["a"]);
    let b = ingest.mkdir(&["b"]);
    ingest.copy_torrent(a, "dup.torrent", &harness.torrent_data);
    ingest.copy_torrent(b, "dup.torrent", &harness.torrent_data);

    let info_hash = hex::encode(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("parse the fixture torrent")
            .info_hash()
            .expect("info hash"),
    );

    let content_ids = ingest.content_ids(&info_hash);
    assert_eq!(
        content_ids.len(),
        1,
        "identical bytes at two source paths must share one content record, got {:?}",
        content_ids
    );
    let sources: Vec<_> = ingest
        .torrents()
        .into_iter()
        .filter(|torrent| torrent.info_hash == info_hash)
        .collect();
    assert_eq!(sources.len(), 2, "both source paths must keep a source row");
    assert_eq!(
        sources[0].torrent_id, sources[1].torrent_id,
        "both source rows must point at the same content record"
    );
    assert_eq!(
        ingest.content_rows(&info_hash),
        1,
        "exactly one content row may back both projections"
    );

    // Read the whole file through the first projection's file inode: the
    // progress it produces must belong to the shared content, so the second
    // projection reports the same state.
    let file_ino = ingest
        .data_lookup(&["a", "dup.torrent", "ingest-05-shared.bin"])
        .expect("the first projection must resolve its file inode");
    let read = read_through_projection(&mut ingest, file_ino, harness.file_content.len() as u32);
    assert_eq!(
        read, harness.file_content,
        "the read must return the seeded bytes"
    );

    let root_a = ingest
        .data_lookup(&["a", "dup.torrent"])
        .expect("data/a/dup.torrent must resolve");
    let root_b = ingest
        .data_lookup(&["b", "dup.torrent"])
        .expect("data/b/dup.torrent must resolve");

    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let stats_a = ingest.stats_text(root_a);
        let stats_b = ingest.stats_text(root_b);
        let lines_a = pinned_stats_lines(&stats_a);
        let lines_b = pinned_stats_lines(&stats_b);
        let marker_present = lines_a.iter().any(|line| line.starts_with("  Pieces: "));
        let shared_progress = progress_percent(&stats_a)
            .map(|progress| progress > 0.0)
            .unwrap_or(false);
        if lines_a == lines_b && marker_present && shared_progress {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "both leaves must show the same non-zero Status/Progress/DL and \
             -- Pieces marker within 10s\n--- a ---\n{}\n--- b ---\n{}",
            stats_a,
            stats_b
        );
        thread::sleep(Duration::from_millis(100));
    }
}

/// Same `info_hash`, different bytes (different tracker): two independent
/// content records, each projected at its own source path.
#[test]
fn tc_ingest_05_same_info_hash_different_bytes_stay_independent() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::with_file_db();

    let (bytes_a, _) =
        create_single_piece_torrent("http://127.0.0.1:9/announce-a", "ingest-05-split.bin");
    let (bytes_b, _) =
        create_single_piece_torrent("http://127.0.0.1:9/announce-b", "ingest-05-split.bin");
    assert_ne!(
        bytes_a, bytes_b,
        "different announce URLs must produce different torrent bytes"
    );
    let hash_a = hex::encode(
        torrentfs::TorrentInfo::from_bytes(bytes_a.clone())
            .expect("parse torrent a")
            .info_hash()
            .expect("info hash a"),
    );
    let hash_b = hex::encode(
        torrentfs::TorrentInfo::from_bytes(bytes_b.clone())
            .expect("parse torrent b")
            .info_hash()
            .expect("info hash b"),
    );
    assert_eq!(
        hash_a, hash_b,
        "the fixture must vary only the tracker, keeping one info_hash"
    );

    let a = ingest.mkdir(&["a"]);
    let b = ingest.mkdir(&["b"]);
    ingest.copy_torrent(a, "same.torrent", &bytes_a);
    ingest.copy_torrent(b, "same.torrent", &bytes_b);

    let content_ids = ingest.content_ids(&hash_a);
    assert_eq!(
        content_ids.len(),
        2,
        "same info_hash with different bytes must keep two content records, got {:?}",
        content_ids
    );
    let sources: Vec<_> = ingest
        .torrents()
        .into_iter()
        .filter(|torrent| torrent.info_hash == hash_a)
        .collect();
    assert_eq!(sources.len(), 2, "one source row per projection");
    assert_ne!(
        sources[0].torrent_id, sources[1].torrent_id,
        "each source must own its own content record"
    );
    assert_eq!(
        ingest.content_rows(&hash_a),
        2,
        "byte-distinct torrents sharing an info_hash keep two content rows"
    );

    assert!(
        ingest
            .data_lookup(&["a", "same.torrent", "ingest-05-split.bin"])
            .is_some(),
        "the first projection must list its own entry"
    );
    assert!(
        ingest
            .data_lookup(&["b", "same.torrent", "ingest-05-split.bin"])
            .is_some(),
        "the second projection must list its own entry"
    );
}

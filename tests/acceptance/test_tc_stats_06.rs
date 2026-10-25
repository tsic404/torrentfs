//! Design acceptance case TC-stats-06: the `.stats` `-- Pieces` block exists,
//! its marker run is exactly the four-state enumeration, and the piece count
//! matches the torrent metadata (`docs/03-acceptance.md` §E AC-45).
//!
//! The run is parsed with a strict enumeration — `[]`, `[N]`, `[x]`, `[X n]` —
//! so a fifth state, a separator, or trailing whitespace fails.  The states are
//! observed in their real conditions: `[]` before any read, `[N]` while a read
//! waits on a delayed peer, and a cached marker after the read lands.

use crate::common::{local_test_config, wave_slot, MiniTracker};
use crate::seed_peer::{PeerDelays, SeedPeer};
use crate::stats_support::{multipiece_torrent, parse_markers, pieces_run, render, Marker};

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::format_bytes;
use torrentfs::{Database, DownloadService, InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-stats-06";

const PIECES: usize = 32;
/// 16 KiB pieces put exactly one BEP-3 block in each piece, so a peer that
/// delays every REQUEST delays the read piece by one delay rather than
/// sixteen (see `stats_support::multipiece_torrent`).
const PIECE_LEN: usize = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(30);

fn field<'a>(stats: &'a str, key: &str) -> Option<&'a str> {
    stats
        .lines()
        .find_map(|line| line.strip_prefix(&format!("  {key}: ")))
}

/// Every render must carry the block and agree with the metadata: one marker
/// per piece, `PieceCount == num_pieces`, `PieceSize == piece length`.
fn assert_piece_block(stats: &str, info: &TorrentInfo) -> Vec<Marker> {
    assert!(
        stats.contains("-- Pieces ("),
        "the leaf .stats must carry a Pieces block:\n{stats}"
    );
    let run = pieces_run(stats).unwrap_or_else(|| panic!("Pieces block has no marker line:\n{stats}"));
    let markers = parse_markers(run);
    let num_pieces = info.num_pieces() as usize;
    assert_eq!(
        markers.len(),
        num_pieces,
        "one marker per metadata piece:\n{stats}"
    );
    assert_eq!(
        field(stats, "PieceCount").and_then(|v| v.parse::<usize>().ok()),
        Some(num_pieces),
        "PieceCount must match the metadata piece count:\n{stats}"
    );
    assert_eq!(
        field(stats, "PieceSize"),
        Some(format_bytes(info.piece_length() as u64).as_str()),
        "PieceSize must match the metadata piece length:\n{stats}"
    );
    assert!(
        stats.contains(&format!(
            "-- Pieces ({num_pieces} pieces, {} each) --",
            format_bytes(info.piece_length() as u64)
        )),
        "the Pieces header must carry the metadata piece count and size:\n{stats}"
    );
    markers
}

/// Poll the rendered stats until `predicate` accepts the marker run, then
/// return it.  `.stats` renders from a snapshot published at the design's
/// cadence, so a single instant read is not evidence.
fn wait_for_markers(
    service: &Arc<DownloadService>,
    db: &Arc<Mutex<Database>>,
    torrent_id: i64,
    info_hash: &str,
    info: &TorrentInfo,
    what: &str,
    mut predicate: impl FnMut(&[Marker]) -> bool,
) -> Vec<Marker> {
    let deadline = Instant::now() + TIMEOUT;
    let mut last = render(service, db, torrent_id, info_hash);
    loop {
        if pieces_run(&last).is_some() {
            let markers = assert_piece_block(&last, info);
            if predicate(&markers) {
                return markers;
            }
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; last .stats:\n{last}"
        );
        thread::sleep(Duration::from_millis(100));
        last = render(service, db, torrent_id, info_hash);
    }
}

#[test]
fn tc_stats_06_piece_block_uses_only_the_four_marker_states() {
    let _slot = wave_slot();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        multipiece_torrent(&announce_url, "stats-06-markers.bin", PIECES, PIECE_LEN);
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));
    let info_hash = hex::encode(info.info_hash().expect("info hash"));
    let peer = SeedPeer::start_delayed(
        info.info_hash().expect("info hash"),
        content.clone(),
        info.piece_length() as usize,
        &announce_url,
        PeerDelays {
            handshake: Duration::ZERO,
            piece: Duration::from_secs(3),
        },
    );

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let service =
        Arc::new(DownloadService::new(cache_dir.path(), &config).expect("download service"));

    let mut db = Database::open_in_memory().expect("in-memory database");
    let torrent_id = match db
        .insert_torrent(
            "stats-06",
            "stats-06-markers.bin",
            "stats-06.bin.torrent",
            content.len() as i64,
            &info_hash,
            info.num_pieces() as i64,
        )
        .expect("insert torrent row")
    {
        InsertTorrentResult::Inserted(id) | InsertTorrentResult::Duplicate(id) => id,
    };
    let db = Arc::new(Mutex::new(db));

    service
        .ensure_handle_lightweight(info.clone())
        .expect("ensure handle");

    // Idle state: the block exists and every marker is the not-wanted form.
    let idle = wait_for_markers(
        &service,
        &db,
        torrent_id,
        &info_hash,
        &info,
        "the idle Pieces block",
        |_| true,
    );
    assert!(
        idle.iter().all(|marker| *marker == Marker::NotWanted),
        "before any read every piece is not wanted, got {idle:?}"
    );

    // A read parked on a slow peer: the read-range piece is wanted (`[N]`),
    // which the strict enumeration accepts as a wanted marker.
    let svc = service.clone();
    let info_for_read = info.clone();
    let expected: Vec<u8> = content[..1024].to_vec();
    let read = thread::spawn(move || svc.read_file_range(info_for_read, 0, 0, 1024));
    let during = wait_for_markers(
        &service,
        &db,
        torrent_id,
        &info_hash,
        &info,
        "a wanted [N] marker",
        |markers| matches!(markers.first(), Some(Marker::Wanted(7))),
    );
    assert_eq!(
        during[0],
        Marker::Wanted(7),
        "the piece being read carries the current priority"
    );

    let data = read.join().expect("read thread").expect("read must succeed");
    assert_eq!(data, expected, "the read must return the seeded bytes");

    // After the read, the served piece is cached and renders as `[x]` or
    // `[X n]` — the cached forms.
    let cached = wait_for_markers(
        &service,
        &db,
        torrent_id,
        &info_hash,
        &info,
        "a cached marker",
        |markers| matches!(markers.first(), Some(Marker::Cached) | Some(Marker::CachedHit(_))),
    );
    assert!(matches!(
        cached[0],
        Marker::Cached | Marker::CachedHit(_)
    ));

    // A second read hits the cached piece; its hit count makes the `[X n]`
    // form observable.
    let second = service
        .read_file_range(info.clone(), 0, 0, 1024)
        .expect("cache-hit read");
    assert_eq!(second, expected, "the second read must return the cached bytes");
    let hit = wait_for_markers(
        &service,
        &db,
        torrent_id,
        &info_hash,
        &info,
        "an [X n] marker",
        |markers| matches!(markers.first(), Some(Marker::CachedHit(_))),
    );
    assert!(
        matches!(hit[0], Marker::CachedHit(n) if n >= 1),
        "the cached piece records its hit count"
    );

    assert!(peer.pieces_served() > 0, "the peer must have served a piece");
    service.shutdown();
}

//! Design acceptance case TC-stats-07: after a read, the workspace piece
//! markers show the priority gradient and its window boundary
//! (`docs/03-acceptance.md` §E AC-46, contract C-6.3.3 / C-6.3.6).
//!
//! A read at piece 10 of a 32-piece torrent with a 256 KiB cache makes the
//! forward window exactly 15 pieces: the read piece is highest, the next four
//! descend, the rest of the window is the lowest positive priority, everything
//! outside is `[]`, and nothing before the offset is wanted.

use crate::common::{local_test_config, wave_slot, MiniTracker};
use crate::seed_peer::{PeerDelays, SeedPeer};
use crate::stats_support::{multipiece_torrent, parse_markers, pieces_run, render, Marker};

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::{Database, DownloadService, InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-stats-07";

const PIECES: usize = 32;
/// 16 KiB pieces put exactly one BEP-3 block in each piece, so a peer that
/// delays every REQUEST delays the read piece by one delay rather than
/// sixteen (see `stats_support::multipiece_torrent`).
const PIECE_LEN: usize = 16 * 1024;
const TIMEOUT: Duration = Duration::from_secs(40);
/// Read offset: inside piece 10 of the fixture.
const READ_PIECE: usize = 10;
/// The cache is 256 KiB and the fixture's piece length is 16 KiB, so
/// `(256 KiB - 16 KiB) / 16 KiB` = 15 pieces of forward window.
const CACHE_BYTES: i64 = 256 * 1024;
const WINDOW_PIECES: usize = 15;

#[test]
fn tc_stats_07_piece_priorities_show_gradient_and_window() {
    let _slot = wave_slot();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (torrent_data, content) =
        multipiece_torrent(&announce_url, "stats-07-gradient.bin", PIECES, PIECE_LEN);
    let info = Arc::new(TorrentInfo::from_bytes(torrent_data).expect("parse torrent"));
    let info_hash_raw = info.info_hash().expect("info hash");
    let info_hash = hex::encode(info_hash_raw);
    let piece_length = info.piece_length() as usize;
    let _peer = SeedPeer::start_delayed(
        info_hash_raw,
        content.clone(),
        piece_length,
        &announce_url,
        PeerDelays {
            handshake: Duration::ZERO,
            piece: Duration::from_secs(3),
        },
    );

    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.cache.cache_size = Some(CACHE_BYTES);
    let service =
        Arc::new(DownloadService::new(cache_dir.path(), &config).expect("download service"));

    let mut db = Database::open_in_memory().expect("in-memory database");
    let torrent_id = match db
        .insert_torrent(
            "stats-07",
            "stats-07-gradient.bin",
            "stats-07.bin.torrent",
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
    // Wait for the peer to be usable before starting the read.
    let deadline = Instant::now() + TIMEOUT;
    while !service
        .try_query_torrent_status(&info_hash)
        .is_some_and(|status| status.num_seeds >= 1)
    {
        assert!(
            Instant::now() < deadline,
            "the delayed peer never counted as a seed"
        );
        thread::sleep(Duration::from_millis(200));
    }

    let offset = (READ_PIECE * piece_length + 1000) as u64;
    let expected: Vec<u8> = content[offset as usize..offset as usize + 1024].to_vec();
    let read_service = service.clone();
    let read_info = info.clone();
    let read = thread::spawn(move || read_service.read_file_range(read_info, 0, offset, 1024));

    // Observe the gradient while the read is parked on the peer.
    let deadline = Instant::now() + TIMEOUT;
    let mut during = Vec::new();
    let mut last = String::new();
    while Instant::now() < deadline {
        last = render(&service, &db, torrent_id, &info_hash);
        if let Some(run) = pieces_run(&last) {
            let markers = parse_markers(run);
            if markers.len() == PIECES
                && markers[READ_PIECE] == Marker::Wanted(7)
                && markers[READ_PIECE + 1..READ_PIECE + 5]
                    == [Marker::Wanted(6), Marker::Wanted(5), Marker::Wanted(4), Marker::Wanted(3)]
                && markers[READ_PIECE + 5..=READ_PIECE + WINDOW_PIECES]
                    .iter()
                    .all(|marker| *marker == Marker::Wanted(1))
                && markers[..READ_PIECE]
                    .iter()
                    .all(|marker| *marker == Marker::NotWanted)
                && markers[READ_PIECE + WINDOW_PIECES + 1..]
                    .iter()
                    .all(|marker| *marker == Marker::NotWanted)
            {
                during = markers;
                break;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    assert!(
        !during.is_empty(),
        "the read's priority gradient never appeared in .stats;\nlast render:\n{last}"
    );
    // State the boundaries once more, so a failure names the contract point
    // rather than only the aggregate predicate above.
    assert!(
        during[..READ_PIECE].iter().all(|m| *m == Marker::NotWanted),
        "no backward prefetch: pieces before the read offset stay not wanted"
    );
    assert!(
        during[READ_PIECE + WINDOW_PIECES + 1..]
            .iter()
            .all(|m| *m == Marker::NotWanted),
        "outside the forward window every piece stays not wanted"
    );

    let data = read.join().expect("read thread").expect("read must succeed");
    assert_eq!(data, expected, "the read must return the seeded bytes");

    // After release the retained prefetch window is still visible.  The cache
    // may already hold some prefetched pieces, so assert the bounds that the
    // contract fixes: the served piece is cached, nothing behind it or beyond
    // the window is wanted, and the wanted set never grows to the whole file.
    let deadline = Instant::now() + TIMEOUT;
    loop {
        last = render(&service, &db, torrent_id, &info_hash);
        if let Some(run) = pieces_run(&last) {
            let markers = parse_markers(run);
            let wanted = markers
                .iter()
                .filter(|marker| matches!(marker, Marker::Wanted(_)))
                .count();
            let behind_and_beyond_clear = markers[..READ_PIECE]
                .iter()
                .chain(markers[READ_PIECE + WINDOW_PIECES + 1..].iter())
                .all(|marker| *marker == Marker::NotWanted);
            if markers.len() == PIECES
                && matches!(markers[READ_PIECE], Marker::Cached | Marker::CachedHit(_))
                && behind_and_beyond_clear
                && wanted <= WINDOW_PIECES
            {
                break;
            }
        }
        assert!(
            Instant::now() < deadline,
            "after the read, the retained window must stay inside its bounds;\nlast render:\n{last}"
        );
        thread::sleep(Duration::from_millis(100));
    }

    service.shutdown();
}

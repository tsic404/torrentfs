//! Design acceptance case TC-negative-04: the published `.stats` format stays
//! parseable by its existing consumers — the field assertions in
//! `tests/torrent_stats_waiting_test.rs` / `tests/torrent_stats_trackers_test.rs`
//! and the section/title assertions in `tests/acceptance/tc_stats_0{1,2}.sh`
//! all still hold against freshly rendered output
//! (`docs/03-acceptance.md` §J AC-93; `docs/02-contracts.md` §C-6.1..§C-6.3).
//!
//! The renderers are driven against real database + engine state, then the
//! output is checked with the same parsers the external consumers use: exact
//! section names, the `Cache:`/`info_hash:`/`tier 0` field forms, the wait
//! fields, and the strict four-state piece marker grammar.  A format change
//! that any existing consumer would trip on fails here.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::{
    generate_directory_stats, generate_global_stats, generate_torrent_stats,
};
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

use crate::common::{base, quiet_config, wave_slot};

pub const TC_ID: &str = "TC-negative-04";

const TRACKER_URL: &str = "http://127.0.0.1:6977/announce";
const SOURCE_PATH: &str = "collection";

/// The consumer-parsed `Waited:` seconds, copied from
/// `tests/torrent_stats_waiting_test.rs`: absent when no read is parked.
fn waited_secs(text: &str) -> Option<u64> {
    text.lines()
        .find_map(|line| line.strip_prefix("  Waited: "))
        .and_then(|value| value.strip_suffix('s'))
        .and_then(|secs| secs.parse().ok())
}

fn line_value<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.lines().find_map(|line| line.strip_prefix(prefix))
}

fn require(text: &str, needle: &str) {
    assert!(
        text.contains(needle),
        "the published .stats format must keep {needle:?}, got:\n{text}"
    );
}

/// Strict C-6.3.1 marker parse: only `[]`, `[x]`, `[X n]` and `[N]` exist.
/// Returns the marker count so it can be compared with `PieceCount`.
fn marker_count(markers: &str) -> usize {
    let mut rest = markers;
    let mut count = 0;
    while !rest.is_empty() {
        let close = rest
            .find(']')
            .unwrap_or_else(|| panic!("unterminated piece marker in {markers:?}"));
        assert_eq!(
            &rest[..1],
            "[",
            "piece markers carry no separators; offending suffix {rest:?}"
        );
        let inner = &rest[1..close];
        let valid = inner.is_empty()
            || inner == "x"
            || inner
                .strip_prefix("X ")
                .map(|hits| hits.parse::<u64>().is_ok_and(|n| n >= 1))
                .unwrap_or(false)
            || inner.parse::<u64>().is_ok_and(|priority| priority >= 1);
        assert!(
            valid,
            "piece marker [{inner}] is not one of the four published states \
             ([] [x] [X n] [N]) in {markers:?}"
        );
        count += 1;
        rest = &rest[close + 1..];
    }
    count
}

/// One inserted torrent with its renderable identity.
struct FixtureTorrent {
    id: i64,
    info_hash: String,
    name: String,
}

fn insert(
    db: &mut Database,
    name: &str,
    filename: &str,
) -> (FixtureTorrent, TorrentInfo) {
    let (bytes, _content) = base::create_single_piece_torrent(TRACKER_URL, name);
    let info = TorrentInfo::from_bytes(bytes).expect("parse fixture torrent");
    let info_hash = hex::encode(info.info_hash().expect("fixture info hash"));
    let InsertTorrentResult::Inserted(id) = db
        .insert_torrent(SOURCE_PATH, name, filename, 16384, &info_hash, 1)
        .expect("insert fixture torrent")
    else {
        panic!("{filename}: expected a fresh insert");
    };
    (
        FixtureTorrent {
            id,
            info_hash,
            name: name.to_string(),
        },
        info,
    )
}

/// Wait for the engine snapshot that carries the handle's tracker list.
fn wait_for_trackers(service: &DownloadService, info_hash: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while service.try_trackers(info_hash).is_none() {
        assert!(
            Instant::now() < deadline,
            "the engine never published a tracker snapshot for {info_hash}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// Wait for the piece snapshot the published `-- Pieces` block renders from.
fn wait_for_pieces(service: &DownloadService, info_hash: &str) {
    let deadline = Instant::now() + Duration::from_secs(20);
    while service.try_get_pieces_status(info_hash).is_none() {
        assert!(
            Instant::now() < deadline,
            "the engine never published a piece snapshot for {info_hash}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[test]
fn tc_negative_04_published_stats_format_satisfies_existing_consumers() {
    let _slot = wave_slot();

    let mut database = Database::open_in_memory().expect("in-memory database");
    let (alpha, alpha_info) = insert(&mut database, "negative-04-alpha", "alpha.torrent");
    let (beta, beta_info) = insert(&mut database, "negative-04-beta", "beta.torrent");
    let db = Some(Arc::new(Mutex::new(database)));

    let cache = tempfile::TempDir::new().expect("cache dir");
    let service = Arc::new(
        DownloadService::new(cache.path(), &quiet_config()).expect("create download service"),
    );
    service
        .ensure_handle_lightweight(Arc::new(alpha_info))
        .expect("register alpha handle");
    service
        .ensure_handle_lightweight(Arc::new(beta_info))
        .expect("register beta handle");
    wait_for_trackers(&service, &alpha.info_hash);
    wait_for_pieces(&service, &alpha.info_hash);

    let cache_for_closure = Arc::clone(&service);
    let render_torrent = |torrent: &FixtureTorrent| {
        String::from_utf8_lossy(&generate_torrent_stats(
            torrent.id,
            &torrent.info_hash,
            &db,
            &Some(Arc::clone(&service)),
            || cache_for_closure.get_cache_manager(),
        ))
        .into_owned()
    };

    let alpha_stats = render_torrent(&alpha);
    let beta_stats = render_torrent(&beta);

    // tc_stats_02.sh: title, sections, cache line, info_hash
    require(
        &alpha_stats,
        &format!("===== torrent: {} =====\n", alpha.name),
    );
    for section in ["\n-- Status --\n", "\n-- Rates --\n", "\n-- Peers --\n"] {
        require(&alpha_stats, section);
    }
    require(&alpha_stats, "\nCache: ");
    require(&alpha_stats, &format!("info_hash: {}\n", alpha.info_hash));
    require(&alpha_stats, &format!("source_path: \"{SOURCE_PATH}\"\n"));

    // torrent_stats_waiting_test.rs: field forms and wait semantics.
    require(&alpha_stats, &format!("  Name: {}\n", alpha.name));
    require(&alpha_stats, "  Status: ");
    require(&alpha_stats, "  Progress: ");
    require(&alpha_stats, "  Size: ");
    require(&alpha_stats, "  DL: ");
    require(&alpha_stats, "  UL: ");
    require(&alpha_stats, "  Ratio: ");
    require(&alpha_stats, "  Rate: ↓ ");
    require(&alpha_stats, "  Peers: ");
    require(&alpha_stats, "  Seeds: ");
    require(&alpha_stats, "  Waiting: no\n");
    require(&alpha_stats, "  Active readers: 0\n");
    assert!(
        waited_secs(&alpha_stats).is_none(),
        "no read is parked, so the consumer's `Waited:` parser must find no line"
    );

    // torrent_stats_trackers_test.rs: tier form and isolation line.
    require(&alpha_stats, "\n-- Trackers --\n");
    require(
        &alpha_stats,
        &format!("\n  tier 0  {TRACKER_URL}\n"),
    );
    require(
        &alpha_stats,
        "private: no  tracker_isolation: merge-eligible\n",
    );

    // C-6.3: strict four-state markers and piece counts
    let markers = line_value(&alpha_stats, "  Pieces: ").unwrap_or_else(|| {
        panic!("the published format must carry the marker line:\n{alpha_stats}")
    });
    let count = marker_count(markers);
    let piece_count: usize = line_value(&alpha_stats, "  PieceCount: ")
        .unwrap_or_else(|| panic!("the published format must carry PieceCount:\n{alpha_stats}"))
        .trim()
        .parse()
        .expect("PieceCount is a number");
    assert_eq!(
        count, piece_count,
        "one marker per piece is part of the published format"
    );
    require(&alpha_stats, "  PieceSize: ");
    require(&alpha_stats, "\n-- Pieces (");
    assert!(
        alpha_stats.contains(&format!("-- Pieces ({piece_count} pieces, ")),
        "the piece header must name the piece count the marker line carries:\n{alpha_stats}"
    );

    // Two different torrents still render different content.
    assert_ne!(
        alpha_stats, beta_stats,
        "two distinct torrents must not render identical .stats content"
    );

    // tc_stats_01.sh: root sections, and no single-torrent title.
    let global = String::from_utf8_lossy(&generate_global_stats(
        Duration::from_secs(1),
        &db,
        Some(service.snapshot_stats()),
        || cache_for_closure.get_cache_manager(),
        "0.0.0.0:6881",
        None,
    ))
    .into_owned();
    for section in [
        "\n-- Overview --\n",
        "\n-- Global Rates --\n",
        "\n-- Connections --\n",
        "\n-- Torrents --\n",
        "\n-- Cache --\n",
        "\n-- Observability --\n",
    ] {
        require(&global, section);
    }
    require(&global, "  Mount:        (dynamic)\n");
    require(&global, "  Total: 2  Unique: 2  ");
    assert!(
        !global.contains("===== torrent:"),
        "the root .stats must not carry a single-torrent title:\n{global}"
    );

    // Directory .stats: title, sections, aggregate count.
    let directory = String::from_utf8_lossy(&generate_directory_stats(
        SOURCE_PATH,
        &db,
        &Some(Arc::clone(&service)),
        || cache_for_closure.get_cache_manager(),
    ))
    .into_owned();
    require(
        &directory,
        &format!("===== directory: {SOURCE_PATH} =====\n"),
    );
    for section in [
        "\n-- Summary --\n",
        "\n-- Rates --\n",
        "\n-- Peers --\n",
        "\n-- Cache --\n",
        "\n-- Torrents --\n",
    ] {
        require(&directory, section);
    }
    require(&directory, "  Torrents: 2  ");
    for name in [alpha.name.as_str(), beta.name.as_str()] {
        require(&directory, name);
    }
    let empty_directory = String::from_utf8_lossy(&generate_directory_stats(
        "no-such-source-path",
        &db,
        &Some(Arc::clone(&service)),
        || cache_for_closure.get_cache_manager(),
    ))
    .into_owned();
    require(&empty_directory, "  No torrents found under this path.\n");

    service.shutdown();
}

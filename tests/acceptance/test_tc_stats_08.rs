//! Design acceptance case TC-stats-08: the leaf `.stats` Trackers section
//! lists entries by tier, explains the zero-tracker state, and carries the
//! global DHT node count when a session exists
//! (`docs/03-acceptance.md` §E AC-47, contract C-6.2.10).
//!
//! Torrent bytes go through the real metadata parser and the real download
//! session, so tier numbers and the empty list are libtorrent's answer, not a
//! fixture the renderer was handed directly.

use crate::common::{local_test_config, wave_slot};
use crate::stats_support::render;

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::{Database, DownloadService, InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-stats-08";

const TIMEOUT: Duration = Duration::from_secs(20);

const TIER_ZERO_A: &str = "http://127.0.0.1:6901/announce";
const TIER_ZERO_B: &str = "http://127.0.0.1:6902/announce";
const TIER_ONE: &str = "http://127.0.0.1:6903/announce";

/// Bencode a single-file torrent, optionally carrying a BEP-12 `announce-list`
/// (each slice is one tier).
fn torrent_bytes(name: &str, tiers: Option<&[&[&str]]>) -> (Vec<u8>, Vec<u8>) {
    const PIECE_LEN: usize = 16 * 1024;
    let mut content = format!("torrentfs acceptance stats-08: {name}").into_bytes();
    content.resize(PIECE_LEN, b'Y');
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(&content);
    let piece_hash = hasher.digest().bytes();

    let mut t = Vec::new();
    t.push(b'd');
    if let Some(tiers) = tiers {
        t.extend_from_slice(b"13:announce-listl");
        for tier in tiers {
            t.push(b'l');
            for url in *tier {
                t.extend_from_slice(url.len().to_string().as_bytes());
                t.push(b':');
                t.extend_from_slice(url.as_bytes());
            }
            t.push(b'e');
        }
        t.push(b'e');
    }
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"6:lengthi");
    t.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces20:");
    t.extend_from_slice(&piece_hash);
    t.extend_from_slice(b"ee");
    (t, content)
}

fn insert_torrent_row(db: &mut Database, name: &str, info_hash: &str) -> i64 {
    match db
        .insert_torrent(
            "stats-08",
            name,
            &format!("{name}.torrent"),
            16 * 1024,
            info_hash,
            1,
        )
        .expect("insert torrent row")
    {
        InsertTorrentResult::Inserted(id) | InsertTorrentResult::Duplicate(id) => id,
    }
}

/// Poll until the engine snapshot carries the tracker list for the torrent —
/// `None` means "not read yet", which renders the neutral unavailable line.
fn wait_for_trackers(
    service: &Arc<DownloadService>,
    info_hash: &str,
) -> Vec<torrentfs::TrackerEntry> {
    let deadline = Instant::now() + TIMEOUT;
    loop {
        if let Some(trackers) = service.try_trackers(info_hash) {
            return trackers;
        }
        assert!(
            Instant::now() < deadline,
            "the engine never published a tracker list for {info_hash}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// The zero-tracker explanation line rendered between the section marker and
/// the `private:` line.
const NO_TRACKERS_NOTICE: &str = "  No trackers — relying on DHT/LSD";

/// Non-blank lines of the Trackers section: everything between the
/// `-- Trackers --` marker and the `private:` line that terminates it.
fn tracker_section<'a>(stats: &'a str) -> Vec<&'a str> {
    let mut lines = stats.lines().skip_while(|line| *line != "-- Trackers --");
    assert!(
        lines.next().is_some(),
        "the leaf .stats must carry a Trackers section:\n{stats}"
    );
    let mut section = Vec::new();
    for line in lines {
        if line.starts_with("private:") {
            return section;
        }
        if !line.trim().is_empty() {
            section.push(line);
        }
    }
    panic!("the Trackers section must end at the private: line:\n{stats}")
}

/// Parse a `  DHT Nodes: <n> (global)` line.
fn parse_dht_nodes(line: &str) -> i32 {
    let value = line
        .strip_prefix("  DHT Nodes: ")
        .and_then(|rest| rest.strip_suffix(" (global)"))
        .unwrap_or_else(|| panic!("DHT line must be `  DHT Nodes: <n> (global)`, got {line:?}"));
    value
        .parse()
        .unwrap_or_else(|_| panic!("DHT node count must be numeric, got {line:?}"))
}

/// `(tier, url)` lines of the Trackers section, in render order.
///
/// Every non-blank line in the block is classified: the zero-tracker notice
/// and the global DHT count are accepted as themselves, and any other line
/// must be a well-formed `  tier <n>  <url>` entry.  A malformed or extra
/// line therefore fails the case instead of being filtered out, and `[]` in
/// the zero-tracker branch means "no line at all", not "no tier-shaped line".
fn tracker_lines(stats: &str) -> Vec<(i32, String)> {
    let mut tiers = Vec::new();
    let mut saw_notice = false;
    for line in tracker_section(stats) {
        if line == NO_TRACKERS_NOTICE {
            saw_notice = true;
            continue;
        }
        if line.starts_with("  DHT Nodes: ") {
            parse_dht_nodes(line);
            continue;
        }
        let rest = line.strip_prefix("  tier ").unwrap_or_else(|| {
            panic!(
                "unexpected Trackers line — only `  tier <n>  <url>`, \
                 `  DHT Nodes: <n> (global)` and the zero-tracker notice are \
                 valid: {line:?}"
            )
        });
        let (tier, url) = rest
            .split_once("  ")
            .unwrap_or_else(|| panic!("tracker line must be `  tier <n>  <url>`, got {line:?}"));
        assert!(!url.is_empty(), "tracker line must carry a URL, got {line:?}");
        let tier: i32 = tier
            .parse()
            .unwrap_or_else(|_| panic!("tier must be numeric, got {line:?}"));
        assert!(tier >= 0, "tier must be non-negative, got {line:?}");
        tiers.push((tier, url.to_string()));
    }
    assert!(
        !saw_notice || tiers.is_empty(),
        "the zero-tracker notice must not accompany tier lines:\n{stats}"
    );
    tiers
}

/// The global DHT node count carried by the Trackers section, if present.
fn dht_nodes_line(stats: &str) -> Option<i32> {
    let mut found = None;
    for line in tracker_section(stats) {
        if line.starts_with("  DHT Nodes: ") {
            assert!(
                found.is_none(),
                "the Trackers section carries exactly one global DHT line:\n{stats}"
            );
            found = Some(parse_dht_nodes(line));
        }
    }
    found
}

#[test]
fn tc_stats_08_trackers_by_tier_and_zero_tracker_notice() {
    let _slot = wave_slot();
    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let service =
        Arc::new(DownloadService::new(cache_dir.path(), &config).expect("download service"));
    let mut db = Database::open_in_memory().expect("in-memory database");

    // ── Multi-tier list ─────────────────────────────────────────────────────
    let (tiered_bytes, _content) = torrent_bytes(
        "stats-08-tiered.bin",
        Some(&[&[TIER_ZERO_A, TIER_ZERO_B], &[TIER_ONE]]),
    );
    let tiered = Arc::new(TorrentInfo::from_bytes(tiered_bytes).expect("parse tiered torrent"));
    let tiered_hash = hex::encode(tiered.info_hash().expect("info hash"));
    let tiered_id = insert_torrent_row(&mut db, "stats-08-tiered.bin", &tiered_hash);
    service
        .ensure_handle_lightweight(tiered.clone())
        .expect("ensure tiered handle");
    let declared = wait_for_trackers(&service, &tiered_hash);
    assert_eq!(
        declared.len(),
        3,
        "the handle must report every announce target: {declared:?}"
    );

    let db = Arc::new(Mutex::new(db));
    let stats = render(&service, &db, tiered_id, &tiered_hash);
    assert!(
        stats.contains("\n-- Trackers --\n"),
        "the leaf .stats must carry a Trackers section:\n{stats}"
    );
    let lines = tracker_lines(&stats);
    assert_eq!(
        lines,
        vec![
            (0, TIER_ZERO_A.to_string()),
            (0, TIER_ZERO_B.to_string()),
            (1, TIER_ONE.to_string()),
        ],
        "trackers must list by ascending tier, announce order inside a tier:\n{stats}"
    );
    assert!(
        dht_nodes_line(&stats).is_some(),
        "an attached session must carry the global DHT node count:\n{stats}"
    );

    // ── Zero trackers ───────────────────────────────────────────────────────
    let (bare_bytes, _content) = torrent_bytes("stats-08-bare.bin", None);
    let bare = Arc::new(TorrentInfo::from_bytes(bare_bytes).expect("parse bare torrent"));
    let bare_hash = hex::encode(bare.info_hash().expect("info hash"));
    let bare_id = insert_torrent_row(
        &mut db.lock().expect("db lock"),
        "stats-08-bare.bin",
        &bare_hash,
    );
    service
        .ensure_handle_lightweight(bare.clone())
        .expect("ensure bare handle");
    let declared = wait_for_trackers(&service, &bare_hash);
    assert!(
        declared.is_empty(),
        "a torrent with no announce target must report an empty list, got {declared:?}"
    );

    let stats = render(&service, &db, bare_id, &bare_hash);
    assert!(
        stats.contains("  No trackers — relying on DHT/LSD\n"),
        "zero trackers must explain the DHT/LSD dependency:\n{stats}"
    );
    assert!(
        tracker_lines(&stats).is_empty(),
        "zero trackers must render no tier lines:\n{stats}"
    );
    assert!(
        dht_nodes_line(&stats).is_some(),
        "the zero-tracker render still carries the global DHT node count:\n{stats}"
    );

    service.shutdown();
}

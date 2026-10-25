//! Design acceptance case TC-cache-03: with a cache limit smaller than the
//! data, pieces are evicted; re-reading an evicted piece downloads it again and
//! the leaf `.stats` piece grid shows it cached (`docs/03-acceptance.md` §D
//! AC-32; `docs/02-contracts.md` §C-5.1.2, §C-5.1.5, §C-6.2.5, §C-6.3.1).
//!
//! Both asserted strings are the real renderers: the piece grid comes from
//! `generate_torrent_stats` (the leaf `.stats` body) and the eviction counter
//! from the `-- Cache --` line of `generate_global_stats` (the root `.stats`).
//! The read path runs through `DownloadService`, the same engine the FUSE layer
//! drives, against a live libtorrent seeder.

use crate::common::{
    acquire_session_lock, build_multipiece_torrent_pieces, local_test_config, wave_slot,
    TestHarness,
};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::{generate_global_stats, generate_torrent_stats};
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-cache-03";

const PIECE_LEN: usize = 256 * 1024;
const NUM_PIECES: usize = 16;
/// Eight of the sixteen pieces.  Small enough that the whole-file read must
/// evict, large enough that the re-read piece and its prefetch window stay
/// resident after the re-download (a four-piece budget would evict the very
/// piece the assertion observes).
const CACHE_BYTES: u64 = (8 * PIECE_LEN) as u64;
const TORRENT_NAME: &str = "tc-cache-03-small-cache.bin";
const READ_BUDGET: Duration = Duration::from_secs(120);

#[test]
fn tc_cache_03_evicted_piece_is_cached_again_on_reread() {
    let _slot = wave_slot();
    let _session_guard = acquire_session_lock();

    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_pieces(announce_url, TORRENT_NAME, NUM_PIECES)
    });
    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    config.local_discovery.lsd_enabled = Some(false);
    config.cache.cache_size = Some(CACHE_BYTES as i64);
    // The re-read of an evicted piece first spends the stale-piece recheck and
    // piece-wait windows before the retry downloads it; the discovery window
    // also caps `read_timeout_secs`, so keep it wide enough that a loaded host
    // can connect the seeder instead of settling on an empty-swarm verdict.
    config.timeouts.read_timeout_secs = Some(60);

    let service = Arc::new(DownloadService::new(cache_dir.path(), &config).expect("engine"));
    let info = Arc::new(TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("parse"));
    let info_hash = hex::encode(info.info_hash().expect("info hash"));

    let mut db = Database::open_in_memory().expect("in-memory db");
    let InsertTorrentResult::Inserted(torrent_id) = db
        .insert_torrent(
            "data",
            TORRENT_NAME,
            "tc-cache-03-small-cache.bin.torrent",
            info.total_size() as i64,
            &info_hash,
            1,
        )
        .expect("insert torrent row")
    else {
        panic!("expected a fresh torrent row");
    };
    let db = Some(Arc::new(Mutex::new(db)));

    // Read the whole 4 MiB file one piece at a time — the shape of a FUSE
    // `cat` — against a cache that only holds 2 MiB.
    let content = harness.file_content.clone();
    let mut offset = 0usize;
    while offset < content.len() {
        let n = PIECE_LEN.min(content.len() - offset);
        let data = read_once(&service, &info, offset as u64, n as u32, READ_BUDGET);
        assert_eq!(
            data,
            &content[offset..offset + n],
            "whole-file read mismatch at offset {offset}"
        );
        offset += n;
    }

    let cache = service.get_cache_manager().expect("cache manager");
    wait_until("an eviction under a cache smaller than the data", Duration::from_secs(60), || {
        cache.lock().expect("cache lock").eviction_count > 0
    });

    // Pick a piece the eviction actually removed.  The grid renders the same
    // `CacheManager` state this check reads, so a non-cached entry here is a
    // non-cached marker in `.stats`.
    let evicted: Vec<usize> = (0..NUM_PIECES)
        .filter(|&piece| {
            !cache
                .lock()
                .expect("cache lock")
                .has_piece(&piece_key(&info_hash, piece))
        })
        .collect();
    assert!(
        !evicted.is_empty(),
        "a cache of {CACHE_BYTES} bytes cannot hold the {} byte file, so at least one piece \
         must have been evicted",
        info.total_size()
    );
    let piece = evicted[0];

    // Re-read the evicted piece: the bytes must come back from a fresh download
    // and the piece must be cached again.
    let piece_start = piece * PIECE_LEN;
    let data = read_once(
        &service,
        &info,
        piece_start as u64,
        PIECE_LEN as u32,
        READ_BUDGET,
    );
    assert_eq!(
        data,
        &content[piece_start..piece_start + PIECE_LEN],
        "re-read of evicted piece {piece} returned the wrong bytes"
    );

    let marker = wait_for_marker(
        &service,
        &db,
        torrent_id,
        &info_hash,
        piece,
        Duration::from_secs(20),
        is_cached_marker,
    );
    assert!(
        marker == "[x]" || marker.starts_with("[X "),
        "re-read piece {piece} must show a cached marker in leaf .stats, got {marker}"
    );

    // The eviction counter is the root `.stats` Cache line.
    let evictions = wait_for_root_evictions(&service, &db, Duration::from_secs(10));
    assert!(
        evictions > 0,
        "root .stats must report a growing eviction count after the eviction"
    );
    let live = cache.lock().expect("cache lock").eviction_count;
    assert!(
        live > 0 && evictions <= live,
        "root .stats eviction count must track the live counter (rendered {evictions}, live {live})"
    );

    service.shutdown();
}

fn piece_key(info_hash: &str, piece: usize) -> String {
    format!("{info_hash}:piece:{piece}")
}

/// `[x]` is cached-but-never-accessed, `[X n]` cached-and-accessed.  Both mean
/// the piece is back in the cache; `[7]`/`[N]` only mean it is wanted.
fn is_cached_marker(marker: &str) -> bool {
    marker == "[x]" || marker.starts_with("[X ")
}

fn read_once(
    service: &DownloadService,
    info: &Arc<TorrentInfo>,
    offset: u64,
    size: u32,
    budget: Duration,
) -> Vec<u8> {
    let start = Instant::now();
    loop {
        match service.read_file_range(Arc::clone(info), 0, offset, size) {
            Ok(data) if data.len() == size as usize => return data,
            Ok(data) => {
                if start.elapsed() >= budget {
                    panic!(
                        "timed out reading {size} bytes at offset {offset} (got {} bytes)",
                        data.len()
                    );
                }
            }
            Err(error) => {
                if start.elapsed() >= budget {
                    panic!("timed out reading {size} bytes at offset {offset}: {error:?}");
                }
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
}

fn render_leaf(
    service: &Arc<DownloadService>,
    db: &Option<Arc<Mutex<Database>>>,
    torrent_id: i64,
    info_hash: &str,
) -> String {
    String::from_utf8_lossy(&generate_torrent_stats(
        torrent_id,
        info_hash,
        db,
        &Some(Arc::clone(service)),
        || service.get_cache_manager(),
    ))
    .into_owned()
}

/// Marker tokens of the `  Pieces: ` line: `[x][X 3][7][]` → four tokens.
/// Markers carry no separator by contract, so the closing bracket delimits.
fn piece_tokens(text: &str) -> Option<Vec<String>> {
    let line = text.lines().find_map(|line| line.strip_prefix("  Pieces: "))?;
    let mut tokens = Vec::new();
    let mut rest = line;
    while let Some(open) = rest.find('[') {
        let close = open + rest[open..].find(']')?;
        tokens.push(rest[open..=close].to_string());
        rest = &rest[close + 1..];
    }
    Some(tokens)
}

fn wait_for_marker(
    service: &Arc<DownloadService>,
    db: &Option<Arc<Mutex<Database>>>,
    torrent_id: i64,
    info_hash: &str,
    piece: usize,
    budget: Duration,
    accept: impl Fn(&str) -> bool,
) -> String {
    let deadline = Instant::now() + budget;
    loop {
        let last = render_leaf(service, db, torrent_id, info_hash);
        if let Some(tokens) = piece_tokens(&last) {
            if let Some(marker) = tokens.get(piece) {
                if accept(marker) {
                    return marker.clone();
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "leaf .stats never showed the expected marker for piece {piece}:\n{last}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

/// Eviction count from the root `.stats` `-- Cache --` line.  The renderer
/// inserts thousand separators (`format_num`), so the digits are reassembled
/// before parsing.  A missing line or an unparsable count is an error — never
/// evidence that no eviction happened.
fn root_evictions(text: &str) -> Result<u64, String> {
    let line = text
        .lines()
        .find(|line| line.trim_start().starts_with("Hits:") && line.contains("Evictions:"))
        .ok_or_else(|| "root .stats carries no Cache line".to_string())?;
    let count = line
        .split("Evictions:")
        .nth(1)
        .and_then(|tail| tail.trim().split_whitespace().next())
        .ok_or_else(|| format!("root .stats Cache line carries no eviction count: {line:?}"))?;
    count
        .replace(',', "")
        .parse::<u64>()
        .map_err(|error| format!("cannot parse the eviction count {count:?}: {error}"))
}

fn wait_for_root_evictions(
    service: &Arc<DownloadService>,
    db: &Option<Arc<Mutex<Database>>>,
    budget: Duration,
) -> u64 {
    let deadline = Instant::now() + budget;
    loop {
        let text = String::from_utf8_lossy(&generate_global_stats(
            Duration::from_secs(1),
            db,
            None,
            || service.get_cache_manager(),
            "0.0.0.0:0",
            None,
        ))
        .into_owned();
        let parsed = match root_evictions(&text) {
            Ok(evictions) => evictions,
            // A count we cannot parse is a broken observable, not a stall.
            Err(reason) => panic!("{reason}\n{text}"),
        };
        if parsed > 0 {
            return parsed;
        }
        // A busy cache lock renders zeroed counters; keep polling, and report
        // the parsed value so "still zero" is distinct from "unparsable".
        assert!(
            Instant::now() < deadline,
            "root .stats reported no eviction growth within {budget:?} (rendered count: \
             {parsed}):\n{text}"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

/// The root `.stats` renderer inserts thousand separators, so a real counter
/// past 1000 must parse instead of being read as "no evictions".
#[test]
fn tc_cache_03_parses_thousands_separated_root_counts() {
    let line = "  Hits: 12,345  Misses: 1,234  Hit Rate: 90.9%  Evictions: 1,024";
    assert_eq!(root_evictions(line), Ok(1024));
}

/// An unparsable count is an error, never a zero count.
#[test]
fn tc_cache_03_rejects_an_unparsable_root_count() {
    let line = "  Hits: 0  Misses: 0  Hit Rate: 0.0%  Evictions: —";
    assert!(root_evictions(line).is_err());
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

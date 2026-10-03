//! StatsGenerator — generates the .stats file content.
//! Extracted from TorrentFs to separate stats generation from FUSE protocol handling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use crate::cache::CacheManager;
use crate::db::{Database, TorrentStatus};
use crate::infrastructure::config::DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS;
use crate::infrastructure::download::PieceStatus;
use crate::infrastructure::download::PieceStore;
use crate::infrastructure::download::SessionStats;
use crate::infrastructure::metadata::TrackerEntry;
use crate::infrastructure::metrics::MetricsSnapshot;
use crate::services::download::DownloadService;

/// Format bytes into human-readable form.
pub fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = KB * 1024;
    const GB: u64 = MB * 1024;

    if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} B", bytes)
    }
}

/// Format a number with thousand separators.
pub fn format_num(n: u64) -> String {
    let s = n.to_string();
    let len = s.len();
    let mut result = String::with_capacity(len + (len.saturating_sub(1)) / 3);
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (len - i) % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result
}

/// Render the cache-usage value for `.stats` with usage capped at the limit.
///
/// `CacheManager::current_size` can transiently exceed `max_cache_size`:
/// `evict_lru` never evicts a piece libtorrent is still writing, so it accepts
/// an over-budget state until the next pass. The raw ratio would print an
/// impossible `200.0%`, reading as a broken limit. Cap the displayed usage at
/// the limit and report the remainder as pending eviction instead.
fn format_cache_usage(current: u64, max: u64) -> String {
    if max == 0 {
        return format!("{} / {} (0.0%)", format_bytes(current), format_bytes(max));
    }
    let used = current.min(max);
    let pct = (used as f64 / max as f64) * 100.0;
    if current > max {
        format!(
            "{} / {} ({:.1}%, {} pending eviction)",
            format_bytes(used),
            format_bytes(max),
            pct,
            format_bytes(current - max)
        )
    } else {
        format!(
            "{} / {} ({:.1}%)",
            format_bytes(used),
            format_bytes(max),
            pct
        )
    }
}

// ── Shared helpers ──────────────────────────────────────────────────────────

const BANNER: &str = "===========================================================\n";
const BANNER_LINE: &str = "===========================================================";

fn write_banner(output: &mut String, subtitle: Option<&str>) {
    let version = env!("CARGO_PKG_VERSION");
    output.push_str(BANNER);
    if let Some(sub) = subtitle {
        output.push_str(&format!("  torrentfs v{} — {}\n", version, sub));
    } else {
        output.push_str(&format!("  torrentfs v{}\n", version));
    }
    output.push_str(BANNER);
    output.push_str("\n\n");
}

fn write_overview(
    output: &mut String,
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    session_stats: Option<&SessionStats>,
    get_cache_manager: &impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    listen_addr: &str,
) {
    let now = std::time::SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let uptime_secs = now.as_secs().saturating_sub(creation_time.as_secs());
    let uptime_h = uptime_secs / 3600;
    let uptime_m = (uptime_secs % 3600) / 60;
    let uptime_s = uptime_secs % 60;

    output.push_str("-- Overview --\n");
    output.push_str(&format!(
        "  Uptime:       {}h {}m {}s\n",
        uptime_h, uptime_m, uptime_s
    ));
    output.push_str("  Mount:        (dynamic)\n");

    let db_path = if db.is_some() { "(active)" } else { "(none)" };
    output.push_str(&format!("  Database:     {}\n", db_path));

    let (cache_total_size, cache_max_size, cache_dir_str) =
        if let Some(ref cm) = get_cache_manager() {
            if let Ok(cm_guard) = cm.try_lock() {
                (
                    cm_guard.current_size(),
                    cm_guard.max_cache_size(),
                    "(cache)",
                )
            } else {
                (0, 0, "(locked)")
            }
        } else {
            (0, 0, "(none)")
        };
    output.push_str(&format!("  Cache Dir:    {}\n", cache_dir_str));
    output.push_str(&format!(
        "  Cache Usage:  {}\n",
        format_cache_usage(cache_total_size, cache_max_size)
    ));

    if let Some(ss) = session_stats {
        output.push_str(&format!("  Listen:       {}\n", listen_addr));
        output.push_str(&format!("  DHT Nodes:    {}\n", ss.dht_nodes));
    } else {
        output.push_str("  Listen:       (not available)\n");
        output.push_str("  DHT Nodes:    —\n");
    }
}

fn write_global_rates(output: &mut String, session_stats: Option<&SessionStats>) {
    output.push_str("\n-- Global Rates --\n");
    if let Some(ss) = session_stats {
        output.push_str(&format!(
            "  Download Rate:  {}/s\n",
            format_bytes(ss.download_rate as u64)
        ));
        output.push_str(&format!(
            "  Upload Rate:    {}/s\n",
            format_bytes(ss.upload_rate as u64)
        ));
        output.push_str(&format!(
            "  Total DL:       {}\n",
            format_bytes(ss.total_downloaded as u64)
        ));
        output.push_str(&format!(
            "  Total UL:       {}\n",
            format_bytes(ss.total_uploaded as u64)
        ));
    } else {
        output.push_str("  Download Rate:  —\n");
        output.push_str("  Upload Rate:    —\n");
        output.push_str("  Total DL:       —\n");
        output.push_str("  Total UL:       —\n");
    }
}

fn write_connections(output: &mut String, session_stats: Option<&SessionStats>) {
    output.push_str("\n-- Connections --\n");
    if let Some(ss) = session_stats {
        output.push_str(&format!("  Connected:      {}\n", ss.peers_connected));
        output.push_str(&format!("  Half-open:      {}\n", ss.half_open_connections));
        output.push_str("  Total Attempts: —\n");
    } else {
        output.push_str("  Connected:      —\n");
        output.push_str("  Half-open:      —\n");
        output.push_str("  Total Attempts: —\n");
    }
}

fn write_torrent_overview_counts(output: &mut String, db: &Option<Arc<Mutex<Database>>>) {
    output.push_str("\n-- Torrents --\n");
    let (pending, downloading, seeding, error, total_torrents) = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard
                .get_torrent_counts_by_status()
                .unwrap_or((0, 0, 0, 0, 0))
        } else {
            (0, 0, 0, 0, 0)
        }
    } else {
        (0, 0, 0, 0, 0)
    };

    let unique_info_hashes = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            if let Ok(torrents) = db_guard.get_all_torrents() {
                let mut set: std::collections::HashSet<&str> = std::collections::HashSet::new();
                for t in &torrents {
                    set.insert(t.info_hash.as_str());
                }
                set.len() as i64
            } else {
                0
            }
        } else {
            0
        }
    } else {
        0
    };

    output.push_str(&format!(
        "  Total: {}  Unique: {}  Pending: {}  Downloading: {}  Seeding: {}  Error: {}\n",
        total_torrents, unique_info_hashes, pending, downloading, seeding, error
    ));
}

fn write_global_cache_summary(
    output: &mut String,
    get_cache_manager: &impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) {
    output.push_str("\n-- Cache --\n");
    let (global_hits, global_misses, global_evictions) = if let Some(cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.try_lock() {
            (
                cm_guard.hit_count,
                cm_guard.miss_count,
                cm_guard.eviction_count,
            )
        } else {
            (0, 0, 0)
        }
    } else {
        (0, 0, 0)
    };
    let global_total = global_hits + global_misses;
    let hit_rate = if global_total > 0 {
        (global_hits as f64 / global_total as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!(
        "  Hits: {}  Misses: {}  Hit Rate: {:.1}%  Evictions: {}\n",
        format_num(global_hits),
        format_num(global_misses),
        hit_rate,
        format_num(global_evictions)
    ));
}

fn write_performance(output: &mut String) {
    output.push_str("\n-- Performance --\n");
    output.push_str("  Tick Interval:  1000 ms\n");
    output.push_str("  Memory (RSS):   —\n");
}

fn hit_rate(hits: u64, misses: u64) -> f64 {
    let total = hits + misses;
    if total > 0 {
        (hits as f64 / total as f64) * 100.0
    } else {
        0.0
    }
}

/// Render the observability counters. Absent counters (no
/// metrics wired, e.g. unit tests) render as zeroes/`—`.
fn write_observability(output: &mut String, metrics: Option<&MetricsSnapshot>) {
    output.push_str("\n-- Observability --\n");

    let m = metrics.cloned().unwrap_or_default();

    output.push_str(&format!(
        "  Cache L1 (memory):   hits {}  misses {}  hit rate {:.1}%  entries {}\n",
        format_num(m.l1_hits),
        format_num(m.l1_misses),
        hit_rate(m.l1_hits, m.l1_misses),
        format_num(m.l1_entries)
    ));
    output.push_str(&format!(
        "  Cache L2 (disk):     hits {}  misses {}  hit rate {:.1}%\n",
        format_num(m.l2_hits),
        format_num(m.l2_misses),
        hit_rate(m.l2_hits, m.l2_misses)
    ));
    output.push_str(&format!(
        "  Cache L3 (metadata): hits {}  misses {}  hit rate {:.1}%\n",
        format_num(m.l3_hits),
        format_num(m.l3_misses),
        hit_rate(m.l3_hits, m.l3_misses)
    ));
    output.push_str(&format!(
        "  Deferred reads:      {}  Pending: {} current / {} peak\n",
        format_num(m.deferred_reads),
        format_num(m.pending_reads_current),
        format_num(m.pending_reads_peak)
    ));
    output.push_str(&format!(
        "  Poll hit rate:       hits {} / checks {} ({:.1}%)\n",
        format_num(m.poll_hits),
        format_num(m.poll_checks),
        hit_rate(m.poll_hits, m.poll_checks)
    ));
    output.push_str(&format!(
        "  Download queue:      {} current / {} peak\n",
        format_num(m.download_queue_current),
        format_num(m.download_queue_peak)
    ));
    output.push_str(&format!(
        "  Workers:             {} active / {} peak\n",
        format_num(m.workers_active),
        format_num(m.workers_peak)
    ));

    let avg_wait_us = if m.lock_acquires > 0 {
        m.lock_wait_nanos / m.lock_acquires / 1_000
    } else {
        0
    };
    output.push_str(&format!(
        "  Lock wait:           {} acquisitions, avg {} µs (total {} ms)\n",
        format_num(m.lock_acquires),
        format_num(avg_wait_us),
        m.lock_wait_nanos / 1_000_000
    ));
}

fn status_to_english(status: &TorrentStatus) -> &'static str {
    match status {
        TorrentStatus::Pending => "Pending",
        TorrentStatus::Downloading => "Downloading",
        TorrentStatus::Seeding => "Seeding",
        TorrentStatus::Error => "Error",
    }
}

/// Display status for a torrent, derived from the same authoritative piece
/// snapshot that drives the progress column.  The persisted `torrents.status`
/// column has no production writer, so reading it would render `Pending` even
/// for a fully cached torrent; deriving from the piece snapshot keeps `Status`
/// and `Progress` in one `.stats` consistent: fully cached → `Seeding` at
/// `100.0%`, partially cached or actively read → `Downloading`, untouched →
/// `Pending`.  With no snapshot (no handle yet) the persisted status is
/// returned, matching the progress column's own fallback.
fn display_status(persisted: &TorrentStatus, pieces: Option<&[PieceStatus]>) -> TorrentStatus {
    let Some(pieces) = pieces else {
        return persisted.clone();
    };
    if is_download_complete(pieces) {
        TorrentStatus::Seeding
    } else if has_cached_piece(pieces) || has_active_reader(pieces) {
        TorrentStatus::Downloading
    } else {
        TorrentStatus::Pending
    }
}

/// Render the piece marker per the `.stats` spec:
/// `[x]` cached but never accessed (`hit_count == 0`),
/// `[X n]` cached and accessed `n` times (`hit_count > 0`),
/// `[N]` wanted but not cached (`!is_cached && priority > 0`),
/// `[]` not wanted and not cached (`!is_cached && priority == 0`).
fn piece_marker(status: &PieceStatus) -> String {
    if status.is_cached {
        if status.hit_count > 0 {
            format!("[X {}]", status.hit_count)
        } else {
            "[x]".to_string()
        }
    } else if status.priority > 0 {
        format!("[{}]", status.priority)
    } else {
        "[]".to_string()
    }
}

/// Download progress `[0.0, 1.0]` from actual cached pieces. libtorrent's
/// `status.progress` is unreliable under the custom `PieceStorageDiskIO`
/// backend — its piece bitmap can report 1.0 even when nothing is downloaded,
/// because `async_check_files` reports success without feeding the bitmap back
/// and `async_hash` failures count as "not present". Recompute from the
/// authoritative `is_cached` snapshot so `.stats` never shows 100% while
/// reads still time out waiting for pieces.
fn piece_progress(pieces: &[PieceStatus]) -> f64 {
    if pieces.is_empty() {
        return 0.0;
    }
    let cached = pieces.iter().filter(|p| p.is_cached).count();
    cached as f64 / pieces.len() as f64
}

/// Downloaded bytes from actual cached pieces. libtorrent's `status.total_done`
/// is unreliable under the custom `PieceStorageDiskIO` backend for the same
/// reason as [`piece_progress`]: the piece bitmap is never fed back, so
/// `total_done` stays 0 after caching. Recompute from the authoritative
/// `is_cached` snapshot so `.stats` shows non-zero Downloaded once cached.
fn piece_downloaded(pieces: &[PieceStatus], piece_length: u64) -> u64 {
    if pieces.is_empty() || piece_length == 0 {
        return 0;
    }
    let cached = pieces.iter().filter(|p| p.is_cached).count() as u64;
    cached * piece_length
}

/// Whether a torrent is fully downloaded, judged by **actual** piece
/// availability (`is_cached`). Returns `false` for an empty piece list — no
/// pieces means the snapshot is absent or the torrent has no content, neither
/// of which counts as a completed download.
fn is_download_complete(pieces: &[PieceStatus]) -> bool {
    !pieces.is_empty() && pieces.iter().all(|p| p.is_cached)
}

/// Whether any piece is cached (`is_cached`) — the torrent has already
/// pulled down some bytes. Partial progress, distinct from the all-cached
/// state covered by [`is_download_complete`].
fn has_cached_piece(pieces: &[PieceStatus]) -> bool {
    pieces.iter().any(|p| p.is_cached)
}

/// Whether any piece currently carries a reader priority (`priority > 0`).
/// True while a reader holds its own gradient — and also while a released
/// reader's gradient is retained as the prefetch window, so this is *not* a
/// live-reader signal; use the scheduler's reader count for that.
fn has_active_reader(pieces: &[PieceStatus]) -> bool {
    pieces.iter().any(|p| p.priority > 0)
}

/// Render the `.stats` Pieces block. The header line is kept verbatim for
/// humans and existing consumers; the piece-marker line is prefixed with a
/// `Pieces:` label so parsers don't match the header's `Pieces (` literal.
/// `PieceSize`/`PieceCount` key-value lines follow the marker so metadata can
/// be read structurally instead of regex-parsing the header (which must stay
/// byte-for-byte unchanged).
///
/// The marker run is contiguous and whitespace-terminated by construction: no
/// separators and no trailing whitespace, so a strict
/// `^(\[\]|\[x\]|\[X n\]|\[N\])+$` check over the run passes.
fn piece_block(piece_length: u64, pieces: &[PieceStatus]) -> String {
    let markers: String = pieces.iter().map(piece_marker).collect();
    let mut out = String::new();
    out.push_str(&format!(
        "\n-- Pieces ({} pieces, {} each) --\n  Pieces: {}\n",
        pieces.len(),
        format_bytes(piece_length),
        markers.trim_end()
    ));
    out.push_str(&format!(
        "  PieceSize: {}\n  PieceCount: {}\n",
        format_bytes(piece_length),
        pieces.len()
    ));
    out
}

/// Render the `.stats` `-- Trackers --` block for one torrent.
///
/// Entries render as `tier <n>  <url>` in BEP-12 announce order (lowest tier
/// first, announce order kept inside a tier). `None` means no tracker list was
/// obtained — no download service, a snapshot locked by the engine, no handle
/// for this info_hash, or a failed handle read — so it renders cause-neutral:
/// naming one cause, or "no trackers", would assert a fact the data never
/// carried. `dht_nodes` is the session-wide DHT node count, marked `(global)`.
fn write_trackers(output: &mut String, trackers: Option<&[TrackerEntry]>, dht_nodes: Option<i32>) {
    output.push_str("\n-- Trackers --\n");
    match trackers {
        Some([]) => output.push_str("  No trackers — relying on DHT/LSD\n"),
        Some(list) => {
            let mut ordered: Vec<&TrackerEntry> = list.iter().collect();
            ordered.sort_by_key(|entry| entry.tier);
            for entry in ordered {
                output.push_str(&format!("  tier {}  {}\n", entry.tier, entry.url));
            }
        }
        None => output.push_str("  (unavailable — tracker list not read)\n"),
    }
    if let Some(nodes) = dht_nodes {
        output.push_str(&format!("  DHT Nodes: {} (global)\n", nodes));
    }
}

/// Consecutive seconds a torrent's swarm must stay empty (no peers, no seeds)
/// before the `.stats` health alert fires.  Peer/seed counts are instantaneous
/// samples that flap around zero while connections are established, so one
/// empty sample is not a health signal: alerting on it contradicts the `Peers:`
/// line a reader saw moments earlier or later.  The grace is the engine's
/// configured no-seeder *piece-wait* window
/// (`[timeouts] no_seeder_read_timeout_secs`), not the peer-discovery window: a
/// read that is still waiting for the swarm keeps its pieces wanted
/// (`priority > 0`), and that `active_download` signal — not this grace — holds
/// the alert off through the discovery phase, which may last
/// `[timeouts] peer_discovery_wait_secs` (30s by default).  A shorter
/// `read_timeout_secs`, which caps the piece-wait window, only makes the grace
/// the more conservative of the two.
fn empty_swarm_grace_secs(download_service: &Option<Arc<DownloadService>>) -> u64 {
    download_service
        .as_ref()
        .map(|ds| ds.no_seeder_read_timeout_secs())
        .unwrap_or(DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS)
}

/// Consecutive seconds a torrent must report a connected seeder with a zero
/// download rate before the `.stats` slow-swarm alert fires.  `download_rate`
/// is an instantaneous sample and a just-connected seeder has not delivered its
/// first block yet, so one zero sample is not a stall — alerting on it would
/// fire on every healthy connection.  Long enough to cover connection setup and
/// a bursty rate window, short enough to surface a stalled seeder well inside
/// the read's wait window.
const HEALTH_ALERT_SLOW_SWARM_GRACE_SECS: u64 = 5;

/// Render the per-torrent `.stats` health alert line, or `None` when the
/// observed state needs no explanation: a read waiting on an empty swarm (no
/// grace — a blocked reader is not a flapping peer count), a connected seeder
/// that has delivered nothing for [`HEALTH_ALERT_SLOW_SWARM_GRACE_SECS`], or a
/// sustained empty swarm with no download in progress (its grace is the
/// engine's configured no-seeder window, see [`empty_swarm_grace_secs`]).
/// `download_complete` suppresses every alert — zero peers is then the expected
/// end state — and counts reflect live connections, not tracker reachability.
fn health_alert(
    num_peers: i32,
    num_seeds: i32,
    download_rate: i64,
    empty_swarm_secs: u64,
    empty_swarm_grace_secs: u64,
    slow_swarm_secs: u64,
    download_complete: bool,
    active_download: bool,
    is_read_waiting: bool,
) -> Option<String> {
    if download_complete {
        return None;
    }
    // A blocked reader is evidence the empty sample is not connections still
    // flapping, so this alert needs no grace window.
    if num_peers == 0 && num_seeds == 0 && is_read_waiting {
        return Some(format!(
            "  ⚠ Waiting for peers ({empty_swarm_secs}s) — no connected peers; \
             a reader is waiting\n"
        ));
    }
    // Connected yet delivering nothing — a bare peer count cannot express it.
    // The rate must have held at zero for the grace window: the sample right
    // after a seeder connects lands before its first block.
    if num_seeds >= 1
        && download_rate == 0
        && is_read_waiting
        && slow_swarm_secs >= HEALTH_ALERT_SLOW_SWARM_GRACE_SECS
    {
        return Some(format!(
            "  ⚠ Slow swarm: seeder connected but no progress for {slow_swarm_secs}s\n"
        ));
    }
    if num_peers == 0
        && num_seeds == 0
        && empty_swarm_secs >= empty_swarm_grace_secs
        && !active_download
    {
        return Some(
            "  ⚠ Health: 0 peers / 0 seeds — no connected peers; tracker may be reachable\n"
                .to_string(),
        );
    }
    None
}

// ── Public API ──────────────────────────────────────────────────────────────

/// Generate global stats (no per-torrent details, no per-infohash cache breakdown).
pub fn generate_global_stats(
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    session_stats: Option<SessionStats>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    listen_addr: &str,
    metrics: Option<MetricsSnapshot>,
) -> Vec<u8> {
    let mut output = String::new();

    write_banner(&mut output, None);
    let ss_ref = session_stats.as_ref();
    write_overview(
        &mut output,
        creation_time,
        db,
        ss_ref,
        &get_cache_manager,
        listen_addr,
    );
    write_global_rates(&mut output, ss_ref);
    write_connections(&mut output, ss_ref);
    write_torrent_overview_counts(&mut output, db);
    write_global_cache_summary(&mut output, &get_cache_manager);
    write_performance(&mut output);
    write_observability(&mut output, metrics.as_ref());

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Generate stats for a single torrent identified by torrent_id and info_hash.
pub fn generate_torrent_stats(
    torrent_id: i64,
    info_hash: &str,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<Arc<DownloadService>>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) -> Vec<u8> {
    let mut output = String::new();

    let torrent = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard.get_torrent_by_id(torrent_id).ok().flatten()
        } else {
            None
        }
    } else {
        None
    };

    let t = match torrent {
        Some(t) => t,
        None => {
            output.push_str(&format!(
                "  Torrent not found (id={}, info_hash={}...)\n",
                torrent_id,
                &info_hash[..std::cmp::min(10, info_hash.len())]
            ));
            output.push('\n');
            output.push_str(BANNER_LINE);
            output.push('\n');
            return output.into_bytes();
        }
    };

    // Torrent title line
    output.push_str(&format!("===== torrent: {} =====\n\n", t.name));

    // Authoritative piece snapshot, shared by the Status and Progress columns
    // so the two can never disagree within this file.
    let piece_statuses = download_service
        .as_ref()
        .and_then(|ds| ds.try_get_pieces_status(info_hash));

    let status_str = status_to_english(&display_status(
        &t.status,
        piece_statuses.as_ref().map(|(_, pieces)| pieces.as_slice()),
    ));

    let (
        dl_rate,
        ul_rate,
        num_peers,
        num_seeds,
        progress,
        total_size,
        total_done,
        total_upload,
        total_download,
    ) = download_service
        .as_ref()
        .and_then(|ds| ds.try_query_torrent_status(info_hash))
        .map(|status| {
            (
                status.download_rate,
                status.upload_rate,
                status.num_peers,
                status.num_seeds,
                status.progress,
                status.total,
                status.total_done,
                status.total_upload,
                status.total_download,
            )
        })
        .unwrap_or((0, 0, 0, 0, 0.0, 0, 0, 0, 0));

    // Override libtorrent's progress with piece-availability-based progress.
    // libtorrent's status.progress is unreliable under the custom storage
    // backend (can report 1.0 before pieces are downloaded). Use the actual
    // cached piece count instead.
    let actual_progress = piece_statuses
        .as_ref()
        .map(|(_, pieces)| piece_progress(pieces))
        .unwrap_or(progress as f64);
    let prog_pct = if total_size > 0 {
        actual_progress * 100.0
    } else {
        0.0
    };

    // Override libtorrent's total_done with piece-availability-based bytes.
    // Same root cause as progress: status.total_done stays 0
    // under the custom storage backend. Recompute from cached pieces.
    let total_done = piece_statuses
        .as_ref()
        .map(|(piece_length, pieces)| piece_downloaded(pieces, *piece_length))
        .unwrap_or(total_done);

    // A fully cached torrent (all pieces present) is download-complete:
    // zero peers is then the expected end state, so suppress the health
    // alert. `piece_statuses` is the authoritative availability
    // source — libtorrent's `progress` can report 1.0 prematurely
    // so every piece must actually be cached, not merely
    // reported as such by libtorrent.
    let download_complete = piece_statuses
        .as_ref()
        .map(|(_, pieces)| is_download_complete(pieces))
        .unwrap_or(false);
    // A torrent actively fetching bytes — some piece already cached, or some
    // piece wanted by an active reader — is making progress, so suppress the
    // health alert during the transient zero-peer window before the tracker
    // announce returns. An absent piece snapshot yields no signal
    // either way: neither flag suppresses the alert.
    let active_download = piece_statuses
        .as_ref()
        .map(|(_, pieces)| has_cached_piece(pieces) || has_active_reader(pieces))
        .unwrap_or(false);
    // The reads the engine is holding for this torrent: one snapshot entry
    // yields both the oldest wait and the count, so `Waiting`, `Waited` and
    // `Active readers` can never disagree — including while a parked read is
    // still settling or rechecking, before it holds any piece priority. An
    // absent entry means no read is parked, which is not the same as waiting
    // zero seconds, so the lines are dropped rather than zeroed.
    let waiting_reads = download_service
        .as_ref()
        .and_then(|ds| ds.try_waiting_reads(info_hash));
    let is_read_waiting = waiting_reads.is_some();
    // How long the swarm has been continuously empty, from the engine's
    // per-tick samples. An absent entry (no handle, unreadable status, or a
    // live peer/seed) is not an observed empty swarm, so it counts as zero.
    let empty_swarm_secs = download_service
        .as_ref()
        .and_then(|ds| ds.try_empty_swarm_secs(info_hash))
        .unwrap_or(0);
    // How long a connected seeder has delivered nothing while a read waited.
    // Zero when the torrent is not in that state.
    let slow_swarm_secs = download_service
        .as_ref()
        .and_then(|ds| ds.try_slow_swarm_secs(info_hash))
        .unwrap_or(0);
    output.push_str("-- Status --\n");
    output.push_str(&format!("  Name: {}\n", t.name));
    output.push_str(&format!(
        "  Status: {}  Progress: {:.1}%  Size: {}\n",
        status_str,
        prog_pct,
        format_bytes(t.total_size as u64)
    ));

    let share = if total_download > 0 {
        format!("{:.2}", total_upload as f64 / total_download as f64)
    } else {
        "—".to_string()
    };

    output.push_str(&format!(
        "  DL: {}  UL: {}  Ratio: {}\n",
        format_bytes(total_done),
        format_bytes(total_upload as u64),
        share
    ));

    // -- Rates --
    output.push_str("\n-- Rates --\n");
    output.push_str(&format!(
        "  Rate: ↓ {}/s  ↑ {}/s\n",
        format_bytes(dl_rate as u64),
        format_bytes(ul_rate as u64)
    ));

    // -- Peers --
    output.push_str("\n-- Peers --\n");
    output.push_str(&format!("  Peers: {}  Seeds: {}\n", num_peers, num_seeds));
    // Wait-period fields, all three read from the one parked-read entry above.
    // `Waiting` is the state — the engine is holding a read right now — and
    // `Waited` carries that read's age; the peer-wait age, when there is one,
    // sits on the alert line (nothing is connected).
    output.push_str(if is_read_waiting {
        "  Waiting: yes\n"
    } else {
        "  Waiting: no\n"
    });
    // The oldest parked read's own wait, which also covers a connected-but-slow
    // seeder the peer wait does not. Dropped once no read is parked.
    if let Some(waits) = waiting_reads {
        output.push_str(&format!("  Waited: {}s\n", waits.oldest_secs));
    }
    // How many reads the engine is holding. Counted from the same aggregation,
    // so this is not the piece grid's `[N]` markers below: a torrent whose
    // readers have all released keeps its retained prefetch gradient but has no
    // parked read, and reports 0 here.
    output.push_str(&format!(
        "  Active readers: {}\n",
        waiting_reads.map(|waits| waits.count).unwrap_or(0)
    ));

    let health = health_alert(
        num_peers,
        num_seeds,
        dl_rate,
        empty_swarm_secs,
        empty_swarm_grace_secs(download_service),
        slow_swarm_secs,
        download_complete,
        active_download,
        is_read_waiting,
    );
    if let Some(line) = &health {
        output.push_str(line);
    }

    // -- Pieces -- visualised piece lifecycle (GitHub commit-record grid).
    // Uses only non-blocking locks so `.stats` never blocks on an active
    // download.
    if let Some((piece_length, pieces)) = &piece_statuses {
        if !pieces.is_empty() {
            output.push_str(&piece_block(*piece_length, pieces));
        }
    }

    // Info fields merged after the piece markers (no `-- Info --` header).
    if let Some(cm) = &get_cache_manager() {
        if let Ok(cm_guard) = cm.try_lock() {
            let cache_stats = cm_guard.get_cache_stats_by_infohash(info_hash);
            output.push_str(&format!(
                "Cache: {} pieces  {}  max={}\n",
                cache_stats.piece_count,
                format_bytes(cache_stats.total_size),
                cm_guard.max_cache_size()
            ));
        }
    }
    output.push_str(&format!("info_hash: {}\n", t.info_hash));
    output.push_str(&format!("source_path: \"{}\"\n", t.source_path));

    // -- Trackers -- announce targets, above the tracker_isolation line: for a
    // private torrent this list is exactly what isolation keeps out of other
    // sites' swarms. Both reads are snapshot reads, so rendering never blocks
    // the FUSE dispatch thread on the engine.
    let trackers = download_service
        .as_ref()
        .and_then(|ds| ds.try_trackers(info_hash));
    let dht_nodes = download_service
        .as_ref()
        .map(|ds| ds.snapshot_stats().dht_nodes);
    write_trackers(&mut output, trackers.as_deref(), dht_nodes);

    // PT isolation info: show the private flag and whether
    // tracker merging is isolated. Private torrents (private=1 in the info
    // dict) never participate in cross-site tracker merging.
    let is_private = download_service
        .as_ref()
        .and_then(|ds| ds.try_is_private(info_hash))
        .unwrap_or(false);
    output.push_str(&format!(
        "private: {}  tracker_isolation: {}\n",
        if is_private { "yes" } else { "no" },
        if is_private {
            "isolated (no cross-site merge)"
        } else {
            "merge-eligible"
        }
    ));

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Per-torrent `(downloaded bytes, progress)` for the directory Summary line.
///
/// Prefers the authoritative piece snapshot; when it is absent, falls back to
/// libtorrent's `(progress, total_done)` — the same fallback the `-- Torrents --`
/// detail list and the leaf `.stats` use, so one torrent never shows two
/// different progress values in the same file. Cached bytes size the final
/// piece at its real (short) length via [`PieceStore::expected_piece_size`], so
/// a fully cached small torrent reports its exact size, never more.
fn summary_torrent_stats(
    pieces: Option<(u64, &[PieceStatus])>,
    status: Option<(f64, u64)>,
    total_size: u64,
) -> (u64, f64) {
    match pieces {
        Some((piece_length, pieces)) => {
            let num_pieces = pieces.len() as i32;
            let downloaded = if piece_length == 0 {
                0
            } else {
                pieces
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| p.is_cached)
                    .map(|(i, _)| {
                        PieceStore::expected_piece_size(
                            i as i32,
                            piece_length,
                            num_pieces,
                            total_size,
                        )
                    })
                    .sum()
            };
            (downloaded, piece_progress(pieces))
        }
        None => {
            let (progress, done) = status.unwrap_or((0.0, 0));
            (done.min(total_size), progress)
        }
    }
}

/// Generate aggregated stats for all torrents under a given source_path.
pub fn generate_directory_stats(
    source_path: &str,
    db: &Option<Arc<Mutex<Database>>>,
    download_service: &Option<Arc<DownloadService>>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
) -> Vec<u8> {
    let mut output = String::new();

    output.push_str(&format!("===== directory: {} =====\n\n", source_path));

    let torrents = if let Some(db) = db.as_ref() {
        if let Ok(db_guard) = db.lock() {
            db_guard
                .get_torrents_by_source_path_prefix(source_path)
                .unwrap_or_default()
        } else {
            vec![]
        }
    } else {
        vec![]
    };

    if torrents.is_empty() {
        output.push_str("  No torrents found under this path.\n");
        output.push('\n');
        output.push_str(BANNER_LINE);
        output.push('\n');
        return output.into_bytes();
    }

    let torrent_count = torrents.len();
    let mut total_size: u64 = 0;
    let mut total_done: u64 = 0;
    let mut total_upload: u64 = 0;
    let mut total_download: u64 = 0;
    let mut aggregate_dl_rate: i64 = 0;
    let mut aggregate_ul_rate: i64 = 0;
    let mut aggregate_peers: i32 = 0;
    let mut aggregate_seeds: i32 = 0;

    // Per-torrent (downloaded bytes, progress, display status) from the piece
    // snapshot, collected while aggregating so the Summary section can render
    // one line per torrent without a second piece query.
    let mut per_torrent: Vec<(u64, f64, TorrentStatus)> = Vec::with_capacity(torrents.len());

    for t in &torrents {
        let total = t.total_size.max(0) as u64;
        total_size += total;

        let status = download_service
            .as_ref()
            .and_then(|ds| ds.try_query_torrent_status(&t.info_hash));
        if let Some(status) = &status {
            total_upload += status.total_upload as u64;
            total_download += status.total_download as u64;
            aggregate_dl_rate += status.download_rate;
            aggregate_ul_rate += status.upload_rate;
            aggregate_peers += status.num_peers;
            aggregate_seeds += status.num_seeds;
        }

        let pieces = download_service
            .as_ref()
            .and_then(|ds| ds.try_get_pieces_status(&t.info_hash));
        let (downloaded, progress) = summary_torrent_stats(
            pieces.as_ref().map(|(len, ps)| (*len, ps.as_slice())),
            status.as_ref().map(|s| (s.progress as f64, s.total_done)),
            total,
        );
        total_done += downloaded;
        per_torrent.push((
            downloaded,
            progress,
            display_status(&t.status, pieces.as_ref().map(|(_, ps)| ps.as_slice())),
        ));
    }

    // -- Summary -- aggregate counts plus a one-line summary per torrent.
    output.push_str("-- Summary --\n");
    output.push_str(&format!(
        "  Torrents: {}  Total Size: {}  Downloaded: {}\n",
        torrent_count,
        format_bytes(total_size),
        format_bytes(total_done)
    ));
    for (t, (downloaded, progress, display)) in torrents.iter().zip(&per_torrent) {
        let status_str = status_to_english(display);
        let prog_pct = if t.total_size > 0 {
            progress * 100.0
        } else {
            0.0
        };
        output.push_str(&format!(
            "  {}  {}  {:.1}%  {} / {}\n",
            t.name,
            status_str,
            prog_pct,
            format_bytes(*downloaded),
            format_bytes(t.total_size.max(0) as u64),
        ));
    }

    // -- Rates --
    output.push_str("\n-- Rates --\n");
    output.push_str(&format!(
        "  DL Rate: ↓ {}/s  UL Rate: ↑ {}/s\n",
        format_bytes(aggregate_dl_rate as u64),
        format_bytes(aggregate_ul_rate as u64)
    ));
    output.push_str(&format!(
        "  Total UL: {}  Total DL: {}\n",
        format_bytes(total_upload),
        format_bytes(total_download)
    ));

    // -- Peers --
    output.push_str("\n-- Peers --\n");
    output.push_str(&format!(
        "  Peers: {}  Seeds: {}\n",
        aggregate_peers, aggregate_seeds
    ));

    // -- Cache --
    output.push_str("\n-- Cache --\n");
    if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.try_lock() {
            let (cache_total_size, cache_max_size) =
                (cm_guard.current_size(), cm_guard.max_cache_size());
            let global_hits = cm_guard.hit_count;
            let global_misses = cm_guard.miss_count;
            let global_total = global_hits + global_misses;
            let hit_rate = if global_total > 0 {
                (global_hits as f64 / global_total as f64) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!(
                "  Cache Usage: {}\n",
                format_cache_usage(cache_total_size, cache_max_size)
            ));
            output.push_str(&format!(
                "  Hits: {}  Misses: {}  Hit Rate: {:.1}%\n",
                format_num(global_hits),
                format_num(global_misses),
                hit_rate
            ));
        } else {
            output.push_str("  (locked)\n");
        }
    } else {
        output.push_str("  (none)\n");
    }

    output.push_str("\n-- Torrents --\n");
    for (idx, t) in torrents.iter().enumerate() {
        let piece_statuses = download_service
            .as_ref()
            .and_then(|ds| ds.try_get_pieces_status(&t.info_hash));
        let status_str = status_to_english(&display_status(
            &t.status,
            piece_statuses.as_ref().map(|(_, pieces)| pieces.as_slice()),
        ));

        let (dl_rate, ul_rate, peers, seeds, progress, ts) = download_service
            .as_ref()
            .and_then(|ds| ds.try_query_torrent_status(&t.info_hash))
            .map(|status| {
                (
                    status.download_rate,
                    status.upload_rate,
                    status.num_peers,
                    status.num_seeds,
                    status.progress,
                    status.total,
                )
            })
            .unwrap_or((0, 0, 0, 0, 0.0, 0));

        // Override libtorrent progress with piece-availability-based progress
        // libtorrent's progress can report 1.0 before pieces are
        // actually downloaded under the custom storage backend.
        let actual_progress = piece_statuses
            .as_ref()
            .map(|(_, pieces)| piece_progress(pieces))
            .unwrap_or(progress as f64);
        let prog_pct = if ts > 0 { actual_progress * 100.0 } else { 0.0 };

        // PT isolation indicator: show a 🔒 marker for private
        // torrents so users can see at a glance which torrents are isolated
        // from cross-site tracker merging.
        let is_private = download_service
            .as_ref()
            .and_then(|ds| ds.try_is_private(&t.info_hash))
            .unwrap_or(false);
        let private_marker = if is_private { "🔒" } else { "  " };

        output.push_str(&format!(
            "  {}#{:<3} {:<40} {}  {:>5.1}%  ↓ {:<10}/s  ↑ {:<10}/s  {:>3}P/{:<3}S\n",
            private_marker,
            idx + 1,
            if t.name.len() > 40 {
                t.name.chars().take(37).collect::<String>() + "..."
            } else {
                t.name.clone()
            },
            status_str,
            prog_pct,
            format_bytes(dl_rate as u64),
            format_bytes(ul_rate as u64),
            peers,
            seeds,
        ));
    }

    output.push('\n');
    output.push_str(BANNER_LINE);
    output.push('\n');
    output.into_bytes()
}

/// Generate the .stats file content (compatibility wrapper).
pub fn generate_stats(
    creation_time: Duration,
    db: &Option<Arc<Mutex<Database>>>,
    session_stats: Option<SessionStats>,
    get_cache_manager: impl Fn() -> Option<Arc<Mutex<CacheManager>>>,
    torrent_data_cache: &Arc<Mutex<HashMap<String, Vec<u8>>>>,
    listen_addr: &str,
    metrics: Option<MetricsSnapshot>,
) -> Vec<u8> {
    let _ = torrent_data_cache;
    generate_global_stats(
        creation_time,
        db,
        session_stats,
        get_cache_manager,
        listen_addr,
        metrics,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_num_zero() {
        assert_eq!(format_num(0), "0");
    }

    #[test]
    fn test_format_num_single_digit() {
        assert_eq!(format_num(5), "5");
    }

    #[test]
    fn test_format_num_two_digits() {
        assert_eq!(format_num(42), "42");
    }

    #[test]
    fn test_format_num_three_digits() {
        assert_eq!(format_num(999), "999");
    }

    #[test]
    fn test_format_num_thousand() {
        assert_eq!(format_num(1000), "1,000");
    }

    #[test]
    fn test_format_num_ten_thousand() {
        assert_eq!(format_num(10000), "10,000");
    }

    #[test]
    fn test_format_num_hundred_thousand() {
        assert_eq!(format_num(100000), "100,000");
    }

    #[test]
    fn test_format_num_million() {
        assert_eq!(format_num(1000000), "1,000,000");
    }

    #[test]
    fn test_format_num_seven_digits() {
        assert_eq!(format_num(1234567), "1,234,567");
    }

    #[test]
    fn test_format_num_eight_digits() {
        assert_eq!(format_num(12345678), "12,345,678");
    }

    #[test]
    fn test_format_num_nine_digits() {
        assert_eq!(format_num(123456789), "123,456,789");
    }

    #[test]
    fn test_format_num_u64_max() {
        assert_eq!(format_num(u64::MAX), "18,446,744,073,709,551,615");
    }

    #[test]
    fn test_piece_marker_semantics() {
        // Downloaded, never accessed → `[x]`
        assert_eq!(
            piece_marker(&PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            }),
            "[x]"
        );
        // Downloaded and accessed 5 times → `[X 5]`
        assert_eq!(
            piece_marker(&PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 5,
            }),
            "[X 5]"
        );
        // Priority 3, not cached → `[3]`
        assert_eq!(
            piece_marker(&PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            }),
            "[3]"
        );
        // Not wanted → `[]`
        assert_eq!(
            piece_marker(&PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            }),
            "[]"
        );
    }

    #[test]
    fn test_piece_progress_empty() {
        assert_eq!(piece_progress(&[]), 0.0);
    }

    #[test]
    fn test_piece_progress_none_cached() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert_eq!(piece_progress(&pieces), 0.0);
    }

    #[test]
    fn test_piece_progress_all_cached() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 2,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
        ];
        assert!((piece_progress(&pieces) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_piece_progress_partial() {
        // 1 of 4 cached → 0.25 (25%)
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 1,
            },
            PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert!((piece_progress(&pieces) - 0.25).abs() < f64::EPSILON);
    }

    #[test]
    fn test_piece_progress_ignores_priority() {
        // High priority but not cached → 0%
        let pieces = vec![
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert_eq!(piece_progress(&pieces), 0.0);
    }

    #[test]
    fn test_piece_grid_shows_priority_during_active_read() {
        // during an active read, pieces with elevated priority
        // (not yet cached) must render as `[N]`, not `[]`.  The bug was that
        // the snapshot never captured elevated priorities, so every piece
        // showed `[]`.  This test locks the `piece_marker` rendering contract
        // the fix depends on: non-cached + priority > 0 → `[N]`.
        let grid = vec![
            // Cached, no accesses → `[x]`
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
            // Active read target, not cached, priority 7 → `[7]`
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
            // Prefetch edge, not cached, priority 1 → `[1]`
            PieceStatus {
                priority: 1,
                is_cached: false,
                hit_count: 0,
            },
            // Outside window, not cached, priority 0 → `[]`
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
        ];
        let rendered: String = grid.iter().map(piece_marker).collect();
        assert_eq!(rendered, "[x][7][1][]");
    }

    #[test]
    fn test_piece_block_has_label_line() {
        // the marker line carries a `Pieces:` label independent of
        // the header's `Pieces (` literal, so parsers can locate the data line.
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
        ];
        let block = piece_block(256 * 1024, &pieces);
        // header stays byte-for-byte verbatim for existing consumers.
        assert!(block.contains("-- Pieces (2 pieces, 256.00 KB each) --\n"));
        assert!(block.contains("\n  Pieces: [x][7]\n"));
        // structured metadata lines appended after the marker line.
        assert!(block.contains("\n  PieceSize: 256.00 KB\n"));
        assert!(block.contains("\n  PieceCount: 2\n"));
    }

    #[test]
    fn test_piece_block_marker_line_is_contiguous_and_whitespace_free() {
        // All four marker forms on one contiguous line, in piece order.
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 12,
            },
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
        ];
        let block = piece_block(256 * 1024, &pieces);
        assert_eq!(
            block,
            "\n-- Pieces (4 pieces, 256.00 KB each) --\n  Pieces: [x][X 12][7][]\n  PieceSize: 256.00 KB\n  PieceCount: 4\n"
        );

        // The marker run must end at a marker, never whitespace, so a strict
        // `^(\[\]|\[x\]|\[X n\]|\[N\])+$` check over the run passes.
        let marker_line = block
            .lines()
            .find(|line| line.starts_with("  Pieces: "))
            .expect("marker line present");
        assert_eq!(marker_line, "  Pieces: [x][X 12][7][]");
        assert!(marker_line.ends_with(']'));
        assert_eq!(marker_line, marker_line.trim_end());
    }

    #[test]
    fn test_global_stats_header_present() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("torrentfs v0.1.0"));
        assert!(text.contains("-- Overview --"));
    }

    #[test]
    fn test_global_stats_no_torrent_details() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("── 种子详情 ──"));
    }

    #[test]
    fn test_global_stats_no_per_infohash_cache() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("[info_hash]"));
    }

    #[test]
    fn test_global_stats_renders_eviction_count() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 64 * 1024).unwrap();

        let first = "aaaa1111:piece:0";
        let second = "bbbb2222:piece:0";

        let first_path = cache.ensure_piece_dir(first).unwrap();
        std::fs::write(&first_path, vec![0u8; 40_000]).unwrap();
        cache.add_piece(first, 40_000).unwrap();

        // The second piece pushes the cache over budget, evicting the first.
        let second_path = cache.ensure_piece_dir(second).unwrap();
        std::fs::write(&second_path, vec![0u8; 40_000]).unwrap();
        cache.add_piece(second, 40_000).unwrap();
        assert_eq!(cache.eviction_count, 1);

        let cm = Arc::new(Mutex::new(cache));
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            {
                let cm = cm.clone();
                move || Some(cm.clone())
            },
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains("Evictions: 1"),
            "stats must render the actual eviction count, got:\n{text}"
        );
    }

    #[test]
    fn test_global_stats_cache_usage_clamped_when_over_budget() {
        // A piece libtorrent is still writing cannot be evicted, so the cache
        // holds more than its limit until the write completes. `.stats` must
        // cap the displayed usage at the limit instead of printing an
        // impossible >100% ratio.
        let temp_dir = tempfile::TempDir::new().unwrap();
        let mut cache = CacheManager::new(temp_dir.path(), 1024 * 1024).unwrap();

        let piece_key = "aaaa1111:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key).unwrap();
        std::fs::write(&piece_path, vec![0u8; 2 * 1024 * 1024]).unwrap();
        cache
            .register_incomplete_piece(piece_key, 2 * 1024 * 1024)
            .unwrap();
        assert_eq!(cache.current_size(), 2 * 1024 * 1024);
        assert_eq!(
            cache.eviction_count, 0,
            "an incomplete piece must not be evicted"
        );

        let cm = Arc::new(Mutex::new(cache));
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            {
                let cm = cm.clone();
                move || Some(cm.clone())
            },
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains("Cache Usage:  1.00 MB / 1.00 MB (100.0%, 1.00 MB pending eviction)"),
            "over-budget cache usage must be capped at the limit, got:\n{text}"
        );
        assert!(
            !text.contains("200.0%"),
            "stats must never render an over-limit percentage, got:\n{text}"
        );
    }

    #[test]
    fn test_torrent_stats_cache_line_shows_max_cache_size() {
        // The leaf `.stats` Cache line must expose the configured limit so a
        // small-cache regression can read `max=<cache_size>` and compare it
        // against the actual config byte-for-byte, rather than re-deriving it
        // from a rounded human-readable form.
        use crate::db::InsertTorrentResult;

        let mut db = Database::open_in_memory().unwrap();
        let InsertTorrentResult::Inserted(source_id) = db
            .insert_torrent(
                "data",
                "ubuntu",
                "ubuntu.torrent",
                1024,
                "hash-cache-max",
                1,
            )
            .unwrap()
        else {
            panic!("expected a fresh insert");
        };
        let db = Some(Arc::new(Mutex::new(db)));

        let temp_dir = tempfile::TempDir::new().unwrap();
        let max_bytes = 32 * 1024 * 1024;
        let mut cache = CacheManager::new(temp_dir.path(), max_bytes).unwrap();
        let piece_key = "hash-cache-max:piece:0";
        let piece_path = cache.ensure_piece_dir(piece_key).unwrap();
        std::fs::write(&piece_path, vec![0u8; 4 * 1024 * 1024]).unwrap();
        cache
            .register_incomplete_piece(piece_key, 4 * 1024 * 1024)
            .unwrap();
        let cm = Arc::new(Mutex::new(cache));

        let stats = generate_torrent_stats(source_id, "hash-cache-max", &db, &None, {
            let cm = cm.clone();
            move || Some(cm.clone())
        });
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains(&format!("Cache: 1 pieces  4.00 MB  max={max_bytes}\n")),
            "leaf Cache line must carry max=<cache_size> in bytes, got:\n{text}"
        );
    }

    #[test]
    fn test_torrent_stats_not_found() {
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("Torrent not found"));
    }

    #[test]
    fn test_directory_stats_empty_path() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("No torrents found"));
    }

    #[test]
    fn test_generate_stats_is_wrapper() {
        let global = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let wrapper = generate_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            &Arc::new(Mutex::new(HashMap::new())),
            "0.0.0.0:6881",
            None,
        );
        let gtext = String::from_utf8_lossy(&global);
        let wtext = String::from_utf8_lossy(&wrapper);
        assert_eq!(
            gtext, wtext,
            "generate_stats should produce same output as generate_global_stats"
        );
    }

    #[test]
    fn test_name_truncation_utf8_safe() {
        let long_cjk = "这是一个很长的种子文件名测试用例".to_string(); // 16 chars, 48 bytes
        assert!(long_cjk.len() > 40);
        let truncated = long_cjk.chars().take(37).collect::<String>() + "...";
        assert_eq!(truncated.chars().count(), 16 + 3);
    }

    #[test]
    fn test_global_stats_ascii_borders() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("====="), "borders must be ASCII '='");
        assert!(!text.contains('\u{2550}'), "no Unicode double-line borders");
    }

    #[test]
    fn test_global_stats_english_headers() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("-- Overview --"));
        assert!(text.contains("-- Global Rates --"));
        assert!(text.contains("-- Connections --"));
        assert!(text.contains("-- Torrents --"));
        assert!(text.contains("-- Cache --"));
        assert!(text.contains("-- Performance --"));
        assert!(text.contains("-- Observability --"));
    }

    #[test]
    fn test_observability_renders_layered_cache_hit_rate() {
        let mut m = MetricsSnapshot::default();
        m.l1_hits = 40;
        m.l1_misses = 60;
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            Some(m),
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains("Cache L1 (memory):"),
            "missing L1 line: {text}"
        );
        assert!(
            text.contains("hit rate 40.0%"),
            "missing L1 hit rate: {text}"
        );
        assert!(text.contains("Cache L2 (disk):"), "missing L2 line: {text}");
        assert!(
            text.contains("Cache L3 (metadata):"),
            "missing L3 line: {text}"
        );
        assert!(
            text.contains("Deferred reads:"),
            "missing Deferred line: {text}"
        );
        assert!(text.contains("Poll hit rate:"), "missing poll line: {text}");
        assert!(
            text.contains("Download queue:"),
            "missing queue line: {text}"
        );
        assert!(text.contains("Workers:"), "missing workers line: {text}");
        assert!(
            text.contains("Lock wait:"),
            "missing lock wait line: {text}"
        );
    }

    #[test]
    fn test_global_stats_total_unique_format() {
        let stats = generate_global_stats(
            Duration::from_secs(0),
            &None,
            None,
            || None,
            "0.0.0.0:6881",
            None,
        );
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("Total: "));
        assert!(text.contains("Unique: "));
    }

    #[test]
    fn test_torrent_stats_english_status() {
        // Without a real DB, this should just not panic with "Torrent not found"
        let stats = generate_torrent_stats(1, "abc", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("等待"));
        assert!(!text.contains("下载"));
        assert!(!text.contains("做种"));
        assert!(!text.contains("错误"));
    }

    #[test]
    fn test_directory_stats_english_headers() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("──"));
        assert!(text.contains("No torrents found"));
    }

    #[test]
    fn test_torrent_stats_has_title_line() {
        // Without a real DB, this should just not panic with "Torrent not found"
        // but we can still verify the format when it IS found by checking code structure.
        // Test with torrent not found case: verify the function doesn't crash.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(
            text.contains("Torrent not found"),
            "stats for missing torrent should include 'Torrent not found', got: {}",
            text
        );
    }

    #[test]
    fn test_torrent_stats_section_headers_present_in_code() {
        // Verify the section header strings exist in the compiled binary
        // by checking the const patterns that would appear in any torrent stats output.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // torrent not found case; version banner must NOT appear (root-only)
        assert!(!text.contains("torrentfs v"));
    }

    #[test]
    fn test_torrent_stats_no_version_banner() {
        // Leaf .stats must not include the version banner.
        let stats = generate_torrent_stats(999, "deadbeef", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("torrentfs v0"));
    }

    #[test]
    fn test_directory_stats_header_format() {
        let stats = generate_directory_stats("/test/path", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(text.contains("===== directory: /test/path ====="));
    }

    #[test]
    fn test_directory_stats_no_version_banner() {
        // Intermediate directory .stats must not include the version banner.
        let stats = generate_directory_stats("/test/path", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        assert!(!text.contains("torrentfs v0"));
    }

    #[test]
    fn test_directory_stats_has_path_title() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // Path title uses spec format: ===== directory: {path} =====
        assert!(text.contains("===== directory: /nonexistent ====="));
    }

    #[test]
    fn test_directory_stats_section_headers_empty() {
        let stats = generate_directory_stats("/nonexistent", &None, &None, || None);
        let text = String::from_utf8_lossy(&stats);
        // Empty path shows "No torrents found" not section headers
        assert!(text.contains("No torrents found"));
    }

    #[test]
    fn test_directory_stats_summary_lists_each_torrent() {
        let mut db = Database::open_in_memory().unwrap();
        db.insert_torrent("os/linux", "ubuntu", "ubuntu.torrent", 1024, "hash-u", 1)
            .unwrap();
        db.insert_torrent("os/linux", "debian", "debian.torrent", 2048, "hash-d", 1)
            .unwrap();
        let db = Some(Arc::new(Mutex::new(db)));

        let stats = generate_directory_stats("os/linux", &db, &None, || None);
        let text = String::from_utf8_lossy(&stats);

        // Summary header carries the aggregate counts ...
        assert!(text.contains("-- Summary --\n"));
        assert!(text.contains("  Torrents: 2  Total Size: 3.00 KB  Downloaded: 0 B\n"));
        // ... and one line per torrent.
        assert!(
            text.contains("  ubuntu  Pending  0.0%  0 B / 1.00 KB\n"),
            "missing ubuntu summary line: {text}"
        );
        assert!(
            text.contains("  debian  Pending  0.0%  0 B / 2.00 KB\n"),
            "missing debian summary line: {text}"
        );

        // Rates holds only rates — the aggregate counts moved to Summary.
        let rates = text
            .split("-- Rates --")
            .nth(1)
            .and_then(|rest| rest.split("\n--").next())
            .unwrap();
        assert!(
            !rates.contains("Torrents:"),
            "Rates section must not carry aggregate counts: {rates}"
        );

        // The per-torrent detail list is still present.
        assert!(text.contains("\n-- Torrents --\n"));
    }

    #[test]
    fn test_summary_torrent_stats_falls_back_to_status_without_pieces() {
        // No piece snapshot: fall back to the same libtorrent status the
        // Torrents detail list uses, so one torrent can't show two different
        // progress values in the same file.
        let (downloaded, progress) = summary_torrent_stats(None, Some((0.42, 500)), 1024);
        assert_eq!(downloaded, 500);
        assert!((progress - 0.42).abs() < f64::EPSILON);
    }

    #[test]
    fn test_summary_torrent_stats_sizes_short_final_piece_exactly() {
        // 1500-byte torrent in two 1024-byte pieces, both cached: the final
        // piece is 476 bytes, so the total is exactly 1500 — not 2×1024.
        let pieces = [
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
        ];
        let (downloaded, progress) =
            summary_torrent_stats(Some((1024, pieces.as_slice())), None, 1500);
        assert_eq!(downloaded, 1500);
        assert!((progress - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_summary_torrent_stats_counts_last_piece_remainder() {
        // Only the final piece is cached: 1500 - 1024 = 476 bytes, not 1024.
        let pieces = [
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
        ];
        let (downloaded, _) = summary_torrent_stats(Some((1024, pieces.as_slice())), None, 1500);
        assert_eq!(downloaded, 476);
    }

    #[test]
    fn test_summary_torrent_stats_prefers_piece_snapshot() {
        // The authoritative piece snapshot wins over a stale status.
        let pieces = [PieceStatus {
            priority: 0,
            is_cached: true,
            hit_count: 0,
        }];
        let (downloaded, progress) =
            summary_torrent_stats(Some((1024, pieces.as_slice())), Some((0.9, 9000)), 1024);
        assert_eq!(downloaded, 1024);
        assert!((progress - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn test_piece_downloaded_empty() {
        assert_eq!(piece_downloaded(&[], 16384), 0);
    }

    #[test]
    fn test_piece_downloaded_zero_piece_length() {
        let pieces = vec![PieceStatus {
            priority: 0,
            is_cached: true,
            hit_count: 0,
        }];
        assert_eq!(piece_downloaded(&pieces, 0), 0);
    }

    #[test]
    fn test_piece_downloaded_none_cached() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert_eq!(piece_downloaded(&pieces, 16384), 0);
    }

    #[test]
    fn test_piece_downloaded_all_cached() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 2,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
        ];
        // 2 pieces × 16384 bytes = 32768
        assert_eq!(piece_downloaded(&pieces, 16384), 32768);
    }

    #[test]
    fn test_piece_downloaded_partial() {
        // 1 of 4 cached, piece_length 262144 → 262144 bytes downloaded
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 1,
            },
            PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert_eq!(piece_downloaded(&pieces, 262144), 262144);
    }

    #[test]
    fn test_is_download_complete_empty_is_false() {
        // an absent/empty piece snapshot must not count as a
        // completed download — that would suppress the health alert for a
        // torrent whose pieces were never queried.
        assert!(!is_download_complete(&[]));
    }

    #[test]
    fn test_is_download_complete_all_cached() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 2,
            },
        ];
        assert!(is_download_complete(&pieces));
    }

    #[test]
    fn test_is_download_complete_partial() {
        let pieces = vec![
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 1,
            },
            PieceStatus {
                priority: 3,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert!(!is_download_complete(&pieces));
    }

    #[test]
    fn test_health_alert_zero_peers_zero_seeds() {
        // the alert fires on a sustained empty swarm but must not claim the
        // tracker is unreachable — connected-peer count is not a
        // tracker-reachability signal.
        let line = health_alert(
            0,
            0,
            0,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            0,
            false,
            false,
            false,
        )
        .expect("alert should fire at 0/0 after the grace window");
        assert!(
            !line.contains("tracker may be unreachable"),
            "must not claim tracker unreachable: {line}"
        );
        assert!(
            line.contains("no connected peers"),
            "should describe the observed state: {line}"
        );
        assert!(
            line.contains("tracker may be reachable"),
            "should acknowledge tracker may be reachable: {line}"
        );
    }

    #[test]
    fn test_health_alert_transient_empty_swarm_suppresses() {
        // a swarm empty for less than the grace window is still connecting —
        // peer counts flap around zero while the tracker announce and
        // connections settle, so no alert may fire on such a sample.
        assert!(
            health_alert(
                0,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS - 1,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                false,
                false,
            )
            .is_none(),
            "no alert for an empty swarm still inside the grace window"
        );
        assert!(
            health_alert(
                0,
                0,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                false,
                false
            )
            .is_none(),
            "no alert for an unobserved/just-empty swarm"
        );
        // A configured no-seeder window of 60s moves the grace with it: no
        // alert 15s in, where the shipped default would already fire.
        assert!(
            health_alert(0, 0, 0, 15, 60, 0, false, false, false).is_none(),
            "no alert while the configured grace has not elapsed"
        );
        assert!(
            health_alert(0, 0, 0, 60, 60, 0, false, false, false).is_some(),
            "the alert must still fire once the configured grace elapses"
        );
    }

    #[test]
    fn test_health_alert_with_peers() {
        assert!(
            health_alert(
                3,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                false,
                false
            )
            .is_none(),
            "no alert when peers > 0"
        );
    }

    #[test]
    fn test_health_alert_with_seeds() {
        assert!(
            health_alert(
                0,
                2,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                false,
                false
            )
            .is_none(),
            "no alert when seeds > 0 and no reader is waiting"
        );
    }

    #[test]
    fn test_health_alert_both_present() {
        assert!(
            health_alert(
                5,
                1,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                false,
                false
            )
            .is_none(),
            "no alert when peers and seeds > 0 and no reader is waiting"
        );
    }

    #[test]
    fn test_health_alert_download_complete_suppresses() {
        // a fully downloaded torrent legitimately has zero peers;
        // the health alert must not fire when download is complete.
        assert!(
            health_alert(
                0,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                true,
                false,
                false
            )
            .is_none(),
            "no alert when download is complete even at 0 peers / 0 seeds"
        );
        assert!(
            health_alert(
                0,
                0,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                true,
                false,
                true
            )
            .is_none(),
            "no alert when download is complete even with a parked read"
        );
        assert!(
            health_alert(
                1,
                1,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                HEALTH_ALERT_SLOW_SWARM_GRACE_SECS,
                true,
                true,
                true
            )
            .is_none(),
            "no alert when a finished torrent's read is served from cache"
        );
    }

    #[test]
    fn test_health_alert_active_download_suppresses() {
        // during the transient zero-peer window before the first
        // tracker announce returns, a download that is actively fetching
        // bytes must not raise a health alert.
        assert!(
            health_alert(
                0,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                true,
                false
            )
            .is_none(),
            "no alert while a download is actively fetching bytes"
        );
    }

    #[test]
    fn test_health_alert_waiting_for_peers_with_parked_read() {
        // A read parked on an empty swarm is a real wait, so the alert fires
        // immediately — the grace window only guards the no-reader case, where
        // an empty sample is just connections flapping.
        let line = health_alert(
            0,
            0,
            0,
            0,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            0,
            false,
            true,
            true,
        )
        .expect("a parked read on an empty swarm must alert without the grace window");
        assert!(
            line.contains("⚠ Waiting for peers (0s)"),
            "should name the wait and its age: {line}"
        );
        assert!(
            line.contains("no connected peers"),
            "should describe the observed swarm: {line}"
        );
    }

    #[test]
    fn test_health_alert_waiting_for_peers_reports_empty_swarm_age() {
        let line = health_alert(
            0,
            0,
            0,
            7,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            0,
            false,
            true,
            true,
        )
        .expect("alert while a read waits on an empty swarm");
        assert!(
            line.contains("⚠ Waiting for peers (7s)"),
            "should report how long the swarm has been empty: {line}"
        );
    }

    #[test]
    fn test_health_alert_slow_swarm_with_seeder() {
        // Seeder connected, read parked, no bytes arriving: the state a bare
        // peer count cannot express.
        let line = health_alert(
            1,
            1,
            0,
            0,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            HEALTH_ALERT_SLOW_SWARM_GRACE_SECS,
            false,
            true,
            true,
        )
        .expect("a connected seeder delivering nothing must alert");
        assert_eq!(
            line,
            format!(
                "  ⚠ Slow swarm: seeder connected but no progress for {}s\n",
                HEALTH_ALERT_SLOW_SWARM_GRACE_SECS
            )
        );
    }

    #[test]
    fn test_health_alert_slow_swarm_requires_sustained_window() {
        // The sample right after a seeder connects lands before its first
        // block, so a single zero rate is not a stall.
        assert!(
            health_alert(
                1,
                1,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                HEALTH_ALERT_SLOW_SWARM_GRACE_SECS - 1,
                false,
                true,
                true
            )
            .is_none(),
            "no alert for a zero rate younger than the grace window"
        );
        let line = health_alert(
            1,
            1,
            0,
            0,
            DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
            7,
            false,
            true,
            true,
        )
        .expect("alert once the stall has held past the grace window");
        assert!(
            line.contains("no progress for 7s"),
            "should report how long the seeder has stalled: {line}"
        );
    }

    #[test]
    fn test_health_alert_slow_swarm_requires_zero_rate() {
        // Bytes flowing with a seeder connected is steady state, not a stall.
        assert!(
            health_alert(
                1,
                1,
                65_536,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                30,
                false,
                true,
                true
            )
            .is_none(),
            "no alert while the seeder is delivering bytes"
        );
        // A seeder with no parked read is seeding, not stalling, however long
        // its rate has been zero.
        assert!(
            health_alert(
                1,
                1,
                0,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                30,
                false,
                false,
                false
            )
            .is_none(),
            "no alert when no read is parked"
        );
    }

    #[test]
    fn test_health_alert_steady_download_silent() {
        // Data arriving with a read parked and peers present: the steady state
        // must stay free of alert lines.
        assert!(
            health_alert(
                2,
                1,
                2_097_152,
                0,
                DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS,
                0,
                false,
                true,
                true
            )
            .is_none(),
            "steady download must not alert"
        );
    }

    #[test]
    fn test_has_cached_piece() {
        let pieces = [
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 0,
                is_cached: true,
                hit_count: 1,
            },
        ];
        assert!(has_cached_piece(&pieces));
        assert!(!has_cached_piece(&pieces[..1]));
        assert!(!has_cached_piece(&[]));
    }

    #[test]
    fn test_has_active_reader() {
        let pieces = [
            PieceStatus {
                priority: 0,
                is_cached: false,
                hit_count: 0,
            },
            PieceStatus {
                priority: 7,
                is_cached: false,
                hit_count: 0,
            },
        ];
        assert!(has_active_reader(&pieces));
        assert!(!has_active_reader(&pieces[..1]));
        assert!(!has_active_reader(&[]));
    }

    /// A cached piece snapshot: `(priority, is_cached)` pairs.
    fn snapshot(entries: &[(i32, bool)]) -> Vec<PieceStatus> {
        entries
            .iter()
            .map(|&(priority, is_cached)| PieceStatus {
                priority,
                is_cached,
                hit_count: 0,
            })
            .collect()
    }

    #[test]
    fn test_display_status_tracks_progress_column() {
        // The reported defect: a fully cached torrent rendered `Status:
        // Pending` beside `Progress: 100.0%`. Deriving status from the same
        // piece snapshot as progress makes the pair consistent: 100% renders
        // `Seeding`, never `Pending`.
        let complete = snapshot(&[(0, true), (0, true)]);
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Pending, Some(&complete))),
            "Seeding"
        );
        assert_eq!(piece_progress(&complete), 1.0);

        let partial = snapshot(&[(0, true), (0, false)]);
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Pending, Some(&partial))),
            "Downloading"
        );
        assert!(piece_progress(&partial) > 0.0 && piece_progress(&partial) < 1.0);

        let untouched = snapshot(&[(0, false), (0, false)]);
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Pending, Some(&untouched))),
            "Pending"
        );
        assert_eq!(piece_progress(&untouched), 0.0);
    }

    #[test]
    fn test_display_status_active_reader_is_downloading() {
        // A read in flight with nothing cached yet is being fetched, not
        // pending — even though the progress column still reads 0.0%.
        let wanted = snapshot(&[(7, false)]);
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Pending, Some(&wanted))),
            "Downloading"
        );
    }

    #[test]
    fn test_display_status_without_snapshot_keeps_persisted() {
        // No handle yet: the progress column falls back to 0.0%, so the
        // persisted status is returned rather than a derived one.
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Error, None)),
            "Error"
        );
        assert_eq!(
            status_to_english(&display_status(&TorrentStatus::Pending, None)),
            "Pending"
        );
    }

    #[test]
    fn test_trackers_block_lists_tier_and_url_in_announce_order() {
        // A multi-tier announce-list renders lowest tier first; announce order
        // is preserved inside a tier (BEP-12 contact order).
        let trackers = vec![
            TrackerEntry {
                tier: 2,
                url: "udp://third.example:1337/announce".into(),
            },
            TrackerEntry {
                tier: 0,
                url: "http://first.example/announce".into(),
            },
            TrackerEntry {
                tier: 0,
                url: "http://second.example/announce".into(),
            },
        ];

        let mut out = String::new();
        write_trackers(&mut out, Some(&trackers), None);

        assert_eq!(
            out.lines().collect::<Vec<_>>(),
            vec![
                "",
                "-- Trackers --",
                "  tier 0  http://first.example/announce",
                "  tier 0  http://second.example/announce",
                "  tier 2  udp://third.example:1337/announce",
            ]
        );
    }

    #[test]
    fn test_trackers_block_zero_trackers_points_at_dht_and_lsd() {
        let mut out = String::new();
        write_trackers(&mut out, Some(&[]), Some(412));

        assert!(
            out.contains("  No trackers — relying on DHT/LSD\n"),
            "empty tracker list must name the DHT/LSD fallback, got:\n{out}"
        );
        assert!(
            out.contains("  DHT Nodes: 412 (global)\n"),
            "DHT count must be marked global (no per-torrent DHT view), got:\n{out}"
        );
    }

    #[test]
    fn test_trackers_block_unread_list_claims_no_cause() {
        // `None` covers three distinct states — the snapshot was locked, it
        // holds no handle for the info_hash, or the handle read failed — so the
        // block must pin none of them. It must not claim zero trackers either:
        // only a definitive empty list may say there are no trackers.
        let mut out = String::new();
        write_trackers(&mut out, None, None);

        assert!(
            out.contains("  (unavailable — tracker list not read)\n"),
            "an unread tracker list must say so, got:\n{out}"
        );
        assert!(!out.contains("No trackers"), "got:\n{out}");
        assert!(
            !out.contains("handle"),
            "the block must not name a cause it cannot establish, got:\n{out}"
        );
    }
}

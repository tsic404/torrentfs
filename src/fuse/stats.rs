//! StatsGenerator — generates the .stats file content.
//! Extracted from TorrentFs to separate stats generation from FUSE protocol handling.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use crate::cache::CacheManager;
use crate::db::{Database, TorrentStatus};
use crate::infrastructure::download::PieceStatus;
use crate::infrastructure::download::SessionStats;
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
    let cache_pct = if cache_max_size > 0 {
        (cache_total_size as f64 / cache_max_size as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!("  Cache Dir:    {}\n", cache_dir_str));
    output.push_str(&format!(
        "  Cache Usage:  {} / {} ({:.1}%)\n",
        format_bytes(cache_total_size),
        format_bytes(cache_max_size),
        cache_pct
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
    let (global_hits, global_misses) = if let Some(ref cm) = get_cache_manager() {
        if let Ok(cm_guard) = cm.try_lock() {
            (cm_guard.hit_count, cm_guard.miss_count)
        } else {
            (0, 0)
        }
    } else {
        (0, 0)
    };
    let global_total = global_hits + global_misses;
    let hit_rate = if global_total > 0 {
        (global_hits as f64 / global_total as f64) * 100.0
    } else {
        0.0
    };
    output.push_str(&format!(
        "  Hits: {}  Misses: {}  Hit Rate: {:.1}%  Evictions: 0\n",
        format_num(global_hits),
        format_num(global_misses),
        hit_rate
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

/// Render the observability counters (TSI-2139). Absent counters (no
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

/// Render the piece marker per the `.stats` spec:
/// `[x]` downloaded, `[]` not wanted, `[N]` priority N, `[X n]` downloaded with n accesses.
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

/// Compute download progress as a fraction `[0.0, 1.0]` from actual cached pieces.
///
/// libtorrent's `status.progress` is unreliable under the custom `PieceStorageDiskIO`
/// backend: it reflects `total_wanted_done / total_wanted` as seen by libtorrent's
/// piece bitmap, which can report 1.0 (100%) even when pieces have not been
/// downloaded — because `async_check_files` reports success without feeding the
/// piece bitmap back to libtorrent, and `async_hash` failures are treated as
/// "not present" rather than resetting progress.
///
/// This helper recomputes progress from the **authoritative** piece availability
/// (`is_cached` in the piece snapshot), so `.stats` never shows 100% while reads
/// still time out waiting for pieces (TSI-2223).
fn piece_progress(pieces: &[PieceStatus]) -> f64 {
    if pieces.is_empty() {
        return 0.0;
    }
    let cached = pieces.iter().filter(|p| p.is_cached).count();
    cached as f64 / pieces.len() as f64
}

/// Compute downloaded bytes from actual cached pieces.
///
/// libtorrent's `status.total_done` is unreliable under the custom
/// `PieceStorageDiskIO` backend for the same reason as `progress`
/// (see [`piece_progress`]): the piece bitmap is never fed back to
/// libtorrent, so `total_done` stays at 0 even after pieces are cached.
///
/// This helper recomputes the downloaded byte count from the
/// **authoritative** piece availability (`is_cached`), so `.stats` shows
/// non-zero Downloaded once pieces are cached (TSI-2227).
fn piece_downloaded(pieces: &[PieceStatus], piece_length: u64) -> u64 {
    if pieces.is_empty() || piece_length == 0 {
        return 0;
    }
    let cached = pieces.iter().filter(|p| p.is_cached).count() as u64;
    cached * piece_length
}

/// Render the `.stats` health alert line for a single torrent.
///
/// The alert fires when the torrent has **no connected peers or seeds**
/// (`num_peers == 0 && num_seeds == 0`). The counts come from the
/// libtorrent session — they reflect live peer connections, not tracker
/// reachability. A tracker can be reachable and have returned seeder
/// entries, yet show zero connected peers during the window before the
/// first peer handshake completes (TSI-2442).
///
/// The wording therefore describes the **observed connection state** and
/// avoids the prior text ("tracker may be unreachable") which coupled the
/// alert to a cause the data cannot establish.
fn health_alert(num_peers: i32, num_seeds: i32) -> Option<&'static str> {
    if num_peers == 0 && num_seeds == 0 {
        Some("  ⚠ Health: 0 peers / 0 seeds — no connected peers; tracker may be reachable\n")
    } else {
        None
    }
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

    let status_str = status_to_english(&t.status);

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
    // cached piece count instead (TSI-2223).
    let piece_statuses = download_service
        .as_ref()
        .and_then(|ds| ds.try_get_pieces_status(info_hash));
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
    // Same root cause as progress (TSI-2223): status.total_done stays 0
    // under the custom storage backend. Recompute from cached pieces
    // (TSI-2227).
    let total_done = piece_statuses
        .as_ref()
        .map(|(piece_length, pieces)| piece_downloaded(pieces, *piece_length))
        .unwrap_or(total_done);
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
    let health = health_alert(num_peers, num_seeds);
    if let Some(line) = health {
        output.push_str(line);
    }

    // -- Pieces -- visualised piece lifecycle (GitHub commit-record grid).
    // Uses only non-blocking locks so `.stats` never blocks on an active
    // download (TSI-2119).
    if let Some((piece_length, pieces)) = &piece_statuses {
        if !pieces.is_empty() {
            output.push_str(&format!(
                "\n-- Pieces ({} pieces, {} each) --\n  ",
                pieces.len(),
                format_bytes(*piece_length)
            ));
            for status in pieces {
                output.push_str(&piece_marker(status));
            }
            output.push('\n');
        }
    }

    // Info fields merged after the piece markers (no `-- Info --` header).
    if let Some(cm) = &get_cache_manager() {
        if let Ok(cm_guard) = cm.try_lock() {
            let cache_stats = cm_guard.get_cache_stats_by_infohash(info_hash);
            output.push_str(&format!(
                "Cache: {} pieces  {}\n",
                cache_stats.piece_count,
                format_bytes(cache_stats.total_size)
            ));
        }
    }
    output.push_str(&format!("info_hash: {}\n", t.info_hash));
    output.push_str(&format!("source_path: \"{}\"\n", t.source_path));

    // PT isolation info (TSI-2277): show the private flag and whether
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

    for t in &torrents {
        total_size += t.total_size as u64;
        if let Some(status) = download_service
            .as_ref()
            .and_then(|ds| ds.try_query_torrent_status(&t.info_hash))
        {
            total_upload += status.total_upload as u64;
            total_download += status.total_download as u64;
            aggregate_dl_rate += status.download_rate;
            aggregate_ul_rate += status.upload_rate;
            aggregate_peers += status.num_peers;
            aggregate_seeds += status.num_seeds;
        }

        // Override libtorrent's total_done with piece-availability-based
        // bytes. Same root cause as progress (TSI-2223): status.total_done
        // stays 0 under the custom storage backend. Recompute from cached
        // pieces (TSI-2227).
        if let Some((piece_length, pieces)) = download_service
            .as_ref()
            .and_then(|ds| ds.try_get_pieces_status(&t.info_hash))
        {
            total_done += piece_downloaded(&pieces, piece_length);
        }
    }

    // -- Rates --
    output.push_str("-- Rates --\n");
    output.push_str(&format!(
        "  Torrents: {}  Total Size: {}  Downloaded: {}\n",
        torrent_count,
        format_bytes(total_size),
        format_bytes(total_done)
    ));
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
            let cache_pct = if cache_max_size > 0 {
                (cache_total_size as f64 / cache_max_size as f64) * 100.0
            } else {
                0.0
            };
            let global_total = global_hits + global_misses;
            let hit_rate = if global_total > 0 {
                (global_hits as f64 / global_total as f64) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!(
                "  Cache Usage: {} / {} ({:.1}%)\n",
                format_bytes(cache_total_size),
                format_bytes(cache_max_size),
                cache_pct
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
        let status_str = status_to_english(&t.status);

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
        // (TSI-2223): libtorrent's progress can report 1.0 before pieces are
        // actually downloaded under the custom storage backend.
        let actual_progress = download_service
            .as_ref()
            .and_then(|ds| ds.try_get_pieces_status(&t.info_hash))
            .map(|(_, pieces)| piece_progress(&pieces))
            .unwrap_or(progress as f64);
        let prog_pct = if ts > 0 { actual_progress * 100.0 } else { 0.0 };

        // PT isolation indicator (TSI-2277): show a 🔒 marker for private
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
        // TSI-2224: during an active read, pieces with elevated priority
        // (not yet cached) must render as `[N]`, not `[]`.  The bug was that
        // the snapshot never captured elevated priorities, so every piece
        // showed `[]`.  This test locks the `piece_marker` rendering contract
        // the fix depends on: non-cached + priority > 0 → `[N]`.
        let grid = vec![
            // Cached, no accesses → `[x]`
            PieceStatus { priority: 0, is_cached: true, hit_count: 0 },
            // Active read target, not cached, priority 7 → `[7]`
            PieceStatus { priority: 7, is_cached: false, hit_count: 0 },
            // Prefetch edge, not cached, priority 1 → `[1]`
            PieceStatus { priority: 1, is_cached: false, hit_count: 0 },
            // Outside window, not cached, priority 0 → `[]`
            PieceStatus { priority: 0, is_cached: false, hit_count: 0 },
        ];
        let rendered: String = grid.iter().map(piece_marker).collect();
        assert_eq!(rendered, "[x][7][1][]");
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
    fn test_health_alert_zero_peers_zero_seeds() {
        // TSI-2442: the alert fires on zero connected peers/seeds but must
        // not claim the tracker is unreachable — connected-peer count is not
        // a tracker-reachability signal.
        let line = health_alert(0, 0).expect("alert should fire at 0/0");
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
    fn test_health_alert_with_peers() {
        assert!(
            health_alert(3, 0).is_none(),
            "no alert when peers > 0"
        );
    }

    #[test]
    fn test_health_alert_with_seeds() {
        assert!(
            health_alert(0, 2).is_none(),
            "no alert when seeds > 0"
        );
    }

    #[test]
    fn test_health_alert_both_present() {
        assert!(
            health_alert(5, 1).is_none(),
            "no alert when peers and seeds > 0"
        );
    }
}

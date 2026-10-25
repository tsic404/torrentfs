//! Design acceptance case TC-recovery-04: debug logging stays lean during
//! reads — a read summary exists, no per-piece flood, and the abnormal path
//! warns (`docs/03-acceptance.md` §I AC-83).
//!
//! The assertions are shape-based, not wording-based: they classify each line
//! by its tracing level and by whether it names a single piece index or
//! reports a byte count, so a reworded message keeps passing and a per-piece
//! message stream does not.  The fixture spans 32 pieces, so "one line per
//! piece" is distinguishable from "a handful of anomaly diagnostics".

use crate::common::{
    base, build_multipiece_torrent_pieces, daemon_config, fuse_mount_blocker,
    ingest_torrent_through_mount, read_when_ready, wait_for_path, wait_for_projection, wave_slot,
    Daemon, MountGate, TestHarness, DAEMON_MOUNT_TIMEOUT,
};
use std::fs;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

pub const TC_ID: &str = "TC-recovery-04";

/// Piece count of the served fixture: enough that a per-piece line stream
/// would be unmistakable.
const FIXTURE_PIECES: usize = 32;
/// The served fixture's file name (also its torrent name).
const FIXTURE_NAME: &str = "recovery-04-log-volume.bin";
/// A read that spans `FIXTURE_PIECES` pieces may still log an anomaly per
/// affected piece; what the criterion forbids is a line per piece.
const MAX_PER_PIECE_LINES: usize = 3;

/// Reading the served fixture across the local swarm.
const READ_BUDGET: Duration = Duration::from_secs(120);
/// The sourceless read fails after the configured short windows.
const ABSENT_READ_BUDGET: Duration = Duration::from_secs(30);
/// Time the daemon gets to meet the local seeder before the cold read.
const SEED_DISCOVERY_TIMEOUT: Duration = Duration::from_secs(120);
/// Parsing and persisting one fixture torrent.
const PROJECTION_TIMEOUT: Duration = Duration::from_secs(60);
/// A graceful stop after the reads.
const EXIT_TIMEOUT: Duration = Duration::from_secs(60);

/// Remove ANSI colour sequences so level and message classification does not
/// depend on whether the writer emitted a TTY stream.
fn strip_ansi(line: &str) -> String {
    let mut out = String::with_capacity(line.len());
    let mut chars = line.chars();
    while let Some(ch) = chars.next() {
        if ch != '\u{1b}' {
            out.push(ch);
            continue;
        }
        // CSI: ESC '[' parameters final-byte.  A non-CSI escape ends here.
        let mut introduced = false;
        for next in chars.by_ref() {
            if !introduced {
                introduced = true;
                if next != '[' {
                    break;
                }
                continue;
            }
            if next.is_ascii_alphabetic() {
                break;
            }
        }
    }
    out
}

/// The tracing level token of a formatted log line.
fn level_of(line: &str) -> Option<&'static str> {
    for token in line.split_whitespace().take(4) {
        match token {
            "TRACE" => return Some("TRACE"),
            "DEBUG" => return Some("DEBUG"),
            "INFO" => return Some("INFO"),
            "WARN" => return Some("WARN"),
            "ERROR" => return Some("ERROR"),
            _ => {}
        }
    }
    None
}

/// A read summary reports how many bytes one read served.
fn is_read_summary(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    matches!(level_of(line), Some("DEBUG") | Some("INFO"))
        && lower.contains("bytes")
        && lower.contains("read")
        && lower.chars().any(|ch| ch.is_ascii_digit())
}

/// A per-piece line names one piece index (`piece 12`, `piece=12`,
/// `pieces 12-13`); such a line repeating per piece is the flood the
/// criterion excludes.
fn mentions_piece_index(line: &str) -> bool {
    let lower = line.to_ascii_lowercase();
    let bytes = lower.as_bytes();
    let mut cursor = 0;
    while let Some(offset) = lower[cursor..].find("piece") {
        let mut index = cursor + offset + "piece".len();
        while index < bytes.len() && !bytes[index].is_ascii_alphanumeric() {
            index += 1;
        }
        if index < bytes.len() && bytes[index].is_ascii_digit() {
            return true;
        }
        cursor = cursor + offset + "piece".len();
    }
    false
}

/// The `Seeds:` count of the `Peers: N  Seeds: M` line of a `.stats` file.
fn seeds_in(stats: &str) -> Option<u32> {
    stats.lines().find_map(|line| {
        let rest = line.trim().strip_prefix("Peers:")?;
        let (_, seeds) = rest.split_once("Seeds:")?;
        seeds.trim().parse().ok()
    })
}

/// Wait until the daemon's own `.stats` reports a seed for the fixture.
///
/// The fixture is served by an in-process seeder announcing to the same
/// tracker as the daemon.  A cold read issued before the daemon has met that
/// seeder spends the whole no-seeder window and answers `ENODATA`, which says
/// nothing about the logging this case judges: the windows are short so the
/// abnormal read below fails fast.  Waiting for the seed keeps "there is a
/// source" a precondition instead of part of the measurement.
fn wait_for_seed(projection: &Path, timeout: Duration) -> bool {
    let stats = projection.join(".stats");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(text) = fs::read_to_string(&stats) {
            if seeds_in(&text).is_some_and(|seeds| seeds >= 1) {
                return true;
            }
        }
        thread::sleep(Duration::from_millis(200));
    }
    false
}

/// The abnormal read path must leave a warning behind.
fn is_read_warning(line: &str) -> bool {
    matches!(level_of(line), Some("WARN") | Some("ERROR"))
        && line.to_ascii_lowercase().contains("read")
}

#[test]
fn tc_recovery_04_debug_logging_stays_lean_during_reads() {
    if let Some(blocker) = fuse_mount_blocker() {
        eprintln!("SKIP TC-recovery-04: this environment cannot mount FUSE ({blocker})");
        return;
    }
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_pieces(announce_url, FIXTURE_NAME, FIXTURE_PIECES)
    });
    assert_eq!(
        harness.info.num_pieces() as usize,
        FIXTURE_PIECES,
        "fixture must span {FIXTURE_PIECES} pieces for the flood bound to mean anything"
    );

    // Debug level is the surface the case judges.  The read window is wide:
    // this fixture is 8 MiB and the swarm is local, so a loaded machine can
    // legitimately spend more than a few seconds fetching the first pieces,
    // and a read timing out would judge the machine instead of the logging.
    // The empty-swarm windows stay short, so the abnormal read below still
    // fails quickly.
    let mut daemon = Daemon::start(Some(&daemon_config(120, 5, 10)), "debug");
    match daemon.wait_for_mount(DAEMON_MOUNT_TIMEOUT) {
        MountGate::Mounted => {}
        MountGate::Environment(detail) => {
            eprintln!("SKIP TC-recovery-04: the environment refused the FUSE mount\n{detail}");
            return;
        }
        MountGate::Failed(detail) => panic!("daemon did not mount:\n{detail}"),
    }
    let mountpoint = daemon.mountpoint();

    // ── served fixture: one cold read, one cache-hit read ───────────────────
    ingest_torrent_through_mount(&mountpoint, "log-volume.torrent", &harness.torrent_data);
    assert!(
        wait_for_projection(&mountpoint, "log-volume.torrent", PROJECTION_TIMEOUT),
        "projection never appeared\n{}",
        daemon.output()
    );
    let data_file = mountpoint
        .join("data")
        .join("log-volume.torrent")
        .join(FIXTURE_NAME);
    assert!(
        wait_for_path(&data_file, PROJECTION_TIMEOUT),
        "projected file {} never appeared\n{}",
        data_file.display(),
        daemon.output()
    );
    assert!(
        wait_for_seed(
            &mountpoint.join("data").join("log-volume.torrent"),
            SEED_DISCOVERY_TIMEOUT
        ),
        "the daemon never met the local seeder; the cold read below would answer ENODATA for \
         the swarm rather than exercise the read path\n{}",
        daemon.output()
    );
    let cold = read_when_ready(&data_file, READ_BUDGET)
        .unwrap_or_else(|e| panic!("cold read failed: {e}\n{}", daemon.output()));
    assert_eq!(
        cold.len(),
        harness.file_content.len(),
        "cold read returned the wrong byte count\n{}",
        daemon.output()
    );
    let warm = read_when_ready(&data_file, READ_BUDGET)
        .unwrap_or_else(|e| panic!("warm read failed: {e}\n{}", daemon.output()));
    assert_eq!(
        warm.len(),
        harness.file_content.len(),
        "warm read returned the wrong byte count\n{}",
        daemon.output()
    );

    // ── abnormal path: a torrent whose swarm is empty ───────────────────────
    let (absent_torrent, _) = base::create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "recovery-04-absent-swarm.bin",
    );
    ingest_torrent_through_mount(&mountpoint, "absent-swarm.torrent", &absent_torrent);
    assert!(
        wait_for_projection(&mountpoint, "absent-swarm.torrent", PROJECTION_TIMEOUT),
        "sourceless projection never appeared\n{}",
        daemon.output()
    );
    let absent_file = mountpoint
        .join("data")
        .join("absent-swarm.torrent")
        .join("recovery-04-absent-swarm.bin");
    assert!(
        wait_for_path(&absent_file, PROJECTION_TIMEOUT),
        "sourceless file {} never appeared\n{}",
        absent_file.display(),
        daemon.output()
    );
    let absent = read_when_ready(&absent_file, ABSENT_READ_BUDGET);
    assert_eq!(
        absent.as_ref().err().and_then(|error| error.raw_os_error()),
        Some(libc::ENODATA),
        "a read from an empty swarm must settle as ENODATA (C-2.3.7), got {absent:?}\n{}",
        daemon.output()
    );

    // ── stop and inspect the log ────────────────────────────────────────────
    assert!(daemon.signal(libc::SIGTERM), "daemon already exited before SIGTERM");
    let status = daemon.wait_for_exit(EXIT_TIMEOUT).unwrap_or_else(|| {
        panic!(
            "daemon did not exit within {EXIT_TIMEOUT:?} after SIGTERM\n{}",
            daemon.output()
        )
    });
    assert_eq!(status.code(), Some(0), "daemon exited with {status}\n{}", daemon.output());

    let log = fs::read_to_string(daemon.log_path()).expect("read daemon log");
    let lines: Vec<String> = log.lines().map(strip_ansi).collect();

    let summaries = lines.iter().filter(|line| is_read_summary(line)).count();
    assert!(
        summaries >= 1,
        "no read summary line (byte count) in a debug log covering cold and warm reads:\n{log}"
    );

    let per_piece: Vec<&String> = lines
        .iter()
        .filter(|line| mentions_piece_index(line))
        .collect();
    assert!(
        per_piece.len() <= MAX_PER_PIECE_LINES,
        "{} per-piece lines for a {FIXTURE_PIECES}-piece read (max {MAX_PER_PIECE_LINES}); the \
         piece stream is the flood the criterion forbids:\n{}",
        per_piece.len(),
        per_piece
            .iter()
            .map(|line| line.as_str())
            .collect::<Vec<_>>()
            .join("\n")
    );

    let warnings = lines.iter().filter(|line| is_read_warning(line)).count();
    assert!(
        warnings >= 1,
        "the sourceless read left no WARN/ERROR line about the failed read:\n{log}"
    );
}

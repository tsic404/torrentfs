//! Design acceptance case TC-read-07: eight concurrent reads at different
//! offsets — head, piece boundary, unaligned middle, spanning, tail, last byte,
//! at EOF, past EOF — each finish on their own with exactly the bytes their
//! range implies (`docs/03-acceptance.md` §B AC-15).
//!
//! Each path is judged separately (exit, byte count, seeded bytes) so a path
//! serving the wrong range fails even when every thread joins cleanly.

use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crate::common::{build_multipiece_torrent_named, local_test_config, wave_slot, TestHarness};
use torrentfs::download::DownloadEngine;
use torrentfs::{is_transient_read_error, TorrentError, TorrentInfo};

pub const TC_ID: &str = "TC-read-07";

/// The fixture is 1 MiB (4 × 256 KiB pieces): "tail to EOF" ends exactly at the
/// file size, so the last two paths start at and past it.
const FILE_LEN: u64 = 1024 * 1024;

/// One read path: `(label, offset, requested size)`.
const PATHS: &[(&str, u64, u32)] = &[
    ("head", 0, 4 * 1024),
    ("piece boundary", 256 * 1024, 128 * 1024),
    ("unaligned middle", 500_000, 100_000),
    ("spanning pieces", 200_000, 200_000),
    ("tail to EOF", 900_000, 148_576),
    ("last byte", FILE_LEN - 1, 1),
    ("at EOF", FILE_LEN, 4 * 1024),
    ("past EOF", 2_000_000, 4 * 1024),
];

/// How long one path may retry a transient cold-start error before its result
/// counts as a failure.  The retry waits out the seeder's first upload only; it
/// never re-judges the byte count.
const RETRY_BUDGET: Duration = Duration::from_secs(60);

/// One path's outcome, judged in the parent so a failure names its path.
type PathOutcome = (&'static str, u64, u32, Result<Vec<u8>, String>);

#[test]
fn tc_read_07_eight_offset_paths_each_return_their_own_range() {
    let _slot = wave_slot();
    let _session_guard = crate::common::acquire_session_lock();

    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_named(announce_url, "tc-read-07-offsets.bin")
    });
    assert_eq!(
        harness.file_content.len() as u64,
        FILE_LEN,
        "the fixture must be the 1 MiB multi-piece torrent the paths below assume"
    );
    let content = Arc::new(harness.file_content.clone());

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    // Ephemeral downloader port: a fixed one would collide between the tests
    // running in parallel, while the tracker must still see the downloader as
    // a peer distinct from the seeder.
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());

    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );
    let info = Arc::new(
        TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("Failed to parse torrent"),
    );
    engine.ensure_handle(info.clone()).expect("ensure_handle");

    // One barrier releases all eight paths together: no path is finished
    // before the last one starts, so they genuinely overlap.
    let barrier = Arc::new(Barrier::new(PATHS.len()));
    let readers: Vec<_> = PATHS
        .iter()
        .map(|&(label, offset, size)| {
            let engine = Arc::clone(&engine);
            let info = Arc::clone(&info);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || -> PathOutcome {
                barrier.wait();
                let result =
                    read_until_settled(&engine, info, offset, size).map_err(|err| format!("{err}"));
                (label, offset, size, result)
            })
        })
        .collect();

    let mut failures: Vec<String> = Vec::new();
    for reader in readers {
        let (label, offset, size, result) = match reader.join() {
            Ok(outcome) => outcome,
            Err(payload) => {
                failures.push(format!("a read path panicked: {}", panic_message(payload)));
                continue;
            }
        };
        let expected = expected_len(offset, size);
        match result {
            Err(err) => failures.push(format!(
                "{label} (offset {offset}, size {size}) exited with an error instead of \
                 its range: {err}"
            )),
            Ok(data) => {
                if data.len() as u64 != expected {
                    failures.push(format!(
                        "{label} (offset {offset}, size {size}) returned {} bytes, expected \
                         {expected}",
                        data.len()
                    ));
                    continue;
                }
                if expected > 0 {
                    let want = &content[offset as usize..(offset + expected) as usize];
                    if data != want {
                        failures.push(format!(
                            "{label} (offset {offset}, size {size}) returned a range that \
                             does not match the seeded bytes at that offset"
                        ));
                    }
                }
            }
        }
    }

    assert!(
        failures.is_empty(),
        "{} of {} offset paths failed:\n{}",
        failures.len(),
        PATHS.len(),
        failures.join("\n")
    );

    engine.shutdown();
}

/// Bytes an offset/size request covers on a `FILE_LEN` file: a request that
/// starts inside the file is clipped at EOF, one at or past EOF is empty.
fn expected_len(offset: u64, size: u32) -> u64 {
    if offset >= FILE_LEN {
        0
    } else {
        (size as u64).min(FILE_LEN - offset)
    }
}

/// Read one path, retrying only transient cold-start errors (the first read can
/// race the seeder's first upload) until [`RETRY_BUDGET`]; the returned bytes —
/// not the retry count — are what the caller judges.
fn read_until_settled(
    engine: &DownloadEngine,
    info: Arc<TorrentInfo>,
    offset: u64,
    size: u32,
) -> Result<Vec<u8>, TorrentError> {
    let deadline = Instant::now() + RETRY_BUDGET;
    loop {
        match engine.read_file_range(Arc::clone(&info), 0, offset, size) {
            Ok(data) => return Ok(data),
            Err(err) => {
                if !is_transient_read_error(&err) || Instant::now() >= deadline {
                    return Err(err);
                }
                thread::sleep(Duration::from_millis(200));
            }
        }
    }
}

/// The panic payload as text, for reporting a path that died instead of
/// returning a range.
fn panic_message(payload: Box<dyn std::any::Any + Send>) -> String {
    if let Some(text) = payload.downcast_ref::<&str>() {
        (*text).to_string()
    } else if let Some(text) = payload.downcast_ref::<String>() {
        text.clone()
    } else {
        "non-string panic payload".to_string()
    }
}

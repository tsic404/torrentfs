//! Design acceptance case TC-read-08: 24 concurrent mixed operations — chunk
//! reads of the large file, whole reads of the small one, and tree walks over
//! both torrents — finish inside the budget with no hang, panic or segfault, and
//! the engine still serves afterwards (`docs/03-acceptance.md` §B AC-16).
//!
//! The wave runs in a child with stderr to a file: the parent can kill a hung
//! wave and assert the log carries no panic.  Tree walks enumerate each
//! torrent's file table — the `data/` listing's source — and read every entry.

use std::fs::{read_to_string, File};
use std::process::{Command, Stdio};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use crate::common::base::create_single_piece_torrent;
use crate::common::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, wave_slot, MiniTracker,
};
use crate::seed_peer::SeedPeer;
use torrentfs::download::DownloadEngine;
use torrentfs::{is_transient_read_error, TorrentInfo};

pub const TC_ID: &str = "TC-read-08";

/// Set in the child spawn so the child body performs the wave (the variable is
/// unique to this TC, so no other test's body can pick it up).
const CHILD_ENV: &str = "TORRENTFS_TC_READ_08_CHILD";
const TEST_NAME: &str =
    "test_tc_read_08::tc_read_08_twenty_plus_concurrent_mixed_operations_survive";

/// Chunk every large-file reader asks for.  1 MiB / 128 KiB = 8 readers cover
/// the whole file, which is also the chunk the FUSE layer issues.
const LARGE_CHUNK: u32 = 128 * 1024;
const LARGE_READERS: usize = 8;
const SMALL_READERS: usize = 8;
const WALKERS: usize = 8;
/// Head bytes a tree walk reads per file entry.
const WALK_HEAD: u32 = 512;

/// Wall-clock budget the parent allows the child wave.  Eight concurrent 1 MiB
/// downloads plus sixteen tiny reads finish in seconds; the budget is what
/// turns "hung" into a failed case instead of a stuck suite.
const WAVE_BUDGET: Duration = Duration::from_secs(120);

/// Budget one read may spend retrying transient cold-start errors.
const RETRY_BUDGET: Duration = Duration::from_secs(60);

/// Budget a tree walk allows the non-blocking status snapshot to be readable.
/// `try_torrent_status` returns `None` while the engine thread rebuilds the
/// snapshot, so under the wave's load this is a short retry, not a liveness
/// verdict — the post-wave command-channel read is what proves service.
const STATUS_BUDGET: Duration = Duration::from_secs(2);

#[test]
fn tc_read_08_twenty_plus_concurrent_mixed_operations_survive() {
    if std::env::var_os(CHILD_ENV).is_some() {
        exercise_wave();
        return;
    }

    let _slot = wave_slot();
    let dir = tempfile::TempDir::new().expect("Failed to create log dir");
    let log_path = dir.path().join("wave.log");
    let mut child = Command::new(std::env::current_exe().expect("test binary path"))
        .args(["--exact", TEST_NAME, "--nocapture"])
        .env(CHILD_ENV, "1")
        .stdout(Stdio::null())
        .stderr(Stdio::from(
            File::create(&log_path).expect("Failed to create the wave log"),
        ))
        .spawn()
        .expect("Failed to spawn the wave child");

    let started = Instant::now();
    let status = loop {
        match child.try_wait().expect("wait on the wave child") {
            Some(status) => break Some(status),
            None if started.elapsed() >= WAVE_BUDGET => {
                let _ = child.kill();
                let _ = child.wait();
                break None;
            }
            None => thread::sleep(Duration::from_millis(100)),
        }
    };
    let log = read_to_string(&log_path).unwrap_or_default();

    let Some(status) = status else {
        panic!(
            "the wave did not finish inside {WAVE_BUDGET:?}: a concurrent operation hung\n{log}"
        );
    };
    assert!(
        status.success(),
        "the concurrent wave did not exit normally ({status}); a read failed, panicked or \
         the process died on a signal:\n{log}"
    );
    assert!(
        !log.contains("panicked at"),
        "a concurrent operation panicked (the AC requires a panic-free wave):\n{log}"
    );
}

/// Child body: run the 24-way mixed wave, then prove the engine still serves.
fn exercise_wave() {
    let _slot = wave_slot();
    let _session_guard = acquire_session_lock();

    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();
    let (large_bytes, large_content) =
        build_multipiece_torrent_named(&announce_url, "tc-read-08-large.bin");
    let (small_bytes, small_content) =
        create_single_piece_torrent(&announce_url, "tc-read-08-small.bin");
    let large = Arc::new(TorrentInfo::from_bytes(large_bytes).expect("large torrent parses"));
    let small = Arc::new(TorrentInfo::from_bytes(small_bytes).expect("small torrent parses"));
    let large_hash = hex::encode(large.info_hash().expect("large info hash"));
    let small_hash = hex::encode(small.info_hash().expect("small info hash"));
    assert_eq!(
        large_content.len(),
        LARGE_READERS * LARGE_CHUNK as usize,
        "the large fixture must be exactly the chunks the readers cover"
    );

    // Protocol-level seeds: the wave is about the client under concurrency, so
    // the sources stay as light as the read path allows.
    let _large_peer = SeedPeer::start(
        large.info_hash().expect("large info hash"),
        large_content.clone(),
        large.piece_length() as usize,
        &announce_url,
    );
    let _small_peer = SeedPeer::start(
        small.info_hash().expect("small info hash"),
        small_content.clone(),
        small.piece_length() as usize,
        &announce_url,
    );
    let large_content = Arc::new(large_content);
    let small_content = Arc::new(small_content);

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let mut config = local_test_config();
    config.local_discovery.lsd_enabled = Some(false);
    config.connections.listen_interfaces = Some("0.0.0.0:0".to_string());
    let engine = Arc::new(
        DownloadEngine::new(cache_dir.path(), &config).expect("Failed to create DownloadEngine"),
    );
    engine
        .ensure_handle(Arc::clone(&large))
        .expect("large handle");
    engine
        .ensure_handle(Arc::clone(&small))
        .expect("small handle");
    wait_for_seed(&engine, &large_hash);
    wait_for_seed(&engine, &small_hash);

    let total = LARGE_READERS + SMALL_READERS + WALKERS;
    let barrier = Arc::new(Barrier::new(total));
    let mut tasks = Vec::with_capacity(total);

    // Large file: one 128 KiB chunk per reader, so the eight paths together
    // cover the whole 1 MiB.
    for index in 0..LARGE_READERS {
        let engine = Arc::clone(&engine);
        let info = Arc::clone(&large);
        let content = Arc::clone(&large_content);
        let barrier = Arc::clone(&barrier);
        tasks.push(thread::spawn(move || {
            let offset = index as u64 * LARGE_CHUNK as u64;
            let want = &content[offset as usize..offset as usize + LARGE_CHUNK as usize];
            barrier.wait();
            let outcome = read_matching(&engine, info, offset, LARGE_CHUNK, want);
            (format!("large chunk {index}"), outcome)
        }));
    }

    // Small file: the whole 16 KiB in one read per path.
    for index in 0..SMALL_READERS {
        let engine = Arc::clone(&engine);
        let info = Arc::clone(&small);
        let content = Arc::clone(&small_content);
        let barrier = Arc::clone(&barrier);
        tasks.push(thread::spawn(move || {
            barrier.wait();
            let outcome = read_matching(&engine, info, 0, content.len() as u32, &content);
            (format!("small read {index}"), outcome)
        }));
    }

    // Tree walk: enumerate each torrent's file table and read every entry's
    // head, plus the status snapshot a listing renders next to it.
    for index in 0..WALKERS {
        let engine = Arc::clone(&engine);
        let large = Arc::clone(&large);
        let small = Arc::clone(&small);
        let large_hash = large_hash.clone();
        let small_hash = small_hash.clone();
        let large_content = Arc::clone(&large_content);
        let small_content = Arc::clone(&small_content);
        let barrier = Arc::clone(&barrier);
        tasks.push(thread::spawn(move || {
            barrier.wait();
            let outcome = (|| -> Result<(), String> {
                let torrents = [
                    ("large", &large, &large_hash, &large_content),
                    ("small", &small, &small_hash, &small_content),
                ];
                for (label, info, hash, content) in torrents {
                    if !status_available(&engine, hash, STATUS_BUDGET) {
                        return Err(format!(
                            "{label}: the status snapshot stayed unavailable for \
                             {STATUS_BUDGET:?} while the wave ran"
                        ));
                    }
                    let files = info
                        .files()
                        .map_err(|err| format!("{label}: file table unreadable: {err:?}"))?;
                    if files.is_empty() {
                        return Err(format!("{label}: the file table is empty"));
                    }
                    let mut offset = 0u64;
                    for file in &files {
                        let head = file.size.min(WALK_HEAD as u64) as u32;
                        let want = &content[offset as usize..(offset + head as u64) as usize];
                        read_matching(&engine, Arc::clone(info), offset, head, want)
                            .map_err(|err| format!("{label}: {}: {err}", file.path))?;
                        offset += file.size;
                    }
                    if offset != content.len() as u64 {
                        return Err(format!(
                            "{label}: the file table covers {offset} bytes, the fixture \
                             carries {}",
                            content.len()
                        ));
                    }
                }
                Ok(())
            })();
            (format!("tree walk {index}"), outcome)
        }));
    }

    let mut failures: Vec<String> = Vec::new();
    for task in tasks {
        match task.join() {
            Ok((_label, Ok(()))) => {}
            Ok((label, Err(err))) => failures.push(format!("{label}: {err}")),
            Err(_) => failures.push("a wave thread panicked".to_string()),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {total} concurrent operations failed:\n{}",
        failures.len(),
        failures.join("\n")
    );

    // The daemon side (the engine) must still be alive and serving.  The
    // snapshot map survives the engine thread, so only a command-channel read
    // is evidence: it fails unless the engine answers.
    let after = engine
        .read_file_range(Arc::clone(&small), 0, 0, WALK_HEAD)
        .expect("the engine must still serve a read after the wave");
    assert_eq!(
        after,
        small_content[..WALK_HEAD as usize],
        "the read served after the wave must still return the seeded bytes"
    );

    engine.shutdown();
}

/// Wait until the fixture peer is counted as a seeder, so the wave starts with
/// the sources usable instead of spending a discovery window per torrent.
fn wait_for_seed(engine: &DownloadEngine, info_hash: &str) {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if engine
            .try_torrent_status(info_hash)
            .is_some_and(|status| status.num_seeds >= 1)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the fixture seed for {info_hash} never became usable"
        );
        thread::sleep(Duration::from_millis(100));
    }
}

/// Wait until the non-blocking status snapshot resolves, up to `budget`.
fn status_available(engine: &DownloadEngine, info_hash: &str, budget: Duration) -> bool {
    let deadline = Instant::now() + budget;
    loop {
        if engine.try_torrent_status(info_hash).is_some() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        thread::sleep(Duration::from_millis(20));
    }
}

/// Read `[offset, offset + size)` and require it to equal `want`.
fn read_matching(
    engine: &DownloadEngine,
    info: Arc<TorrentInfo>,
    offset: u64,
    size: u32,
    want: &[u8],
) -> Result<(), String> {
    let data = read_until_settled(engine, info, offset, size)?;
    if data != want {
        return Err(format!(
            "offset {offset} size {size} returned {} bytes that differ from the seeded range",
            data.len()
        ));
    }
    Ok(())
}

/// Read one range, retrying only transient cold-start errors; the returned
/// bytes — not the retry count — are what the caller judges.
fn read_until_settled(
    engine: &DownloadEngine,
    info: Arc<TorrentInfo>,
    offset: u64,
    size: u32,
) -> Result<Vec<u8>, String> {
    let deadline = Instant::now() + RETRY_BUDGET;
    loop {
        match engine.read_file_range(Arc::clone(&info), 0, offset, size) {
            Ok(data) => return Ok(data),
            Err(err) if is_transient_read_error(&err) && Instant::now() < deadline => {
                thread::sleep(Duration::from_millis(200));
            }
            Err(err) => return Err(format!("{err:?}")),
        }
    }
}

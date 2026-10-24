//! Design acceptance case TC-config-06: the no-seeder wait key's default and
//! fallback, and the empty-swarm health-alert grace derived from it
//! (`docs/03-acceptance.md` §G AC-65). Unset and non-positive resolve to 15 s
//! and the read window is `min(read_timeout_secs, that value)`, while the
//! `.stats` grace is that same resolved value and is not narrowed by a shorter
//! read timeout (C-6.4.5). Config inputs load through the shipped file loader;
//! the tests pin the resolved config, the engine value and the first-alert
//! instant, so a 0 that stops matching "unset" fails here.

use crate::common::{base, local_test_config, wave_slot};
use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::infrastructure::db::Database;
use torrentfs::services::download::DownloadService;
use torrentfs::{InsertTorrentResult, TorrentInfo};

pub const TC_ID: &str = "TC-config-06";

/// Shipped default for the no-seeder wait.
const DEFAULT_NO_SEEDER_WAIT_SECS: u64 = 15;
/// Read timeout chosen above every no-seeder value under test, so the `min` in
/// the read window resolves to the no-seeder term.
const READ_TIMEOUT_ABOVE_GRACE_SECS: i64 = 60;

fn config_with_timeouts(
    read_timeout: i64,
    no_seeder: Option<i64>,
) -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.timeouts.read_timeout_secs = Some(read_timeout);
    config.timeouts.no_seeder_read_timeout_secs = no_seeder;
    config
}

/// Load `text` through the shipped file loader — read → parse → `validate()` →
/// `validate_enum_ranges()` — so the config surface under test is the one the
/// CLI and the container see, not the bare deserializer.
fn config_from_toml(text: &str) -> torrentfs::TorrentfsConfig {
    let mut file = tempfile::NamedTempFile::new().expect("temp config file");
    write!(file, "{text}").expect("write temp config");
    torrentfs::TorrentfsConfig::from_file(file.path()).expect("test config must load")
}

/// Unset, 0 and a negative value are the same input class: each resolves to the
/// shipped 15 s default (C-8.2.4), so none of them produces a zero window.
/// Every input goes through the shipped file loader, so a value the loader
/// rejects or rewrites fails here rather than at the deserializer.
#[test]
fn tc_config_06_missing_and_non_positive_values_resolve_to_default() {
    for (text, raw) in [
        ("[timeouts]\n", None),
        ("[timeouts]\nno_seeder_read_timeout_secs = 0\n", Some(0)),
        ("[timeouts]\nno_seeder_read_timeout_secs = -1\n", Some(-1)),
    ] {
        let config = config_from_toml(text);
        assert_eq!(
            config.timeouts.no_seeder_read_timeout_secs, raw,
            "the loader must carry the configured value into the config surface: {text:?}"
        );
        assert_eq!(
            config.timeouts.resolved_no_seeder_read_timeout_secs(),
            DEFAULT_NO_SEEDER_WAIT_SECS,
            "unset and non-positive values must both fall back to 15: {text:?}"
        );
        assert!(
            config.timeouts.read_timeout_secs.is_none(),
            "the no-seeder key must not imply a read timeout: {text:?}"
        );
    }
}

/// A positive value is used as configured, and the engine applies it to both the
/// exposed value and the read-wait budget (`min(read_timeout_secs, value)`).
#[test]
fn tc_config_06_positive_value_is_effective_and_caps_the_read_wait() {
    let _slot = wave_slot();
    let cache_dir = tempfile::TempDir::new().unwrap();

    let mut budgets = Vec::new();
    for (label, value) in [
        ("unset", None),
        ("zero", Some(0)),
        ("negative", Some(-1)),
        ("four", Some(4)),
    ] {
        let engine = torrentfs::download::DownloadEngine::new(
            cache_dir.path(),
            &config_with_timeouts(READ_TIMEOUT_ABOVE_GRACE_SECS, value),
        )
        .unwrap_or_else(|e| panic!("{label}: engine must start: {e:?}"));
        let expected = match value {
            Some(v) if v > 0 => v as u64,
            _ => DEFAULT_NO_SEEDER_WAIT_SECS,
        };
        assert_eq!(
            engine.no_seeder_read_timeout_secs(),
            expected,
            "{label}: the engine must expose the resolved value"
        );
        budgets.push((label, engine.read_wait_budget_secs(), expected));
        engine.shutdown();
    }

    let budget_at_15 = budgets[0].1;
    for (label, budget, effective) in &budgets {
        if *effective == DEFAULT_NO_SEEDER_WAIT_SECS {
            assert_eq!(
                *budget, budget_at_15,
                "{label}: a resolved 15 s window must produce the same read budget as unset"
            );
        }
    }

    // The 4 s value is the one that saves wait time: the budget shrinks by
    // exactly the difference in the no-seeder term.
    let budget_at_4 = budgets[3].1;
    assert_eq!(
        budget_at_15 - budget_at_4,
        DEFAULT_NO_SEEDER_WAIT_SECS - 4,
        "min(read_timeout_secs, value) must enter the read budget by the value's difference"
    );
}

/// First `.stats` sample that carries the empty-swarm health alert, measured
/// from the moment the torrent handle is requested. The torrent announces to a
/// dead port, so the swarm stays empty for the whole window.
fn measure_first_health_alert(
    read_timeout: i64,
    no_seeder: Option<i64>,
    name: &'static str,
) -> Duration {
    let cache_dir = tempfile::TempDir::new().unwrap();
    let (torrent_bytes, _content) =
        base::create_single_piece_torrent("http://127.0.0.1:6973/announce", name);
    let info = Arc::new(TorrentInfo::from_bytes(torrent_bytes).unwrap());
    let info_hash = hex::encode(info.info_hash().unwrap());

    let mut db = Database::open_in_memory().unwrap();
    let InsertTorrentResult::Inserted(torrent_id) = db
        .insert_torrent("data", name, &format!("{name}.torrent"), 16384, &info_hash, 1)
        .unwrap()
    else {
        panic!("{name}: expected a fresh insert");
    };
    let db = Some(Arc::new(Mutex::new(db)));

    let service = Arc::new(
        DownloadService::new(cache_dir.path(), &config_with_timeouts(read_timeout, no_seeder))
            .unwrap_or_else(|e| panic!("{name}: service must start: {e:?}")),
    );
    service.ensure_handle_lightweight(info).unwrap();

    let start = Instant::now();
    let deadline = start + Duration::from_secs(30);
    loop {
        let text = String::from_utf8_lossy(&generate_torrent_stats(
            torrent_id,
            &info_hash,
            &db,
            &Some(Arc::clone(&service)),
            || service.get_cache_manager(),
        ))
        .into_owned();
        if text.contains("⚠ Health: 0 peers / 0 seeds") {
            let elapsed = start.elapsed();
            service.shutdown();
            return elapsed;
        }
        assert!(
            Instant::now() < deadline,
            "{name}: the empty-swarm health alert must appear within 30s:\n{text}"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// The alert grace is the engine's effective no-seeder value: an unset key and
/// an explicit 0 must alert at the same point, while a 4 s configuration alerts
/// materially earlier.
#[test]
fn tc_config_06_health_alert_grace_matches_effective_value() {
    let _slot = wave_slot();
    let _session_lock = base::acquire_session_lock();

    let unset = measure_first_health_alert(
        READ_TIMEOUT_ABOVE_GRACE_SECS,
        None,
        "tc_config_06_unset",
    );
    let zero = measure_first_health_alert(
        READ_TIMEOUT_ABOVE_GRACE_SECS,
        Some(0),
        "tc_config_06_zero",
    );
    let four = measure_first_health_alert(
        READ_TIMEOUT_ABOVE_GRACE_SECS,
        Some(4),
        "tc_config_06_four",
    );
    println!("first health alert: unset={unset:?} zero={zero:?} four={four:?}");

    for (label, elapsed) in [("unset", unset), ("zero", zero)] {
        assert!(
            elapsed >= Duration::from_secs(14),
            "{label}: the alert must wait out the 15s grace, fired after {elapsed:?}"
        );
        assert!(
            elapsed <= Duration::from_secs(25),
            "{label}: the alert must fire on the 15s grace (snapshot-start skew included), \
             fired after {elapsed:?}"
        );
    }

    let split = unset.abs_diff(zero);
    assert!(
        split <= Duration::from_secs(2),
        "0 and unset must alert at the same point, split by {split:?} \
         (unset {unset:?}, zero {zero:?})"
    );

    assert!(
        four >= Duration::from_secs(2) && four <= Duration::from_secs(10),
        "a 4s value must alert on its own grace, fired after {four:?}"
    );
    assert!(
        unset.saturating_sub(four) >= Duration::from_secs(5),
        "the 4s configuration must alert materially earlier than the default \
         (four {four:?}, unset {unset:?})"
    );
}

/// C-6.4.5: a read timeout shorter than the no-seeder wait caps only the read
/// budget's piece-wait term (`min(5, 15) = 5`); the alert grace stays the
/// resolved 15 s value instead of being narrowed to the read timeout.
#[test]
fn tc_config_06_short_read_timeout_caps_the_budget_not_the_grace() {
    let _slot = wave_slot();
    let _session_lock = base::acquire_session_lock();
    let cache_dir = tempfile::TempDir::new().unwrap();

    let budget = |no_seeder: Option<i64>| {
        let engine = torrentfs::download::DownloadEngine::new(
            cache_dir.path(),
            &config_with_timeouts(5, no_seeder),
        )
        .unwrap_or_else(|e| panic!("{no_seeder:?}: engine must start: {e:?}"));
        let budget = engine.read_wait_budget_secs();
        engine.shutdown();
        budget
    };

    // The piece-wait term is `min(5, resolved)`: an explicit 5 and the resolved
    // 15 land on the same budget, and only a value below the read timeout moves
    // it. The comparison is relative on purpose — the other budget terms
    // (recheck, peer discovery) are not what this case pins.
    let unset = budget(None);
    assert_eq!(
        budget(Some(5)),
        unset,
        "min(5, 15) and min(5, 5) must both enter the read budget as 5"
    );
    assert_eq!(
        budget(Some(4)) + 1,
        unset,
        "min(5, 4) must shorten the piece-wait term by exactly one second"
    );

    let alert = measure_first_health_alert(5, None, "tc_config_06_short_read_timeout");
    println!("first health alert at read_timeout=5s: {alert:?}");
    assert!(
        alert >= Duration::from_secs(14),
        "a 5s read timeout must not shorten the 15s alert grace, fired after {alert:?}"
    );
    assert!(
        alert <= Duration::from_secs(25),
        "the alert must still fire on the 15s grace, fired after {alert:?}"
    );
}

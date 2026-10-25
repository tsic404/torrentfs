//! Design acceptance case TC-config-04: a partial configuration overrides only
//! the keys it names, every unnamed key keeps the all-default behaviour
//! (`docs/03-acceptance.md` §G AC-63; `docs/02-contracts.md` §C-8.1.1).
//!
//! Both files load through the shipped loader and pass `--config-check`, and
//! the daemon's own `FsService` renders the root `.stats` each run publishes.
//! The cache-only control moves exactly one line, so the all-default equality
//! for the unnamed keys cannot pass on an inert surface.

use std::io::Write;
use std::path::Path;

use torrentfs::fuse::fs_types::StatsKind;
use torrentfs::fuse::FsService;
use torrentfs::TorrentfsConfig;

use crate::common::wave_slot;

pub const TC_ID: &str = "TC-config-04";

/// No key at all: the all-default run every partial run is compared against.
const ALL_DEFAULT: &str = "# every key unset\n";

/// Names a torrentfs-level wait and one upstream connection knob.  The cache
/// budget, listen address, discovery wait, cap and no-seeder wait stay unnamed.
const PARTIAL_WAITS_AND_CONNECTIONS: &str = "\
[timeouts]
read_timeout_secs = 90

[connections]
max_connections = 200
";

/// Names only the cache budget: the control proving the cache line follows the
/// config, so the equality between the all-default and partial runs above is
/// not vacuous.
const PARTIAL_CACHE_ONLY: &str = "\
[cache]
cache_size = 67108864
";

/// `.stats` lines whose value is derived from the config: the listen address,
/// the cache directory/budget and the database state.  Runtime counters
/// (uptime, DHT nodes, rates) are session state, not config surface, and stay
/// outside the comparison.
const CONFIG_LINES: &[&str] = &["Listen:", "Cache Dir:", "Cache Usage:", "Database:"];

/// The config-derived `.stats` surface plus the engine's wait observables.
struct Surface {
    lines: Vec<String>,
    read_wait_budget_secs: u64,
    no_seeder_read_timeout_secs: u64,
}

fn write_config(dir: &Path, body: &str) -> std::path::PathBuf {
    let path = dir.join("config.toml");
    let mut file = std::fs::File::create(&path).expect("create config file");
    write!(file, "{body}").expect("write config file");
    path
}

fn load(body: &str) -> TorrentfsConfig {
    let dir = tempfile::TempDir::new().expect("temp config dir");
    let path = write_config(dir.path(), body);
    TorrentfsConfig::from_file(&path).expect("config must load through the shipped loader")
}

fn config_check(body: &str) -> std::process::Output {
    let dir = tempfile::TempDir::new().expect("temp config dir");
    let path = write_config(dir.path(), body);
    std::process::Command::new(env!("CARGO_BIN_EXE_torrentfs"))
        .args(["--config-check", "--config"])
        .arg(path)
        .output()
        .expect("failed to run torrentfs")
}

/// Render the root `.stats` the daemon serves for `config` and read the wait
/// values the engine resolved from it.
fn surface(config: &TorrentfsConfig) -> Surface {
    let cache_dir = tempfile::TempDir::new().expect("temp cache dir");
    let service = FsService::new_with_cache_path(cache_dir.path().to_path_buf(), config);
    let started = std::time::Instant::now();
    let stats = loop {
        let stats = String::from_utf8_lossy(&service.read_stats(StatsKind::Global)).into_owned();
        // A startup piece scan can hold the cache lock; wait for the real line.
        if !stats.contains("(locked)") {
            break stats;
        }
        assert!(
            started.elapsed() < std::time::Duration::from_secs(5),
            "the cache manager stayed locked, so .stats cannot publish the cache budget"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    let lines = stats
        .lines()
        .filter(|line| {
            CONFIG_LINES
                .iter()
                .any(|label| line.trim_start().starts_with(label))
        })
        .map(str::to_string)
        .collect();
    let engine = service
        .download_service
        .as_ref()
        .expect("the daemon's download engine must start for the stats surface");
    let surface = Surface {
        lines,
        read_wait_budget_secs: engine.read_wait_budget_secs(),
        no_seeder_read_timeout_secs: engine.no_seeder_read_timeout_secs(),
    };
    engine.shutdown();
    surface
}

fn cache_usage_line(surface: &Surface) -> &str {
    surface
        .lines
        .iter()
        .find(|line| line.trim_start().starts_with("Cache Usage:"))
        .expect("root .stats must publish the cache usage line")
}

/// Every partial file is legal through the shipped loader and
/// `--config-check`, and the downstream settings blob carries exactly the keys
/// the file names: an unnamed key is never filled with a default, so what a
/// partial deployment runs on is the upstream default (C-8.1.1/C-8.1.3).
#[test]
fn tc_config_04_partial_files_are_accepted_and_push_only_named_keys() {
    for (label, body) in [
        ("all-default", ALL_DEFAULT),
        ("waits+connections", PARTIAL_WAITS_AND_CONNECTIONS),
        ("cache-only", PARTIAL_CACHE_ONLY),
    ] {
        let out = config_check(body);
        assert_eq!(
            out.status.code(),
            Some(0),
            "{label}: a partial config must be accepted by --config-check; stdout={} stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
    }

    let default = load(ALL_DEFAULT);
    assert_eq!(
        default.to_settings_json(),
        "{}",
        "an empty config file must push no key downstream"
    );

    let partial = load(PARTIAL_WAITS_AND_CONNECTIONS);
    assert_eq!(
        partial.to_settings_json(),
        r#"{"max_connections":200}"#,
        "only the named upstream key may be pushed downstream"
    );
    assert_eq!(
        partial.timeouts.resolved_read_timeout_secs(),
        90,
        "the named read timeout must resolve to its configured value"
    );
    assert_eq!(
        partial.timeouts.resolved_peer_discovery_wait_secs(),
        30,
        "the unnamed discovery wait must keep its default"
    );
    assert_eq!(
        partial.timeouts.resolved_peer_wait_cap_secs(),
        None,
        "the unnamed cap must keep the uncapped default"
    );
    assert_eq!(
        partial.timeouts.resolved_no_seeder_read_timeout_secs(),
        15,
        "the unnamed no-seeder wait must keep its default"
    );
    assert!(
        partial.cache.cache_size.is_none(),
        "the unnamed cache budget must stay unset instead of being filled with a default"
    );

    let cache_only = load(PARTIAL_CACHE_ONLY);
    assert_eq!(
        cache_only.to_settings_json(),
        r#"{"cache_size":67108864}"#,
        "only the named cache key may be pushed downstream"
    );
    assert_eq!(
        cache_only.timeouts.resolved_read_timeout_secs(),
        60,
        "a cache-only file must leave the read timeout at its default"
    );
    assert_eq!(
        cache_only.timeouts.resolved_no_seeder_read_timeout_secs(),
        15,
        "a cache-only file must leave the no-seeder wait at its default"
    );
}

/// The daemon-visible config surface of a partial run equals the all-default
/// run for every key it leaves unnamed, and the cache-only file moves exactly
/// the cache line — so the equalities track the file, not an inert surface.
#[test]
fn tc_config_04_unnamed_keys_keep_the_all_default_surface() {
    let _slot = wave_slot();

    let default = surface(&load(ALL_DEFAULT));
    let partial = surface(&load(PARTIAL_WAITS_AND_CONNECTIONS));
    let cache_only = surface(&load(PARTIAL_CACHE_ONLY));

    assert!(
        default
            .lines
            .iter()
            .any(|line| line.contains("Listen:") && line.contains("0.0.0.0:6881")),
        "the all-default run must publish the default listen address: {:?}",
        default.lines
    );
    assert_eq!(
        partial.lines, default.lines,
        "a partial config must leave the listen/cache/database surface at the all-default values"
    );
    assert_eq!(
        partial.no_seeder_read_timeout_secs, default.no_seeder_read_timeout_secs,
        "the unnamed no-seeder wait must keep the default the engine resolves"
    );
    assert!(
        partial.read_wait_budget_secs > default.read_wait_budget_secs,
        "the named 90s read timeout must widen the read budget (default {}s, partial {}s)",
        default.read_wait_budget_secs,
        partial.read_wait_budget_secs
    );

    assert_eq!(
        cache_usage_line(&default),
        "  Cache Usage:  0 B / 1.00 GB (0.0%)",
        "the all-default run must publish the shipped 1 GiB budget"
    );
    assert_eq!(
        cache_usage_line(&cache_only),
        "  Cache Usage:  0 B / 64.00 MB (0.0%)",
        "a cache-only config must publish exactly the named budget"
    );
    let cache_only_other: Vec<&String> = cache_only
        .lines
        .iter()
        .filter(|line| !line.trim_start().starts_with("Cache Usage:"))
        .collect();
    let default_other: Vec<&String> = default
        .lines
        .iter()
        .filter(|line| !line.trim_start().starts_with("Cache Usage:"))
        .collect();
    assert_eq!(
        cache_only_other, default_other,
        "naming the cache budget must not move any other config-derived line"
    );
    assert_eq!(
        cache_only.no_seeder_read_timeout_secs, default.no_seeder_read_timeout_secs,
        "a cache-only config must leave the no-seeder wait at the default"
    );
    assert_eq!(
        cache_only.read_wait_budget_secs, default.read_wait_budget_secs,
        "a cache-only config must leave the read budget at the default"
    );
}

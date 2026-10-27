//! Design acceptance case TC-negative-06: resolving entries by name must not
//! rescale its cost with the directory (`docs/03-acceptance.md` 验收用例,
//! AC-91).  Two scales four times apart, the smaller at twenty thousand
//! entries, are resolved name by name on separate instances; growth must stay
//! under 8x.  The bound separates an indexed lookup (about 4x) from one
//! rescanning the directory per name (about 16x) only while one scan costs at
//! least half a fixed lookup: the calibration below measures both, raises the
//! scales when that headroom is missing, and fails as undecidable if it stays.

use std::collections::BTreeSet;
use std::hint::black_box;
use std::time::{Duration, Instant};

use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::DATA_INO;
use torrentfs::{Database, FileEntry, FsService, InsertTorrentResult};

use crate::common::{service_with_state, wave_slot};

pub const TC_ID: &str = "TC-negative-06";

/// Smaller scale: AC-91 puts it at no fewer than twenty thousand entries and
/// the larger one at four times it.  Both are raised together when the
/// calibration finds no headroom.
const SMALL_ENTRIES: usize = 20_000;
const SCALE_FACTOR: usize = 4;

/// Calibration tier, small enough that a scan of it costs nothing next to one
/// lookup's fixed cost.
const CALIBRATION_ENTRIES: usize = 8;

/// The design's bound: per-name resolution is about `SCALE_FACTOR`, a lookup
/// that rescans the directory per name is about `SCALE_FACTOR²`.
const MAX_GROWTH: f64 = 8.0;

/// The design allows one 4x raise of both scales when headroom is missing and
/// then stops the determination.
const MAX_RAISES: usize = 1;

/// Walks averaged for the scan bound; the minimum is the bound.
const SCAN_SAMPLES: usize = 5;

/// Floor under the calibrated fixed cost, so a zero reading cannot be divided
/// by.
const MIN_FIXED_COST: Duration = Duration::from_micros(1);

/// One scale's resolution record.
struct Tier {
    entries: usize,
    names: Vec<String>,
    elapsed: Duration,
}

impl Tier {
    fn per_lookup(&self) -> Duration {
        Duration::from_secs_f64(self.elapsed.as_secs_f64() / self.entries as f64)
    }
}

/// The metadata-declared file list of one torrent: every file in a single
/// `bulk/` directory, each declaring a size of its own, so a resolved entry's
/// size proves it came from that entry's row and not a same-named neighbour.
fn metadata_files(entries: usize) -> Vec<FileEntry> {
    (0..entries)
        .map(|index| FileEntry {
            path: format!("bulk/{index:05}.bin"),
            size: index as i64 + 1,
        })
        .collect()
}

/// The entry names the metadata declares, in declaration order.  Taken from
/// the paths alone — no listing of the projected directory is involved.
fn declared_names(files: &[FileEntry]) -> Vec<String> {
    files
        .iter()
        .map(|file| match file.path.rsplit_once('/') {
            Some((_, name)) => name.to_string(),
            None => file.path.clone(),
        })
        .collect()
}

/// Seed one torrent row plus its metadata file list.
fn seed_torrent(db: &mut Database, filename: &str, files: &[FileEntry]) {
    let source_id = match db
        .insert_torrent(
            "",
            filename,
            filename,
            files.len() as i64,
            filename,
            files.len() as i64,
        )
        .expect("insert torrent row")
    {
        InsertTorrentResult::Inserted(id) | InsertTorrentResult::Duplicate(id) => id,
    };
    db.insert_files(source_id, files).expect("insert file tree");
}

/// Resolve the `bulk/` directory of one seeded torrent through the projection,
/// the way the kernel reaches it: `data/<filename>` then the torrent's tree.
fn bulk_directory(service: &mut FsService, filename: &str) -> u64 {
    let root = service
        .lookup(DATA_INO, filename)
        .unwrap_or_else(|e| panic!("lookup data/{filename}: {e:?}"))
        .unwrap_or_else(|| panic!("data/{filename} must be projected"));
    service
        .lookup(root.ino, "bulk")
        .unwrap_or_else(|e| panic!("lookup {filename}/bulk: {e:?}"))
        .unwrap_or_else(|| panic!("{filename}/bulk must be projected"))
        .ino
}

/// Resolve every declared name under `dir` and time only that loop.
fn resolve_all_by_name(
    service: &mut FsService,
    dir: u64,
    names: &[String],
) -> (Vec<u64>, Duration) {
    let mut inodes = Vec::with_capacity(names.len());
    let start = Instant::now();
    for name in names {
        let entry = service
            .lookup(dir, name)
            .unwrap_or_else(|e| panic!("lookup({dir}, {name}): {e:?}"))
            .unwrap_or_else(|| panic!("{name} must resolve by name"));
        inodes.push(entry.ino);
    }
    (inodes, start.elapsed())
}

/// The resolved set must be exactly the metadata-declared set: the name at
/// each declaration position resolves to its own regular file, carrying that
/// declaration's size.
fn assert_resolved_set(
    service: &mut FsService,
    dir: u64,
    inodes: &[u64],
    files: &[FileEntry],
    label: &str,
) {
    assert_eq!(
        inodes.len(),
        files.len(),
        "{label}: every metadata entry must resolve"
    );
    let distinct: BTreeSet<u64> = inodes.iter().copied().collect();
    assert_eq!(
        distinct.len(),
        files.len(),
        "{label}: {} metadata entries must resolve to {} distinct inodes",
        files.len(),
        files.len()
    );
    for (index, ino) in inodes.iter().copied().enumerate() {
        let attr = service
            .getattr(ino)
            .unwrap_or_else(|e| panic!("{label}: getattr({ino}): {e:?}"));
        assert_eq!(
            attr.kind,
            FileKind::RegularFile,
            "{label}: resolved entry {ino} must be a regular file"
        );
        assert_eq!(
            attr.size, files[index].size as u64,
            "{label}: {} must resolve to the entry declaring {} bytes, got {ino} \
             of {} bytes",
            files[index].path, files[index].size, attr.size
        );
    }
    let absent = service
        .lookup(dir, "not-declared.bin")
        .unwrap_or_else(|e| panic!("{label}: lookup absent name: {e:?}"));
    assert!(
        absent.is_none(),
        "{label}: a name the metadata does not declare must not resolve"
    );
}

/// Measure one scale on its own instance: seeded with only this directory's
/// torrent, a lookup that scans the whole table cannot match the other scale's
/// cost, and a lookup that rescans the directory pays for this scale alone.
fn measure_tier(entries: usize, label: &str) -> Tier {
    let (mut service, _cache) = service_with_state();
    let files = metadata_files(entries);
    let filename = format!("{label}.torrent");
    {
        let db = service
            .db
            .as_ref()
            .expect("service owns its database")
            .clone();
        let mut guard = db.lock().expect("lock database");
        seed_torrent(&mut guard, &filename, &files);
    }
    let dir = bulk_directory(&mut service, &filename);
    let names = declared_names(&files);

    // Warm one lookup outside the window: the first resolution pays the page
    // and inode initialisation that no later one repeats, and the design does
    // not judge a one-off materialisation amortised over the entries.
    service
        .lookup(dir, &names[0])
        .unwrap_or_else(|e| panic!("{label}: warm lookup: {e:?}"))
        .unwrap_or_else(|| panic!("{label}: {} must resolve", names[0]));

    let (inodes, elapsed) = resolve_all_by_name(&mut service, dir, &names);
    assert_resolved_set(
        &mut service,
        dir,
        &inodes,
        &files,
        &format!("{label} scale"),
    );
    Tier {
        entries,
        names,
        elapsed,
    }
}

/// The fixed per-lookup cost `c`, calibrated on a tier where a per-lookup scan
/// costs nothing measurable.
fn calibrated_fixed_cost() -> Duration {
    let tier = measure_tier(CALIBRATION_ENTRIES, "calibration");
    tier.per_lookup().max(MIN_FIXED_COST)
}

/// A lower bound on what one per-lookup full scan of `names` adds: a linear
/// in-memory walk comparing every name against one probe, the cheapest shape
/// the scope forbids (a materialised list searched by name).  A database scan
/// costs more, so this bounds the hardest violation to catch; the minimum of
/// several walks is the bound.
fn full_scan_lower_bound(names: &[String]) -> Duration {
    let probe = names[names.len() / 2].as_str();
    (0..SCAN_SAMPLES)
        .map(|_| {
            let start = Instant::now();
            let hits = names
                .iter()
                .filter(|name| name.as_str() == probe)
                .count();
            let elapsed = start.elapsed();
            black_box(hits);
            elapsed
        })
        .min()
        .expect("SCAN_SAMPLES is not zero")
}

#[test]
fn tc_negative_06_per_name_resolution_does_not_scan_the_directory() {
    let _slot = wave_slot();

    let fixed_cost = calibrated_fixed_cost();
    let mut small_entries = SMALL_ENTRIES;
    let mut small = measure_tier(small_entries, "small");
    let mut scan_cost = full_scan_lower_bound(&small.names);
    let mut raises = 0;
    while scan_cost < fixed_cost * 2 {
        assert!(
            raises < MAX_RAISES,
            "TC-negative-06 has no discriminating power on this machine: one full scan \
             of {small_entries} entries costs {scan_cost:?} against a fixed lookup cost \
             of {fixed_cost:?}, short of the 2x headroom the {MAX_GROWTH}x bound needs \
             to separate a per-name scan from an indexed lookup after {raises} raise(s)"
        );
        raises += 1;
        small_entries *= SCALE_FACTOR;
        small = measure_tier(small_entries, "small");
        scan_cost = full_scan_lower_bound(&small.names);
        eprintln!(
            "TC-negative-06: raised both scales to {small_entries}/{} entries; a full \
             scan now costs {scan_cost:?} against a fixed lookup cost of {fixed_cost:?}",
            small_entries * SCALE_FACTOR
        );
    }

    let large = measure_tier(small_entries * SCALE_FACTOR, "large");

    eprintln!(
        "TC-negative-06: fixed lookup {fixed_cost:?} (calibrated on {CALIBRATION_ENTRIES} \
         entries), full-scan lower bound {scan_cost:?} on {small_entries} entries \
         ({:.2}x the fixed cost, 2x required), scales {}/{} entries, raises {raises}",
        scan_cost.as_secs_f64() / fixed_cost.as_secs_f64(),
        small.entries,
        large.entries
    );
    eprintln!(
        "TC-negative-06: {} entries resolved in {:?} ({:.2?} each), {} entries in {:?} \
         ({:.2?} each) — growth {:.2}x, bound {MAX_GROWTH}x",
        small.entries,
        small.elapsed,
        small.per_lookup(),
        large.entries,
        large.elapsed,
        large.per_lookup(),
        large.elapsed.as_secs_f64() / small.elapsed.as_secs_f64().max(1e-9)
    );

    let growth = large.elapsed.as_secs_f64() / small.elapsed.as_secs_f64().max(1e-9);
    assert!(
        growth < MAX_GROWTH,
        "resolving {} entries by name took {:?} against {:?} for {} ({growth:.2}x): a \
         {SCALE_FACTOR}x directory must not cost {MAX_GROWTH}x or more when every entry \
         is resolved by name",
        large.entries,
        large.elapsed,
        small.elapsed,
        small.entries
    );
}

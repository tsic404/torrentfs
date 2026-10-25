//! Executable acceptance suite — registry and runner.
//!
//! `tsic/torrentfs-design@4bc1833` `docs/03-acceptance.md` fixes the TC
//! identifiers ("编号冻结，不得重排") and declares that the executable form
//! lives in this repo under `tests/acceptance/<TC-id>`.  This crate root
//! carries the registry: every design TC id, which ids have an executable
//! module here, and validation tests that keep the registry honest (an id
//! without a module is a missing-test-evidence finding for QA; a module
//! without an id is drift).
//!
//! One module per landed TC (`test_tc_<id lowercased, `-` → `_`>.rs`), each
//! declaring `pub const TC_ID: &str`.  Run everything:
//!
//! ```text
//! cargo test --test acceptance
//! ```
//!
//! Run one TC by id (libtest name filter on the module's test functions):
//!
//! ```text
//! cargo test --test acceptance -- tc_read_01
//! ```

// ── landed TC modules ────────────────────────────────────────────────────────
// P0 batch.  Modules are added here as each TC's executable form lands.

mod common;
mod seed_peer;

mod test_tc_cache_01;
mod test_tc_config_01;
mod test_tc_config_02;
mod test_tc_config_03;
mod test_tc_config_04;
mod test_tc_config_05;
mod test_tc_config_06;
mod test_tc_config_07;
mod test_tc_read_01;
mod test_tc_read_02;
mod test_tc_read_03;
mod test_tc_read_05;
mod test_tc_read_06;
mod test_tc_rofs_02;
mod test_tc_rofs_03;
mod test_tc_seeding_01;
mod test_tc_seeding_02;
mod test_tc_seeding_03;

/// The full id set fixed by `docs/03-acceptance.md` (60 ids, design anchor
/// 4bc1833).  Sorted; the validation test below pins it exactly.
pub const TC_IDS: &[&str] = &[
    "TC-cache-01",
    "TC-cache-02",
    "TC-cache-03",
    "TC-cache-04",
    "TC-cache-05",
    "TC-config-01",
    "TC-config-02",
    "TC-config-03",
    "TC-config-04",
    "TC-config-05",
    "TC-config-06",
    "TC-config-07",
    "TC-container-01",
    "TC-container-02",
    "TC-container-03",
    "TC-container-04",
    "TC-container-05",
    "TC-container-06",
    "TC-container-07",
    "TC-container-08",
    "TC-ingest-01",
    "TC-ingest-02",
    "TC-ingest-03",
    "TC-ingest-04",
    "TC-ingest-05",
    "TC-ingest-06",
    "TC-ingest-07",
    "TC-negative-01",
    "TC-negative-02",
    "TC-negative-03",
    "TC-negative-04",
    "TC-read-01",
    "TC-read-02",
    "TC-read-03",
    "TC-read-04",
    "TC-read-05",
    "TC-read-06",
    "TC-read-07",
    "TC-read-08",
    "TC-read-09",
    "TC-read-10",
    "TC-recovery-01",
    "TC-recovery-02",
    "TC-recovery-03",
    "TC-recovery-04",
    "TC-recovery-05",
    "TC-rofs-01",
    "TC-rofs-02",
    "TC-rofs-03",
    "TC-seeding-01",
    "TC-seeding-02",
    "TC-seeding-03",
    "TC-stats-01",
    "TC-stats-02",
    "TC-stats-03",
    "TC-stats-04",
    "TC-stats-05",
    "TC-stats-06",
    "TC-stats-07",
    "TC-stats-08",
];

/// Ids with an executable module in this directory right now.  A TC the
/// design declares but this list does not carry is pending its batch — the
/// issue tracks that explicitly (no silent shrinking).
///
/// Each id is paired with its module's `TC_ID` constant so the constants are
/// genuinely referenced (and a module that renames its id breaks this table
/// at compile time).
pub const LANDED: &[&str] = &[
    test_tc_cache_01::TC_ID,
    test_tc_config_01::TC_ID,
    test_tc_config_02::TC_ID,
    test_tc_config_03::TC_ID,
    test_tc_config_04::TC_ID,
    test_tc_config_05::TC_ID,
    test_tc_config_06::TC_ID,
    test_tc_config_07::TC_ID,
    test_tc_read_01::TC_ID,
    test_tc_read_02::TC_ID,
    test_tc_read_03::TC_ID,
    test_tc_read_05::TC_ID,
    test_tc_read_06::TC_ID,
    test_tc_rofs_02::TC_ID,
    test_tc_rofs_03::TC_ID,
    test_tc_seeding_01::TC_ID,
    test_tc_seeding_02::TC_ID,
    test_tc_seeding_03::TC_ID,
];

/// Ids whose executable form is a FUSE/container-level shell script in this
/// directory (TC-ingest-01 → `tc_ingest_01.sh`).  The script declares its TC
/// id in its header comment; `script_form_modules_have_their_script` checks
/// the file exists and carries the id.
pub const SCRIPT_FORM: &[&str] = &[
    "TC-cache-02",
    "TC-container-01",
    "TC-container-02",
    "TC-ingest-01",
    "TC-read-04",
    "TC-recovery-01",
    "TC-rofs-01",
    "TC-stats-01",
    "TC-stats-02",
];

/// Map a script-form TC id to its script filename: `TC-read-04` →
/// `tc_read_04.sh`.
pub fn script_name(tc_id: &str) -> String {
    format!("{}.sh", tc_id.to_lowercase().replace('-', "_"))
}

/// Map a TC id to its module filename: `TC-read-01` → `test_tc_read_01.rs`.
pub fn module_name(tc_id: &str) -> String {
    format!("test_{}.rs", tc_id.to_lowercase().replace('-', "_"))
}

#[cfg(test)]
mod registry_tests {
    use super::{module_name, script_name, LANDED, SCRIPT_FORM, TC_IDS};

    /// The registry must carry exactly the 60 ids the design fixes, in the
    /// frozen order-independent sense (set equality; order here is just the
    /// sorted listing).  A missing id means the suite silently dropped a
    /// design TC — exactly what QA reports as missing-test-evidence.
    #[test]
    fn registry_pins_the_design_id_set() {
        let mut ids = TC_IDS.to_vec();
        ids.sort_unstable();
        ids.dedup();
        assert_eq!(
            ids.len(),
            TC_IDS.len(),
            "TC_IDS must not contain duplicates"
        );
        assert_eq!(ids.len(), 60, "design docs/03-acceptance.md fixes 60 TCs");
        assert!(
            ids.iter().all(|id| id.starts_with("TC-")),
            "every id must carry the TC- prefix"
        );
    }

    /// Every landed id must have a module file that declares the same id.
    /// The module's `pub const TC_ID` is checked textually: if the module
    /// compiles but renamed its id, the declaration no longer matches.
    #[test]
    fn every_landed_id_has_a_declaring_module() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("acceptance");
        for tc_id in LANDED {
            let path = dir.join(module_name(tc_id));
            assert!(
                path.exists(),
                "{} has no executable module at tests/acceptance/{}",
                tc_id,
                module_name(tc_id)
            );
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("cannot read {}", path.display()));
            assert!(
                source.contains(&format!("pub const TC_ID: &str = \"{}\"", tc_id)),
                "{} module must declare `pub const TC_ID: &str = \"{}\"`",
                path.display(),
                tc_id
            );
        }
    }

    /// No module may exist that is not in the landed registry: a stray case
    /// module would run in `cargo test --test acceptance` while QA's id
    /// matrix cannot attribute it to a design TC.
    #[test]
    fn no_unregistered_case_modules() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("acceptance");
        let mut stray = Vec::new();
        for entry in std::fs::read_dir(&dir).expect("read acceptance dir") {
            let path = entry.expect("dir entry").path();
            let name = match path.file_name().and_then(|n| n.to_str()) {
                Some(n) if n.starts_with("test_tc_") && n.ends_with(".rs") => n.to_string(),
                _ => continue,
            };
            let id = format!(
                "TC-{}",
                name["test_tc_".len()..name.len() - ".rs".len()].replace('_', "-")
            );
            if !LANDED.contains(&id.as_str()) {
                stray.push(path.display().to_string());
            }
        }
        assert!(
            stray.is_empty(),
            "case modules missing from the registry: {:?}",
            stray
        );
    }

    /// Every script-form TC must have its `tc_<id>.sh` file, and the file
    /// must carry the id in its header so the mapping stays greppable.
    #[test]
    fn script_form_tcs_have_their_script() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("acceptance");
        for tc_id in SCRIPT_FORM {
            let path = dir.join(script_name(tc_id));
            assert!(
                path.exists(),
                "{} has no executable script at tests/acceptance/{}",
                tc_id,
                script_name(tc_id)
            );
            let source = std::fs::read_to_string(&path)
                .unwrap_or_else(|_| panic!("cannot read {}", path.display()));
            assert!(
                source.starts_with("#!/usr/bin/env bash"),
                "{} must be an executable bash script",
                path.display()
            );
            assert!(
                source.contains(&format!("case {}", tc_id)),
                "{} must declare its case id in the header (case {})",
                path.display(),
                tc_id
            );
        }
    }
}

//! Design acceptance case TC-ingest-02: the `metadata/` hierarchy is mirrored
//! level for level under `data/`, and a seed stored at the `metadata/` root
//! appears at the `data/` root (`docs/03-acceptance.md` §A AC-02).
//!
//! The carrier drives the service calls the FUSE adapter forwards: `mkdir`
//! builds the hierarchy, a copy (create → write → flush → release) ingests a
//! seed, and `readdir` / `getattr` observe the projection.  Every projected
//! path is a directory named after the full `.torrent` filename, and every
//! level is `0555`.

use crate::common::{create_single_piece_torrent, wave_slot, IngestHarness};
use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};

pub const TC_ID: &str = "TC-ingest-02";

/// Multi-level directories and a root-level seed coexist: the relative paths
/// listed under `data/` equal the `metadata/` paths (root seed included), and
/// every level is a `0555` directory.
#[test]
fn tc_ingest_02_mirrors_nested_levels_and_root_seed() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let announce = "http://127.0.0.1:9/announce";

    let (root_seed, root_content) = create_single_piece_torrent(announce, "ingest-02-root.bin");
    let (alpha_seed, _) = create_single_piece_torrent(announce, "ingest-02-alpha.bin");
    let (beta_seed, _) = create_single_piece_torrent(announce, "ingest-02-beta.bin");

    // metadata/root.torrent
    ingest.copy_torrent(METADATA_INO, "root.torrent", &root_seed);
    // metadata/alpha/alpha.torrent
    let alpha = ingest.mkdir(&["alpha"]);
    ingest.copy_torrent(alpha, "alpha.torrent", &alpha_seed);
    // metadata/alpha/beta/beta.torrent, plus an empty level that still mirrors.
    let beta = ingest.mkdir(&["alpha", "beta"]);
    ingest.copy_torrent(beta, "beta.torrent", &beta_seed);
    ingest.mkdir(&["alpha", "beta", "empty"]);

    // Each metadata/ entry (directory or .torrent file) is one projected
    // directory at the same relative path — nothing added, nothing missing.
    assert_eq!(
        ingest.data_dirs(),
        ingest.metadata_projection_paths(),
        "data/ must mirror the metadata/ hierarchy path for path"
    );

    // The root-level seed sits directly under data/, not under a subsection.
    assert!(
        ingest
            .children(DATA_INO)
            .iter()
            .any(|entry| entry.name == "root.torrent"),
        "a metadata/ root seed must appear in the data/ root"
    );
    let root_dir = ingest
        .data_lookup(&["root.torrent"])
        .expect("data/root.torrent must resolve");
    assert_eq!(
        ingest
            .service()
            .getattr(root_dir)
            .expect("getattr root seed")
            .kind,
        FileKind::Directory,
        "a projected seed is a directory named after the full filename"
    );

    // Each projected level is 0555; the files inside are 0444 with the size
    // declared by the torrent metadata.
    for entry in ingest.data_snapshot() {
        match entry.kind {
            FileKind::Directory => assert_eq!(
                entry.perm, 0o555,
                "projected directory data/{} must be 0555",
                entry.path
            ),
            FileKind::RegularFile => assert_eq!(
                entry.perm, 0o444,
                "projected file data/{} must be 0444",
                entry.path
            ),
        }
        if entry.kind == FileKind::RegularFile {
            assert_eq!(
                entry.size,
                root_content.len() as u64,
                "projected file data/{} must carry the torrent-declared size",
                entry.path
            );
        }
    }

    // The seed's own file is reachable through the projected path.
    let file = ingest
        .data_lookup(&["root.torrent", "ingest-02-root.bin"])
        .expect("the projected seed root must list its file");
    assert_eq!(
        ingest
            .service()
            .getattr(file)
            .expect("getattr seed file")
            .size,
        root_content.len() as u64
    );
}

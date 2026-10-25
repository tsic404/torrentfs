//! Design acceptance case TC-ingest-07: a batch of 20 seeds projects one
//! directory per `metadata/` file, deleting a `metadata/` subtree cleans the
//! matching `data/` subtree, and a remount preserves structure, permissions
//! and sizes (`docs/03-acceptance.md` §A AC-07, contract C-12.9).

use crate::common::{create_single_piece_torrent, wave_slot, IngestHarness};
use torrentfs::fuse::fs_types::FileKind;

pub const TC_ID: &str = "TC-ingest-07";

const DIRS: [&str; 4] = ["00", "01", "02", "03"];
const PER_DIR: usize = 5;
const SEEDS: usize = DIRS.len() * PER_DIR;
/// The subtree removed before the remount.
const REMOVED_DIR: &str = "02";

/// Import 20 seeds across four nested directories, remove one subtree, then
/// remount: the projection set is one-to-one both times, the deleted subtree
/// is gone, and the rest survives unchanged.
#[test]
fn tc_ingest_07_batch_import_tree_delete_and_remount() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::with_file_db();
    let announce = "http://127.0.0.1:9/announce";
    let batch = ingest.mkdir(&["batch"]);

    let mut expected_seeds = Vec::new();
    let mut dir_inos = Vec::new();
    for dir in DIRS {
        let ino = ingest.mkdir(&["batch", dir]);
        dir_inos.push((dir, ino));
        for index in 0..PER_DIR {
            let filename = format!("{dir}-{index}.torrent");
            let (bytes, _) =
                create_single_piece_torrent(announce, &format!("ingest-07-{dir}-{index}.bin"));
            ingest.copy_torrent(ino, &filename, &bytes);
            expected_seeds.push(format!("batch/{dir}/{filename}"));
        }
    }
    expected_seeds.sort();

    // 20 seeds, one projection directory each, at the same relative paths.
    let metadata_after_import = ingest.metadata_projection_paths();
    let projected_seeds: Vec<String> = metadata_after_import
        .iter()
        .filter(|path| path.ends_with(".torrent"))
        .cloned()
        .collect();
    assert_eq!(
        projected_seeds.len(),
        SEEDS,
        "all seeds must be in metadata/"
    );
    assert_eq!(
        projected_seeds, expected_seeds,
        "the imported set must be exactly the 20 seeds"
    );
    assert_eq!(
        ingest.data_dirs(),
        metadata_after_import,
        "data/ must list one directory per metadata/ entry"
    );

    // Remove one subtree the way `rm -r` does: drop every seed, then rmdir.
    let removed_ino = dir_inos
        .iter()
        .find(|(name, _)| *name == REMOVED_DIR)
        .map(|(_, ino)| *ino)
        .expect("the removed directory was created");
    for index in 0..PER_DIR {
        ingest
            .service()
            .unlink(removed_ino, &format!("{REMOVED_DIR}-{index}.torrent"))
            .expect("removing a seed must succeed")
            .expect("the removed seed had a content row");
    }
    ingest
        .service()
        .rmdir(batch, REMOVED_DIR)
        .expect("removing the emptied subtree must succeed");

    let after_delete = ingest.data_snapshot();
    assert!(
        !after_delete
            .iter()
            .any(|entry| entry.path == format!("batch/{REMOVED_DIR}")
                || entry.path.starts_with(&format!("batch/{REMOVED_DIR}/"))),
        "the deleted metadata subtree must leave no projection"
    );
    assert_eq!(
        after_delete
            .iter()
            .filter(|entry| entry.kind == FileKind::Directory && entry.path.ends_with(".torrent"))
            .count(),
        SEEDS - PER_DIR,
        "the remaining seeds must still be projected"
    );

    // Remount from the same database and cache directory.
    let mut remounted = ingest.remount();
    assert_eq!(
        remounted.data_snapshot(),
        after_delete,
        "structure, permissions and sizes must survive a remount"
    );
    assert_eq!(
        remounted.data_dirs(),
        after_delete
            .iter()
            .filter(|entry| entry.kind == FileKind::Directory)
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>(),
        "the remounted listing must carry the same directories"
    );
    assert_eq!(
        remounted.metadata_projection_paths(),
        metadata_after_import
            .iter()
            .filter(|path| !path.starts_with(&format!("batch/{REMOVED_DIR}")))
            .cloned()
            .collect::<Vec<_>>(),
        "metadata/ must keep the surviving seeds after the restart"
    );
}

//! Design acceptance case TC-ingest-04: a directory moved across parents keeps
//! both `data/` projections correct — the old location stops listing it, the
//! new one starts (`docs/03-acceptance.md` §A AC-04, C-3.3.6).
//!
//! Driving `FsService` directly is the shipped path: the FUSE adapter
//! forwards `rename` to it and maps the domain error through
//! `src/fuse/errno.rs`, so the assertions below are the entries the kernel
//! sees, read back immediately — the fix must not depend on the 1s entry TTL
//! expiring.

use crate::common::{base, errno, ingest_into, listing, rig, wave_slot, UNUSED_ANNOUNCE_URL};
use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::FsService;

pub const TC_ID: &str = "TC-ingest-04";

/// The names a `data/` directory projects, without the `.`/`..`/`.stats`
/// carriers.
fn projected(svc: &mut FsService, ino: u64) -> Vec<String> {
    listing(svc, ino)
        .into_iter()
        .filter(|(name, _)| !matches!(name.as_str(), "." | ".." | ".stats"))
        .map(|(name, _)| name)
        .collect()
}

/// The inode of the `data/` directory projecting `source_path`.
fn data_dir(svc: &mut FsService, source_path: &str) -> u64 {
    svc.lookup(DATA_INO, source_path)
        .unwrap_or_else(|e| panic!("lookup data/{source_path}: {e:?}"))
        .unwrap_or_else(|| panic!("data/{source_path} must be projected"))
        .ino
}

/// AC-04's move face: `mv metadata/a/tree metadata/b/tree` — with a torrent
/// directly in `tree` and a subdirectory holding another — must leave
/// `data/a/tree` gone, `data/b/tree` present with its whole subtree, and no
/// intermediate state on either side.
#[test]
fn tc_ingest_04_cross_parent_move_swaps_both_projections() {
    let _slot = wave_slot();
    let mut fixture = rig();
    let svc = &mut fixture.svc;

    let a = svc.mkdir(METADATA_INO, "a").expect("mkdir metadata/a");
    let b = svc.mkdir(METADATA_INO, "b").expect("mkdir metadata/b");
    let tree = svc.mkdir(a.ino, "tree").expect("mkdir metadata/a/tree");
    let sub = svc
        .mkdir(tree.ino, "sub")
        .expect("mkdir metadata/a/tree/sub");

    let (tree_bytes, _) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_ingest_04_tree.txt");
    let tree_info = ingest_into(svc, tree.ino, "a/tree", "seed.torrent", &tree_bytes);
    let (sub_bytes, _) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_ingest_04_sub.txt");
    ingest_into(svc, sub.ino, "a/tree/sub", "nested.torrent", &sub_bytes);

    // Precondition: the projection mirrors the metadata tree.
    let a_data = data_dir(svc, "a");
    assert_eq!(projected(svc, a_data), vec!["tree"]);
    let b_data = data_dir(svc, "b");
    assert!(projected(svc, b_data).is_empty());
    let tree_data = svc
        .lookup(a_data, "tree")
        .expect("lookup data/a/tree")
        .expect("data/a/tree must be projected")
        .ino;
    assert_eq!(projected(svc, tree_data), vec!["seed.torrent", "sub"]);

    // Failure face: a destination parent that does not exist is `ENOENT` and
    // leaves both projections untouched.
    let gone = svc
        .mkdir(METADATA_INO, "gone")
        .expect("mkdir metadata/gone");
    svc.rmdir(METADATA_INO, "gone")
        .expect("rmdir metadata/gone");
    let err = svc
        .rename(a.ino, "tree", gone.ino, "tree")
        .expect_err("moving into a directory that no longer exists must fail");
    assert_eq!(
        errno(err),
        libc::ENOENT,
        "a missing destination parent must be ENOENT"
    );
    assert_eq!(projected(svc, a_data), vec!["tree"], "failed move: source");
    assert!(
        projected(svc, b_data).is_empty(),
        "failed move: destination"
    );

    // Success face, read back with no TTL wait.
    svc.rename(a.ino, "tree", b.ino, "tree")
        .expect("cross-parent directory move must succeed");

    assert!(
        projected(svc, a_data).is_empty(),
        "the old location must stop listing the moved directory"
    );
    assert!(
        svc.lookup(a_data, "tree")
            .expect("lookup data/a/tree")
            .is_none(),
        "data/a/tree must no longer resolve"
    );
    assert_eq!(projected(svc, b_data), vec!["tree"]);

    let moved = svc
        .lookup(b_data, "tree")
        .expect("lookup data/b/tree")
        .expect("data/b/tree must be projected")
        .ino;
    assert_eq!(
        projected(svc, moved),
        vec!["seed.torrent", "sub"],
        "the moved directory keeps its own children"
    );
    let moved_sub = svc
        .lookup(moved, "sub")
        .expect("lookup data/b/tree/sub")
        .expect("data/b/tree/sub must be projected")
        .ino;
    assert_eq!(
        projected(svc, moved_sub),
        vec!["nested.torrent"],
        "the subtree moves with the directory"
    );
    let torrent_root = svc
        .lookup(moved, "seed.torrent")
        .expect("lookup data/b/tree/seed.torrent")
        .expect("the moved torrent must be projected under the new path");
    assert_eq!(torrent_root.attr.kind, FileKind::Directory);
    assert!(
        svc.lookup(torrent_root.ino, &tree_info.name())
            .expect("lookup the torrent's inner file")
            .is_some(),
        "the moved torrent's contents stay reachable under the new path"
    );
}

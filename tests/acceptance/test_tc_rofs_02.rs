//! Design acceptance case TC-rofs-02: moving an entry inside `data/` is
//! rejected (`docs/03-acceptance.md` §C AC-21, C-2.2.1, C-3.3.1).
//!
//! The FUSE adapter forwards `rename` straight to `FsService::rename` and
//! maps the domain error through `src/fuse/errno.rs`, so each assertion below
//! checks the errno the kernel would receive.  Every attempt is followed by a
//! re-read of both sides: the entry set and the attributes must be untouched.

use crate::common::{
    base, errno, ingest, listing, rig, wave_slot, UNUSED_ANNOUNCE_URL,
};
use torrentfs::fuse::fs_types::Attr;
use torrentfs::fuse::inodes::{DATA_INO, DATA_TORRENT_INO_BASE, METADATA_INO};

pub const TC_ID: &str = "TC-rofs-02";

fn assert_attr_unchanged(before: Attr, after: Attr, label: &str) {
    assert_eq!(after.ino, before.ino, "{label}: inode must not change");
    assert_eq!(after.size, before.size, "{label}: size must not change");
    assert_eq!(after.kind, before.kind, "{label}: kind must not change");
    assert_eq!(after.perm, before.perm, "{label}: mode must not change");
    assert_eq!(after.nlink, before.nlink, "{label}: nlink must not change");
}

/// AC-21's primary face: a rename whose source and target parent are both
/// `data/` directories must fail with `EROFS` (C-2.2.1) and leave both sides
/// exactly as they were.
#[test]
fn tc_rofs_02_rename_inside_data_is_erofs_and_both_sides_survive() {
    let _slot = wave_slot();
    let mut fixture = rig();
    let svc = &mut fixture.svc;
    let (torrent_bytes, content) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_rofs_02_inside.txt");
    let info = ingest(svc, "seed.torrent", &torrent_bytes);

    let torrent_root = svc
        .lookup(DATA_INO, "seed.torrent")
        .expect("lookup projected torrent root")
        .expect("data/seed.torrent must exist after ingest");
    let inner_name = info.name();
    let inner = svc
        .lookup(torrent_root.ino, &inner_name)
        .expect("lookup projected file")
        .expect("the projected torrent root must list its file");
    assert_eq!(
        inner.attr.size,
        content.len() as u64,
        "fixture precondition: projected size must be the metadata size (C-3.1.4)"
    );

    let data_before = listing(svc, DATA_INO);
    let seed_before = listing(svc, torrent_root.ino);
    let data_root_attr_before = svc.getattr(DATA_INO).expect("getattr data/");
    let torrent_root_attr_before = svc.getattr(torrent_root.ino).expect("getattr torrent root");
    let inner_attr_before = svc.getattr(inner.ino).expect("getattr projected file");

    // Rename the projected torrent directory within data/.
    let err = svc
        .rename(DATA_INO, "seed.torrent", DATA_INO, "moved.torrent")
        .expect_err("renaming a projected directory inside data/ must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "data/ rename must be EROFS (C-2.2.1 / C-3.3.1)"
    );

    // Rename the projected file within its torrent root.
    let err = svc
        .rename(torrent_root.ino, &inner_name, torrent_root.ino, "renamed.txt")
        .expect_err("renaming a projected file inside data/ must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "data/ file rename must be EROFS (C-2.2.1 / C-3.3.1)"
    );

    // The read-only guard runs before the source-existence check: a missing
    // source inside data/ is still EROFS, never ENOENT (C-3.3.1).
    let err = svc
        .rename(DATA_INO, "absent.torrent", DATA_INO, "renamed.torrent")
        .expect_err("rename inside data/ must be rejected regardless of source existence");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "the data/ guard must precede the existence check (C-3.3.1)"
    );

    assert_eq!(
        listing(svc, DATA_INO),
        data_before,
        "data/ entry set changed after rejected renames"
    );
    assert_eq!(
        listing(svc, torrent_root.ino),
        seed_before,
        "torrent root entry set changed after rejected renames"
    );
    assert_attr_unchanged(
        data_root_attr_before,
        svc.getattr(DATA_INO).expect("getattr data/"),
        "data/",
    );
    assert_attr_unchanged(
        torrent_root_attr_before,
        svc.getattr(torrent_root.ino).expect("getattr torrent root"),
        "data/seed.torrent",
    );
    assert_attr_unchanged(
        inner_attr_before,
        svc.getattr(inner.ino).expect("getattr projected file"),
        "data/seed.torrent/inner file",
    );

    // Neither the new names nor a disappearance: both sides keep their names.
    assert!(
        svc.lookup(DATA_INO, "seed.torrent")
            .expect("lookup seed.torrent")
            .is_some(),
        "the source directory name must survive a rejected rename"
    );
    assert!(
        svc.lookup(DATA_INO, "moved.torrent")
            .expect("lookup moved.torrent")
            .is_none(),
        "no destination name may appear for a rejected rename"
    );
    assert!(
        svc.lookup(torrent_root.ino, &inner_name)
            .expect("lookup inner file")
            .is_some(),
        "the source file name must survive a rejected rename"
    );
    assert!(
        svc.lookup(torrent_root.ino, "renamed.txt")
            .expect("lookup renamed.txt")
            .is_none(),
        "no destination file name may appear for a rejected rename"
    );
}

/// AC-21's boundary face: a rename whose source or target parent is `data/`
/// must be refused with `EROFS` (C-3.3.1 covers both sides), leaving the
/// metadata entry and the projection untouched.
#[test]
fn tc_rofs_02_move_across_the_data_boundary_is_erofs() {
    let _slot = wave_slot();
    let mut fixture = rig();
    let svc = &mut fixture.svc;
    let (torrent_bytes, _content) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_rofs_02_boundary.txt");
    ingest(svc, "seed.torrent", &torrent_bytes);

    let torrent_root = svc
        .lookup(DATA_INO, "seed.torrent")
        .expect("lookup projected torrent root")
        .expect("data/seed.torrent must exist after ingest");

    // A real metadata-side entry to move.  It is left unreleased so it stays a
    // metadata inode (no DB row, no projection) and the rename is the only
    // thing that could relocate it.
    let other = svc
        .create(METADATA_INO, "other.torrent")
        .expect("create metadata entry");
    svc.write(other.attr.ino, 0, &torrent_bytes)
        .expect("write metadata entry");

    let metadata_before = listing(svc, METADATA_INO);
    let data_before = listing(svc, DATA_INO);
    let other_attr_before = svc.getattr(other.attr.ino).expect("getattr other.torrent");
    let torrent_root_attr_before = svc.getattr(torrent_root.ino).expect("getattr torrent root");

    // metadata/ → data/: the target parent is read-only.
    let err = svc
        .rename(METADATA_INO, "other.torrent", DATA_INO, "landed.torrent")
        .expect_err("moving a metadata entry into data/ must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "a target parent inside data/ must be EROFS (C-3.3.1)"
    );

    // data/ → metadata/: the source parent is read-only.
    let err = svc
        .rename(DATA_INO, "seed.torrent", METADATA_INO, "out.torrent")
        .expect_err("moving a projected entry out of data/ must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "a source parent inside data/ must be EROFS (C-3.3.1)"
    );

    // Same precedence rule at the boundary: the read-only parent decides, so a
    // missing source name still reports EROFS rather than ENOENT.
    let err = svc
        .rename(METADATA_INO, "missing.torrent", DATA_INO, "landed.torrent")
        .expect_err("a move into data/ must be rejected regardless of source existence");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "the data/ guard must precede the existence check (C-3.3.1)"
    );

    assert_eq!(
        listing(svc, METADATA_INO),
        metadata_before,
        "metadata/ entry set changed after rejected moves"
    );
    assert_eq!(
        listing(svc, DATA_INO),
        data_before,
        "data/ entry set changed after rejected moves"
    );
    assert_attr_unchanged(
        other_attr_before,
        svc.getattr(other.attr.ino).expect("getattr other.torrent"),
        "metadata/other.torrent",
    );
    assert_attr_unchanged(
        torrent_root_attr_before,
        svc.getattr(torrent_root.ino).expect("getattr torrent root"),
        "data/seed.torrent",
    );

    assert!(
        svc.lookup(METADATA_INO, "other.torrent")
            .expect("lookup other.torrent")
            .is_some(),
        "the metadata source entry must survive a rejected move"
    );
    assert!(
        svc.lookup(DATA_INO, "landed.torrent")
            .expect("lookup landed.torrent")
            .is_none(),
        "no destination name may appear inside data/"
    );
    assert!(
        svc.lookup(DATA_INO, "seed.torrent")
            .expect("lookup seed.torrent")
            .is_some(),
        "the projected entry must survive a rejected move out of data/"
    );
    assert!(
        svc.lookup(METADATA_INO, "out.torrent")
            .expect("lookup out.torrent")
            .is_none(),
        "no destination name may appear inside metadata/"
    );
}

/// C-3.3.1's ordering face: a parent inode that falls in the `data/` range but
/// was never materialised must still decide `EROFS` before the parent-existence
/// check.  Every parent in the two cases above is materialised, so hoisting
/// `parent_exists` above the read-only guard would keep them green while the
/// kernel received `ENOENT` — these two cases make that regression visible.
#[test]
fn tc_rofs_02_unmaterialized_data_parent_still_decides_erofs() {
    let _slot = wave_slot();
    let mut fixture = rig();
    let svc = &mut fixture.svc;
    let (torrent_bytes, _content) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_rofs_02_stale_parent.txt");
    ingest(svc, "seed.torrent", &torrent_bytes);

    let other = svc
        .create(METADATA_INO, "other.torrent")
        .expect("create metadata entry");
    svc.write(other.attr.ino, 0, &torrent_bytes)
        .expect("write metadata entry");

    // A data-range inode no lookup ever produced: absent from `data_inodes`
    // and without a DB row behind its id.
    let stale = DATA_TORRENT_INO_BASE + 500_000;
    assert_eq!(
        errno(
            svc.getattr(stale)
                .expect_err("an unmaterialised data inode must not resolve")
        ),
        libc::ENOENT,
        "precondition: the stale parent must be absent from the namespace"
    );

    let data_before = listing(svc, DATA_INO);
    let metadata_before = listing(svc, METADATA_INO);
    let other_attr_before = svc.getattr(other.attr.ino).expect("getattr other.torrent");

    // Source side: the source parent is an unmaterialised data-range inode.
    let err = svc
        .rename(stale, "moved.torrent", DATA_INO, "landed.torrent")
        .expect_err("a rename whose source parent is in the data range must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "an unmaterialised data-range source parent must be EROFS, not ENOENT (C-3.3.1)"
    );

    // Target side: the destination parent is an unmaterialised data-range inode.
    let err = svc
        .rename(METADATA_INO, "other.torrent", stale, "landed.torrent")
        .expect_err("a rename whose target parent is in the data range must fail");
    assert_eq!(
        errno(err),
        libc::EROFS,
        "an unmaterialised data-range target parent must be EROFS, not ENOENT (C-3.3.1)"
    );

    // Contrast: the same missing parent outside the data range is ENOENT, so
    // these cases discriminate the guard's placement, not a blanket rule that
    // every missing parent is EROFS (C-3.3.2).
    let missing_metadata_parent = DATA_TORRENT_INO_BASE - 1;
    let err = svc
        .rename(missing_metadata_parent, "other.torrent", METADATA_INO, "kept.torrent")
        .expect_err("a rename from a missing non-data parent must fail");
    assert_eq!(
        errno(err),
        libc::ENOENT,
        "a missing parent outside data/ stays ENOENT (C-3.3.2)"
    );

    assert_eq!(
        listing(svc, DATA_INO),
        data_before,
        "data/ entry set changed after rejected renames"
    );
    assert_eq!(
        listing(svc, METADATA_INO),
        metadata_before,
        "metadata/ entry set changed after rejected renames"
    );
    assert_attr_unchanged(
        other_attr_before,
        svc.getattr(other.attr.ino).expect("getattr other.torrent"),
        "metadata/other.torrent",
    );
    assert!(
        svc.lookup(METADATA_INO, "other.torrent")
            .expect("lookup other.torrent")
            .is_some(),
        "the metadata source entry must survive rejected renames"
    );
    assert!(
        svc.lookup(DATA_INO, "landed.torrent")
            .expect("lookup landed.torrent")
            .is_none(),
        "no destination name may appear inside data/"
    );
    assert!(
        svc.lookup(METADATA_INO, "kept.torrent")
            .expect("lookup kept.torrent")
            .is_none(),
        "no destination name may appear inside metadata/"
    );
    assert!(
        svc.lookup(DATA_INO, "seed.torrent")
            .expect("lookup seed.torrent")
            .is_some(),
        "the projected entry must survive rejected renames"
    );
}

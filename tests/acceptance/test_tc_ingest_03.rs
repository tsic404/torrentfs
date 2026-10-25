//! Design acceptance case TC-ingest-03: renaming a seed under `metadata/`
//! renames its `data/` projection, deleting it makes the projection vanish,
//! and renaming a directory moves the projections of every seed below it
//! (`docs/03-acceptance.md` §A AC-03, contracts C-3.3.6 / C-1.3.4).
//!
//! Every assertion runs immediately after the mutation — no sleep, no TTL
//! grace — so a projection that only converges when the kernel's 1s entry
//! cache expires fails the case.  Kernel dentry invalidation itself is
//! adapter-side and not observable without a mount.

use crate::common::{create_single_piece_torrent, wave_slot, IngestHarness};
use torrentfs::fuse::inodes::METADATA_INO;

pub const TC_ID: &str = "TC-ingest-03";

/// `mv` a seed inside `metadata/`: exit 0, old projection gone, new projection
/// present.  `rm` the seed: exit 0, projection gone.
#[test]
fn tc_ingest_03_rename_and_delete_sync_the_projection() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let announce = "http://127.0.0.1:9/announce";
    let (seed, _) = create_single_piece_torrent(announce, "ingest-03.bin");

    let library = ingest.mkdir(&["library"]);
    ingest.copy_torrent(library, "first.torrent", &seed);
    assert!(
        ingest
            .data_lookup(&["library", "first.torrent", "ingest-03.bin"])
            .is_some(),
        "the freshly copied seed must be projected before the rename"
    );

    ingest
        .service()
        .rename(library, "first.torrent", library, "renamed.torrent")
        .expect("renaming a metadata seed must succeed");

    assert!(
        ingest.data_lookup(&["library", "first.torrent"]).is_none(),
        "the old projection name must disappear at once"
    );
    assert!(
        ingest
            .data_lookup(&["library", "renamed.torrent", "ingest-03.bin"])
            .is_some(),
        "the renamed seed must project at its new name"
    );
    let listing = ingest.children(library);
    assert!(
        listing.iter().any(|entry| entry.name == "renamed.torrent"),
        "the listing must show the new name"
    );
    assert!(
        !listing.iter().any(|entry| entry.name == "first.torrent"),
        "the listing must not keep the old name"
    );

    ingest
        .service()
        .unlink(library, "renamed.torrent")
        .expect("deleting a metadata seed must succeed")
        .expect("the deleted seed had a content row");

    assert!(
        ingest
            .data_lookup(&["library", "renamed.torrent"])
            .is_none(),
        "the projection must vanish after the delete"
    );
    let listing = ingest.children(library);
    assert!(
        !listing.iter().any(|entry| entry.name == "renamed.torrent"),
        "the listing must not keep the deleted seed"
    );
}

/// `mv` a `metadata/` directory: every seed below it projects at the new path
/// immediately, and no stale path survives.
#[test]
fn tc_ingest_03_directory_rename_moves_descendant_projections() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let announce = "http://127.0.0.1:9/announce";
    let (outer, _) = create_single_piece_torrent(announce, "ingest-03-outer.bin");
    let (inner, _) = create_single_piece_torrent(announce, "ingest-03-inner.bin");

    let group = ingest.mkdir(&["group"]);
    let sub = ingest.mkdir(&["group", "sub"]);
    ingest.copy_torrent(group, "outer.torrent", &outer);
    ingest.copy_torrent(sub, "inner.torrent", &inner);

    ingest
        .service()
        .rename(METADATA_INO, "group", METADATA_INO, "moved")
        .expect("renaming a metadata directory must succeed");

    assert!(
        ingest.data_lookup(&["group"]).is_none(),
        "the old directory projection must disappear"
    );
    assert!(
        ingest
            .data_lookup(&["group", "sub", "inner.torrent"])
            .is_none(),
        "a descendant seed must not stay projected under the old path"
    );
    assert!(
        ingest.data_lookup(&["moved"]).is_some(),
        "the renamed directory must project under the new path"
    );
    assert!(
        ingest
            .data_lookup(&["moved", "outer.torrent", "ingest-03-outer.bin"])
            .is_some(),
        "a seed directly below the renamed directory must follow it"
    );
    assert!(
        ingest
            .data_lookup(&["moved", "sub", "inner.torrent", "ingest-03-inner.bin"])
            .is_some(),
        "a seed in a nested directory must follow the rename"
    );
}

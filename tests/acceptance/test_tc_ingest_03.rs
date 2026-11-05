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

/// Renaming a directory whose name is non-ASCII must rewrite the descendant
/// directory paths and source paths by character position, not by byte
/// length — and the corrected paths must still be there after a remount
/// (`C-3.1.5`, `C-3.3.6`).
#[test]
fn tc_ingest_03_non_ascii_directory_rename_rewrites_descendants() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::with_file_db();
    let announce = "http://127.0.0.1:9/announce";
    let (inner, _) = create_single_piece_torrent(announce, "ingest-03-unicode.bin");

    let group = ingest.mkdir(&["电影"]);
    let sub = ingest.mkdir(&["电影", "子目录"]);
    ingest.copy_torrent(sub, "inner.torrent", &inner);

    ingest
        .service()
        .rename(METADATA_INO, "电影", METADATA_INO, "films")
        .expect("renaming a non-ASCII directory must succeed");

    assert!(
        ingest.data_lookup(&["电影"]).is_none(),
        "the old non-ASCII projection must disappear"
    );
    assert!(
        ingest
            .data_lookup(&["films", "子目录", "inner.torrent", "ingest-03-unicode.bin"])
            .is_some(),
        "the descendant seed must project under the renamed path with its suffix intact"
    );

    let mut remounted = ingest.remount();
    assert!(
        remounted.data_lookup(&["电影"]).is_none(),
        "the old path must stay gone after a remount"
    );
    assert!(
        remounted
            .data_lookup(&["films", "子目录", "inner.torrent", "ingest-03-unicode.bin"])
            .is_some(),
        "the character-based path rewrite must survive a remount"
    );
}

/// Renaming `movies` must not rewrite a sibling `Movies/` whose path only
/// matches case-insensitively (`C-3.3.6`).
#[test]
fn tc_ingest_03_case_variant_sibling_survives_directory_rename() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::with_file_db();
    let announce = "http://127.0.0.1:9/announce";
    let (lower, _) = create_single_piece_torrent(announce, "ingest-03-lower.bin");
    let (upper, _) = create_single_piece_torrent(announce, "ingest-03-upper.bin");

    let lower_dir = ingest.mkdir(&["movies"]);
    ingest.copy_torrent(lower_dir, "lower.torrent", &lower);
    let upper_dir = ingest.mkdir(&["Movies"]);
    ingest.copy_torrent(upper_dir, "upper.torrent", &upper);

    ingest
        .service()
        .rename(METADATA_INO, "movies", METADATA_INO, "films")
        .expect("renaming the lower-case directory must succeed");

    assert!(
        ingest
            .data_lookup(&["films", "lower.torrent", "ingest-03-lower.bin"])
            .is_some(),
        "the renamed directory's seed must follow it"
    );
    assert!(
        ingest
            .data_lookup(&["Movies", "upper.torrent", "ingest-03-upper.bin"])
            .is_some(),
        "the case-variant sibling must keep its own path"
    );

    let mut remounted = ingest.remount();
    assert!(
        remounted
            .data_lookup(&["Movies", "upper.torrent", "ingest-03-upper.bin"])
            .is_some(),
        "the case-variant sibling must keep its path after a remount"
    );
    assert!(
        remounted
            .data_lookup(&["films", "lower.torrent", "ingest-03-lower.bin"])
            .is_some(),
        "the renamed seed must stay renamed after a remount"
    );
}

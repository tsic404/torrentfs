//! Design acceptance case TC-ingest-04: a directory moved across parents keeps
//! both `data/` projections correct — the old location stops listing it, the
//! new one starts (`docs/03-acceptance.md` §A AC-04, C-3.3.6).
//!
//! The seed-move faces of the same AC: a torrent moved across `metadata/`
//! directories updates both projections; a move whose destination parent does
//! not exist fails non-zero (`ENOENT`) and leaves the projection untouched; an
//! overwrite move replaces an existing destination's content record
//! atomically (C-3.3.2 / C-3.3.5).  The overwrite case builds a real
//! destination first and asserts the post-state carries exactly one source
//! row — no half commit.
//!
//! Driving `FsService` directly is the shipped path: the FUSE adapter
//! forwards `rename` to it and maps the domain error through
//! `src/fuse/errno.rs`, so the assertions below are the entries the kernel
//! sees, read back immediately — the fix must not depend on the 1s entry TTL
//! expiring.

use crate::common::{
    base, create_single_piece_torrent, errno, ingest_into, listing, rig, wave_slot, IngestHarness,
    UNUSED_ANNOUNCE_URL,
};
use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::{FsService, TorrentInfo};

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

/// A successful cross-directory move: old projection gone, new one present.
#[test]
fn tc_ingest_04_cross_directory_move_updates_both_sides() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let announce = "http://127.0.0.1:9/announce";
    let (seed, _) = create_single_piece_torrent(announce, "ingest-04.bin");

    let src = ingest.mkdir(&["src"]);
    let dst = ingest.mkdir(&["dst"]);
    ingest.copy_torrent(src, "seed.torrent", &seed);
    assert!(
        ingest
            .data_lookup(&["src", "seed.torrent", "ingest-04.bin"])
            .is_some(),
        "the seed must project under its source directory first"
    );

    ingest
        .service()
        .rename(src, "seed.torrent", dst, "seed.torrent")
        .expect("a cross-directory move must succeed");

    assert!(
        ingest.data_lookup(&["src", "seed.torrent"]).is_none(),
        "the old position must disappear"
    );
    assert!(
        ingest
            .data_lookup(&["dst", "seed.torrent", "ingest-04.bin"])
            .is_some(),
        "the new position must appear with its entries"
    );
    assert!(
        ingest
            .service()
            .lookup(src, "seed.torrent")
            .unwrap()
            .is_none(),
        "metadata/ must not keep the source name"
    );
    assert!(
        ingest
            .service()
            .lookup(dst, "seed.torrent")
            .unwrap()
            .is_some(),
        "metadata/ must show the destination name"
    );
}

/// Moving into a directory that no longer exists — the kernel can forward a
/// stale parent inode — fails with `ENOENT` and changes nothing.
#[test]
fn tc_ingest_04_move_into_missing_parent_fails_without_projection_change() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let announce = "http://127.0.0.1:9/announce";
    let (seed, _) = create_single_piece_torrent(announce, "ingest-04-missing.bin");

    let src = ingest.mkdir(&["src"]);
    let dst = ingest.mkdir(&["dst"]);
    ingest.copy_torrent(src, "seed.torrent", &seed);

    // Create then remove the destination directory so its inode is one the
    // daemon knows about but that no longer exists.
    let gone = ingest.mkdir(&["gone"]);
    ingest
        .service()
        .rmdir(METADATA_INO, "gone")
        .expect("removing the empty directory must succeed");

    let before = ingest.data_snapshot();
    let err = ingest
        .service()
        .rename(src, "seed.torrent", gone, "seed.torrent")
        .expect_err("a move into a missing parent must fail");
    assert_eq!(
        libc::c_int::from(err),
        libc::ENOENT,
        "a missing destination parent must report ENOENT"
    );

    assert_eq!(
        ingest.data_snapshot(),
        before,
        "a failed move must not change the projection"
    );
    assert!(
        ingest
            .service()
            .lookup(src, "seed.torrent")
            .expect("lookup source")
            .is_some(),
        "the source name must be retained"
    );
    assert!(
        ingest
            .service()
            .lookup(dst, "seed.torrent")
            .expect("lookup destination")
            .is_none(),
        "the destination must not gain the name"
    );
}

/// Overwriting an existing destination seed: the destination projection is
/// replaced by the source's content, its old content row is gone from the
/// content table, and the move committed exactly once (one row at the
/// destination, none at the source).
#[test]
fn tc_ingest_04_overwrite_move_replaces_the_destination_atomically() {
    let _slot = wave_slot();
    // File-backed so the content layer (`torrents`) can be queried directly.
    let mut ingest = IngestHarness::with_file_db();
    let announce = "http://127.0.0.1:9/announce";
    let (source_bytes, _) = create_single_piece_torrent(announce, "ingest-04-source.bin");
    let (target_bytes, _) = create_single_piece_torrent(announce, "ingest-04-target.bin");
    let source_hash = info_hash(&source_bytes);
    let target_hash = info_hash(&target_bytes);

    let src = ingest.mkdir(&["a"]);
    let dst = ingest.mkdir(&["b"]);
    ingest.copy_torrent(src, "x.torrent", &source_bytes);
    ingest.copy_torrent(dst, "x.torrent", &target_bytes);
    assert!(
        ingest
            .data_lookup(&["b", "x.torrent", "ingest-04-target.bin"])
            .is_some(),
        "the destination must project its own content before the overwrite"
    );

    ingest
        .service()
        .rename(src, "x.torrent", dst, "x.torrent")
        .expect("an overwrite rename over an existing destination must succeed");

    // Source name is gone on both sides.
    assert!(
        ingest.data_lookup(&["a", "x.torrent"]).is_none(),
        "the source position must disappear"
    );
    assert!(
        ingest.service().lookup(src, "x.torrent").unwrap().is_none(),
        "metadata/ must not keep the source name"
    );
    // The destination now projects the source content, and the replaced
    // content's entries are gone.
    let root = ingest
        .data_lookup(&["b", "x.torrent"])
        .expect("the destination must keep projecting at its own name");
    assert!(
        ingest
            .data_lookup(&["b", "x.torrent", "ingest-04-source.bin"])
            .is_some(),
        "the destination projection must carry the source's file"
    );
    assert!(
        ingest
            .data_lookup(&["b", "x.torrent", "ingest-04-target.bin"])
            .is_none(),
        "the replaced destination content must be cleaned from the projection"
    );
    let stats = ingest.stats_text(root);
    assert!(
        stats.contains("torrent: ingest-04-source.bin"),
        "the destination root must report the source content: {stats}"
    );

    // No half commit: exactly one source row for this name, at the
    // destination, and the replaced content record is gone with no orphan.
    let rows: Vec<_> = ingest
        .torrents()
        .into_iter()
        .filter(|torrent| torrent.filename == "x.torrent")
        .collect();
    assert_eq!(rows.len(), 1, "the move must commit exactly one row");
    assert_eq!(
        rows[0].source_path, "b",
        "the surviving row must sit at the destination"
    );
    let source_content = ingest.content_ids(&source_hash);
    assert_eq!(
        source_content.len(),
        1,
        "the source's content record must survive the move"
    );
    assert_eq!(
        rows[0].torrent_id,
        *source_content.iter().next().expect("source content id"),
        "the surviving row must carry the source's content"
    );
    assert!(
        ingest.content_ids(&target_hash).is_empty(),
        "no source row may keep referencing the replaced content"
    );
    // Content layer: the source row's deletion alone would leave the
    // replaced content row orphaned and still pass the join above.
    assert_eq!(
        ingest.content_rows(&target_hash),
        0,
        "the replaced content row must be deleted from the content table"
    );
    assert_eq!(
        ingest.content_rows(&source_hash),
        1,
        "the moved source content row must remain in the content table"
    );
}

fn info_hash(bytes: &[u8]) -> String {
    hex::encode(
        TorrentInfo::from_bytes(bytes.to_vec())
            .expect("parse fixture torrent")
            .info_hash()
            .expect("info hash"),
    )
}

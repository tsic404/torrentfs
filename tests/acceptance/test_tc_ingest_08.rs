//! Design acceptance case TC-ingest-08: a multi-file seed projects the name
//! its metadata declares as the seed root's only first level, and each file's
//! `path` unfolds level by level beneath it (`docs/03-acceptance.md` §A AC-08,
//! contracts C-3.1.3).
//!
//! The carrier drives the service calls the FUSE adapter forwards: a copy
//! (create → write → flush → release) ingests the seed, `readdir` renders the
//! projected tree, `getattr` renders each entry's kind, permission bits and
//! size, and `lookup` resolves every declared path by name.

use crate::common::{
    build_multi_file_torrent_with_content, wave_slot, IngestHarness, UNUSED_ANNOUNCE_URL,
};
use std::collections::BTreeMap;
use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::TorrentInfo;

pub const TC_ID: &str = "TC-ingest-08";

const SEED_FILE: &str = "multi.torrent";
const SEED_NAME: &str = "ingest-08-tree";
const PIECE_LEN: usize = 16 * 1024;

/// The declared file tree unfolds under the declared name: the seed root's
/// first level is exactly that name, every listed path equals «declared name +
/// file `path`» walking down its directories, directories are `0555` and files
/// are `0444` regular files carrying the metadata-declared size.
#[test]
fn tc_ingest_08_multi_file_tree_follows_the_declared_paths() {
    let _slot = wave_slot();

    // Three files over two embedded directory levels.  TC-read-11 declares the
    // same relative path shape with its own seed name and sizes; this carrier
    // judges the projection only, so its sizes carry no piece geometry.
    let clip = vec![b'a'; 20 * 1024];
    let part = vec![b'b'; 12 * 1024];
    let tail = vec![b'c'; 8 * 1024];
    let files = [
        ("clip.bin", clip.as_slice()),
        ("sub/part.bin", part.as_slice()),
        ("sub/deep/tail.bin", tail.as_slice()),
    ];
    let (torrent_bytes, _) = build_multi_file_torrent_with_content(
        UNUSED_ANNOUNCE_URL,
        SEED_NAME,
        &files,
        PIECE_LEN,
    );

    // The tree the projection must render, stated as the design states it:
    // the declared name joined with each declared file `path`, in the
    // declaration order the metadata keeps.
    let declared: Vec<(String, u64)> = files
        .iter()
        .map(|(path, content)| (format!("{SEED_NAME}/{path}"), content.len() as u64))
        .collect();

    // The metadata must declare exactly that tree — otherwise the fixture, not
    // the projection, would be what this case measures.
    let info = TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse the fixture torrent");
    assert_eq!(info.name(), SEED_NAME, "the fixture must declare its name");
    let parsed: Vec<(String, u64)> = info
        .files()
        .expect("declared file list")
        .into_iter()
        .map(|file| (file.path, file.size))
        .collect();
    assert_eq!(
        parsed, declared,
        "the fixture metadata must declare exactly the tree under test"
    );

    let mut ingest = IngestHarness::new();
    ingest.copy_torrent(METADATA_INO, SEED_FILE, &torrent_bytes);

    // The seed root is a directory named after the full `.torrent` filename,
    // and its only first-level entry is the declared name (C-3.1.3).
    let seed_root = ingest
        .data_lookup(&[SEED_FILE])
        .expect("data/multi.torrent must resolve");
    assert_eq!(
        ingest
            .service()
            .getattr(seed_root)
            .expect("getattr the seed root")
            .kind,
        FileKind::Directory,
        "the projected seed root must be a directory"
    );
    let first_level: Vec<String> = {
        let mut names: Vec<String> = ingest
            .children(seed_root)
            .into_iter()
            // The virtual `.stats` file is observability, not a projection of
            // a declared entry (C-2.1.4).
            .filter(|entry| entry.name != ".stats")
            .map(|entry| entry.name)
            .collect();
        names.sort();
        names
    };
    assert_eq!(
        first_level,
        vec![SEED_NAME.to_string()],
        "the seed root's first level must be the declared name and nothing else"
    );

    // Every listed entry equals the declared tree — no extra level, no missing
    // level, no flattened file name.
    let mut actual: Vec<(String, FileKind)> = ingest
        .data_snapshot()
        .into_iter()
        .map(|entry| (entry.path, entry.kind))
        .collect();
    actual.sort_by(|a, b| a.0.cmp(&b.0));
    let mut expected: Vec<(String, FileKind)> = vec![(SEED_FILE.to_string(), FileKind::Directory)];
    for (declared_path, _) in &declared {
        // Each directory level between the seed root and the file.
        let components: Vec<&str> = declared_path.split('/').collect();
        for depth in 1..components.len() {
            let directory = format!("{SEED_FILE}/{}", components[..depth].join("/"));
            if !expected.iter().any(|(path, _)| path == &directory) {
                expected.push((directory, FileKind::Directory));
            }
        }
        expected.push((
            format!("{SEED_FILE}/{declared_path}"),
            FileKind::RegularFile,
        ));
    }
    expected.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(
        actual, expected,
        "data/ must render exactly «declared name + file path» for a multi-file seed"
    );

    // Permissions and sizes are checked per entry: directories `0555`, files
    // `0444` regular files of the metadata-declared size.
    let declared_sizes: BTreeMap<String, u64> = declared
        .iter()
        .map(|(path, size)| (format!("{SEED_FILE}/{path}"), *size))
        .collect();
    for entry in ingest.data_snapshot() {
        match entry.kind {
            FileKind::Directory => assert_eq!(
                entry.perm, 0o555,
                "projected directory data/{} must be 0555",
                entry.path
            ),
            FileKind::RegularFile => {
                assert_eq!(
                    entry.perm, 0o444,
                    "projected file data/{} must be 0444",
                    entry.path
                );
                assert_eq!(
                    entry.size,
                    declared_sizes[&entry.path],
                    "projected file data/{} must carry the declared full size",
                    entry.path
                );
            }
        }
    }

    // Name resolution walks the same tree: every declared path, including the
    // nested one, resolves through `lookup` component by component.
    for (declared_path, size) in &declared {
        let parts: Vec<&str> = std::iter::once(SEED_FILE)
            .chain(declared_path.split('/'))
            .collect();
        let ino = ingest
            .data_lookup(&parts)
            .unwrap_or_else(|| panic!("data/{} must resolve by name", parts.join("/")));
        let attr = ingest
            .service()
            .getattr(ino)
            .unwrap_or_else(|e| panic!("getattr data/{} failed: {:?}", parts.join("/"), e));
        assert_eq!(attr.kind, FileKind::RegularFile);
        assert_eq!(attr.size, *size);
    }

    // The projection sits at the data/ root for a metadata/ root seed, so the
    // seed root is a child of DATA_INO, not a subsection.
    assert!(
        ingest
            .children(DATA_INO)
            .iter()
            .any(|entry| entry.name == SEED_FILE),
        "a metadata/ root seed must project into the data/ root"
    );
}

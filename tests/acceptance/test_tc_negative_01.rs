//! Design acceptance case TC-negative-01: the `data/` namespace never accepts
//! a mutation, every attempt fails with `EROFS`, and the rejected attempts
//! leave the projection's entry set and attributes untouched
//! (`docs/03-acceptance.md` §J AC-90; `docs/02-contracts.md` §C-2.2.1).
//!
//! The FUSE adapter forwards each kernel operation to the matching
//! `FsService` method and translates its domain error through the single
//! `fuse::errno` exit point.  This module drives those methods directly, so
//! it can assert both the domain variant and the errno the kernel would see.
//! The mutation classes are the exhaustive F1 list: create, write, mkdir,
//! rmdir, unlink, rename (into, out of and within `data/`), attribute change,
//! truncate and symlink.

use std::path::Path;

use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::{DATA_INO, METADATA_INO};
use torrentfs::fuse::FsService;
use torrentfs::FsError;

use crate::common::{
    build_multi_file_torrent, ingest_torrent, readdir_from, service_with_state, wave_slot,
};

pub const TC_ID: &str = "TC-negative-01";

/// A fixture holding one root-level single-piece torrent and one multi-file
/// torrent under a `metadata/` subdirectory, so the projection covers a
/// source-path directory, a torrent root, an internal directory and files.
fn fixture() -> (FsService, tempfile::TempDir) {
    let (mut service, cache) = service_with_state();

    let (alpha, _) = crate::common::base::create_single_piece_torrent(
        "http://127.0.0.1:1/announce",
        "negative-01-alpha.txt",
    );
    ingest_torrent(&mut service, METADATA_INO, "alpha.torrent", &alpha);

    let sub = service
        .mkdir(METADATA_INO, "sub")
        .expect("create metadata subdirectory")
        .ino;
    let bravo = build_multi_file_torrent(
        "http://127.0.0.1:1/announce",
        "negative-01-bravo",
        &[("inner/deep.bin", 8), ("top.bin", 4)],
    );
    ingest_torrent(&mut service, sub, "bravo.torrent", &bravo);

    (service, cache)
}

/// Assert the operation failed with the read-only domain error and that the
/// kernel-visible errno is `EROFS` — not `ENOENT`, `EPERM` or `EACCES`.
fn assert_erofs<T>(label: &str, result: Result<T, FsError>) {
    let err = match result {
        Ok(_) => panic!("{label}: data/ mutation must fail, but it succeeded"),
        Err(e) => e,
    };
    assert_eq!(
        err,
        FsError::ReadOnlyFileSystem,
        "{label}: must report the read-only domain error"
    );
    let errno: libc::c_int = err.into();
    assert_eq!(errno, libc::EROFS, "{label}: the kernel must see EROFS");
}

/// One projected entry with the attributes a rejected mutation must not move.
#[derive(Debug, PartialEq, Eq)]
struct ProjectionRow {
    path: String,
    ino: u64,
    offset: i64,
    kind: FileKind,
    perm: u16,
    size: u64,
}

/// Snapshot a namespace recursively (`.`/`..` and the virtual `.stats` are
/// excluded: `.stats` is explicitly outside AC-90's "no trace" rule).
fn snapshot(service: &mut FsService, root: u64) -> Vec<ProjectionRow> {
    fn walk(service: &mut FsService, ino: u64, prefix: &str, out: &mut Vec<ProjectionRow>) {
        let mut entries = readdir_from(service, ino, -1);
        entries.sort_by(|a, b| a.name.cmp(&b.name));
        for entry in entries {
            if entry.name == "." || entry.name == ".." || entry.name == ".stats" {
                continue;
            }
            let attr = service
                .getattr(entry.ino)
                .unwrap_or_else(|e| panic!("attrs for {}: {e:?}", entry.name));
            let path = format!("{prefix}/{}", entry.name);
            out.push(ProjectionRow {
                path: path.clone(),
                ino: entry.ino,
                offset: entry.offset,
                kind: entry.kind,
                perm: attr.perm,
                size: attr.size,
            });
            if entry.kind == FileKind::Directory {
                walk(service, entry.ino, &path, out);
            }
        }
    }

    let mut rows = Vec::new();
    walk(service, root, "", &mut rows);
    rows
}

/// Every mutation class on `data/` fails with `EROFS`: the namespace stays
/// unwritable at every level, and the rejection is decided by the namespace
/// (not by missing entries), which is why the attempts target both live and
/// absent names.
#[test]
fn tc_negative_01_every_data_mutation_returns_erofs() {
    let _slot = wave_slot();
    let (mut service, _cache) = fixture();

    let alpha_root = service
        .lookup(DATA_INO, "alpha.torrent")
        .expect("lookup root torrent")
        .expect("root torrent must be projected");
    let alpha_file = service
        .lookup(alpha_root.ino, "negative-01-alpha.txt")
        .expect("lookup projected file")
        .expect("projected file must exist");

    let source_dir = service
        .lookup(DATA_INO, "sub")
        .expect("lookup source-path dir")
        .expect("source-path dir must be projected");
    let bravo_root = service
        .lookup(source_dir.ino, "bravo.torrent")
        .expect("lookup nested torrent")
        .expect("nested torrent must be projected");
    // libtorrent's file paths for a multi-file torrent start with the
    // torrent's `name` component, so the projection carries that directory
    // before the entry's own `inner/` path.
    let bravo_name_dir = service
        .lookup(bravo_root.ino, "negative-01-bravo")
        .expect("lookup torrent name dir")
        .expect("torrent name dir must exist");
    let inner_dir = service
        .lookup(bravo_name_dir.ino, "inner")
        .expect("lookup torrent-internal dir")
        .expect("torrent-internal dir must exist");
    let deep_file = service
        .lookup(inner_dir.ino, "deep.bin")
        .expect("lookup torrent-internal file")
        .expect("torrent-internal file must exist");

    // Creation: files and directories, at the data root, under a source-path
    // directory and inside a torrent.
    assert_erofs("create at data root", service.create(DATA_INO, "new.torrent"));
    assert_erofs("mknod at data root", service.mknod(DATA_INO, "node.torrent"));
    assert_erofs("mkdir at data root", service.mkdir(DATA_INO, "newdir"));
    assert_erofs(
        "mkdir under source-path dir",
        service.mkdir(source_dir.ino, "nested"),
    );
    assert_erofs(
        "create inside torrent root",
        service.create(alpha_root.ino, "child.torrent"),
    );

    // Writes: a projected data file and the torrent's internal directory.
    assert_erofs("write projected file", service.write(alpha_file.ino, 0, b"x"));
    assert_erofs("write projected dir", service.write(alpha_root.ino, 0, b"x"));
    assert_erofs(
        "write torrent-internal file",
        service.write(deep_file.ino, 0, b"x"),
    );

    // Attribute changes and truncation.
    assert_erofs(
        "chmod projected file",
        service.setattr(alpha_file.ino, None),
    );
    assert_erofs(
        "truncate projected file",
        service.setattr(alpha_file.ino, Some(0)),
    );
    assert_erofs("chmod projected dir", service.setattr(alpha_root.ino, None));

    // Removal, at every level.
    assert_erofs(
        "unlink projected root",
        service.unlink(DATA_INO, "alpha.torrent"),
    );
    assert_erofs(
        "unlink projected file",
        service.unlink(alpha_root.ino, "negative-01-alpha.txt"),
    );
    assert_erofs(
        "unlink nested projected root",
        service.unlink(source_dir.ino, "bravo.torrent"),
    );
    assert_erofs(
        "rmdir projected root",
        service.rmdir(DATA_INO, "alpha.torrent"),
    );
    assert_erofs(
        "rmdir source-path dir",
        service.rmdir(DATA_INO, "sub"),
    );

    // Rename: within data/, out of data/ and into data/.
    assert_erofs(
        "rename within data",
        service.rename(
            DATA_INO,
            "alpha.torrent",
            DATA_INO,
            "renamed.torrent",
        ),
    );
    assert_erofs(
        "rename out of data",
        service.rename(
            DATA_INO,
            "alpha.torrent",
            METADATA_INO,
            "escaped.torrent",
        ),
    );
    assert_erofs(
        "rename into data",
        service.rename(
            METADATA_INO,
            "alpha.torrent",
            DATA_INO,
            "imported.torrent",
        ),
    );
    assert_erofs(
        "rename projected file",
        service.rename(
            alpha_root.ino,
            "negative-01-alpha.txt",
            alpha_root.ino,
            "renamed.txt",
        ),
    );

    // Symlinks are unsupported everywhere; in data/ the namespace itself
    // decides, so the answer must be EROFS rather than EPERM.
    for (label, ino) in [
        ("symlink at data root", DATA_INO),
        ("symlink inside torrent", alpha_root.ino),
        ("symlink on projected file", alpha_file.ino),
    ] {
        let err = service.symlink(ino);
        assert_eq!(
            err,
            FsError::ReadOnlyFileSystem,
            "{label}: must report the read-only domain error"
        );
        assert_eq!(
            libc::c_int::from(err),
            libc::EROFS,
            "{label}: the kernel must see EROFS"
        );
    }

    if let Some(download) = &service.download_service {
        download.shutdown();
    }
}

/// Recursive path set of the on-disk state root (the piece cache):
/// `(relative path, is_directory)`.  AC-90's "no trace" clause covers cache
/// directories and hidden files, so a rejected mutation must not add either.
fn state_tree(root: &Path) -> Vec<(String, bool)> {
    fn walk(root: &Path, dir: &Path, out: &mut Vec<(String, bool)>) {
        let mut entries: Vec<_> = std::fs::read_dir(dir)
            .unwrap_or_else(|e| panic!("read state dir {}: {e}", dir.display()))
            .filter_map(|entry| entry.ok())
            .collect();
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let relative = path
                .strip_prefix(root)
                .unwrap_or(&path)
                .to_string_lossy()
                .into_owned();
            let metadata = entry
                .metadata()
                .unwrap_or_else(|e| panic!("stat state entry {}: {e}", path.display()));
            out.push((relative, metadata.is_dir()));
            if metadata.is_dir() {
                walk(root, &path, out);
            }
        }
    }

    let mut entries = Vec::new();
    walk(root, root, &mut entries);
    entries
}

/// The rejected attempts leave no trace: the `data/` listing (names, inode
/// numbers, offsets, kinds, permissions, sizes), the `metadata/` tree and the
/// on-disk state directory are the same before and after, no hidden entry
/// appears, and the projection still resolves by name.
#[test]
fn tc_negative_01_rejected_mutations_leave_the_projection_unchanged() {
    let _slot = wave_slot();
    let (mut service, cache) = fixture();

    let alpha_root = service
        .lookup(DATA_INO, "alpha.torrent")
        .expect("lookup root torrent")
        .expect("root torrent must be projected");
    let alpha_file = service
        .lookup(alpha_root.ino, "negative-01-alpha.txt")
        .expect("lookup projected file")
        .expect("projected file must exist");

    let data_before = snapshot(&mut service, DATA_INO);
    let metadata_before = snapshot(&mut service, METADATA_INO);
    let cache_before = state_tree(cache.path());
    assert!(
        !data_before.is_empty(),
        "the fixture must project at least one torrent"
    );

    // A representative attempt from each mutation class.  Each one is
    // rejected, so none of them may reach persistence or the inode tables.
    let _ = service.create(DATA_INO, "ghost.torrent");
    let _ = service.mkdir(DATA_INO, "ghostdir");
    let _ = service.write(alpha_file.ino, 0, b"tampered");
    let _ = service.setattr(alpha_file.ino, Some(0));
    let _ = service.setattr(alpha_file.ino, None);
    let _ = service.unlink(alpha_root.ino, "negative-01-alpha.txt");
    let _ = service.unlink(DATA_INO, "alpha.torrent");
    let _ = service.rename(DATA_INO, "alpha.torrent", METADATA_INO, "ghost.torrent");
    let _ = service.rename(METADATA_INO, "alpha.torrent", DATA_INO, "ghost.torrent");
    let _ = service.symlink(DATA_INO);

    // The kernel may still ask for the rejected names; they must not resolve.
    for name in ["ghost.torrent", "ghostdir"] {
        assert!(
            service
                .lookup(DATA_INO, name)
                .unwrap_or_else(|e| panic!("lookup {name}: {e:?}"))
                .is_none(),
            "rejected {name} must not leave a projection entry"
        );
    }

    assert_eq!(
        snapshot(&mut service, DATA_INO),
        data_before,
        "the data/ projection (entries and attributes) must be unchanged"
    );
    assert_eq!(
        snapshot(&mut service, METADATA_INO),
        metadata_before,
        "rejected data/ attempts must not touch the metadata/ tree"
    );
    assert_eq!(
        state_tree(cache.path()),
        cache_before,
        "rejected data/ attempts must not create cache directories or scratch files"
    );

    // No hidden entry (a `.stats` is the one allowed virtual file).
    for row in &data_before {
        let name = row.path.rsplit('/').next().unwrap_or(&row.path);
        assert!(
            !name.starts_with('.'),
            "data/ must not carry hidden entries, found {:?}",
            row.path
        );
    }

    // The projection still resolves and exposes its documented attributes.
    let file = service
        .lookup(alpha_root.ino, "negative-01-alpha.txt")
        .expect("projection must still resolve the file")
        .expect("projected file must survive the rejected attempts");
    let attr = service.getattr(file.ino).expect("projected file attrs");
    assert_eq!(attr.perm, 0o444, "projected files stay 0444");
    let recorded = data_before
        .iter()
        .find(|row| row.path == "/alpha.torrent/negative-01-alpha.txt")
        .expect("the snapshot must carry the projected file");
    assert_eq!(
        (attr.ino, attr.size),
        (recorded.ino, recorded.size),
        "the projected file's identity and size must not move"
    );

    if let Some(download) = &service.download_service {
        download.shutdown();
    }
}

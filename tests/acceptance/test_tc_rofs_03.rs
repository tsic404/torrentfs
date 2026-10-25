//! Design acceptance case TC-rofs-03: the three `.stats` layers are read-only
//! (`docs/03-acceptance.md` §C AC-22, C-2.2.2, C-6.1.4).
//!
//! Write, delete and truncate are attempted on the root, `data/` and torrent
//! `.stats` files; each must fail with `EPERM` and the attributes must stay
//! `0444` regular file.  `FsService` results go through the single errno exit
//! (`src/fuse/errno.rs`), so the assertions are the errno the kernel receives.

use crate::common::{
    base, errno, ingest, rig, root_stats_without_uptime, wave_slot, UNUSED_ANNOUNCE_URL,
};
use torrentfs::fuse::fs_types::{FileKind, StatsKind};
use torrentfs::fuse::inodes::{InodeManager, DATA_INO, ROOT_INO};
use torrentfs::FsService;

pub const TC_ID: &str = "TC-rofs-03";

/// C-6.1.4's attribute invariant: `0444` regular file, still resolvable under
/// its parent's `.stats` name.
fn assert_stats_attr(svc: &mut FsService, label: &str, parent: u64, ino: u64) {
    let attr = svc
        .getattr(ino)
        .unwrap_or_else(|e| panic!("{label}: getattr must succeed: {e:?}"));
    assert_eq!(
        attr.kind,
        FileKind::RegularFile,
        "{label}: .stats must be a regular file (C-6.1.4)"
    );
    assert_eq!(
        attr.perm,
        0o444,
        "{label}: .stats mode must stay 0444 (C-6.1.4)"
    );

    let entry = svc
        .lookup(parent, ".stats")
        .unwrap_or_else(|e| panic!("{label}: lookup must succeed: {e:?}"))
        .unwrap_or_else(|| panic!("{label}: .stats must still exist"));
    assert_eq!(entry.ino, ino, "{label}: .stats inode identity must stay");
}

/// AC-22's per-layer face: write, delete and truncate must all report `EPERM`,
/// and the attributes must be unchanged after each rejection.
fn assert_mutations_rejected(svc: &mut FsService, label: &str, parent: u64, ino: u64) {
    let err = svc
        .write(ino, 0, b"x")
        .expect_err("write to .stats must fail");
    assert_eq!(
        errno(err),
        libc::EPERM,
        "{label}: write must be EPERM (C-2.2.2 / C-2.3.2)"
    );
    assert_stats_attr(svc, label, parent, ino);

    let err = svc
        .unlink(parent, ".stats")
        .expect_err("unlink of .stats must fail");
    assert_eq!(
        errno(err),
        libc::EPERM,
        "{label}: delete must be EPERM (C-2.2.2 / C-2.3.2)"
    );
    assert_stats_attr(svc, label, parent, ino);

    // A pure truncate is a size-only SETATTR; both the O_TRUNC (0) and a
    // non-zero target must be refused (C-2.2.2 "改属性（含纯截断）").
    for target in [0u64, 1] {
        let err = svc
            .setattr(ino, Some(target))
            .expect_err("truncate of .stats must fail");
        assert_eq!(
            errno(err),
            libc::EPERM,
            "{label}: truncate to {target} must be EPERM (C-2.2.2 / C-2.3.2)"
        );
        assert_stats_attr(svc, label, parent, ino);
    }
}

/// Each of the three layers — root, `data/` directory, torrent root — rejects
/// write/delete/truncate with `EPERM`, keeps its `0444` regular-file
/// attributes, and renders the same bytes before and after.
#[test]
fn tc_rofs_03_three_layer_stats_reject_mutations_with_eperm() {
    let _slot = wave_slot();
    let mut fixture = rig();
    let svc = &mut fixture.svc;
    let (torrent_bytes, _content) =
        base::create_single_piece_torrent(UNUSED_ANNOUNCE_URL, "tc_rofs_03.txt");
    ingest(svc, "seed.torrent", &torrent_bytes);

    let torrent_root = svc
        .lookup(DATA_INO, "seed.torrent")
        .expect("lookup projected torrent root")
        .expect("data/seed.torrent must exist after ingest");

    let root_stats = svc
        .lookup(ROOT_INO, ".stats")
        .expect("lookup root .stats")
        .expect("the mount root must carry .stats");
    let dir_stats = svc
        .lookup(DATA_INO, ".stats")
        .expect("lookup data/ .stats")
        .expect("data/ must carry .stats");
    let torrent_stats = svc
        .lookup(torrent_root.ino, ".stats")
        .expect("lookup torrent .stats")
        .expect("the torrent root must carry .stats");

    // The derived stats inodes also satisfy the data-inode range predicate, so
    // EPERM (not EROFS) below proves the stats guard runs before the data/ one
    // (C-2.2.2 ordering); the assertion pins that precondition.
    assert!(
        InodeManager::is_stats_ino(dir_stats.ino),
        "data/.stats must be a derived stats inode"
    );
    assert!(
        InodeManager::is_stats_ino(torrent_stats.ino),
        "torrent .stats must be a derived stats inode"
    );

    // A rejection must not change what a reader observes: snapshot each layer
    // before the first attempt and compare after the last.  The root file's
    // `Uptime:` line advances with the wall clock (not with any mutation), so
    // it is masked; every other byte must match, and the two derived layers
    // are compared whole.
    let root_before = root_stats_without_uptime(svc);
    let dir_before = svc.read_stats(StatsKind::StatsInode { ino: dir_stats.ino });
    let torrent_before = svc.read_stats(StatsKind::StatsInode {
        ino: torrent_stats.ino,
    });

    assert_mutations_rejected(svc, "/.stats", ROOT_INO, root_stats.ino);
    assert_mutations_rejected(svc, "/data/.stats", DATA_INO, dir_stats.ino);
    assert_mutations_rejected(
        svc,
        "/data/seed.torrent/.stats",
        torrent_root.ino,
        torrent_stats.ino,
    );

    assert_eq!(
        root_stats_without_uptime(svc),
        root_before,
        "/.stats content changed after rejected mutations (uptime masked)"
    );
    assert_eq!(
        svc.read_stats(StatsKind::StatsInode { ino: dir_stats.ino }),
        dir_before,
        "/data/.stats content changed after rejected mutations"
    );
    assert_eq!(
        svc.read_stats(StatsKind::StatsInode {
            ino: torrent_stats.ino
        }),
        torrent_before,
        "/data/seed.torrent/.stats content changed after rejected mutations"
    );
}

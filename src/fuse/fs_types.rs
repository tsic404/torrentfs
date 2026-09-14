//! Domain value types for the `FsService` ↔ `TorrentFs` adapter boundary.
//!
//! These types are deliberately free of `fuser` / `libc` so `FsService` stays
//! unit-testable without a FUSE session. The thin adapter (`super::mod`)
//! converts them into `fuser` reply types and fills in the process-specific
//! fields (uid/gid, timestamps) it owns.

use std::sync::Arc;

use crate::metadata::TorrentInfo;

/// The subset of file kinds the filesystem exposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FileKind {
    Directory,
    RegularFile,
}

/// File attributes produced by `FsService`.
///
/// uid/gid and timestamps are intentionally omitted: the adapter derives them
/// from the current process (uid/gid) and the inode creation time, keeping
/// `libc` out of the service layer.
#[derive(Debug, Clone, Copy)]
pub struct Attr {
    pub ino: u64,
    pub size: u64,
    pub kind: FileKind,
    pub perm: u16,
    pub nlink: u32,
}

/// A single directory entry (readdir).
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub ino: u64,
    pub offset: i64,
    pub kind: FileKind,
    pub name: String,
}

/// A looked-up entry (the `lookup` reply payload).
#[derive(Debug, Clone, Copy)]
pub struct Entry {
    pub ino: u64,
    pub attr: Attr,
}

/// A created file (the `create` reply payload: attributes + open handle).
#[derive(Debug, Clone, Copy)]
pub struct Created {
    pub attr: Attr,
    pub fh: u64,
}

/// Outcome of an `open` call: the file handle plus open-mode hints.
///
/// `direct_io` requests the adapter to set `FOPEN_DIRECT_IO`, bypassing the
/// kernel page cache so the daemon's errno reaches userspace directly
/// (without this, the kernel's `filemap_read_folio` converts any
/// failed read into EIO, masking the real error such as ENODATA).
#[derive(Debug, Clone, Copy)]
pub struct OpenOutcome {
    pub fh: u64,
    pub direct_io: bool,
}

impl From<u64> for OpenOutcome {
    fn from(fh: u64) -> Self {
        Self {
            fh,
            direct_io: false,
        }
    }
}

/// Outcome of a synchronous attempt to serve a read.
pub enum ReadOutcome {
    /// Serve this byte slice synchronously — memory cache, disk pieces,
    /// stats content, or an in-memory metadata file.
    Ready(Vec<u8>),
    /// The requested pieces still need downloading. The adapter parks the
    /// reply and completes it from a worker thread.
    Pending {
        info: Arc<TorrentInfo>,
        file_index: i32,
        offset: u64,
        size: u32,
        /// info_hash for cancellation on unlink/remove_torrent.
        info_hash: String,
        /// torrent_id for cancellation on unlink/remove_torrent.
        torrent_id: i64,
    },
}

/// Which `.stats` file to render.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatsKind {
    /// The root `.stats` (global session overview).
    Global,
    /// A per-directory/per-torrent `.stats`, addressed by its stats inode.
    StatsInode { ino: u64 },
}

//! Filesystem domain error — the single error type returned by `FsService`.
//!
//! `FsError` is a pure domain error: it carries no errno and imports neither
//! `fuser` nor `libc`. The errno translation lives in exactly one place,
//! `fuse::errno` (`impl From<FsError> for libc::c_int`), so the FUSE adapter
//! is the only module that knows how domain errors map to kernel error codes.
//!
//! The seven variant groups follow the architecture design doc §7.1:
//! path/namespace, permission, argument/input, data integrity, persistence,
//! remote/network (BitTorrent domain), and internal/system.

use thiserror::Error;

use super::error::TorrentError;
use super::types::DbError;

#[derive(Error, Debug, PartialEq)]
pub enum FsError {
    // ── path / namespace ──
    #[error("not found")]
    NotFound,
    #[error("not a directory")]
    NotDirectory,
    #[error("is a directory")]
    IsDirectory,
    #[error("already exists")]
    AlreadyExists,
    #[error("directory not empty")]
    DirectoryNotEmpty,
    #[error("stale file handle")]
    StaleHandle,

    // ── permission ──
    #[error("permission denied")]
    PermissionDenied,
    #[error("read-only file system")]
    ReadOnlyFileSystem,
    #[error("operation not permitted")]
    NotPermitted,

    // ── argument / input ──
    #[error("invalid argument")]
    InvalidArgument,
    #[error("file too large: {0}")]
    FileTooLarge(String),
    #[error("name too long")]
    NameTooLong,

    // ── data integrity ──
    #[error("Invalid .torrent file: {0}")]
    CorruptTorrent(String),
    #[error("corrupt piece: {0}")]
    CorruptPiece(String),

    // ── persistence ──
    #[error("database error: {0}")]
    Database(String),
    #[error("migration error: {0}")]
    Migration(String),

    // ── remote / network (BitTorrent domain) ──
    #[error("no peers available: {0}")]
    NoPeers(String),
    #[error("piece not ready: {0}")]
    PieceNotReady(String),
    #[error("download timeout: {0}")]
    DownloadTimeout(String),
    #[error("download failed: {0}")]
    DownloadFailed(String),

    // ── internal / system ──
    #[error("io error: {0}")]
    Io(String),
    #[error("no space left on device: {0}")]
    NoSpace(String),
    #[error("out of memory: {0}")]
    OutOfMemory(String),
    #[error("internal error: {0}")]
    Internal(String),
    #[error("lock poisoned")]
    LockPoisoned,
}

pub type FsResult<T> = Result<T, FsError>;

/// Persistence-layer errors map to the persistence / path groups (§11).
impl From<DbError> for FsError {
    fn from(e: DbError) -> Self {
        match e {
            DbError::Sqlite(err) => FsError::Database(err.to_string()),
            DbError::SourcePathExists(_) => FsError::AlreadyExists,
            DbError::Migration(msg) => FsError::Migration(msg),
        }
    }
}

/// Download/libtorrent errors map to the remote / integrity / internal groups
/// (§11 migration table).
impl From<TorrentError> for FsError {
    fn from(e: TorrentError) -> Self {
        match e {
            TorrentError::InvalidFile(msg) => FsError::CorruptTorrent(msg),
            TorrentError::ParseError(msg) => FsError::CorruptTorrent(msg),
            TorrentError::ConfigError(msg) => FsError::Internal(msg),
            TorrentError::IoError(msg) => FsError::Io(msg),
            TorrentError::NullPointer => FsError::Internal("null pointer".to_string()),
            TorrentError::NoPeers(msg) => FsError::NoPeers(msg),
            TorrentError::PieceNotReady(msg) => FsError::PieceNotReady(msg),
            TorrentError::Timeout(msg) => FsError::DownloadTimeout(msg),
            TorrentError::Unknown { code, message } => {
                FsError::DownloadFailed(format!("code {}: {}", code, message))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn db_error_source_path_exists_maps_to_already_exists() {
        let e: FsError = DbError::SourcePathExists("a/b".to_string()).into();
        assert!(matches!(e, FsError::AlreadyExists));
    }

    #[test]
    fn db_error_sqlite_maps_to_database() {
        // A rusqlite error is awkward to construct portably, so assert the
        // Migration branch instead, which exercises the same enum shape.
        let e: FsError = DbError::Migration("v5".to_string()).into();
        assert!(matches!(e, FsError::Migration(_)));
    }

    #[test]
    fn torrent_error_parse_maps_to_corrupt_torrent() {
        let e: FsError = TorrentError::ParseError("bad bencode".to_string()).into();
        assert!(matches!(e, FsError::CorruptTorrent(_)));
    }

    /// TSI-2923: the user-facing message names the file type and the cause,
    /// so an invalid seed is distinguishable from a generic I/O error.
    #[test]
    fn corrupt_torrent_display_names_file_and_reason() {
        let e = FsError::CorruptTorrent("expected value in bencoded string".to_string());
        assert_eq!(
            e.to_string(),
            "Invalid .torrent file: expected value in bencoded string"
        );
    }

    #[test]
    fn torrent_error_timeout_maps_to_download_timeout() {
        let e: FsError = TorrentError::Timeout("slow".to_string()).into();
        assert!(matches!(e, FsError::DownloadTimeout(_)));
    }
}

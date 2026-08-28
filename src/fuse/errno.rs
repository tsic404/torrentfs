//! Single errno mapping exit point: `FsError` → `libc::c_int`.
//!
//! This is the ONLY place in the codebase that translates a domain `FsError`
//! into an errno. See the architecture design doc §7.2 for the full table.

use libc::{
    EACCES, EEXIST, EFBIG, EINVAL, EIO, EISDIR, ENAMETOOLONG, ENODATA, ENOENT, ENOMEM, ENOSPC,
    ENOTDIR, ENOTEMPTY, EPERM, EROFS, ESTALE,
};

use crate::domain::fs_error::FsError;

impl From<FsError> for libc::c_int {
    fn from(e: FsError) -> Self {
        match e {
            // ── path / namespace ──
            FsError::NotFound => ENOENT,
            FsError::NotDirectory => ENOTDIR,
            FsError::IsDirectory => EISDIR,
            FsError::AlreadyExists => EEXIST,
            FsError::DirectoryNotEmpty => ENOTEMPTY,
            FsError::StaleHandle => ESTALE,
            // ── permission ──
            FsError::PermissionDenied => EACCES,
            FsError::ReadOnlyFileSystem => EROFS,
            FsError::NotPermitted => EPERM,
            // ── argument / input ──
            FsError::InvalidArgument => EINVAL,
            FsError::FileTooLarge(_) => EFBIG,
            FsError::NameTooLong => ENAMETOOLONG,
            // ── data integrity ──
            FsError::CorruptTorrent(_) => EINVAL,
            FsError::CorruptPiece(_) => EIO,
            // ── persistence ──
            FsError::Database(_) | FsError::Migration(_) => EIO,
            // ── remote / network (BitTorrent domain) ──
            // TSI-2246/TSI-2483: NoPeers and DownloadTimeout map to ENODATA
            // ("No data available") so the user sees a meaningful error
            // ("no available seeder" / "waiting for a seeder timed out")
            // instead of the generic EIO ("Input/output error") when the
            // swarm has no seeder. Other download errors (failure, corrupt
            // piece) still map to EIO.
            FsError::NoPeers(_) | FsError::DownloadTimeout(_) => ENODATA,
            FsError::PieceNotReady(_) | FsError::DownloadFailed(_) => EIO,
            // ── internal / system ──
            FsError::Io(_) => EIO,
            FsError::NoSpace(_) => ENOSPC,
            FsError::OutOfMemory(_) => ENOMEM,
            FsError::Internal(_) | FsError::LockPoisoned => EIO,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_peers_maps_to_enodata_not_eio() {
        let e: libc::c_int = FsError::NoPeers("no seeder".to_string()).into();
        assert_eq!(e, libc::ENODATA);
        assert_ne!(e, libc::EIO);
    }

    /// TSI-2483: `DownloadTimeout` ("waiting for a seeder timed out") maps to
    /// ENODATA so a read that blocks out the piece-wait limit surfaces as
    /// "no data available", not the generic EIO.
    #[test]
    fn download_timeout_maps_to_enodata_not_eio() {
        let e: libc::c_int = FsError::DownloadTimeout("slow".to_string()).into();
        assert_eq!(e, libc::ENODATA);
        assert_ne!(e, libc::EIO);
    }

    #[test]
    fn download_failed_still_maps_to_eio() {
        let e: libc::c_int = FsError::DownloadFailed("boom".to_string()).into();
        assert_eq!(e, libc::EIO);
    }

    #[test]
    fn piece_not_ready_still_maps_to_eio() {
        let e: libc::c_int = FsError::PieceNotReady("waiting".to_string()).into();
        assert_eq!(e, libc::EIO);
    }

    /// TSI-2261: when the piece-wait times out with zero seeders, the engine
    /// returns `NoPeers` (not `Timeout`). This must still map to ENODATA, not
    /// EIO, so the user sees "no data available" instead of "I/O error".
    #[test]
    fn no_seeder_timeout_maps_to_enodata() {
        let e: libc::c_int = FsError::NoPeers(
            "No seeder connected for info_hash abc after 30s. \
             The torrent has no available seeder — check tracker health or \
             try again later."
                .to_string(),
        )
        .into();
        assert_eq!(e, libc::ENODATA);
        assert_ne!(e, libc::EIO);
    }
}

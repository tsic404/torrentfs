//! Domain layer — core domain types, errors, and repository traits.
//!
//! This module contains the pure domain model with no infrastructure dependencies.
//! Sub-modules:
//! - `types` — domain data types (Torrent, TorrentFile, etc.)
//! - `error` — domain error types (TorrentError, TorrentResult)
//! - `repository` — repository traits (TorrentRepository, FileRepository)
//! - `time` — deadline arithmetic that saturates instead of panicking

pub mod error;
pub mod fs_error;
pub mod repository;
pub mod time;
pub mod types;

pub use error::{TorrentError, TorrentResult};
pub use fs_error::{FsError, FsResult};
pub use repository::{FileRepository, TorrentRepository};
pub use time::deadline_after;
pub use types::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};

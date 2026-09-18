// Re-export domain types for backward compatibility.
// The canonical definitions now live in crate::domain::types.
pub use crate::domain::types::{
    DbError, FileEntry, InsertTorrentResult, MoveOverwriteResult, Torrent, TorrentDirectory,
    TorrentFile, TorrentStatus,
};

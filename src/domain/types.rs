//! Core domain data types — pure data models with no infrastructure dependencies.

#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum DbError {
    #[error("database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("torrent with source_path already exists: {0}")]
    SourcePathExists(String),
    #[error("migration error: {0}")]
    Migration(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum TorrentStatus {
    Pending,
    Downloading,
    Seeding,
    Error,
}

impl TorrentStatus {
    #[allow(dead_code)]
    pub fn as_str(&self) -> &'static str {
        match self {
            TorrentStatus::Pending => "pending",
            TorrentStatus::Downloading => "downloading",
            TorrentStatus::Seeding => "seeding",
            TorrentStatus::Error => "error",
        }
    }
}

impl From<&str> for TorrentStatus {
    fn from(s: &str) -> Self {
        match s {
            "downloading" => TorrentStatus::Downloading,
            "seeding" => TorrentStatus::Seeding,
            "error" => TorrentStatus::Error,
            _ => TorrentStatus::Pending,
        }
    }
}

impl From<String> for TorrentStatus {
    fn from(s: String) -> Self {
        TorrentStatus::from(s.as_str())
    }
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct Torrent {
    pub id: i64,
    pub source_path: String,
    pub name: String,
    pub filename: String,
    pub total_size: i64,
    pub info_hash: String,
    pub file_count: i64,
    pub status: TorrentStatus,
    pub torrent_data: Option<Vec<u8>>,
    pub resume_data: Option<Vec<u8>>,
    pub created_at: String,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentFile {
    pub id: i64,
    pub torrent_id: i64,
    pub directory_id: Option<i64>,
    pub name: String,
    pub path: String,
    pub size: i64,
    pub first_piece: i64,
    pub last_piece: i64,
    pub piece_start: Option<i64>,
    pub piece_end: Option<i64>,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentDirectory {
    pub id: i64,
    pub torrent_id: i64,
    pub parent_id: Option<i64>,
    pub name: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum InsertTorrentResult {
    Inserted(i64),
    Duplicate(i64),
}

/// Outcome of atomically moving a torrent over an existing destination.
#[derive(Debug, Clone, PartialEq)]
pub enum MoveOverwriteResult {
    /// The source row was absent — nothing was changed (a pending add may have
    /// failed to persist). The destination must be preserved.
    SourceAbsent,
    /// The source row was re-pointed at the destination. `removed_target` is
    /// `Some((id, info_hash))` when a destination row was deleted in the same
    /// transaction; `None` when the destination had no persisted row.
    Moved {
        removed_target: Option<(i64, String)>,
    },
}

pub struct FileEntry {
    pub path: String,
    pub size: i64,
}

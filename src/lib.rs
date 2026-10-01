pub mod domain;
pub mod error;
pub mod fuse;
pub mod infrastructure;
pub mod services;

// Re-exports for backward compatibility — old paths still work
// infrastructure layer
pub use infrastructure::db::{
    DbError, FileEntry, InsertTorrentResult, Torrent, TorrentDirectory, TorrentFile, TorrentStatus,
};
pub use infrastructure::{
    cache, config, db, download, metadata, CacheManager, Database, DownloadEngine,
    DownloadTorrentStatus, FilePieceInfo, PieceMetadata, PiecePriorityConfig, PieceScheduler,
    PieceStatus, PieceStore, Session, TorrentHandle, TorrentInfo, TorrentState, TorrentfsConfig,
    TrackerEntry,
};

// domain layer re-exports
pub use domain::{
    DbError as DomainDbError, FileEntry as DomainFileEntry, FileRepository,
    InsertTorrentResult as DomainInsertResult, Torrent as DomainTorrent,
    TorrentDirectory as DomainTorrentDirectory, TorrentFile as DomainTorrentFile,
    TorrentRepository, TorrentStatus as DomainTorrentStatus,
};

// other re-exports
pub use domain::fs_error::{FsError, FsResult};
pub use error::{is_transient_read_error, TorrentError, TorrentResult};
pub use fuse::{FsService, TorrentFs};
pub use services::download::DownloadService;

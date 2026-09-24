//! Download module — libtorrent session management and piece download orchestration.
//!
//! Split from the original monolithic `download.rs` into focused sub-modules:
//! - `types` — data types (SessionStats, TorrentStatus, TorrentState, FilePieceInfo)
//! - `session` — Session and TorrentHandle wrappers around libtorrent FFI
//! - `piece_store` — the piece data plane (disk cache access)
//! - `piece_scheduler` — the piece control plane (priority gradient + events)
//! - `engine` — the DownloadEngine actor (single-owner-thread session + handles)

mod engine;
mod piece_scheduler;
mod piece_store;
mod session;
mod types;

pub use engine::{
    Command as DownloadCommand, DownloadEngine, DownloadSnapshot, NO_SEEDER_READ_TIMEOUT_SECS,
    PEER_WAIT_CAP_SECS,
};
pub use piece_scheduler::{PiecePriorityConfig, PieceScheduler, PieceStatus};
pub use piece_store::PieceStore;
pub use session::{Session, TorrentHandle};
pub use types::{FilePieceInfo, SessionStats, TorrentState, TorrentStatus};

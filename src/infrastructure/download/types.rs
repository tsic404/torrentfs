/// Statistics collected from the libtorrent session.
#[derive(Debug, Clone, Default)]
pub struct SessionStats {
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_downloaded: i64,
    pub total_uploaded: i64,
    pub dht_nodes: i32,
    pub peers_connected: i32,
    pub half_open_connections: i32,
}

/// Snapshot of a single torrent's status from libtorrent.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentStatus {
    pub state: TorrentState,
    pub progress: f32,
    pub total_done: u64,
    pub total: u64,
    pub download_rate: i64,
    pub upload_rate: i64,
    pub total_download: i64,
    pub total_upload: i64,
    pub num_peers: i32,
    pub num_seeds: i32,
}

/// libtorrent torrent state enum.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TorrentState {
    QueuedForChecking,
    CheckingFiles,
    DownloadingMetadata,
    Downloading,
    Finished,
    Seeding,
    Allocating,
    CheckingResumeData,
    Unknown,
}

impl From<i32> for TorrentState {
    fn from(value: i32) -> Self {
        match value {
            0 => TorrentState::QueuedForChecking,
            1 => TorrentState::CheckingFiles,
            2 => TorrentState::DownloadingMetadata,
            3 => TorrentState::Downloading,
            4 => TorrentState::Finished,
            5 => TorrentState::Seeding,
            6 => TorrentState::Allocating,
            7 => TorrentState::CheckingResumeData,
            _ => TorrentState::Unknown,
        }
    }
}

/// Piece-range information for a single file within a torrent.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FilePieceInfo {
    pub first_piece: i64,
    pub num_pieces: i64,
    pub file_offset: i64,
}

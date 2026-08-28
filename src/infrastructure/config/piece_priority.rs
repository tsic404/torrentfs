use serde::{Deserialize, Serialize};

// ============================================================
// Piece priority gradient (torrentfs-internal, NOT a libtorrent
// settings_pack value — no WriteJson impl).
// ============================================================

/// TOML representation of the `[piece_priority]` config section.
///
/// Controls the per-torrent piece priority gradient used for selective
/// (on-demand) piece download.  All fields are optional; missing values fall
/// back to [`crate::infrastructure::download::PiecePriorityConfig::default`]
/// when converted via `PiecePriorityConfig::from_toml`.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PiecePriorityToml {
    /// Access window in MiB (default 4096 = 4 GB).  Pieces within this window
    /// of an active read are "wanted"; everything outside stays at 0.
    pub access_window_mb: Option<u32>,
    /// Priority for pieces inside the current read range (default 7).
    pub current_priority: Option<i32>,
    /// Step priorities for pieces at distance 1..=4 past the current range.
    pub step_priorities: Option<[i32; 4]>,
    /// Priority at the far forward edge of the access window (default 1).
    pub window_edge_priority: Option<i32>,
    /// Priority for pieces beyond the access window (default 0 = not wanted).
    pub rest_priority: Option<i32>,
    /// Priority for pieces before the current read offset but still within
    /// the access window (default 1); pieces further back are 0.
    pub backward_priority: Option<i32>,
}

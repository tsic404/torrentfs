use serde::{Deserialize, Serialize};

/// Torrentfs-level concurrency limits (TSI-2144).
///
/// These bound the number of OS threads used by the FUSE download worker pool
/// and the capacity of its submission queue. Unlike the libtorrent settings
/// sections (which implement `WriteJson`), this section is deliberately absent
/// from `to_settings_json()` — it is a torrentfs-level knob, not a libtorrent
/// `settings_pack` key.
#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ConcurrencyConfig {
    /// Number of download worker threads in the bounded pool. When unset,
    /// defaults to the number of logical CPUs (at least 1).
    pub download_workers: Option<usize>,
    /// Capacity of the bounded download submission queue. When unset,
    /// defaults to 256.
    pub download_queue_depth: Option<usize>,
}

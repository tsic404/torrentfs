use serde::{Deserialize, Serialize};

/// Torrentfs-level concurrency limits — the number of OS threads in the FUSE
/// download worker pool and its submission-queue capacity. Unlike the
/// libtorrent settings sections (which implement `WriteJson`), this section is
/// absent from `to_settings_json()`: it is a torrentfs-level knob, not a
/// libtorrent `settings_pack` key.
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

/// Upper bound on download worker threads.
///
/// Each worker is a dedicated OS thread and a slot in the bounded pool, so an
/// absurd value fails thread creation at startup. The ceiling is far above any
/// useful pool while keeping startup allocation bounded.
pub const MAX_DOWNLOAD_WORKERS: usize = 1_024;

/// Upper bound on the submission-queue capacity.
///
/// The bounded channel preallocates one slot per unit of capacity, so an absurd
/// value fails allocation. The ceiling is generous for backpressure tuning and
/// still allocates in bounded space.
pub const MAX_DOWNLOAD_QUEUE_DEPTH: usize = 65_536;

impl ConcurrencyConfig {
    /// Reject the two keys above their ceilings.
    ///
    /// Zero keeps its documented meaning (use the default), and a negative
    /// value never reaches this point — it fails deserialization into `usize`.
    pub(crate) fn validate(&self) -> Result<(), String> {
        if let Some(v) = self.download_workers {
            if v > MAX_DOWNLOAD_WORKERS {
                return Err(format!(
                    "[concurrency] download_workers must be <= {}, got {}",
                    MAX_DOWNLOAD_WORKERS, v
                ));
            }
        }
        if let Some(v) = self.download_queue_depth {
            if v > MAX_DOWNLOAD_QUEUE_DEPTH {
                return Err(format!(
                    "[concurrency] download_queue_depth must be <= {}, got {}",
                    MAX_DOWNLOAD_QUEUE_DEPTH, v
                ));
            }
        }
        Ok(())
    }
}

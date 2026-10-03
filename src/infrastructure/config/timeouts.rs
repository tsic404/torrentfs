use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_int;

// ============================================================
// Timeouts
// ============================================================

/// Default FUSE read timeout (seconds) used when `read_timeout_secs` is unset
/// or non-positive. Raised from 30s to 60s so a cold piece (e.g. piece 0 of a
/// large uncached file) has time to arrive from a healthy-but-slow swarm
/// before the read surfaces ENODATA.
pub const DEFAULT_READ_TIMEOUT_SECS: u64 = 60;

/// Default peer-discovery wait (seconds) used when
/// `peer_discovery_wait_secs` is unset or non-positive.
///
/// Every read that finds an empty swarm waits for a peer to appear before the
/// swarm is declared sourceless. The wait must outlast the tracker's first
/// announce plus the peer connect, or the first read of a fresh mount fails
/// with `NoPeers` inside that cold window (measured ~9s in a container with a
/// host-network tracker); 30s leaves room for a slower announce.
pub const DEFAULT_PEER_DISCOVERY_WAIT_SECS: u64 = 30;

/// Default piece-wait window (seconds) for a read with no connected seeder,
/// used when `no_seeder_read_timeout_secs` is unset or non-positive.
///
/// Such a read can never be served by the swarm, so it fails fast with
/// `NoPeers` after this short window rather than the full `read_timeout_secs`
/// — unless a seeder connects mid-wait and upgrades the window back to the
/// full read timeout. Raise it in a low-peer swarm where a seeder may take
/// longer than 15s to connect; lower it to fail faster.
pub const DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS: u64 = 15;

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct TimeoutsConfig {
    pub peer_timeout: Option<i64>,
    pub urlseed_timeout: Option<i64>,
    pub urlseed_pipeline_size: Option<i64>,
    pub stop_tracker_timeout: Option<i64>,
    pub tracker_completion_timeout: Option<i64>,
    pub tracker_receive_timeout: Option<i64>,
    pub inactivity_timeout: Option<i64>,
    /// Timeout in seconds for waiting on torrent state transitions and piece
    /// downloads during FUSE read operations. Defaults to
    /// [`DEFAULT_READ_TIMEOUT_SECS`] when unset or non-positive.
    /// This is a torrentfs-level timeout, not passed to libtorrent.
    pub read_timeout_secs: Option<i64>,
    /// Seconds a read may wait for a peer to appear (peer discovery) before
    /// the swarm is declared sourceless. Defaults to
    /// [`DEFAULT_PEER_DISCOVERY_WAIT_SECS`] when unset or non-positive. Like
    /// `read_timeout_secs` this is a torrentfs-level timeout, not passed to
    /// libtorrent.
    pub peer_discovery_wait_secs: Option<i64>,
    /// Hard ceiling in seconds on the peer-discovery window: however long
    /// `peer_discovery_wait_secs` asks a read to wait for a peer, it never
    /// waits past this. Unset or non-positive means no extra cap, leaving the
    /// window at `min(read_timeout_secs, peer_discovery_wait_secs)`. Lower it
    /// to fail fast in a low-peer swarm without shortening the configured
    /// discovery wait itself. A torrentfs-level timeout, not passed to
    /// libtorrent.
    pub peer_wait_cap_secs: Option<i64>,
    /// Seconds a read may wait for its pieces while no seeder is connected
    /// before failing fast with `NoPeers`. Defaults to
    /// [`DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS`] when unset or non-positive, and
    /// like the other waits is capped by `read_timeout_secs`. Raise it in a
    /// low-peer swarm where a seeder may arrive late; lower it to fail faster.
    /// A torrentfs-level timeout, not passed to libtorrent.
    pub no_seeder_read_timeout_secs: Option<i64>,
}

impl TimeoutsConfig {
    /// Resolved read timeout: the configured value when positive, otherwise
    /// [`DEFAULT_READ_TIMEOUT_SECS`].
    pub fn resolved_read_timeout_secs(&self) -> u64 {
        self.read_timeout_secs
            .filter(|&v| v > 0)
            .map(|v| v as u64)
            .unwrap_or(DEFAULT_READ_TIMEOUT_SECS)
    }

    /// Resolved peer-discovery wait: the configured value when positive,
    /// otherwise [`DEFAULT_PEER_DISCOVERY_WAIT_SECS`].
    pub fn resolved_peer_discovery_wait_secs(&self) -> u64 {
        self.peer_discovery_wait_secs
            .filter(|&v| v > 0)
            .map(|v| v as u64)
            .unwrap_or(DEFAULT_PEER_DISCOVERY_WAIT_SECS)
    }

    /// Resolved peer-wait cap: the configured ceiling when positive, `None`
    /// (no extra cap) when unset or non-positive — so raising
    /// `peer_discovery_wait_secs` alone always widens the window, and the
    /// shipped behaviour is the uncapped one.
    pub fn resolved_peer_wait_cap_secs(&self) -> Option<u64> {
        self.peer_wait_cap_secs.filter(|&v| v > 0).map(|v| v as u64)
    }

    /// Resolved no-seeder piece-wait window: the configured value when
    /// positive, otherwise [`DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS`].
    pub fn resolved_no_seeder_read_timeout_secs(&self) -> u64 {
        self.no_seeder_read_timeout_secs
            .filter(|&v| v > 0)
            .map(|v| v as u64)
            .unwrap_or(DEFAULT_NO_SEEDER_READ_TIMEOUT_SECS)
    }
}

impl WriteJson for TimeoutsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, peer_timeout);
        json_field_int!(map, self, urlseed_timeout);
        json_field_int!(map, self, urlseed_pipeline_size);
        json_field_int!(map, self, stop_tracker_timeout);
        json_field_int!(map, self, tracker_completion_timeout);
        json_field_int!(map, self, tracker_receive_timeout);
        json_field_int!(map, self, inactivity_timeout);
    }
}

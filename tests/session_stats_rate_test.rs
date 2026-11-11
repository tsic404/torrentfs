//! `.stats` global rates must follow real session traffic (C-6.2.2).
//!
//! libtorrent 2.1 publishes cumulative `net.recv_bytes` / `net.sent_bytes`
//! counters and no rate counters, so the wrapper derives a one-second rate from
//! the delta between two samples. Reading `net.recv_rate` left both rate rows at
//! 0 forever. A real swarm transfer here must surface a rate above zero; with
//! the broken metric names the shared snapshot stays at 0 for the whole run.

mod common;

use std::sync::Arc;
use std::time::{Duration, Instant};

use common::{
    acquire_session_lock, build_multipiece_torrent_pieces, local_test_config, TestHarness,
};
use torrentfs::services::download::DownloadService;
use torrentfs::TorrentInfo;

const PIECES: usize = 32; // 32 x 256 KiB = 8 MiB of payload
const PIECE_LEN: usize = 256 * 1024;
const READ_PAUSE: Duration = Duration::from_millis(150);

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
}

#[test]
fn session_stats_rate_tracks_download_traffic() {
    let _guard = acquire_session_lock();
    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_pieces(announce_url, "rate-probe", PIECES)
    });
    let total = harness.file_content.len() as u64;

    let cache = tempfile::tempdir().expect("cache dir");
    let service = Arc::new(DownloadService::new(cache.path(), &quiet_config()).expect("service"));
    let info = Arc::new(
        TorrentInfo::from_bytes(harness.torrent_data.clone()).expect("parse torrent"),
    );

    // Read one piece at a time with a pause: each read blocks until its piece
    // arrives, so the swarm transfers in bursts spread over several seconds
    // instead of one sub-second spike the one-second sampler can miss.
    let reader = {
        let service = Arc::clone(&service);
        let info = Arc::clone(&info);
        std::thread::spawn(move || {
            let mut offset = 0u64;
            while offset < total {
                let size = std::cmp::min(PIECE_LEN as u64, total - offset) as u32;
                service
                    .read_file_range_blocking(Arc::clone(&info), 0, offset, size)
                    .expect("read from swarm");
                offset += u64::from(size);
                std::thread::sleep(READ_PAUSE);
            }
        })
    };

    // Sample the shared stats until a rate appears. The engine posts a stats
    // sample while a read is parked, so the first sample lands before the
    // transfer starts and the next full one-second sample sees its bytes; a
    // nonzero rate therefore has to appear within the first couple of seconds.
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut observed = 0i64;
    while Instant::now() < deadline {
        let stats = service.snapshot_stats();
        if stats.download_rate > 0 {
            observed = stats.download_rate;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    reader.join().expect("download thread");
    assert!(
        observed > 0,
        "session download rate stayed 0 while downloading {total} bytes"
    );

    let stats = service.snapshot_stats();
    assert!(
        stats.total_downloaded > 0,
        "cumulative download bytes stayed 0"
    );
    service.shutdown();
}

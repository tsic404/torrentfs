//! Design acceptance case TC-seeding-01: a peer downloads correct bytes from
//! a seeding instance (`docs/03-acceptance.md` §F AC-50).
//!
//! Absorbed `tests/peer_download_test.rs::test_peer_discovery_via_tracker`
//! (the old copy is deleted): the downloader discovers the seeder via the
//! local tracker and reads the exact seeded content — i.e. the seeding
//! instance uploaded the correct bytes to the swarm.

use crate::common::{
    build_multipiece_torrent_pieces, local_test_config, unique_torrent, wave_slot, TestHarness,
};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

pub const TC_ID: &str = "TC-seeding-01";

#[test]
fn tc_seeding_01_peer_downloads_correct_bytes_from_seeder() {
    let _slot = wave_slot();
    let harness = TestHarness::new();

    let info_hash = hex::encode(harness.info.info_hash().expect("Failed to get info hash"));
    println!("Info hash: {}", info_hash);

    // Downloader session discovers the seeder via the shared tracker.
    let config = local_test_config();
    let mut dl_session =
        torrentfs::download::Session::new(&config).expect("Failed to create downloader session");

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");

    let torrent_data = harness.torrent_data.clone();
    let dl_info = torrentfs::TorrentInfo::from_bytes(torrent_data)
        .expect("Failed to parse torrent for downloader");

    let handle = dl_session
        .add_torrent(&dl_info, cache_dir.path())
        .expect("Failed to add torrent to downloader session");

    // Brief settle — give the tracker's peer list time to include
    // the seeder on slow CI hardware.
    thread::sleep(Duration::from_secs(2));

    // ── Wait for the downloader to discover the seeder ─────────────────
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(60);
    let mut peers_found = false;

    loop {
        match handle.status() {
            Ok(status) => {
                if status.num_peers > 0 || status.num_seeds > 0 {
                    println!(
                        "Downloader: found {} peers, {} seeds!",
                        status.num_peers, status.num_seeds
                    );
                    peers_found = true;
                    break;
                }
                // Also check if already finished/seeding (file was cached somehow)
                if matches!(
                    status.state,
                    torrentfs::download::TorrentState::Finished
                        | torrentfs::download::TorrentState::Seeding
                ) {
                    println!("Downloader: torrent completed (may have found data locally)");
                    peers_found = true;
                    break;
                }
            }
            Err(e) => {
                panic!("Downloader: status error: {:?}", e);
            }
        }

        if start.elapsed() > timeout {
            break;
        }

        thread::sleep(Duration::from_millis(500));
    }

    let final_announce_count = harness.tracker.announce_count();
    assert!(
        peers_found,
        "Downloader did not find any peers within {} seconds. Tracker announces: {}",
        timeout.as_secs(),
        final_announce_count
    );

    // The downloader must also be able to read the full seeded content — this
    // is the observable "another peer obtains the correct bytes" criterion.
    let download_dir = tempfile::TempDir::new().expect("temp dir");
    let content: Vec<u8> = {
        // Wait for the download to complete.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        loop {
            match handle.status() {
                Ok(status) => {
                    if matches!(
                        status.state,
                        torrentfs::download::TorrentState::Finished
                            | torrentfs::download::TorrentState::Seeding
                    ) {
                        break;
                    }
                }
                Err(_) => break,
            }
            if std::time::Instant::now() > deadline {
                panic!("downloader did not complete the transfer in time");
            }
            thread::sleep(Duration::from_millis(500));
        }
        let path = cache_dir.path().join("final_verification.txt");
        let content = std::fs::read(&path)
            .unwrap_or_else(|_| panic!("downloaded file missing at {:?}", path));
        assert_eq!(
            content, harness.file_content,
            "downloaded bytes must equal the seeded content"
        );
        content
    };
    assert_eq!(content.len(), 16384);
    let _ = download_dir;

    println!("\n=== Peer upload/download verified: exact bytes ===");
}

/// The criterion in the shape QA exercises: a foreign peer connects to the
/// daemon's session from the same host address the session already holds a
/// connection to, and pulls every piece the daemon caches.
///
/// Uses the shipped defaults rather than `local_test_config()`: the test-only
/// `allow_multiple_connections_per_ip` shortcut masks the peer-admission path
/// this case is about.
#[test]
fn tc_seeding_01_external_peer_pulls_cached_pieces() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("seeding-01-external-peer.txt"));

    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    let listen_port = reserve_tcp_port();
    config.connections.listen_interfaces = Some(format!("127.0.0.1:{}", listen_port));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to create DownloadEngine");

    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );
    let info_hash = info.info_hash().expect("Failed to get info hash");
    let status_key = hex::encode(info_hash);
    let full_len = harness.file_content.len() as u32;

    // Reading the whole file is what fills the daemon's cache and opens its
    // outgoing connection to the seeder — both on 127.0.0.1.
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        match engine.read_file_range(info.clone(), 0, 0, full_len) {
            Ok(data) => {
                assert_eq!(data, harness.file_content, "daemon read must match the seed");
                break;
            }
            Err(e) => {
                assert!(
                    Instant::now() < deadline,
                    "daemon could not read the seeded file: {:?}",
                    e
                );
                thread::sleep(Duration::from_millis(500));
            }
        }
    }

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let peers = engine
            .try_torrent_status(&status_key)
            .map(|status| status.num_peers)
            .unwrap_or(0);
        if peers > 0 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "daemon session never connected to the seeder; the external peer's \
             same-address precondition would not be exercised"
        );
        thread::sleep(Duration::from_millis(200));
    }

    let uploaded_before = engine.snapshot_stats().total_uploaded;
    let pulled = external_peer::pull_all(
        listen_port,
        info_hash,
        harness.file_content.len(),
        info.piece_length() as usize,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap_or_else(|e| panic!("external peer could not pull from the daemon: {}", e));

    assert_eq!(
        pulled,
        harness.file_content,
        "external peer must obtain the exact bytes the daemon cached"
    );

    // The upload side is part of the criterion: the session counter has to
    // move once the peer has taken the piece.
    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let uploaded = engine.snapshot_stats().total_uploaded;
        if uploaded > uploaded_before {
            println!(
                "upload counter grew {} -> {} after serving {} bytes",
                uploaded_before,
                uploaded,
                pulled.len()
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "session upload counter stayed at {} after serving {} bytes",
            uploaded_before,
            pulled.len()
        );
        thread::sleep(Duration::from_millis(200));
    }

    engine.shutdown();
}

/// AC-50 across a session boundary: the piece cache outlives the session, so a
/// daemon that starts with a warm cache and no read must still serve those
/// pieces. The regression this pins: the custom `disk_io` answered
/// `check_files` with an empty status, leaving the new session's bitfield
/// empty — every peer was unchoked with nothing to request.
#[test]
fn tc_seeding_01_cached_pieces_are_advertised_after_restart() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(unique_torrent("seeding-01-restart.txt"));

    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    let listen_port = reserve_tcp_port();
    config.connections.listen_interfaces = Some(format!("127.0.0.1:{}", listen_port));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );
    let info_hash = info.info_hash().expect("Failed to get info hash");
    let status_key = hex::encode(info_hash);
    let full_len = harness.file_content.len() as u32;

    // First session: one full read fills the piece cache on disk.
    {
        let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine");
        let deadline = Instant::now() + Duration::from_secs(60);
        loop {
            match engine.read_file_range(info.clone(), 0, 0, full_len) {
                Ok(data) => {
                    assert_eq!(
                        data, harness.file_content,
                        "first session read must match the seed"
                    );
                    break;
                }
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "first session could not read the seeded file: {:?}",
                        e
                    );
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
        engine.shutdown();
    }

    // Second session: no read, so every piece has to come from the cache.
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to reopen the engine on the same cache");
    engine
        .ensure_handle(info.clone())
        .expect("Failed to register the torrent in the new session");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = engine
            .try_torrent_status(&status_key)
            .map(|status| status.total_done)
            .unwrap_or(0);
        if done == u64::from(full_len) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the new session never learned the cached pieces (total_done {})",
            done
        );
        thread::sleep(Duration::from_millis(200));
    }

    let uploaded_before = engine.snapshot_stats().total_uploaded;
    let pulled = external_peer::pull_all(
        listen_port,
        info_hash,
        harness.file_content.len(),
        info.piece_length() as usize,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap_or_else(|e| panic!("external peer could not pull the cached pieces: {}", e));
    assert_eq!(
        pulled, harness.file_content,
        "external peer must obtain the bytes the previous session cached"
    );

    let deadline = Instant::now() + Duration::from_secs(15);
    loop {
        let uploaded = engine.snapshot_stats().total_uploaded;
        if uploaded > uploaded_before {
            println!(
                "upload counter grew {} -> {} after serving cached pieces",
                uploaded_before, uploaded
            );
            break;
        }
        assert!(
            Instant::now() < deadline,
            "session upload counter stayed at {} after serving cached pieces",
            uploaded_before
        );
        thread::sleep(Duration::from_millis(200));
    }

    engine.shutdown();
}

/// AC-50 with hash checks off: libtorrent skips the comparison, so a full
/// check passes every piece and would broadcast a partial cache as complete.
/// The cached set must come from the piece files instead — one piece here, so
/// the peer must be told about that piece and nothing else.
#[test]
fn tc_seeding_01_partial_cache_is_not_broadcast_when_hash_checks_disabled() {
    let _slot = wave_slot();
    let harness = TestHarness::with_torrent(|announce_url| {
        build_multipiece_torrent_pieces(announce_url, "seeding-01-disabled.txt", 16)
    });

    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.misc.disable_hash_checks = Some(true);
    let listen_port = reserve_tcp_port();
    config.connections.listen_interfaces = Some(format!("127.0.0.1:{}", listen_port));

    let cache_dir = tempfile::TempDir::new().expect("Failed to create cache dir");
    let info = Arc::new(
        torrentfs::TorrentInfo::from_bytes(harness.torrent_data.clone())
            .expect("Failed to parse torrent"),
    );
    let info_hash = info.info_hash().expect("Failed to get info hash");
    let status_key = hex::encode(info_hash);
    let piece_len = info.piece_length() as usize;
    let full_len = harness.file_content.len();
    let num_pieces = full_len.div_ceil(piece_len);
    assert!(num_pieces >= 16, "fixture exposes {} pieces", num_pieces);

    // First session fills the cache with every piece.  A read that outlives
    // its piece-wait window returns the completed prefix, so consume the file
    // the way a client does: resume at the new offset until it is all read.
    {
        let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
            .expect("Failed to create DownloadEngine");
        let deadline = Instant::now() + Duration::from_secs(120);
        let mut content = vec![0u8; full_len];
        let mut read = 0usize;
        while read < full_len {
            let want = (full_len - read) as u32;
            match engine.read_file_range(info.clone(), 0, read as u64, want) {
                Ok(data) if !data.is_empty() => {
                    assert!(
                        read + data.len() <= full_len,
                        "read past the end: {} + {} > {}",
                        read,
                        data.len(),
                        full_len
                    );
                    content[read..read + data.len()].copy_from_slice(&data);
                    read += data.len();
                }
                Ok(_) => {
                    assert!(
                        Instant::now() < deadline,
                        "first session stalled after {} of {} bytes",
                        read,
                        full_len
                    );
                    thread::sleep(Duration::from_millis(200));
                }
                Err(e) => {
                    assert!(
                        Instant::now() < deadline,
                        "first session could not read the seeded file: {:?}",
                        e
                    );
                    thread::sleep(Duration::from_millis(500));
                }
            }
        }
        assert_eq!(content, harness.file_content, "first session read must match the seed");
        engine.shutdown();
    }

    // Keep one piece on disk and drop the rest, which is what an eviction
    // leaves behind.
    let keep = num_pieces / 3;
    let pieces_dir = cache_dir.path().join("pieces").join(&status_key);
    let prefix = format!("{}:piece:", status_key);
    let mut kept = 0usize;
    for entry in std::fs::read_dir(&pieces_dir).expect("pieces dir must exist") {
        let entry = entry.expect("dir entry");
        let name = entry.file_name().to_string_lossy().into_owned();
        let Some(index) = name
            .strip_prefix(&prefix)
            .and_then(|suffix| suffix.parse::<usize>().ok())
        else {
            continue;
        };
        if index == keep {
            kept += 1;
        } else {
            std::fs::remove_file(entry.path()).expect("evict piece");
        }
    }
    assert_eq!(kept, 1, "the cache must hold exactly one piece, saw {}", kept);

    // Second session: no read, so the peer's bitfield comes from the declared
    // cached set alone.
    let engine = torrentfs::download::DownloadEngine::new(cache_dir.path(), &config)
        .expect("Failed to reopen the engine on the same cache");
    engine
        .ensure_handle(info.clone())
        .expect("Failed to register the torrent in the new session");

    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let done = engine
            .try_torrent_status(&status_key)
            .map(|status| status.total_done)
            .unwrap_or(0);
        assert!(
            done <= piece_len as u64,
            "only the cached piece may be declared, total_done={} (one piece = {})",
            done,
            piece_len
        );
        if done == piece_len as u64 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the cached piece was never declared (total_done {})",
            done
        );
        thread::sleep(Duration::from_millis(200));
    }

    let (declared, data) = external_peer::pull_declared(
        listen_port,
        info_hash,
        full_len,
        piece_len,
        Instant::now() + Duration::from_secs(30),
    )
    .unwrap_or_else(|e| panic!("external peer could not read the declared pieces: {}", e));

    assert_eq!(
        declared,
        vec![keep],
        "the peer must be told about the cached piece only"
    );
    assert_eq!(
        &data[keep * piece_len..(keep + 1) * piece_len],
        &harness.file_content[keep * piece_len..(keep + 1) * piece_len],
        "the cached piece must be served with its exact bytes"
    );

    engine.shutdown();
}

/// Reserve a loopback port by binding and immediately releasing it. The daemon
/// session needs a known port: unlike the downloader-side tests it must be
/// reachable at a fixed address.
fn reserve_tcp_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("Failed to bind a probe port")
        .local_addr()
        .expect("Failed to read the probe port")
        .port()
}

/// A minimal BitTorrent peer: handshake, `interested`, then one `request` per
/// block of every piece the daemon declares. It speaks only what the download
/// direction needs, so a successful pull proves the daemon served a foreign
/// client rather than another libtorrent instance.
mod external_peer {
    use super::*;

    const HANDSHAKE_LEN: usize = 68;
    /// Block size every real client requests.
    const BLOCK_LEN: usize = 16 * 1024;
    const BITFIELD_ID: u8 = 5;
    const REQUEST_ID: u8 = 6;
    const PIECE_ID: u8 = 7;
    const CHOKE_ID: u8 = 0;
    const UNCHOKE_ID: u8 = 1;

    /// Connect, request every declared piece, and return the assembled bytes.
    /// Requires the peer to declare and serve the whole torrent.
    pub fn pull_all(
        port: u16,
        info_hash: [u8; 20],
        total_len: usize,
        piece_len: usize,
        deadline: Instant,
    ) -> Result<Vec<u8>, String> {
        exchange(port, info_hash, total_len, piece_len, deadline, true)
            .map(|(_declared, data)| data)
    }

    /// Connect, request only the pieces the peer declares, and return those
    /// indices with the assembled bytes. A peer that serves a partial cache
    /// must declare exactly that subset.
    pub fn pull_declared(
        port: u16,
        info_hash: [u8; 20],
        total_len: usize,
        piece_len: usize,
        deadline: Instant,
    ) -> Result<(Vec<usize>, Vec<u8>), String> {
        exchange(port, info_hash, total_len, piece_len, deadline, false)
    }

    fn exchange(
        port: u16,
        info_hash: [u8; 20],
        total_len: usize,
        piece_len: usize,
        deadline: Instant,
        require_all: bool,
    ) -> Result<(Vec<usize>, Vec<u8>), String> {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        let mut stream = TcpStream::connect_timeout(&addr, Duration::from_secs(5))
            .map_err(|e| format!("connect {}: {}", addr, e))?;

        let mut handshake = Vec::with_capacity(HANDSHAKE_LEN);
        handshake.push(19u8);
        handshake.extend_from_slice(b"BitTorrent protocol");
        handshake.extend_from_slice(&[0u8; 8]);
        handshake.extend_from_slice(&info_hash);
        handshake.extend_from_slice(b"-TF0001-000000000001");
        stream
            .write_all(&handshake)
            .map_err(|e| format!("handshake write: {}", e))?;

        let mut reply = [0u8; HANDSHAKE_LEN];
        recv_exact(&mut stream, &mut reply, deadline)
            .map_err(|e| format!("peer did not answer the handshake: {}", e))?;
        if reply[28..48] != info_hash {
            return Err("handshake reply carries a different info hash".to_string());
        }

        stream
            .write_all(&[0, 0, 0, 1, 2])
            .map_err(|e| format!("interested write: {}", e))?;

        let num_pieces = total_len.div_ceil(piece_len);
        let mut data = vec![0u8; total_len];
        let mut received = vec![0usize; num_pieces];
        let mut bitfield: Option<Vec<u8>> = None;
        let mut unchoked = false;
        let mut requested = false;
        let mut messages = 0usize;

        // A piece is complete only when every one of its blocks arrived:
        // `received` counts bytes, so a single block of a multi-block piece
        // must not end the exchange. `require_all` pins the whole torrent;
        // otherwise the declared subset is the whole contract.
        let complete = |declared: &[usize], received: &[usize]| -> bool {
            if require_all {
                received
                    .iter()
                    .enumerate()
                    .all(|(index, filled)| *filled >= piece_size(index, piece_len, total_len))
            } else {
                !declared.is_empty()
                    && declared
                        .iter()
                        .all(|&index| received[index] >= piece_size(index, piece_len, total_len))
            }
        };
        let mut declared_pieces: Vec<usize> = Vec::new();

        while !(requested && complete(&declared_pieces, &received)) {
            let mut header = [0u8; 4];
            recv_exact(&mut stream, &mut header, deadline)
                .map_err(|e| format!("{} after {} messages: {}", e, messages, progress(&received)))?;
            let len = u32::from_be_bytes(header) as usize;
            if len == 0 {
                continue; // keep-alive
            }
            let mut body = vec![0u8; len];
            recv_exact(&mut stream, &mut body, deadline)
                .map_err(|e| format!("{} mid-message: {}", e, progress(&received)))?;
            messages += 1;
            match body[0] {
                BITFIELD_ID => bitfield = Some(body[1..].to_vec()),
                UNCHOKE_ID => unchoked = true,
                CHOKE_ID => unchoked = false,
                PIECE_ID if body.len() >= 9 => {
                    let index = u32::from_be_bytes([
                        body[1], body[2], body[3], body[4],
                    ]) as usize;
                    let begin = u32::from_be_bytes([
                        body[5], body[6], body[7], body[8],
                    ]) as usize;
                    let payload = &body[9..];
                    let offset = index * piece_len + begin;
                    let end = offset + payload.len();
                    if index >= num_pieces || end > total_len {
                        return Err(format!(
                            "peer sent an out-of-range block piece={} begin={} len={}",
                            index,
                            begin,
                            payload.len()
                        ));
                    }
                    data[offset..end].copy_from_slice(payload);
                    received[index] += payload.len();
                }
                _ => {}
            }

            // A seeder that is still hashing its cache unchokes before it can
            // declare a bitfield; keep waiting for the bitfield instead of
            // treating the order as a protocol error.
            if unchoked && !requested {
                if let Some(declared) = bitfield.as_deref() {
                    declared_pieces = declared_indices(declared, num_pieces);
                    request_all(&mut stream, declared, piece_len, total_len)?;
                    requested = true;
                }
            }
        }

        Ok((declared_pieces, data))
    }

    fn progress(received: &[usize]) -> String {
        let done = received.iter().filter(|filled| **filled > 0).count();
        format!("{}/{} pieces", done, received.len())
    }

    fn piece_size(index: usize, piece_len: usize, total_len: usize) -> usize {
        let start = index * piece_len;
        (total_len - start).min(piece_len)
    }

    /// Whether `bitfield` declares `index`.
    fn declares(bitfield: &[u8], index: usize) -> bool {
        bitfield
            .get(index / 8)
            .map(|byte| byte & (0x80 >> (index % 8)) != 0)
            .unwrap_or(false)
    }

    /// The piece indices `bitfield` declares.
    fn declared_indices(bitfield: &[u8], num_pieces: usize) -> Vec<usize> {
        (0..num_pieces)
            .filter(|&index| declares(bitfield, index))
            .collect()
    }

    fn request_all(
        stream: &mut TcpStream,
        bitfield: &[u8],
        piece_len: usize,
        total_len: usize,
    ) -> Result<(), String> {
        let num_pieces = total_len.div_ceil(piece_len);
        for index in 0..num_pieces {
            if !declares(bitfield, index) {
                continue;
            }
            let size = piece_size(index, piece_len, total_len);
            let mut begin = 0usize;
            while begin < size {
                let len = BLOCK_LEN.min(size - begin);
                let mut request = Vec::with_capacity(13);
                request.extend_from_slice(&13u32.to_be_bytes());
                request.push(REQUEST_ID);
                request.extend_from_slice(&(index as u32).to_be_bytes());
                request.extend_from_slice(&(begin as u32).to_be_bytes());
                request.extend_from_slice(&(len as u32).to_be_bytes());
                stream
                    .write_all(&request)
                    .map_err(|e| format!("request piece {} begin {}: {}", index, begin, e))?;
                begin += len;
            }
        }
        Ok(())
    }

    fn recv_exact(
        stream: &mut TcpStream,
        buf: &mut [u8],
        deadline: Instant,
    ) -> Result<(), String> {
        let mut filled = 0;
        while filled < buf.len() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("timed out".to_string());
            }
            stream
                .set_read_timeout(Some(remaining))
                .map_err(|e| format!("set_read_timeout: {}", e))?;
            match stream.read(&mut buf[filled..]) {
                Ok(0) => {
                    return Err(format!(
                        "peer closed the connection after {} of {} bytes",
                        filled,
                        buf.len()
                    ))
                }
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(format!("read: {}", e)),
            }
        }
        Ok(())
    }
}

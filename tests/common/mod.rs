//! Test infrastructure for CI-level BitTorrent testing.
//!
//! Provides:
//! - `MiniTracker`: a minimal HTTP BitTorrent tracker for local peer discovery
//! - `TestHarness`: orchestrates tracker + seeder to create a complete test environment
//! - Helper to create .torrent files pointing to the test tracker

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread;
use std::time::Duration;

use torrentfs::TorrentInfo;

/// Global lock to serialize libtorrent session creation during tests.
///
/// When multiple tests run in parallel within the same binary, each
/// creating its own libtorrent session (seeder + downloader), the
/// internal libtorrent thread pool can be overwhelmed, causing seeder
/// startup timeouts.  Acquiring this lock before creating any
/// libtorrent session prevents concurrent session initialization.
static SESSION_LOCK: Mutex<()> = Mutex::new(());

/// Acquire the global session serialization lock.
/// Returns the guard — hold it for the lifetime of all libtorrent
/// session creation and use within a test.  Serializes within a
/// single test binary; cross-binary contention is mitigated by
/// the `aio_threads=2` limit in `local_test_config()`.
#[allow(dead_code)]
pub fn acquire_session_lock() -> MutexGuard<'static, ()> {
    SESSION_LOCK.lock().unwrap_or_else(|e| e.into_inner())
}

// ── MiniTracker ──────────────────────────────────────────────────────────────

/// Compact peer representation: 4-byte IPv4 + 2-byte port + 8-byte left (big-endian).
#[derive(Clone, Debug)]
struct PeerInfo {
    ip: [u8; 4],
    port: u16,
    /// Bytes remaining to download (0 = seeder).  Used to distinguish different
    /// clients that may share the same IP:port on localhost (TSI-1977).
    left: u64,
}

impl PeerInfo {
    fn to_compact(&self) -> [u8; 6] {
        let mut buf = [0u8; 6];
        buf[..4].copy_from_slice(&self.ip);
        buf[4..6].copy_from_slice(&self.port.to_be_bytes());
        buf
    }
}

/// A minimal HTTP BitTorrent tracker for local testing.
///
/// Listens on a random localhost port, tracks peers per info_hash,
/// and returns compact peer lists on `/announce` requests.
pub struct MiniTracker {
    addr: std::net::SocketAddr,
    #[allow(dead_code)]
    peers: Arc<Mutex<HashMap<[u8; 20], Vec<PeerInfo>>>>,
    announce_count: Arc<Mutex<u64>>,
    running: Arc<Mutex<bool>>,
}

impl MiniTracker {
    /// Start the tracker on `127.0.0.1:0` (OS-assigned port).
    /// Spawns a background thread to handle HTTP requests.
    pub fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("MiniTracker: failed to bind");
        let addr = listener
            .local_addr()
            .expect("MiniTracker: failed to get local addr");

        let peers: Arc<Mutex<HashMap<[u8; 20], Vec<PeerInfo>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let announce_count = Arc::new(Mutex::new(0u64));
        let running = Arc::new(Mutex::new(true));

        let peers_clone = Arc::clone(&peers);
        let announce_count_clone = Arc::clone(&announce_count);
        let running_clone = Arc::clone(&running);

        thread::spawn(move || {
            listener
                .set_nonblocking(true)
                .expect("MiniTracker: set_nonblocking failed");

            loop {
                if !*running_clone.lock().unwrap() {
                    break;
                }

                match listener.accept() {
                    Ok((stream, _)) => {
                        let p = Arc::clone(&peers_clone);
                        let ac = Arc::clone(&announce_count_clone);
                        thread::spawn(move || {
                            handle_announce(stream, p, ac);
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(50));
                        continue;
                    }
                    Err(e) => {
                        eprintln!("MiniTracker: accept error: {:?}", e);
                        break;
                    }
                }
            }
        });

        MiniTracker {
            addr,
            peers,
            announce_count,
            running,
        }
    }

    /// The announce URL clients should use (e.g. `http://127.0.0.1:XXXXX/announce`).
    pub fn announce_url(&self) -> String {
        format!("http://{}/announce", self.addr)
    }

    /// Number of announce requests handled so far.
    pub fn announce_count(&self) -> u64 {
        *self.announce_count.lock().unwrap()
    }

    /// Check whether the tracker has at least one peer registered for
    /// the given info_hash.  Used by TestHarness to verify that the
    /// seeder is actually discoverable before letting the downloader
    /// start — avoids a race where announce_count is incremented before
    /// the peer is inserted into the HashMap.
    pub fn peer_count(&self, info_hash: &[u8; 20]) -> usize {
        self.peers
            .lock()
            .unwrap()
            .get(info_hash)
            .map(|v| v.len())
            .unwrap_or(0)
    }
}

impl Drop for MiniTracker {
    fn drop(&mut self) {
        *self.running.lock().unwrap() = false;
    }
}

/// Handle a single HTTP announce request.
fn handle_announce(
    mut stream: TcpStream,
    peers: Arc<Mutex<HashMap<[u8; 20], Vec<PeerInfo>>>>,
    announce_count: Arc<Mutex<u64>>,
) {
    let mut buf = [0u8; 4096];
    let n = match stream.read(&mut buf) {
        Ok(n) if n > 0 => n,
        _ => return,
    };

    *announce_count.lock().unwrap() += 1;

    let request = String::from_utf8_lossy(&buf[..n]);
    let first_line = request.lines().next().unwrap_or("");

    eprintln!("MiniTracker: received request: {}", first_line);

    // Parse: GET /announce?info_hash=...&port=... HTTP/1.1
    let path_and_query = first_line.split_whitespace().nth(1).unwrap_or("/");

    if !path_and_query.starts_with("/announce") {
        let resp = "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n";
        let _ = stream.write_all(resp.as_bytes());
        return;
    }

    // Extract query string
    let query_str = path_and_query.split('?').nth(1).unwrap_or("");

    let params = parse_query_params(query_str);

    // Extract peer info from the announce
    let info_hash: [u8; 20] = match params.get("info_hash") {
        Some(raw) => match url_decode_binary(raw) {
            Ok(v) if v.len() == 20 => {
                let mut arr = [0u8; 20];
                arr.copy_from_slice(&v);
                arr
            }
            other => {
                eprintln!(
                    "MiniTracker: bad info_hash (len={:?}): {:?}",
                    other.as_ref().map(|v| v.len()).ok(),
                    raw
                );
                let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
                let _ = stream.write_all(resp.as_bytes());
                return;
            }
        },
        None => {
            eprintln!("MiniTracker: missing info_hash in request");
            let resp = "HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n";
            let _ = stream.write_all(resp.as_bytes());
            return;
        }
    };

    let port: u16 = params
        .get("port")
        .and_then(|v| v.parse().ok())
        .unwrap_or(6881);

    // The peer's IP is the connection's remote address
    let peer_ip = match stream.peer_addr() {
        Ok(addr) => match addr.ip() {
            std::net::IpAddr::V4(v4) => v4.octets(),
            std::net::IpAddr::V6(_) => [127, 0, 0, 1],
        },
        Err(_) => [127, 0, 0, 1],
    };

    // Parse the `left` parameter to determine if this is a seeder or leecher
    let left: u64 = params.get("left").and_then(|v| v.parse().ok()).unwrap_or(0);

    // Register this peer — update any existing entry for the same
    // (ip, port) so that a seeder transitioning from left=16384
    // (CheckingResumeData) to left=0 (Seeding) doesn't leave behind
    // a stale leecher entry that can collide with the downloader's
    // identity and get filtered out.
    let peer_count: usize = {
        let mut map = peers.lock().unwrap();
        let entry = map.entry(info_hash).or_default();

        // Remove any old entries for this (ip, port) pair
        entry.retain(|p| !(p.ip == peer_ip && p.port == port));

        // Deduplicate by ip:port AND left — two distinct clients on the
        // same IP:port with different `left` values (seeder vs downloader)
        // must both be kept.  This fixes TSI-1977.
        let already_exists = entry
            .iter()
            .any(|p| p.ip == peer_ip && p.port == port && p.left == left);
        if !already_exists {
            entry.push(PeerInfo {
                ip: peer_ip,
                port,
                left,
            });
        }
        entry.len()
    };

    // Build response: bencoded dict with interval + compact peer list
    // IMPORTANT: exclude the announcing peer from the response.
    // Use (ip, port, left) identity — same IP:port but different `left`
    // means a different client (TSI-1977).
    let compact_peers: Vec<u8> = {
        let map = peers.lock().unwrap();
        let list = map.get(&info_hash).map(|v| v.clone()).unwrap_or_default();
        list.iter()
            .filter(|p| !(p.ip == peer_ip && p.port == port && p.left == left))
            .flat_map(|p| p.to_compact().to_vec())
            .collect()
    };

    // Use a short interval so seeders re-announce within test timeouts.
    // The default 1800s prevents re-announces entirely during test runs,
    // causing the TestHarness re-announce wait to always time out and the
    // downloader to start with a stale peer list when the seeder announced
    // during CheckingResumeData before it was ready to serve pieces.
    let response_body = bencode_tracker_response(5, &compact_peers);

    eprintln!(
        "MiniTracker: announce from {}:{}, left={}, total_peers={}, returning {} peers ({} compact bytes)",
        std::net::Ipv4Addr::from(peer_ip),
        port,
        left,
        peer_count,
        compact_peers.len() / 6,
        compact_peers.len()
    );

    let resp = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {}\r\n\r\n",
        response_body.len()
    );
    let _ = stream.write_all(resp.as_bytes());
    let _ = stream.write_all(&response_body);
}

/// Parse URL query string into key-value pairs.
fn parse_query_params(query: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    for pair in query.split('&') {
        let mut parts = pair.splitn(2, '=');
        let key = parts.next().unwrap_or("").to_string();
        let value = parts.next().unwrap_or("").to_string();
        if !key.is_empty() {
            map.insert(key, value);
        }
    }
    map
}

/// URL-decode a percent-encoded binary string.
fn url_decode_binary(input: &str) -> Result<Vec<u8>, ()> {
    let mut result = Vec::new();
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' if i + 2 < bytes.len() => {
                let hex = &input[i + 1..i + 3];
                let byte = u8::from_str_radix(hex, 16).map_err(|_| ())?;
                result.push(byte);
                i += 3;
            }
            b'+' => {
                result.push(b' ');
                i += 1;
            }
            b => {
                result.push(b);
                i += 1;
            }
        }
    }
    Ok(result)
}

/// Build a minimal bencoded tracker response:
/// `d8:intervali<interval>e5:peers<len>:<compact_peer_list>e`
fn bencode_tracker_response(interval: i64, compact_peers: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(b'd');

    // "interval" i<interval>e
    out.extend_from_slice(b"8:intervali");
    out.extend_from_slice(interval.to_string().as_bytes());
    out.push(b'e');

    // "peers" <len>:<data>
    out.extend_from_slice(b"5:peers");
    out.extend_from_slice(compact_peers.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(compact_peers);

    out.push(b'e');
    out
}

// ── Torrent creation helpers ─────────────────────────────────────────────────

/// Create a minimal single-file .torrent (bencoded) pointing at a specific
/// announce URL. Returns (torrent_bytes, file_content).
///
/// The content is 162 bytes of known data, piece length 16384 (single piece),
/// so the entire file fits in one piece.
pub fn create_test_torrent_with_tracker(announce_url: &str) -> (Vec<u8>, Vec<u8>) {
    let mut test_content = b"Hello, this is a test file for torrentfs verification.\n".to_vec();
    while test_content.len() < 16384 {
        test_content.push(b'X');
    }
    test_content.truncate(16384);

    let sha1_hash = {
        use sha1_smol::Sha1;
        let mut hasher = Sha1::new();
        hasher.update(&test_content);
        hasher.digest().bytes()
    };

    let mut torrent = Vec::new();
    // "d8:announce<len>:<url>4:infod..."
    torrent.push(b'd');
    torrent.extend_from_slice(b"8:announce");
    torrent.extend_from_slice(announce_url.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(announce_url.as_bytes());

    torrent.extend_from_slice(b"4:infod");
    torrent.extend_from_slice(b"6:lengthi16384e");
    torrent.extend_from_slice(b"4:name22:final_verification.txt");
    torrent.extend_from_slice(b"12:piece lengthi16384e");
    torrent.extend_from_slice(b"6:pieces20:");
    torrent.extend_from_slice(&sha1_hash);
    torrent.extend_from_slice(b"ee");

    (torrent, test_content)
}

/// Build a multi-piece single-file `.torrent` (4 × 256 KiB = 1 MiB) pointing
/// at `announce_url`.  Returns `(torrent_bytes, file_content)`.
///
/// TSI-3041: the multi-piece fixture exposes the per-read O(num_pieces)
/// download-machinery overhead that the single-piece 16 KiB fixture hides —
/// a byte-granular read on a cached file previously paid `post_torrent_updates`
/// + a full piece-priority sweep on every read, regardless of the 1-byte size.
pub fn build_multipiece_torrent(announce_url: &str) -> (Vec<u8>, Vec<u8>) {
    const PIECE_LEN: usize = 256 * 1024;
    const NUM_PIECES: usize = 4;
    let total = PIECE_LEN * NUM_PIECES;

    let mut content = Vec::with_capacity(total);
    for i in 0..total {
        content.push((i as u8).wrapping_mul(31).wrapping_add(7));
    }

    let mut hashes = Vec::with_capacity(20 * NUM_PIECES);
    for p in 0..NUM_PIECES {
        use sha1_smol::Sha1;
        let mut hasher = Sha1::new();
        hasher.update(&content[p * PIECE_LEN..(p + 1) * PIECE_LEN]);
        hashes.extend_from_slice(&hasher.digest().bytes());
    }

    let mut t = Vec::new();
    t.push(b'd');
    t.extend_from_slice(b"8:announce");
    t.extend_from_slice(announce_url.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(announce_url.as_bytes());
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"6:lengthi");
    t.extend_from_slice(total.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"4:name9:multi.bin");
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(PIECE_LEN.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces");
    t.extend_from_slice(hashes.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(&hashes);
    t.extend_from_slice(b"ee");

    (t, content)
}

/// Build a config suitable for local testing: disable DHT, enable LSD,
/// to ensure reliable peer discovery on localhost.
pub fn local_test_config() -> torrentfs::TorrentfsConfig {
    let mut c = torrentfs::TorrentfsConfig::default_config();
    c.dht.enabled = Some(false);
    // Enable LSD for local peer discovery as fallback
    c.local_discovery.lsd_enabled = Some(true);
    c.local_discovery.upnp_enabled = Some(false);
    c.local_discovery.natpmp_enabled = Some(false);
    // Allow multiple connections from same IP (critical for localhost testing)
    c.connections.allow_multiple_connections_per_ip = Some(true);
    // Fast peer connect timeout for localhost — default 10s is excessive
    // when both peers are on the same machine.
    c.connections.peer_connect_timeout = Some(5);
    c.timeouts.read_timeout_secs = Some(30);
    // Optimize tracker announce for local testing — reduce minimum interval
    // so peers discover each other faster (mitigates CI flakiness in
    // test_peer_discovery_via_tracker).
    c.tracker.announce_to_all_trackers = Some(true);
    c.tracker.announce_to_all_tiers = Some(true);
    c.tracker.min_announce_interval = Some(5);
    // Restrict libtorrent thread pools to avoid resource exhaustion when
    // multiple test binaries run in parallel (each with its own session).
    // Default is std::thread::hardware_concurrency(), which on CI can be
    // as low as 2; with 4+ sessions competing, thread pool contention
    // causes timeouts in seeder startup and piece download.
    c.performance.aio_threads = Some(2);
    // network_threads is deprecated in libtorrent 2.0 (silently ignored).
    // c.performance.network_threads = Some(2);
    c
}

// ── TestHarness ──────────────────────────────────────────────────────────────

/// A complete test environment: tracker + seeder.
///
/// The seeder runs a libtorrent session with the complete file data,
/// announces to the tracker, and serves peers.
pub struct TestHarness {
    pub tracker: MiniTracker,
    /// Directory containing the seeder's file data.
    #[allow(dead_code)]
    seed_dir: tempfile::TempDir,
    /// The test content that was seeded.
    pub file_content: Vec<u8>,
    /// The torrent info (parsed from the torrent data).
    pub info: TorrentInfo,
    /// The raw torrent bytes.
    pub torrent_data: Vec<u8>,
    // We hold the seeder thread alive.
    _seeder_thread: Option<thread::JoinHandle<()>>,
    // Signal to stop the seeder
    _seeder_stop: Arc<Mutex<bool>>,
}

impl TestHarness {
    /// Create a new test harness with a running tracker and seeder.
    ///
    /// The seeder will have the complete file data and will announce to the
    /// tracker, making it available for peer download.
    ///
    /// Waits for the seeder to fully start and announce before returning.
    pub fn new() -> Self {
        Self::with_torrent(create_test_torrent_with_tracker)
    }

    /// Create a harness whose seeder serves a custom single-file torrent.
    ///
    /// `torrent_fn` receives the tracker announce URL and returns
    /// `(torrent_bytes, file_content)`.  The seed file name is taken from the
    /// parsed torrent info (`info.name()`), so the torrent's `name` field must
    /// match the seeded file name (as in [`create_test_torrent_with_tracker`]
    /// and [`build_multipiece_torrent`]).
    pub fn with_torrent<F>(torrent_fn: F) -> Self
    where
        F: FnOnce(&str) -> (Vec<u8>, Vec<u8>),
    {
        let tracker = MiniTracker::start();
        let announce_url = tracker.announce_url();

        eprintln!("TestHarness: tracker started at {}", announce_url);

        let (torrent_data, file_content) = torrent_fn(&announce_url);

        let info = TorrentInfo::from_bytes(torrent_data.clone())
            .expect("TestHarness: failed to parse torrent");

        let seed_dir =
            tempfile::TempDir::new().expect("TestHarness: failed to create seed temp dir");

        // Write the file data to the seed directory so libtorrent finds it complete.
        // The seed file name must match the torrent's single-file `name` field.
        let seed_file_path = seed_dir.path().join(info.name());
        std::fs::write(&seed_file_path, &file_content)
            .expect("TestHarness: failed to write seed file");

        eprintln!(
            "TestHarness: wrote seed file to {:?} ({} bytes)",
            seed_file_path,
            file_content.len()
        );

        // Start the seeder in a background thread.
        let torrent_data_for_seeder = torrent_data.clone();
        let seeder_save_path = seed_dir.path().to_path_buf();
        let stop_signal = Arc::new(Mutex::new(false));
        let stop_clone = Arc::clone(&stop_signal);

        // Signal when the seeder reaches Seeding/Finished state
        let seeder_ready = Arc::new(Mutex::new(false));
        let seeder_ready_clone = Arc::clone(&seeder_ready);

        let seeder_thread = thread::spawn(move || {
            // Use local test config (DHT disabled) for clean tracker-only path
            let config = local_test_config();
            let mut session = match torrentfs::download::Session::new(&config) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("TestHarness seeder: failed to create session: {:?}", e);
                    return;
                }
            };

            eprintln!("TestHarness seeder: session created");

            // Re-parse torrent data inside the thread (raw pointer can't cross threads).
            let seeder_info = match TorrentInfo::from_bytes(torrent_data_for_seeder) {
                Ok(info) => info,
                Err(e) => {
                    eprintln!("TestHarness seeder: failed to parse torrent: {:?}", e);
                    return;
                }
            };

            eprintln!(
                "TestHarness seeder: torrent parsed, name={}, adding to session...",
                seeder_info.name()
            );

            match session.add_torrent(&seeder_info, &seeder_save_path) {
                Ok(handle) => {
                    eprintln!("TestHarness seeder: torrent added to session");

                    // Wait for torrent to finish checking and start seeding
                    let start = std::time::Instant::now();
                    let timeout = Duration::from_secs(60);
                    loop {
                        match handle.status() {
                            Ok(status) => {
                                eprintln!(
                                    "TestHarness seeder: state={:?}, progress={:.2}%, peers={}, seeds={}",
                                    status.state, status.progress * 100.0, status.num_peers, status.num_seeds
                                );

                                // Once we're seeding (or finished checking), we're ready
                                if matches!(
                                    status.state,
                                    torrentfs::download::TorrentState::Seeding
                                        | torrentfs::download::TorrentState::Finished
                                ) {
                                    eprintln!("TestHarness seeder: now seeding!");
                                    *seeder_ready_clone.lock().unwrap() = true;
                                    break;
                                }

                                // Timeout safety
                                if start.elapsed() > timeout {
                                    eprintln!(
                                        "TestHarness seeder: timeout waiting for seed state, current={:?}",
                                        status.state
                                    );
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("TestHarness seeder: status error: {:?}", e);
                                break;
                            }
                        }
                        thread::sleep(Duration::from_millis(500));
                    }

                    // Keep handle and session alive
                    let _handle = handle;

                    // Wait until stopped
                    loop {
                        if *stop_clone.lock().unwrap() {
                            break;
                        }
                        thread::sleep(Duration::from_millis(100));
                    }
                }
                Err(e) => {
                    eprintln!("TestHarness seeder: failed to add torrent: {:?}", e);
                }
            }
        });

        // Wait for the seeder to finish checking and announce
        // (the seeder thread will print its status, we poll the tracker)
        let start = std::time::Instant::now();
        let timeout = Duration::from_secs(60);
        loop {
            let count = tracker.announce_count();
            if count >= 1 {
                eprintln!(
                    "TestHarness: seeder has announced ({} total announces)",
                    count
                );
                break;
            }
            if start.elapsed() > timeout {
                eprintln!(
                    "TestHarness: timeout waiting for seeder announce (got {} announces)",
                    count
                );
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        // Wait for the seeder to reach Seeding/Finished state (not just announce).
        // On slow CI hardware, the announce may arrive during CheckingFiles before
        // the seeder can actually serve pieces.  Without this wait, the downloader
        // may start too early and time out waiting for a peer that isn't ready yet.
        let start = std::time::Instant::now();
        let ready_timeout = Duration::from_secs(30);
        loop {
            if *seeder_ready.lock().unwrap() {
                eprintln!("TestHarness: seeder is ready (Seeding/Finished)");
                break;
            }
            if start.elapsed() > ready_timeout {
                eprintln!("TestHarness: timeout waiting for seeder to reach Seeding state");
                break;
            }
            thread::sleep(Duration::from_millis(200));
        }

        // After the seeder is ready, wait for it to complete at least one
        // more announce cycle.  On slow CI hardware the tracker may still
        // be returning stale peer lists from the CheckingFiles phase;
        // forcing one extra announce ensures the downloader will see the
        // seeder as a peer when it queries the tracker.
        {
            let start = std::time::Instant::now();
            let announce_timeout = Duration::from_secs(15);
            let target = tracker.announce_count() + 1;
            loop {
                if tracker.announce_count() >= target {
                    eprintln!(
                        "TestHarness: seeder re-announced ({} total announces)",
                        tracker.announce_count()
                    );
                    break;
                }
                if start.elapsed() > announce_timeout {
                    eprintln!(
                        "TestHarness: timeout waiting for seeder re-announce (got {}, wanted {})",
                        tracker.announce_count(),
                        target
                    );
                    break;
                }
                thread::sleep(Duration::from_millis(200));
            }
        }

        // Verify the seeder actually appears in the tracker's peer
        // list.  On slow CI, announce_count may have been incremented
        // before the peer was inserted into the HashMap — if we proceed
        // too early the downloader will get an empty peer list.
        {
            let info_hash = info.info_hash().expect("Failed to get info hash");
            let start = std::time::Instant::now();
            loop {
                if tracker.peer_count(&info_hash) > 0 {
                    eprintln!("TestHarness: seeder confirmed in tracker peer list");
                    break;
                }
                if start.elapsed() > Duration::from_secs(10) {
                    eprintln!("TestHarness: timeout waiting for seeder in peer list");
                    break;
                }
                thread::sleep(Duration::from_millis(200));
            }
        }

        // Extra grace period for network stack to stabilize on slow CI.
        thread::sleep(Duration::from_secs(2));

        TestHarness {
            tracker,
            seed_dir,
            file_content,
            info,
            torrent_data,
            _seeder_thread: Some(seeder_thread),
            _seeder_stop: stop_signal,
        }
    }
}

impl TestHarness {
    /// Stop the seeder and wait for its thread to exit.
    /// The tracker remains running so the downloader can still use it.
    pub fn stop_seeder(&mut self) {
        *self._seeder_stop.lock().unwrap() = true;
        if let Some(handle) = self._seeder_thread.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for TestHarness {
    fn drop(&mut self) {
        *self._seeder_stop.lock().unwrap() = true;
        // Join the seeder thread to ensure it's fully stopped
        // before the next test starts.  Without this join, a
        // still-running libtorrent session from a previous test
        // can contend with the next test's session, causing the
        // NoPeers race when the downloader can't connect to the
        // new seeder.
        if let Some(handle) = self._seeder_thread.take() {
            let _ = handle.join();
        }
    }
}

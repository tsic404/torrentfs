//! `torrentfs-selfseed-env` — self-contained QA seeder environment (TSI-2418).
//!
//! Public Ubuntu/Debian sample torrents frequently have no reachable seeders,
//! so reads against them legitimately fail with `NoPeers` → ENODATA.  This
//! binary builds a deterministic single-file torrent from a fixed payload and
//! serves it via a local tracker + libtorrent seeder, giving QA a swarm that
//! works fully offline.
//!
//! Flow:
//!  1. start a minimal HTTP tracker on a loopback port
//!  2. bencode a single-file .torrent pointing at that tracker
//!  3. copy the payload into the seed directory (complete data)
//!  4. run a libtorrent session in seeding state until killed
//!
//! The tracker and seeder logic mirror `tests/common/mod.rs` (`MiniTracker`,
//! `TestHarness`) so QA gets the same loopback-only swarm the CI tests use,
//! without needing the test tree.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use torrentfs::download::{Session, TorrentState};
use torrentfs::TorrentInfo;

// ── minimal bencoding ────────────────────────────────────────────────────────

fn bencode_int(i: i64) -> Vec<u8> {
    format!("i{}e", i).into_bytes()
}

fn bencode_bytes(b: &[u8]) -> Vec<u8> {
    let mut out = format!("{}:", b.len()).into_bytes();
    out.extend_from_slice(b);
    out
}

// ── minimal HTTP tracker ─────────────────────────────────────────────────────

#[derive(Clone, Copy)]
struct Peer {
    ip: [u8; 4],
    port: u16,
}

struct TrackerState {
    /// info_hash → registered peers.
    peers: Mutex<HashMap<[u8; 20], Vec<Peer>>>,
}

fn query_param(query: &str, key: &str) -> Option<String> {
    for pair in query.split('&') {
        let mut it = pair.splitn(2, '=');
        if it.next()? == key {
            return Some(it.next().unwrap_or("").to_string());
        }
    }
    None
}

/// Percent-decode a query value back into raw bytes (`%XX`, `+` untouched).
///
/// Malformed input (truncated `%` / `%4` escape, or non-hex digits after
/// `%`) is passed through verbatim rather than rejected: this is a QA-only
/// tracker and libtorrent always sends well-formed announces, so lenient
/// decoding keeps the loop simple without hiding anything that matters.
fn percent_decode(input: &str) -> Vec<u8> {
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(v) = u8::from_str_radix(&input[i + 1..i + 3], 16) {
                out.push(v);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    out
}

/// Handle one `/announce`: register the peer, reply with a compact peer list
/// excluding the requester itself.
fn handle_announce(state: Arc<TrackerState>, mut stream: std::net::TcpStream) {
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);
    let request = String::from_utf8_lossy(&buf[..n]);

    // Request line: GET /announce?<query> HTTP/1.x
    let request_target = request.lines().next().unwrap_or_default();
    let raw_query = match request_target.split_once('?') {
        Some((_, q)) => q.split(' ').next().unwrap_or(q).to_string(),
        None => return,
    };

    let info_hash: [u8; 20] = match query_param(&raw_query, "info_hash")
        .map(|v| percent_decode(&v))
        .and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok())
    {
        Some(h) => h,
        None => return,
    };
    let peer_port: u16 = match query_param(&raw_query, "port").and_then(|v| v.parse().ok()) {
        Some(p) => p,
        None => return,
    };
    // Unknown `left` ⇒ treat as incomplete (leecher); either way we register it.
    let _left: u64 = query_param(&raw_query, "left")
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX);

    let ip = match stream.peer_addr() {
        Ok(std::net::SocketAddr::V4(v4)) => v4.ip().octets(),
        _ => return,
    };

    {
        let mut peers = state.peers.lock();
        let entry = peers.entry(info_hash).or_default();
        entry.retain(|p| !(p.ip == ip && p.port == peer_port));
        entry.push(Peer {
            ip,
            port: peer_port,
        });
    }

    let compact: Vec<u8> = {
        let peers = state.peers.lock();
        peers
            .get(&info_hash)
            .map(|list| {
                list.iter()
                    .filter(|p| !(p.ip == ip && p.port == peer_port))
                    .flat_map(|p| {
                        [
                            p.ip[0],
                            p.ip[1],
                            p.ip[2],
                            p.ip[3],
                            (p.port >> 8) as u8,
                            (p.port & 0xff) as u8,
                        ]
                    })
                    .collect()
            })
            .unwrap_or_default()
    };

    // d8:intervali5e5:peers<len>:<compact_peer_list>e
    let mut body = b"d8:intervali5e5:peers".to_vec();
    body.extend_from_slice(format!("{}:", compact.len()).as_bytes());
    body.extend_from_slice(&compact);
    body.extend_from_slice(b"e");

    let response = format!(
        "HTTP/1.0 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.write_all(&body);
}

fn start_tracker(port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", port))?;
    let state = Arc::new(TrackerState {
        peers: Mutex::new(HashMap::new()),
    });
    eprintln!("[tracker] listening on 127.0.0.1:{}", port);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let state = Arc::clone(&state);
            // One thread per announce — QA load is trivially small.
            std::thread::spawn(move || handle_announce(state, stream));
        }
    });
    Ok(())
}

// ── main ─────────────────────────────────────────────────────────────────────

struct Args {
    payload: PathBuf,
    tracker_port: u16,
    torrent_out: PathBuf,
    url_out: PathBuf,
}

fn parse_args() -> Args {
    let mut args = Args {
        payload: PathBuf::from("payload.txt"),
        tracker_port: 16969,
        torrent_out: PathBuf::from("selfseed.torrent"),
        url_out: PathBuf::from("tracker.url"),
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        macro_rules! value_for {
            ($name:expr) => {{
                assert_eq!(arg, $name, "flag {} needs a value", $name);
                argv.next()
                    .unwrap_or_else(|| panic!("missing value after {}", $name))
            }};
        }
        match arg.as_str() {
            "--payload" => args.payload = value_for!("--payload").into(),
            "--tracker-port" => {
                args.tracker_port = value_for!("--tracker-port").parse().expect("bad port")
            }
            "--torrent-out" => args.torrent_out = value_for!("--torrent-out").into(),
            "--url-out" => args.url_out = value_for!("--url-out").into(),
            other => panic!("unknown argument: {}", other),
        }
    }
    args
}

fn main() {
    let args = parse_args();

    // 1. Tracker first — the announce URL must be live before we bencode.
    start_tracker(args.tracker_port).expect("failed to start local tracker");
    let announce_url = format!("http://127.0.0.1:{}/announce", args.tracker_port);
    std::fs::write(&args.url_out, &announce_url).expect("failed to write tracker.url");

    // 2. Deterministic single-file torrent over the fixed payload.
    let payload = std::fs::read(&args.payload).expect("failed to read payload");
    assert!(!payload.is_empty(), "payload must not be empty");

    const PIECE_LEN: usize = 262_144; // 256 KiB
    use sha1_smol::Sha1;
    let pieces: Vec<u8> = payload
        .chunks(PIECE_LEN)
        .flat_map(|chunk| {
            let mut h = Sha1::new();
            h.update(chunk);
            h.digest().bytes()
        })
        .collect();
    let num_pieces = payload.chunks(PIECE_LEN).count();

    let name = "selfseed";
    let dict: Vec<u8> = {
        let mut d = vec![b'd'];
        d.extend_from_slice(b"8:announce");
        d.extend_from_slice(&bencode_bytes(announce_url.as_bytes()));
        d.extend_from_slice(b"4:infod");
        d.extend_from_slice(b"6:length");
        d.extend_from_slice(&bencode_int(payload.len() as i64));
        d.extend_from_slice(b"4:name");
        d.extend_from_slice(&bencode_bytes(name.as_bytes()));
        d.extend_from_slice(b"12:piece length");
        d.extend_from_slice(&bencode_int(PIECE_LEN as i64));
        d.extend_from_slice(b"6:pieces");
        d.extend_from_slice(&bencode_bytes(&pieces));
        d.extend_from_slice(b"ee");
        d
    };

    std::fs::write(&args.torrent_out, &dict).expect("failed to write torrent");
    println!(
        "[selfseed] wrote {} ({} bytes, {} pieces)",
        args.torrent_out.display(),
        dict.len(),
        num_pieces
    );

    // 3. Seed directory holds the complete file under the torrent's name.
    let seed_dir = args
        .torrent_out
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("seed_data");
    std::fs::create_dir_all(&seed_dir).expect("failed to create seed dir");
    std::fs::write(seed_dir.join(name), &payload).expect("failed to write seed file");

    let info = TorrentInfo::from_bytes(dict.clone()).expect("failed to parse generated torrent");
    println!(
        "[seeder] name={} size={} bytes info_hash={}",
        info.name(),
        info.total_size(),
        hex::encode(info.info_hash().expect("info hash"))
    );
    println!("[seeder] announcing to {}", announce_url);
    // 4. Loopback-only config mirrors tests/common/mod.rs::local_test_config:
    // no DHT, no UPnP/NAT-PMP, short connect timeout, aggressive announce.
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(true);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.allow_multiple_connections_per_ip = Some(true);
    config.connections.peer_connect_timeout = Some(5);
    config.tracker.announce_to_all_trackers = Some(true);
    config.tracker.announce_to_all_tiers = Some(true);
    config.tracker.min_announce_interval = Some(5);

    let mut session = Session::new(&config).expect("failed to create libtorrent session");
    let handle = session
        .add_torrent(&info, &seed_dir)
        .expect("failed to add torrent to session");

    // Wait until we're actually serving before declaring readiness.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        let status = handle.status().expect("status failed");
        eprintln!(
            "[seeder] state={:?} progress={:.1}% seeds={} peers={}",
            status.state,
            status.progress * 100.0,
            status.num_seeds,
            status.num_peers
        );
        if matches!(status.state, TorrentState::Seeding | TorrentState::Finished) {
            break;
        }
        if std::time::Instant::now() > deadline {
            eprintln!("[seeder] timed out waiting for Seeding state; continuing anyway");
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    handle.force_reannounce();

    println!("[seeder] ready — Ctrl-C to stop");
    loop {
        // Keep the session alive; re-announce each hour so the swarm entry
        // never expires for long-running QA sessions.
        std::thread::sleep(Duration::from_secs(3600));
        handle.force_reannounce();
    }
}

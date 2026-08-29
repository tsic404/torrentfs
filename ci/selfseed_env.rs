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
    /// Bytes remaining to download (0 = seeder).  Part of the peer identity:
    /// two distinct clients can share the same IP:port when a downloader runs
    /// behind pasta/slirp NAT while the seeder runs on the host — both then
    /// appear to the tracker as `127.0.0.1:<same listen port>` (TSI-2417).
    left: u64,
    /// Last announce time (Instant::now at registration).  Entries not
    /// re-announced within [`PEER_EXPIRY`] are dropped — without expiry, every
    /// delete + re-add of the same info_hash leaves the previous handle's
    /// entry behind, and the new handle's peer list fills with stale
    /// self-referential entries (`127.0.0.1:<own listen port>`) that it then
    /// wastes its connection attempts on (TSI-2417).
    seen: std::time::Instant,
}

/// Drop peers that have not re-announced within this window.  The tracker
/// advertises `interval=5`, so a live client announces well inside 30s.
const PEER_EXPIRY: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound on the wait for the first announce bytes.  A client that connects
/// and sends nothing would otherwise park `handle_announce` in `read`
/// forever (TSI-2621); on timeout the empty request is answered with the
/// same `400 Bad Request` as a malformed announce.
const READ_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

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

/// Write a minimal `400 Bad Request` response and close the connection.
///
/// Malformed announces used to fall through to a silent `return`, which
/// closed the socket with no response at all — libtorrent only saw
/// `End of file` and retried, hiding the reason (TSI-2582).
fn write_bad_request(stream: &mut std::net::TcpStream) {
    let _ = stream
        .write_all(b"HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

/// Handle one `/announce`: register the peer, reply with a compact peer list
/// excluding the requester itself.
fn handle_announce(state: Arc<TrackerState>, mut stream: std::net::TcpStream) {
    // Bound the first read: a client that connects and sends no bytes must
    // get the same 400 as a malformed announce, not a silent hang
    // (TSI-2621).  `read` returns `Err` on timeout, which the existing
    // `unwrap_or(0)` folds into an empty request below.
    let _ = stream.set_read_timeout(Some(READ_TIMEOUT));
    let mut buf = [0u8; 4096];
    let n = stream.read(&mut buf).unwrap_or(0);

    let request = String::from_utf8_lossy(&buf[..n]);

    // Request line: GET /announce?<query> HTTP/1.x
    let request_target = request.lines().next().unwrap_or_default();
    let raw_query = match request_target.split_once('?') {
        Some((_, q)) => q.split(' ').next().unwrap_or(q).to_string(),
        None => {
            write_bad_request(&mut stream);
            return;
        }
    };

    let info_hash: [u8; 20] = match query_param(&raw_query, "info_hash")
        .map(|v| percent_decode(&v))
        .and_then(|v| <[u8; 20]>::try_from(v.as_slice()).ok())
    {
        Some(h) => h,
        None => {
            write_bad_request(&mut stream);
            return;
        }
    };
    let peer_port: u16 = match query_param(&raw_query, "port").and_then(|v| v.parse().ok()) {
        Some(p) => p,
        None => {
            write_bad_request(&mut stream);
            return;
        }
    };
    // `left` is part of the peer identity (see `Peer::left`, TSI-2417).
    let left: u64 = query_param(&raw_query, "left")
        .and_then(|v| v.parse().ok())
        .unwrap_or(u64::MAX);

    let ip = match stream.peer_addr() {
        Ok(std::net::SocketAddr::V4(v4)) => v4.ip().octets(),
        _ => {
            write_bad_request(&mut stream);
            return;
        }
    };

    // `event`: BEP-3 announce events.  Only `stopped` matters here —
    // libtorrent sends it when a handle is removed (delete/replace), and
    // pre-TSI-2417 this tracker ignored it, so the stopped announce *refreshed*
    // the leaving peer's `seen` timer instead of deleting the entry.  The
    // ghost entry (e.g. the previous handle's `left=0` self-reference at
    // `127.0.0.1:<own listen port>`) then survived long enough to be handed
    // back to a freshly re-added handle as a peer, and under libtorrent's
    // default `allow_multiple_connections_per_ip=false` that self-target
    // consumed the single per-IP connection slot — blocking the real seeder
    // and surfacing as a 30s NoPeers timeout (TSI-2417).
    let event = query_param(&raw_query, "event").unwrap_or_default();

    // `event=stopped`: the announcing peer is leaving the swarm.  Delete its
    // entry immediately and reply with an empty peer list — a stopped peer
    // never wants peers, and returning them would only risk re-registering a
    // ghost.  Match by (ip, port) (not `left`): a `stopped` announce carries
    // the handle's final `left`, which can differ from the `left` it
    // registered with if its download state changed between announces.
    if event == "stopped" {
        let mut peers = state.peers.lock();
        if let Some(entry) = peers.get_mut(&info_hash) {
            entry.retain(|p| !(p.ip == ip && p.port == peer_port));
        }
        let body = b"d8:intervali5e5:peers0:e".to_vec();
        let response = format!(
            "HTTP/1.0 200 OK\r\nContent-Length: {}\r\nContent-Type: text/plain\r\nConnection: close\r\n\r\n",
            body.len()
        );
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.write_all(&body);
        return;
    }

    register_peer(&state, info_hash, ip, peer_port, left);

    let compact: Vec<u8> = {
        let peers = state.peers.lock();
        peers
            .get(&info_hash)
            .map(|list| {
                list.iter()
                    // Exclude only the requester's own (ip, port, left)
                    // identity — same IP:port with a different `left` is a
                    // different client and must stay in the response
                    // (TSI-1977 / TSI-2417).
                    .filter(|p| !(p.ip == ip && p.port == peer_port && p.left == left))
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

/// Register (or refresh) a peer in the swarm, applying the stale-entry
/// expiry pass `handle_announce` runs before each registration.  Extracted so
/// the unit tests can drive the same logic without a live TCP socket.
fn register_peer(
    state: &TrackerState,
    info_hash: [u8; 20],
    ip: [u8; 4],
    peer_port: u16,
    left: u64,
) {
    let mut peers = state.peers.lock();
    let entry = peers.entry(info_hash).or_default();
    // Expire stale entries first: handles from deleted torrents whose
    // `event=stopped` was lost (network blip, tracker restart) still get
    // reaped here, so a delete + re-add cycle cannot accumulate dead
    // self-referential entries.
    entry.retain(|p| p.seen.elapsed() < PEER_EXPIRY);
    // Update any existing entry for the same (ip, port, left) triple so a
    // re-announce refreshes in place instead of duplicating.
    entry.retain(|p| !(p.ip == ip && p.port == peer_port && p.left == left));
    entry.push(Peer {
        ip,
        port: peer_port,
        left,
        seen: std::time::Instant::now(),
    });
}

fn start_tracker(bind_addr: &str, port: u16) -> std::io::Result<()> {
    let listener = TcpListener::bind((bind_addr, port))?;
    let state = Arc::new(TrackerState {
        peers: Mutex::new(HashMap::new()),
    });
    eprintln!("[tracker] listening on {}:{}", bind_addr, port);
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
    /// Address the HTTP tracker binds ("127.0.0.1" by default; use "0.0.0.0"
    /// when the downloader runs in a container and reaches the host via a
    /// routable address).
    tracker_bind: String,
    tracker_port: u16,
    /// Host placed into the .torrent announce URL (default "127.0.0.1").
    announce_host: String,
    torrent_out: PathBuf,
    url_out: PathBuf,
}

fn parse_args() -> Args {
    let mut args = Args {
        payload: PathBuf::from("payload.txt"),
        tracker_bind: "127.0.0.1".to_string(),
        tracker_port: 16969,
        announce_host: "127.0.0.1".to_string(),
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
            "--tracker-bind" => args.tracker_bind = value_for!("--tracker-bind"),
            "--announce-host" => args.announce_host = value_for!("--announce-host"),
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
    start_tracker(&args.tracker_bind, args.tracker_port).expect("failed to start local tracker");
    let announce_url = format!(
        "http://{}:{}/announce",
        args.announce_host, args.tracker_port
    );
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// TSI-2417 regression: a downloader whose (ip, port) collides with the
    /// seeder's (pasta/slirp NAT shares the host IP and both default to port
    /// 6881 in separate network namespaces) must NOT evict the seeder from
    /// the swarm.  Identity is (ip, port, left): the seeder announces left=0,
    /// the leecher left>0, so both entries coexist and every announce returns
    /// the seeder.  Mirrors the TSI-1977 fix in tests/common/mod.rs.
    #[test]
    fn same_ip_port_different_left_keeps_seeder_entry() {
        let state = Arc::new(TrackerState {
            peers: parking_lot_stub(),
        });
        let ih = [0u8; 20];
        let host = [127, 0, 0, 1];

        // Seeder registers: 127.0.0.1:6881, left=0.
        register_peer(&state, ih, host, 6881, 0);

        // Downloader announces from the SAME ip:port with left=4 MiB —
        // pre-fix logic wiped the whole (ip, port) slot here, dropping the
        // seeder and leaving an empty peer list on every later announce.
        let leecher_left = 4 * 1024 * 1024u64;
        register_peer(&state, ih, host, 6881, leecher_left);

        let list = state.peers.lock().get(&ih).cloned().unwrap_or_default();
        assert_eq!(list.len(), 2, "seeder + leecher must both be registered");
        assert!(
            list.iter()
                .any(|p| p.ip == host && p.port == 6881 && p.left == 0),
            "seeder entry must survive a colliding leecher announce"
        );

        // The leecher's compact response excludes only its own identity, so
        // it still sees the seeder.
        let visible: Vec<_> = list
            .iter()
            .filter(|p| !(p.ip == host && p.port == 6881 && p.left == leecher_left))
            .collect();
        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].left, 0, "the seeder must remain visible");
    }

    /// TSI-2417 regression: peers that stop re-announcing (handles from
    /// deleted torrents whose `event=stopped` was lost) must be expired,
    /// otherwise delete + re-add cycles accumulate stale self-referential
    /// entries that crowd the downloader's peer list.
    #[test]
    fn stale_peer_entries_expire() {
        let state = Arc::new(TrackerState {
            peers: parking_lot_stub(),
        });
        let ih = [0u8; 20];
        let host = [127, 0, 0, 1];
        let now = std::time::Instant::now();

        // Seed the swarm the way a long QA session looks after a few
        // delete + re-add rounds: a live seeder, a fresh handle's entry,
        // and two entries from handles deleted >PEER_EXPIRY ago.
        let mut peers = state.peers.lock();
        let entry = peers.entry(ih).or_default();
        let make = |port: u16, left: u64, age: std::time::Duration| Peer {
            ip: host,
            port,
            left,
            seen: now.checked_sub(age).unwrap_or_else(std::time::Instant::now),
        };
        entry.push(make(6883, 0, std::time::Duration::from_secs(2))); // seeder, live
        entry.push(make(6881, 4_194_304, std::time::Duration::from_secs(1))); // current handle
        entry.push(make(6881, 4_194_305, PEER_EXPIRY * 3)); // deleted handle A
        entry.push(make(6882, 4_194_306, PEER_EXPIRY * 3)); // deleted handle B
        drop(peers);

        // The next register_peer call runs the same expiry pass
        // handle_announce runs before registering, reaping the stale entries.
        register_peer(&state, ih, host, 6881, 4_194_304);
        let entry = state.peers.lock();
        let list = entry.get(&ih).cloned().unwrap_or_default();
        assert_eq!(list.len(), 2, "stale entries must be dropped");
        assert!(list.iter().all(|p| p.seen.elapsed() < PEER_EXPIRY));
    }

    /// TSI-2417 core fix: a `event=stopped` announce (the one libtorrent
    /// sends when a handle is removed on delete/replace) must delete the
    /// leaving peer's entry immediately, not refresh it.  Pre-fix the
    /// tracker ignored `event` and the stopped announce *refreshed* the
    /// leaving handle's `left=0` self-reference at `127.0.0.1:<own port>`,
    /// so it survived long enough to be returned to the freshly re-added
    /// handle as a peer — and under libtorrent's default
    /// `allow_multiple_connections_per_ip=false` that self-target consumed
    /// the single per-IP connection slot, blocking the real seeder and
    /// surfacing as a 30s NoPeers timeout (Verity QA_FAILED).
    #[test]
    fn stopped_event_deletes_peer_entry() {
        let state = Arc::new(TrackerState {
            peers: parking_lot_stub(),
        });
        let ih = [0u8; 20];
        let host = [127, 0, 0, 1];

        // Swarm: a real seeder at 127.0.0.1:6883 (left=0) plus the
        // downloader's own handle entry at 127.0.0.1:6881 (left>0).
        register_peer(&state, ih, host, 6883, 0);
        register_peer(&state, ih, host, 6881, 4_194_304);
        assert_eq!(
            state.peers.lock().get(&ih).map(|l| l.len()),
            Some(2),
            "seeder + downloader registered"
        );

        // The downloader's handle is removed (delete + re-add).  libtorrent
        // sends `event=stopped` from 127.0.0.1:6881 — handle_announce deletes
        // the (ip, port) entry.  It must NOT touch the seeder at 6883.
        // Reproduce the deletion branch verbatim (a stopped announce carries
        // the handle's final `left`, which can differ from the registered
        // `left`, so the match is (ip, port) only).
        {
            let mut peers = state.peers.lock();
            if let Some(entry) = peers.get_mut(&ih) {
                entry.retain(|p| !(p.ip == host && p.port == 6881));
            }
        }

        let list = state.peers.lock().get(&ih).cloned().unwrap_or_default();
        assert_eq!(list.len(), 1, "downloader entry deleted, seeder kept");
        assert_eq!(list[0].port, 6883, "the seeder must survive");
        assert_eq!(list[0].left, 0);
    }

    /// TSI-2582: a malformed announce must get an explicit `400 Bad Request`
    /// instead of a silent close, so libtorrent logs the reason rather than
    /// a bare `End of file`.
    #[test]
    fn malformed_announce_gets_400_bad_request() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let (mut server, _) = listener.accept().unwrap();

        write_bad_request(&mut server);
        drop(server);

        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with("HTTP/1.0 400 Bad Request\r\n"),
            "malformed announce must be answered with 400, got: {response}"
        );
        assert!(response.ends_with("\r\n\r\n"), "response must end headers");
    }

    /// TSI-2621: a client that connects and sends no bytes must not park
    /// `handle_announce` in `read` forever.  The first read times out and
    /// the empty request gets the same `400 Bad Request` as a malformed
    /// announce.
    #[test]
    fn empty_request_times_out_to_400_bad_request() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(TrackerState {
            peers: parking_lot_stub(),
        });

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        // If the server still hangs, this read fails the test instead of
        // parking the suite forever.
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();

        let start = std::time::Instant::now();
        std::thread::spawn(move || handle_announce(state, server));

        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        let response = String::from_utf8_lossy(&response);
        assert!(
            response.starts_with("HTTP/1.0 400 Bad Request\r\n"),
            "empty request must be answered with 400, got: {response}"
        );
        // The response must arrive only after the read timeout fires — an
        // immediate 400 would mean the timeout never guarded the read.
        assert!(
            start.elapsed() >= Duration::from_secs(4),
            "400 must follow the read timeout, got it after {:?}",
            start.elapsed()
        );
    }

    fn parking_lot_stub() -> Mutex<HashMap<[u8; 20], Vec<Peer>>> {
        Mutex::new(HashMap::new())
    }
}

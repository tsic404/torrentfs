//! Shared infrastructure for the QA seeder examples (`torrentfs-selfseed-env`
//! and `torrentfs-mffs-seeder`): a minimal HTTP tracker, bencoding helpers,
//! and a keep-alive seeding loop. Both examples seed a deterministic local
//! swarm fully offline (no DHT/UPnP/NAT-PMP/public trackers) so QA read
//! scenarios do not depend on unreachable public seeders. Each example keeps
//! only its torrent `info`-dict construction; everything here is identical
//! between them and lives once so tracker fixes (e.g. the `event=stopped`
//! deletion, `min interval` advertising, and `left` peer-identity fixes) are
//! applied in a single place instead of hand-synced across two copies.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;

use torrentfs::download::{TorrentHandle, TorrentState};

/// Piece length used when bencoding the seeder torrents.
pub const PIECE_LEN: usize = 262_144; // 256 KiB

// ── minimal bencoding ────────────────────────────────────────────────────────

pub fn bencode_int(i: i64) -> Vec<u8> {
    format!("i{}e", i).into_bytes()
}

pub fn bencode_bytes(b: &[u8]) -> Vec<u8> {
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
    /// appear to the tracker as `127.0.0.1:<same listen port>`.
    left: u64,
    /// Last announce time (Instant::now at registration).  Entries not
    /// re-announced within [`PEER_EXPIRY`] are dropped — without expiry, every
    /// delete + re-add of the same info_hash leaves the previous handle's
    /// entry behind, and the new handle's peer list fills with stale
    /// self-referential entries (`127.0.0.1:<own listen port>`) that it then
    /// wastes its connection attempts on.
    seen: std::time::Instant,
}

/// Announce interval (seconds) advertised by the tracker as both `interval`
/// and `min interval`, and used by the seeder's keep-alive loop and
/// `min_announce_interval` setting.  Must stay below [`PEER_EXPIRY`] so a live
/// seeder re-announces before the tracker reaps its entry.  Advertising
/// `min interval` matters: libtorrent defaults a missing `min interval` to
/// 30s, which would clamp the 5s interval up to 30s and race the 30s expiry.
const ANNOUNCE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Drop peers that have not re-announced within this window.  The tracker
/// advertises [`ANNOUNCE_INTERVAL`], so a live client announces well inside
/// 30s.
const PEER_EXPIRY: std::time::Duration = std::time::Duration::from_secs(30);

/// Bound on the wait for the first announce bytes.  A client that connects
/// and sends nothing would otherwise park `handle_announce` in `read`
/// forever; on timeout the empty request is answered with the
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

/// Decode an `info_hash` query value into its 20 raw bytes.
///
/// libtorrent sends `info_hash` in one of two forms: `%XX`-escaped raw
/// bytes, or bare ASCII hex (`info_hash=6be64a…`).  `percent_decode` only
/// handles the former; the latter comes through as 40 ASCII bytes, so the
/// direct `[u8; 20]` conversion rejects it and the announce wrongly gets a
/// `400 Bad Request`.  Fall back to hex decoding when the
/// percent-decoded value is not already 20 raw bytes.
fn decode_info_hash(value: &str) -> Option<[u8; 20]> {
    let raw = percent_decode(value);
    if let Ok(hash) = <[u8; 20]>::try_from(raw.as_slice()) {
        return Some(hash);
    }

    let hex = std::str::from_utf8(&raw).ok()?;
    if hex.len() != 40 {
        return None;
    }
    let mut out = [0u8; 20];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

/// Write a minimal `400 Bad Request` response and close the connection.
///
/// Malformed announces used to fall through to a silent `return`, which
/// closed the socket with no response at all — libtorrent only saw
/// `End of file` and retried, hiding the reason.
fn write_bad_request(stream: &mut std::net::TcpStream) {
    let _ = stream
        .write_all(b"HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
}

/// Handle one `/announce`: register the peer, reply with a compact peer list
/// excluding the requester itself.
fn handle_announce(state: Arc<TrackerState>, mut stream: std::net::TcpStream) {
    // Bound the first read: a client that connects and sends no bytes must
    // get the same 400 as a malformed announce, not a silent hang.
    // `read` returns `Err` on timeout, which the existing
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

    let info_hash: [u8; 20] =
        match query_param(&raw_query, "info_hash").and_then(|v| decode_info_hash(&v)) {
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
    // `left` is part of the peer identity.
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

    // `event`: BEP-3 announce events.  Only `stopped` matters here — libtorrent
    // sends it when a handle is removed (delete/replace). Previously this
    // tracker ignored it, so the stopped announce *refreshed* the leaving
    // peer's `seen` timer instead of deleting the entry; the ghost
    // self-reference survived and, under `allow_multiple_connections_per_ip=false`,
    // consumed the single per-IP slot — blocking the real seeder and surfacing
    // as a 30s NoPeers timeout.
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
        let body = format!(
            "d8:intervali{}e12:min intervali{}e5:peers0:e",
            ANNOUNCE_INTERVAL.as_secs(),
            ANNOUNCE_INTERVAL.as_secs()
        )
        .into_bytes();
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
                    // different client and must stay in the response.
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

    // d8:intervali<interval>e12:min intervali<interval>e5:peers<len>:<compact_peer_list>e
    let mut body = format!(
        "d8:intervali{}e12:min intervali{}e5:peers",
        ANNOUNCE_INTERVAL.as_secs(),
        ANNOUNCE_INTERVAL.as_secs()
    )
    .into_bytes();
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

/// Start the loopback HTTP tracker in a background thread and return the port
/// it is listening on once it is bound.
///
/// Pass `0` to let the OS assign a free ephemeral port (the convention
/// `tests/common/mod.rs::MiniTracker` uses) and publish the returned port in
/// the announce URL.  A fixed port collides with a tracker left behind by a
/// killed run: the stale socket stays in `LISTEN` and Linux rejects a second
/// bind there regardless of `SO_REUSEADDR`, which only relaxes the `TIME_WAIT`
/// case `TcpListener::bind` already covers.
pub fn start_tracker(bind_addr: &str, port: u16) -> std::io::Result<u16> {
    let listener = TcpListener::bind((bind_addr, port))?;
    let bound_port = listener.local_addr()?.port();
    let state = Arc::new(TrackerState {
        peers: Mutex::new(HashMap::new()),
    });
    eprintln!("[tracker] listening on {}:{}", bind_addr, bound_port);
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(stream) = stream else { continue };
            let state = Arc::clone(&state);
            // One thread per announce — QA load is trivially small.
            std::thread::spawn(move || handle_announce(state, stream));
        }
    });
    Ok(bound_port)
}

// ── session + keep-alive ─────────────────────────────────────────────────────

/// Set by the SIGINT/SIGTERM handler so the keep-alive loop can observe the
/// signal instead of sleeping out the full interval (SIGKILL on a hung sleep
/// terminates the process mid-sleep, silently and without an error line).
/// Polling a 500ms sleep keeps the loop signal-responsive.
static SHUTDOWN: AtomicBool = AtomicBool::new(false);

/// Async-signal-safe handler: only stores an atomic flag, mirroring
/// `src/main.rs::handle_shutdown_signal`.
extern "C" fn handle_shutdown_signal(_sig: libc::c_int) {
    SHUTDOWN.store(true, Ordering::SeqCst);
}

/// Install SIGINT/SIGTERM handlers that flip [`SHUTDOWN`].  Call from `main`
/// before any thread spawns.
pub fn install_signal_handlers() {
    // SAFETY: installing a trivial flag-setting handler; `libc::signal`
    // is async-signal-safe to call from main before any thread spawns.
    unsafe {
        libc::signal(
            libc::SIGINT,
            handle_shutdown_signal as *const () as libc::sighandler_t,
        );
        libc::signal(
            libc::SIGTERM,
            handle_shutdown_signal as *const () as libc::sighandler_t,
        );
    }
}

/// Loopback-only session config mirroring `tests/common/mod.rs`: no DHT, no
/// UPnP/NAT-PMP, short connect timeout, aggressive announce.
pub fn session_config() -> torrentfs::TorrentfsConfig {
    let mut config = torrentfs::TorrentfsConfig::default_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(true);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.allow_multiple_connections_per_ip = Some(true);
    config.connections.peer_connect_timeout = Some(5);
    config.tracker.announce_to_all_trackers = Some(true);
    config.tracker.announce_to_all_tiers = Some(true);
    config.tracker.min_announce_interval = Some(ANNOUNCE_INTERVAL.as_secs() as i64);
    config
}

/// Wait for every torrent in `torrents` to reach a serving state, then
/// re-announce all of them at [`ANNOUNCE_INTERVAL`] forever (or until
/// SIGINT/SIGTERM) so no swarm entry expires.
///
/// One run can serve several torrents from a single session (e.g. a
/// single-file and a multi-file payload), and the tracker reaps peers per
/// info_hash — so every handle must keep announcing, not just the first.  Each
/// entry is `(label, handle)`, the label being the torrent name, which is what
/// identifies the handle in the per-poll state log.
pub fn seed_until_shutdown(torrents: &[(&str, TorrentHandle)]) {
    assert!(
        !torrents.is_empty(),
        "seeder must serve at least one torrent"
    );

    // Wait until every torrent is actually serving before declaring readiness.
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        // A signal arriving during warmup must not wait out the 60s deadline:
        // the supervisor's SIGTERM→SIGKILL grace (podman/docker: 10s) would
        // fire first, killing the process silently.
        if SHUTDOWN.load(Ordering::SeqCst) {
            eprintln!("[seeder] shutdown signal received — stopping");
            return;
        }
        let mut all_serving = true;
        for (label, handle) in torrents {
            let status = handle.status().expect("status failed");
            eprintln!(
                "[seeder] {} state={:?} progress={:.1}% seeds={} peers={}",
                label,
                status.state,
                status.progress * 100.0,
                status.num_seeds,
                status.num_peers
            );
            if status.state != TorrentState::Seeding && status.state != TorrentState::Finished {
                all_serving = false;
            }
        }
        if all_serving {
            break;
        }
        if std::time::Instant::now() > deadline {
            eprintln!("[seeder] timed out waiting for Seeding state; continuing anyway");
            break;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    for (_, handle) in torrents {
        handle.force_reannounce();
    }

    println!("[seeder] ready — Ctrl-C to stop");
    loop {
        // Re-announce at the tracker's advertised interval so the swarm entry
        // never expires: the tracker reaps peers that have not re-announced
        // within PEER_EXPIRY (30s), and libtorrent's own periodic announce is
        // not guaranteed to keep a seeding torrent's entry alive across a long
        // QA session.  Poll SHUTDOWN on a short sleep instead of sleeping the
        // full interval: a supervisor that escalates SIGTERM to SIGKILL
        // (podman/docker's 10s default grace period) would otherwise kill the
        // process mid-sleep with no error line, surfacing as an intermittent
        // silent exit after "ready".
        // ceil-division keeps SHUTDOWN polling non-empty for sub-500ms intervals.
        for _ in 0..(ANNOUNCE_INTERVAL.as_millis().div_ceil(500)) {
            if SHUTDOWN.load(Ordering::SeqCst) {
                eprintln!("[seeder] shutdown signal received — stopping");
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
        for (_, handle) in torrents {
            handle.force_reannounce();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// a downloader whose (ip, port) collides with the
    /// seeder's (pasta/slirp NAT shares the host IP and both default to port
    /// 6881 in separate network namespaces) must NOT evict the seeder from
    /// the swarm.  Identity is (ip, port, left): the seeder announces left=0,
    /// the leecher left>0, so both entries coexist and every announce returns
    /// the seeder.  Mirrors the fix in tests/common/mod.rs.
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

    /// peers that stop re-announcing (handles from
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

    /// An `event=stopped` announce (sent on handle delete/replace) must delete
    /// the leaving peer's entry immediately, not refresh it. Pre-fix the
    /// tracker ignored `event`, so the stopped announce refreshed the leaving
    /// handle's `left=0` self-reference; it survived to be returned to the
    /// re-added handle as a peer and, under `allow_multiple_connections_per_ip=false`,
    /// consumed the single per-IP slot — blocking the real seeder (30s NoPeers).
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

    /// a malformed announce must get an explicit `400 Bad Request`
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

    /// a client that connects and sends no bytes must not park
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

    /// a successful announce response must advertise `min interval` equal to
    /// [`ANNOUNCE_INTERVAL`].  libtorrent defaults a missing `min interval` to
    /// 30s, which clamps the 5s `interval` up to 30s and races [`PEER_EXPIRY`]
    /// (the seeder entry expires before the next announce, surfacing as an
    /// empty `peers` list).  Asserting the field guards that regression.
    #[test]
    fn announce_response_advertises_min_interval() {
        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let addr = listener.local_addr().unwrap();
        let state = Arc::new(TrackerState {
            peers: parking_lot_stub(),
        });

        let mut client = std::net::TcpStream::connect(addr).unwrap();
        client
            .set_read_timeout(Some(Duration::from_secs(10)))
            .unwrap();
        let (server, _) = listener.accept().unwrap();
        std::thread::spawn(move || handle_announce(state, server));

        // Bare ASCII hex info_hash (decode_info_hash's alternate form) keeps
        // the request human-readable; a real announce also carries peer_id
        // and port, which this tracker ignores beyond `port`.
        let request = concat!(
            "GET /announce?info_hash=0102030405060708090a0b0c0d0e0f1011121314",
            "&peer_id=0123456789abcdefghij&port=6881&left=0&event=started ",
            "HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n"
        );
        client.write_all(request.as_bytes()).unwrap();

        let mut response = Vec::new();
        client.read_to_end(&mut response).unwrap();
        let response = String::from_utf8_lossy(&response);
        let expected = format!("12:min intervali{}e", ANNOUNCE_INTERVAL.as_secs());
        assert!(
            response.contains(&expected),
            "announce response must advertise {expected}, got: {response}"
        );
    }

    /// `%XX`-encoded info_hash still decodes to 20 raw bytes.
    #[test]
    fn percent_encoded_info_hash_decodes() {
        let encoded = "%01%23%45%67%89%AB%CD%EF%01%23%45%67%89%AB%CD%EF%FE%DC%BA%98";
        let hash = decode_info_hash(encoded).expect("percent-encoded hash must decode");
        assert_eq!(
            hash,
            [
                0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab,
                0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98,
            ]
        );
    }

    /// bare ASCII hex info_hash (libtorrent's alternate form) must
    /// decode via the `from_str_radix` fallback instead of being rejected.
    #[test]
    fn bare_hex_info_hash_decodes() {
        let hex = "0102030405060708090a0b0c0d0e0f1011121314";
        let hash = decode_info_hash(hex).expect("bare hex hash must decode");
        assert_eq!(
            hash,
            [
                0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e,
                0x0f, 0x10, 0x11, 0x12, 0x13, 0x14,
            ]
        );
    }

    /// uppercase hex digits are accepted too; anything that is not
    /// 20 raw bytes or exactly 40 hex digits stays rejected.
    #[test]
    fn malformed_info_hash_rejected() {
        let uppercase = "0102030405060708090A0B0C0D0E0F1011121314";
        assert!(
            decode_info_hash(uppercase).is_some(),
            "uppercase hex must decode"
        );

        assert!(
            decode_info_hash("0102").is_none(),
            "short hex must be rejected"
        );
        assert!(
            decode_info_hash("").is_none(),
            "empty value must be rejected"
        );
        let mut forty_non_hex = String::from("z");
        forty_non_hex.push_str(&"0".repeat(39));
        assert!(
            decode_info_hash(&forty_non_hex).is_none(),
            "non-hex 40-byte value must be rejected"
        );
    }

    fn parking_lot_stub() -> Mutex<HashMap<[u8; 20], Vec<Peer>>> {
        Mutex::new(HashMap::new())
    }
}

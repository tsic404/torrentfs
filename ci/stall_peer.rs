//! Stalled BitTorrent peer for the slow-swarm end-to-end case.
//!
//! The libtorrent self-seed seeder cannot produce "seeder connected, zero
//! progress": it serves every requested piece immediately and the QA swarm is
//! loopback-only, so a read finishes in milliseconds.  This module speaks just
//! enough of the peer protocol to look like a complete seeder to the client —
//! handshake, full bitfield, unchoke — and then never sends a payload byte, so
//! the client's `download_rate` stays 0 while `num_seeds` is 1 and a parked
//! read makes `.stats` report `⚠ Slow swarm`.
//!
//! Discovery stays on the normal path: the peer announces itself to the same
//! tracker `run_self_seed_env.sh` starts (`left=0`, its own listen port) and
//! re-announces on the tracker interval, the way the libtorrent seeder it
//! replaces registers itself.  The client connects to the announced port, and
//! the connection is answered by the handshake below — a peer that never
//! delivers is the fixture, not a failed connection.

use std::io::{ErrorKind, Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use crate::seeder_common::shutdown_requested;

/// The torrent this peer claims to seed: the info hash the client handshakes
/// with, and the piece count its bitfield must cover.
pub struct StallTorrent {
    pub info_hash: [u8; 20],
    pub num_pieces: u32,
}

/// BEP-3 handshake: 19-byte pstrlen-prefixed protocol string, 8 reserved
/// bytes, info hash, peer id.
const PROTOCOL: &[u8; 19] = b"BitTorrent protocol";
const HANDSHAKE_LEN: usize = 68;
/// Tracker re-announce period.  Matches the self-seed tracker's advertised
/// interval so the entry never expires (it reaps peers after 30s).
const ANNOUNCE_INTERVAL: Duration = Duration::from_secs(5);
/// Bound on the wait for a client handshake: a connection that sends nothing
/// must not hold its thread forever.
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(5);

/// Own the stalled peer for the life of the process: bind a listen port,
/// announce it to the tracker, and answer every client connection with the
/// handshake + bitfield.  Returns once SIGINT/SIGTERM asks the process to stop.
pub fn run(torrents: Vec<StallTorrent>, announce_url: &str) -> std::io::Result<()> {
    assert!(
        !torrents.is_empty(),
        "stall peer needs at least one torrent"
    );
    let torrents = Arc::new(torrents);
    let target = AnnounceTarget::parse(announce_url)?;
    let peer_id = peer_id();

    // 0.0.0.0 so the peer is reachable at whichever address the tracker saw the
    // announce from (loopback in the e2e, a routable host address otherwise).
    let listener = TcpListener::bind(("0.0.0.0", 0))?;
    let port = listener.local_addr()?.port();
    eprintln!("[stall-peer] listening on 0.0.0.0:{port}");

    {
        let torrents = Arc::clone(&torrents);
        let target = target.clone();
        std::thread::spawn(move || announce_loop(&target, &torrents, port, peer_id));
    }
    {
        let torrents = Arc::clone(&torrents);
        std::thread::spawn(move || accept_loop(listener, torrents, peer_id));
    }

    println!("[stall-peer] ready — Ctrl-C to stop");
    while !shutdown_requested() {
        std::thread::sleep(Duration::from_millis(500));
    }
    eprintln!("[stall-peer] shutdown signal received — stopping");
    Ok(())
}

// ── tracker announce ─────────────────────────────────────────────────────────

/// Announce URL split into the parts the HTTP request needs.  The self-seed
/// tracker is IPv4-only by construction, so a host is a name or a dotted quad
/// and the port is explicit in the generated URL.
#[derive(Clone)]
struct AnnounceTarget {
    host: String,
    port: u16,
    path: String,
}

impl AnnounceTarget {
    fn parse(url: &str) -> std::io::Result<Self> {
        let rest = url.strip_prefix("http://").ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("announce URL must be http, got '{url}'"),
            )
        })?;
        let (authority, path) = match rest.split_once('/') {
            Some((authority, path)) => (authority, format!("/{path}")),
            None => (rest, "/announce".to_string()),
        };
        let (host, port) = authority.rsplit_once(':').ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("announce URL is missing a port: '{url}'"),
            )
        })?;
        let port = port.parse().map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("announce URL has a non-numeric port: '{url}'"),
            )
        })?;
        Ok(Self {
            host: host.to_string(),
            port,
            path,
        })
    }
}

fn announce_loop(target: &AnnounceTarget, torrents: &[StallTorrent], port: u16, peer_id: [u8; 20]) {
    let mut first = true;
    loop {
        if shutdown_requested() {
            return;
        }
        for torrent in torrents {
            match announce(target, torrent, port, &peer_id, first) {
                Ok(()) => eprintln!(
                    "[stall-peer] announced {} (left=0, port={port})",
                    hex::encode(torrent.info_hash)
                ),
                // The tracker may not be up yet on the first pass; the next
                // tick retries, so a failure is logged rather than fatal.
                Err(e) => eprintln!("[stall-peer] announce failed: {e}"),
            }
        }
        first = false;
        for _ in 0..ANNOUNCE_INTERVAL.as_millis().div_ceil(500) {
            if shutdown_requested() {
                return;
            }
            std::thread::sleep(Duration::from_millis(500));
        }
    }
}

/// One HTTP announce (`left=0` marks this peer a seeder in the swarm).  The
/// response body's peer list is of no use to a seeder — it is read only to
/// learn whether the tracker accepted the announce: a registered announce and
/// a `400 Bad Request` rejection both end with the tracker closing the
/// connection, so only the status line tells them apart.
fn announce(
    target: &AnnounceTarget,
    torrent: &StallTorrent,
    port: u16,
    peer_id: &[u8; 20],
    first: bool,
) -> std::io::Result<()> {
    let mut query = format!(
        "{}?info_hash={}&peer_id={}&port={port}&uploaded=0&downloaded=0&left=0&compact=1&numwant=50",
        target.path,
        percent_encode(&torrent.info_hash),
        percent_encode(peer_id),
    );
    if first {
        query.push_str("&event=started");
    }
    let mut stream = std::net::TcpStream::connect((target.host.as_str(), target.port))?;
    stream.set_read_timeout(Some(HANDSHAKE_TIMEOUT))?;
    stream.write_all(
        format!(
            "GET {query} HTTP/1.0\r\nHost: {}:{}\r\nConnection: close\r\n\r\n",
            target.host, target.port
        )
        .as_bytes(),
    )?;
    let mut response = Vec::new();
    let _ = stream.read_to_end(&mut response);
    match response_status(&response) {
        Some(200) => Ok(()),
        Some(code) => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("tracker rejected the announce with HTTP {code}"),
        )),
        None => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "tracker closed the connection without an HTTP status line",
        )),
    }
}

/// Status code of an HTTP response's first line, or `None` when the response
/// carries no status line at all (the tracker dropped the connection).
fn response_status(response: &[u8]) -> Option<u16> {
    let first_line = response.split(|&byte| byte == b'\n').next()?;
    let line = std::str::from_utf8(first_line).ok()?;
    let (version, rest) = line.split_once(' ')?;
    if !version.starts_with("HTTP/") {
        return None;
    }
    rest.split_whitespace().next()?.parse().ok()
}

/// Percent-encode raw bytes for a tracker query value.  Everything outside the
/// unreserved set is escaped, so an info hash byte that is not a printable
/// character cannot corrupt the query.
fn percent_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 3);
    for byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~') {
            out.push(*byte as char);
        } else {
            out.push_str(&format!("%{byte:02X}"));
        }
    }
    out
}

// ── peer protocol ────────────────────────────────────────────────────────────

fn accept_loop(listener: TcpListener, torrents: Arc<Vec<StallTorrent>>, peer_id: [u8; 20]) {
    for stream in listener.incoming() {
        if shutdown_requested() {
            return;
        }
        let Ok(stream) = stream else { continue };
        let torrents = Arc::clone(&torrents);
        std::thread::spawn(move || {
            // One thread per client — QA load is trivially small.
            let _ = serve_peer(stream, &torrents, &peer_id);
        });
    }
}

/// Handshake with one client and advertise a complete bitfield, then hold the
/// connection open while sending nothing.
fn serve_peer(
    mut stream: std::net::TcpStream,
    torrents: &[StallTorrent],
    peer_id: &[u8; 20],
) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_millis(500)))?;
    stream.set_write_timeout(Some(HANDSHAKE_TIMEOUT))?;

    // The client sends its handshake as soon as the connection is up, so the
    // accepting side learns the info hash before replying — the reply must
    // carry the same one.
    let info_hash = match read_handshake(&mut stream)? {
        Some(hash) => hash,
        None => return Ok(()),
    };
    let Some(torrent) = torrents.iter().find(|t| t.info_hash == info_hash) else {
        eprintln!(
            "[stall-peer] client asked for an unknown info hash {} — closing",
            hex::encode(info_hash)
        );
        return Ok(());
    };

    let mut handshake = Vec::with_capacity(HANDSHAKE_LEN);
    handshake.push(PROTOCOL.len() as u8);
    handshake.extend_from_slice(PROTOCOL);
    handshake.extend_from_slice(&[0u8; 8]);
    handshake.extend_from_slice(&info_hash);
    handshake.extend_from_slice(peer_id);
    stream.write_all(&handshake)?;
    stream.write_all(&bitfield_message(torrent.num_pieces))?;
    // Unchoke without ever sending a piece: the client is free to request, and
    // those requests are what go unanswered — a source that is connected but
    // makes no progress.
    stream.write_all(&[0, 0, 0, 1, 1])?;
    stream.flush()?;
    eprintln!(
        "[stall-peer] client connected for {} ({} pieces) — staying silent",
        hex::encode(info_hash),
        torrent.num_pieces
    );

    let mut buf = [0u8; 4096];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return Ok(()),
            Ok(_) => continue,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {
                if shutdown_requested() {
                    return Ok(());
                }
            }
            Err(e) => return Err(e),
        }
    }
}

/// Read a peer's 68-byte handshake, polling for shutdown while waiting.
/// `Ok(None)` means the peer went away or sent something that is not a
/// BitTorrent handshake for a protocol we speak.
fn read_handshake(stream: &mut std::net::TcpStream) -> std::io::Result<Option<[u8; 20]>> {
    let mut buf = [0u8; HANDSHAKE_LEN];
    let mut filled = 0;
    let deadline = std::time::Instant::now() + HANDSHAKE_TIMEOUT;
    while filled < buf.len() {
        if shutdown_requested() || std::time::Instant::now() > deadline {
            return Ok(None);
        }
        match stream.read(&mut buf[filled..]) {
            Ok(0) => return Ok(None),
            Ok(n) => filled += n,
            Err(e) if e.kind() == ErrorKind::WouldBlock || e.kind() == ErrorKind::TimedOut => {}
            Err(e) => return Err(e),
        }
    }
    if buf[0] != PROTOCOL.len() as u8 || &buf[1..20] != PROTOCOL {
        return Ok(None);
    }
    let mut info_hash = [0u8; 20];
    info_hash.copy_from_slice(&buf[28..48]);
    Ok(Some(info_hash))
}

/// A `bitfield` message (id 5) with every piece set, trailing bits of a
/// non-byte-aligned piece count masked off so the peer never claims a piece
/// index the torrent does not have.
fn bitfield_message(num_pieces: u32) -> Vec<u8> {
    let bytes = num_pieces.div_ceil(8) as usize;
    let mut payload = vec![5u8];
    payload.resize(1 + bytes, 0xff);
    let used_bits = num_pieces % 8;
    if used_bits != 0 {
        payload[bytes] &= 0xff << (8 - used_bits);
    }
    let mut message = (payload.len() as u32).to_be_bytes().to_vec();
    message.extend_from_slice(&payload);
    message
}

/// Azureus-style peer id: `-STALLP-` plus the process id, unique per run so
/// two stalled peers on one host do not announce the same identity.
fn peer_id() -> [u8; 20] {
    let mut id = [b'0'; 20];
    let prefix = b"-STALLP-";
    id[..prefix.len()].copy_from_slice(prefix);
    let suffix = format!("{:012}", std::process::id());
    id[prefix.len()..].copy_from_slice(suffix.as_bytes());
    id
}

#[cfg(test)]
mod tests {
    use super::*;

    /// the bitfield must describe exactly the torrent's pieces: a full byte per
    /// 8 pieces, and no set bit past the piece count (a claimed piece the
    /// torrent does not have makes the peer look broken, not seeded).
    #[test]
    fn bitfield_sets_exactly_the_torrent_pieces() {
        // 16 pieces = 2 full bytes.
        assert_eq!(bitfield_message(16), vec![0, 0, 0, 3, 5, 0xff, 0xff]);
        // 9 pieces = 1 full byte + 1 masked byte (7 bits used).
        assert_eq!(bitfield_message(9), vec![0, 0, 0, 3, 5, 0xff, 0x80]);
        // 1 piece = a single byte with only the first bit.
        assert_eq!(bitfield_message(1), vec![0, 0, 0, 2, 5, 0x80]);
    }

    /// One-shot TCP "tracker": captures the request bytes and answers with
    /// `response`, so a test can assert on what `announce()` really wrote
    /// instead of on a query string rebuilt from the same literals.
    fn stub_tracker(response: &'static str) -> (u16, std::sync::mpsc::Receiver<String>) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind stub tracker");
        let port = listener.local_addr().expect("stub tracker address").port();
        let (sender, receiver) = std::sync::mpsc::channel();
        std::thread::spawn(move || {
            if let Ok((mut stream, _)) = listener.accept() {
                let mut buf = [0u8; 4096];
                // The announce request is one small `write_all`, so one read
                // carries it whole.
                let read = stream.read(&mut buf).unwrap_or(0);
                let _ = sender.send(String::from_utf8_lossy(&buf[..read]).into_owned());
                let _ = stream.write_all(response.as_bytes());
            }
        });
        (port, receiver)
    }

    /// The stall peer for these tests: a 20-byte info hash whose first four
    /// bytes are mixed printable/escaped, and a fixed peer id.
    fn announce_fixture(port: u16) -> (AnnounceTarget, StallTorrent) {
        let mut info_hash = [0u8; 20];
        info_hash[..4].copy_from_slice(&[0x00, 0x0f, 0xff, 0x41]);
        (
            AnnounceTarget {
                host: "127.0.0.1".to_string(),
                port,
                path: "/announce".to_string(),
            },
            StallTorrent {
                info_hash,
                num_pieces: 16,
            },
        )
    }

    /// what `announce()` actually puts on the wire must be the query the
    /// tracker registers this peer from — raw percent-escaped identity bytes
    /// (a binary info hash or peer id otherwise corrupts the query), `left=0`
    /// so the swarm lists it as a seeder, its own listen port, and the
    /// `event=started` join — and it must read the TCP host:port it dialed.
    /// Driven through the real `announce()`, so any change to the produced
    /// request fails this test.
    #[test]
    fn announce_sends_the_registration_query() {
        let (port, requests) =
            stub_tracker("HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let (target, torrent) = announce_fixture(port);
        let peer_id = *b"-STALLP-0123456789ab";

        announce(&target, &torrent, 6881, &peer_id, true).expect("announce must succeed");

        let request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("stub tracker received no request");
        let request_line = request.lines().next().unwrap_or_default().trim_end();
        assert!(
            request_line.starts_with("GET /announce?info_hash=%00%0F%FFA"),
            "{request}"
        );
        assert!(
            request_line.contains("&peer_id=-STALLP-0123456789ab"),
            "{request}"
        );
        assert!(request_line.contains("&port=6881"), "{request}");
        assert!(request_line.contains("&left=0"), "{request}");
        assert!(request_line.contains("&event=started"), "{request}");
        assert!(request_line.ends_with(" HTTP/1.0"), "{request}");
        assert!(
            request.contains(&format!("Host: 127.0.0.1:{port}")),
            "{request}"
        );
    }

    /// a re-announce refreshes an existing swarm entry, so it must not carry
    /// `event=started` a second time (BEP-3: that event marks the join).
    #[test]
    fn repeat_announce_omits_the_started_event() {
        let (port, requests) =
            stub_tracker("HTTP/1.0 200 OK\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
        let (target, torrent) = announce_fixture(port);

        announce(&target, &torrent, 6881, b"-STALLP-0123456789ab", false)
            .expect("announce must succeed");

        let request = requests
            .recv_timeout(Duration::from_secs(5))
            .expect("stub tracker received no request");
        assert!(request.starts_with("GET /announce?"), "{request}");
        assert!(!request.contains("event=started"), "{request}");
    }

    /// a tracker rejection (`400 Bad Request` is what the self-seed tracker
    /// answers a malformed announce with) must surface as an error, not as
    /// `Ok` — otherwise the announce loop logs a rejected announce as a
    /// success line and the swarm silently stays empty.
    #[test]
    fn announce_reports_a_tracker_rejection() {
        let (port, requests) = stub_tracker(
            "HTTP/1.0 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
        );
        let (target, torrent) = announce_fixture(port);

        let error = announce(&target, &torrent, 6881, b"-STALLP-0123456789ab", true)
            .expect_err("a 400 response must not be reported as a successful announce");
        assert!(error.to_string().contains("400"), "{error}");
        requests
            .recv_timeout(Duration::from_secs(5))
            .expect("stub tracker received no request");
    }

    /// percent-encoding is the reason a binary info hash survives the query:
    /// every byte outside the unreserved set is escaped.
    #[test]
    fn percent_encode_escapes_binary_identity_bytes() {
        let mut info_hash = [0u8; 20];
        info_hash[..4].copy_from_slice(&[0x00, 0x0f, 0xff, 0x41]);
        assert_eq!(
            percent_encode(&info_hash),
            format!("%00%0F%FFA{}", "%00".repeat(16))
        );
        assert_eq!(percent_encode(b"aZ0-_.~"), "aZ0-_.~");
    }

    /// the announce URL parser must split host, port and path — including the
    /// path the request line uses.
    #[test]
    fn announce_url_splits_into_host_port_and_path() {
        let target = AnnounceTarget::parse("http://127.0.0.1:51234/announce").unwrap();
        assert_eq!(target.host, "127.0.0.1");
        assert_eq!(target.port, 51234);
        assert_eq!(target.path, "/announce");

        let bare = AnnounceTarget::parse("http://10.0.0.7:80").unwrap();
        assert_eq!(bare.path, "/announce");

        assert!(AnnounceTarget::parse("udp://127.0.0.1:1/announce").is_err());
        assert!(AnnounceTarget::parse("http://127.0.0.1/announce").is_err());
    }
}

//! Protocol-level seed peer for the read-path acceptance cases.
//!
//! The design makes libtorrent the only BitTorrent implementation in the
//! product (F5/NG2), so a libtorrent seeder cannot prove that a *foreign* peer
//! is usable: both ends would be the same implementation.  This fixture speaks
//! the minimum a real seed sends — handshake, complete bitfield, unchoke, and
//! a PIECE reply per REQUEST — and records whether the client ever asked it
//! for a byte.  A read that returns the seeded bytes therefore proves the
//! client treated an unchoked, piece-declaring peer as a data source.
//!
//! Discovery stays on the normal path: the peer announces itself to the same
//! tracker the client uses, exactly as the libtorrent seeder it replaces does.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

const HANDSHAKE_LEN: usize = 68;
const BITFIELD_ID: u8 = 5;
const REQUEST_ID: u8 = 6;
const PIECE_ID: u8 = 7;

/// A running seed peer: announces one torrent and serves its bytes.
pub struct SeedPeer {
    port: u16,
    requests: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
}

impl SeedPeer {
    /// Bind a peer port, announce it to `announce_url`, and start serving
    /// `content` for `info_hash`.  `piece_len` must be the torrent's piece
    /// length; the bitfield declares every piece.
    pub fn start(
        info_hash: [u8; 20],
        content: Vec<u8>,
        piece_len: usize,
        announce_url: &str,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("seed peer: bind failed");
        let port = listener.local_addr().expect("seed peer: no addr").port();
        listener
            .set_nonblocking(true)
            .expect("seed peer: nonblocking");
        announce(port, &info_hash, announce_url);

        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));

        // One thread per connection: the client may reconnect while the
        // torrent settles, and each connection needs its own message loop.
        let accept_requests = Arc::clone(&requests);
        let accept_stop = Arc::clone(&stop);
        let piece_count = content.len().div_ceil(piece_len.max(1));
        thread::spawn(move || {
            let mut bitfield = vec![0u8; piece_count.div_ceil(8)];
            for piece in 0..piece_count {
                bitfield[piece / 8] |= 1 << (7 - (piece % 8));
            }
            let bitfield = Arc::new(bitfield);
            let content = Arc::new(content);
            while !accept_stop.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((sock, _)) => {
                        let requests = Arc::clone(&accept_requests);
                        let stop = Arc::clone(&accept_stop);
                        let bitfield = Arc::clone(&bitfield);
                        let content = Arc::clone(&content);
                        thread::spawn(move || {
                            serve(
                                sock,
                                &info_hash,
                                &bitfield,
                                &content,
                                piece_len,
                                &requests,
                                &stop,
                            )
                        });
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20))
                    }
                    Err(e) => {
                        eprintln!("seed peer: accept failed: {e}");
                        return;
                    }
                }
            }
        });

        Self {
            port,
            requests,
            stop,
        }
    }

    /// The listen port this peer announced.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// How many PIECE replies the peer sent to the client.
    pub fn pieces_served(&self) -> usize {
        self.requests.load(Ordering::Relaxed)
    }
}

impl Drop for SeedPeer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
    }
}

fn serve(
    mut sock: TcpStream,
    info_hash: &[u8; 20],
    bitfield: &[u8],
    content: &[u8],
    piece_len: usize,
    requests: &AtomicUsize,
    stop: &AtomicBool,
) {
    let _ = sock.set_nonblocking(false);
    let _ = sock.set_read_timeout(Some(Duration::from_secs(30)));

    let mut peer_handshake = vec![0u8; HANDSHAKE_LEN];
    if sock.read_exact(&mut peer_handshake).is_err() {
        return;
    }
    let _ = sock.write_all(&handshake(info_hash));
    // A real seed un-chokes and declares its pieces straight after the
    // handshake; waiting for the client's extended handshake would make this
    // fixture depend on a message the product is free not to send.
    let _ = sock.write_all(&frame(1, &[]));
    let _ = sock.write_all(&frame(BITFIELD_ID, bitfield));
    let _ = sock.flush();

    loop {
        if stop.load(Ordering::Relaxed) {
            return;
        }
        let mut len_buf = [0u8; 4];
        if sock.read_exact(&mut len_buf).is_err() {
            return;
        }
        let len = u32::from_be_bytes(len_buf) as usize;
        if len == 0 {
            continue;
        }
        let mut msg = vec![0u8; len];
        if sock.read_exact(&mut msg).is_err() {
            return;
        }
        match msg[0] {
            REQUEST_ID if len >= 13 => {
                let index =
                    u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
                let begin =
                    u32::from_be_bytes([msg[5], msg[6], msg[7], msg[8]]) as usize;
                let length =
                    u32::from_be_bytes([msg[9], msg[10], msg[11], msg[12]]) as usize;
                let start = index * piece_len + begin;
                let end = (start + length).min(content.len());
                if start >= content.len() || end <= start {
                    continue;
                }
                let mut payload = Vec::with_capacity(8 + (end - start));
                payload.extend_from_slice(&(index as u32).to_be_bytes());
                payload.extend_from_slice(&(begin as u32).to_be_bytes());
                payload.extend_from_slice(&content[start..end]);
                if sock.write_all(&frame(PIECE_ID, &payload)).is_err() {
                    return;
                }
                let _ = sock.flush();
                requests.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }
}

fn frame(id: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(5 + payload.len());
    out.extend_from_slice(&((payload.len() + 1) as u32).to_be_bytes());
    out.push(id);
    out.extend_from_slice(payload);
    out
}

fn handshake(info_hash: &[u8; 20]) -> Vec<u8> {
    let mut hs = vec![19u8];
    hs.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    // BEP-10 bit, as a real client sets it.  The client under test answers
    // with its own extended handshake, which is what keeps this fixture's
    // traffic looking like a normal peer's.
    reserved[5] |= 0x10;
    hs.extend_from_slice(&reserved);
    hs.extend_from_slice(info_hash);
    hs.extend_from_slice(b"-SP0001-012345678901");
    hs
}

/// Register this peer with the tracker as a complete seed (`left=0`).
fn announce(port: u16, info_hash: &[u8; 20], announce_url: &str) {
    let mut escaped = String::new();
    for byte in info_hash {
        escaped.push_str(&format!("%{byte:02x}"));
    }
    let host = announce_url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .expect("seed peer: announce url has no host");
    let path = format!("/announce?info_hash={escaped}&port={port}&left=0&compact=1");
    match TcpStream::connect(host) {
        Ok(mut stream) => {
            let _ = stream.write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            );
            let mut sink = [0u8; 1024];
            let _ = stream.read(&mut sink);
        }
        Err(e) => panic!("seed peer: announce to {host} failed: {e}"),
    }
}

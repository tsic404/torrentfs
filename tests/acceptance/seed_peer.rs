//! Protocol-level peer for the read-path acceptance cases.
//!
//! The design makes libtorrent the only BitTorrent implementation in the
//! product (F5/NG2), so a libtorrent seeder cannot prove that a *foreign* peer
//! is usable: both ends would be the same implementation.  This fixture speaks
//! the minimum a real peer sends — handshake, bitfield, unchoke, and a PIECE
//! reply per REQUEST — and records whether the client ever asked it for a byte.
//! A read that returns the seeded bytes therefore proves the client treated an
//! unchoked, piece-declaring peer as a data source.
//!
//! The peer can also declare no pieces (a leecher, which keeps a swarm
//! non-empty without providing a source), can delay its handshake or its
//! PIECE replies, so a case can make a seed become usable at a chosen point
//! inside a read's wait, and can stall its PIECE replies outright, so a case
//! can hold a read on a swarm whose connected seeder never delivers.
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

/// Extra latency a fixture peer applies before the client can use it.
#[derive(Clone, Copy, Default)]
pub struct PeerDelays {
    /// Wait after the client's handshake before answering it.  Until the
    /// answer lands the client counts a connecting peer, not a seed.
    pub handshake: Duration,
    /// Wait after each REQUEST before sending its PIECE reply.
    pub piece: Duration,
}

/// What the peer declares about its pieces.
#[derive(Clone, Copy)]
enum Declared {
    Seed,
    Leecher,
}

impl Declared {
    /// The `left` value this role announces to the tracker (0 = complete).
    fn left(self, total_len: usize) -> u64 {
        match self {
            Self::Seed => 0,
            Self::Leecher => total_len as u64,
        }
    }
}

/// A running protocol peer: announces one torrent and serves its bytes when it
/// declares them.
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
        Self::launch(
            info_hash,
            content,
            piece_len,
            announce_url,
            Declared::Seed,
            PeerDelays::default(),
            false,
        )
    }

    /// A peer that declares no pieces and announces the full size as `left`:
    /// the client counts a connected peer, not a seed.
    pub fn start_leecher(
        info_hash: [u8; 20],
        content: Vec<u8>,
        piece_len: usize,
        announce_url: &str,
    ) -> Self {
        Self::launch(
            info_hash,
            content,
            piece_len,
            announce_url,
            Declared::Leecher,
            PeerDelays::default(),
            false,
        )
    }

    /// A complete seed that answers the client only after `delays.handshake`
    /// and each REQUEST after `delays.piece`, so a case can make the seed
    /// usable at a chosen point inside a read's wait.
    pub fn start_delayed(
        info_hash: [u8; 20],
        content: Vec<u8>,
        piece_len: usize,
        announce_url: &str,
        delays: PeerDelays,
    ) -> Self {
        Self::launch(
            info_hash,
            content,
            piece_len,
            announce_url,
            Declared::Seed,
            delays,
            false,
        )
    }

    /// A complete seed that declares every piece and unchokes but never
    /// answers a REQUEST, so the client counts a connected seeder while a read
    /// cannot make progress (the slow-swarm stall the third frozen prefix
    /// diagnoses).
    pub fn start_stalled(
        info_hash: [u8; 20],
        content: Vec<u8>,
        piece_len: usize,
        announce_url: &str,
    ) -> Self {
        Self::launch(
            info_hash,
            content,
            piece_len,
            announce_url,
            Declared::Seed,
            PeerDelays::default(),
            true,
        )
    }

    fn launch(
        info_hash: [u8; 20],
        content: Vec<u8>,
        piece_len: usize,
        announce_url: &str,
        declared: Declared,
        delays: PeerDelays,
        is_stalled: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("seed peer: bind failed");
        let port = listener.local_addr().expect("seed peer: no addr").port();
        listener
            .set_nonblocking(true)
            .expect("seed peer: nonblocking");
        announce(port, &info_hash, announce_url, declared.left(content.len()));

        let stop = Arc::new(AtomicBool::new(false));
        let requests = Arc::new(AtomicUsize::new(0));
        let peer_id = peer_id(port);

        // One thread per connection: the client may reconnect while the
        // torrent settles, and each connection needs its own message loop.
        let accept_requests = Arc::clone(&requests);
        let accept_stop = Arc::clone(&stop);
        let piece_count = content.len().div_ceil(piece_len.max(1));
        thread::spawn(move || {
            let byte_len = piece_count.div_ceil(8);
            let bitfield = match declared {
                Declared::Seed => {
                    let mut bitfield = vec![0u8; byte_len];
                    for piece in 0..piece_count {
                        bitfield[piece / 8] |= 1 << (7 - (piece % 8));
                    }
                    bitfield
                }
                Declared::Leecher => vec![0u8; byte_len],
            };
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
                                delays,
                                is_stalled,
                                &peer_id,
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
    delays: PeerDelays,
    is_stalled: bool,
    peer_id: &[u8; 20],
    requests: &AtomicUsize,
    stop: &AtomicBool,
) {
    let _ = sock.set_nonblocking(false);
    let _ = sock.set_read_timeout(Some(Duration::from_secs(30)));

    let mut peer_handshake = vec![0u8; HANDSHAKE_LEN];
    if sock.read_exact(&mut peer_handshake).is_err() {
        return;
    }
    if !delays.handshake.is_zero() {
        thread::sleep(delays.handshake);
    }
    let _ = sock.write_all(&handshake(info_hash, peer_id));
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
            REQUEST_ID if len >= 13 && !is_stalled => {
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
                if !delays.piece.is_zero() {
                    thread::sleep(delays.piece);
                }
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

/// A peer id unique to this peer's listen port, so two fixture peers in one
/// swarm are not rejected as a duplicate connection.
fn peer_id(port: u16) -> [u8; 20] {
    let text = format!("-SP0001-{port:012}");
    let mut id = [0u8; 20];
    id.copy_from_slice(text.as_bytes());
    id
}

fn handshake(info_hash: &[u8; 20], peer_id: &[u8; 20]) -> Vec<u8> {
    let mut hs = vec![19u8];
    hs.extend_from_slice(b"BitTorrent protocol");
    let mut reserved = [0u8; 8];
    // BEP-10 bit, as a real client sets it.  The client under test answers
    // with its own extended handshake, which is what keeps this fixture's
    // traffic looking like a normal peer's.
    reserved[5] |= 0x10;
    hs.extend_from_slice(&reserved);
    hs.extend_from_slice(info_hash);
    hs.extend_from_slice(peer_id);
    hs
}

/// Register this peer with the tracker, announcing `left` bytes remaining
/// (0 = complete seed).
fn announce(port: u16, info_hash: &[u8; 20], announce_url: &str, left: u64) {
    let mut escaped = String::new();
    for byte in info_hash {
        escaped.push_str(&format!("%{byte:02x}"));
    }
    let host = announce_url
        .trim_start_matches("http://")
        .split('/')
        .next()
        .expect("seed peer: announce url has no host");
    let path = format!("/announce?info_hash={escaped}&port={port}&left={left}&compact=1");
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

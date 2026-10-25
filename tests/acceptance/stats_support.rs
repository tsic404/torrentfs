//! Shared fixtures for the leaf `.stats` cases (TC-stats-06/07/08).
//!
//! The stats cases render the same leaf file and read the same `-- Pieces --`
//! marker run, so the torrent builder, the marker parser and the render call
//! live here once.  The builder takes the piece length as an argument: the
//! engine-level cases need 16 KiB pieces (one BEP-3 block per piece) while the
//! shared fixture's 256 KiB pieces would put sixteen block requests behind a
//! delayed peer; that difference is a parameter, not a reason to fork the
//! fixture code.

use std::sync::{Arc, Mutex};

use torrentfs::fuse::stats::generate_torrent_stats;
use torrentfs::{Database, DownloadService};

/// A single-file torrent of `pieces` pieces of `piece_len` bytes each, with
/// the content and its piece hashes derived from the same buffer.
pub fn multipiece_torrent(
    announce_url: &str,
    name: &str,
    pieces: usize,
    piece_len: usize,
) -> (Vec<u8>, Vec<u8>) {
    let content: Vec<u8> = (0..piece_len * pieces)
        .map(|i| (i as u8).wrapping_mul(31).wrapping_add(7))
        .collect();
    let mut hashes = Vec::with_capacity(20 * pieces);
    for chunk in content.chunks(piece_len) {
        let mut hasher = sha1_smol::Sha1::new();
        hasher.update(chunk);
        hashes.extend_from_slice(&hasher.digest().bytes());
    }

    let mut torrent = Vec::new();
    torrent.push(b'd');
    torrent.extend_from_slice(b"8:announce");
    torrent.extend_from_slice(announce_url.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(announce_url.as_bytes());
    torrent.extend_from_slice(b"4:infod");
    torrent.extend_from_slice(b"6:lengthi");
    torrent.extend_from_slice((piece_len * pieces).to_string().as_bytes());
    torrent.push(b'e');
    torrent.extend_from_slice(format!("4:name{}:{}", name.len(), name).as_bytes());
    torrent.extend_from_slice(b"12:piece lengthi");
    torrent.extend_from_slice(piece_len.to_string().as_bytes());
    torrent.push(b'e');
    torrent.extend_from_slice(b"6:pieces");
    torrent.extend_from_slice(hashes.len().to_string().as_bytes());
    torrent.push(b':');
    torrent.extend_from_slice(&hashes);
    torrent.extend_from_slice(b"ee");
    (torrent, content)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Marker {
    NotWanted,
    Wanted(i32),
    Cached,
    CachedHit(u64),
}

/// Strictly parse the marker run into the four-state enumeration: `[]`,
/// `[N]` with a positive priority, `[x]`, and `[X n]` with a positive hit
/// count.  Returns the observed states in order.
pub fn parse_markers(run: &str) -> Vec<Marker> {
    let mut markers = Vec::new();
    let mut rest = run;
    while !rest.is_empty() {
        assert!(
            rest.starts_with('['),
            "marker run must consist of bracketed tokens, got {rest:?}"
        );
        let end = rest
            .find(']')
            .unwrap_or_else(|| panic!("unterminated marker in {rest:?}"));
        let body = &rest[1..end];
        let marker = if body.is_empty() {
            Marker::NotWanted
        } else if body == "x" {
            Marker::Cached
        } else if let Some(n) = body.strip_prefix("X ") {
            let n: u64 = n
                .parse()
                .unwrap_or_else(|_| panic!("[X n] must carry a decimal count, got {body:?}"));
            assert!(n > 0, "[X n] must carry a positive hit count, got {body:?}");
            Marker::CachedHit(n)
        } else {
            let n: i32 = body
                .parse()
                .unwrap_or_else(|_| panic!("only [] [N] [x] [X n] are valid markers, got [{body}]"));
            assert!(n > 0, "[N] must carry a positive priority, got [{body}]");
            Marker::Wanted(n)
        };
        markers.push(marker);
        rest = &rest[end + 1..];
    }
    markers
}

/// The marker run carried by the single `  Pieces: ` line.
pub fn pieces_run(stats: &str) -> Option<&str> {
    let mut runs = stats
        .lines()
        .filter_map(|line| line.strip_prefix("  Pieces: "));
    let run = runs.next()?;
    assert!(runs.next().is_none(), "exactly one Pieces marker line");
    Some(run)
}

/// Render the leaf `.stats` of one torrent through the production renderer.
pub fn render(
    service: &Arc<DownloadService>,
    db: &Arc<Mutex<Database>>,
    torrent_id: i64,
    info_hash: &str,
) -> String {
    let bytes = generate_torrent_stats(
        torrent_id,
        info_hash,
        &Some(db.clone()),
        &Some(service.clone()),
        || service.get_cache_manager(),
    );
    String::from_utf8(bytes).expect("leaf .stats is UTF-8")
}

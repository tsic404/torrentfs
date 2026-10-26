//! Design acceptance case TC-read-11: every file of a multi-file seed reads
//! back whole and byte-identical, including the files whose boundaries fall
//! inside one shared piece (`docs/03-acceptance.md` §B AC-10).
//!
//! A foreign protocol peer serves the torrent's flat byte stream, so the read
//! path has to slice pieces back into the declared files itself: a client that
//! maps a file offset to the wrong piece range returns bytes that belong to a
//! neighbour file, and the digest comparison catches it.

use crate::common::{
    build_multi_file_torrent_with_content, read_projection_file, wave_slot, IngestHarness,
    MiniTracker,
};
use crate::seed_peer::SeedPeer;
use std::thread;
use std::time::{Duration, Instant};
use torrentfs::fuse::fs_types::FileKind;
use torrentfs::fuse::inodes::METADATA_INO;
use torrentfs::TorrentInfo;

pub const TC_ID: &str = "TC-read-11";

const SEED_FILE: &str = "multi.torrent";
const SEED_NAME: &str = "read-11-multi";
const PIECE_LEN: usize = 16 * 1024;

fn sha1_of(bytes: &[u8]) -> String {
    let mut hasher = sha1_smol::Sha1::new();
    hasher.update(bytes);
    hex::encode(hasher.digest().bytes())
}

/// Each declared file reads back with its `stat` size and its authoritative
/// digest; the two files whose boundaries sit inside a shared piece are both
/// read, so a piece fetched for one file must still serve the other correctly.
#[test]
fn tc_read_11_reads_files_whose_boundaries_cross_pieces() {
    let _slot = wave_slot();
    let tracker = MiniTracker::start();
    let announce_url = tracker.announce_url();

    // Layout over 16 KiB pieces: clip.bin (0..20 KiB) ends inside piece 1,
    // part.bin (20..30 KiB) lies wholly inside it, and tail.bin (30..40 KiB)
    // starts inside it — so both adjacent boundaries fall mid-piece and every
    // file meets piece 1.  Distinct fill patterns keep a mis-sliced read from
    // accidentally matching.
    let clip: Vec<u8> = (0..20 * 1024).map(|i| (i % 251) as u8).collect();
    let part: Vec<u8> = (0..10 * 1024).map(|i| (i % 241) as u8).collect();
    let tail: Vec<u8> = (0..10 * 1024).map(|i| (i % 239) as u8).collect();
    let files = [
        ("clip.bin", clip.as_slice()),
        ("sub/part.bin", part.as_slice()),
        ("sub/deep/tail.bin", tail.as_slice()),
    ];
    let (torrent_bytes, flat) =
        build_multi_file_torrent_with_content(&announce_url, SEED_NAME, &files, PIECE_LEN);

    let info = TorrentInfo::from_bytes(torrent_bytes.clone()).expect("parse the fixture torrent");
    let declared: Vec<(String, u64)> = info
        .files()
        .expect("declared file list")
        .into_iter()
        .map(|file| (file.path, file.size))
        .collect();
    let expected: Vec<(String, u64)> = files
        .iter()
        .map(|(path, content)| (format!("{SEED_NAME}/{path}"), content.len() as u64))
        .collect();
    assert_eq!(
        declared, expected,
        "the fixture metadata must declare exactly the files under test"
    );

    // Piece geometry from the declared layout: each file's byte range mapped
    // onto piece indices.  Adjacent files must meet in one piece, otherwise
    // the fixture proves nothing about a piece shared across file boundaries.
    let mut cursor = 0usize;
    let mut occupied: Vec<(&str, usize, usize)> = Vec::new();
    for (path, content) in &files {
        let first = cursor / PIECE_LEN;
        let last = (cursor + content.len() - 1) / PIECE_LEN;
        occupied.push((path, first, last));
        cursor += content.len();
    }
    for pair in occupied.windows(2) {
        assert_eq!(
            pair[0].2, pair[1].1,
            "{} and {} must share the piece their boundaries meet in",
            pair[0].0, pair[1].0
        );
    }

    let info_hash = info.info_hash().expect("info hash");
    let _peer = SeedPeer::start(info_hash, flat, PIECE_LEN, &announce_url);

    let mut ingest = IngestHarness::new();
    ingest.copy_torrent(METADATA_INO, SEED_FILE, &torrent_bytes);

    // A read started before the peer's declaration lands would only exercise
    // the wait path, so the case waits for the seed the way a cold mount's
    // first read does: handle added, peer discovered, read arrives.
    let hex_hash = hex::encode(info_hash);
    let started = Instant::now();
    loop {
        let seeds = ingest
            .service()
            .download_service
            .as_ref()
            .expect("the service must hold a download engine")
            .try_query_torrent_status(&hex_hash)
            .map(|status| status.num_seeds)
            .unwrap_or(0);
        if seeds >= 1 {
            break;
        }
        assert!(
            started.elapsed() < Duration::from_secs(30),
            "the fixture peer never counted as a seed (info hash {hex_hash})"
        );
        thread::sleep(Duration::from_millis(200));
    }

    // Read in declaration order: clip.bin pulls piece 1, and part.bin /
    // tail.bin need that same piece, so their reads prove the shared piece is
    // sliced per file rather than per read.
    for ((path, content), (declared_path, declared_size)) in files.iter().zip(&declared) {
        let parts: Vec<&str> = std::iter::once(SEED_FILE)
            .chain(declared_path.split('/'))
            .collect();
        let ino = ingest
            .data_lookup(&parts)
            .unwrap_or_else(|| panic!("data/{} must resolve", parts.join("/")));
        let attr = ingest
            .service()
            .getattr(ino)
            .unwrap_or_else(|e| panic!("getattr data/{} failed: {:?}", parts.join("/"), e));
        assert_eq!(
            attr.kind,
            FileKind::RegularFile,
            "data/{} must be a regular file",
            parts.join("/")
        );
        assert_eq!(
            attr.size, *declared_size,
            "data/{} must carry the declared full size",
            parts.join("/")
        );

        let data = read_projection_file(&mut ingest, ino, attr.size as u32);
        assert_eq!(
            data.len(),
            attr.size as usize,
            "data/{} read {} bytes, its stat size is {}",
            parts.join("/"),
            data.len(),
            attr.size
        );
        assert_eq!(
            sha1_of(&data),
            sha1_of(content),
            "data/{} digest must equal the authoritative copy ({path})",
            parts.join("/")
        );
        assert_eq!(
            data.as_slice(),
            *content,
            "data/{} bytes must equal the authoritative copy ({path})",
            parts.join("/")
        );
    }
}

//! A v2-only torrent pads every file to a piece boundary in the layout, but
//! libtorrent only requests the payload bytes: `piece_size_for_req` uses
//! `piece_size2`, and the piece picker treats the pad tail as finished without
//! delivering an async_write for it. A storage that waits for the whole padded
//! layout therefore never completes the piece, and a hash arriving before that
//! discards the only staged payload.
//!
//! These fixtures use short files at file boundaries (3000-byte files in
//! 64 KiB pieces), started from an empty cache and downloaded for real.

mod common;

use std::time::{Duration, Instant};

use common::{acquire_session_lock, local_test_config, MiniTracker};
use sha2::{Digest, Sha256};
use torrentfs::{Session, TorrentInfo};

const PIECE_LENGTH: usize = 65_536;
const FILE_SIZE: usize = 3_000;
const BLOCK: usize = 16_384;

fn ben_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn ben_int(out: &mut Vec<u8>, value: u64) {
    out.push(b'i');
    out.extend_from_slice(value.to_string().as_bytes());
    out.push(b'e');
}

fn sha256(bytes: &[u8]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher.finalize().into()
}

fn sha256_pair(left: &[u8; 32], right: &[u8; 32]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(left);
    hasher.update(right);
    hasher.finalize().into()
}

/// BEP 52 file merkle root: 16 KiB leaves, with the leaf layer padded to a
/// power of two by zero hashes when the file is smaller than one piece.
fn merkle_root(data: &[u8]) -> [u8; 32] {
    let mut layer: Vec<[u8; 32]> = data.chunks(BLOCK).map(sha256).collect();
    let target = layer.len().next_power_of_two();
    layer.resize(target, [0u8; 32]);
    while layer.len() > 1 {
        layer = layer
            .chunks(2)
            .map(|pair| sha256_pair(&pair[0], &pair[1]))
            .collect();
    }
    layer[0]
}

fn patterned(size: usize, modulus: usize) -> Vec<u8> {
    (0..size).map(|i| (i % modulus) as u8).collect()
}

/// Build a v2-only torrent: a `file tree` with two short files, no `files`
/// list and no `pieces`. Each file is padded to a piece boundary in the
/// canonical layout, so each piece's payload is only `FILE_SIZE` bytes.
fn v2_only_fixture(announce_url: &str, name: &str, files: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut tree = Vec::new();
    tree.push(b'd');
    for (file_name, data) in files {
        ben_bytes(&mut tree, file_name);
        tree.push(b'd');
        ben_bytes(&mut tree, b"");
        tree.push(b'd');
        ben_bytes(&mut tree, b"length");
        ben_int(&mut tree, data.len() as u64);
        ben_bytes(&mut tree, b"pieces root");
        ben_bytes(&mut tree, &merkle_root(data));
        tree.push(b'e');
        tree.push(b'e');
    }
    tree.push(b'e');

    let mut info = Vec::new();
    info.push(b'd');
    ben_bytes(&mut info, b"file tree");
    info.extend_from_slice(&tree);
    ben_bytes(&mut info, b"meta version");
    ben_int(&mut info, 2);
    ben_bytes(&mut info, b"name");
    ben_bytes(&mut info, name.as_bytes());
    ben_bytes(&mut info, b"piece length");
    ben_int(&mut info, PIECE_LENGTH as u64);
    info.push(b'e');

    let mut torrent = Vec::new();
    torrent.push(b'd');
    ben_bytes(&mut torrent, b"announce");
    ben_bytes(&mut torrent, announce_url.as_bytes());
    ben_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&info);
    // Both files fit in one piece, so no piece layers are required.
    ben_bytes(&mut torrent, b"piece layers");
    torrent.extend_from_slice(b"de");
    torrent.push(b'e');
    torrent
}

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
}

fn wait_until(timeout: Duration, condition: impl Fn() -> bool) -> bool {
    let deadline = Instant::now() + timeout;
    while !condition() {
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    true
}

#[test]
fn v2_only_short_file_pieces_download_from_an_empty_cache() {
    let _guard = acquire_session_lock();
    let tracker = MiniTracker::start();
    let file_a = patterned(FILE_SIZE, 251);
    let file_b = patterned(FILE_SIZE, 241);
    let torrent_data = v2_only_fixture(
        &tracker.announce_url(),
        "v2short",
        &[(b"a.bin", &file_a), (b"b.bin", &file_b)],
    );
    let info = TorrentInfo::from_bytes(torrent_data).expect("parse v2-only torrent");
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));

    // Seeder: stock storage with the two real files. The canonical layout
    // pads each file to the piece boundary; requests only cover real bytes.
    let seed_dir = tempfile::TempDir::new().expect("seed dir");
    let torrent_name = seed_dir.path().join("v2short");
    std::fs::create_dir_all(&torrent_name).expect("seed torrent dir");
    std::fs::write(torrent_name.join("a.bin"), &file_a).expect("seed a.bin");
    std::fs::write(torrent_name.join("b.bin"), &file_b).expect("seed b.bin");
    let mut seeder = Session::new(&quiet_config()).expect("seeder session");
    let seed_handle = seeder
        .add_torrent(&info, seed_dir.path())
        .expect("seeder add torrent");
    assert!(
        wait_until(Duration::from_secs(30), || seed_handle.have_piece(0)
            && seed_handle.have_piece(1)),
        "the v2-only seeder never verified its two files"
    );
    assert!(
        wait_until(Duration::from_secs(60), || tracker
            .peer_count(&info.info_hash().expect("info hash"))
            >= 1),
        "seeder never announced to the tracker"
    );

    // Downloader: empty custom-storage cache, real network download.
    let cache_dir = tempfile::TempDir::new().expect("cache dir");
    let mut downloader = Session::new_with_custom_storage(&quiet_config(), cache_dir.path())
        .expect("downloader session");
    let handle = downloader
        .add_torrent(&info, cache_dir.path())
        .expect("downloader add torrent");

    let deadline = Instant::now() + Duration::from_secs(60);
    while !(handle.have_piece(0) && handle.have_piece(1)) {
        assert!(
            Instant::now() < deadline,
            "v2-only short-file pieces never completed (have: {}, {})",
            handle.have_piece(0),
            handle.have_piece(1)
        );
        std::thread::sleep(Duration::from_millis(20));
    }

    let piece_dir = cache_dir.path().join(&info_hash_hex);
    for (index, expected) in [&file_a, &file_b].into_iter().enumerate() {
        let path = piece_dir.join(format!("{info_hash_hex}:piece:{index}"));
        let stored = std::fs::read(&path).expect("published piece");
        assert_eq!(
            stored.len(),
            FILE_SIZE,
            "piece {index} was published with the padded layout length instead of the v2 payload"
        );
        assert_eq!(
            stored, *expected,
            "piece {index} does not match the downloaded payload"
        );
        assert!(
            !path.with_extension("part").exists(),
            "piece {index} left a staging file behind"
        );
    }
}

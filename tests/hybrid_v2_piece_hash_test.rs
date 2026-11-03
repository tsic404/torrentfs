//! Hybrid (v1+v2) cache check must verify the v2 block hashes.
//!
//! BEP 52 hashes a piece in fixed 16 KiB blocks and truncates the last block at
//! `piece_size2`, the piece length without v1 tail padding. Slicing the piece
//! evenly (`piece_size / block count`) instead folds padding bytes into the last
//! block, so a padded piece fails v2 verification while its v1 hash matches —
//! the seed is dropped into an error state.
//!
//! Two fixtures cover the shapes that expose the difference. The multi-file
//! one has file A at 50000 bytes and file B at 3000, both padded to a 65536
//! piece boundary: piece 0 holds file A plus 15536 padding bytes (v2 length
//! 50000 — not a multiple of 16 KiB), piece 1 holds file B plus 62536 padding
//! bytes (v2 length 3000). The single-file one has one 50000-byte file closed
//! by a trailing `.pad`, so the padding is not tied to a second real file.
//! Verifying any of those pieces needs the fixed-block slicing.

mod common;

use std::path::Path;
use std::time::{Duration, Instant};

use common::{acquire_session_lock, local_test_config};
use sha1_smol::Sha1;
use sha2::{Digest, Sha256};
use torrentfs::{Session, TorrentInfo, TorrentState};

const PIECE_LENGTH: usize = 65_536;
const FILE_A_SIZE: usize = 50_000;
const FILE_B_SIZE: usize = 3_000;
const BLOCK: usize = 16_384;

struct HybridFixture {
    torrent: Vec<u8>,
    /// v1 piece payloads: file A + tail padding, then file B + tail padding.
    pieces: Vec<Vec<u8>>,
}

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

fn sha1(bytes: &[u8]) -> [u8; 20] {
    let mut hasher = Sha1::new();
    hasher.update(bytes);
    hasher.digest().bytes()
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

/// Merkle root of the 16 KiB block hashes of one file whose block count is a
/// power of two — the only shape this fixture produces.
fn merkle_root(leaves: &[[u8; 32]]) -> [u8; 32] {
    assert!(leaves.len().is_power_of_two());
    let mut layer = leaves.to_vec();
    while layer.len() > 1 {
        layer = layer
            .chunks(2)
            .map(|pair| sha256_pair(&pair[0], &pair[1]))
            .collect();
    }
    layer[0]
}

fn block_hashes(data: &[u8]) -> Vec<[u8; 32]> {
    data.chunks(BLOCK).map(sha256).collect()
}

/// One real file of the fixture: its name inside the torrent and its bytes.
type FixtureFile<'a> = (&'a [u8], Vec<u8>);

fn patterned(size: usize, modulus: usize) -> Vec<u8> {
    (0..size).map(|i| (i % modulus) as u8).collect()
}

/// Build a hybrid torrent from `files`, padding every real file to the piece
/// boundary. The v1 file list carries those pads explicitly as `attr: p`
/// entries under `.pad/<size>`; the v2 file tree lists only the real files.
fn hybrid_fixture(name: &str, files: &[FixtureFile<'_>]) -> HybridFixture {
    // Canonical v1 layout: every file is followed by the padding that makes
    // the next file start on a piece boundary.
    let mut stream = Vec::new();
    let mut pads = Vec::with_capacity(files.len());
    for (_, data) in files {
        stream.extend_from_slice(data);
        let pad = (PIECE_LENGTH - data.len() % PIECE_LENGTH) % PIECE_LENGTH;
        pads.push(pad);
        stream.resize(stream.len() + pad, 0);
    }

    let pieces: Vec<Vec<u8>> = stream.chunks(PIECE_LENGTH).map(<[u8]>::to_vec).collect();
    let piece_hashes: Vec<u8> = pieces.iter().flat_map(|piece| sha1(piece)).collect();

    // v2 file tree: every file entry lives under the empty key of its path
    // component; padding files are implicit and never listed.
    let mut tree = Vec::new();
    tree.push(b'd');
    for ((file_name, data), _) in files.iter().zip(&pads) {
        ben_bytes(&mut tree, *file_name);
        tree.push(b'd');
        ben_bytes(&mut tree, b"");
        tree.push(b'd');
        ben_bytes(&mut tree, b"length");
        ben_int(&mut tree, data.len() as u64);
        ben_bytes(&mut tree, b"pieces root");
        ben_bytes(&mut tree, &merkle_root(&block_hashes(data)));
        tree.push(b'e');
        tree.push(b'e');
    }
    tree.push(b'e');

    // v1 file list, which hybrid torrents keep alongside the v2 file tree. Pad
    // files are real entries here: without them libtorrent rejects the torrent
    // ("the v1 and v2 file metadata does not match").
    let mut v1_files = Vec::new();
    v1_files.push(b'l');
    let mut push_file = |out: &mut Vec<u8>, size: usize, names: &[&[u8]]| {
        out.push(b'd');
        ben_bytes(out, b"length");
        ben_int(out, size as u64);
        ben_bytes(out, b"path");
        out.push(b'l');
        for name in names {
            ben_bytes(out, name);
        }
        out.push(b'e');
        out.push(b'e');
    };
    let mut push_pad = |out: &mut Vec<u8>, size: usize| {
        out.push(b'd');
        ben_bytes(out, b"attr");
        ben_bytes(out, b"p");
        ben_bytes(out, b"length");
        ben_int(out, size as u64);
        ben_bytes(out, b"path");
        out.push(b'l');
        ben_bytes(out, b".pad");
        ben_bytes(out, size.to_string().as_bytes());
        out.push(b'e');
        out.push(b'e');
    };
    for ((file_name, data), pad) in files.iter().zip(&pads) {
        push_file(&mut v1_files, data.len(), &[*file_name]);
        if *pad > 0 {
            push_pad(&mut v1_files, *pad);
        }
    }
    v1_files.push(b'e');

    // Info dict, keys in bencode order.
    let mut info = Vec::new();
    info.push(b'd');
    ben_bytes(&mut info, b"file tree");
    info.extend_from_slice(&tree);
    ben_bytes(&mut info, b"files");
    info.extend_from_slice(&v1_files);
    ben_bytes(&mut info, b"meta version");
    ben_int(&mut info, 2);
    ben_bytes(&mut info, b"name");
    ben_bytes(&mut info, name.as_bytes());
    ben_bytes(&mut info, b"piece length");
    ben_int(&mut info, PIECE_LENGTH as u64);
    ben_bytes(&mut info, b"pieces");
    ben_bytes(&mut info, &piece_hashes);
    info.push(b'e');

    let mut torrent = Vec::new();
    torrent.push(b'd');
    ben_bytes(&mut torrent, b"announce");
    ben_bytes(&mut torrent, b"http://127.0.0.1:1/announce");
    ben_bytes(&mut torrent, b"info");
    torrent.extend_from_slice(&info);
    ben_bytes(&mut torrent, b"piece layers");
    torrent.extend_from_slice(b"de");
    torrent.push(b'e');

    HybridFixture { torrent, pieces }
}

/// Two real files; piece 0 carries 15536 padding bytes and piece 1 62536.
fn multi_file_fixture() -> HybridFixture {
    hybrid_fixture(
        "hybridpad",
        &[
            (b"a.bin", patterned(FILE_A_SIZE, 251)),
            (b"b.bin", patterned(FILE_B_SIZE, 241)),
        ],
    )
}

/// One real file closed by a trailing `.pad`: a single piece whose v1 length
/// is 65536 and whose v2 length is 50000.
fn single_file_fixture() -> HybridFixture {
    hybrid_fixture(
        "hybridpadsingle",
        &[(b"only.bin", patterned(FILE_A_SIZE, 251))],
    )
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

fn write_cached_pieces(pieces_root: &Path, info_hash_hex: &str, fixture: &HybridFixture) {
    let dir = pieces_root.join(info_hash_hex);
    std::fs::create_dir_all(&dir).expect("create piece directory");
    for (index, data) in fixture.pieces.iter().enumerate() {
        let path = dir.join(format!("{}:piece:{}", info_hash_hex, index));
        std::fs::write(path, data).expect("write piece file");
    }
}

/// Add the fixture to a custom-storage session and wait for the full check
/// its cached piece files trigger.
fn ingest_fixture(
    pieces_root: &Path,
    fixture: &HybridFixture,
) -> (Session, torrentfs::TorrentHandle) {
    let info = TorrentInfo::from_bytes(fixture.torrent.clone()).expect("parse hybrid torrent");
    // The disk storage names the piece directory after the torrent's best
    // info hash (`storage_params::info_hash`), which is the v2 hash for a
    // hybrid torrent.
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));
    write_cached_pieces(pieces_root, &info_hash_hex, fixture);

    let mut session =
        Session::new_with_custom_storage(&quiet_config(), pieces_root).expect("create session");
    let handle = session
        .add_torrent(&info, pieces_root)
        .expect("add torrent to custom-storage session");
    wait_for_piece_check(&handle);
    (session, handle)
}

/// Every cached piece is hashed through `async_hash`, which computes both the
/// v1 SHA-1 and the v2 block hashes.
fn wait_for_piece_check(handle: &torrentfs::TorrentHandle) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        let status = handle.status().expect("status");
        if !matches!(
            status.state,
            TorrentState::QueuedForChecking
                | TorrentState::CheckingFiles
                | TorrentState::CheckingResumeData
                | TorrentState::Allocating
        ) {
            return;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    panic!("piece check did not finish");
}

/// A rejected piece stays missing and the torrent starts fetching it; data
/// counted as done is therefore the observable that v2 verification passed.
fn assert_all_pieces_verified(handle: &torrentfs::TorrentHandle) {
    let status = handle.status().expect("status");
    assert_eq!(
        status.total_done, status.total,
        "the check did not accept every cached piece (state {:?})",
        status.state
    );
}

#[test]
fn cached_hybrid_pieces_pass_v2_block_verification() {
    let _guard = acquire_session_lock();
    let temp = tempfile::tempdir().expect("temp dir");
    let pieces_root = temp.path().join("pieces");
    std::fs::create_dir_all(&pieces_root).expect("create pieces root");

    let (_session, handle) = ingest_fixture(&pieces_root, &multi_file_fixture());
    assert!(
        handle.have_piece(0),
        "padded hybrid piece failed v2 block verification"
    );
    assert!(
        handle.have_piece(1),
        "short final hybrid piece failed v2 block verification"
    );
    assert_all_pieces_verified(&handle);
}

#[test]
fn single_file_hybrid_pieces_pass_v2_block_verification() {
    let _guard = acquire_session_lock();
    let temp = tempfile::tempdir().expect("temp dir");
    let pieces_root = temp.path().join("pieces");
    std::fs::create_dir_all(&pieces_root).expect("create pieces root");

    let (_session, handle) = ingest_fixture(&pieces_root, &single_file_fixture());
    assert!(
        handle.have_piece(0),
        "single-file hybrid piece failed v2 block verification"
    );
    assert_all_pieces_verified(&handle);
}

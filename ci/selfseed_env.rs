//! `torrentfs-selfseed-env` — self-contained QA seeder environment. Public
//! sample torrents frequently have no reachable seeders, so reads fail with
//! `NoPeers` → ENODATA; this binary builds a deterministic single-file torrent
//! and serves it via a local tracker + libtorrent seeder, fully offline.
//! Flow: (1) start a minimal HTTP tracker, (2) stream the payload into the seed
//! directory hashing each piece, (3) bencode a single-file .torrent, (4) run a
//! libtorrent session in seeding state until killed. The tracker, bencoding
//! helpers, and keep-alive loop are shared with `torrentfs-mffs-seeder` via
//! [`seeder_common`] (`ci/seeder_common.rs`), which mirrors
//! `tests/common/mod.rs` (`MiniTracker`, `TestHarness`).

#[path = "seeder_common.rs"]
mod seeder_common;

use seeder_common::{
    bencode_bytes, bencode_int, install_signal_handlers, seed_until_shutdown, session_config,
    start_tracker, PIECE_LEN,
};

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use torrentfs::download::Session;
use torrentfs::TorrentInfo;

/// Stream `payload` into `seed_file` while hashing it piece-by-piece.
///
/// Returns the concatenated SHA-1 digests (one 20-byte digest per piece, in
/// order) and the total payload length.  Memory stays bounded by one piece
/// buffer plus the digest list — the previous `std::fs::read` held the whole
/// payload resident, which OOM'd at 1024/2048 MiB.
///
/// A full-size all-zero piece (the sparse >4 GiB QA payload) reuses one cached
/// digest and is seeked over instead of written, so the seed file stays sparse
/// instead of physically allocating the payload.
fn hash_and_seed(payload: &std::path::Path, seed_file: &std::path::Path) -> (Vec<u8>, u64) {
    use sha1_smol::Sha1;

    let mut input = std::fs::File::open(payload).expect("failed to read payload");
    let mut output = std::fs::File::create(seed_file).expect("failed to write seed file");

    let mut pieces = Vec::new();
    let mut buf = vec![0u8; PIECE_LEN];
    let mut total: u64 = 0;
    let mut zero_piece_digest: Option<[u8; 20]> = None;
    loop {
        let mut filled = 0;
        while filled < buf.len() {
            match input.read(&mut buf[filled..]) {
                Ok(0) => break, // EOF
                Ok(n) => filled += n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => panic!("failed to read payload: {e}"),
            }
        }
        if filled == 0 {
            break;
        }
        let chunk = &buf[..filled];
        if filled == PIECE_LEN && chunk.iter().all(|&b| b == 0) {
            let digest = *zero_piece_digest.get_or_insert_with(|| {
                let mut h = Sha1::new();
                h.update(chunk);
                h.digest().bytes()
            });
            pieces.extend_from_slice(&digest);
            output
                .seek(SeekFrom::Current(filled as i64))
                .expect("failed to seek seed file");
        } else {
            let mut h = Sha1::new();
            h.update(chunk);
            pieces.extend_from_slice(&h.digest().bytes());
            output.write_all(chunk).expect("failed to write seed file");
        }
        total += filled as u64;
    }
    // Seeked-over zero pieces do not extend the file (lseek past EOF leaves
    // the size unchanged), so pin the full logical size; the holes read back
    // as zeros and match the skipped pieces.
    output.set_len(total).expect("failed to set seed file size");
    (pieces, total)
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

    install_signal_handlers();

    // 1. Validate the payload up front: an empty payload must fail before the
    // tracker is started or tracker.url is written, leaving no residue behind.
    let payload_len = std::fs::metadata(&args.payload)
        .expect("failed to read payload")
        .len();
    assert!(payload_len > 0, "payload must not be empty");

    // 2. Tracker — the announce URL must be live before we bencode.
    start_tracker(&args.tracker_bind, args.tracker_port).expect("failed to start local tracker");
    let announce_url = format!(
        "http://{}:{}/announce",
        args.announce_host, args.tracker_port
    );
    std::fs::write(&args.url_out, &announce_url).expect("failed to write tracker.url");

    // 3. Deterministic single-file torrent over the fixed payload. Hashing
    // and seeding share one bounded pass: the payload is streamed into the
    // seed file piece-by-piece and never held whole in memory.

    let seed_dir = args
        .torrent_out
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("seed_data");
    std::fs::create_dir_all(&seed_dir).expect("failed to create seed dir");

    let name = "selfseed";
    let (pieces, _) = hash_and_seed(&args.payload, &seed_dir.join(name));
    let num_pieces = pieces.len() / 20;

    let dict: Vec<u8> = {
        let mut d = vec![b'd'];
        d.extend_from_slice(b"8:announce");
        d.extend_from_slice(&bencode_bytes(announce_url.as_bytes()));
        d.extend_from_slice(b"4:infod");
        d.extend_from_slice(b"6:length");
        d.extend_from_slice(&bencode_int(payload_len as i64));
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

    let info = TorrentInfo::from_bytes(dict.clone()).expect("failed to parse generated torrent");
    println!(
        "[seeder] name={} size={} bytes info_hash={}",
        info.name(),
        info.total_size(),
        hex::encode(info.info_hash().expect("info hash"))
    );
    println!("[seeder] announcing to {}", announce_url);

    let config = session_config();
    let mut session = Session::new(&config).expect("failed to create libtorrent session");
    let handle = session
        .add_torrent(&info, &seed_dir)
        .expect("failed to add torrent to session");

    seed_until_shutdown(&handle);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Removes a temp directory on drop so a failing assertion does not leak
    /// it in `/tmp`.
    struct TempDirGuard(std::path::PathBuf);
    impl Drop for TempDirGuard {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// the seeder must hash and seed the payload in one bounded
    /// pass.  Verify `hash_and_seed` yields piece digests identical to an
    /// in-memory reference, copies the payload verbatim into the seed file,
    /// and handles a trailing partial piece.
    #[test]
    fn hash_and_seed_streams_correctly() {
        let dir =
            std::env::temp_dir().join(format!("selfseed-env-hash-and-seed-{}", std::process::id()));
        let _guard = TempDirGuard(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        let payload_path = dir.join("payload.bin");
        let seed_path = dir.join("seed_data").join("selfseed");
        std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();

        // 2 full pieces plus a trailing partial piece.
        let mut data = vec![0u8; PIECE_LEN * 2 + 12345];
        for (i, b) in data.iter_mut().enumerate() {
            *b = (i % 251) as u8;
        }
        std::fs::write(&payload_path, &data).unwrap();

        let (pieces, total) = hash_and_seed(&payload_path, &seed_path);

        assert_eq!(total, data.len() as u64);
        assert_eq!(std::fs::read(&seed_path).unwrap(), data);

        let expected: Vec<u8> = data
            .chunks(PIECE_LEN)
            .flat_map(|c| {
                let mut h = sha1_smol::Sha1::new();
                h.update(c);
                h.digest().bytes()
            })
            .collect();
        assert_eq!(pieces, expected);
    }

    /// an all-zero (sparse) payload hashes every full piece to the same zero
    /// digest and leaves the seed file sparse, not physically allocated —
    /// the >4 GiB window-boundary seed must not consume disk space.
    #[test]
    fn hash_and_seed_keeps_zero_payload_sparse() {
        use std::os::unix::fs::MetadataExt;

        let dir =
            std::env::temp_dir().join(format!("selfseed-env-hash-sparse-{}", std::process::id()));
        let _guard = TempDirGuard(dir.clone());
        std::fs::create_dir_all(&dir).unwrap();
        let payload_path = dir.join("payload.bin");
        let seed_path = dir.join("seed_data").join("selfseed");
        std::fs::create_dir_all(seed_path.parent().unwrap()).unwrap();

        // 4 full zero pieces, created sparse via set_len (holes read as zero).
        let size = (PIECE_LEN * 4) as u64;
        let f = std::fs::File::create(&payload_path).unwrap();
        f.set_len(size).unwrap();

        let (pieces, total) = hash_and_seed(&payload_path, &seed_path);

        assert_eq!(total, size);
        let zero_piece = vec![0u8; PIECE_LEN];
        let mut h = sha1_smol::Sha1::new();
        h.update(&zero_piece);
        let zero_digest = h.digest().bytes();
        let mut expected = Vec::new();
        for _ in 0..4 {
            expected.extend_from_slice(&zero_digest);
        }
        assert_eq!(pieces, expected);

        let meta = std::fs::metadata(&seed_path).unwrap();
        assert_eq!(meta.len(), size);
        assert!(
            meta.blocks() * 512 < size,
            "all-zero seed file must stay sparse, allocated {} bytes",
            meta.blocks() * 512
        );
    }
}

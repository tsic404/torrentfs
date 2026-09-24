//! `torrentfs-mffs-seeder` — self-contained QA seeder for *multi-file*
//! torrents, mirroring `torrentfs-selfseed-env` (`ci/selfseed_env.rs`) but for
//! an `info` dict with a `files` list (BEP-3 multi-file layout) instead of a
//! single `length`/`name` pair.  It starts a local tracker, hashes the payload
//! directory into a seed tree (pieces span file boundaries), bencodes the
//! `.torrent`, and seeds until killed — a deterministic offline swarm for
//! multi-file read scenarios.  Shared helpers: `seeder_common` (tracker,
//! bencoding, keep-alive) and `mffs_common` (multi-file construction).

#[path = "seeder_common.rs"]
mod seeder_common;

#[path = "mffs_common.rs"]
mod mffs_common;

use mffs_common::{bencode_multifile_torrent, collect_files, hash_and_seed_files};
use seeder_common::{install_signal_handlers, seed_until_shutdown, session_config, start_tracker};

use std::path::PathBuf;

use torrentfs::download::Session;
use torrentfs::TorrentInfo;

// ── main ─────────────────────────────────────────────────────────────────────

struct Args {
    payload_dir: PathBuf,
    /// Address the HTTP tracker binds ("127.0.0.1" by default; use "0.0.0.0"
    /// when the downloader runs in a container and reaches the host via a
    /// routable address).
    tracker_bind: String,
    /// Port the tracker binds; 0 (the default) lets the OS pick a free
    /// ephemeral one, which is then published in the announce URL.
    tracker_port: u16,
    /// Host placed into the .torrent announce URL (default "127.0.0.1").
    announce_host: String,
    torrent_out: PathBuf,
    url_out: PathBuf,
}

fn parse_args() -> Args {
    let mut args = Args {
        payload_dir: PathBuf::from("payload"),
        tracker_bind: "127.0.0.1".to_string(),
        tracker_port: 0,
        announce_host: "127.0.0.1".to_string(),
        torrent_out: PathBuf::from("mffs.torrent"),
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
            "--payload-dir" => args.payload_dir = value_for!("--payload-dir").into(),
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

    // 1. Validate the payload up front: an empty directory must fail before
    // the tracker is started or tracker.url is written, leaving no residue.
    let files = collect_files(&args.payload_dir);
    assert!(
        !files.is_empty(),
        "payload dir must contain at least one file"
    );

    // 2. Tracker — the announce URL must be live before we bencode.
    let tracker_port = start_tracker(&args.tracker_bind, args.tracker_port)
        .expect("failed to start local tracker");
    let announce_url = format!("http://{}:{}/announce", args.announce_host, tracker_port);
    std::fs::write(&args.url_out, &announce_url).expect("failed to write tracker.url");

    // 3. Deterministic multi-file torrent over the payload directory. Hashing
    // and seeding share one bounded pass: every file is streamed into the seed
    // tree piece-by-piece and never held whole in memory.
    let seed_dir = args
        .torrent_out
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("seed_data");
    std::fs::create_dir_all(&seed_dir).expect("failed to create seed dir");

    let name = "mffs";
    let (pieces, total_size) = hash_and_seed_files(&args.payload_dir, &files, &seed_dir, name);
    let dict = bencode_multifile_torrent(&announce_url, name, &files, &pieces);

    std::fs::write(&args.torrent_out, &dict).expect("failed to write torrent");
    println!(
        "[mffs] wrote {} ({} bytes, {} files, {} pieces)",
        args.torrent_out.display(),
        dict.len(),
        files.len(),
        pieces.len() / 20
    );

    let info = TorrentInfo::from_bytes(dict.clone()).expect("failed to parse generated torrent");
    println!(
        "[mffs] name={} size={} bytes files={} info_hash={}",
        info.name(),
        info.total_size(),
        info.num_files(),
        hex::encode(info.info_hash().expect("info hash"))
    );
    assert_eq!(
        info.total_size(),
        total_size,
        "torrent total size must match the streamed payload"
    );
    println!("[mffs] announcing to {}", announce_url);

    let config = session_config();
    let mut session = Session::new(&config).expect("failed to create libtorrent session");
    let handle = session
        .add_torrent(&info, &seed_dir)
        .expect("failed to add torrent to session");

    seed_until_shutdown(&[(name, handle)]);
}

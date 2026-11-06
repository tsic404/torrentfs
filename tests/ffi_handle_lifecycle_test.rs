//! FFI boundary failures must be reported, never fatal (ADR-002: no C++
//! exception crosses the C ABI).
//!
//! A torrent detached from its session keeps a live handle object, and every
//! libtorrent call on it throws `invalid_torrent_handle`. Before the wrapper's
//! exception fence that exception reached `std::terminate` and killed the
//! process; the read path must instead settle the same failure as `EIO`
//! (C-2.3.5).

mod common;

use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use common::{
    acquire_session_lock, build_multipiece_torrent_pieces, create_single_piece_torrent,
    local_test_config,
};
use torrentfs::{FsError, Session, TorrentError, TorrentInfo, TorrentState};

fn quiet_config() -> torrentfs::TorrentfsConfig {
    let mut config = local_test_config();
    config.dht.enabled = Some(false);
    config.local_discovery.lsd_enabled = Some(false);
    config.local_discovery.upnp_enabled = Some(false);
    config.local_discovery.natpmp_enabled = Some(false);
    config.connections.listen_interfaces = Some("127.0.0.1:0".to_string());
    config
}

fn ingest(
    name: &str,
    save_path: &std::path::Path,
) -> (Session, torrentfs::TorrentHandle, TorrentInfo) {
    let (torrent_bytes, _) = create_single_piece_torrent("http://127.0.0.1:1/announce", name);
    let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
    let mut session = Session::new(&quiet_config()).expect("create session");
    let handle = session
        .add_torrent(&info, save_path)
        .expect("add torrent to session");
    (session, handle, info)
}

/// Write the torrent's pieces in the custom-storage layout, which the storage
/// verifies on ingest.
fn write_piece_files(pieces_root: &Path, info: &TorrentInfo, content: &[u8]) {
    let info_hash_hex = hex::encode(info.info_hash().expect("info hash"));
    let dir = pieces_root.join(&info_hash_hex);
    std::fs::create_dir_all(&dir).expect("create piece directory");
    let piece_length = info.piece_length() as usize;
    for (index, piece) in content.chunks(piece_length).enumerate() {
        std::fs::write(
            dir.join(format!("{}:piece:{}", info_hash_hex, index)),
            piece,
        )
        .expect("write piece file");
    }
}

/// Wait until `handle` leaves the piece-check states.
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

/// Redirect the process's stderr into a file while this value is alive.
///
/// The wrapper's exception fence reports a caught exception with `fprintf` on
/// stderr, and nothing else in this binary writes that line. Seeing it is what
/// separates "the call reached the fence" from "the call returned early from a
/// validity pre-check" — both produce the same error code.
struct StderrCapture {
    saved_fd: libc::c_int,
    path: PathBuf,
    _dir: tempfile::TempDir,
    is_active: bool,
}

impl StderrCapture {
    fn begin() -> Self {
        let dir = tempfile::tempdir().expect("temp dir for stderr");
        let path = dir.path().join("stderr.log");
        let file = std::fs::File::create(&path).expect("create stderr log");
        let saved_fd = unsafe { libc::dup(libc::STDERR_FILENO) };
        assert!(saved_fd >= 0, "dup(stderr) failed");
        let redirected = unsafe { libc::dup2(file.as_raw_fd(), libc::STDERR_FILENO) };
        assert_eq!(redirected, libc::STDERR_FILENO, "dup2(stderr) failed");
        Self {
            saved_fd,
            path,
            _dir: dir,
            is_active: true,
        }
    }

    /// Restore stderr and return everything written to it meanwhile.
    fn finish(mut self) -> String {
        self.restore();
        std::fs::read_to_string(&self.path).expect("read captured stderr")
    }

    fn restore(&mut self) {
        if !self.is_active {
            return;
        }
        unsafe {
            libc::fflush(std::ptr::null_mut());
            libc::dup2(self.saved_fd, libc::STDERR_FILENO);
            libc::close(self.saved_fd);
        }
        self.is_active = false;
    }
}

impl Drop for StderrCapture {
    fn drop(&mut self) {
        self.restore();
    }
}

/// `status()` on a detached handle returns an error and leaves the process
/// alive; the same failure maps to `EIO` on the read path.
#[test]
fn status_on_detached_handle_fails_instead_of_terminating() {
    let _guard = acquire_session_lock();
    let dir = tempfile::tempdir().expect("temp dir");
    let (mut session, handle, _info) = ingest("ffi-fence", dir.path());
    assert!(handle.status().is_ok(), "a live handle must report status");

    let detached = session.remove_torrent(handle, false);

    // Removal completes on libtorrent's thread; wait for it so the handle is
    // observably detached rather than racing the removal.
    let deadline = Instant::now() + Duration::from_secs(10);
    while detached.is_valid() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        !detached.is_valid(),
        "torrent was not detached from the session"
    );

    let capture = StderrCapture::begin();
    let error = detached
        .status()
        .expect_err("status on a detached handle must fail");
    let stderr = capture.finish();

    assert!(
        matches!(error, TorrentError::Unknown { .. }),
        "unexpected error variant: {error:?}"
    );
    let errno: libc::c_int = FsError::from(error).into();
    assert_eq!(errno, libc::EIO, "a failed status must settle as EIO");

    // An `is_valid()` pre-check would return the error above without ever
    // calling `status()`: the race the fence exists for would still reach
    // `std::terminate`, so the fence's own report is the point of the test.
    assert!(
        stderr.contains("[wrapper] C++ exception in lt_torrent_handle_status"),
        "status() rejected the detached handle without entering the exception \
         fence; captured stderr: {stderr:?}"
    );
}

/// Piece and file queries reject out-of-range indices instead of reading past
/// the piece bitfield.
#[test]
fn out_of_range_piece_queries_are_rejected() {
    let _guard = acquire_session_lock();
    let dir = tempfile::tempdir().expect("temp dir");
    let pieces_root = dir.path().join("pieces");
    std::fs::create_dir_all(&pieces_root).expect("create pieces root");

    // One bitfield word: all 32 bits are set once every piece verifies, which
    // makes the pre-guard `pieces[-1]` read of that word observable.
    const NUM_PIECES: usize = 32;
    let (torrent_bytes, content) =
        build_multipiece_torrent_pieces("http://127.0.0.1:1/announce", "ffi-bounds", NUM_PIECES);
    let info = TorrentInfo::from_bytes(torrent_bytes).expect("parse torrent");
    assert_eq!(info.num_pieces() as usize, NUM_PIECES);
    write_piece_files(&pieces_root, &info, &content);

    let mut session =
        Session::new_with_custom_storage(&quiet_config(), &pieces_root).expect("create session");
    let handle = session
        .add_torrent(&info, &pieces_root)
        .expect("add torrent to custom-storage session");
    wait_for_piece_check(&handle);

    assert!(handle.have_piece(0), "fixture pieces must verify");
    assert!(
        handle.have_piece(NUM_PIECES as i32 - 1),
        "fixture pieces must verify"
    );
    assert!(
        !handle.have_piece(-1),
        "negative piece index must not be 'have'"
    );
    assert!(
        !handle.have_piece(NUM_PIECES as i32),
        "past-the-end piece index must not be 'have'"
    );
    assert!(
        !handle.have_piece(1_000_000),
        "far past-the-end piece index must not be 'have'"
    );

    assert_eq!(
        handle.get_file_piece_info(0).expect("file 0").num_pieces,
        NUM_PIECES as i64
    );

    let error = handle
        .get_file_piece_info(-1)
        .expect_err("negative file index must fail");
    assert!(
        matches!(error, TorrentError::InvalidFile(_)),
        "unexpected error variant: {error:?}"
    );
    let error = handle
        .get_file_piece_info(7)
        .expect_err("file index past the end must fail");
    assert!(
        matches!(error, TorrentError::Unknown { .. }),
        "unexpected error variant: {error:?}"
    );
}

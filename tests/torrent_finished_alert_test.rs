//! Integration test: the C wrapper's `torrent_finished` completeness
//! determination. `finished_complete` must be 1 when every piece is
//! downloaded and 0 when selective piece priorities leave filtered pieces
//! missing — the signal `src/infrastructure/alert.rs` uses to keep the two
//! termination reasons distinct in the daemon log.

mod common;

use std::ffi::CString;
use std::ptr;
use std::time::{Duration, Instant};

use libtorrent_sys as lts;

/// Drain alerts from `session` until a `torrent_finished` alert is seen (or
/// `timeout` elapses), returning its `finished_complete` value.
///
/// # Safety
/// `session` must be a live `lt_session_t`.
unsafe fn wait_for_torrent_finished(session: lts::lt_session_t, timeout: Duration) -> Option<i32> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let list = lts::lt_session_pop_alerts(session);
        if !list.is_null() {
            let count = (*list).count;
            let alerts = (*list).alerts;
            let mut result = None;
            for i in 0..count {
                let a = *alerts.add(i as usize);
                if a.type_ == lts::lt_alert_type_t_LT_ALERT_TORRENT_FINISHED as i32 {
                    result = Some(a.finished_complete);
                }
            }
            lts::lt_alert_list_destroy(list);
            if result.is_some() {
                return result;
            }
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    None
}

/// A fully-seeded torrent (every piece already on disk) must fire
/// `torrent_finished` with `finished_complete == 1`.
#[test]
fn torrent_finished_reports_complete_when_all_pieces_downloaded() {
    let _lock = common::acquire_session_lock();
    let (torrent_bytes, content) =
        common::create_test_torrent_with_tracker("http://127.0.0.1:1/announce");

    let dir = tempfile::TempDir::new().unwrap();
    std::fs::write(dir.path().join("final_verification.txt"), &content).unwrap();

    unsafe {
        let mut err = lts::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        let session = lts::lt_session_create(ptr::null(), &mut err);
        assert!(!session.is_null(), "session creation failed");

        let info = lts::lt_torrent_info_create_from_buffer(
            torrent_bytes.as_ptr(),
            torrent_bytes.len(),
            &mut err,
        );
        assert!(!info.is_null(), "torrent info creation failed");

        let save_path = CString::new(dir.path().to_string_lossy().into_owned()).unwrap();
        let handle = lts::lt_session_add_torrent(session, info, save_path.as_ptr(), &mut err);
        assert!(!handle.is_null(), "add torrent failed");

        let complete = wait_for_torrent_finished(session, Duration::from_secs(15))
            .expect("torrent_finished must fire for a fully-seeded torrent");
        assert_eq!(complete, 1, "every piece downloaded → complete");

        lts::lt_torrent_handle_destroy(handle);
        lts::lt_torrent_info_destroy(info);
        lts::lt_session_destroy(session);
    }
}

/// A selective-download torrent (filtered pieces left at priority 0 and not
/// downloaded) must fire `torrent_finished` with `finished_complete == 0`.
#[test]
fn torrent_finished_reports_incomplete_when_filtered_pieces_remain() {
    let _lock = common::acquire_session_lock();
    let (torrent_bytes, content) = common::build_multipiece_torrent("http://127.0.0.1:1/announce");
    const PIECE_LEN: usize = 256 * 1024;

    let dir = tempfile::TempDir::new().unwrap();
    // Only the first two pieces' data is present; pieces 2-3 stay missing.
    std::fs::write(dir.path().join("multi.bin"), &content[..2 * PIECE_LEN]).unwrap();

    unsafe {
        let mut err = lts::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        let session = lts::lt_session_create(ptr::null(), &mut err);
        assert!(!session.is_null(), "session creation failed");

        let info = lts::lt_torrent_info_create_from_buffer(
            torrent_bytes.as_ptr(),
            torrent_bytes.len(),
            &mut err,
        );
        assert!(!info.is_null(), "torrent info creation failed");

        let save_path = CString::new(dir.path().to_string_lossy().into_owned()).unwrap();
        let handle = lts::lt_session_add_torrent(session, info, save_path.as_ptr(), &mut err);
        assert!(!handle.is_null(), "add torrent failed");

        // Filter out pieces 2 and 3 (priority 0 = not wanted), leaving only
        // pieces 0-1 wanted and downloaded — a selective-download finish.
        assert_eq!(lts::lt_torrent_handle_set_piece_priority(handle, 2, 0), 0);
        assert_eq!(lts::lt_torrent_handle_set_piece_priority(handle, 3, 0), 0);

        let complete = wait_for_torrent_finished(session, Duration::from_secs(15))
            .expect("torrent_finished must fire once all wanted pieces are downloaded");
        assert_eq!(complete, 0, "filtered pieces still missing → incomplete");

        lts::lt_torrent_handle_destroy(handle);
        lts::lt_torrent_info_destroy(info);
        lts::lt_session_destroy(session);
    }
}

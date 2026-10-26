//! Design acceptance case TC-ingest-06: a non-bencode `.torrent` write fails
//! at close with `EINVAL`, leaves no `data/` projection, and the rejection
//! leaves nothing behind that would decide an immediate retry of the same
//! name — the retry is judged by its content alone
//! (`docs/03-acceptance.md` §A AC-06, contracts C-2.4.2 / C-2.4.4).

use crate::common::{create_single_piece_torrent, wave_slot, IngestHarness};
use std::time::{Duration, Instant};

pub const TC_ID: &str = "TC-ingest-06";

/// The retry bound the design fixes: a rejected write must not delay a
/// same-name retry's verdict past this window.
const RETRY_WINDOW: Duration = Duration::from_millis(200);

/// A non-bencode close is rejected with `EINVAL`; a same-name retry started
/// inside 200ms is still judged by content — invalid again fails the same way,
/// valid is accepted and projected.
#[test]
fn tc_ingest_06_rejects_non_bencode_and_retries_by_content() {
    let _slot = wave_slot();
    let mut ingest = IngestHarness::new();
    let incoming = ingest.mkdir(&["incoming"]);
    let invalid = b"not a bencoded torrent\n";
    let (valid, _) = create_single_piece_torrent("http://127.0.0.1:9/announce", "ingest-06.bin");

    let err = ingest
        .try_copy_torrent(incoming, "candidate.torrent", invalid)
        .expect_err("a non-bencode write must be rejected");
    assert_eq!(
        libc::c_int::from(err),
        libc::EINVAL,
        "the close must report EINVAL for non-bencode content"
    );
    assert!(
        ingest
            .data_lookup(&["incoming", "candidate.torrent"])
            .is_none(),
        "a rejected write must not project a directory"
    );
    let data_incoming = ingest
        .data_lookup(&["incoming"])
        .expect("the metadata/ directory must still mirror under data/");
    assert!(
        !ingest
            .children(data_incoming)
            .iter()
            .any(|entry| entry.name == "candidate.torrent"),
        "the data/ listing must not carry the rejected name"
    );

    // Immediate retry with the same invalid content: judged by content, so it
    // fails identically.  A stale buffer inode would surface as EEXIST here,
    // and a delayed verdict would break the design's 200ms bound.
    let rejected_at = Instant::now();
    let err = ingest
        .try_copy_torrent(incoming, "candidate.torrent", invalid)
        .expect_err("an immediate same-name retry must be rejected by content");
    assert_eq!(
        libc::c_int::from(err),
        libc::EINVAL,
        "the retry must report the same EINVAL, not a residue error"
    );
    assert!(
        rejected_at.elapsed() <= RETRY_WINDOW,
        "the retry must be decided within {:?}, took {:?}",
        RETRY_WINDOW,
        rejected_at.elapsed()
    );
    assert!(
        ingest
            .data_lookup(&["incoming", "candidate.torrent"])
            .is_none(),
        "the rejected retry must not project a directory"
    );

    // Immediate retry with valid content under the same name: accepted, and
    // the projection appears.
    let rejected_at = Instant::now();
    ingest
        .try_copy_torrent(incoming, "candidate.torrent", &valid)
        .expect("valid content under the same name must be accepted");
    assert!(
        rejected_at.elapsed() <= RETRY_WINDOW,
        "the valid retry's verdict must land within {:?}, took {:?}",
        RETRY_WINDOW,
        rejected_at.elapsed()
    );
    ingest.wait_persisted();
    assert!(
        ingest
            .data_lookup(&["incoming", "candidate.torrent", "ingest-06.bin"])
            .is_some(),
        "the accepted retry must project its directory"
    );
}

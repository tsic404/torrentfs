//! Shared helpers for the acceptance suite.
//!
//! Re-exports the repo's integration-test infrastructure (`tests/common`,
//! the MiniTracker + TestHarness + torrent fixtures) and adds the
//! wave-slot concurrency cap used by the engine-level read tests.  A
//! per-TC `name` gives each swarm its own info hash, so parallel tests in
//! this binary never pair through LSD with another test's seeder.

#[path = "../common/mod.rs"]
pub mod base;

pub use base::{
    acquire_session_lock, build_multipiece_torrent_named, local_test_config, MiniTracker,
    TestHarness,
};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

/// How many tests in this binary may run at once.
///
/// Each acceptance test drives one to three libtorrent sessions.  Letting
/// them all run at once makes libtorrent 2.1 segfault inside `add_torrent`
/// and starves the engine threads enough to stretch a read's wait windows;
/// capping the wave bounds how many sessions are live while the module stays
/// far below libtest's 60s slow-test warning.  Four is what a 4-core CI
/// runner already gets from libtest's own default thread count.
const MAX_CONCURRENT_TESTS: usize = 4;

/// Tests currently holding a wave slot.
static ACTIVE_TESTS: AtomicUsize = AtomicUsize::new(0);

/// Claim a wave slot, blocking until one frees.
///
/// Hold the returned guard for the whole test — it is the test's *sessions*,
/// not merely their creation, that must stay bounded.
pub fn wave_slot() -> WaveSlot {
    loop {
        let active = ACTIVE_TESTS.load(Ordering::SeqCst);
        if active < MAX_CONCURRENT_TESTS
            && ACTIVE_TESTS
                .compare_exchange(active, active + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return WaveSlot;
        }
        thread::sleep(Duration::from_millis(50));
    }
}

/// Releases its wave slot on drop.
pub struct WaveSlot;

impl Drop for WaveSlot {
    fn drop(&mut self) {
        ACTIVE_TESTS.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The single-piece fixture torrent, under a `name` unique to one test.
///
/// Every test in this binary runs in parallel and `local_test_config()`
/// leaves LSD enabled: libtorrent pairs two same-host sessions that serve
/// the same info hash, so a shared fixture name would let one test's
/// downloader read from another test's seeder — and lose that connection
/// mid-read when that test tears its session down.  A per-test `name` gives
/// each test its own swarm identity.
pub fn unique_torrent(name: &'static str) -> impl FnOnce(&str) -> (Vec<u8>, Vec<u8>) {
    move |announce_url| base::create_single_piece_torrent(announce_url, name)
}

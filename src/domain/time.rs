//! Deadline arithmetic that cannot panic.
//!
//! Every deadline in the process is stamped as `now + duration`, and that
//! addition panics on overflow. The duration can come from configuration, so a
//! value near the clock's limit must degrade into a very distant deadline
//! rather than abort the thread that computes it.

use std::time::{Duration, Instant};

/// Fallback span for a deadline whose requested duration overflows the clock.
///
/// One year is beyond any configured window (the loader caps the local timeout
/// keys) and comfortably inside `Instant`'s range on every supported platform.
pub const MAX_DEADLINE_SPAN: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// `now + duration`, saturating instead of panicking on overflow.
pub fn deadline_from(now: Instant, duration: Duration) -> Instant {
    now.checked_add(duration)
        .or_else(|| now.checked_add(MAX_DEADLINE_SPAN))
        .unwrap_or(now)
}

/// `Instant::now() + duration`, saturating instead of panicking on overflow.
pub fn deadline_after(duration: Duration) -> Instant {
    deadline_from(Instant::now(), duration)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deadline_after_accepts_ordinary_spans() {
        let before = Instant::now();
        let deadline = deadline_after(Duration::from_secs(30));
        assert!(deadline > before);
        assert!(deadline <= Instant::now() + Duration::from_secs(30));
    }

    /// A span the clock cannot represent must not panic; it saturates to the
    /// fallback window instead of unwinding the caller.
    #[test]
    fn deadline_after_saturates_instead_of_panicking() {
        let before = Instant::now();
        let deadline = deadline_after(Duration::from_secs(u64::MAX));
        assert!(deadline > before);
    }
}

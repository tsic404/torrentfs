//! Renderer for the per-torrent `.read-errors` diagnostics file.
//!
//! A failed read reaches the client as a bare `ENODATA`, indistinguishable
//! between "no seeder", "the on-disk cache lost the piece", and "the seeder is
//! slow".  The engine records why the read failed (see
//! [`crate::infrastructure::download::ReadFailure`]); this module turns those
//! records into the file the operator cats from inside the mount.

use std::time::{SystemTime, UNIX_EPOCH};

use crate::infrastructure::download::ReadFailure;

/// Render the `.read-errors` content for one torrent: the recorded failures,
/// newest first.  Empty when the torrent has no recorded failure, so the file
/// reads as empty rather than as a stale report.
pub fn generate_read_errors(
    torrent_name: &str,
    info_hash: &str,
    failures: &[ReadFailure],
) -> Vec<u8> {
    if failures.is_empty() {
        return Vec::new();
    }

    let mut output = String::new();
    output.push_str(&format!("===== read errors: {} =====\n", torrent_name));
    output.push_str(&format!("info_hash: {}\n", info_hash));
    output.push_str(&format!("records: {} (newest first)\n", failures.len()));

    for (index, failure) in failures.iter().enumerate() {
        output.push('\n');
        output.push_str(&format!(
            "[{}] cause: {}\n",
            index + 1,
            failure.cause.as_str()
        ));
        output.push_str(&format!("at: {}\n", format_utc(failure.at)));
        output.push_str(&format!(
            "swarm: Peers:{} Seeds:{} Progress:{:.2}%\n",
            failure.num_peers, failure.num_seeds, failure.progress
        ));
        output.push_str(&format!("message: {}\n", failure.message));
        output.push_str(&format!(
            "suggested action: {}\n",
            failure.cause.suggested_action()
        ));
    }

    output.into_bytes()
}

/// Render a [`SystemTime`] as an ISO-8601 UTC timestamp
/// (`YYYY-MM-DDTHH:MM:SSZ`).
///
/// Hand-rolled calendar conversion: the crate carries no date-time dependency,
/// and the file is read by operators, for whom a bare epoch second is useless
/// when correlating with a log line.  Times before the Unix epoch render as the
/// epoch itself — a read failure happens now, so a pre-epoch stamp can only
/// come from a wrong system clock.
fn format_utc(time: SystemTime) -> String {
    let secs = time
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let days = (secs / 86_400) as i64;
    let secs_of_day = secs % 86_400;
    let (year, month, day) = civil_from_days(days);
    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        year,
        month,
        day,
        secs_of_day / 3600,
        (secs_of_day / 60) % 60,
        secs_of_day % 60
    )
}

/// Days since 1970-01-01 → `(year, month, day)`, via Howard Hinnant's
/// `civil_from_days` (proleptic Gregorian, valid for any `i64` day count).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let year = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let month = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

#[cfg(test)]
mod tests {
    use super::{format_utc, generate_read_errors};
    use crate::infrastructure::download::{ReadFailure, ReadStallCause};
    use std::time::{Duration, UNIX_EPOCH};

    fn failure(cause: ReadStallCause, message: &str, at_secs: u64) -> ReadFailure {
        ReadFailure {
            cause,
            message: message.to_string(),
            at: UNIX_EPOCH + Duration::from_secs(at_secs),
            num_peers: 3,
            num_seeds: 1,
            progress: 41.5,
        }
    }

    /// A torrent that never failed a read renders an empty file — the operator
    /// must not read a leftover or a placeholder as a failure.
    #[test]
    fn no_failures_renders_empty_content() {
        assert!(generate_read_errors("ubuntu.iso", "abc123", &[]).is_empty());
    }

    /// The acceptance format: a recorded failure is readable with its cause,
    /// the reason text, the instant, the swarm state at that instant, and the
    /// action to take.
    #[test]
    fn failure_renders_cause_message_instant_swarm_and_action() {
        let cause = ReadStallCause::NoSeeder;
        let failure = failure(
            cause,
            "No seeder connected for info_hash abc123 after 9s peer discovery",
            // 2026-09-28T21:17:06Z
            1_790_630_226,
        );

        let content = String::from_utf8(generate_read_errors("ubuntu.iso", "abc123", &[failure]))
            .expect("utf-8");

        assert!(content.contains("===== read errors: ubuntu.iso ====="));
        assert!(content.contains("cause: NoSeeder"));
        assert!(content
            .contains("message: No seeder connected for info_hash abc123 after 9s peer discovery"));
        assert!(content.contains("at: 2026-09-28T21:17:06Z"));
        assert!(content.contains("swarm: Peers:3 Seeds:1 Progress:41.50%"));
        assert!(content.contains(&format!("suggested action: {}", cause.suggested_action())));
    }

    /// Records render newest first: the top block is the failure that just
    /// happened, which is what the operator comes to the file for.
    #[test]
    fn records_render_newest_first() {
        let older = failure(ReadStallCause::SlowSwarm, "older failure", 1_000);
        let newer = failure(ReadStallCause::CacheStall, "newer failure", 2_000);

        let content =
            String::from_utf8(generate_read_errors("t", "h", &[newer, older])).expect("utf-8");

        assert!(content.contains("records: 2 (newest first)"));
        let newer_at = content.find("newer failure").expect("newer record");
        let older_at = content.find("older failure").expect("older record");
        assert!(newer_at < older_at, "newest record must render first");
        assert!(content.contains("[1] cause: CacheStall"));
        assert!(content.contains("[2] cause: SlowSwarm"));
    }

    /// The timestamp is the operator's only way to correlate a read failure
    /// with anything else, so the calendar conversion is pinned against dates
    /// that exercise a leap year and a leap day.
    #[test]
    fn utc_timestamps_match_known_instants() {
        assert_eq!(format_utc(UNIX_EPOCH), "1970-01-01T00:00:00Z");
        assert_eq!(
            format_utc(UNIX_EPOCH + Duration::from_secs(946_684_800)),
            "2000-01-01T00:00:00Z"
        );
        assert_eq!(
            format_utc(UNIX_EPOCH + Duration::from_secs(1_709_210_096)),
            "2024-02-29T12:34:56Z"
        );
    }
}

//! Domain error types — core error enum and conversion utilities.

use std::ffi::CStr;
use thiserror::Error;

#[derive(Error, Debug)]
#[allow(dead_code)]
pub enum TorrentError {
    #[error("Invalid torrent file: {0}")]
    InvalidFile(String),

    #[error("Failed to parse torrent: {0}")]
    ParseError(String),

    #[error("invalid configuration: {0}")]
    ConfigError(String),

    #[error("IO error: {0}")]
    IoError(String),

    #[error("Null pointer encountered")]
    NullPointer,

    #[error("No peers available: {0}. Tracker returned 0 peers and 0 seeds. Check tracker health or try again later.")]
    NoPeers(String),

    #[error("Piece not ready: {0}")]
    PieceNotReady(String),

    #[error("Operation timed out: {0}")]
    Timeout(String),

    #[error("Unknown error: code {code}, message: {message}")]
    Unknown { code: i32, message: String },
}

impl TorrentError {
    /// Returns `true` if this error represents a transient condition that
    /// may succeed on retry (e.g., timeout, no peers yet, piece not ready).
    ///
    /// Permanent errors (invalid file, parse errors, null pointers) return
    /// `false`.  `IoError` is conservatively treated as non-transient since
    /// I/O errors may indicate disk-full or permission problems.
    pub fn is_transient(&self) -> bool {
        matches!(
            self,
            TorrentError::NoPeers(_) | TorrentError::PieceNotReady(_) | TorrentError::Timeout(_)
        )
    }
}

impl From<std::io::Error> for TorrentError {
    fn from(err: std::io::Error) -> Self {
        TorrentError::IoError(err.to_string())
    }
}

pub type TorrentResult<T> = Result<T, TorrentError>;

/// Classifies a read error as transient (should retry) or permanent.
///
/// Returns `true` for errors that are likely to resolve on retry:
/// - `PieceNotReady` — piece data not yet available
/// - `NoPeers` — no peers connected, may connect later
/// - `Timeout` — operation timed out, may succeed on retry
/// - `IoError` — transient I/O issues (disk busy, network flake)
/// - `Unknown` — unknown errors from libtorrent, may be temporary
///
/// Returns `false` for permanent errors that retrying won't fix:
/// - `InvalidFile` — path/format issues
/// - `ParseError` — bdecode / data corruption
/// - `NullPointer` — internal unrecoverable null
///
/// This is intentionally broader than `TorrentError::is_transient()`:
/// `IoError` and `Unknown` are treated as transient here because the
/// read retry loop runs under a budget and I/O flakes / unknown libtorrent
/// errors often resolve on a subsequent attempt.
pub fn is_transient_read_error(err: &TorrentError) -> bool {
    matches!(
        err,
        TorrentError::PieceNotReady(_)
            | TorrentError::NoPeers(_)
            | TorrentError::Timeout(_)
            | TorrentError::IoError(_)
            | TorrentError::Unknown { .. }
    )
}

/// Convert a C `lt_error_t` pointer into a `TorrentError`.
///
/// # Safety
///
/// `error` must be either null or a valid, aligned pointer to an `lt_error_t`.
pub(crate) unsafe fn error_from_c(error: *const libtorrent_sys::lt_error_t) -> TorrentError {
    if error.is_null() {
        return TorrentError::Unknown {
            code: -1,
            message: "Unknown error".to_string(),
        };
    }

    // SAFETY: `error` was checked for null above; if non-null it is a valid
    // pointer from libtorrent that remains valid for the duration of this call.
    let error_ref = &*error;
    let message = if error_ref.message.is_null() {
        "Unknown error".to_string()
    } else {
        // SAFETY: `message` was checked for non-null above; the C string is
        // NUL-terminated per libtorrent's contract and remains valid here.
        CStr::from_ptr(error_ref.message)
            .to_string_lossy()
            .into_owned()
    };

    let lower = message.to_lowercase();
    if lower.contains("bdecode") || lower.contains("parse") || lower.contains("invalid") {
        TorrentError::ParseError(message)
    } else if lower.contains("file") || lower.contains("path") || lower.contains("not found") {
        TorrentError::InvalidFile(message)
    } else if lower.contains("timed out") || lower.contains("timeout") {
        TorrentError::Timeout(message)
    } else {
        TorrentError::Unknown {
            code: error_ref.code,
            message,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── is_transient() ──────────────────────────────────────────────────

    #[test]
    fn test_is_transient_true_for_no_peers() {
        assert!(TorrentError::NoPeers("no peers".into()).is_transient());
    }

    #[test]
    fn test_is_transient_true_for_piece_not_ready() {
        assert!(TorrentError::PieceNotReady("piece 0".into()).is_transient());
    }

    #[test]
    fn test_is_transient_true_for_timeout() {
        assert!(TorrentError::Timeout("timed out".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_invalid_file() {
        assert!(!TorrentError::InvalidFile("bad file".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_parse_error() {
        assert!(!TorrentError::ParseError("bad parse".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_io_error() {
        assert!(!TorrentError::IoError("disk full".into()).is_transient());
    }

    #[test]
    fn test_is_transient_false_for_null_pointer() {
        assert!(!TorrentError::NullPointer.is_transient());
    }

    #[test]
    fn test_is_transient_false_for_unknown() {
        assert!(!TorrentError::Unknown {
            code: -1,
            message: "unknown".into()
        }
        .is_transient());
    }

    // ── error_from_c ────────────────────────────────────────────────────

    /// Helper: simulate an lt_error_t with the given message string.
    unsafe fn make_lt_error(msg: &str) -> libtorrent_sys::lt_error_t {
        use std::ffi::CString;
        let c_msg = CString::new(msg).unwrap();
        libtorrent_sys::lt_error_t {
            message: c_msg.into_raw(),
            code: 0,
        }
    }

    #[test]
    fn test_error_from_c_null_returns_unknown() {
        let result = unsafe { error_from_c(std::ptr::null()) };
        assert!(matches!(result, TorrentError::Unknown { .. }));
    }

    #[test]
    fn test_error_from_c_timeout_lowercase() {
        let err = unsafe { make_lt_error("operation timed out") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_titlecase() {
        let err = unsafe { make_lt_error("Timed out waiting for peers") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_uppercase() {
        let err = unsafe { make_lt_error("TIMEOUT: connection failed") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_timeout_as_substring() {
        let err = unsafe { make_lt_error("request timed out after 30s") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Timeout(_)));
    }

    #[test]
    fn test_error_from_c_bdecode_maps_to_parse_error() {
        let err = unsafe { make_lt_error("bdecode error: invalid encoding") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::ParseError(_)));
    }

    #[test]
    fn test_error_from_c_invalid_maps_to_parse_error() {
        let err = unsafe { make_lt_error("invalid torrent file") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::ParseError(_)));
    }

    #[test]
    fn test_error_from_c_file_maps_to_invalid_file() {
        let err = unsafe { make_lt_error("file not found: /path/to/torrent") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::InvalidFile(_)));
    }

    #[test]
    fn test_error_from_c_unclassified_maps_to_unknown() {
        let err = unsafe { make_lt_error("some random error text") };
        let result = unsafe { error_from_c(&err) };
        assert!(matches!(result, TorrentError::Unknown { .. }));
    }

    // ── is_transient_read_error — transient (should return true) ──

    #[test]
    fn test_piece_not_ready_is_transient() {
        let err = TorrentError::PieceNotReady("piece 42 not available".into());
        assert!(
            is_transient_read_error(&err),
            "PieceNotReady should be transient"
        );
    }

    #[test]
    fn test_no_peers_is_transient() {
        let err = TorrentError::NoPeers("tracker returned 0 peers".into());
        assert!(is_transient_read_error(&err), "NoPeers should be transient");
    }

    #[test]
    fn test_timeout_is_transient() {
        let err = TorrentError::Timeout("timed out".into());
        assert!(is_transient_read_error(&err), "Timeout should be transient");
    }

    #[test]
    fn test_io_error_is_transient() {
        let err = TorrentError::IoError("disk busy".into());
        assert!(is_transient_read_error(&err), "IoError should be transient");
    }

    #[test]
    fn test_unknown_is_transient() {
        let err = TorrentError::Unknown {
            code: 42,
            message: "something went wrong".into(),
        };
        assert!(is_transient_read_error(&err), "Unknown should be transient");
    }

    #[test]
    fn test_unknown_zero_code_is_transient() {
        let err = TorrentError::Unknown {
            code: 0,
            message: String::new(),
        };
        assert!(
            is_transient_read_error(&err),
            "Unknown with code 0 should be transient"
        );
    }

    // ── is_transient_read_error — non-transient (should return false) ──

    #[test]
    fn test_invalid_file_is_not_transient() {
        let err = TorrentError::InvalidFile("path not found".into());
        assert!(
            !is_transient_read_error(&err),
            "InvalidFile should NOT be transient"
        );
    }

    #[test]
    fn test_parse_error_is_not_transient() {
        let err = TorrentError::ParseError("bdecode failed".into());
        assert!(
            !is_transient_read_error(&err),
            "ParseError should NOT be transient"
        );
    }

    #[test]
    fn test_null_pointer_is_not_transient() {
        let err = TorrentError::NullPointer;
        assert!(
            !is_transient_read_error(&err),
            "NullPointer should NOT be transient"
        );
    }

    // ── is_transient_read_error — edge cases ──

    #[test]
    fn test_empty_piece_not_ready() {
        let err = TorrentError::PieceNotReady(String::new());
        assert!(
            is_transient_read_error(&err),
            "PieceNotReady with empty message should be transient"
        );
    }

    #[test]
    fn test_empty_no_peers() {
        let err = TorrentError::NoPeers(String::new());
        assert!(
            is_transient_read_error(&err),
            "NoPeers with empty message should be transient"
        );
    }

    #[test]
    fn test_empty_io_error() {
        let err = TorrentError::IoError(String::new());
        assert!(
            is_transient_read_error(&err),
            "IoError with empty message should be transient"
        );
    }

    #[test]
    fn test_empty_invalid_file() {
        let err = TorrentError::InvalidFile(String::new());
        assert!(
            !is_transient_read_error(&err),
            "InvalidFile with empty message should NOT be transient"
        );
    }

    #[test]
    fn test_empty_parse_error() {
        let err = TorrentError::ParseError(String::new());
        assert!(
            !is_transient_read_error(&err),
            "ParseError with empty message should NOT be transient"
        );
    }

    #[test]
    fn test_negative_unknown_code() {
        let err = TorrentError::Unknown {
            code: -1,
            message: "error".into(),
        };
        assert!(
            is_transient_read_error(&err),
            "Unknown with negative code should be transient"
        );
    }

    // ── Classification completeness contract ──
    // Helper with exhaustive match — adding a new TorrentError variant
    // causes a compile-time error here, forcing the author to classify it.
    fn classify_via_match(err: &TorrentError) -> bool {
        match err {
            TorrentError::PieceNotReady(_)
            | TorrentError::NoPeers(_)
            | TorrentError::Timeout(_)
            | TorrentError::IoError(_)
            | TorrentError::Unknown { .. } => true,
            TorrentError::InvalidFile(_)
            | TorrentError::ParseError(_)
            | TorrentError::ConfigError(_)
            | TorrentError::NullPointer => false,
        }
    }

    #[test]
    fn test_all_variants_covered_explicitly() {
        // Every variant must be reachable in the exhaustive match above,
        // and is_transient_read_error must agree with it on every variant.
        let variants: &[TorrentError] = &[
            TorrentError::PieceNotReady("p".into()),
            TorrentError::NoPeers("n".into()),
            TorrentError::Timeout("t".into()),
            TorrentError::IoError("i".into()),
            TorrentError::Unknown {
                code: 1,
                message: "u".into(),
            },
            TorrentError::InvalidFile("f".into()),
            TorrentError::ParseError("p".into()),
            TorrentError::ConfigError("c".into()),
            TorrentError::NullPointer,
        ];

        for err in variants {
            assert_eq!(
                is_transient_read_error(err),
                classify_via_match(err),
                "mismatch for variant: {:?}",
                err
            );
        }
    }
}

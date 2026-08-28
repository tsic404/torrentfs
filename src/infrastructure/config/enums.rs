//! Strongly-typed enum fields backed by `Option<i64>`.
//!
//! TOML files use bare integers (libtorrent's `settings_pack` wire format), so
//! each enum stays deserialize-compatible with `Option<i64>` while enforcing
//! the value domain in `validate_enum_ranges()` with `--config-check`. The
//! ranges below mirror the C++ enums in libtorrent's `settings_pack.hpp`
//! (ABI v2), including deprecated-but-legal members (libtorrent's own
//! `validate_setting` accepts them):
//!
//! - `suggest_mode_t`:              `no_piece_suggestions` / `suggest_read_cache`
//! - `choking_algorithm_t`:         `fixed_slots_choker` / `rate_based_choker` /
//!                                  `deprecated_bittyrant_choker`
//! - `seed_choking_algorithm_t`:    `round_robin` / `fastest_upload` / `anti_leech`
//! - `bandwidth_mixed_algo_t`:      `prefer_tcp` / `peer_proportional`
//! - `enc_policy`:                  `pe_forced` / `pe_enabled` / `pe_disabled`
//! - `enc_level`:                   `pe_plaintext` / `pe_rc4` / `pe_both`
//!
//! See `grep -n "enum.*_t\|enum enc_" /usr/include/libtorrent/settings_pack.hpp`.

use std::fmt;

/// Validation error for an out-of-domain enum value.
pub struct EnumRangeError {
    /// TOML field name (e.g. `algorithms.choking_algorithm`).
    pub field: &'static str,
    /// Human-readable legal range (e.g. `0, 2`).
    pub allowed: &'static str,
}

impl fmt::Display for EnumRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "enum value for `{}` out of range: legal values are {{{}}}",
            self.field, self.allowed
        )
    }
}

/// Validate an enum-shaped integer field against an explicit legal set.
fn check(
    field: &'static str,
    allowed: &'static str,
    values: &[i64],
    value: i64,
) -> Option<EnumRangeError> {
    if values.contains(&value) {
        None
    } else {
        Some(EnumRangeError { field, allowed })
    }
}

pub struct EnumValidator;

impl EnumValidator {
    /// libtorrent `suggest_mode_t`: `no_piece_suggestions=0`, `suggest_read_cache=1`.
    pub fn suggest_mode(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| check("algorithms.suggest_mode", "0, 1", &[0, 1], n))
    }

    /// libtorrent `choking_algorithm_t`: `fixed_slots_choker=0`, `rate_based_choker=2`,
    /// `deprecated_bittyrant_choker=3` (deprecated but accepted by libtorrent).
    pub fn choking_algorithm(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| check("algorithms.choking_algorithm", "0, 2, 3", &[0, 2, 3], n))
    }

    /// libtorrent `seed_choking_algorithm_t`: `round_robin=0`, `fastest_upload=1`, `anti_leech=2`.
    pub fn seed_choking_algorithm(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| {
            check(
                "algorithms.seed_choking_algorithm",
                "0, 1, 2",
                &[0, 1, 2],
                n,
            )
        })
    }

    /// libtorrent `bandwidth_mixed_algo_t`: `prefer_tcp=0`, `peer_proportional=1`.
    pub fn mixed_mode_algorithm(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| check("algorithms.mixed_mode_algorithm", "0, 1", &[0, 1], n))
    }

    /// libtorrent `enc_policy`: `pe_forced=0`, `pe_enabled=1`, `pe_disabled=2`.
    pub fn encryption_policy(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| check("encryption.encryption_policy", "0, 1, 2", &[0, 1, 2], n))
    }

    /// libtorrent `enc_level`: `pe_plaintext=1`, `pe_rc4=2`, `pe_both=3`.
    pub fn allowed_encryption_level(v: Option<i64>) -> Option<EnumRangeError> {
        v.and_then(|n| {
            check(
                "encryption.allowed_encryption_level",
                "1, 2, 3",
                &[1, 2, 3],
                n,
            )
        })
    }
}

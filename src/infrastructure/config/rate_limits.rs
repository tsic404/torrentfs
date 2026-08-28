use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Rate Limits
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct RateLimitsConfig {
    pub download_rate_limit: Option<i64>,
    pub upload_rate_limit: Option<i64>,
    pub rate_limit_utp: Option<bool>,
    pub rate_limit_ip_overhead: Option<bool>,
}

impl RateLimitsConfig {
    /// Validate rate limits: `0` is a legal libtorrent value meaning
    /// "unlimited" (`settings_pack.hpp`), so only negative values are
    /// rejected. The upper bound is `i32::MAX` because the wrapper applies
    /// settings via `static_cast<int>` (`libtorrent_wrapper.cpp:1055-1056`),
    /// which silently truncates larger values.
    pub(crate) fn validate(&self) -> Result<(), String> {
        check_non_negative_i32("download_rate_limit", self.download_rate_limit)?;
        check_non_negative_i32("upload_rate_limit", self.upload_rate_limit)?;
        Ok(())
    }
}

/// A rate limit must be `0..=i32::MAX`: negative is invalid, and anything
/// above `i32::MAX` would be truncated at the FFI boundary.
fn check_non_negative_i32(name: &str, value: Option<i64>) -> Result<(), String> {
    match value {
        Some(v) if v < 0 => Err(format!("[rate_limits] {} must be >= 0, got {}", name, v)),
        Some(v) if v > i32::MAX as i64 => Err(format!(
            "[rate_limits] {} must be <= {}, got {}",
            name,
            i32::MAX,
            v
        )),
        _ => Ok(()),
    }
}

impl WriteJson for RateLimitsConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, download_rate_limit);
        json_field_int!(map, self, upload_rate_limit);
        json_field_bool!(map, self, rate_limit_utp);
        json_field_bool!(map, self, rate_limit_ip_overhead);
    }
}

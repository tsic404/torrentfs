use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Misc
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct MiscConfig {
    pub ignore_resume_timestamps: Option<bool>,
    pub no_recheck_incomplete_resume: Option<bool>,
    pub disable_hash_checks: Option<bool>,
    pub allow_i2p_mixed: Option<bool>,
    pub incoming_starts_queued: Option<bool>,
    pub ban_web_seeds: Option<bool>,
    pub report_web_seed_downloads: Option<bool>,
    pub num_optimistic_unchoke_slots: Option<i64>,
    pub max_failcount: Option<i64>,
    pub max_rejects: Option<i64>,
    pub share_mode_target: Option<i64>,
    pub apply_ip_filter_to_trackers: Option<bool>,
    pub announce_double_nat: Option<bool>,
    pub lock_files: Option<bool>,
    pub local_service_announce_interval: Option<i64>,
    pub read_job_every: Option<i64>,
    pub strict_super_seeding: Option<bool>,
    pub enable_os_cache: Option<bool>,
    pub close_redundant_connections: Option<bool>,
}

impl WriteJson for MiscConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_bool!(map, self, ignore_resume_timestamps);
        json_field_bool!(map, self, no_recheck_incomplete_resume);
        json_field_bool!(map, self, disable_hash_checks);
        json_field_bool!(map, self, allow_i2p_mixed);
        json_field_bool!(map, self, incoming_starts_queued);
        json_field_bool!(map, self, ban_web_seeds);
        json_field_bool!(map, self, report_web_seed_downloads);
        json_field_int!(map, self, num_optimistic_unchoke_slots);
        json_field_int!(map, self, max_failcount);
        json_field_int!(map, self, max_rejects);
        json_field_int!(map, self, share_mode_target);
        json_field_bool!(map, self, apply_ip_filter_to_trackers);
        json_field_bool!(map, self, announce_double_nat);
        json_field_bool!(map, self, lock_files);
        json_field_int!(map, self, local_service_announce_interval);
        json_field_int!(map, self, read_job_every);
        json_field_bool!(map, self, strict_super_seeding);
        json_field_bool!(map, self, enable_os_cache);
        json_field_bool!(map, self, close_redundant_connections);
    }
}

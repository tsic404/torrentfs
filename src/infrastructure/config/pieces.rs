use serde::{Deserialize, Serialize};

use crate::infrastructure::config::WriteJson;
use crate::json_field_bool;
use crate::json_field_int;

// ============================================================
// Pieces
// ============================================================

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(deny_unknown_fields)]
pub struct PiecesConfig {
    pub whole_pieces_threshold: Option<i64>,
    pub prioritize_partial_pieces: Option<bool>,
    pub max_out_request_queue: Option<i64>,
    pub max_allowed_in_request_queue: Option<i64>,
    pub piece_timeout: Option<i64>,
    pub request_timeout: Option<i64>,
    pub predictive_piece_announce: Option<i64>,
    pub max_suggest_pieces: Option<i64>,
    pub drop_skipped_requests: Option<bool>,
    pub seeding_piece_quota: Option<i64>,
    pub max_sparse_regions: Option<i64>,
}

impl WriteJson for PiecesConfig {
    fn write_json(&self, map: &mut serde_json::Map<String, serde_json::Value>) {
        json_field_int!(map, self, whole_pieces_threshold);
        json_field_bool!(map, self, prioritize_partial_pieces);
        json_field_int!(map, self, max_out_request_queue);
        json_field_int!(map, self, max_allowed_in_request_queue);
        json_field_int!(map, self, piece_timeout);
        json_field_int!(map, self, request_timeout);
        json_field_int!(map, self, predictive_piece_announce);
        json_field_int!(map, self, max_suggest_pieces);
        json_field_bool!(map, self, drop_skipped_requests);
        json_field_int!(map, self, seeding_piece_quota);
        json_field_int!(map, self, max_sparse_regions);
    }
}

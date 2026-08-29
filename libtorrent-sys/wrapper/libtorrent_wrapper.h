#ifndef LIBTORRENT_WRAPPER_H
#define LIBTORRENT_WRAPPER_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef void* lt_session_t;
typedef void* lt_torrent_handle_t;
typedef void* lt_torrent_info_t;
typedef void* lt_file_storage_t;

typedef struct {
    const char* path;
    uint64_t size;
} lt_file_entry_t;

typedef struct {
    const char* name;
    uint64_t total_size;
    uint32_t piece_length;
    uint32_t num_pieces;
    uint32_t num_files;
    lt_file_entry_t* files;
    const uint8_t* info_hash;
} lt_torrent_metadata_t;

typedef struct {
    const char* message;
    int code;
} lt_error_t;

// ── Alert types for background alert consumption ──
typedef enum {
    LT_ALERT_READ_PIECE = 1,
    LT_ALERT_SESSION_STATS = 2,
    LT_ALERT_TORRENT_FINISHED = 3,
    LT_ALERT_TORRENT_REMOVED = 4,
    LT_ALERT_OTHER = 99,
} lt_alert_type_t;

typedef struct {
    int type;                    // lt_alert_type_t
    char info_hash[41];          // hex-encoded, empty if N/A
    int piece_index;             // for read_piece_alert
    int error_code;              // for read_piece_alert (0 = no error), session_stats N/A
    // read_piece_alert data
    uint8_t* piece_data;
    size_t piece_data_size;
    // session_stats data
    int64_t download_rate;
    int64_t upload_rate;
    int64_t total_downloaded;
    int64_t total_uploaded;
    int32_t dht_nodes;
    int32_t peers_connected;
    int32_t half_open_connections;
    // other alert info
    const char* message;
    int category;                // libtorrent alert_category bits (for tracing)
} lt_alert_data_t;

typedef struct {
    lt_alert_data_t* alerts;
    int count;
} lt_alert_list_t;

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error);
lt_torrent_info_t lt_torrent_info_create_from_buffer(const uint8_t* data, size_t size, lt_error_t* error);
void lt_torrent_info_destroy(lt_torrent_info_t info);

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info);
void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata);

const char* lt_torrent_info_name(lt_torrent_info_t info);
uint64_t lt_torrent_info_total_size(lt_torrent_info_t info);
uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info);
uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info);
uint32_t lt_torrent_info_num_files(lt_torrent_info_t info);

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count);
void lt_files_free(lt_file_entry_t* files);

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out);
int lt_torrent_info_hash_for_piece(lt_torrent_info_t info, int piece_index, uint8_t* hash_out);

// TSI-2277: Check whether the torrent's info dict has the 'private' flag
// set (BEP-27). Returns 1 if private, 0 if not, -1 on error (null handle).
// Used for PT (Private Tracker) isolation: private torrents must not
// participate in cross-site tracker merging.
int lt_torrent_info_is_private(lt_torrent_info_t info);


lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error);
lt_session_t lt_session_create_with_custom_storage(const char* piece_cache_dir, const char* settings_json, lt_error_t* error);
void lt_session_destroy(lt_session_t session);
lt_torrent_handle_t lt_session_add_torrent(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error);
lt_torrent_handle_t lt_session_add_torrent_with_custom_storage(lt_session_t session, lt_torrent_info_t info, const char* piece_cache_dir, const char* settings_json, lt_error_t* error);
lt_torrent_handle_t lt_session_add_torrent_upload_mode(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error);
lt_torrent_handle_t lt_session_add_torrent_with_custom_storage_upload_mode(lt_session_t session, lt_torrent_info_t info, const char* piece_cache_dir, const char* settings_json, lt_error_t* error);
void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files);
void lt_torrent_handle_destroy(lt_torrent_handle_t handle);

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle);
int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total,
    int64_t* download_rate, int64_t* upload_rate, int64_t* total_download, int64_t* total_upload,
    int32_t* num_peers, int32_t* num_seeds);
int lt_torrent_handle_read_piece(lt_session_t session, lt_torrent_handle_t handle, int piece_index, uint8_t** data_out, size_t* size_out, lt_error_t* error);
void lt_piece_data_free(uint8_t* data);
int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset);
int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces);
int lt_torrent_handle_have_piece(lt_torrent_handle_t handle, int piece_index);
int lt_torrent_handle_set_piece_deadline(lt_torrent_handle_t handle, int piece_index, int deadline_ms);
int lt_torrent_handle_set_piece_priority(lt_torrent_handle_t handle, int piece_index, int priority);
int lt_torrent_handle_set_all_piece_priorities(lt_torrent_handle_t handle, int priority);
int lt_torrent_handle_set_flags(lt_torrent_handle_t handle, uint64_t flags);
int lt_torrent_handle_unset_flags(lt_torrent_handle_t handle, uint64_t flags);
int lt_torrent_handle_force_recheck(lt_torrent_handle_t handle);

void lt_session_apply_settings(lt_session_t session, const char* settings_json);

int lt_session_get_bool_setting(lt_session_t session, const char* key, int* out);
int lt_session_get_int_setting(lt_session_t session, const char* key, int* out);

// TSI-2547: report whether the libtorrent build compiled I2P support in
// (libtorrent/config.hpp `TORRENT_USE_I2P`). Rust config validation gates the
// `i2p_proxy` proxy_type value on this flag so I2P-disabled builds reject the
// value at config time instead of silently dropping it inside
// apply_str_setting.
int lt_torrent_i2p_enabled(void);

// TSI-2344: request a `session_stats_alert`. The alert is delivered through
// the normal alert queue and must be drained by the single alert-consumer
// thread (`lt_session_pop_alerts`), which fills the shared stats snapshot.
// This call is fire-and-forget and never blocks.
void lt_session_post_session_stats(lt_session_t session);
void lt_session_post_torrent_updates(lt_session_t session);
lt_alert_list_t* lt_session_pop_alerts(lt_session_t session);
void lt_alert_list_destroy(lt_alert_list_t* list);
// Set (or clear) the alert notify callback. `callback` is invoked on one of
// libtorrent's internal threads whenever the alert queue transitions from
// empty to non-empty. It MUST be non-blocking and MUST NOT call back into
// the session or pop alerts — it should only signal the waiting consumer.
// Pass `callback == NULL` to clear.
void lt_session_set_alert_notify(lt_session_t session, void (*callback)(void* user_data), void* user_data);

// TSI-2262: Per-info-hash shared read lock. Acquire before reading piece
// files from Rust (PieceStore::read_piece) to prevent reading partially-
// written data during active downloads. Must be paired with
// lt_unlock_piece_read. info_hash_hex is the hex-encoded SHA-1 info hash.
void lt_lock_piece_read(const char* info_hash_hex);
void lt_unlock_piece_read(const char* info_hash_hex);

// ============================================================================
// TSI-2276: Tracker manipulation FFI — extract / replace / re-announce
// ============================================================================

// Extract all trackers (announce + announce-list with tier) from a
// torrent_info as a JSON array string: [{"tier":0,"url":"http://..."},...]
// Returns 0 on success (writes a strdup'd string into *out_json), -1 on
// error (sets *error). Caller MUST free *out_json via lt_string_free.
int lt_torrent_info_trackers(lt_torrent_info_t info, char** out_json, lt_error_t* error);

// Replace all trackers on a torrent_handle with the given JSON array:
// [{"tier":0,"url":"http://..."},...]. Internally calls
// handle.replace_trackers(vector<announce_entry>). Returns 0 on success,
// -1 on error (sets *error).
int lt_torrent_handle_replace_trackers(lt_torrent_handle_t handle, const char* trackers_json, lt_error_t* error);

// Force an immediate tracker re-announce on the handle. Calls
// handle.force_reannounce(). Returns 0 on success, -1 on error.
int lt_torrent_handle_force_reannounce(lt_torrent_handle_t handle);

// TSI-2277: Extract the current tracker list from a torrent_handle (the
// live list, reflecting any prior replace_trackers calls). Same JSON format
// as lt_torrent_info_trackers. Used by the tracker merge logic to get the
// existing handle's trackers for dedup. Returns 0 on success, -1 on error.
int lt_torrent_handle_trackers(lt_torrent_handle_t handle, char** out_json, lt_error_t* error);

// Free a string returned by lt_torrent_info_trackers (or any wrapper
// function that returns a strdup'd char* via out-parameter).
void lt_string_free(char* str);

#ifdef __cplusplus
}
#endif

#endif

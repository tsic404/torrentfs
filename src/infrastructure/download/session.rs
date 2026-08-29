use std::ffi::{c_char, CStr, CString};
use std::path::Path;
use std::ptr;

use crate::config::TorrentfsConfig;
use crate::error::{error_from_c, TorrentError, TorrentResult};

use super::types::{FilePieceInfo, TorrentState, TorrentStatus};

pub struct Session {
    pub(crate) inner: libtorrent_sys::lt_session_t,
    /// Settings JSON for applying to the session (via apply_settings or baked
    /// into session_params during session creation).
    settings_json: CString,
}

pub struct TorrentHandle {
    pub(crate) inner: libtorrent_sys::lt_torrent_handle_t,
    pub(crate) info_hash: String,
    #[allow(dead_code)]
    pub(crate) session: libtorrent_sys::lt_session_t,
}

impl Session {
    pub fn new(config: &TorrentfsConfig) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // Create session with default settings (no listen_interface, no custom storage)
        let inner = unsafe { libtorrent_sys::lt_session_create(ptr::null(), &mut error) };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        // Save settings JSON for later application
        let settings_json = config.to_settings_json();
        let settings_json_c = CString::new(&settings_json[..]).unwrap_or_default();

        let session = Session {
            inner,
            settings_json: settings_json_c,
        };

        // Apply user configuration via JSON
        if settings_json != "{}" {
            unsafe {
                libtorrent_sys::lt_session_apply_settings(
                    session.inner,
                    session.settings_json.as_ptr(),
                );
            }
        }

        Ok(session)
    }

    /// Create a session with custom piece storage (PieceStorageDiskIO) from the start.
    /// Settings are baked into the session_params on the C++ side, so no post-hoc
    /// apply_settings is needed.
    pub fn new_with_custom_storage(
        config: &TorrentfsConfig,
        piece_cache_dir: &Path,
    ) -> TorrentResult<Self> {
        let settings_json = config.to_settings_json();
        let settings_json_c = CString::new(&settings_json[..]).unwrap_or_default();

        let piece_cache_dir_c = CString::new(piece_cache_dir.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Cache dir contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let inner = unsafe {
            libtorrent_sys::lt_session_create_with_custom_storage(
                piece_cache_dir_c.as_ptr(),
                if settings_json != "{}" {
                    settings_json_c.as_ptr()
                } else {
                    ptr::null()
                },
                &mut error,
            )
        };

        if inner.is_null() {
            return Err(unsafe { error_from_c(&error) });
        }

        Ok(Session {
            inner,
            settings_json: settings_json_c,
        })
    }

    /// Read a boolean setting from the live libtorrent session.
    pub fn get_bool_setting(&self, key: &str) -> TorrentResult<bool> {
        let key_c = CString::new(key).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Setting key contains null byte".to_string(),
        })?;
        let mut out: i32 = 0;
        let result = unsafe {
            libtorrent_sys::lt_session_get_bool_setting(self.inner, key_c.as_ptr(), &mut out)
        };
        if result == 0 {
            Ok(out != 0)
        } else {
            Err(TorrentError::Unknown {
                code: result,
                message: format!("Setting '{}' not found or session unavailable", key),
            })
        }
    }

    /// Read a string setting from the live libtorrent session.
    pub fn get_str_setting(&self, key: &str) -> TorrentResult<String> {
        let key_c = CString::new(key).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Setting key contains null byte".to_string(),
        })?;
        let mut out: *mut c_char = ptr::null_mut();
        let result = unsafe {
            libtorrent_sys::lt_session_get_str_setting(self.inner, key_c.as_ptr(), &mut out)
        };
        if result == 0 && !out.is_null() {
            let val = unsafe { CStr::from_ptr(out) }
                .to_string_lossy()
                .into_owned();
            unsafe { libtorrent_sys::lt_string_free(out) };
            Ok(val)
        } else {
            Err(TorrentError::Unknown {
                code: result,
                message: format!("Setting '{}' not found or session unavailable", key),
            })
        }
    }

    /// Read an integer setting from the live libtorrent session.
    pub fn get_int_setting(&self, key: &str) -> TorrentResult<i32> {
        let key_c = CString::new(key).map_err(|_| TorrentError::Unknown {
            code: -1,
            message: "Setting key contains null byte".to_string(),
        })?;
        let mut out: i32 = 0;
        let result = unsafe {
            libtorrent_sys::lt_session_get_int_setting(self.inner, key_c.as_ptr(), &mut out)
        };
        if result == 0 {
            Ok(out)
        } else {
            Err(TorrentError::Unknown {
                code: result,
                message: format!("Setting '{}' not found or session unavailable", key),
            })
        }
    }

    /// Re-apply saved settings JSON to the libtorrent session.
    /// No longer needed in normal flow: settings are now baked into session_params
    /// on the C++ side during session rebuild. Kept for testing / manual recovery.
    #[allow(dead_code)]
    fn reapply_settings(&self) {
        if self.settings_json.as_bytes() != b"{}" {
            unsafe {
                libtorrent_sys::lt_session_apply_settings(self.inner, self.settings_json.as_ptr());
            }
        }
    }

    pub fn add_torrent(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    /// Add a torrent in upload_mode: connects to trackers and peers
    /// but never requests pieces. Use for lightweight status-only handles.
    pub fn add_torrent_upload_mode(
        &mut self,
        info: &crate::TorrentInfo,
        save_path: &Path,
    ) -> TorrentResult<TorrentHandle> {
        let save_path_c = CString::new(save_path.to_string_lossy().into_owned())
            .map_err(|_| TorrentError::InvalidFile("Save path contains null byte".to_string()))?;

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        let handle = unsafe {
            libtorrent_sys::lt_session_add_torrent_upload_mode(
                self.inner,
                info.inner,
                save_path_c.as_ptr(),
                &mut error,
            )
        };

        if handle.is_null() {
            Err(unsafe { error_from_c(&error) })
        } else {
            let info_hash = hex::encode(info.info_hash()?);
            Ok(TorrentHandle {
                inner: handle,
                info_hash,
                session: self.inner,
            })
        }
    }

    /// Remove a torrent from the session.  The C++ wrapper (`lt_session_remove_torrent`)
    /// deletes the underlying `lt::torrent_handle*`, so we must prevent Rust's
    /// `TorrentHandle::Drop` from calling `lt_torrent_handle_destroy` on the
    /// same pointer — otherwise double-free (TSI-2232).
    pub fn remove_torrent(&mut self, handle: TorrentHandle, remove_files: bool) {
        unsafe {
            libtorrent_sys::lt_session_remove_torrent(
                self.inner,
                handle.inner,
                if remove_files { 1 } else { 0 },
            );
        }
        // The C++ side has freed the handle pointer; skip Rust's Drop.
        std::mem::forget(handle);
    }

    pub(crate) fn inner(&self) -> libtorrent_sys::lt_session_t {
        self.inner
    }

    /// TSI-2344: request a fresh session-stats sample. The resulting
    /// `session_stats_alert` is drained by the alert-consumer thread, which
    /// updates the shared stats snapshot. Fire-and-forget and never blocks.
    pub fn post_stats(&self) {
        unsafe { libtorrent_sys::lt_session_post_session_stats(self.inner) };
    }

    /// TSI-2468: fire-and-forget request for libtorrent to refresh per-torrent
    /// statistics (num_peers, num_seeds, etc.). Without this, `status()` may
    /// return stale peer counts — the internal peer list is only refreshed
    /// when the session processes a tick or `post_torrent_updates`.
    pub fn post_torrent_updates(&self) {
        unsafe { libtorrent_sys::lt_session_post_torrent_updates(self.inner) };
    }
}

impl Drop for Session {
    fn drop(&mut self) {
        unsafe {
            libtorrent_sys::lt_session_destroy(self.inner);
        }
    }
}

impl TorrentHandle {
    pub fn is_valid(&self) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_is_valid(self.inner) != 0 }
    }

    pub fn status(&self) -> TorrentResult<TorrentStatus> {
        let mut state: i32 = 0;
        let mut progress: f32 = 0.0;
        let mut total_done: u64 = 0;
        let mut total: u64 = 0;
        let mut download_rate: i64 = 0;
        let mut upload_rate: i64 = 0;
        let mut total_download: i64 = 0;
        let mut total_upload: i64 = 0;
        let mut num_peers: i32 = 0;
        let mut num_seeds: i32 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_status(
                self.inner,
                &mut state,
                &mut progress,
                &mut total_done,
                &mut total,
                &mut download_rate,
                &mut upload_rate,
                &mut total_download,
                &mut total_upload,
                &mut num_peers,
                &mut num_seeds,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent status".to_string(),
            })
        } else {
            Ok(TorrentStatus {
                state: TorrentState::from(state),
                progress,
                total_done,
                total,
                download_rate,
                upload_rate,
                total_download,
                total_upload,
                num_peers,
                num_seeds,
            })
        }
    }

    pub fn get_file_piece_info(&self, file_index: i32) -> TorrentResult<FilePieceInfo> {
        let mut first_piece: i64 = 0;
        let mut num_pieces: i64 = 0;
        let mut file_offset: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_piece_info(
                self.inner,
                file_index,
                &mut first_piece,
                &mut num_pieces,
                &mut file_offset,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file piece info".to_string(),
            })
        } else {
            Ok(FilePieceInfo {
                first_piece,
                num_pieces,
                file_offset,
            })
        }
    }

    pub fn get_torrent_info(&self) -> TorrentResult<(i64, i64)> {
        let mut piece_length: i64 = 0;
        let mut num_pieces: i64 = 0;

        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_get_torrent_info(
                self.inner,
                &mut piece_length,
                &mut num_pieces,
            )
        };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get torrent info from handle".to_string(),
            })
        } else {
            Ok((piece_length, num_pieces))
        }
    }

    pub fn have_piece(&self, piece_index: i32) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_have_piece(self.inner, piece_index) != 0 }
    }

    /// Set a piece deadline to prioritize downloading this piece.
    pub fn set_piece_deadline(&self, piece_index: i32, deadline_ms: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_deadline(
                self.inner,
                piece_index,
                deadline_ms,
            ) == 0
        }
    }

    /// Set piece priority for seeding/availability announcement.
    pub fn set_piece_priority(&self, piece_index: i32, priority: i32) -> bool {
        unsafe {
            libtorrent_sys::lt_torrent_handle_set_piece_priority(self.inner, piece_index, priority)
                == 0
        }
    }

    /// Set one or more `torrent_flags_t` bits on the underlying handle.
    ///
    /// Used to return a download handle to idle upload_mode after a read.
    pub fn set_flags(&self, flags: u64) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_set_flags(self.inner, flags) == 0 }
    }

    /// Clear one or more `torrent_flags_t` bits on the underlying handle.
    ///
    /// Used to switch a lightweight upload_mode handle into download mode by
    /// clearing `torrent_flags::upload_mode` (numeric value `1 << 1`).
    pub fn unset_flags(&self, flags: u64) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_unset_flags(self.inner, flags) == 0 }
    }

    /// Force libtorrent to re-verify all pieces.  After a `delete_piece` the
    /// on-disk piece file is gone, but libtorrent's internal piece bitmask
    /// may still mark it as complete (stale resume state).  `force_recheck`
    /// puts the torrent back into the checking queue so libtorrent re-reads
    /// the custom storage and discovers the missing piece, clearing its
    /// `have_piece` bit so the next read triggers a fresh download (TSI-2258).
    pub fn force_recheck(&self) -> bool {
        unsafe { libtorrent_sys::lt_torrent_handle_force_recheck(self.inner) == 0 }
    }

    /// Replace all trackers on the underlying handle with the given entries.
    /// Internally calls `handle.replace_trackers(vector<announce_entry>)`.
    /// After replacing, call `force_reannounce()` to immediately contact the
    /// new trackers.
    pub fn replace_trackers(&self, trackers: &[crate::TrackerEntry]) -> bool {
        // Serialize via serde_json — guarantees RFC 8259 compliance
        // (control char escaping, proper string handling).
        let json = match serde_json::to_string(trackers) {
            Ok(s) => s,
            Err(_) => return false,
        };
        let c_json = match CString::new(json) {
            Ok(s) => s,
            Err(_) => return false,
        };

        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // SAFETY: `self.inner` is a valid handle; `c_json` is a valid
        // NUL-terminated C string; `error` is stack-allocated.
        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_replace_trackers(
                self.inner,
                c_json.as_ptr(),
                &mut error,
            )
        };

        result == 0
    }

    /// Force an immediate tracker re-announce. After `replace_trackers`,
    /// this tells libtorrent to contact the new tracker set right away
    /// instead of waiting for the next scheduled announce interval.
    pub fn force_reannounce(&self) -> bool {
        // SAFETY: `self.inner` is a valid handle.
        unsafe { libtorrent_sys::lt_torrent_handle_force_reannounce(self.inner) == 0 }
    }

    /// Extract the current tracker list from the handle (TSI-2277).
    ///
    /// Returns the live tracker list reflecting any prior `replace_trackers`
    /// calls. Used by the tracker merge logic to get the existing handle's
    /// trackers for dedup before merging in a duplicate torrent's trackers.
    pub fn trackers(&self) -> TorrentResult<Vec<crate::TrackerEntry>> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        let mut json_ptr: *mut std::os::raw::c_char = ptr::null_mut();

        // SAFETY: `self.inner` is a valid handle; `json_ptr` and `error`
        // are stack-allocated and properly initialized.
        let result = unsafe {
            libtorrent_sys::lt_torrent_handle_trackers(self.inner, &mut json_ptr, &mut error)
        };

        if result != 0 {
            return Err(unsafe { error_from_c(&error) });
        }

        if json_ptr.is_null() {
            return Ok(Vec::new());
        }

        // SAFETY: `json_ptr` was populated by the successful FFI call and
        // points to a NUL-terminated C string. We copy it before freeing.
        let json_str = unsafe { CStr::from_ptr(json_ptr) }
            .to_string_lossy()
            .into_owned();

        // SAFETY: `json_ptr` was allocated by `strdup` in the C++ wrapper;
        // `lt_string_free` calls `free` exactly once.
        unsafe { libtorrent_sys::lt_string_free(json_ptr) };

        serde_json::from_str::<Vec<crate::TrackerEntry>>(&json_str).map_err(|e| {
            TorrentError::ParseError(format!("Failed to parse handle trackers JSON: {e}"))
        })
    }

    pub fn info_hash(&self) -> &str {
        &self.info_hash
    }
}

impl Drop for TorrentHandle {
    fn drop(&mut self) {
        if !self.inner.is_null() {
            unsafe {
                libtorrent_sys::lt_torrent_handle_destroy(self.inner);
            }
        }
    }
}

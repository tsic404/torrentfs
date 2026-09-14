//! Torrent metadata parsing module.
//! Provides TorrentInfo for parsing .torrent files and extracting metadata.

use std::ffi::{CStr, CString};
use std::path::Path;
use std::ptr;

use crate::error::{error_from_c, TorrentError, TorrentResult};

pub struct TorrentInfo {
    pub(crate) inner: libtorrent_sys::lt_torrent_info_t,
    /// Owned buffer backing the C++ torrent_info. The C++ side (bdecode /
    /// torrent_info(bdecode_node)) only references the buffer without copying
    /// it; dropping this Vec would leave the C++ object with a dangling pointer,
    /// causing stack smashing on libtorrent 2.0.x. This field MUST be dropped
    /// AFTER `inner`.
    #[allow(dead_code)]
    _data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub path: String,
    pub size: u64,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct TorrentMetadata {
    pub name: String,
    pub total_size: u64,
    pub piece_length: u32,
    pub num_pieces: u32,
    pub num_files: u32,
    pub files: Vec<FileInfo>,
    pub info_hash: [u8; 20],
}

/// A single tracker entry extracted from a torrent file.
///
/// `tier` follows the BitTorrent BEP-12 convention: lower tier numbers
/// are contacted first. The bare `announce` key maps to tier 0.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct TrackerEntry {
    pub tier: i32,
    pub url: String,
}

impl TorrentInfo {
    #[allow(dead_code)]
    /// Parse a `.torrent` file from the filesystem. On Unix, paths are
    /// arbitrary bytes (not necessarily UTF-8): `Path::to_str()` returns
    /// `None` for non-UTF-8 paths, which failed torrents with non-ASCII names
    /// (e.g. GBK filenames). Use `OsStrExt::as_bytes()` (Unix) to build a
    /// `CString` directly — libtorrent's `torrent_info(const std::string&)`
    /// accepts arbitrary bytes, so no encoding conversion is needed.
    pub fn from_file<P: AsRef<Path>>(path: P) -> TorrentResult<Self> {
        #[cfg(unix)]
        {
            use std::os::unix::ffi::OsStrExt;
            let path_bytes = path.as_ref().as_os_str().as_bytes();
            let c_path = CString::new(path_bytes)
                .map_err(|_| TorrentError::InvalidFile("Path contains null byte".to_string()))?;
            Self::from_file_cstr(&c_path)
        }
        #[cfg(not(unix))]
        {
            let path_str = path.as_ref().to_str().ok_or_else(|| {
                TorrentError::InvalidFile("Path contains invalid UTF-8".to_string())
            })?;
            let c_path = CString::new(path_str)
                .map_err(|_| TorrentError::InvalidFile("Path contains null byte".to_string()))?;
            Self::from_file_cstr(&c_path)
        }
    }

    /// Parse a `.torrent` file from a NUL-terminated C string path.
    fn from_file_cstr(c_path: &CString) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        // SAFETY: `c_path` is a valid, NUL-terminated C string;
        // `error` is stack-allocated and properly initialized. The FFI
        // function is safe as long as these preconditions hold.
        let inner = unsafe { libtorrent_sys::lt_torrent_info_create(c_path.as_ptr(), &mut error) };
        if inner.is_null() {
            // SAFETY: `error` is a live, initialized stack variable; no
            // aliasing issues since it's passed by shared reference.
            Err(unsafe { error_from_c(&error) })
        } else {
            Ok(TorrentInfo {
                inner,
                _data: Vec::new(),
            })
        }
    }

    pub fn from_bytes(data: Vec<u8>) -> TorrentResult<Self> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };

        // SAFETY: `data.as_ptr()` points to `data.len()` valid bytes.
        // `error` is stack-allocated and properly initialized. The FFI
        // call reads from the buffer and writes to `error` on failure.
        let inner = unsafe {
            libtorrent_sys::lt_torrent_info_create_from_buffer(
                data.as_ptr(),
                data.len(),
                &mut error,
            )
        };

        if inner.is_null() {
            // SAFETY: `error` is a live, initialized stack variable.
            Err(unsafe { error_from_c(&error) })
        } else {
            // Keep `data` alive: the C++ bdecode / torrent_info only
            // references the buffer, so the Vec must outlive `inner`.
            Ok(TorrentInfo { inner, _data: data })
        }
    }

    pub fn name(&self) -> String {
        // SAFETY: `self.inner` is a valid, non-null `lt_torrent_info_t`.
        // `lt_torrent_info_name` returns a pointer to an internal C string.
        unsafe {
            let name_ptr = libtorrent_sys::lt_torrent_info_name(self.inner);
            if name_ptr.is_null() {
                String::new()
            } else {
                // SAFETY: `name_ptr` was checked for non-null; it points to
                // a NUL-terminated C string owned by libtorrent. We copy via
                // `to_string_lossy().into_owned()` so it's safe after return.
                CStr::from_ptr(name_ptr).to_string_lossy().into_owned()
            }
        }
    }

    pub fn total_size(&self) -> u64 {
        // SAFETY: `self.inner` is a valid handle; the FFI call is a pure
        // getter with no side effects.
        unsafe { libtorrent_sys::lt_torrent_info_total_size(self.inner) }
    }

    pub fn piece_length(&self) -> u32 {
        // SAFETY: `self.inner` is a valid handle; the FFI call is a pure
        // getter with no side effects.
        unsafe { libtorrent_sys::lt_torrent_info_piece_length(self.inner) }
    }

    pub fn num_pieces(&self) -> u32 {
        // SAFETY: `self.inner` is a valid handle; the FFI call is a pure
        // getter with no side effects.
        unsafe { libtorrent_sys::lt_torrent_info_num_pieces(self.inner) }
    }

    pub fn num_files(&self) -> u32 {
        // SAFETY: `self.inner` is a valid handle; the FFI call is a pure
        // getter with no side effects.
        unsafe { libtorrent_sys::lt_torrent_info_num_files(self.inner) }
    }

    pub fn files(&self) -> TorrentResult<Vec<FileInfo>> {
        let mut files_ptr: *mut libtorrent_sys::lt_file_entry_t = ptr::null_mut();
        let mut count: u32 = 0;

        // SAFETY: `self.inner` is a valid handle. `files_ptr` and `count`
        // are stack-allocated and will be populated by the FFI call.
        let result = unsafe {
            libtorrent_sys::lt_torrent_info_get_files(self.inner, &mut files_ptr, &mut count)
        };

        if result != 0 {
            return Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get file list".to_string(),
            });
        }

        let files = if count == 0 || files_ptr.is_null() {
            Vec::new()
        } else {
            // SAFETY: `files_ptr` was populated by the successful FFI call
            // above and points to `count` valid `lt_file_entry_t` values.
            let slice = unsafe { std::slice::from_raw_parts(files_ptr, count as usize) };
            let result: Vec<FileInfo> = slice
                .iter()
                .map(|entry| FileInfo {
                    path: if entry.path.is_null() {
                        String::new()
                    } else {
                        // SAFETY: `entry.path` is checked for non-null; it
                        // points to a NUL-terminated C string owned by libtorrent.
                        unsafe { CStr::from_ptr(entry.path) }
                            .to_string_lossy()
                            .into_owned()
                    },
                    size: entry.size,
                })
                .collect();

            // SAFETY: `files_ptr` was allocated by libtorrent and must be
            // freed exactly once via `lt_files_free`.
            unsafe { libtorrent_sys::lt_files_free(files_ptr) };

            result
        };

        Ok(files)
    }

    pub fn info_hash(&self) -> TorrentResult<[u8; 20]> {
        let mut hash = [0u8; 20];
        // SAFETY: `self.inner` is a valid handle; `hash` is a stack-allocated
        // 20-byte buffer that the FFI call writes into.
        let result =
            unsafe { libtorrent_sys::lt_torrent_info_get_info_hash(self.inner, hash.as_mut_ptr()) };

        if result != 0 {
            Err(TorrentError::Unknown {
                code: result,
                message: "Failed to get info hash".to_string(),
            })
        } else {
            Ok(hash)
        }
    }

    /// The expected SHA-1 hash of the piece at `piece_index`.
    /// Returns `None` when the index is out of range or the torrent has no
    /// SHA-1 piece hashes (e.g. v2-only torrents).
    pub fn hash_for_piece(&self, piece_index: i32) -> Option<[u8; 20]> {
        if piece_index < 0 {
            return None;
        }
        let mut hash = [0u8; 20];
        // SAFETY: `self.inner` is a valid handle; `hash` is a stack-allocated
        // 20-byte buffer that the FFI call writes into on success.
        let result = unsafe {
            libtorrent_sys::lt_torrent_info_hash_for_piece(
                self.inner,
                piece_index,
                hash.as_mut_ptr(),
            )
        };
        if result != 0 {
            None
        } else {
            Some(hash)
        }
    }

    /// Whether the info dict sets `private=1` (BEP-27). torrentfs isolates
    /// private torrents: they never join cross-site tracker merging, which
    /// would expose passkeys and cross-pollinate peers. On FFI error (-1:
    /// null handle/exception) returns `true` — treat as private — so
    /// `merge_trackers` skips the merge rather than risk passkey leakage on an
    /// uncertain flag.
    pub fn is_private(&self) -> bool {
        // SAFETY: `self.inner` is a valid handle; the FFI call is a pure
        // getter with no side effects. Returns 1 if private, 0 if not,
        // -1 on error (treated as private — conservative skip-merge).
        let result = unsafe { libtorrent_sys::lt_torrent_info_is_private(self.inner) };
        result != 0
    }

    /// The on-disk byte length of the piece at `piece_index`: `piece_length`
    /// for every piece except the last, which is the trailing remainder of
    /// `total_size`. Returns `None` when the index is out of range.
    ///
    /// Used by the background cache verification to
    /// distinguish an incomplete/crash-interrupted piece (wrong size → leave
    /// it unverified for on-demand re-download) from a complete piece whose
    /// SHA-1 must still be checked.
    pub fn piece_size(&self, piece_index: i32) -> Option<u64> {
        let num_pieces = self.num_pieces() as i32;
        if piece_index < 0 || piece_index >= num_pieces {
            return None;
        }
        let piece_length = self.piece_length() as u64;
        if piece_index == num_pieces - 1 {
            let remainder = self
                .total_size()
                .saturating_sub((num_pieces - 1) as u64 * piece_length);
            Some(if remainder > 0 {
                remainder
            } else {
                piece_length
            })
        } else {
            Some(piece_length)
        }
    }

    pub fn metadata(&self) -> TorrentResult<TorrentMetadata> {
        Ok(TorrentMetadata {
            name: self.name(),
            total_size: self.total_size(),
            piece_length: self.piece_length(),
            num_pieces: self.num_pieces(),
            num_files: self.num_files(),
            files: self.files()?,
            info_hash: self.info_hash()?,
        })
    }

    /// Extract all trackers (announce + announce-list with tier) from the
    /// torrent file. Returns entries in the order libtorrent stores them
    /// internally (libtorrent does not guarantee tier ordering, callers
    /// should not rely on a specific sort order).
    pub fn trackers(&self) -> TorrentResult<Vec<TrackerEntry>> {
        let mut error = libtorrent_sys::lt_error_t {
            message: ptr::null(),
            code: 0,
        };
        let mut json_ptr: *mut std::os::raw::c_char = ptr::null_mut();

        // SAFETY: `self.inner` is a valid handle; `json_ptr` and `error`
        // are stack-allocated and properly initialized.
        let result = unsafe {
            libtorrent_sys::lt_torrent_info_trackers(self.inner, &mut json_ptr, &mut error)
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

        serde_json::from_str::<Vec<TrackerEntry>>(&json_str)
            .map_err(|e| TorrentError::ParseError(format!("Failed to parse trackers JSON: {e}")))
    }
}

impl Drop for TorrentInfo {
    fn drop(&mut self) {
        // SAFETY: `self.inner` is a valid handle that was successfully
        // created by a prior FFI call. It is dropped exactly once per
        // `TorrentInfo` instance.
        unsafe {
            libtorrent_sys::lt_torrent_info_destroy(self.inner);
        }
    }
}

// SAFETY: `TorrentInfo` owns an opaque C handle (`lt_torrent_info_t`).
// libtorrent's API is not documented as thread-safe, but after creation
// the handle is only read, never mutated. `Send` + `Sync` are needed for
// FUSE's multi-threaded event loop. All reads through the handle are via
// const-qualified FFI calls internally.
unsafe impl Send for TorrentInfo {}
unsafe impl Sync for TorrentInfo {}

/// Shared test helper — build a multi-file bencoded torrent
/// with arbitrary byte-string file names. Used by both `metadata::tests`
/// and `fs_service::tests` to avoid duplication.
#[cfg(test)]
#[doc(hidden)]
pub(crate) fn build_multifile_torrent(
    name_bytes: &[u8],
    files: &[(Vec<u8>, usize)],
    piece_length: usize,
) -> Vec<u8> {
    let total: usize = files.iter().map(|(_, s)| s).sum();
    let content: Vec<u8> = (0..total).map(|i| (i % 251) as u8).collect();
    let mut pieces = Vec::new();
    for chunk in content.chunks(piece_length) {
        use sha1_smol::Sha1;
        pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
    }
    let mut t = Vec::new();
    t.push(b'd');
    t.extend_from_slice(b"4:infod");
    t.extend_from_slice(b"5:filesl");
    for (path, size) in files {
        t.push(b'd');
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(size.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:pathl");
        t.extend_from_slice(path.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(path);
        t.push(b'e');
        t.push(b'e');
    }
    t.push(b'e');
    t.extend_from_slice(b"4:name");
    t.extend_from_slice(name_bytes.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(name_bytes);
    t.extend_from_slice(b"12:piece lengthi");
    t.extend_from_slice(piece_length.to_string().as_bytes());
    t.push(b'e');
    t.extend_from_slice(b"6:pieces");
    t.extend_from_slice(pieces.len().to_string().as_bytes());
    t.push(b':');
    t.extend_from_slice(&pieces);
    t.extend_from_slice(b"ee");
    t
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[cfg(unix)]
    use std::os::unix::ffi::OsStrExt;

    fn create_test_torrent() -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        write!(file, "this is not a valid torrent file").unwrap();
        file
    }

    #[test]
    fn test_invalid_torrent_returns_error() {
        let tmp = create_test_torrent();
        let result = TorrentInfo::from_file(tmp.path());
        assert!(result.is_err());
    }

    /// Build a minimal single-file bencoded torrent with `total` bytes of
    /// content and `piece_length`-byte pieces, returning the raw torrent bytes.
    fn build_test_torrent(total: usize, piece_length: usize) -> Vec<u8> {
        let content = (0..total).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let mut pieces = Vec::new();
        for chunk in content.chunks(piece_length) {
            use sha1_smol::Sha1;
            pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
        }
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(total.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces");
        t.extend_from_slice(pieces.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(&pieces);
        t.extend_from_slice(b"ee");
        t
    }

    #[test]
    fn test_piece_size_matches_piece_length_and_last_remainder() {
        // 40 bytes total, 16-byte pieces -> pieces 0..2 with sizes 16, 16, 8.
        let torrent = build_test_torrent(40, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse valid torrent");

        assert_eq!(info.piece_size(0), Some(16));
        assert_eq!(info.piece_size(1), Some(16));
        assert_eq!(info.piece_size(2), Some(8));
        assert_eq!(info.piece_size(3), None);
        assert_eq!(info.piece_size(-1), None);
    }

    #[test]
    fn test_piece_size_exact_multiple_uses_full_piece_length() {
        // 32 bytes total, 16-byte pieces -> pieces 0..2 with sizes 16, 16.
        let torrent = build_test_torrent(32, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse valid torrent");

        assert_eq!(info.piece_size(0), Some(16));
        assert_eq!(info.piece_size(1), Some(16));
        assert_eq!(info.piece_size(2), None);
    }

    #[test]
    fn test_nonexistent_file_returns_error() {
        let result = TorrentInfo::from_file("/nonexistent/path.torrent");
        assert!(result.is_err());
    }

    /// Build a torrent with a single `announce` key (no announce-list).
    /// The announce URL is at the top level of the bencoded dict.
    fn build_torrent_with_single_tracker(
        announce_url: &str,
        total: usize,
        piece_length: usize,
    ) -> Vec<u8> {
        let content = (0..total).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let mut pieces = Vec::new();
        for chunk in content.chunks(piece_length) {
            use sha1_smol::Sha1;
            pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
        }
        let mut t = Vec::new();
        t.push(b'd');
        // announce key
        t.extend_from_slice(b"8:announce");
        t.extend_from_slice(announce_url.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(announce_url.as_bytes());
        // info dict
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(total.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces");
        t.extend_from_slice(pieces.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(&pieces);
        t.extend_from_slice(b"ee");
        t
    }

    /// Build a torrent with an `announce-list` (BEP-12 multi-tier).
    /// Each inner Vec is a tier; the tier index is the position in the
    /// outer Vec.
    fn build_torrent_with_announce_list(
        tiers: &[Vec<&str>],
        total: usize,
        piece_length: usize,
    ) -> Vec<u8> {
        let content = (0..total).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let mut pieces = Vec::new();
        for chunk in content.chunks(piece_length) {
            use sha1_smol::Sha1;
            pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
        }
        let mut t = Vec::new();
        t.push(b'd');
        // announce-list key
        t.extend_from_slice(b"13:announce-listl");
        for tier in tiers {
            t.push(b'l');
            for url in tier {
                t.extend_from_slice(url.len().to_string().as_bytes());
                t.push(b':');
                t.extend_from_slice(url.as_bytes());
            }
            t.push(b'e'); // close tier list
        }
        t.push(b'e'); // close announce-list
                      // info dict
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(total.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces");
        t.extend_from_slice(pieces.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(&pieces);
        t.extend_from_slice(b"ee");
        t
    }

    #[test]
    fn test_trackers_single_tracker() {
        let torrent =
            build_torrent_with_single_tracker("http://tracker.example.com/announce", 32, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse valid torrent");
        let trackers = info.trackers().expect("extract trackers");

        assert_eq!(trackers.len(), 1);
        assert_eq!(trackers[0].tier, 0);
        assert_eq!(trackers[0].url, "http://tracker.example.com/announce");
    }

    #[test]
    fn test_trackers_multi_tier() {
        let torrent = build_torrent_with_announce_list(
            &[
                vec!["http://tracker1.example.com/announce"],
                vec![
                    "udp://tracker2.example.com:6969/announce",
                    "http://tracker3.example.com/announce",
                ],
            ],
            32,
            16,
        );
        let info = TorrentInfo::from_bytes(torrent).expect("parse valid torrent");
        let trackers = info.trackers().expect("extract trackers");

        assert_eq!(trackers.len(), 3);
        // Tier 0
        assert_eq!(trackers[0].tier, 0);
        assert_eq!(trackers[0].url, "http://tracker1.example.com/announce");
        // Tier 1
        assert_eq!(trackers[1].tier, 1);
        assert_eq!(trackers[1].url, "udp://tracker2.example.com:6969/announce");
        assert_eq!(trackers[2].tier, 1);
        assert_eq!(trackers[2].url, "http://tracker3.example.com/announce");
    }

    #[test]
    fn test_trackers_no_tracker() {
        // build_test_torrent produces a torrent with no announce or announce-list.
        let torrent = build_test_torrent(32, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse valid torrent");
        let trackers = info.trackers().expect("extract trackers");

        assert!(trackers.is_empty());
    }

    #[test]
    fn test_tracker_entry_serde_roundtrip() {
        let entries = vec![
            TrackerEntry {
                tier: 0,
                url: "http://tracker.example.com/announce".to_string(),
            },
            TrackerEntry {
                tier: 1,
                url: "udp://tracker2.example.com:6969/announce".to_string(),
            },
        ];
        let json = serde_json::to_string(&entries).expect("serialize");
        let parsed: Vec<TrackerEntry> = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(entries, parsed);
    }

    #[test]
    fn test_tracker_entry_serde_escapes_control_chars() {
        let entry = TrackerEntry {
            tier: 0,
            url: "http://example.com/\t\r\n".to_string(),
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        // Control chars must be escaped, not literal.
        assert!(!json.contains('\t'));
        assert!(!json.contains('\r'));
        assert!(!json.contains('\n'));
        assert!(json.contains("\\t"));
        assert!(json.contains("\\r"));
        assert!(json.contains("\\n"));
    }

    // ── private flag tests ──────────────────────────────────

    /// Build a torrent with the `private` flag set inside the info dict.
    /// `private=1` marks the torrent as a PT (Private Tracker) torrent.
    fn build_private_torrent(total: usize, piece_length: usize) -> Vec<u8> {
        let content = (0..total).map(|i| (i % 251) as u8).collect::<Vec<u8>>();
        let mut pieces = Vec::new();
        for chunk in content.chunks(piece_length) {
            use sha1_smol::Sha1;
            pieces.extend_from_slice(&Sha1::from(chunk).digest().bytes());
        }
        let mut t = Vec::new();
        t.push(b'd');
        t.extend_from_slice(b"8:announce");
        t.extend_from_slice(b"43:http://tracker.private.example.com/announce");
        // info dict with private flag
        t.extend_from_slice(b"4:infod");
        t.extend_from_slice(b"6:lengthi");
        t.extend_from_slice(total.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"4:name4:test");
        t.extend_from_slice(b"12:piece lengthi");
        t.extend_from_slice(piece_length.to_string().as_bytes());
        t.push(b'e');
        t.extend_from_slice(b"6:pieces");
        t.extend_from_slice(pieces.len().to_string().as_bytes());
        t.push(b':');
        t.extend_from_slice(&pieces);
        // private=1 — must come before the closing 'e' of the info dict
        t.extend_from_slice(b"7:privatei1e");
        t.extend_from_slice(b"ee");
        t
    }

    #[test]
    fn test_is_private_true_for_private_torrent() {
        let torrent = build_private_torrent(32, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse private torrent");
        assert!(
            info.is_private(),
            "PT torrent with private=1 should return true"
        );
    }

    #[test]
    fn test_is_private_false_for_public_torrent() {
        // build_test_torrent has no private field in the info dict.
        let torrent = build_test_torrent(32, 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse public torrent");
        assert!(
            !info.is_private(),
            "Public torrent without private flag should return false"
        );
    }

    #[test]
    fn test_is_private_false_for_torrent_with_trackers() {
        // A torrent with announce-list but no private flag.
        let torrent = build_torrent_with_announce_list(
            &[vec!["http://tracker.example.com/announce"]],
            32,
            16,
        );
        let info = TorrentInfo::from_bytes(torrent).expect("parse torrent");
        assert!(
            !info.is_private(),
            "Public torrent with trackers but no private flag should return false"
        );
    }

    #[test]
    fn test_multifile_utf8_nonascii_names() {
        // Multi-file torrent with UTF-8 Chinese file names.
        let name = "测试种子".as_bytes(); // 4 CJK chars, 12 bytes
        let file1 = "你好.txt".as_bytes(); // 2 CJK + .txt
        let file2 = "世界.txt".as_bytes();
        let torrent =
            build_multifile_torrent(name, &[(file1.to_vec(), 16), (file2.to_vec(), 16)], 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse torrent");

        // name() should preserve valid UTF-8.
        assert_eq!(info.name(), "测试种子");

        let files = info.files().expect("get files");
        assert_eq!(files.len(), 2);
        // file_path() prepends the torrent name for multi-file torrents.
        assert_eq!(files[0].path, "测试种子/你好.txt");
        assert_eq!(files[1].path, "测试种子/世界.txt");
        assert_eq!(files[0].size, 16);
        assert_eq!(files[1].size, 16);
    }

    #[test]
    fn test_multifile_non_utf8_gbk_names() {
        // Multi-file torrent with GBK-encoded file names (not valid UTF-8).
        // libtorrent sanitizes non-UTF-8 bytes to '_' — the names are
        // mangled but must be internally consistent so that DB lookups
        // match readdir/lookup round-trips.
        let name: &[u8] = b"\xb2\xe2\xca\xd4\xd6\xd6\xd7\xd3"; // 测试种子 in GBK
        let file1: &[u8] = b"\xc4\xe3\xba\xc3.txt"; // 你好.txt in GBK
        let file2: &[u8] = b"\xca\xc0\xbd\xe7.txt"; // 世界.txt in GBK
        let torrent =
            build_multifile_torrent(name, &[(file1.to_vec(), 16), (file2.to_vec(), 16)], 16);
        let info = TorrentInfo::from_bytes(torrent).expect("parse torrent");

        let files = info.files().expect("get files");
        assert_eq!(files.len(), 2);
        // The sanitized names must be valid UTF-8 (so they can be stored
        // in SQLite TEXT columns and round-tripped through FUSE OsStr).
        assert!(
            std::str::from_utf8(files[0].path.as_bytes()).is_ok(),
            "sanitized file path must be valid UTF-8"
        );
        assert!(
            std::str::from_utf8(files[1].path.as_bytes()).is_ok(),
            "sanitized file path must be valid UTF-8"
        );
        // The two files must have different names after sanitization
        // (libtorrent appends a numeric suffix to avoid collisions).
        let name0 = files[0].path.split('/').last().unwrap();
        let name1 = files[1].path.split('/').last().unwrap();
        assert_ne!(name0, name1, "sanitized file names must be unique");
    }

    #[test]
    #[cfg(unix)]
    fn test_from_file_non_utf8_path() {
        // from_file must not fail on non-UTF-8 file paths.
        // On Unix, file paths are arbitrary bytes.  A path with non-UTF-8
        // bytes should be passed through to libtorrent as raw bytes, not
        // rejected with "Path contains invalid UTF-8".
        //
        // We create a valid torrent file at a non-UTF-8 path and verify
        // that from_file can open it.
        use std::os::unix::ffi::OsStringExt;

        let dir = tempfile::TempDir::new().unwrap();
        // Build a path containing a non-UTF-8 byte (0xff is invalid UTF-8).
        let mut path_bytes = dir.path().as_os_str().as_bytes().to_vec();
        path_bytes.push(std::path::MAIN_SEPARATOR as u8);
        path_bytes.extend_from_slice(b"\xff");
        path_bytes.extend_from_slice(b".torrent");
        let non_utf8_path = std::ffi::OsString::from_vec(path_bytes);
        let path = std::path::PathBuf::from(non_utf8_path);

        // Write a valid single-file torrent to this path.
        let torrent = build_test_torrent(32, 16);
        std::fs::write(&path, &torrent).expect("write torrent to non-UTF-8 path");

        // from_file should succeed — the old implementation would fail
        // with "Path contains invalid UTF-8".
        let result = TorrentInfo::from_file(&path);
        assert!(
            result.is_ok(),
            "from_file should handle non-UTF-8 paths: {:?}",
            result.err()
        );

        let info = result.unwrap();
        assert_eq!(info.name(), "test");
        assert_eq!(info.total_size(), 32);
    }
}

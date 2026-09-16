//! `PieceScheduler` — the piece control plane.
//!
//! Owns the per-torrent piece priority state and recomputes it from *events*:
//! `ReaderAdded` / `ReaderReleased` (reference-counted readers), `PieceReady`
//! (a piece finished downloading) and `PieceEvicted` (a cached piece was
//! evicted).  The download engine actor thread is the **single writer** of
//! this state, so it holds no lock and needs no `Send`.
//!
//! It deliberately holds no [`CacheManager`]; cached-piece presence is queried
//! through [`super::piece_store::PieceStore`] (read-only) when a gradient is
//! applied.

use std::collections::HashMap;

use crate::error::TorrentResult;
use crate::infrastructure::config::PiecePriorityToml;
use crate::infrastructure::download::TorrentHandle;
use crate::infrastructure::metadata::TorrentInfo;

use super::piece_store::PieceStore;
use super::types::FilePieceInfo;

/// Idle baseline piece priority: 0 = "not wanted".  Pieces outside every
/// active reader's access window stay at 0, so libtorrent never requests them
/// on its own.  The torrent is held in `upload_mode` while idle, so it still
/// connects and seeds without requesting any piece.
const DEFAULT_PRIORITY: i32 = 0;

/// Priority gradient configuration for selective piece download.
#[derive(Debug, Clone)]
pub struct PiecePriorityConfig {
    /// Access window in MiB beyond (and behind) the current read range
    /// (default 4096 = 4 GB).  Pieces within the window are "wanted" with a
    /// descending gradient; pieces outside it stay at 0 (not wanted).
    pub access_window_mb: u32,
    /// Priority for pieces inside the current read range (default 7).
    pub current_priority: i32,
    /// Step priorities for pieces at distance 1..=4 from the current range end.
    pub step_priorities: [i32; 4],
    /// Priority at the far forward edge of the access window (default 1).
    /// Beyond this the piece falls to [`Self::rest_priority`].
    pub window_edge_priority: i32,
    /// Priority for pieces beyond the access window (default 0 = not wanted).
    pub rest_priority: i32,
    /// Priority for pieces before the current read offset but still within
    /// the access window (default 1).  Pieces further back are 0.
    pub backward_priority: i32,
}

impl Default for PiecePriorityConfig {
    fn default() -> Self {
        Self {
            access_window_mb: 4096,
            current_priority: 7,
            step_priorities: [6, 5, 4, 3],
            window_edge_priority: 1,
            rest_priority: 0,
            backward_priority: 1,
        }
    }
}

impl PiecePriorityConfig {
    /// Build the runtime config from optional TOML overrides, falling back to
    /// [`Self::default`] for any field the user did not specify.
    pub fn from_toml(toml: &PiecePriorityToml) -> Self {
        let d = Self::default();
        Self {
            access_window_mb: toml.access_window_mb.unwrap_or(d.access_window_mb),
            current_priority: toml.current_priority.unwrap_or(d.current_priority),
            step_priorities: toml.step_priorities.unwrap_or(d.step_priorities),
            window_edge_priority: toml
                .window_edge_priority
                .unwrap_or(d.window_edge_priority),
            rest_priority: toml.rest_priority.unwrap_or(d.rest_priority),
            backward_priority: toml.backward_priority.unwrap_or(d.backward_priority),
        }
    }
}

/// Status of a single piece for `.stats` rendering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PieceStatus {
    /// Current piece priority (0 = not wanted).
    pub priority: i32,
    /// Whether the piece is present in the disk cache (downloaded).
    pub is_cached: bool,
    /// Number of times the cached piece has been accessed.
    pub hit_count: u64,
}

/// An active reader: its precomputed priority gradient.  Reference-counted by
/// [`PieceScheduler::readers`].
#[derive(Debug, Clone)]
struct ReadRange {
    gradient: Vec<i32>,
}
/// Manages piece priority lifecycle across all active torrents.
pub struct PieceScheduler {
    /// Per-info_hash priority vector: `elevated[info_hash][piece_idx]` = priority.
    elevated: HashMap<String, Vec<i32>>,
    /// Per-info_hash piece length (bytes) for `.stats` rendering without the handle.
    piece_lengths: HashMap<String, i64>,
    /// Priority configuration.
    config: PiecePriorityConfig,
    /// Active readers per info_hash (reference counting).  The engine thread
    /// is single-threaded and `read_file_range` blocks it, so at most one
    /// reader per torrent is ever active; the LIFO `pop` in `reader_released`
    /// therefore always removes the reader that just finished.
    readers: HashMap<String, Vec<ReadRange>>,
    /// Per-info_hash retained prefetch gradient: the last reader's gradient,
    /// kept after the final reader releases so the read-ahead window keeps
    /// downloading and stays visible in `.stats` as `[N]` markers.
    prefetch: HashMap<String, Vec<i32>>,
}

impl PieceScheduler {
    pub fn new(config: PiecePriorityConfig) -> Self {
        Self {
            elevated: HashMap::new(),
            piece_lengths: HashMap::new(),
            config,
            readers: HashMap::new(),
            prefetch: HashMap::new(),
        }
    }

    /// Initialize a torrent: records an all-zero priority vector and its
    /// piece length.
    pub fn init_torrent(
        &mut self,
        info_hash: &str,
        num_pieces: i32,
        piece_length: i64,
    ) -> TorrentResult<()> {
        if num_pieces <= 0 {
            return Ok(());
        }
        self.elevated.insert(
            info_hash.to_string(),
            vec![DEFAULT_PRIORITY; num_pieces as usize],
        );
        self.piece_lengths.insert(info_hash.to_string(), piece_length);
        self.prefetch.remove(info_hash);
        Ok(())
    }

    // ── Priority events ───────────────────────────────────────────────

    /// `ReaderAdded` event: a reader started.  Records the read range and
    /// recomputes the priority gradient (union over all active readers).
    pub fn reader_added(
        &mut self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        let info_hash = hex::encode(info.info_hash()?);
        let gradient = match self.gradient_for(handle, info, file_index, offset, size) {
            Some(g) => g,
            None => return Ok(()),
        };
        self.readers
            .entry(info_hash.clone())
            .or_default()
            .push(ReadRange { gradient });
        self.recompute(handle, &info_hash, store);
        Ok(())
    }

    /// `ReaderReleased` event: a reader finished.  Decrements the reference
    /// count and recomputes.  When the last reader releases, its gradient is
    /// retained as the prefetch window so the read-ahead download continues.
    ///
    /// Infallible: the engine is single-threaded (at most one reader per
    /// torrent), so the LIFO `pop` removes the reader that just finished, and
    /// `recompute` only mutates in-memory state and best-effort FFI priorities.
    pub fn reader_released(&mut self, handle: &TorrentHandle, info_hash: &str, store: &PieceStore) {
        let (empty, last_gradient) = if let Some(ranges) = self.readers.get_mut(info_hash) {
            let popped = ranges.pop();
            (ranges.is_empty(), popped.map(|r| r.gradient))
        } else {
            (true, None)
        };
        if empty {
            // Retain the most recent reader's gradient as the prefetch window:
            // pieces beyond the read range keep their descending priorities so
            // libtorrent continues the read-ahead prefetch and `.stats` shows
            // `[6][5][4][3]` for the queued pieces instead of resetting to `[]`.
            if let Some(mut gradient) = last_gradient {
                // Drop pieces already available locally from the retained
                // window. `recompute` skips them, so they would never trigger
                // `piece_ready` again and their entries would keep the gradient
                // non-zero forever, blocking the all-zero cleanup that clears
                // `prefetch`.
                for (p, prio) in gradient.iter_mut().enumerate() {
                    let piece_key = PieceStore::piece_key(info_hash, p as i32);
                    if store.has_piece(info_hash, p as i32) || store.has_piece_on_disk(&piece_key) {
                        *prio = 0;
                    }
                }
                if gradient.iter().any(|&p| p != 0) {
                    self.prefetch.insert(info_hash.to_string(), gradient);
                } else {
                    // The whole window is already cached — nothing to prefetch.
                    self.prefetch.remove(info_hash);
                }
            }
        }
        self.recompute(handle, info_hash, store);
    }

    /// `PieceReady` event: a piece finished downloading.  Deprioritize it and
    /// drop it from the retained prefetch window.  Once every wanted piece is
    /// ready the whole prefetch state is cleared, so a later eviction cannot
    /// re-elevate a stale gradient.
    pub fn piece_ready(&mut self, handle: &TorrentHandle, info_hash: &str, piece_index: i32) {
        if let Some(priorities) = self.elevated.get_mut(info_hash) {
            if piece_index >= 0 && (piece_index as usize) < priorities.len() {
                if priorities[piece_index as usize] != 0 {
                    priorities[piece_index as usize] = 0;
                    handle.set_piece_priority(piece_index, 0);
                }
            }
        }
        let clear_prefetch = if let Some(pref) = self.prefetch.get_mut(info_hash) {
            clear_ready_piece(pref, piece_index)
        } else {
            false
        };
        if clear_prefetch {
            self.prefetch.remove(info_hash);
        }
    }

    /// `PieceEvicted` event: a cached piece was evicted.  Recompute the
    /// gradient so the piece is re-elevated if an active reader still wants it.
    pub fn piece_evicted(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        store: &PieceStore,
    ) -> TorrentResult<()> {
        self.recompute(handle, info_hash, store);
        Ok(())
    }
    // ── Status queries ────────────────────────────────────────────────

    /// The current priority vector for an info_hash.
    pub fn priorities(&self, info_hash: &str) -> Option<&[i32]> {
        self.elevated.get(info_hash).map(|v| v.as_slice())
    }

    /// Current priority for a single piece (0..7, 0 = not wanted).
    pub fn get_piece_priority(&self, info_hash: &str, piece_index: i32) -> i32 {
        self.priorities(info_hash)
            .and_then(|v| {
                if piece_index >= 0 && (piece_index as usize) < v.len() {
                    Some(v[piece_index as usize])
                } else {
                    None
                }
            })
            .unwrap_or(0)
    }

    /// Number of pieces tracked for this info_hash.
    pub fn num_pieces(&self, info_hash: &str) -> Option<i32> {
        self.elevated.get(info_hash).map(|v| v.len() as i32)
    }

    /// Piece length (bytes) recorded at torrent initialization.
    pub fn piece_length(&self, info_hash: &str) -> Option<u64> {
        self.piece_lengths.get(info_hash).map(|&v| v as u64)
    }

    /// Indices of pieces currently elevated (priority > 0).
    pub fn elevated_pieces(&self, info_hash: &str) -> Vec<i32> {
        self.priorities(info_hash)
            .map(|v| {
                v.iter()
                    .enumerate()
                    .filter(|(_, &prio)| prio > 0)
                    .map(|(i, _)| i as i32)
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Whether a torrent has converged to idle: no active reader and no
    /// wanted piece.  The engine uses this (on reader release and on the
    /// periodic tick) to restore `upload_mode` exactly when both the reader
    /// count and the retained wanted set reach zero.
    pub fn is_idle(&self, info_hash: &str) -> bool {
        let has_readers = self.readers.get(info_hash).map_or(false, |r| !r.is_empty());
        !has_readers && self.elevated_pieces(info_hash).is_empty()
    }

    /// Remove all per-torrent state for an info_hash (priority vector,
    /// piece length, reader ranges).  Called when the download engine drops
    /// a torrent handle so the scheduler does not leak state for a removed
    /// torrent.
    pub fn remove_torrent(&mut self, info_hash: &str) {
        self.elevated.remove(info_hash);
        self.piece_lengths.remove(info_hash);
        self.readers.remove(info_hash);
        self.prefetch.remove(info_hash);
    }

    // ── Internals ─────────────────────────────────────────────────────

    /// Recompute the priority gradient as the element-wise maximum over all
    /// active readers' gradients (falling back to the retained prefetch
    /// gradient once the last reader releases), then apply it to the handle
    /// (skipping already-cached pieces).  Infallible: it only mutates
    /// in-memory state and issues best-effort piece-priority FFI calls.
    fn recompute(&mut self, handle: &TorrentHandle, info_hash: &str, store: &PieceStore) {
        let num_pieces = self
            .elevated
            .get(info_hash)
            .map(|v| v.len() as i32)
            .unwrap_or(0);

        if num_pieces <= 0 {
            return;
        }

        let priorities = self
            .elevated
            .entry(info_hash.to_string())
            .or_insert_with(|| vec![0i32; num_pieces as usize]);

        let readers = self
            .readers
            .get(info_hash)
            .map(|r| r.as_slice())
            .unwrap_or(&[]);
        let prefetch = self.prefetch.get(info_hash).map(|p| p.as_slice());
        let target = match resolve_target(readers, prefetch, num_pieces as usize) {
            Some(t) => t,
            None => {
                // Nothing wanted (no readers, no prefetch): idle baseline.
                for (p, prio) in priorities.iter_mut().enumerate() {
                    if *prio != DEFAULT_PRIORITY {
                        *prio = DEFAULT_PRIORITY;
                        handle.set_piece_priority(p as i32, DEFAULT_PRIORITY);
                    }
                }
                return;
            }
        };

        for (p, &prio) in target.iter().enumerate() {
            let piece_key = PieceStore::piece_key(info_hash, p as i32);
            let cached = store.has_piece(info_hash, p as i32) || store.has_piece_on_disk(&piece_key);
            let new_prio = if cached { 0 } else { prio };
            if priorities[p] != new_prio {
                priorities[p] = new_prio;
                handle.set_piece_priority(p as i32, new_prio);
            }
        }
    }

    /// Compute the priority gradient for a single read range, as a full
    /// `Vec<i32>` of length `num_pieces` (zero outside the accessed file's
    /// piece range).  Returns `None` for an invalid / out-of-range read.
    fn gradient_for(
        &self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
    ) -> Option<Vec<i32>> {
        let piece_length = info.piece_length() as u64;
        let num_pieces = info.num_pieces() as i32;
        if num_pieces <= 0 || piece_length == 0 {
            return None;
        }

        let piece_info: FilePieceInfo = handle.get_file_piece_info(file_index).ok()?;
        let file_offset = piece_info.file_offset as u64;
        let p_file_start = piece_info.first_piece as i32;
        let p_file_end = p_file_start + piece_info.num_pieces as i32 - 1;

        if p_file_start >= num_pieces || p_file_end < 0 {
            return None;
        }

        let absolute_offset = file_offset + offset;
        let files = info.files().ok()?;
        let file_size = files.get(file_index as usize).map(|f| f.size).unwrap_or(0);
        let file_abs_end = file_offset + file_size;
        let clamped_size = if absolute_offset < file_abs_end {
            std::cmp::min(size as u64, file_abs_end - absolute_offset) as u32
        } else {
            return None; // past EOF
        };

        let p_cur_start = (absolute_offset / piece_length) as i32;
        let p_cur_end = if clamped_size > 0 {
            ((absolute_offset + clamped_size as u64 - 1) / piece_length) as i32
        } else {
            p_cur_start
        };

        // Access window in pieces, shared by the forward and backward regions.
        // `piece_length > 0` is guaranteed by the early return above.
        let window_bytes = self.config.access_window_mb as u64 * 1024 * 1024;
        let window_pieces = window_bytes.div_ceil(piece_length) as i32;

        let mut gradient = vec![0i32; num_pieces as usize];
        let p_start = std::cmp::max(0, p_file_start);
        let p_end = std::cmp::min(num_pieces - 1, p_file_end);

        for p in p_start..=p_end {
            gradient[p as usize] =
                decide_priority(&self.config, p, p_cur_start, p_cur_end, window_pieces);
        }

        Some(gradient)
    }
}

/// Resolve the target piece-priority gradient for a torrent: the element-wise
/// maximum over all active readers' gradients, falling back to the retained
/// prefetch gradient once the last reader releases.  Returns `None` when
/// nothing is wanted (no readers, no prefetch) — the idle baseline.
fn resolve_target(
    readers: &[ReadRange],
    prefetch: Option<&[i32]>,
    num_pieces: usize,
) -> Option<Vec<i32>> {
    let mut target = vec![0i32; num_pieces];
    let mut any = false;
    for range in readers {
        any = true;
        for (i, &p) in range.gradient.iter().enumerate() {
            if p > target[i] {
                target[i] = p;
            }
        }
    }
    if !any {
        if let Some(pref) = prefetch {
            any = true;
            for (i, &p) in pref.iter().enumerate() {
                if p > target[i] {
                    target[i] = p;
                }
            }
        }
    }
    if any {
        Some(target)
    } else {
        None
    }
}

/// Zero a newly-ready piece in the retained prefetch gradient and report
/// whether the whole gradient is now satisfied (so the caller can drop the
/// prefetch state).  Out-of-range indices leave the gradient untouched.
fn clear_ready_piece(pref: &mut [i32], piece_index: i32) -> bool {
    if piece_index >= 0 && (piece_index as usize) < pref.len() {
        pref[piece_index as usize] = 0;
    }
    pref.iter().all(|&p| p == 0)
}

/// Piece priority decision for a single piece — pure and unit-testable.
///
/// `p_cur_start..=p_cur_end` is the current read range; `window_pieces` is the
/// access window size in pieces.  Backward pieces within the window get
/// `backward_priority`, forward pieces descend through `step_priorities` to
/// `window_edge_priority` at the window edge, and everything beyond the window
/// gets `rest_priority` (0 = not wanted).
fn decide_priority(
    config: &PiecePriorityConfig,
    p: i32,
    p_cur_start: i32,
    p_cur_end: i32,
    window_pieces: i32,
) -> i32 {
    if p < p_cur_start {
        // Backward region: only pieces within the window behind the read range
        // stay "wanted"; anything further back is not.
        if p >= p_cur_start.saturating_sub(window_pieces) {
            config.backward_priority
        } else {
            0
        }
    } else if p <= p_cur_end {
        config.current_priority
    } else {
        let dist = p - p_cur_end; // 1-based distance past the current read end.
        let idx = (dist - 1) as usize;
        if idx < config.step_priorities.len() {
            config.step_priorities[idx]
        } else if p <= p_cur_end.saturating_add(window_pieces) {
            config.window_edge_priority
        } else {
            config.rest_priority
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_priority_is_zero() {
        assert_eq!(DEFAULT_PRIORITY, 0);
    }

    #[test]
    fn default_config_matches_design() {
        let c = PiecePriorityConfig::default();
        assert_eq!(c.access_window_mb, 4096);
        assert_eq!(c.current_priority, 7);
        assert_eq!(c.step_priorities, [6, 5, 4, 3]);
        assert_eq!(c.window_edge_priority, 1);
        assert_eq!(c.rest_priority, 0);
        assert_eq!(c.backward_priority, 1);
    }

    #[test]
    fn forward_gradient_descends_to_edge_then_zero() {
        let c = PiecePriorityConfig::default();
        // Current read range: piece 10.  Access window: 8 pieces ahead.
        let (cur_start, cur_end) = (10, 10);
        let window_pieces = 8;

        // Current piece is highest.
        assert_eq!(decide_priority(&c, 10, cur_start, cur_end, window_pieces), 7);
        // Step priorities for distance 1..=4.
        assert_eq!(decide_priority(&c, 11, cur_start, cur_end, window_pieces), 6);
        assert_eq!(decide_priority(&c, 12, cur_start, cur_end, window_pieces), 5);
        assert_eq!(decide_priority(&c, 13, cur_start, cur_end, window_pieces), 4);
        assert_eq!(decide_priority(&c, 14, cur_start, cur_end, window_pieces), 3);
        // Remainder of the window sits at the edge priority.
        assert_eq!(decide_priority(&c, 15, cur_start, cur_end, window_pieces), 1);
        assert_eq!(decide_priority(&c, 18, cur_start, cur_end, window_pieces), 1);
        // Beyond the window: not wanted.
        assert_eq!(decide_priority(&c, 19, cur_start, cur_end, window_pieces), 0);
    }

    #[test]
    fn backward_priority_is_gated_by_window() {
        let c = PiecePriorityConfig::default();
        let (cur_start, cur_end) = (10, 10);
        let window_pieces = 8;

        // Within the window behind the read range: wanted (1).
        assert_eq!(decide_priority(&c, 9, cur_start, cur_end, window_pieces), 1);
        assert_eq!(decide_priority(&c, 2, cur_start, cur_end, window_pieces), 1);
        // Further back than the window: not wanted (0).
        assert_eq!(decide_priority(&c, 1, cur_start, cur_end, window_pieces), 0);
        assert_eq!(decide_priority(&c, 0, cur_start, cur_end, window_pieces), 0);
    }

    #[test]
    fn from_toml_defaults_and_overrides() {
        // Empty section → every field falls back to the default.
        let d = PiecePriorityConfig::from_toml(&PiecePriorityToml::default());
        assert_eq!(d.access_window_mb, 4096);
        assert_eq!(d.rest_priority, 0);
        assert_eq!(d.backward_priority, 1);

        // Partial override: only the specified fields change.
        let partial = PiecePriorityToml {
            access_window_mb: Some(2048),
            current_priority: None,
            step_priorities: None,
            window_edge_priority: None,
            rest_priority: None,
            backward_priority: None,
        };
        let p = PiecePriorityConfig::from_toml(&partial);
        assert_eq!(p.access_window_mb, 2048);
        assert_eq!(p.current_priority, 7);
        assert_eq!(p.step_priorities, [6, 5, 4, 3]);

        // Full override.
        let full = PiecePriorityToml {
            access_window_mb: Some(512),
            current_priority: Some(8),
            step_priorities: Some([7, 6, 5, 4]),
            window_edge_priority: Some(2),
            rest_priority: Some(1),
            backward_priority: Some(2),
        };
        let f = PiecePriorityConfig::from_toml(&full);
        assert_eq!(f.access_window_mb, 512);
        assert_eq!(f.current_priority, 8);
        assert_eq!(f.step_priorities, [7, 6, 5, 4]);
        assert_eq!(f.window_edge_priority, 2);
        assert_eq!(f.rest_priority, 1);
        assert_eq!(f.backward_priority, 2);
    }

    #[test]
    fn resolve_target_unions_active_readers_over_prefetch() {
        let readers = vec![
            ReadRange {
                gradient: vec![7, 0, 6, 5],
            },
            ReadRange {
                gradient: vec![0, 3, 0, 0],
            },
        ];
        let prefetch = vec![1, 1, 1, 1];
        // Active readers take precedence over any retained prefetch.
        let target = resolve_target(&readers, Some(&prefetch), 4).unwrap();
        assert_eq!(target, vec![7, 3, 6, 5]);
    }

    #[test]
    fn resolve_target_falls_back_to_prefetch_when_idle() {
        let prefetch = vec![1, 1, 0, 6, 5, 4, 3];
        let no_readers: &[ReadRange] = &[];
        let target = resolve_target(no_readers, Some(&prefetch), 7).unwrap();
        assert_eq!(target, prefetch);
    }

    #[test]
    fn resolve_target_none_when_nothing_wanted() {
        let no_readers: &[ReadRange] = &[];
        assert_eq!(resolve_target(no_readers, None, 4), None);
    }

    #[test]
    fn clear_ready_piece_zeroes_and_reports_satisfied() {
        let mut pref = vec![1, 6, 5, 4];
        assert!(!clear_ready_piece(&mut pref, 0));
        assert_eq!(pref, vec![0, 6, 5, 4]);
        assert!(!clear_ready_piece(&mut pref, 1));
        assert_eq!(pref, vec![0, 0, 5, 4]);
        assert!(!clear_ready_piece(&mut pref, 2));
        assert_eq!(pref, vec![0, 0, 0, 4]);
        // The last wanted piece is now ready → the gradient is fully satisfied.
        assert!(clear_ready_piece(&mut pref, 3));
        assert_eq!(pref, vec![0, 0, 0, 0]);
    }

    #[test]
    fn clear_ready_piece_ignores_out_of_range_indices() {
        let mut pref = vec![1, 1];
        assert!(!clear_ready_piece(&mut pref, -1));
        assert!(!clear_ready_piece(&mut pref, 2));
        assert_eq!(pref, vec![1, 1]);
    }

    #[test]
    fn is_idle_requires_no_readers_and_no_wanted_pieces() {
        let mut s = PieceScheduler::new(PiecePriorityConfig::default());
        s.init_torrent("hash", 4, 256).unwrap();

        // Fresh torrent: no readers, no wanted pieces.
        assert!(s.is_idle("hash"));

        // A wanted piece keeps it active.
        s.elevated.insert("hash".to_string(), vec![0, 6, 0, 0]);
        assert!(!s.is_idle("hash"));

        // No wanted pieces but an active reader also keeps it active.
        s.elevated.insert("hash".to_string(), vec![0, 0, 0, 0]);
        s.readers.insert(
            "hash".to_string(),
            vec![ReadRange {
                gradient: vec![7, 0, 0, 0],
            }],
        );
        assert!(!s.is_idle("hash"));

        // No readers, no wanted pieces → idle.
        s.readers.remove("hash");
        assert!(s.is_idle("hash"));

        // An unknown torrent is trivially idle.
        assert!(s.is_idle("other"));
    }
}

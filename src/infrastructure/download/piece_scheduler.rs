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

use std::collections::{HashMap, HashSet};

use crate::error::TorrentResult;
use crate::infrastructure::config::PiecePriorityToml;
use crate::infrastructure::download::TorrentHandle;
use crate::infrastructure::metadata::TorrentInfo;

use super::piece_store::PieceStore;
use super::types::FilePieceInfo;

/// Idle baseline piece priority: 0 = "not wanted".  Pieces outside every
/// active reader's access window stay at 0, so libtorrent never requests them
/// on its own — the priority vector is the sole "what to download" authority,
/// which is why the torrent is never returned to `upload_mode` after a read.
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
}

impl Default for PiecePriorityConfig {
    fn default() -> Self {
        Self {
            access_window_mb: 4096,
            current_priority: 7,
            step_priorities: [6, 5, 4, 3],
            window_edge_priority: 1,
            rest_priority: 0,
        }
    }
}

impl PiecePriorityConfig {
    /// Build the runtime config from optional TOML overrides, falling back to
    /// [`Self::default`] for any field the user did not specify.
    pub fn from_toml(toml: &PiecePriorityToml) -> Self {
        if toml.backward_priority.is_some() {
            tracing::warn!(
                "[piece_priority] backward_priority is deprecated and ignored; \
                 backward prefetch is no longer supported"
            );
        }
        let d = Self::default();
        Self {
            access_window_mb: toml.access_window_mb.unwrap_or(d.access_window_mb),
            current_priority: toml.current_priority.unwrap_or(d.current_priority),
            step_priorities: toml.step_priorities.unwrap_or(d.step_priorities),
            window_edge_priority: toml
                .window_edge_priority
                .unwrap_or(d.window_edge_priority),
            rest_priority: toml.rest_priority.unwrap_or(d.rest_priority),
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

/// Unique id of an active reader.  [`PieceScheduler::reader_added`] returns it
/// and [`PieceScheduler::reader_released`] requires it, so a release removes
/// exactly the reader that finished — several readers on one torrent can be
/// active at once now that a read can be parked off the engine thread.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadId(u64);

/// An active reader: its precomputed priority gradient.  Reference-counted by
/// [`PieceScheduler::readers`].
#[derive(Debug, Clone)]
struct ReadRange {
    id: ReadId,
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
    /// Active readers per info_hash (reference counting), each tagged with a
    /// unique [`ReadId`].  A read parked off the engine thread keeps its reader
    /// registered while it waits, so one torrent can hold several active
    /// readers; `reader_released` therefore removes by id, never by position.
    readers: HashMap<String, Vec<ReadRange>>,
    /// Next [`ReadId`] to hand out.  Monotonic, so an id is never reused and a
    /// stale release cannot match a newer reader.
    next_read_id: u64,
    /// Per-info_hash retained prefetch gradient: the last reader's gradient,
    /// kept after the final reader releases so the read-ahead window keeps
    /// downloading and stays visible in `.stats` as `[N]` markers.
    prefetch: HashMap<String, Vec<i32>>,
    /// Info hashes whose libtorrent piece priorities were reset out-of-band
    /// (e.g. by a `force_recheck`) and no longer match `elevated`.  The next
    /// `recompute` fully rewrites every priority once, then clears the flag;
    /// ordinary recomputes write only changed pieces (O(changed), not
    /// O(num_pieces)).
    dirty: HashSet<String>,
    /// Cache capacity (bytes).  Caps the read-ahead window so prefetch never
    /// outruns the cache: a window larger than the cache would make the
    /// prefetch download evict the pieces the current read is still serving,
    /// leaving stale `have_piece` bits that re-trigger `force_recheck`.
    cache_capacity_bytes: u64,
}

impl PieceScheduler {
    pub fn new(config: PiecePriorityConfig, cache_capacity_bytes: u64) -> Self {
        Self {
            elevated: HashMap::new(),
            piece_lengths: HashMap::new(),
            config,
            readers: HashMap::new(),
            next_read_id: 0,
            prefetch: HashMap::new(),
            dirty: HashSet::new(),
            cache_capacity_bytes,
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
        self.dirty.remove(info_hash);
        Ok(())
    }

    // ── Priority events ───────────────────────────────────────────────

    /// `ReaderAdded` event: a reader started.  Records the read range under a
    /// fresh [`ReadId`] and recomputes the priority gradient (union over all
    /// active readers).  The caller must pass that id back to
    /// [`Self::reader_released`]; an unreadable range registers no gradient but
    /// still gets an id, whose release is a no-op.
    pub fn reader_added(
        &mut self,
        handle: &TorrentHandle,
        info: &TorrentInfo,
        file_index: i32,
        offset: u64,
        size: u32,
        store: &PieceStore,
    ) -> TorrentResult<ReadId> {
        let info_hash = hex::encode(info.info_hash()?);
        let id = ReadId(self.next_read_id);
        self.next_read_id += 1;
        let gradient = match self.gradient_for(handle, info, file_index, offset, size) {
            Some(g) => g,
            None => return Ok(id),
        };
        self.readers
            .entry(info_hash.clone())
            .or_default()
            .push(ReadRange { id, gradient });
        self.recompute(handle, &info_hash, store);
        Ok(id)
    }

    /// `ReaderReleased` event: the reader identified by `id` finished.  Removes
    /// exactly that reader and recomputes.  When the last reader releases, its
    /// gradient is retained as the prefetch window so the read-ahead download
    /// continues.
    ///
    /// Infallible: `recompute` only mutates in-memory state and best-effort FFI
    /// priorities.
    pub fn reader_released(
        &mut self,
        handle: &TorrentHandle,
        info_hash: &str,
        id: ReadId,
        store: &PieceStore,
    ) {
        let (empty, last_gradient) = if let Some(ranges) = self.readers.get_mut(info_hash) {
            let released = ranges
                .iter()
                .position(|r| r.id == id)
                .map(|pos| ranges.remove(pos).gradient);
            (ranges.is_empty(), released)
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

    /// Mark a torrent's libtorrent piece priorities as out-of-band with the
    /// in-memory cache (a `force_recheck` reset them).  The next `recompute`
    /// fully rewrites every priority once, then clears the flag; ordinary
    /// recomputes write only changed pieces.
    pub fn mark_priorities_dirty(&mut self, info_hash: &str) {
        if self.elevated.contains_key(info_hash) {
            self.dirty.insert(info_hash.to_string());
        }
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

    /// Remove all per-torrent state for an info_hash (priority vector,
    /// piece length, reader ranges).  Called when the download engine drops
    /// a torrent handle so the scheduler does not leak state for a removed
    /// torrent.
    pub fn remove_torrent(&mut self, info_hash: &str) {
        self.elevated.remove(info_hash);
        self.piece_lengths.remove(info_hash);
        self.readers.remove(info_hash);
        self.prefetch.remove(info_hash);
        self.dirty.remove(info_hash);
    }

    // ── Internals ─────────────────────────────────────────────────────

    /// Recompute the priority gradient as the element-wise maximum over all
    /// active readers' gradients (falling back to the retained prefetch
    /// gradient once the last reader releases), then apply it to the handle
    /// (skipping already-cached pieces).  Infallible: it only mutates
    /// in-memory state and issues best-effort piece-priority FFI calls.
    pub fn recompute(&mut self, handle: &TorrentHandle, info_hash: &str, store: &PieceStore) {
        let num_pieces = self
            .elevated
            .get(info_hash)
            .map(|v| v.len() as i32)
            .unwrap_or(0);

        if num_pieces <= 0 {
            return;
        }

        // A recheck reset libtorrent's piece priorities out-of-band, leaving
        // the in-memory cache stale.  Rewrite every piece once when that
        // happened; the ordinary path below writes only changed pieces.
        let full_rewrite = self.dirty.remove(info_hash);

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
                // Bulk-reset every piece to `dont_download`.
                for prio in priorities.iter_mut() {
                    *prio = DEFAULT_PRIORITY;
                }
                handle.set_all_piece_priorities(DEFAULT_PRIORITY);
                return;
            }
        };

        for (p, &prio) in target.iter().enumerate() {
            let piece_key = PieceStore::piece_key(info_hash, p as i32);
            let cached =
                store.has_piece(info_hash, p as i32) || store.has_piece_on_disk(&piece_key);
            let new_prio = if cached { 0 } else { prio };
            // Skip unchanged pieces on the ordinary path (O(changed)); the
            // in-memory cache is authoritative except after a recheck, whose
            // out-of-band priority reset is covered by the one-shot full
            // rewrite above.
            if !full_rewrite && priorities[p] == new_prio {
                continue;
            }
            priorities[p] = new_prio;
            handle.set_piece_priority(p as i32, new_prio);
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
        let window_pieces = effective_window_pieces(
            self.config.access_window_mb,
            self.cache_capacity_bytes,
            piece_length,
        );

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

/// Effective read-ahead window in pieces, capped so prefetch never outruns the
/// cache: it reserves one piece for the piece being served (cache minus one
/// piece), so prefetch can't evict that piece before its sub-reads complete.
/// A one-piece cache yields a zero window (no prefetch; the current piece stays
/// wanted).  Pure for unit testing.
fn effective_window_pieces(
    access_window_mb: u32,
    cache_capacity_bytes: u64,
    piece_length: u64,
) -> i32 {
    let window_bytes = access_window_mb as u64 * 1024 * 1024;
    let prefetch_bytes = cache_capacity_bytes.saturating_sub(piece_length);
    let window_bytes = std::cmp::min(window_bytes, prefetch_bytes);
    // Floor division, not ceil: a fractional-multiple cache (e.g. 1.5 pieces)
    // must round the headroom DOWN, or the ceiling over-reserves and prefetch
    // evicts the piece being served.
    (window_bytes / piece_length) as i32
}

/// Piece priority decision for a single piece — pure and unit-testable.
///
/// `p_cur_start..=p_cur_end` is the current read range; `window_pieces` is the
/// access window size in pieces.  Backward pieces are never prefetched; forward
/// pieces descend through `step_priorities` to `window_edge_priority` at the
/// window edge, and everything beyond the window gets `rest_priority`
/// (0 = not wanted).
fn decide_priority(
    config: &PiecePriorityConfig,
    p: i32,
    p_cur_start: i32,
    p_cur_end: i32,
    window_pieces: i32,
) -> i32 {
    if p < p_cur_start {
        // No backward prefetch.  Forward prefetch + the piece being served
        // already fill a small cache; prefetching pieces behind the read on top
        // of that evicts the served piece (stale bit → recheck → slow
        // re-download).
        0
    } else if p <= p_cur_end {
        config.current_priority
    } else {
        // The window bounds the whole forward region, including `step_priorities`:
        // without this a window smaller than `step_priorities.len()` would still
        // elevate those pieces and the prefetch reach would have a 4-piece floor
        // (`max(4, window_pieces)`), outrunning a sub-4-piece cache.
        let dist = p - p_cur_end; // 1-based distance past the current read end.
        if dist > window_pieces {
            config.rest_priority
        } else {
            let idx = (dist - 1) as usize;
            if idx < config.step_priorities.len() {
                config.step_priorities[idx]
            } else {
                config.window_edge_priority
            }
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

    /// The window must reserve one piece for the piece being served, and a
    /// one-piece cache yields a zero window (no prefetch) rather than a floor
    /// of one — otherwise prefetch would evict the in-service piece.
    #[test]
    fn effective_window_reserves_one_piece() {
        const PIECE: u64 = 256 * 1024;
        // QA scenario: 1 MiB cache = 4 pieces → 3-piece window.
        assert_eq!(effective_window_pieces(4096, 4 * PIECE, PIECE), 3);
        // One-piece cache → zero window: no prefetch, current piece stays wanted.
        assert_eq!(effective_window_pieces(4096, PIECE, PIECE), 0);
        // Two-piece cache → one piece of headroom.
        assert_eq!(effective_window_pieces(4096, 2 * PIECE, PIECE), 1);
        // Fractional-multiple caches round the headroom DOWN: 1.5 pieces leaves
        // no whole piece of headroom, 2.5 leaves exactly one.
        assert_eq!(effective_window_pieces(4096, 3 * PIECE / 2, PIECE), 0);
        assert_eq!(effective_window_pieces(4096, 5 * PIECE / 2, PIECE), 1);
        // Default 1 GiB cache (4096 pieces) → 4095; the access window is the
        // larger bound here.
        assert_eq!(
            effective_window_pieces(4096, 1024 * 1024 * 1024, PIECE),
            4095
        );
        // A narrow access window still wins when it is smaller than the cache.
        assert_eq!(effective_window_pieces(1, 1024 * 1024 * 1024, PIECE), 4);
    }

    #[test]
    fn default_config_matches_design() {
        let c = PiecePriorityConfig::default();
        assert_eq!(c.access_window_mb, 4096);
        assert_eq!(c.current_priority, 7);
        assert_eq!(c.step_priorities, [6, 5, 4, 3]);
        assert_eq!(c.window_edge_priority, 1);
        assert_eq!(c.rest_priority, 0);
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

    /// A window smaller than `step_priorities.len()` must also bound the step
    /// priorities — otherwise the forward reach has a 4-piece floor
    /// (`max(4, window_pieces)`) and a sub-4-piece cache still prefetches past
    /// its capacity.
    #[test]
    fn forward_step_priorities_are_bounded_by_window() {
        let c = PiecePriorityConfig::default();
        let (cur_start, cur_end) = (10, 10);
        let window_pieces = 2;

        // Distances 1..=2 stay on the step ladder.
        assert_eq!(
            decide_priority(&c, 11, cur_start, cur_end, window_pieces),
            6
        );
        assert_eq!(
            decide_priority(&c, 12, cur_start, cur_end, window_pieces),
            5
        );
        // Distance 3 is inside step_priorities but beyond the window: not wanted.
        assert_eq!(
            decide_priority(&c, 13, cur_start, cur_end, window_pieces),
            0
        );
        assert_eq!(
            decide_priority(&c, 14, cur_start, cur_end, window_pieces),
            0
        );
    }

    #[test]
    fn backward_pieces_are_never_prefetched() {
        let c = PiecePriorityConfig::default();
        let (cur_start, cur_end) = (10, 10);
        let window_pieces = 8;

        // Backward pieces are never prefetched, regardless of the window:
        // forward prefetch + the served piece already fill a small cache, and
        // prefetching behind the read would evict the served piece.
        assert_eq!(decide_priority(&c, 9, cur_start, cur_end, window_pieces), 0);
        assert_eq!(decide_priority(&c, 0, cur_start, cur_end, window_pieces), 0);
    }

    #[test]
    fn from_toml_defaults_and_overrides() {
        // Empty section → every field falls back to the default.
        let d = PiecePriorityConfig::from_toml(&PiecePriorityToml::default());
        assert_eq!(d.access_window_mb, 4096);
        assert_eq!(d.rest_priority, 0);

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

        // Full override.  `backward_priority` is accepted but ignored
        // (deprecated); setting it must not affect the result.
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
    }

    #[test]
    fn resolve_target_unions_active_readers_over_prefetch() {
        let readers = vec![
            ReadRange {
                id: ReadId(0),
                gradient: vec![7, 0, 6, 5],
            },
            ReadRange {
                id: ReadId(1),
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

    /// Two readers can be active on one torrent at once: a read parked off the
    /// engine thread keeps its reader registered while it waits on the swarm.
    /// Releasing one must remove exactly that reader — the old LIFO `pop`
    /// removed the last-added one instead, so a finished read's gradient
    /// lingered while the still-waiting read's was dropped, corrupting the
    /// union gradient and the retained prefetch window.
    #[test]
    fn concurrent_readers_release_by_id_not_lifo() {
        use crate::infrastructure::cache::CacheManager;
        use std::sync::{Arc, Mutex};

        let mut s = PieceScheduler::new(PiecePriorityConfig::default(), 1024 * 1024 * 1024);
        s.init_torrent("hash", 4, 256).unwrap();

        // Reader 0 wants pieces 0..1, reader 1 wants pieces 2..3.
        s.readers.insert(
            "hash".to_string(),
            vec![
                ReadRange {
                    id: ReadId(0),
                    gradient: vec![7, 6, 0, 0],
                },
                ReadRange {
                    id: ReadId(1),
                    gradient: vec![0, 0, 6, 5],
                },
            ],
        );

        let dir = tempfile::TempDir::new().unwrap();
        let store = PieceStore::new(Arc::new(Mutex::new(
            CacheManager::new(dir.path(), 1024 * 1024).unwrap(),
        )));
        // Piece-priority FFI calls are no-ops on a null handle; the assertions
        // cover the scheduler's own bookkeeping, which is where the bug was.
        let handle = TorrentHandle {
            inner: std::ptr::null_mut(),
            info_hash: "hash".to_string(),
            session: std::ptr::null_mut(),
        };

        // The first reader finishes while the second is still waiting.
        s.reader_released(&handle, "hash", ReadId(0), &store);
        assert_eq!(
            s.readers.get("hash").map(|r| r.len()),
            Some(1),
            "releasing reader 0 must leave reader 1 active"
        );
        // The union still wants reader 1's pieces, not the released reader's.
        assert_eq!(s.elevated_pieces("hash"), vec![2, 3]);

        // The last reader's own gradient becomes the retained prefetch window.
        s.reader_released(&handle, "hash", ReadId(1), &store);
        assert_eq!(s.readers.get("hash").map(|r| r.len()), Some(0));
        assert_eq!(s.prefetch.get("hash").cloned(), Some(vec![0, 0, 6, 5]));
    }

    /// A release carrying an id that was never registered (or was already
    /// released) is a no-op: it must not remove an unrelated live reader.
    #[test]
    fn release_of_unknown_id_leaves_other_readers_alone() {
        use crate::infrastructure::cache::CacheManager;
        use std::sync::{Arc, Mutex};

        let mut s = PieceScheduler::new(PiecePriorityConfig::default(), 1024 * 1024 * 1024);
        s.init_torrent("hash", 4, 256).unwrap();
        s.readers.insert(
            "hash".to_string(),
            vec![ReadRange {
                id: ReadId(7),
                gradient: vec![0, 0, 6, 5],
            }],
        );

        let dir = tempfile::TempDir::new().unwrap();
        let store = PieceStore::new(Arc::new(Mutex::new(
            CacheManager::new(dir.path(), 1024 * 1024).unwrap(),
        )));
        let handle = TorrentHandle {
            inner: std::ptr::null_mut(),
            info_hash: "hash".to_string(),
            session: std::ptr::null_mut(),
        };

        s.reader_released(&handle, "hash", ReadId(3), &store);
        assert_eq!(s.readers.get("hash").map(|r| r.len()), Some(1));
        assert_eq!(s.elevated_pieces("hash"), vec![2, 3]);
        assert!(s.prefetch.get("hash").is_none());
    }
}

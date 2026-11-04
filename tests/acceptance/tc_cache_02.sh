#!/usr/bin/env bash
# Design acceptance case TC-cache-02: unmount → remount with the same cache
# directory → the cache still hits and the content is correct
# (`docs/03-acceptance.md` §D AC-31).
#
# Container-executable form: the released binary plus the local tracker/peer
# fixtures (`lib_acceptance_env.sh`), so the case runs inside the published
# image with no build step.  The same cache and state directories are mounted
# twice; the tracker and the seeder are stopped before the second mount, so a
# byte-exact read that finishes inside the cache-hit bound can only have come
# from the on-disk cache — a miss has no peer at all and ends in ENODATA.
#
# Usage: ./tests/acceptance/tc_cache_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-cache-02
write_fast_config

# Every piece cached (`[x]` / `[X n]`), none wanted-but-uncached (`[N]`) and
# none uncached (`[]`); the grid must also carry the torrent's piece count.
wait_all_cached() {
    local leaf="$1" want="$2" limit="${3:-60}" i tokens token ok
    for ((i = 0; i < limit * 4; i++)); do
        tokens="$(piece_markers "$leaf" 2>/dev/null)"
        [ -n "$tokens" ] || { sleep 0.25; continue; }
        ok=1
        while IFS= read -r token; do
            case "$token" in
                "[x]"|"[X "*) : ;;
                *) ok=0; break ;;
            esac
        done <<<"$tokens"
        [ "$ok" -eq 1 ] || { sleep 0.25; continue; }
        [ "$(wc -l <<<"$tokens")" -eq "$want" ] && return 0
        sleep 0.25
    done
    return 1
}

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

LEAF="$MNT/data/seed.torrent/.stats"
FILE="$MNT/data/seed.torrent/seed.bin"

# ── First mount: ingest and read the whole file (downloads + caches) ────
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "the client never announced to the tracker"
read_retry "$FILE" "$W/first" 3 || fail "the first read failed"
cmp -s "$W/first" "$SRC" || fail "the first read bytes differ from the source"

# The second mount has no peer, so every piece must reach the cache before the
# daemon goes away; the marker grid is what says so.
wait_all_cached "$LEAF" "$PIECE_COUNT" 120 \
    || fail "not every piece reached the cache before the remount: $(tr '\n' ' ' <<<"$(piece_markers "$LEAF")")"
CACHE_BYTES="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "${CACHE_BYTES:-0}" -gt 0 ] || fail "the cache directory is still empty after the first read"
echo "  first mount: cache=${CACHE_BYTES}B, pieces=$(tr '\n' ' ' <<<"$(piece_markers "$LEAF")")"
daemon_stop

# Stop the swarm: the second mount's read must be served by the cache alone.
peer_stop
kill "$TRACKER_PID" >/dev/null 2>&1 || true
wait "$TRACKER_PID" >/dev/null 2>&1 || true
TRACKER_PID=""
sleep 2

# ── Second mount: same cache + state, no swarm ──────────────────────────
daemon_start
wait_path "$LEAF" 60 || fail "the projection did not come back after the remount"
CACHE_BYTES_AFTER="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "$CACHE_BYTES_AFTER" -ge "$CACHE_BYTES" ] \
    || fail "cache usage shrank across the remount (${CACHE_BYTES}B → ${CACHE_BYTES_AFTER}B)"

# The daemon re-verifies the retained pieces from disk on startup; its own
# summary is the statement that the cache survived the remount.
for _ in $(seq 1 60); do
    grep -q "Cache piece verification complete: $PIECE_COUNT verified" "$LOG_FILE" && break
    sleep 1
done
grep -q "Cache piece verification complete: $PIECE_COUNT verified" "$LOG_FILE" \
    || fail "the remount did not re-verify the $PIECE_COUNT cached pieces: $(grep -m1 'Cache piece verification' "$LOG_FILE" || echo 'no verification line')"

# Per-piece hit counters from the leaf grid, one integer per piece: n for a
# cached piece (`[x]` = 0, `[X n]` = n), -1 for anything not cached.
capture_grid() {
    local tok
    : > "$1"
    while IFS= read -r tok; do
        case "$tok" in
            "[x]") echo 0 >> "$1" ;;
            *"[X "*) echo "${tok##"[X "}" | tr -d ' ]' >> "$1" ;;
            *) echo -1 >> "$1" ;;
        esac
    done <<<"$(piece_markers "$LEAF" 2>/dev/null)"
}

# Every piece of the torrent is present in the grid (nothing evicted or lost).
grid_all_cached() {
    local n total=0
    while IFS= read -r n; do
        [ "$n" -ge 0 ] || return 1
        total=$((total + 1))
    done < "$1"
    [ "$total" -eq "$PIECE_COUNT" ]
}

# The grid is the all-zero-hit shape: every piece cached and untouched since
# this daemon started.
grid_untouched() {
    local n
    grid_all_cached "$1" || return 1
    while IFS= read -r n; do
        [ "$n" -eq 0 ] || return 1
    done < "$1"
    return 0
}

# At least one piece's hit counter rose above what the attempt started from.
grid_grew() {
    local before="$1" after="$2" b a
    while IFS=$'\t' read -r b a; do
        if [ "$b" -ge 0 ] && [ "$a" -gt "$b" ]; then return 0; fi
    done < <(paste "$before" "$after")
    return 1
}

# Root `.stats` cache hit counter: cumulative for this process and read live,
# unlike the piece grid (published from a ~1s snapshot).  It cannot be
# credited to accesses made before the remount.
root_hits() {
    sed -n 's/^  Hits: \([0-9,]*\).*/\1/p' "$MNT/.stats" 2>/dev/null | head -n1 | tr -d ','
}

BASELINE="$W/grid.baseline"
CURRENT="$W/grid.current"

# AC-31: the read after the remount must be served by the cache.  The hit is
# credited as growth over a stable baseline taken before the read.
#
# Fast path: the current implementation re-registers the retained pieces once
# after the mount (libtorrent's file check) and that registration zeroes the
# counters the previous mount persisted, so a read issued before that one-shot
# reset leaves no trace.  Wait (bounded) for the zero-hit grid, which is the
# post-reset state.
#
# Fallback: whether the counters survive a restart is a contract question
# (C-5.3.1 loads them, C-5.3.3 zeroes them), so the carrier must not bind
# itself to the zeroing side.  If the grid never goes to zero within the
# window, it is still a usable baseline once every piece is cached and two
# consecutive samples agree piece by piece — the counters then stay as they
# are (nothing is being reset mid-read) and growth over them still has to be
# produced by the read below.
BASELINE_KIND=""
capture_grid "$BASELINE"
DEADLINE=$((SECONDS + 30))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    capture_grid "$BASELINE"
    if grid_untouched "$BASELINE"; then
        BASELINE_KIND="zeroed"
        break
    fi
    sleep 0.5
done
if [ -z "$BASELINE_KIND" ]; then
    PREV="$W/grid.prev"
    for _ in $(seq 1 10); do
        cp "$BASELINE" "$PREV"
        sleep 1
        capture_grid "$BASELINE"
        if grid_all_cached "$BASELINE" && cmp -s "$PREV" "$BASELINE"; then
            BASELINE_KIND="kept"
            break
        fi
    done
fi
[ -n "$BASELINE_KIND" ] \
    || fail "the remount grid is neither zeroed nor stable (no usable baseline): $(tr '\n' ' ' < "$BASELINE")"
case "$BASELINE_KIND" in
    zeroed) echo "  remount baseline: zero hit counters (restart zeroed the persisted values)" ;;
    kept)   echo "  remount baseline: persisted hit counters kept across the restart (本实现未清零); baseline=$(tr '\n' ' ' < "$BASELINE")" ;;
esac

HITS_BEFORE="$(root_hits)"
START_NS="$(date +%s%N)"
rc=0
timeout 60 cat "$FILE" > "$W/second" 2> "$W/second.err" || rc=$?
END_NS="$(date +%s%N)"
[ "$rc" -eq 0 ] || fail "the remount read failed with no seeder (rc=$rc): $(tail -n 3 "$W/second.err")"
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
[ "$ELAPSED_MS" -lt 10000 ] \
    || fail "the remount read took ${ELAPSED_MS}ms; a cache miss would stall on the no-seeder window"
cmp -s "$W/second" "$SRC" || fail "the remount read content differs from the source"

# The leaf grid is published at most once per second and is not republished
# right after a read, so sample it with a bounded budget at the contract's 1s
# polling cadence (C-5.2.4 asks for at least a 5s window; leave margin).  The
# gate is the per-piece growth over the baseline: it can only come from the
# read just issued, because the baseline is stable and nothing else reads this
# mount.
#
# The root `.stats` cache counters are evidence only, never a gate: C-6.5.6
# lets the accessor degrade them to zero when the cache lock cannot be taken,
# so a zero reading says nothing about the read.
HIT=0
HITS_EVIDENCE="unavailable"
SAMPLE_WINDOW_SECS=12
DEADLINE=$((SECONDS + SAMPLE_WINDOW_SECS))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    sleep 1
    capture_grid "$CURRENT"
    grid_all_cached "$CURRENT" && grid_grew "$BASELINE" "$CURRENT" || continue
    HITS_AFTER="$(root_hits)"
    if [ "${HITS_AFTER:-0}" -gt 0 ]; then
        HITS_EVIDENCE="${HITS_BEFORE:-0}->${HITS_AFTER} (evidence, not a gate)"
    fi
    HIT=1
    break
done
[ "$HIT" -eq 1 ] \
    || fail "no piece reports a cache hit after the remount read: $(tr '\n' ' ' <<<"$(piece_markers "$LEAF")") (baseline ${BASELINE_KIND}; root hits ${HITS_BEFORE:-unreadable})"

if grep -q "no seeder connected\|NoPeers\|ENODATA" "$LOG_FILE"; then
    fail "the remount read fell back to the swarm: $(grep -m1 'no seeder connected\|NoPeers\|ENODATA' "$LOG_FILE")"
fi

echo "TC-cache-02: PASS (remount read ${ELAPSED_MS}ms, byte-exact from cache; cache=${CACHE_BYTES_AFTER}B; baseline ${BASELINE_KIND}; root hits ${HITS_EVIDENCE}; $(tr '\n' ' ' <<<"$(piece_markers "$LEAF")"))"

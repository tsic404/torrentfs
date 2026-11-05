#!/usr/bin/env bash
# Design acceptance case TC-cache-03: with a cache cap below the data size the
# cache evicts, and re-reading an evicted piece re-establishes its record
# (`docs/03-acceptance.md` §D AC-32, §TC-cache-03).
#
# Container-executable form: released binary; 1 MiB cap against a 4 MiB file.
# The observable is the leaf `.stats` piece grid plus the root cache counters.
# The engine republishes the piece snapshot about once a second, so every
# assertion below is about published samples, never about an instant the
# renderer cannot show.
#
# The re-read that triggers the rebuild is one 32 KiB block inside the piece:
# one FUSE request, so it can produce at most one hit of the piece.  A
# whole-piece read would issue four of them and the later three would hit the
# record the first one created — the count would say nothing about whether the
# rebuild started at zero.
#
# Two sampling points on the evicted piece:
#
#   1. right after the rebuild-triggering read — the rebuild creates the record,
#      and a record starts at zero hits, so the published form is `[7]` (wanted,
#      still downloading) or `[x]` (cached, zero hits).  `[X n]` (n >= 1) at
#      that instant would mean the rebuild was counted as a hit of the record it
#      created, or that the new record inherited the evicted one's count.
#   2. one more read of that piece — the count grows by the number of hits the
#      read made on the piece: `[x]` becomes `[X 1]`, `[X n]` becomes `[X n+1]`.
#      A second read repeats the increment, which is the half that separates a
#      live counter from one frozen at its first value.
#
# Usage: ./tests/acceptance/tc_cache_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-cache-03

# The grid is republished about once a second; the rebuild itself is bounded by
# the read timeout plus the re-download, and the hit counter needs one publish
# interval to show up.
REBUILD_DEADLINE_SECS=90
COUNT_DEADLINE_SECS=30
SETTLE_DEADLINE_SECS=60
PROBE_LEN=32768
PROBE_INSET=16384
# Each hit read uses a fresh block of the same piece: 16 KiB, 176 KiB and
# 112 KiB into it, so no two probes overlap and none crosses the boundary.
PROBE_HIT1=$((PROBE_INSET + 5 * PROBE_LEN))
PROBE_HIT2=$((PROBE_INSET + 3 * PROBE_LEN))

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 12
no_seeder_read_timeout_secs = 4
[cache]
cache_size = 1048576
[connections]
listen_interfaces = "127.0.0.1:0"
min_reconnect_time = 1
# No read-ahead: with the window open the pieces after this read download too,
# and their registrations evict the rebuilt piece between the two hit reads.
[piece_priority]
access_window_mb = 0
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"
LEAF="$MNT/data/seed.torrent/.stats"

# grid_tokens DST — snapshot the published marker run, one token per line, and
# reject anything outside the four-state grammar.  A token outside it is a
# render regression, not a state this case can reason about.
grid_tokens() {
    local dst="$1" bad
    piece_markers "$LEAF" > "$dst" 2>/dev/null
    bad="$(grep -Ev '^(\[\]|\[x\]|\[X [0-9]+\]|\[[0-9]+\])$' "$dst" | head -n1)"
    [ -z "$bad" ] || fail "the piece grid carries '$bad', outside the four-state marker grammar"
}

# token_at SNAPSHOT IDX — the marker token rendered for piece IDX.
token_at() {
    sed -n "$(($2 + 1))p" "$1"
}

# cached_hits TOKEN — the hit count a cached marker carries (`[x]` is zero).
cached_hits() {
    local token="$1"
    case "$token" in
        "[x]") printf '0\n' ;;
        "[X "*"]") printf '%s\n' "${token:3:${#token}-4}" ;;
        *) return 1 ;;
    esac
}

# root_counter COUNTER — one aggregate counter, read live from the root
# `.stats`.  `format_num` groups thousands with commas, so they are stripped
# before the value reaches arithmetic.
root_counter() {
    case "$1" in
        Hits) sed -n 's/^  Hits: \([0-9,]*\).*/\1/p' "$MNT/.stats" ;;
        Evictions) sed -n 's/^  Hits: .*Evictions: \([0-9,]*\).*/\1/p' "$MNT/.stats" ;;
    esac 2>/dev/null | head -n1 | tr -d ','
}

# settle_grid DST — wait until the published grid stays byte-identical for two
# publish intervals.  A `[]` sample is only "evicted and unwatched" once the
# read's own priority gradient has been released; before that the same piece
# renders `[7]`/`[N]` and the grid is still moving.
settle_grid() {
    local dst="$1" start=$SECONDS last_change
    grid_tokens "$dst"
    last_change=$SECONDS
    while [ $((SECONDS - start)) -lt "$SETTLE_DEADLINE_SECS" ]; do
        sleep 0.25
        grid_tokens "$dst.next"
        if ! cmp -s "$dst" "$dst.next"; then
            mv "$dst.next" "$dst"
            last_change=$SECONDS
            continue
        fi
        rm -f "$dst.next"
        [ $((SECONDS - last_change)) -ge 2 ] && return 0
    done
    return 1
}

# wait_count IDX WANT DST — poll the published grid until piece IDX carries
# exactly WANT hits.  A count above WANT means the read under test produced more
# hits than the aggregate counter reported, which is the same mismatch as too
# few: the marker must follow the counter that recorded the read.
wait_count() {
    local idx="$1" want="$2" dst="$3" token got
    local deadline=$((SECONDS + COUNT_DEADLINE_SECS))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grid_tokens "$dst"
        token="$(token_at "$dst" "$idx")"
        if got="$(cached_hits "$token")"; then
            if [ "$got" -eq "$want" ]; then return 0; fi
            [ "$got" -lt "$want" ] || return 1
        fi
        sleep 0.2
    done
    return 1
}

# read_block DST — read PROBE_LEN bytes of the evicted piece, anchored inside it
# so the range never crosses a piece boundary.
read_block() {
    slice "$FILE" "$PROBE_OFFSET" "$PROBE_LEN" "$1" 2> "$1.err"
}

# ── First mount: ingest and read the whole file (downloads + caches) ────
read_retry "$FILE" "$W/first" 3 || fail "full read over a smaller cache failed"
cmp -s "$W/first" "$SRC" || fail "full read content differs from the source"

EVICTIONS_1="$(root_counter Evictions)"
[ "${EVICTIONS_1:-0}" -gt 0 ] || fail "no eviction was recorded while reading 4 MiB through a 1 MiB cache"
CACHE_AFTER="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "$CACHE_AFTER" -lt $((2 * 1024 * 1024)) ] \
    || fail "cache usage ${CACHE_AFTER}B exceeds the 1 MiB cap"
echo "  after first read: evictions=$EVICTIONS_1 cache=${CACHE_AFTER}B"

# Pick a piece the cap actually evicted: the grid renders that state as `[]`.
settle_grid "$W/markers" \
    || fail "the grid did not settle after the first read: $(tr '\n' ' ' < "$W/markers")"
EVICTED_IDX=""
idx=0
while read -r token; do
    if [ "$token" = "[]" ] && [ -z "$EVICTED_IDX" ]; then EVICTED_IDX="$idx"; fi
    idx=$((idx + 1))
done < "$W/markers"
[ -n "$EVICTED_IDX" ] \
    || fail "no piece was evicted from a 1 MiB cache holding $PIECE_COUNT x $PIECE_LENGTH byte pieces: $(tr '\n' ' ' < "$W/markers")"
echo "  evicted piece index: $EVICTED_IDX (grid: $(tr '\n' ' ' < "$W/markers"))"

# ── Trigger the rebuild with one block read ────────────────────────────
# Whether the re-download completes inside the read or the read is classified as
# a cache stall, the rebuild is what the assertions below sample.
PIECE_OFFSET_BASE=$((EVICTED_IDX * PIECE_LENGTH))
PROBE_OFFSET=$((PIECE_OFFSET_BASE + PROBE_INSET))
rc=0
read_block "$W/probe" || rc=$?
PROBE_RC=$rc
if [ "$rc" = 0 ]; then
    [ "$(stat -c '%s' "$W/probe")" = "$PROBE_LEN" ] || fail "re-read returned the wrong byte count"
    slice "$SRC" "$PROBE_OFFSET" "$PROBE_LEN" "$W/probe.want"
    cmp -s "$W/probe" "$W/probe.want" || fail "re-read bytes differ from the source"
fi

# ── Sample 1: the form right after the rebuild-triggering read ─────────
# The first published state that leaves `[]` is the rebuild's footprint: `[7]`
# while the piece is wanted, `[x]` once its record exists.  A cached form that
# already carries hits at this instant means the rebuild was counted as a hit of
# the record it created, or that the rebuilt record inherited the evicted
# record's count — both are exactly what TC-cache-03 refuses here.
FIRST_FORM=""
DEADLINE=$((SECONDS + REBUILD_DEADLINE_SECS))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    grid_tokens "$W/now"
    TOKEN="$(token_at "$W/now" "$EVICTED_IDX")"
    if [ -n "$TOKEN" ] && [ "$TOKEN" != "[]" ]; then
        FIRST_FORM="$TOKEN"
        break
    fi
    sleep 0.2
done
case "$FIRST_FORM" in
    "[7]"|"[x]") ;;
    "")
        fail "piece $EVICTED_IDX stayed unwatched for ${REBUILD_DEADLINE_SECS}s after the re-read: $(tr '\n' ' ' < "$W/now")"
        ;;
    "[X "*)
        fail "piece $EVICTED_IDX renders '$FIRST_FORM' right after the re-read: the rebuild must start at zero hits, TC-cache-03 accepts only [7] or [x] at this instant"
        ;;
    *)
        fail "piece $EVICTED_IDX renders '$FIRST_FORM' right after the re-read, expected [7] or [x]"
        ;;
esac

# A `[7]` sample means the piece was still downloading; wait for the record to
# exist (`[x]`, zero hits) before reading it.  No read is issued here, so a
# count above zero cannot come from a served read.
BASE_FORM="$FIRST_FORM"
if [ "$FIRST_FORM" = "[7]" ]; then
    while [ "$SECONDS" -lt "$DEADLINE" ]; do
        grid_tokens "$W/now"
        TOKEN="$(token_at "$W/now" "$EVICTED_IDX")"
        case "$TOKEN" in
            "[x]")
                BASE_FORM="[x]"
                break
                ;;
            "[X "*)
                fail "piece $EVICTED_IDX renders '$TOKEN' before any read of the rebuilt record: the rebuild did not start at zero hits"
                ;;
        esac
        sleep 0.2
    done
    [ "$BASE_FORM" = "[x]" ] \
        || fail "piece $EVICTED_IDX never settled into the cache after the re-download: $(tr '\n' ' ' < "$W/now")"
fi
echo "  re-read piece $EVICTED_IDX: first form $FIRST_FORM, settled $BASE_FORM"

# ── Sample 2: one more read of that piece grows the count ──────────────
# The read stays inside the piece, so every hit it produces lands on this piece:
# the count the grid shows afterwards must equal the hits the aggregate counter
# recorded, which is the "增量等于该次读命中该片的次数" clause.
count_after_read() {
    local want="$1" dst="$2" before after delta
    before="$(root_counter Hits)"
    [ -n "$before" ] || fail "root .stats carries no readable Hits counter"
    read_block "$dst" || fail "the read of the rebuilt piece $EVICTED_IDX failed: $(tail -n 2 "$dst.err")"
    [ "$(stat -c '%s' "$dst")" = "$PROBE_LEN" ] || fail "the hit read returned the wrong byte count"
    slice "$SRC" "$PROBE_OFFSET" "$PROBE_LEN" "$dst.want"
    cmp -s "$dst" "$dst.want" || fail "the hit read served bytes that differ from the source"
    after="$(root_counter Hits)"
    delta=$((after - before))
    [ "$delta" -ge 1 ] || fail "the read of the rebuilt piece $EVICTED_IDX recorded no cache hit (Hits $before -> $after)"
    COUNT_BEFORE="$before"
    COUNT_AFTER="$after"
    COUNT_DELTA="$delta"
    want=$((want + delta))
    wait_count "$EVICTED_IDX" "$want" "$dst.grid" \
        || fail "piece $EVICTED_IDX renders '$(token_at "$dst.grid" "$EVICTED_IDX")' after a read that made $delta hit(s), expected [X $want]"
    COUNT_EXPECTED="$want"
}

COUNT_EXPECTED=0
PROBE_OFFSET=$((PIECE_OFFSET_BASE + PROBE_HIT1))
count_after_read "$COUNT_EXPECTED" "$W/hit1"
FIRST_HIT="$(token_at "$W/hit1.grid" "$EVICTED_IDX")"
echo "  first hit read: $BASE_FORM -> $FIRST_HIT (aggregate hits $COUNT_BEFORE -> $COUNT_AFTER, $COUNT_DELTA hit(s))"

PROBE_OFFSET=$((PIECE_OFFSET_BASE + PROBE_HIT2))
count_after_read "$COUNT_EXPECTED" "$W/hit2"
SECOND_HIT="$(token_at "$W/hit2.grid" "$EVICTED_IDX")"
echo "  second hit read: $FIRST_HIT -> $SECOND_HIT (aggregate hits $COUNT_BEFORE -> $COUNT_AFTER, $COUNT_DELTA hit(s))"

# ── The whole piece still serves byte-exact ────────────────────────────
# The hit reads above only prove the counter; the piece itself has to come back
# whole and unchanged, which is what the evicted-then-rebuilt state is for.
rc=0
PIECE_OFFSET=$((EVICTED_IDX * PIECE_LENGTH))
slice "$FILE" "$PIECE_OFFSET" "$PIECE_LENGTH" "$W/second" 2> "$W/reread.err" || rc=$?
[ "$rc" = 0 ] || fail "the whole-piece read of the rebuilt piece $EVICTED_IDX failed: $(tail -n 2 "$W/reread.err")"
[ "$(stat -c '%s' "$W/second")" = "$PIECE_LENGTH" ] || fail "re-read returned the wrong byte count"
slice "$SRC" "$PIECE_OFFSET" "$PIECE_LENGTH" "$W/second.want"
cmp -s "$W/second" "$W/second.want" || fail "re-read bytes differ from the source"

EVICTIONS_2="$(root_counter Evictions)"
[ "${EVICTIONS_2:-0}" -ge "${EVICTIONS_1:-0}" ] || fail "eviction counter moved backwards"
if [ "$PROBE_RC" != 0 ]; then
    # A cache stall is the documented outcome when the evicted piece has no
    # source; AC-32 only requires the piece to be re-marked.
    grep -q "read stalled on the on-disk cache" "$LOG_FILE" \
        || fail "the re-read failed without the documented cache-stall classification"
fi

echo "TC-cache-03: PASS (evictions=$EVICTIONS_1, evicted piece $EVICTED_IDX re-read → $FIRST_FORM → $SECOND_HIT, cache=${CACHE_AFTER}B)"

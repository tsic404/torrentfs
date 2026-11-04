#!/usr/bin/env bash
# Design acceptance case TC-cache-03: with a cache cap below the data size the
# cache evicts, and re-reading an evicted piece re-marks it (`docs/03-acceptance.md`
# §D AC-32).
#
# Container-executable form: released binary; 3.5 MiB cap against a 4 MiB file.
# The cap has to leave room for the pieces one probe read pulls in: a smaller cap
# makes the re-download race its own eviction and the read ends as a cache stall.
#
# The observable is the leaf `.stats` piece grid.  AC-32 is two-sided:
#
#   - the re-download creates the piece's record, and that record starts at zero
#     hits: the first published sample showing the piece cached renders it `[x]`.
#     `[X n]` there would mean the re-download was counted as a hit of the record
#     it created; C-5.3.3 keeps a rebuilt record's count at zero until a read is
#     served from it.
#   - the next hit of that record shows `[X 1]`.
#
# A read re-downloads every piece it covers, so the probe reads an eighth of a
# piece: bytes across a piece boundary, the piece on either side.  The grid is
# published about once a second, so a state is read by polling the marker run.
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

PROBE_LEN=32768
SETTLE_SECS=3
SAMPLE_DEADLINE_SECS=60
REBUILD_DEADLINE_SECS=90

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
cache_size = 3670016
[connections]
listen_interfaces = "127.0.0.1:0"
min_reconnect_time = 1
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"
LEAF="$MNT/data/seed.torrent/.stats"

# grid_tokens DST — snapshot the published marker run, one token per line.
grid_tokens() {
    piece_markers "$LEAF" 2>/dev/null > "$1"
}

# token_at SNAPSHOT IDX — the marker token rendered for piece IDX.
token_at() {
    sed -n "$(($2 + 1))p" "$1"
}

# root_hits — cumulative cache hits for this process, read live.
root_hits() {
    sed -n 's/^  Hits: \([0-9,]*\).*/\1/p' "$MNT/.stats" 2>/dev/null | head -n1 | tr -d ','
}

# settle_grid DST — wait until the published grid stays byte-identical for
# SETTLE_SECS, longer than the publish interval, so the settle is a statement
# about published samples rather than about polling.
settle_grid() {
    local dst="$1" start=$SECONDS last_change
    grid_tokens "$dst"
    last_change=$SECONDS
    while [ $((SECONDS - start)) -lt "$SAMPLE_DEADLINE_SECS" ]; do
        sleep 0.2
        grid_tokens "$dst.next"
        if ! cmp -s "$dst" "$dst.next"; then
            mv "$dst.next" "$dst"
            last_change=$SECONDS
            continue
        fi
        rm -f "$dst.next"
        [ $((SECONDS - last_change)) -ge "$SETTLE_SECS" ] && return 0
    done
    return 1
}

# wait_target_change REFERENCE DST IDX — poll until the target piece renders
# something other than it does in REFERENCE, then leave that sample in DST.
wait_target_change() {
    local ref="$1" dst="$2" idx="$3" want got start=$SECONDS
    want="$(token_at "$ref" "$idx")"
    while [ $((SECONDS - start)) -lt "$SAMPLE_DEADLINE_SECS" ]; do
        grid_tokens "$dst"
        got="$(token_at "$dst" "$idx")"
        if [ -n "$got" ] && [ "$got" != "$want" ]; then return 0; fi
        sleep 0.05
    done
    return 1
}

# ── First mount: ingest and read the whole file (downloads + caches) ────
read_retry "$FILE" "$W/first" 3 || fail "the first read failed"
cmp -s "$W/first" "$SRC" || fail "the first read bytes differ from the source"

EVICTIONS_1="$(sed -n 's/^  Hits: .*Evictions: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "${EVICTIONS_1:-0}" -gt 0 ] || fail "no eviction was recorded while reading 4 MiB through a 3.5 MiB cache"
CACHE_AFTER="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "$CACHE_AFTER" -lt $((4 * 1024 * 1024)) ] \
    || fail "cache usage ${CACHE_AFTER}B exceeds the 3.5 MiB cap"
echo "  after first read: evictions=$EVICTIONS_1 cache=${CACHE_AFTER}B"

# ── Pick an evicted piece once the grid has settled ─────────────────────
# Nothing reads the torrent from here until the probe below, so a settled grid
# is static: the piece picked is still `[]` when the probe reaches it.
settle_grid "$W/quiet" \
    || fail "the grid did not settle after the first read: $(tr '\n' ' ' < "$W/quiet")"
EVICTED_IDX=""
idx=0
while read -r token; do
    if [ "$token" = "[]" ] && [ "$idx" -lt $((PIECE_COUNT - 1)) ]; then
        EVICTED_IDX="$idx"
        break
    fi
    idx=$((idx + 1))
done < "$W/quiet"
[ -n "$EVICTED_IDX" ] \
    || fail "no evicted piece left on a 3.5 MiB cache holding $PIECE_COUNT x $PIECE_LENGTH byte pieces: $(tr '\n' ' ' < "$W/quiet")"
echo "  evicted piece index: $EVICTED_IDX (grid: $(tr '\n' ' ' < "$W/quiet"))"

# ── Re-read the evicted piece's bytes ──────────────────────────────────
# The probe spans the piece boundary by a sixteenth of a piece on either side,
# so the evicted piece is re-downloaded (a new record) rather than served.
OFFSET=$((EVICTED_IDX * PIECE_LENGTH))
PROBE_OFFSET=$((OFFSET + PIECE_LENGTH - PROBE_LEN / 2))
slice "$SRC" "$PROBE_OFFSET" "$PROBE_LEN" "$W/want"

rc=0
slice "$FILE" "$PROBE_OFFSET" "$PROBE_LEN" "$W/second" 2> "$W/reread.err" || rc=$?
if [ "$rc" -eq 0 ]; then
    [ "$(stat -c '%s' "$W/second")" = "$PROBE_LEN" ] || fail "the re-read returned the wrong byte count"
    cmp -s "$W/second" "$W/want" || fail "the re-read bytes differ from the source"
else
    # A read the on-disk cache cannot serve yet ends in the documented cache
    # stall; the piece stays wanted and is downloaded for it afterwards.
    grep -q "read stalled on the on-disk cache" "$LOG_FILE" \
        || fail "the re-read failed without the documented cache-stall classification: $(tail -n 3 "$W/reread.err")"
fi

# AC-32, first half: the record the re-download creates starts at zero hits.
# The rebuild can outlive the read that started it, so wait for the piece to
# appear in the grid; its first cached form must be `[x]`.  No read is issued
# here, so nothing else can raise the count.
DEADLINE=$((SECONDS + REBUILD_DEADLINE_SECS))
REBUILT=""
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    grid_tokens "$W/now"
    token="$(token_at "$W/now" "$EVICTED_IDX")"
    case "$token" in
        "[x]") REBUILT="$token"; break ;;
        "[X "*)
            fail "piece $EVICTED_IDX renders '$token' with no read issued since the re-download: the rebuild was counted as a hit of the record it created"
            ;;
        "") : ;;
        "[7]"|"[]"|"[N]"*) : ;;
        *) fail "piece $EVICTED_IDX renders '$token'" ;;
    esac
    sleep 0.25
done
[ -n "$REBUILT" ] \
    || fail "piece $EVICTED_IDX never reached the cache after the re-download: $(tr '\n' ' ' < "$W/now")"
echo "  rebuilt piece $EVICTED_IDX: $REBUILT (zero hits on the new record)"

# AC-32, second half: the next hit of that record shows [X 1].
HITS_BEFORE="$(root_hits)"
rc=0
slice "$FILE" "$PROBE_OFFSET" "$PROBE_LEN" "$W/third" 2> "$W/third.err" || rc=$?
[ "$rc" -eq 0 ] || fail "the second read of the probe range failed (rc=$rc): $(tail -n 3 "$W/third.err")"
cmp -s "$W/third" "$W/want" || fail "the second read of the range served different bytes"
wait_target_change "$W/now" "$W/next" "$EVICTED_IDX" \
    || fail "the probe's second read produced no hit on piece $EVICTED_IDX in ${SAMPLE_DEADLINE_SECS}s"
HIT_MARKER="$(token_at "$W/next" "$EVICTED_IDX")"
[ "$HIT_MARKER" = "[X 1]" ] \
    || fail "piece $EVICTED_IDX renders '$HIT_MARKER' after one hit on the rebuilt record, expected [X 1]"
HITS_AFTER="$(root_hits)"
echo "  next hit: $HIT_MARKER (root hits ${HITS_BEFORE:-unreadable} -> ${HITS_AFTER:-unreadable})"

EVICTIONS_2="$(sed -n 's/^  Hits: .*Evictions: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "${EVICTIONS_2:-0}" -ge "${EVICTIONS_1:-0}" ] || fail "eviction counter moved backwards"

echo "TC-cache-03: PASS (evictions=$EVICTIONS_1, evicted piece $EVICTED_IDX re-read → $REBUILT → $HIT_MARKER, cache=${CACHE_AFTER}B)"

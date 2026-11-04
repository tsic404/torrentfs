#!/usr/bin/env bash
# Design acceptance case TC-cache-03: with a cache cap below the data size the
# cache evicts, and re-reading an evicted piece re-marks it as cached or wanted
# (`docs/03-acceptance.md` §D AC-32).
#
# Container-executable form: released binary; 1 MiB cap against a 4 MiB file.
# The observable is the leaf `.stats` piece grid plus the root eviction counter:
# an evicted piece carries `[]` before the re-read and `[X n]` / `[x]` / `[7]`
# after it (AC-32's "重新提升/下载").
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
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"
LEAF="$MNT/data/seed.torrent/.stats"

read_retry "$FILE" "$W/first" 3 || fail "full read over a smaller cache failed"
cmp -s "$W/first" "$SRC" || fail "full read content differs from the source"

EVICTIONS_1="$(sed -n 's/^  Hits: .*Evictions: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "${EVICTIONS_1:-0}" -gt 0 ] || fail "no eviction was recorded while reading 4 MiB through a 1 MiB cache"
CACHE_AFTER="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "$CACHE_AFTER" -lt $((2 * 1024 * 1024)) ] \
    || fail "cache usage ${CACHE_AFTER}B exceeds the 1 MiB cap"
echo "  after first read: evictions=$EVICTIONS_1 cache=${CACHE_AFTER}B"

# Pick a piece the cap actually evicted: the grid renders that state as `[]`.
piece_markers "$LEAF" > "$W/markers"
EVICTED_IDX=""
idx=0
while read -r token; do
    if [ "$token" = "[]" ] && [ -z "$EVICTED_IDX" ]; then EVICTED_IDX="$idx"; fi
    idx=$((idx + 1))
done < "$W/markers"
[ -n "$EVICTED_IDX" ] || fail "no piece was evicted from a 1 MiB cache holding 16 x 256 KiB pieces"
echo "  evicted piece index: $EVICTED_IDX (grid: $(tr '\n' ' ' < "$W/markers"))"

# Re-read that piece's bytes.  Whether the re-download completes or the read is
# classified as a cache stall, AC-32's observable is the marker afterwards.
OFFSET=$((EVICTED_IDX * PIECE_LENGTH))
rc=0
slice "$FILE" "$OFFSET" "$PIECE_LENGTH" "$W/second" 2> "$W/reread.err" || rc=$?
if [ "$rc" = 0 ]; then
    [ "$(stat -c '%s' "$W/second")" = "$PIECE_LENGTH" ] || fail "re-read returned the wrong byte count"
    slice "$SRC" "$OFFSET" "$PIECE_LENGTH" "$W/second.want"
    cmp -s "$W/second" "$W/second.want" || fail "re-read bytes differ from the source"
fi

MARKER=""
for _ in $(seq 1 40); do
    piece_markers "$LEAF" > "$W/markers"
    MARKER="$(sed -n "$((EVICTED_IDX + 1))p" "$W/markers")"
    case "$MARKER" in
        "[x]"|"[X "*) break ;;
        "[7]") break ;;
    esac
    sleep 0.5
done
case "$MARKER" in
    "[x]"|"[X "*) ;;
    "[7]") ;;
    *) fail "re-read piece $EVICTED_IDX carries '$MARKER', expected a cached or wanted marker" ;;
esac

EVICTIONS_2="$(sed -n 's/^  Hits: .*Evictions: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "${EVICTIONS_2:-0}" -ge "${EVICTIONS_1:-0}" ] || fail "eviction counter moved backwards"
if [ "$rc" != 0 ]; then
    # A cache stall is the documented outcome when the evicted piece has no
    # source; AC-32 only requires the piece to be re-marked.
    grep -q "read stalled on the on-disk cache" "$LOG_FILE" \
        || fail "the re-read failed without the documented cache-stall classification"
fi

echo "TC-cache-03: PASS (evictions=$EVICTIONS_1, evicted piece $EVICTED_IDX re-read → marker $MARKER, cache=${CACHE_AFTER}B)"

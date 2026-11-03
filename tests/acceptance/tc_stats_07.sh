#!/usr/bin/env bash
# Design acceptance case TC-stats-07: a parked read publishes a descending
# prefetch gradient, everything outside the access window stays not-wanted, and
# the whole file is never prefetched (`docs/03-acceptance.md` §E AC-46).
#
# Container-executable form: released binary with a seeded-but-stalled peer, so
# the read stays parked and its priority ladder is observable; the access
# window is capped at 2 MiB (8 pieces) so both the four steps and the window
# edge are inside the 4 MiB file and the boundary is observable.
#
# Usage: ./tests/acceptance/tc_stats_07.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats07}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-07

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --stall
wait_peer_known "$INFOHASH" 1 || fail "stalled peer never registered with the tracker"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 30
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = 3
[piece_priority]
access_window_mb = 2
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
LEAF="$MNT/data/seed.torrent/.stats"
FILE="$MNT/data/seed.torrent/seed.bin"

# Park a one-byte read: it covers piece 0 only and never completes, because the
# seeder unchokes and never delivers.
( dd if="$FILE" of="$W/byte" bs=1 count=1 status=none 2>/dev/null ) &
READER=$!
trap 'kill "$READER" 2>/dev/null || true; env_cleanup' EXIT

GRADIENT=""
for _ in $(seq 1 40); do
    piece_markers "$LEAF" > "$W/markers"
    if [ -s "$W/markers" ] && [ "$(head -n1 "$W/markers")" = "[7]" ]; then
        GRADIENT=1
        break
    fi
    sleep 0.25
done
[ "$GRADIENT" = 1 ] || fail "the read never published a [7] priority for its own piece"

mapfile -t MARKS < "$W/markers"
[ "${MARKS[0]}" = "[7]" ] || fail "read-range piece is ${MARKS[0]}, expected [7]"

# marker_value TOKEN IDX — the positive priority inside `[N]`, or a failure.
marker_value() {
    local token="$1" idx="$2" value
    value="${token#\[}"
    value="${value%\]}"
    case "$value" in
        ''|*[!0-9]*) fail "marker at position $idx is '$token', expected a positive integer" ;;
    esac
    printf '%s\n' "$value"
}

# The four step priorities descend after the read range.
PREV="$(marker_value "${MARKS[0]}" 0)"
for idx in 1 2 3 4; do
    value="$(marker_value "${MARKS[$idx]}" "$idx")"
    [ "$value" -lt "$PREV" ] || fail "step markers do not descend: position $idx is $value after $PREV"
    PREV="$value"
done
STEP_LAST="$PREV"

# 2 MiB / 256 KiB = an 8-piece window: after the four steps the remaining
# window pieces sit at the edge priority.  They must be wanted (not `[]`),
# all equal, and below the last step — collapsing the edge to "not wanted"
# would make this indistinguishable from the region outside the window.
EDGE="${MARKS[5]}"
for idx in 5 6 7 8; do
    [ "${MARKS[$idx]}" = "$EDGE" ] \
        || fail "window-edge marker at position $idx is ${MARKS[$idx]}, expected ${EDGE} on every window-edge piece"
done
EDGE_VALUE="$(marker_value "$EDGE" 5)"
[ "$EDGE_VALUE" -lt "$STEP_LAST" ] \
    || fail "window-edge priority $EDGE_VALUE is not below the last step $STEP_LAST"

# Everything past the window is outside it: not wanted at all, so the file is
# not prefetched wholesale.
OUTSIDE=0
for idx in "${!MARKS[@]}"; do
    [ "$idx" -le 8 ] && continue
    [ "${MARKS[$idx]}" = "[]" ] || fail "marker at position $idx is ${MARKS[$idx]}, expected [] outside the window"
    OUTSIDE=$((OUTSIDE + 1))
done
[ "$OUTSIDE" -gt 0 ] || fail "no piece lies outside the access window; the boundary was never observed"
[ "$OUTSIDE" -eq $((PIECE_COUNT - 9)) ] || fail "$OUTSIDE pieces outside the window, expected $((PIECE_COUNT - 9))"

echo "  gradient: $(tr '\n' ' ' < "$W/markers")"
kill "$READER" 2>/dev/null || true

echo "TC-stats-07: PASS (piece 0 = [7], steps ${MARKS[1]}${MARKS[2]}${MARKS[3]}${MARKS[4]}, window edge ${EDGE}×4, ${OUTSIDE} pieces not wanted — no whole-file prefetch)"

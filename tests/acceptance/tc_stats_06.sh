#!/usr/bin/env bash
# Design acceptance case TC-stats-06: the Pieces block renders the four marker
# states only, its marker count equals the metadata piece count, and the piece
# geometry lines match the metadata (`docs/03-acceptance.md` §E AC-45).
#
# Container-executable form: released binary; one scenario serves from a real
# seeder (cached-and-visited / cached-unvisited), one parks on a stalled seeder
# (wanted / not wanted).
#
# Usage: ./tests/acceptance/tc_stats_06.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats06}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-06

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
TORRENT_SRC="$TORRENT"

# render_bytes BYTES — the `.stats` byte rendering (format_bytes in stats.rs):
# GB/MB/KB with two decimals above their unit, plain bytes below 1 KiB.
render_bytes() {
    awk -v b="$1" 'BEGIN {
        if (b >= 1073741824) printf "%.2f GB", b / 1073741824;
        else if (b >= 1048576) printf "%.2f MB", b / 1048576;
        else if (b >= 1024) printf "%.2f KB", b / 1024;
        else printf "%d B", b;
    }'
}

# check_grammar STATS_FILE: every token is one of [x] / [X n] / [N] / [], the
# count equals the declared piece count, and the geometry lines are present.
check_grammar() {
    local stats="$1" declared="$2"
    piece_markers "$stats" > "$W/markers"
    local count
    count="$(wc -l < "$W/markers")"
    [ "$count" = "$declared" ] || fail "$stats carries $count markers, metadata declares $declared"
    local token
    while read -r token; do
        case "$token" in
            "[x]"|"[]") ;;
            "[X "*) printf '%s' "$token" | grep -qE '^\[X [0-9]+\]$' || fail "bad cached marker: $token" ;;
            # `[N]` is a positive priority only: `[0]` is rendered as `[]`
            # (C-6.3.1), so accepting it would let a violating output pass.
            *) printf '%s' "$token" | grep -qE '^\[[1-9][0-9]*\]$' || fail "bad marker token: $token" ;;
        esac
    done < "$W/markers"
    # The geometry lines must match the metadata this torrent declares, not a
    # value written into the carrier.
    grep -q "^  PieceSize: $(render_bytes "$PIECE_LENGTH")$" "$stats" \
        || fail "$stats lacks the PieceSize line for a $PIECE_LENGTH-byte piece"
    grep -q "^  PieceCount: $declared$" "$stats" || fail "$stats lacks the PieceCount line"
}

# ── Scenario A: a real seeder → [X n] and [x] ────────────────────────────
MNT="$W/mnt-served"; CACHE_DIR="$W/cache-served"; DB_FILE="$W/db-served.db"; LOG_FILE="$W/log-served"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = 3
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
LEAF="$MNT/data/seed.torrent/.stats"
slice "$MNT/data/seed.torrent/seed.bin" 0 65536 "$W/part"

SEEN_CACHED=0
SEEN_UNVISITED=0
for _ in $(seq 1 40); do
    piece_markers "$LEAF" > "$W/markers"
    grep -qE '^\[X [0-9]+\]$' "$W/markers" && SEEN_CACHED=1
    grep -qx '\[x\]' "$W/markers" && SEEN_UNVISITED=1
    [ "$SEEN_CACHED" = 1 ] && [ "$SEEN_UNVISITED" = 1 ] && break
    sleep 0.5
done
check_grammar "$LEAF" "$PIECE_COUNT"
[ "$SEEN_CACHED" = 1 ] || fail "no cached-and-visited [X n] marker appeared after a read"
[ "$SEEN_UNVISITED" = 1 ] || fail "no cached-unvisited [x] marker appeared for prefetched pieces"
echo "  served: $(tr '\n' ' ' < "$W/markers")"
daemon_stop

# ── Scenario B: a stalled seeder → [N] and [] ────────────────────────────
MNT="$W/mnt-stall"; CACHE_DIR="$W/cache-stall"; DB_FILE="$W/db-stall.db"; LOG_FILE="$W/log-stall"
peer_stop
: > "$TRACKER_STATE"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = 3
[piece_priority]
access_window_mb = 1
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --stall
wait_peer_known "$INFOHASH" 1 || fail "stalled peer never registered with the tracker"
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
LEAF="$MNT/data/seed.torrent/.stats"
slice "$MNT/data/seed.torrent/seed.bin" 0 1 "$W/byte"

SEEN_WANTED=0
SEEN_NOT_WANTED=0
for _ in $(seq 1 40); do
    piece_markers "$LEAF" > "$W/markers"
    grep -qE '^\[[1-9][0-9]*\]$' "$W/markers" && SEEN_WANTED=1
    grep -qx '\[\]' "$W/markers" && SEEN_NOT_WANTED=1
    [ "$SEEN_WANTED" = 1 ] && [ "$SEEN_NOT_WANTED" = 1 ] && break
    sleep 0.5
done
check_grammar "$LEAF" "$PIECE_COUNT"
[ "$SEEN_WANTED" = 1 ] || fail "no wanted-not-cached [N] marker appeared for the parked read's pieces"
[ "$SEEN_NOT_WANTED" = 1 ] || fail "no [ ] marker appeared outside the access window"
echo "  stall: $(tr '\n' ' ' < "$W/markers")"

echo "TC-stats-06: PASS (all four marker states observed; both runs match ^\\[(x|X n|N|\\])\\]$ with $PIECE_COUNT markers)"

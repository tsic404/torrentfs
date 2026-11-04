#!/usr/bin/env bash
# Design acceptance case TC-negative-04: F7 — the published `.stats` format
# still satisfies the existing consumers: the strict marker enumeration from
# `tests/acceptance/stats_support.rs`, the field shape `ci/tests/slow_swarm_e2e.sh`
# samples, and the section list the root-surface assertions pin
# (`docs/03-acceptance.md` §J AC-93).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_negative_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-04
write_fast_config

SRC="$(payload_patterned payload.bin 262144)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 65536 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
mkdir -p "$MNT/metadata/sub"
ingest "$TORRENT" "$MNT/metadata/sub/seed.torrent" >/dev/null
read_retry "$MNT/data/sub/seed.torrent/seed.bin" "$W/read" 3 || fail "read failed"

LEAF="$MNT/data/sub/seed.torrent/.stats"
ROOT_STATS="$MNT/.stats"
DIR_STATS="$MNT/data/sub/.stats"

# 1. Exactly one Pieces line and the strict four-state grammar.
RUNS="$(grep -c '^  Pieces: ' "$LEAF")"
[ "$RUNS" = 1 ] || fail "leaf .stats carries $RUNS 'Pieces:' lines, expected exactly 1"
piece_markers "$LEAF" > "$W/markers"
[ -s "$W/markers" ] || fail "the Pieces marker run is empty"
BAD="$(grep -cvE '^(\[\]|\[x\]|\[[1-9][0-9]*\]|\[X [1-9][0-9]*\])$' "$W/markers" || true)"
[ "${BAD:-0}" = 0 ] || { grep -vE '^(\[\]|\[x\]|\[[1-9][0-9]*\]|\[X [1-9][0-9]*\])$' "$W/markers" >&2; fail "marker run outside the four-state enumeration"; }
COUNT="$(wc -l < "$W/markers")"
DECLARED="$(sed -n 's/^  PieceCount: \([0-9]*\)$/\1/p' "$LEAF" | head -n1)"
[ "$COUNT" = "$DECLARED" ] || fail "$COUNT markers vs PieceCount $DECLARED"
grep -qE '^  PieceSize: [0-9.]+ (B|KB|MB|GB)$' "$LEAF" || fail "PieceSize line missing or malformed"

# 2. The leaf fields the slow-swarm consumer samples.
grep -qE '^  Peers: [0-9]+  Seeds: [0-9]+$' "$LEAF" || fail "Peers/Seeds line malformed"
grep -qE '^  Rate: ↓ [0-9.]+ (B|KB|MB|GB)/s  ↑ [0-9.]+ (B|KB|MB|GB)/s$' "$LEAF" \
    || fail "Rate line malformed"
grep -qE '^  Waiting: (yes|no)$' "$LEAF" || fail "Waiting line malformed"
grep -qE '^Cache: [0-9]+ pieces  [0-9.]+ (B|KB|MB|GB)  max=[0-9]+$' "$LEAF" \
    || fail "leaf Cache line malformed"

# 3. The root surface's section list and the directory summary.
for seg in Overview "Global Rates" Connections Torrents Cache; do
    grep -q -- "-- $seg --" "$ROOT_STATS" || fail "root .stats is missing the $seg section"
done
grep -qE '^  Torrents: [0-9]+  Total Size: [0-9.]+ (B|KB|MB|GB)  Downloaded: [0-9.]+ (B|KB|MB|GB)$' "$DIR_STATS" \
    || fail "directory summary line malformed"
grep -qE '^  Hits: [0-9,]+  Misses: [0-9,]+  Hit Rate: [0-9.]+%  Evictions: [0-9]+$' "$ROOT_STATS" \
    || fail "root Cache counters line malformed"

# 4. No placeholder text leaked into a published surface.
if grep -q '\[info_hash\]' "$LEAF" "$ROOT_STATS" "$DIR_STATS"; then
    fail "a published .stats still carries the literal [info_hash] placeholder"
fi

echo "TC-negative-04: PASS (marker enumeration strict, Peers/Seeds/Rate/Waiting/Cache lines parse, root sections and directory summary intact)"

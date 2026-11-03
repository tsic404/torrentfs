#!/usr/bin/env bash
# Design acceptance case TC-recovery-04: the debug log stays concise during
# reads — a read summary line, no per-piece flooding — and the abnormal path
# still warns (`docs/03-acceptance.md` §I AC-83).
#
# Container-executable form: released binary, synthesized local swarm, debug
# logging into a file.
#
# Usage: ./tests/acceptance/tc_recovery_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_recovery04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-recovery-04
write_fast_config

SRC="$(payload_patterned payload.bin 262144)"
tracker_start
build_torrent "$TORRENT_FILES/served.torrent" --name served.bin --content "$SRC" \
    --piece-length 65536 --tracker "$TRACKER_URL"
SERVED_TORRENT="$TORRENT"
SERVED_HASH="$INFOHASH"
SERVED_PIECE="$PIECE_LENGTH"
build_torrent "$TORRENT_FILES/lonely.torrent" --name lonely.bin \
    --content "$(payload_patterned lonely.bin 65536)" --piece-length 16384
LONELY_TORRENT="$TORRENT"
peer_start "$SERVED_HASH" "$SERVED_PIECE" "$SRC"
wait_peer_known "$SERVED_HASH" 1 || fail "peer never registered with the tracker"

DEBUG_LOG="$W/debug.log"
: > "$DEBUG_LOG"
daemon_start --log-level debug --log-file "$DEBUG_LOG"
ingest "$SERVED_TORRENT" >/dev/null
ingest "$LONELY_TORRENT" >/dev/null
read_retry "$MNT/data/served.torrent/served.bin" "$W/read" 3 || fail "read failed"

grep -qE "Read [0-9]+ bytes from torrent file" "$DEBUG_LOG" \
    || fail "the debug log carries no read summary line"
# The repository's per-piece lines spell the index as `piece={}` (alert.rs) or
# `piece {}` (engine.rs), so both forms count as flooding.
SPAM="$(grep -cE 'piece[= ][0-9]+' "$DEBUG_LOG" || true)"
[ "${SPAM:-0}" = 0 ] || {
    grep -nE 'piece[= ][0-9]+' "$DEBUG_LOG" | head -5 >&2
    fail "the debug log carries $SPAM per-piece lines during a 4-piece read"
}
TOTAL="$(wc -l < "$DEBUG_LOG")"
[ "$TOTAL" -lt 200 ] || fail "the debug log grew to $TOTAL lines for a 4-piece read"

# Abnormal path: a torrent with no source must warn, not stay silent.
rc=0
slice "$MNT/data/lonely.torrent/lonely.bin" 0 1024 "$W/lonely.out" 2>/dev/null || rc=$?
[ "$rc" -ne 0 ] || fail "a sourceless read succeeded"
grep -q "Failed to read torrent file data" "$DEBUG_LOG" \
    || fail "the failed read left no warning in the debug log"

echo "TC-recovery-04: PASS (read summary present, 0 per-piece lines, $TOTAL total lines, failure warned)"

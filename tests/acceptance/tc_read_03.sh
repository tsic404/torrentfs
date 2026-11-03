#!/usr/bin/env bash
# Design acceptance case TC-read-03: a read at or past the end of file is a
# clean EOF — 0 bytes, exit 0, no errno (`docs/03-acceptance.md` §B AC-12).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_read_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-03
write_fast_config

SRC="$(payload_patterned payload.bin 1048576)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"
SIZE="$(stat -c '%s' "$FILE")"
[ "$SIZE" = "$TOTAL_SIZE" ] || fail "stat size=$SIZE, metadata declares $TOTAL_SIZE"

# Warm the file so the tail/EOF reads land on a served file, not a cold one.
read_retry "$FILE" "$W/full" 3 || fail "warm-up read failed"

for off in "$SIZE" "$((SIZE + 4096))"; do
    rc=0
    slice "$FILE" "$off" 64 "$W/eof.out" || rc=$?
    [ "$rc" -eq 0 ] || fail "read at offset $off exited $rc, expected 0"
    GOT="$(stat -c '%s' "$W/eof.out")"
    [ "$GOT" = 0 ] || fail "read at offset $off returned $GOT bytes, expected 0 (clean EOF)"
done

if grep -qE 'ERROR|panicked' "$LOG_FILE"; then
    grep -E 'ERROR|panicked' "$LOG_FILE" >&2
    fail "daemon logged an error while reading past EOF"
fi

echo "TC-read-03: PASS (offsets $SIZE and $((SIZE + 4096)) returned 0 bytes, exit 0, no daemon error)"

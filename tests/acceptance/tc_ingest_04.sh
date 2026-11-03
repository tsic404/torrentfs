#!/usr/bin/env bash
# Design acceptance case TC-ingest-04: moving a torrent between `metadata/`
# subtrees moves its projection, and a move into a missing directory fails with
# ENOENT leaving the projection untouched (`docs/03-acceptance.md` §A AC-04).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_ingest_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-ingest-04
write_fast_config

tracker_start
build_torrent "$TORRENT_FILES/t.torrent" --name t.bin \
    --content "$(payload_patterned t.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
mkdir -p "$MNT/metadata/d1" "$MNT/metadata/d2"
ingest "$TORRENT" "$MNT/metadata/d1/t.torrent" >/dev/null

# Successful cross-directory move: exit 0, old projection gone, new one there.
mv "$MNT/metadata/d1/t.torrent" "$MNT/metadata/d2/t.torrent" \
    || fail "cross-directory move exited non-zero"
wait_path "$MNT/data/d2/t.torrent" 60 || fail "projection did not move to data/d2/t.torrent"
[ -e "$MNT/data/d1/t.torrent" ] && fail "old projection data/d1/t.torrent still exists after the move"
[ -f "$MNT/data/d2/t.torrent/t.bin" ] || fail "moved projection lost its content"
SIZE_BEFORE="$(stat -c '%s' "$MNT/data/d2/t.torrent/t.bin")"

# Move into a directory that does not exist: ENOENT, projection unchanged.
rc=0
mv "$MNT/metadata/d2/t.torrent" "$MNT/metadata/missing/t.torrent" 2> "$W/mv.err" || rc=$?
[ "$rc" -ne 0 ] || fail "move into a missing directory exited 0"
grep -q "No such file or directory" "$W/mv.err" \
    || { cat "$W/mv.err" >&2; fail "failed move did not report ENOENT"; }
[ -f "$MNT/metadata/d2/t.torrent" ] || fail "failed move removed the source"
[ -f "$MNT/data/d2/t.torrent/t.bin" ] || fail "failed move changed the projection"
[ "$(stat -c '%s' "$MNT/data/d2/t.torrent/t.bin")" = "$SIZE_BEFORE" ] || fail "failed move changed the projected size"

echo "TC-ingest-04: PASS (move d1→d2 moved the projection; move into a missing dir returned ENOENT with the projection intact)"

#!/usr/bin/env bash
# Design acceptance case TC-read-06: five concurrent reads of one file all
# return the same digest and the stat size (`docs/03-acceptance.md` §B AC-14).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_read_06.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read06}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-06
write_fast_config

SRC="$(payload_patterned payload.bin 2097152)"
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

# All five start together, so they share one download through the read
# coalescing path rather than serialising against a warm cache.
READERS=()
for i in 1 2 3 4 5; do
    ( cat "$FILE" > "$W/out.$i" 2>"$W/err.$i"; echo $? > "$W/rc.$i" ) &
    READERS+=($!)
done
for pid in "${READERS[@]}"; do wait "$pid"; done

for i in 1 2 3 4 5; do
    rc="$(cat "$W/rc.$i")"
    [ "$rc" = 0 ] || { cat "$W/err.$i" >&2; fail "reader $i exited $rc"; }
    got="$(stat -c '%s' "$W/out.$i")"
    [ "$got" = "$SIZE" ] || fail "reader $i returned $got bytes, stat says $SIZE"
done

WANT="$(digest "$SRC")"
for i in 1 2 3 4 5; do
    got="$(digest "$W/out.$i")"
    [ "$got" = "$WANT" ] || fail "reader $i digest $got differs from source $WANT"
done

echo "TC-read-06: PASS (5 concurrent readers, all $SIZE bytes, sha256=$WANT)"

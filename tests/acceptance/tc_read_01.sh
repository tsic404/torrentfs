#!/usr/bin/env bash
# Design acceptance case TC-read-01: a full read returns the torrent's declared
# size and bytes identical to the source (`docs/03-acceptance.md` §B AC-10).
#
# Container-executable form: drives the released binary
# (/usr/local/bin/torrentfs) against a synthesized local swarm; no build step.
#
# Usage: ./tests/acceptance/tc_read_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-01
write_fast_config

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"

DECLARED="$(stat -c '%s' "$FILE")"
[ "$DECLARED" = "$TOTAL_SIZE" ] || fail "stat size=$DECLARED, metadata declares $TOTAL_SIZE"

OK=0
for _ in 1 2 3; do
    if cat "$FILE" > "$W/read.out" 2>"$W/read.err"; then OK=1; break; fi
    sleep 1
done
[ "$OK" = 1 ] || { cat "$W/read.err" >&2; fail "full read failed"; }

GOT="$(stat -c '%s' "$W/read.out")"
[ "$GOT" = "$DECLARED" ] || fail "read returned $GOT bytes, stat says $DECLARED"
SRC_SUM="$(sha256sum < "$SRC" | cut -d' ' -f1)"
OUT_SUM="$(sha256sum < "$W/read.out" | cut -d' ' -f1)"
[ "$SRC_SUM" = "$OUT_SUM" ] || fail "content digest differs from the source"

echo "TC-read-01: PASS (size=$GOT=stat, sha256=$OUT_SUM=source, pieces served=$(peer_served))"

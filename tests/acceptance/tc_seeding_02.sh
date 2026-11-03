#!/usr/bin/env bash
# Design acceptance case TC-seeding-02: after the torrent is copied in — before
# any file is read — the peer/seeder counts are above zero, and the first read
# switches the torrent into download and converges to cached pieces
# (`docs/03-acceptance.md` §F AC-51).
#
# Container-executable form: released binary, local tracker + fixture peer.
#
# Usage: ./tests/acceptance/tc_seeding_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_seeding02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-seeding-02
write_fast_config

SRC="$(payload_patterned payload.bin 524288)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 65536 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
ingest "$TORRENT" >/dev/null
LEAF="$MNT/data/seed.torrent/.stats"

# Before any read: the lightweight handle already announced, so the swarm is
# visible in the stats surface.
SEEDS=0
for _ in $(seq 1 60); do
    SEEDS="$(sed -n 's/^  Peers: [0-9]*  Seeds: \([0-9]*\).*/\1/p' "$LEAF" | head -n1)"
    [ "${SEEDS:-0}" -ge 1 ] && break
    sleep 0.25
done
[ "${SEEDS:-0}" -ge 1 ] || fail "seeder count was ${SEEDS:-0} before the first read, expected > 0"
echo "  before read: Seeds=$SEEDS, $(sed -n 's/^  \(Pieces: .*\)$/\1/p' "$LEAF" | head -n1)"

read_retry "$MNT/data/seed.torrent/seed.bin" "$W/read" 3 || fail "first read did not trigger the download"
cmp -s "$W/read" "$SRC" || fail "first read returned the wrong bytes"

CONVERGED=0
for _ in $(seq 1 40); do
    piece_markers "$LEAF" > "$W/markers"
    if [ -s "$W/markers" ] && ! grep -qvE '^\[(x|X [0-9]+)\]$' "$W/markers"; then
        CONVERGED=1
        break
    fi
    sleep 0.5
done
[ "$CONVERGED" = 1 ] || {
    echo "  markers: $(tr '\n' ' ' < "$W/markers")" >&2
    fail "pieces did not converge to the cached state after the read"
}

echo "TC-seeding-02: PASS (announce visible before the read: Seeds=$SEEDS; after the read all $PIECE_COUNT pieces cached)"

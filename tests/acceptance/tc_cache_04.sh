#!/usr/bin/env bash
# Design acceptance case TC-cache-04: deleting a torrent removes its cache
# directory and the cache metadata converges on the next poll window
# (`docs/03-acceptance.md` §D AC-33).
#
# Container-executable form: released binary; the on-disk cache layout is
# observed directly (`pieces/<info_hash>/` plus `cache_metadata.txt`).
#
# Usage: ./tests/acceptance/tc_cache_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-cache-04
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
read_retry "$MNT/data/seed.torrent/seed.bin" "$W/read" 3 || fail "cold read failed"

PIECE_DIR="$CACHE_DIR/pieces/$INFOHASH"
[ -d "$PIECE_DIR" ] || fail "no cache directory for $INFOHASH after the read"
[ -n "$(ls -A "$PIECE_DIR")" ] || fail "cache directory for $INFOHASH holds no pieces"
echo "  cached: $(ls -A "$PIECE_DIR" | wc -l) piece files under pieces/$INFOHASH"

rm "$MNT/metadata/seed.torrent"
# C-5.2.4: the on-disk metadata is a lazy view during the daemon's lifetime, so
# the criterion allows a >=5s poll window.
CONVERGED=0
for _ in $(seq 1 40); do
    if [ ! -d "$PIECE_DIR" ]; then CONVERGED=1; break; fi
    sleep 0.25
done
[ -d "$MNT/data/seed.torrent" ] && fail "projection data/seed.torrent still present after the delete"
[ "$CONVERGED" = 1 ] || fail "cache directory pieces/$INFOHASH was never removed after the torrent was deleted"
echo "  cache directory removed while the daemon was running"

daemon_stop
# After the daemon stops, the metadata file is authoritative: no line may still
# reference the removed info hash.
META="$CACHE_DIR/cache_metadata.txt"
[ -f "$META" ] || fail "cache metadata file missing after shutdown"
if grep -q "$INFOHASH" "$META"; then
    fail "cache metadata still references $INFOHASH after shutdown"
fi

echo "TC-cache-04: PASS (pieces/$INFOHASH removed within the poll window, metadata converged to 0, projection gone)"

#!/usr/bin/env bash
# Design acceptance case TC-negative-02: F2/F3 — directory listing does not
# depend on an O(directory) scan and never waits on the network; a large
# directory lists in seconds and a listing completes while a read is parked
# (`docs/03-acceptance.md` §J AC-91).
#
# Container-executable form: released binary; a 5000-entry directory plus a
# stalled peer holding a read in flight.
#
# Usage: ./tests/acceptance/tc_negative_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-02

COUNT=5000
SRC="$(payload_patterned slow.bin 1048576)"
tracker_start
build_torrent "$TORRENT_FILES/bulk.torrent" --name payload --bulk "$COUNT" \
    --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/bulk-src" \
    --piece-length 65536 --tracker "$TRACKER_URL"
BULK_TORRENT="$TORRENT"
build_torrent "$TORRENT_FILES/slow.torrent" --name slow.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
SLOW_TORRENT="$TORRENT"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 25
peer_discovery_wait_secs = 10
no_seeder_read_timeout_secs = 3
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --stall
wait_peer_known "$INFOHASH" 1 || fail "stalled peer never registered with the tracker"

daemon_start
ingest "$BULK_TORRENT" >/dev/null
ingest "$SLOW_TORRENT" >/dev/null
BULK_DIR="$MNT/data/bulk.torrent/payload/bulk"
wait_path "$BULK_DIR/04999.bin" 120 || fail "payload/bulk projection did not finish publishing"

# By-name resolution on a name this run never looked up: `wait_path` resolved
# 04999.bin, and FUSE entry caching (1s TTL) plus the dentry cache would answer
# a repeat of that name from the kernel instead of the daemon.  02500.bin was
# never resolved, so this stat has to reach the lookup path.
start=$(date +%s%N)
stat "$BULK_DIR/02500.bin" > /dev/null || fail "02500.bin could not be resolved by name"
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$MS" -lt 2000 ] || fail "resolving an entry by name took ${MS}ms, expected sub-second"
echo "  lookup 02500.bin (never looked up before): ${MS}ms"

start=$(date +%s%N)
ENTRIES="$(ls -U "$BULK_DIR" | wc -l)"
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$ENTRIES" = "$COUNT" ] || fail "listing returned $ENTRIES entries, expected $COUNT"
[ "$MS" -lt 5000 ] || fail "listing $COUNT entries took ${MS}ms, expected seconds"
echo "  list $COUNT entries: ${MS}ms"

# Park a read on the stalled seeder, then list the large directory while it is
# in flight: the listing must not wait behind the read.
( slice "$MNT/data/slow.torrent/slow.bin" 0 1024 "$W/parked.out" 2>/dev/null; echo $? > "$W/parked.rc" ) &
READER=$!
sleep 2
kill -0 "$READER" 2>/dev/null || fail "the read did not stay parked (stalled peer should hold it)"

start=$(date +%s%N)
DURING="$(ls -U "$BULK_DIR" | wc -l)"
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$DURING" = "$COUNT" ] || fail "concurrent listing returned $DURING entries, expected $COUNT"
[ "$MS" -lt 5000 ] || fail "listing during a parked read took ${MS}ms, expected seconds"
echo "  list during parked read: ${MS}ms"

# And a recursive listing of the same tree is equally prompt.
start=$(date +%s%N)
find "$MNT/data/bulk.torrent" -type f ! -name '.stats' -printf '%P\n' > "$W/tree.out"
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$(wc -l < "$W/tree.out")" = "$COUNT" ] || fail "recursive listing was incomplete"
[ "$MS" -lt 5000 ] || fail "recursive listing during a parked read took ${MS}ms"
echo "  find during parked read: ${MS}ms"

wait "$READER" 2>/dev/null || true
[ "$(cat "$W/parked.rc")" != 0 ] || fail "the stalled read unexpectedly succeeded"
kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon exited during the concurrent listing"

echo "TC-negative-02: PASS ($COUNT-entry listing and lookup in seconds; listing while a read is parked took ${MS}ms, daemon alive)"

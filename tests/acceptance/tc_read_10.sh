#!/usr/bin/env bash
# Design acceptance case TC-read-10: an inherited empty-swarm verdict must not
# wedge peer discovery — a seeder that appears afterwards is still found and
# the file reads completely, without a remount
# (`docs/03-acceptance.md` §B AC-19).
#
# Container-executable form: released binary; the seeder joins the synthesized
# tracker after the first read already failed.
#
# Usage: ./tests/acceptance/tc_read_10.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read10}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-10

SRC="$(payload_patterned payload.bin 2097152)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 25
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = 3
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
FILE="$MNT/data/seed.torrent/seed.bin"

# timed_read OUT ERR — milliseconds the read took; sets READ_RC.
timed_read() {
    local start end
    start=$(date +%s%N)
    READ_RC=0
    slice "$FILE" 0 1024 "$1" 2> "$2" || READ_RC=$?
    end=$(date +%s%N)
    READ_MS=$(( (end - start) / 1000000 ))
}

# 1. Probe: the swarm is empty, so the read fails and leaves a verdict.
timed_read "$W/probe.out" "$W/probe.err"
[ "$READ_RC" -ne 0 ] || fail "probe read on an empty swarm succeeded"
grep -q "No data available" "$W/probe.err" \
    || { cat "$W/probe.err" >&2; fail "the probe read did not fail with ENODATA"; }
echo "  probe read failed as expected after ${READ_MS}ms"

# 1b. While the verdict holds, the next read must fail at once instead of
#     spending a second discovery window (AC-19's "fails immediately").
timed_read "$W/probe2.out" "$W/probe2.err"
[ "$READ_RC" -ne 0 ] || fail "the inheriting read on the empty swarm succeeded"
grep -q "No data available" "$W/probe2.err" \
    || { cat "$W/probe2.err" >&2; fail "the inheriting read did not fail with ENODATA"; }
[ "$READ_MS" -lt 4000 ] \
    || fail "the inheriting read took ${READ_MS}ms — the verdict did not fail it immediately"
echo "  inheriting read: ${READ_MS}ms (verdict applied, no second window)"

# 2. A seeder joins after the verdict is in place.
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 2 || fail "seeder never registered with the tracker"
echo "  seeder announced after the failed probe"

# 3. The same mount must now read the whole file, byte for byte.  Discovery
#    continues through the inheriting reads' announce cadence, so poll like a
#    user retrying; no remount is involved.
OK=0
JOINED=$(date +%s)
for _ in $(seq 1 24); do
    if read_retry "$FILE" "$W/out" 1; then OK=1; break; fi
    sleep 5
done
[ "$OK" = 1 ] || fail "read still failed 120s after the seeder joined (verdict wedged discovery)"
DISC_MS=$(( ($(date +%s) - JOINED) * 1000 ))

GOT="$(stat -c '%s' "$W/out")"
[ "$GOT" = "$TOTAL_SIZE" ] || fail "read returned $GOT bytes, metadata declares $TOTAL_SIZE"
cmp -s "$W/out" "$SRC" || fail "content differs from the source after the late seeder joined"
kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon exited"

echo "TC-read-10: PASS (empty-swarm verdict, then $GOT bytes byte-exact ${DISC_MS}ms after the late seeder announced, no remount)"

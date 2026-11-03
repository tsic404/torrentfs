#!/usr/bin/env bash
# Design acceptance case TC-seeding-03: with a genuinely empty swarm, the
# health alert appears once the grace passes, `.stats` stays readable, and a
# read still ends in bounded time (`docs/03-acceptance.md` §F AC-52).
#
# Container-executable form: released binary, empty local tracker.
#
# Usage: ./tests/acceptance/tc_seeding_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_seeding03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-seeding-03

tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$(payload_patterned seed.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

GRACE=4
write_config <<TOML
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = $GRACE
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
LEAF="$MNT/data/seed.torrent/.stats"

# The grace is the effective no-seeder window: the alert must not fire before
# it, and must fire shortly after it.
sleep 2
if grep -q "Health: 0 peers / 0 seeds" "$LEAF"; then
    fail "the health alert fired before the ${GRACE}s grace elapsed"
fi
ALERT_AT=0
for _ in $(seq 1 30); do
    if grep -q "Health: 0 peers / 0 seeds" "$LEAF"; then ALERT_AT=1; break; fi
    sleep 0.5
done
[ "$ALERT_AT" = 1 ] || fail "no health alert within 15s although the swarm is empty and the grace is ${GRACE}s"
echo "  alert: $(grep -m1 'Health: 0 peers / 0 seeds' "$LEAF" | sed 's/^ *//')"

# The alert does not block the observability surface.
start=$(date +%s%N)
sed -n '1,5p' "$LEAF" > /dev/null
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$MS" -lt 1000 ] || fail "reading .stats with the alert present took ${MS}ms"
echo "  .stats read while alerting: ${MS}ms"

# A read still settles: bounded by the discovery window plus the grace.
start=$(date +%s%N)
rc=0
slice "$MNT/data/seed.torrent/seed.bin" 0 1024 "$W/read.out" 2>/dev/null || rc=$?
end=$(date +%s%N)
MS=$(( (end - start) / 1000000 ))
[ "$rc" -ne 0 ] || fail "an empty-swarm read succeeded"
[ "$MS" -lt 20000 ] || fail "the empty-swarm read took ${MS}ms, expected a bounded failure"
kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon exited while the alert was showing"

echo "TC-seeding-03: PASS (alert after the ${GRACE}s grace, .stats readable in ${MS}ms, read bounded, daemon alive)"

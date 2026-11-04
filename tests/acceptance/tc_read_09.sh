#!/usr/bin/env bash
# Design acceptance case TC-read-09: an empty-swarm read consumes one peer
# discovery window; a following read short-circuits, and once the evidence
# expires the window reopens (`docs/03-acceptance.md` §B AC-18).
#
# Container-executable form: released binary, empty local tracker.
#
# Usage: ./tests/acceptance/tc_read_09.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read09}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-09

WINDOW=8
SRC="$(payload_patterned payload.bin 524288)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"

write_config <<TOML
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = $WINDOW
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

# 1. First read: the whole discovery window must be spent before failing.
timed_read "$W/r1.out" "$W/r1.err"
[ "$READ_RC" -ne 0 ] || fail "empty-swarm read succeeded, expected a non-zero exit"
LOW=$(( (WINDOW - 2) * 1000 ))
HIGH=$(( 2 * WINDOW * 1000 ))
[ "$READ_MS" -ge "$LOW" ] && [ "$READ_MS" -lt "$HIGH" ] \
    || fail "first read took ${READ_MS}ms, expected within [${LOW},${HIGH}) for a ${WINDOW}s window"
echo "  first read: ${READ_MS}ms (window ${WINDOW}s)"

# 2. Second read inside the evidence TTL: no second full window.
timed_read "$W/r2.out" "$W/r2.err"
[ "$READ_RC" -ne 0 ] || fail "inherited read succeeded, expected a non-zero exit"
[ "$READ_MS" -lt "$LOW" ] \
    || fail "second read took ${READ_MS}ms — the discovery window was spent twice"
echo "  second read: ${READ_MS}ms (inherited verdict, no re-wait)"

# 3. After the TTL the window reopens rather than short-circuiting forever.
sleep $((WINDOW + 2))
timed_read "$W/r3.out" "$W/r3.err"
[ "$READ_RC" -ne 0 ] || fail "post-TTL read succeeded, expected a non-zero exit"
[ "$READ_MS" -ge "$LOW" ] && [ "$READ_MS" -lt "$HIGH" ] \
    || fail "post-TTL read took ${READ_MS}ms, expected a re-opened window within [${LOW},${HIGH})"
echo "  post-TTL read: ${READ_MS}ms (window re-opened)"

echo "TC-read-09: PASS (one window spent, inherited read immediate, TTL expiry re-opened the window)"

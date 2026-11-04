#!/usr/bin/env bash
# Design acceptance case TC-seeding-01: pieces this instance already cached are
# uploaded to another peer, whose download of the same torrent therefore
# completes with byte-exact content (`docs/03-acceptance.md` §F AC-50).
#
# Container-executable form: two released-binary instances on one tracker.  The
# fixture peer serves the first instance, is then stopped, so the second
# instance can only be served by the first one.
#
# Usage: ./tests/acceptance/tc_seeding_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_seeding01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-seeding-01

SRC="$(payload_patterned payload.bin 2097152)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

cat > "$W/a.toml" <<'TOML'
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 14
no_seeder_read_timeout_secs = 4
[connections]
listen_interfaces = "127.0.0.1:0"
[tracker]
min_announce_interval = 2
[dht]
enabled = false
TOML
cp "$W/a.toml" "$W/b.toml"

MNT_A="$W/mnt-a"
MNT_B="$W/mnt-b"
mkdir -p "$MNT_A" "$MNT_B"
"$BIN" "$MNT_A" --cache "$W/cache-a" --db "$W/db-a.db" --config "$W/a.toml" > "$W/log-a" 2>&1 &
PID_A=$!
track_pid "$PID_A"
"$BIN" "$MNT_B" --cache "$W/cache-b" --db "$W/db-b.db" --config "$W/b.toml" > "$W/log-b" 2>&1 &
PID_B=$!
track_pid "$PID_B"
wait_mount "$MNT_A" || fail "instance A did not mount"
wait_mount "$MNT_B" || fail "instance B did not mount"

MNT="$MNT_A"
ingest "$TORRENT" >/dev/null
read_retry "$MNT/data/seed.torrent/seed.bin" "$W/a.read" 3 || fail "instance A could not download from the fixture peer"
cmp -s "$W/a.read" "$SRC" || fail "instance A downloaded the wrong bytes"

UL_BEFORE="$(sed -n 's/^  Total UL: *//p' "$MNT_A/.stats" | head -n1)"
echo "  A uploaded $(to_bytes "$UL_BEFORE") bytes before the second peer joined"

# The fixture peer goes away: only instance A can now serve the torrent.
peer_stop
sleep 2

MNT="$MNT_B"
ingest "$TORRENT" >/dev/null
FILE_B="$MNT_B/data/seed.torrent/seed.bin"
OK=0
for _ in $(seq 1 18); do
    if read_retry "$FILE_B" "$W/b.read" 1; then OK=1; break; fi
    sleep 5
done
[ "$OK" = 1 ] || fail "instance B never completed its download from the peer instance"
cmp -s "$W/b.read" "$SRC" || fail "instance B received the wrong bytes"

# Session stats are published on a ~1s tick while the stats surface is being
# polled (C-6.5.2), so the counter is sampled until it moves rather than once.
BEFORE_B="$(to_bytes "$UL_BEFORE")"
AFTER_B="$BEFORE_B"
UL_AFTER="$UL_BEFORE"
# B could only have come from A, so A must report at least one piece uploaded.
NEED=$((BEFORE_B + PIECE_LENGTH))
for _ in $(seq 1 30); do
    UL_AFTER="$(sed -n 's/^  Total UL: *//p' "$MNT_A/.stats" | head -n1)"
    AFTER_B="$(to_bytes "$UL_AFTER")"
    [ "$AFTER_B" -ge "$NEED" ] && break
    sleep 0.5
done
[ "$AFTER_B" -ge "$NEED" ] \
    || fail "instance A's upload counter grew only ${UL_BEFORE} → ${UL_AFTER}, expected at least one piece ($PIECE_LENGTH bytes) more"

echo "TC-seeding-01: PASS (B downloaded $(stat -c '%s' "$W/b.read") bytes from A after the fixture peer left; A upload ${UL_BEFORE} → ${UL_AFTER})"

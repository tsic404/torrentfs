#!/usr/bin/env bash
# Design acceptance case TC-read-05: every read that exhausts its windows and
# is classified ends with ENODATA and one registered cause line — no seeder,
# slow swarm, or cache stall (`docs/03-acceptance.md` §B AC-17).
#
# Container-executable form: released binary, synthesized local swarm (an
# empty tracker, a seeder that unchokes but never delivers, and a cache smaller
# than the read span).  No build step.
#
# Usage: ./tests/acceptance/tc_read_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-05

SRC="$(payload_patterned payload.bin 1048576)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
TORRENT_SRC="$TORRENT"

# ── Case 1: empty swarm → the no-seeder prefix, discovery branch ─────────
MNT="$W/mnt-noseeder"; CACHE_DIR="$W/cache-noseeder"; DB_FILE="$W/db-noseeder.db"
LOG_FILE="$W/log-noseeder"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 8
peer_discovery_wait_secs = 5
no_seeder_read_timeout_secs = 3
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT_SRC" >/dev/null
FILE="$MNT/data/seed.torrent/seed.bin"
rc=0
slice "$FILE" 0 1024 "$W/noseeder.out" 2> "$W/noseeder.err" || rc=$?
[ "$rc" -ne 0 ] || fail "empty-swarm read succeeded, expected ENODATA"
grep -q "No data available" "$W/noseeder.err" \
    || { cat "$W/noseeder.err" >&2; fail "empty-swarm read did not fail with ENODATA"; }
grep -qE "no seeder connected \(Peers:0 Seeds:0\) within the [0-9]+s peer-discovery window; " "$LOG_FILE" \
    || { grep -n "no seeder connected" "$LOG_FILE" >&2 || true; fail "no-seeder discovery-window line missing"; }
enter_line="$(grep -m1 "no seeder connected" "$LOG_FILE")"
echo "  no-seeder: $(echo "$enter_line" | sed 's/^.*no seeder/no seeder/')"
daemon_stop

# ── Case 2: connected seeder that never delivers → slow-swarm prefix ──────
MNT="$W/mnt-slow"; CACHE_DIR="$W/cache-slow"; DB_FILE="$W/db-slow.db"; LOG_FILE="$W/log-slow"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 6
peer_discovery_wait_secs = 5
no_seeder_read_timeout_secs = 3
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --stall
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
wait_seeds "$MNT/data/seed.torrent/.stats" 1 20 || fail "the stalled seeder never showed up as connected"
FILE="$MNT/data/seed.torrent/seed.bin"
rc=0
slice "$FILE" 0 1024 "$W/slow.out" 2> "$W/slow.err" || rc=$?
[ "$rc" -ne 0 ] || fail "stalled-swarm read succeeded, expected ENODATA"
grep -q "No data available" "$W/slow.err" \
    || { cat "$W/slow.err" >&2; fail "stalled-swarm read did not fail with ENODATA"; }
grep -qE "read stalled on a slow swarm \(Peers:1 Seeds:1\): piece [0-9]+ did not arrive" "$LOG_FILE" \
    || { grep -n "read stalled" "$LOG_FILE" >&2 || true; fail "slow-swarm line missing"; }
echo "  slow-swarm: $(grep -m1 'read stalled on a slow swarm' "$LOG_FILE" | sed 's/^.*read stalled/read stalled/')"
daemon_stop

# ── Case 3: cache smaller than the read span → cache-stall prefix ─────────
MNT="$W/mnt-cache"; CACHE_DIR="$W/cache-cache"; DB_FILE="$W/db-cache.db"; LOG_FILE="$W/log-cache"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 6
peer_discovery_wait_secs = 5
no_seeder_read_timeout_secs = 3
[cache]
cache_size = 1024
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
wait_seeds "$MNT/data/seed.torrent/.stats" 1 20 || fail "the stalled seeder never showed up as connected"
FILE="$MNT/data/seed.torrent/seed.bin"
rc=0
read_retry "$FILE" "$W/cache.out" 1 || rc=$?
[ "$rc" -ne 0 ] || fail "read larger than the cache succeeded, expected ENODATA"
grep -q "No data available" "$W/read.err" \
    || { cat "$W/read.err" >&2; fail "cache-stall read did not fail with ENODATA"; }
grep -q "read span exceeds cache" "$LOG_FILE" \
    || { grep -n "on-disk cache" "$LOG_FILE" >&2 || true; fail "cache-stall line missing"; }
grep -qE "read stalled on the on-disk cache \(cache_size=[0-9.]+ MiB, read span=[0-9.]+ MiB, read span exceeds cache\)" "$LOG_FILE" \
    || fail "cache-stall line does not carry the registered wording"
echo "  cache-stall: $(grep -m1 'read stalled on the on-disk cache' "$LOG_FILE" | sed 's/^.*read stalled/read stalled/')"
daemon_stop

echo "TC-read-05: PASS (all three classified failures ended with ENODATA and their frozen prefix)"

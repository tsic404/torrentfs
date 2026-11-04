#!/usr/bin/env bash
# Design acceptance case TC-config-06: the no-seeder wait key takes effect and
# falls back correctly — default 15s, configured 4s, health-alert grace follows
# the effective value, the piece wait is `min(read_timeout, no_seeder)`, and a
# seeder joining mid-wait upgrades the read to the full timeout
# (`docs/03-acceptance.md` §G AC-65).
#
# Container-executable form: released binary; a leecher-only swarm gives the
# "no seeder, peer present" branch, an empty swarm gives the health alert.
#
# Usage: ./tests/acceptance/tc_config_06.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config06}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-06

SRC="$(payload_patterned payload.bin 262144)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 65536 --tracker "$TRACKER_URL"
TORRENT_SRC="$TORRENT"

# ── Case 1: key unset → 15s no-seeder piece wait ─────────────────────────
MNT="$W/mnt-default"; CACHE_DIR="$W/cache-default"; DB_FILE="$W/db-default.db"; LOG_FILE="$W/log-default"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 30
peer_discovery_wait_secs = 20
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --leecher
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
sleep 2
start=$(date +%s)
rc=0
slice "$MNT/data/seed.torrent/seed.bin" 0 1024 "$W/r1" 2>/dev/null || rc=$?
elapsed=$(( $(date +%s) - start ))
[ "$rc" -ne 0 ] || fail "a leecher-only swarm still served the read"
[ "$elapsed" -ge 13 ] && [ "$elapsed" -le 22 ] || fail "unset key waited ${elapsed}s, expected ~15s"
grep -q "after the 15s no-seeder piece wait" "$LOG_FILE" \
    || { grep -n "no seeder connected" "$LOG_FILE" >&2; fail "the 15s fallback was not stated in the diagnostic"; }
echo "  default key: read failed after ${elapsed}s with the 15s no-seeder piece wait"
daemon_stop
peer_stop

# ── Case 2: key = 4 → 4s piece wait ──────────────────────────────────────
MNT="$W/mnt-four"; CACHE_DIR="$W/cache-four"; DB_FILE="$W/db-four.db"; LOG_FILE="$W/log-four"
: > "$TRACKER_STATE"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 30
peer_discovery_wait_secs = 20
no_seeder_read_timeout_secs = 4
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --leecher
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
sleep 2
start=$(date +%s)
rc=0
slice "$MNT/data/seed.torrent/seed.bin" 0 1024 "$W/r2" 2>/dev/null || rc=$?
elapsed=$(( $(date +%s) - start ))
[ "$rc" -ne 0 ] || fail "a leecher-only swarm served the read under a 4s key"
[ "$elapsed" -le 9 ] || fail "configured 4s key waited ${elapsed}s, expected the short window"
grep -q "after the 4s no-seeder piece wait" "$LOG_FILE" \
    || { grep -n "no seeder connected" "$LOG_FILE" >&2; fail "the configured 4s window was not stated"; }
echo "  key=4: read failed after ${elapsed}s with the 4s no-seeder piece wait"
daemon_stop
peer_stop

# time_to_alert STATS LIMIT_SECS — milliseconds until the empty-swarm alert
# shows up, measured from the call (just after daemon_start, so the ingest and
# publish cost is inside both branches alike).  Prints nothing when it never
# appears inside the limit.  C-6.5.3: a leaf `.stats` publishes only while it is
# watched and the first read after an idle period returns the frozen snapshot,
# so the file is polled instead of sampled once.
time_to_alert() {
    local stats="$1" limit="$2" start
    start=$(date +%s%N)
    while [ $(( ($(date +%s%N) - start) / 1000000 )) -lt $(( limit * 1000 )) ]; do
        if grep -q "Health: 0 peers / 0 seeds" "$stats" 2>/dev/null; then
            echo $(( ($(date +%s%N) - start) / 1000000 ))
            return 0
        fi
        sleep 0.5
    done
    return 1
}

# ── Case 3: health-alert grace follows the effective value ───────────────
MNT="$W/mnt-alert4"; CACHE_DIR="$W/cache-alert4"; DB_FILE="$W/db-alert4.db"; LOG_FILE="$W/log-alert4"
: > "$TRACKER_STATE"
write_config <<'TOML'
[timeouts]
no_seeder_read_timeout_secs = 4
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT_SRC" >/dev/null
ALERT4_MS="$(time_to_alert "$MNT/data/seed.torrent/.stats" 12)" || true
[ -n "$ALERT4_MS" ] || fail "the 4s grace never produced the empty-swarm health alert within 12s"
daemon_stop

MNT="$W/mnt-alert15"; CACHE_DIR="$W/cache-alert15"; DB_FILE="$W/db-alert15.db"; LOG_FILE="$W/log-alert15"
: > "$TRACKER_STATE"
write_config <<'TOML'
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT_SRC" >/dev/null
ALERT15_MS="$(time_to_alert "$MNT/data/seed.torrent/.stats" 30)" || true
[ -n "$ALERT15_MS" ] || fail "the default grace never produced the health alert within 30s"
[ $(( ALERT15_MS - ALERT4_MS )) -ge 6000 ] \
    || fail "the default grace appeared at ${ALERT15_MS}ms and the 4s one at ${ALERT4_MS}ms — the grace does not follow the effective value"
echo "  grace: 4s alert at ${ALERT4_MS}ms, default alert at ${ALERT15_MS}ms (later by $(( (ALERT15_MS - ALERT4_MS) / 1000 ))s)"
daemon_stop

# ── Case 4: piece wait = min(read_timeout, no_seeder) ────────────────────
# Every other case keeps read_timeout above the no-seeder key, so the `min`
# never binds.  Here read_timeout (3) is the smaller one: a leecher-only swarm
# must give up after ~3s and say so, not after the 15s no-seeder key.
MNT="$W/mnt-min"; CACHE_DIR="$W/cache-min"; DB_FILE="$W/db-min.db"; LOG_FILE="$W/log-min"
: > "$TRACKER_STATE"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 3
peer_discovery_wait_secs = 20
no_seeder_read_timeout_secs = 15
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --leecher
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
sleep 2
start=$(date +%s%N)
rc=0
slice "$MNT/data/seed.torrent/seed.bin" 0 1024 "$W/r4" 2>/dev/null || rc=$?
elapsed_ms=$(( ($(date +%s%N) - start) / 1000000 ))
[ "$rc" -ne 0 ] || fail "a leecher-only swarm served the read with read_timeout_secs = 3"
[ "$elapsed_ms" -ge 3000 ] && [ "$elapsed_ms" -lt 12000 ] \
    || fail "the read failed after ${elapsed_ms}ms, expected the discovery window and the piece wait each capped at 3s (min(read_timeout_secs, no_seeder_read_timeout_secs))"
grep -q "after the 3s no-seeder piece wait" "$LOG_FILE" \
    || { grep -n "no seeder connected" "$LOG_FILE" >&2; fail "the piece wait did not take min(3, 15) = 3s"; }
echo "  min(read_timeout, no_seeder): read failed after ${elapsed_ms}ms with the 3s no-seeder piece wait"
daemon_stop
peer_stop

# ── Case 5: a seeder joining mid-wait upgrades the read to the full timeout ─
# The seeder is known to the swarm before the read, but completes its handshake
# a few seconds later — inside the 4s no-seeder window, which upgrades the wait
# to the full 45s — and withholds every PIECE until 10s after it started.  The
# read can therefore only finish well past the 4s short window.
MNT="$W/mnt-join"; CACHE_DIR="$W/cache-join"; DB_FILE="$W/db-join.db"; LOG_FILE="$W/log-join"
: > "$TRACKER_STATE"
write_config <<'TOML'
[timeouts]
read_timeout_secs = 45
peer_discovery_wait_secs = 40
no_seeder_read_timeout_secs = 4
[connections]
listen_interfaces = "127.0.0.1:0"
[tracker]
min_announce_interval = 2
[dht]
enabled = false
TOML
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --delay-handshake 3 --serve-after 10
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"
daemon_start
ingest "$TORRENT_SRC" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
start=$(date +%s%N)
( read_retry "$MNT/data/seed.torrent/seed.bin" "$W/join.out" 1; echo $? > "$W/join.rc" ) &
READER=$!
wait "$READER" 2>/dev/null || true
elapsed_ms=$(( ($(date +%s%N) - start) / 1000000 ))
[ "$(cat "$W/join.rc")" = 0 ] || fail "the parked read failed although a seeder connected mid-wait"
cmp -s "$W/join.out" "$SRC" || fail "the mid-wait read returned the wrong bytes"
[ "$elapsed_ms" -gt 5000 ] && [ "$elapsed_ms" -lt 30000 ] \
    || fail "the parked read finished in ${elapsed_ms}ms, expected past the 4s short window but well inside the 45s one"
echo "  mid-wait seeder: parked read completed with the full file after ${elapsed_ms}ms (past the 4s short window)"

echo "TC-config-06: PASS (15s fallback, 4s override, grace follows the effective value, min(read_timeout, no_seeder) binds, mid-wait seeder joins complete the read past the short window)"

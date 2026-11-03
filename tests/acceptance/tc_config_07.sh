#!/usr/bin/env bash
# Design acceptance case TC-config-07: the peer-discovery cap narrows the
# window to the smallest of the three terms, an unset or non-positive cap adds
# no ceiling, and the diagnostic names the knob that actually binds
# (`docs/03-acceptance.md` §G AC-66).
#
# Container-executable form: released binary, empty swarm, measured wall time.
#
# Usage: ./tests/acceptance/tc_config_07.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config07}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-07

SRC="$(payload_patterned payload.bin 65536)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 16384 --tracker "$TRACKER_URL"
TORRENT_SRC="$TORRENT"

# run_case NAME CONFIG_BODY EXPECT_LOW EXPECT_HIGH EXPECT_ADVICE FORBIDDEN
run_case() {
    local name="$1" body="$2" low="$3" high="$4" advice="$5" forbidden="$6"
    MNT="$W/mnt-$name"; CACHE_DIR="$W/cache-$name"; DB_FILE="$W/db-$name.db"; LOG_FILE="$W/log-$name"
    : > "$TRACKER_STATE"
    printf '%s\n' "$body" > "$CONFIG_FILE"
    daemon_start
    ingest "$TORRENT_SRC" >/dev/null
    local start elapsed rc=0
    start=$(date +%s)
    slice "$MNT/data/seed.torrent/seed.bin" 0 1024 "$W/$name.out" 2>/dev/null || rc=$?
    elapsed=$(( $(date +%s) - start ))
    [ "$rc" -ne 0 ] || fail "[$name] the empty swarm served a read"
    [ "$elapsed" -ge "$low" ] && [ "$elapsed" -le "$high" ] \
        || fail "[$name] read failed after ${elapsed}s, expected ${low}..${high}s"
    grep -qF "$advice" "$LOG_FILE" \
        || { grep -n "no seeder connected" "$LOG_FILE" >&2; fail "[$name] diagnostic does not name '$advice'"; }
    if [ -n "$forbidden" ]; then
        if grep -qF "$forbidden" "$LOG_FILE"; then
            fail "[$name] diagnostic wrongly names '$forbidden'"
        fi
    fi
    echo "  $name: failed after ${elapsed}s; diagnostic names the binding knob"
    daemon_stop
}

# Cap binds: min(read_timeout 60, discovery 30, cap 3) = 3.
run_case cap \
'[timeouts]
read_timeout_secs = 60
peer_discovery_wait_secs = 30
peer_wait_cap_secs = 3
no_seeder_read_timeout_secs = 2
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false' 2 8 "raise [timeouts] peer_wait_cap_secs (it caps the window) and retry" ""

# No cap: min(read_timeout 20, discovery 8) = 8; the cap must not be named.
run_case uncapped \
'[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 8
no_seeder_read_timeout_secs = 2
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false' 6 14 "raise [timeouts] peer_discovery_wait_secs and retry" "peer_wait_cap_secs"

# Non-positive cap: same as unset — no extra ceiling.
run_case nonpositive \
'[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 8
peer_wait_cap_secs = 0
no_seeder_read_timeout_secs = 2
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false' 6 14 "raise [timeouts] peer_discovery_wait_secs and retry" "peer_wait_cap_secs"

# read_timeout is the smallest term: min(4, 30) = 4, and the advice says so.
run_case read-timeout-binds \
'[timeouts]
read_timeout_secs = 4
peer_discovery_wait_secs = 30
peer_wait_cap_secs = 10
no_seeder_read_timeout_secs = 2
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false' 3 9 "raise [timeouts] read_timeout_secs (it caps the window) and retry" ""

echo "TC-config-07: PASS (window = min(three terms); unset/non-positive cap adds none; advice names the binding knob)"

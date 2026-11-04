#!/usr/bin/env bash
# Design acceptance case TC-config-01: with no configuration file the daemon
# starts on the documented defaults, its read path works, and offline
# validation accepts positive values for the keys that are not asserted at
# runtime (`docs/03-acceptance.md` §G AC-60).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_config_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-01

SRC="$(payload_patterned payload.bin 262144)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 65536 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

# No --config argument at all: every default applies.
daemon_start_bare --cache "$CACHE_DIR" --db "$DB_FILE"
ingest "$TORRENT" >/dev/null

LISTEN="$(sed -n 's/^  Listen: *//p' "$MNT/.stats" | head -n1)"
[ "$LISTEN" = "0.0.0.0:6881" ] || fail "default listen address is '$LISTEN', expected 0.0.0.0:6881"
CACHE_MAX="$(sed -n 's/^Cache: .*max=\([0-9]*\).*/\1/p' "$MNT/data/seed.torrent/.stats" | head -n1)"
[ "$CACHE_MAX" = 1073741824 ] || fail "default cache cap is $CACHE_MAX, expected 1073741824 (1 GiB)"

read_retry "$MNT/data/seed.torrent/seed.bin" "$W/read" 3 || fail "read path unusable under the default configuration"
cmp -s "$W/read" "$SRC" || fail "default-config read returned the wrong bytes"
echo "  defaults: Listen=$LISTEN cache_max=$CACHE_MAX read=ok"
daemon_stop

# Offline validation accepts positive values for the two concurrency keys and
# the timeout keys; their runtime values are not asserted (AC-60).
write_config <<'TOML'
[timeouts]
read_timeout_secs = 7
peer_discovery_wait_secs = 7
no_seeder_read_timeout_secs = 3
[concurrency]
download_workers = 2
download_queue_depth = 8
TOML
rc=0
"$BIN" --config "$CONFIG_FILE" --config-check > "$W/check.log" 2>&1 || rc=$?
[ "$rc" = 0 ] || { cat "$W/check.log" >&2; fail "offline validation rejected positive values (exit $rc)"; }
grep -q "is valid" "$W/check.log" || fail "offline validation produced no acceptance line"

echo "TC-config-01: PASS (defaults: Listen=0.0.0.0:6881, cache cap=1 GiB, read path usable; --config-check accepts positive values)"

#!/usr/bin/env bash
# Design acceptance case TC-config-02: a specified configuration file takes
# effect — the listen address and the cache cap are observable in the stats
# surface and the file is accepted (`docs/03-acceptance.md` §G AC-61).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_config_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-02

tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$(payload_patterned seed.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

write_config <<'TOML'
[connections]
listen_interfaces = "127.0.0.1:51413"
[cache]
cache_size = 1048576
[local_discovery]
lsd_enabled = true
[timeouts]
read_timeout_secs = 10
peer_discovery_wait_secs = 5
no_seeder_read_timeout_secs = 3
TOML

rc=0
"$BIN" --config "$CONFIG_FILE" --config-check > "$W/check.log" 2>&1 || rc=$?
[ "$rc" = 0 ] || { cat "$W/check.log" >&2; fail "the configuration file was rejected (exit $rc)"; }

daemon_start
LISTEN="$(sed -n 's/^  Listen: *//p' "$MNT/.stats" | head -n1)"
[ "$LISTEN" = "127.0.0.1:51413" ] || fail "Listen is '$LISTEN', expected the configured 127.0.0.1:51413"

ingest "$TORRENT" >/dev/null
CACHE_MAX="$(sed -n 's/^Cache: .*max=\([0-9]*\).*/\1/p' "$MNT/data/seed.torrent/.stats" | head -n1)"
[ "$CACHE_MAX" = 1048576 ] || fail "leaf cache cap is $CACHE_MAX, expected the configured 1048576"
ROOT_USAGE="$(sed -n 's/^  Cache Usage: .* \/ \([0-9.]* [A-Z]*\).*/\1/p' "$MNT/.stats" | head -n1)"
echo "  Listen=$LISTEN cache_max=$CACHE_MAX root_usage_cap='$ROOT_USAGE'"

echo "TC-config-02: PASS ([connections].listen_interfaces and [cache].cache_size are observable; the file is accepted)"

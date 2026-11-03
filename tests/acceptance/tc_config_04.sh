#!/usr/bin/env bash
# Design acceptance case TC-config-04: a partial configuration overrides only
# what it names; unset keys keep their defaults and the daemon starts cleanly
# (`docs/03-acceptance.md` §G AC-63).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_config_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-04

tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$(payload_patterned seed.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 25
TOML
daemon_start
ingest "$TORRENT" >/dev/null

LISTEN="$(sed -n 's/^  Listen: *//p' "$MNT/.stats" | head -n1)"
[ "$LISTEN" = "0.0.0.0:6881" ] || fail "unset listen_interfaces should stay at the default 0.0.0.0:6881, got '$LISTEN'"
CACHE_MAX="$(sed -n 's/^Cache: .*max=\([0-9]*\).*/\1/p' "$MNT/data/seed.torrent/.stats" | head -n1)"
[ "$CACHE_MAX" = 1073741824 ] || fail "unset cache_size should stay at 1073741824, got $CACHE_MAX"
grep -qE 'ERROR|panicked' "$LOG_FILE" && { grep -E 'ERROR|panicked' "$LOG_FILE" >&2; fail "a partial configuration logged an error"; }

echo "TC-config-04: PASS (only read_timeout_secs overridden; Listen=$LISTEN and cache cap=$CACHE_MAX kept their defaults, no error)"

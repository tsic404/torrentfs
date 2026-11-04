#!/usr/bin/env bash
# Design acceptance case TC-config-05: offline validation only validates — it
# creates no mountpoint, no state database and no cache directory, needs no
# FUSE device, and its exit code carries the verdict
# (`docs/03-acceptance.md` §G AC-64).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_config_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
env_init TC-config-05

GOOD="$W/good.toml"
BAD="$W/bad.toml"
CHECK_MNT="$W/never-created-mnt"
CHECK_CACHE="$W/never-created-cache"
CHECK_DB="$W/never-created-state/metadata.db"
printf '[timeouts]\nread_timeout_secs = 9\n' > "$GOOD"
printf '[nope]\nx = 1\n' > "$BAD"

rc=0
"$BIN" --config "$GOOD" --cache "$CHECK_CACHE" --db "$CHECK_DB" --config-check \
    > "$W/good.log" 2>&1 || rc=$?
[ "$rc" = 0 ] || { cat "$W/good.log" >&2; fail "valid configuration did not exit 0 (exit $rc)"; }
[ -e "$CHECK_CACHE" ] && fail "offline validation created the cache directory"
[ -e "$CHECK_DB" ] && fail "offline validation created the state database"
[ -e "$(dirname "$CHECK_DB")" ] && fail "offline validation created the state directory"

rc=0
"$BIN" --config "$BAD" --cache "$CHECK_CACHE" --db "$CHECK_DB" --config-check \
    > "$W/bad.log" 2>&1 || rc=$?
[ "$rc" -ne 0 ] || fail "invalid configuration exited 0 in offline validation"
[ -e "$CHECK_CACHE" ] && fail "offline validation of an invalid file created the cache directory"
[ -e "$CHECK_DB" ] && fail "offline validation of an invalid file created the state database"

# A mountpoint together with --config-check is a usage error (exit 2), so the
# mode cannot silently mount what it was asked only to validate.
rc=0
"$BIN" "$CHECK_MNT" --config "$GOOD" --config-check > "$W/usage.log" 2>&1 || rc=$?
[ "$rc" = 2 ] || { cat "$W/usage.log" >&2; fail "--config-check with a mountpoint exited $rc, expected 2"; }
[ -e "$CHECK_MNT" ] && fail "the usage error still created the mountpoint"

echo "TC-config-05: PASS (valid→0, invalid→non-zero, mountpoint+cache+database never created, mountpoint+config-check→2)"

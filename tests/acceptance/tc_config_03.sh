#!/usr/bin/env bash
# Design acceptance case TC-config-03: five classes of invalid configuration
# fail fast with a non-zero exit and a reason, identically for a direct start
# and for offline validation (`docs/03-acceptance.md` §G AC-62).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_config_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_config03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-config-03

mkdir -p "$MNT"
BAD="$W/bad"
mkdir -p "$BAD"
printf 'not toml [[[\n' > "$BAD/syntax.toml"
printf '[nope]\nx = 1\n' > "$BAD/unknown-section.toml"
printf '[timeouts]\nunknown_key = 1\n' > "$BAD/unknown-key.toml"
printf '[connections]\nmax_connections = -1\n' > "$BAD/out-of-range.toml"
printf '[algorithms]\nchoking_algorithm = 9\n' > "$BAD/out-of-range-enum.toml"

declare -A EXPECT=(
    [syntax]='expected `.`, `=`'
    [unknown-section]='unknown field `nope`'
    [unknown-key]='unknown field `unknown_key`'
    [out-of-range]='Invalid config value'
    [out-of-range-enum]='enum value for `algorithms.choking_algorithm` out of range'
)

for name in syntax unknown-section unknown-key out-of-range out-of-range-enum; do
    cfg="$BAD/$name.toml"
    want="${EXPECT[$name]}"

    rc=0
    "$BIN" --config "$cfg" --config-check > "$W/check.$name.log" 2>&1 || rc=$?
    [ "$rc" -ne 0 ] || fail "[$name] offline validation exited 0 for an invalid file"
    grep -qF "$want" "$W/check.$name.log" \
        || { cat "$W/check.$name.log" >&2; fail "[$name] offline validation did not name the cause ('$want')"; }

    rc=0
    "$BIN" "$MNT" --cache "$W/cache-$name" --db "$W/db-$name.sqlite" --config "$cfg" \
        > "$W/start.$name.log" 2>&1 || rc=$?
    [ "$rc" -ne 0 ] || fail "[$name] direct start exited 0 for an invalid file"
    grep -qF "$want" "$W/start.$name.log" \
        || { cat "$W/start.$name.log" >&2; fail "[$name] direct start did not name the cause ('$want')"; }

    [ -e "$MNT/.stats" ] && fail "[$name] an invalid configuration still mounted the filesystem"
    echo "  $name: exit=$rc both modes, cause matched"
done

echo "TC-config-03: PASS (syntax / unknown section / unknown key / out-of-range value / out-of-range enum all fail fast in both modes)"

#!/usr/bin/env bash
# Design acceptance case TC-rofs-02: moving an entry inside `data/` is refused
# with a read-only errno and both sides of the move keep their original state
# (`docs/03-acceptance.md` §C AC-21).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_rofs_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_rofs02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-rofs-02
write_fast_config

tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$(payload_patterned seed.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
ingest "$TORRENT" >/dev/null
SEED_ROOT="$MNT/data/seed.torrent"
FILE="$SEED_ROOT/seed.bin"
[ -f "$FILE" ] || fail "projected file missing before the move"

snapshot() {
    ( cd "$MNT/data" && find . -mindepth 1 ! -name '.stats' -printf '%p %s %m\n' | sort )
}
BEFORE="$(snapshot)"

expect_readonly() {
    local label="$1" errfile="$2" rc="$3"
    [ "$rc" -ne 0 ] || fail "$label succeeded but must be refused"
    grep -qE "Read-only file system|Operation not permitted" "$errfile" \
        || { cat "$errfile" >&2; fail "$label did not report a read-only errno"; }
}

rc=0
mv "$FILE" "$SEED_ROOT/moved.bin" 2> "$W/mv1.err" || rc=$?
expect_readonly "rename inside the seed root" "$W/mv1.err" "$rc"
[ -f "$FILE" ] || fail "source file disappeared after the refused rename"
[ -e "$SEED_ROOT/moved.bin" ] && fail "the refused rename created its target"

rc=0
mv "$SEED_ROOT" "$MNT/data/renamed" 2> "$W/mv2.err" || rc=$?
expect_readonly "rename of the seed root" "$W/mv2.err" "$rc"
[ -d "$SEED_ROOT" ] || fail "the seed root disappeared after the refused rename"
[ -e "$MNT/data/renamed" ] && fail "the refused rename created a new seed root"

AFTER="$(snapshot)"
[ "$BEFORE" = "$AFTER" ] || {
    diff <(printf '%s\n' "$BEFORE") <(printf '%s\n' "$AFTER") >&2
    fail "data/ entries changed across the refused moves"
}

echo "TC-rofs-02: PASS (both moves refused with a read-only errno; entry set, sizes and modes unchanged)"

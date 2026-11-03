#!/usr/bin/env bash
# Design acceptance case TC-rofs-03: the three `.stats` files (root, `data/`,
# seed root) reject writes, deletes and truncation with EPERM and always report
# a 0444 regular file (`docs/03-acceptance.md` §C AC-22).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_rofs_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_rofs03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-rofs-03
write_fast_config

tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$(payload_patterned seed.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
ingest "$TORRENT" >/dev/null

STATS_FILES=("$MNT/.stats" "$MNT/data/.stats" "$MNT/data/seed.torrent/.stats")
for stats in "${STATS_FILES[@]}"; do
    [ -f "$stats" ] || fail "${stats#"$MNT"} is not a regular file"
    mode="$(stat -c '%a' "$stats")"
    [ "$mode" = 444 ] || fail "${stats#"$MNT"} has mode $mode, expected 444"
    type="$(stat -c '%F' "$stats")"
    [ "$type" = "regular file" ] || fail "${stats#"$MNT"} is a $type, expected a regular file"

    rc=0
    printf 'x\n' >> "$stats" 2> "$W/append.err" || rc=$?
    [ "$rc" -ne 0 ] || fail "appending to ${stats#"$MNT"} succeeded"
    grep -q "Operation not permitted" "$W/append.err" \
        || { cat "$W/append.err" >&2; fail "append to ${stats#"$MNT"} did not report EPERM"; }

    rc=0
    truncate -s 0 "$stats" 2> "$W/trunc.err" || rc=$?
    [ "$rc" -ne 0 ] || fail "truncating ${stats#"$MNT"} succeeded"
    grep -q "Operation not permitted" "$W/trunc.err" \
        || { cat "$W/trunc.err" >&2; fail "truncate of ${stats#"$MNT"} did not report EPERM"; }

    rc=0
    rm -f "$stats" 2> "$W/rm.err" || rc=$?
    [ "$rc" -ne 0 ] || fail "deleting ${stats#"$MNT"} succeeded"
    grep -q "Operation not permitted" "$W/rm.err" \
        || { cat "$W/rm.err" >&2; fail "delete of ${stats#"$MNT"} did not report EPERM"; }

    [ -f "$stats" ] || fail "${stats#"$MNT"} disappeared after the refused operations"
    mode="$(stat -c '%a' "$stats")"
    [ "$mode" = 444 ] || fail "${stats#"$MNT"} mode changed to $mode after the refused operations"
    echo "  ${stats#"$MNT"}: append/truncate/delete → EPERM, attributes still 444 regular file"
done

echo "TC-rofs-03: PASS (root/data/seed .stats all rejected write+truncate+delete with EPERM, attributes 0444)"

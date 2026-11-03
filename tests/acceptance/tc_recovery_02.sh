#!/usr/bin/env bash
# Design acceptance case TC-recovery-02: a seed directory with thousands of
# files enters, lists, lists recursively and resolves by name within seconds
# without hanging (`docs/03-acceptance.md` §I AC-81).
#
# Container-executable form: released binary, a 3000-file torrent.
#
# Usage: ./tests/acceptance/tc_recovery_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_recovery02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-recovery-02
write_fast_config

COUNT=3000
tracker_start
build_torrent "$TORRENT_FILES/bulk.torrent" --name payload --bulk "$COUNT" \
    --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/bulk-src" \
    --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
ingest "$TORRENT" >/dev/null
SEED_ROOT="$MNT/data/bulk.torrent/payload/bulk"
wait_path "$SEED_ROOT/02999.bin" 120 || fail "payload/bulk seed directory did not finish publishing"

# timed LABEL OUTFILE CMD... — run CMD, capture stdout to OUTFILE, and set
# ELAPSED_MS in this shell (a command substitution would lose it).
timed() {
    local label="$1" outfile="$2"
    shift 2
    local start end rc=0
    start=$(date +%s%N)
    "$@" > "$outfile" 2> "$W/$label.err" || rc=$?
    end=$(date +%s%N)
    ELAPSED_MS=$(( (end - start) / 1000000 ))
    [ "$rc" = 0 ] || { cat "$W/$label.err" >&2; fail "$label exited $rc"; }
}

timed enter "$W/enter.out" stat "$SEED_ROOT"
[ "$ELAPSED_MS" -lt 5000 ] || fail "entering payload/bulk took ${ELAPSED_MS}ms, expected seconds"
echo "  enter payload/bulk: ${ELAPSED_MS}ms"

timed list "$W/list.out" ls -U "$SEED_ROOT"
[ "$ELAPSED_MS" -lt 5000 ] || fail "listing payload/bulk took ${ELAPSED_MS}ms, expected seconds"
N="$(wc -l < "$W/list.out")"
[ "$N" = "$COUNT" ] || fail "listing payload/bulk returned $N entries, expected $COUNT"
echo "  list payload/bulk: ${ELAPSED_MS}ms for $N entries"

timed find "$W/find.out" find "$SEED_ROOT" -type f -printf '%P\n'
[ "$ELAPSED_MS" -lt 5000 ] || fail "recursive listing took ${ELAPSED_MS}ms, expected seconds"
N="$(wc -l < "$W/find.out")"
[ "$N" = "$COUNT" ] || fail "recursive listing returned $N files, expected $COUNT (complete)"
echo "  find -type f: ${ELAPSED_MS}ms for $N files"

timed lookup "$W/lookup.out" stat -c '%s' "$SEED_ROOT/02999.bin"
[ "$ELAPSED_MS" -lt 5000 ] || fail "resolving 02999.bin took ${ELAPSED_MS}ms, expected seconds"
LOOKUP="$(cat "$W/lookup.out")"
[ "$LOOKUP" = 1 ] || fail "resolved bulk/02999.bin with size $LOOKUP, expected 1"
echo "  lookup 02999.bin: ${ELAPSED_MS}ms, size=$LOOKUP"

echo "TC-recovery-02: PASS (enter/list/find/lookup on a $COUNT-file directory all within seconds, outputs complete)"

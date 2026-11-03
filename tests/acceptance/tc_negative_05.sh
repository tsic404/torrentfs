#!/usr/bin/env bash
# Design acceptance case TC-negative-05: expanding a directory at one-entry
# pagination granularity costs must grow with the entry count, not with its
# square (`docs/03-acceptance.md` §J AC-91, F2).
#
# Container-executable form: released binary; two isolated instances hold 20000
# and 80000 entries in one directory, and the walk consumes exactly one entry
# per getdents64 (lib_dir_walk.pl) — the granularity the design names.
#
# Usage: ./tests/acceptance/tc-negative_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-05

# Fixed-overhead calibration directory: 100 entries in the same instance.
CAL=100
SMALL=20000
LARGE=80000
MAX_GROWTH=8

write_fast_config
tracker_start

# Each scale is measured on an instance holding only its own directory.  The
# calibration tier (4th argument) rides on the smaller tier's instance, so the
# per-call overhead is measured on the target mount, not on a tmpfs.
run_scale() {
    local name="$1" torrent="$2" declared="$3" calibrate="${4:-0}"
    MNT="$W/mnt-$name"; CACHE_DIR="$W/cache-$name"; DB_FILE="$W/db-$name.db"; LOG_FILE="$W/log-$name"
    daemon_start
    ingest "$torrent" >/dev/null
    local dir="$MNT/data/$name.torrent/$name/bulk"
    wait_path "$dir/00000.bin" 180 || fail "[$name] <name>/bulk projection did not finish publishing"

    # One-entry pagination: the buffer is exactly one record, so every entry
    # costs one getdents64 call, as the design's granularity requires.
    local start end
    start=$(date +%s%N)
    local got
    got="$(perl "$ROOT_DIR/tests/acceptance/lib_dir_walk.pl" list "$dir" 2>"$W/$name.walk.err")" \
        || fail "[$name] getdents walk failed"
    end=$(date +%s%N)
    SCALE_MS=$(( (end - start) / 1000000 ))
    printf '%s\n' "$got" > "$W/$name.names"
    local count
    count="$(wc -l < "$W/$name.names")"
    [ "$count" = "$declared" ] || fail "[$name] walk returned $count entries, expected $declared"
    local unique
    unique="$(sort -u "$W/$name.names" | wc -l)"
    [ "$unique" = "$declared" ] || fail "[$name] walk returned $unique unique entries, expected $declared (no repeats)"

    if [ "$calibrate" = 1 ]; then
        # Fixed per-request overhead on this very mount: 100 entries are cheap
        # to scan, so the measured time is the getdents64 call cost, which is
        # what the two-scale contrast must not be dominated by.
        build_torrent "$TORRENT_FILES/cal.torrent" --name cal --bulk "$CAL" \
            --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/cal" \
            --piece-length 65536 --tracker "$TRACKER_URL"
        ingest "$TORRENT_FILES/cal.torrent" >/dev/null
        local caldir="$MNT/data/cal.torrent/cal/bulk"
        wait_path "$caldir/00000.bin" 180 || fail "[$name] calibration directory did not finish publishing"
        local cstart cend
        cstart=$(date +%s%N)
        perl "$ROOT_DIR/tests/acceptance/lib_dir_walk.pl" list "$caldir" >/dev/null 2>&1 \
            || fail "[$name] calibration walk failed"
        cend=$(date +%s%N)
        FIXED_MS=$(( (cend - cstart) / 1000000 ))
        echo "  calibration: $CAL entries on $MNT in ${FIXED_MS}ms (fixed per-call overhead)"
    fi
    daemon_stop
    echo "  $name: $count entries in ${SCALE_MS}ms ($(sed -n 's/^BUF=//p' "$W/$name.walk.err" | head -n1)-byte buffer)"
}

# Measure both tiers; the calibration rides on the smaller tier's instance.
measure_tiers() {
    build_torrent "$TORRENT_FILES/small.torrent" --name small --bulk "$SMALL" \
        --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/small" \
        --piece-length 65536 --tracker "$TRACKER_URL"
    local small_torrent="$TORRENT"
    run_scale small "$small_torrent" "$SMALL" 1
    SMALL_MS="$SCALE_MS"
    build_torrent "$TORRENT_FILES/large.torrent" --name large --bulk "$LARGE" \
        --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/large" \
        --piece-length 65536 --tracker "$TRACKER_URL"
    local large_torrent="$TORRENT"
    run_scale large "$large_torrent" "$LARGE" 0
    LARGE_MS="$SCALE_MS"
}

# Sensitivity precondition (design §验收范围规则): the full scan's added cost
# must be at least twice the fixed overhead.  Short of it, both tiers move up by
# 4x and the measurement repeats; still short, the judgement is aborted (exit 2)
# rather than recorded as a pass.
MARGIN=0
for attempt in 1 2; do
    measure_tiers
    if [ "$(scan_added_ms "$FIXED_MS" "$SMALL_MS")" -ge $((2 * FIXED_MS)) ]; then
        MARGIN=1
        break
    fi
    echo "  ${SMALL_MS}ms scan vs ${FIXED_MS}ms fixed overhead: no margin, raising both tiers 4x"
    SMALL=$((SMALL * 4))
    LARGE=$((LARGE * 4))
done
[ "$MARGIN" = 1 ] \
    || skip "fixed overhead ${FIXED_MS}ms still dominates the ${SMALL_MS}ms scan at $SMALL/$LARGE entries (no verdict)"
ADDED_MS="$(scan_added_ms "$FIXED_MS" "$SMALL_MS")"

[ "$SMALL_MS" -gt 0 ] || SMALL_MS=1
RATIO="$(awk -v a="$LARGE_MS" -v b="$SMALL_MS" 'BEGIN { printf "%.2f", a / b }')"
OK="$(awk -v a="$LARGE_MS" -v b="$SMALL_MS" -v m="$MAX_GROWTH" 'BEGIN { print (a / b < m) ? 1 : 0 }')"
[ "$OK" = 1 ] || fail "4x entries took ${RATIO}x the time (limit ${MAX_GROWTH}x): ${SMALL_MS}ms → ${LARGE_MS}ms"

echo "TC-negative-05: PASS ($SMALL→$LARGE entries: ${SMALL_MS}ms → ${LARGE_MS}ms = ${RATIO}x < ${MAX_GROWTH}x; calibration: $CAL-entry FUSE directory in the same instance, ${FIXED_MS}ms fixed overhead, ${ADDED_MS}ms added scan cost ≥ 2×)"

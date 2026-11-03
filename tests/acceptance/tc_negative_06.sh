#!/usr/bin/env bash
# Design acceptance case TC-negative-06: resolving each entry of a large
# directory by name costs must grow with the entry count, not with its square
# (`docs/03-acceptance.md` §J AC-91, F2/F3).
#
# Container-executable form: released binary; two isolated instances hold 20000
# and 80000 entries.  The names come from the torrent metadata (the generated
# payload), never from listing the projected directory, and only the resolution
# phase is timed.
#
# Usage: ./tests/acceptance/tc_negative_06.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative06}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-06

SMALL=20000
LARGE=80000
CAL=100
MAX_GROWTH=8

write_fast_config
tracker_start

# The entry names the torrent metadata declares, derived from the fixture's
# declared file list (never obtained by listing the projected directory).
metadata_names() {
    local declared="$1"
    seq 0 $((declared - 1)) | while read -r i; do printf '%05d.bin\n' "$i"; done | sort
}

# Each scale is measured on an instance holding only its own directory.  The
# calibration tier (4th argument) rides on the smaller tier's instance: 100
# names resolved by the same walker on the target mount give the per-call fixed
# overhead, which the full-resolution cost must exceed by at least 2x.
run_scale() {
    local name="$1" torrent="$2" declared="$3" calibrate="${4:-0}"
    MNT="$W/mnt-$name"; CACHE_DIR="$W/cache-$name"; DB_FILE="$W/db-$name.db"; LOG_FILE="$W/log-$name"
    daemon_start
    ingest "$torrent" >/dev/null
    local dir="$MNT/data/$name.torrent/$name/bulk"
    wait_path "$dir/00000.bin" 180 || fail "[$name] <name>/bulk projection did not finish publishing"

    metadata_names "$declared" > "$W/$name.names"
    local count
    count="$(wc -l < "$W/$name.names")"
    [ "$count" = "$declared" ] || fail "[$name] metadata declares $count names, expected $declared"

    local start end
    start=$(date +%s%N)
    perl "$ROOT_DIR/tests/acceptance/lib_dir_walk.pl" stat "$dir" "$W/$name.names" > "$W/$name.resolved" \
        || fail "[$name] by-name resolution failed"
    end=$(date +%s%N)
    SCALE_MS=$(( (end - start) / 1000000 ))

    local resolved
    resolved="$(wc -l < "$W/$name.resolved")"
    [ "$resolved" = "$declared" ] || fail "[$name] resolved $resolved names, expected $declared"
    # Every resolved entry must be the declared 1-byte file, and the set must
    # equal the metadata's.
    if awk '$2 != 1 { exit 1 }' "$W/$name.resolved"; then :; else
        fail "[$name] at least one resolved entry does not have the declared size"
    fi
    cut -d' ' -f1 "$W/$name.resolved" | sort > "$W/$name.resolved.names"
    cmp -s "$W/$name.resolved.names" "$W/$name.names" \
        || fail "[$name] resolved set differs from the torrent metadata's entry set"

    if [ "$calibrate" = 1 ]; then
        build_torrent "$TORRENT_FILES/cal.torrent" --name cal --bulk "$CAL" \
            --bulk-prefix bulk --bulk-size 1 --payload-out "$PAYLOAD/cal" \
            --piece-length 65536 --tracker "$TRACKER_URL"
        ingest "$TORRENT_FILES/cal.torrent" >/dev/null
        local caldir="$MNT/data/cal.torrent/cal/bulk"
        wait_path "$caldir/00000.bin" 180 || fail "[$name] calibration directory did not finish publishing"
        metadata_names "$CAL" > "$W/$name.cal.names"
        local cstart cend
        cstart=$(date +%s%N)
        perl "$ROOT_DIR/tests/acceptance/lib_dir_walk.pl" stat "$caldir" "$W/$name.cal.names" >/dev/null \
            || fail "[$name] calibration resolution failed"
        cend=$(date +%s%N)
        FIXED_MS=$(( (cend - cstart) / 1000000 ))
        echo "  calibration: $CAL names on $MNT in ${FIXED_MS}ms (fixed per-call overhead)"
    fi
    daemon_stop
    echo "  $name: resolved $resolved names in ${SCALE_MS}ms"
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

# Sensitivity precondition (design §验收范围规则): the full resolution's added
# cost must be at least twice the fixed per-call overhead.  Short of it, both
# tiers move up by 4x and the measurement repeats; still short, the judgement is
# aborted (exit 2) rather than recorded as a pass.
MARGIN=0
for attempt in 1 2; do
    measure_tiers
    if [ "$(scan_added_ms "$FIXED_MS" "$SMALL_MS")" -ge $((2 * FIXED_MS)) ]; then
        MARGIN=1
        break
    fi
    echo "  ${SMALL_MS}ms resolution vs ${FIXED_MS}ms fixed overhead: no margin, raising both tiers 4x"
    SMALL=$((SMALL * 4))
    LARGE=$((LARGE * 4))
done
[ "$MARGIN" = 1 ] \
    || skip "fixed overhead ${FIXED_MS}ms still dominates the ${SMALL_MS}ms resolution at $SMALL/$LARGE entries (no verdict)"
ADDED_MS="$(scan_added_ms "$FIXED_MS" "$SMALL_MS")"

[ "$SMALL_MS" -gt 0 ] || SMALL_MS=1
RATIO="$(awk -v a="$LARGE_MS" -v b="$SMALL_MS" 'BEGIN { printf "%.2f", a / b }')"
OK="$(awk -v a="$LARGE_MS" -v b="$SMALL_MS" -v m="$MAX_GROWTH" 'BEGIN { print (a / b < m) ? 1 : 0 }')"
[ "$OK" = 1 ] || fail "4x entries took ${RATIO}x the time (limit ${MAX_GROWTH}x): ${SMALL_MS}ms → ${LARGE_MS}ms"

echo "TC-negative-06: PASS ($SMALL→$LARGE by-name resolutions: ${SMALL_MS}ms → ${LARGE_MS}ms = ${RATIO}x < ${MAX_GROWTH}x; resolved sets match metadata; calibration: $CAL-name FUSE directory in the same instance, ${FIXED_MS}ms fixed overhead, ${ADDED_MS}ms added resolution cost ≥ 2×)"

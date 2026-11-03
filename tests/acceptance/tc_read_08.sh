#!/usr/bin/env bash
# Design acceptance case TC-read-08: 20+ concurrent mixed reads (large file,
# small file, directory traversal) all exit without hang or crash and the
# daemon stays alive (`docs/03-acceptance.md` §B AC-16).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_read_08.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read08}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-08
write_fast_config

BIG="$(payload_patterned big.bin 4194304)"
SMALL="$(payload_patterned small.bin 65536)"
tracker_start
build_torrent "$TORRENT_FILES/big.torrent" --name big.bin --content "$BIG" \
    --piece-length 262144 --tracker "$TRACKER_URL"
BIG_HASH="$INFOHASH"
build_torrent "$TORRENT_FILES/small.torrent" --name small.bin --content "$SMALL" \
    --piece-length 16384 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SMALL"
peer_start "$BIG_HASH" 262144 "$BIG"
wait_peer_known "$BIG_HASH" 1 || fail "big-file peer never registered"
wait_peer_known "$INFOHASH" 1 || fail "small-file peer never registered"

daemon_start
ingest "$TORRENT_FILES/big.torrent" >/dev/null
ingest "$TORRENT_FILES/small.torrent" >/dev/null
wait_peer_known "$BIG_HASH" 2 || fail "client never announced the big torrent"
BIG_FILE="$MNT/data/big.torrent/big.bin"
SMALL_FILE="$MNT/data/small.torrent/small.bin"

# 24 processes: 10 full reads of the large file, 6 offset reads, 4 reads of the
# small file, 4 recursive directory traversals — all started together, each
# under a hard budget so a hang fails this case instead of stalling the runner.
READER_BUDGET="${TC_READ08_BUDGET_SECS:-150}"
START=$(date +%s)
READERS=()
for i in $(seq 1 10); do
    ( timeout "$READER_BUDGET" cat "$BIG_FILE" > "$W/big.$i" 2>/dev/null; echo $? > "$W/big.rc.$i" ) &
    READERS+=($!)
done
for i in $(seq 1 6); do
    ( timeout "$READER_BUDGET" dd if="$BIG_FILE" of="$W/seg.$i" bs=65536 \
        skip=$((i * 524288)) count=262144 iflag=skip_bytes,count_bytes status=none 2>/dev/null
      echo $? > "$W/seg.rc.$i" ) &
    READERS+=($!)
done
for i in $(seq 1 4); do
    ( timeout "$READER_BUDGET" cat "$SMALL_FILE" > "$W/small.$i" 2>/dev/null; echo $? > "$W/small.rc.$i" ) &
    READERS+=($!)
done
for i in $(seq 1 4); do
    ( timeout "$READER_BUDGET" find "$MNT/data" -type f > "$W/tree.$i" 2>/dev/null; echo $? > "$W/tree.rc.$i" ) &
    READERS+=($!)
done
for pid in "${READERS[@]}"; do wait "$pid"; done
ELAPSED=$(( $(date +%s) - START ))
[ "$ELAPSED" -le "$READER_BUDGET" ] \
    || fail "the concurrent run took ${ELAPSED}s, over its ${READER_BUDGET}s budget"

# A reader killed by the budget reports 124: that is a hang, not a wrong byte.
check_reader_rc() {
    local rc="$1" label="$2"
    [ "$rc" != 124 ] || fail "$label hit the ${READER_BUDGET}s budget (no exit, a hang)"
    [ "$rc" = 0 ] || fail "$label failed (rc=$rc)"
}
for i in $(seq 1 10); do
    check_reader_rc "$(cat "$W/big.rc.$i")" "large-file reader $i"
    [ "$(digest "$W/big.$i")" = "$(digest "$BIG")" ] || fail "large-file reader $i digest differs"
done
for i in $(seq 1 6); do
    check_reader_rc "$(cat "$W/seg.rc.$i")" "offset reader $i"
    [ "$(stat -c '%s' "$W/seg.$i")" = 262144 ] || fail "offset reader $i returned the wrong byte count"
done
for i in $(seq 1 4); do
    check_reader_rc "$(cat "$W/small.rc.$i")" "small-file reader $i"
    [ "$(digest "$W/small.$i")" = "$(digest "$SMALL")" ] || fail "small-file reader $i digest differs"
    check_reader_rc "$(cat "$W/tree.rc.$i")" "directory traversal $i"
    [ "$(wc -l < "$W/tree.$i")" -ge 2 ] || fail "directory traversal $i found no files"
done

kill -0 "$DAEMON_PID" 2>/dev/null || fail "daemon exited during the concurrent run"
if grep -qE 'panicked|Segmentation' "$LOG_FILE"; then
    fail "daemon log carries a panic or segfault"
fi

echo "TC-read-08: PASS (24 concurrent processes in ${ELAPSED}s, daemon alive, no panic)"

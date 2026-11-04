#!/usr/bin/env bash
# Design acceptance case TC-stats-03: a directory `.stats` aggregates exactly
# the torrents under its `source_path` prefix and carries the documented
# segments (`docs/03-acceptance.md` §E AC-42).
#
# Container-executable form: released binary, three torrents in two subtrees.
#
# Usage: ./tests/acceptance/tc_stats_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-03
write_fast_config

tracker_start
build_torrent "$TORRENT_FILES/root.torrent" --name root.bin \
    --content "$(payload_patterned p0.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"
ROOT_TORRENT="$TORRENT"
build_torrent "$TORRENT_FILES/a.torrent" --name a.bin \
    --content "$(payload_patterned p1.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"
A_TORRENT="$TORRENT"
build_torrent "$TORRENT_FILES/b.torrent" --name b.bin \
    --content "$(payload_patterned p2.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"
B_TORRENT="$TORRENT"

daemon_start
mkdir -p "$MNT/metadata/sub"
ingest "$ROOT_TORRENT" >/dev/null
ingest "$A_TORRENT" "$MNT/metadata/sub/a.torrent" >/dev/null
ingest "$B_TORRENT" "$MNT/metadata/sub/b.torrent" >/dev/null
wait_path "$MNT/data/sub/.stats" 60 || fail "data/sub/.stats did not appear"

for seg in Summary Rates Peers Cache Torrents; do
    grep -q -- "-- $seg --" "$MNT/data/sub/.stats" || fail "data/sub/.stats is missing the $seg segment"
    grep -q -- "-- $seg --" "$MNT/data/.stats" || fail "data/.stats is missing the $seg segment"
done
grep -q "torrentfs v" "$MNT/data/sub/.stats" && fail "directory .stats carries the version banner"
grep -q "torrentfs v" "$MNT/data/.stats" && fail "directory .stats carries the version banner"

SUB_COUNT="$(sed -n 's/^  Torrents: \([0-9]*\).*/\1/p' "$MNT/data/sub/.stats" | head -n1)"
ROOT_COUNT="$(sed -n 's/^  Torrents: \([0-9]*\).*/\1/p' "$MNT/data/.stats" | head -n1)"
[ "$SUB_COUNT" = 2 ] || fail "data/sub/.stats counts $SUB_COUNT torrents, expected 2"
[ "$ROOT_COUNT" = 3 ] || fail "data/.stats counts $ROOT_COUNT torrents, expected 3"

# The subdirectory aggregate lists both of its own torrents, not the root one.
grep -q "a.bin" "$MNT/data/sub/.stats" || fail "a.bin missing from data/sub/.stats"
grep -q "b.bin" "$MNT/data/sub/.stats" || fail "b.bin missing from data/sub/.stats"
grep -q "root.bin" "$MNT/data/sub/.stats" && fail "root.bin leaked into the sub-directory aggregate"

echo "TC-stats-03: PASS (data/sub/.stats=2 torrents, data/.stats=3, all five segments, aggregated by prefix)"

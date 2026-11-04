#!/usr/bin/env bash
# Design acceptance case TC-stats-04: writing a new torrent into `metadata/`
# raises the root total and the directory count by one immediately
# (`docs/03-acceptance.md` §E AC-43).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_stats_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-04
write_fast_config

tracker_start
A_TORRENT=""
for name in a b; do
    build_torrent "$TORRENT_FILES/$name.torrent" --name "$name.bin" \
        --content "$(payload_patterned "$name.bin" 65536)" --piece-length 16384 --tracker "$TRACKER_URL"
    eval "${name^^}_TORRENT=\"\$TORRENT\""
done

daemon_start
mkdir -p "$MNT/metadata/sub"
ingest "$A_TORRENT" "$MNT/metadata/sub/a.torrent" >/dev/null

root_total() { sed -n 's/^  Total: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1; }
dir_total() { sed -n 's/^  Torrents: \([0-9]*\).*/\1/p' "$MNT/data/sub/.stats" | head -n1; }

BEFORE_ROOT="$(root_total)"
BEFORE_DIR="$(dir_total)"
[ "$BEFORE_ROOT" = 1 ] || fail "root total before the write is $BEFORE_ROOT, expected 1"
[ "$BEFORE_DIR" = 1 ] || fail "directory count before the write is $BEFORE_DIR, expected 1"

ingest "$B_TORRENT" "$MNT/metadata/sub/b.torrent" >/dev/null
wait_path "$MNT/data/sub/b.torrent" 60 || fail "second projection did not appear"

# The counters are re-rendered per read; poll briefly for the publish tick.
AFTER_ROOT=""
AFTER_DIR=""
for _ in $(seq 1 40); do
    AFTER_ROOT="$(root_total)"
    AFTER_DIR="$(dir_total)"
    [ "$AFTER_ROOT" = 2 ] && [ "$AFTER_DIR" = 2 ] && break
    sleep 0.25
done
[ "$AFTER_ROOT" = 2 ] || fail "root total after the write is $AFTER_ROOT, expected 2"
[ "$AFTER_DIR" = 2 ] || fail "directory count after the write is $AFTER_DIR, expected 2"

echo "TC-stats-04: PASS (root Total $BEFORE_ROOT→$AFTER_ROOT, data/sub Torrents $BEFORE_DIR→$AFTER_DIR)"

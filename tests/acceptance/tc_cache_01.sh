#!/usr/bin/env bash
# Design acceptance case TC-cache-01: the first read lands the data on disk and
# the second read is served from the cache without downloading again
# (`docs/03-acceptance.md` §D AC-30).
#
# Container-executable form: released binary; the fixture peer counts every
# PIECE it sends, so "no re-download" is observed directly.
#
# Usage: ./tests/acceptance/tc_cache_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-cache-01
write_fast_config

SRC="$(payload_patterned payload.bin 4194304)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FILE="$MNT/data/seed.torrent/seed.bin"

read_retry "$FILE" "$W/first" 3 || fail "cold read failed"
SERVED_AFTER_FIRST="$(peer_served)"
CACHE_BYTES="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "${CACHE_BYTES:-0}" -gt 0 ] || fail "cache directory is still empty after the cold read"
HITS_BEFORE="$(sed -n 's/^  Hits: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
PIECES_LINE="$(grep -m1 '^Cache:' "$MNT/data/seed.torrent/.stats")"
echo "  cold read: served=$SERVED_AFTER_FIRST cache=${CACHE_BYTES}B hits=$HITS_BEFORE '${PIECES_LINE}'"

read_retry "$FILE" "$W/second" 1 || fail "second read failed"
cmp -s "$W/second" "$SRC" || fail "second read content differs from the source"
SERVED_AFTER_SECOND="$(peer_served)"
[ "$SERVED_AFTER_SECOND" = "$SERVED_AFTER_FIRST" ] \
    || fail "second read re-downloaded: peer served $SERVED_AFTER_FIRST → $SERVED_AFTER_SECOND pieces"

HITS_AFTER="$(sed -n 's/^  Hits: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "${HITS_AFTER:-0}" -gt "${HITS_BEFORE:-0}" ] \
    || fail "cache hit counter did not grow (${HITS_BEFORE} → ${HITS_AFTER})"
CACHE_BYTES_AFTER="$(du -sb "$CACHE_DIR" | cut -f1)"
[ "$CACHE_BYTES_AFTER" -ge "$CACHE_BYTES" ] || fail "cache usage shrank across the hit"

echo "TC-cache-01: PASS (second read served from cache: hits ${HITS_BEFORE}→${HITS_AFTER}, peer served unchanged at ${SERVED_AFTER_SECOND}, cache=${CACHE_BYTES_AFTER}B)"

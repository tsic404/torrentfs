#!/usr/bin/env bash
# Design acceptance case TC-cache-05: a read the on-disk cache cannot serve
# fails with ENODATA and the cache-stall diagnostic naming its evidence
# (`docs/03-acceptance.md` §D AC-34).
#
# Container-executable form: released binary; 1 KiB cache cap against a
# 1 MiB read, with a connected seeder so the classification is cache-side.
#
# Usage: ./tests/acceptance/tc_cache_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-cache-05

SRC="$(payload_patterned payload.bin 1048576)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin --content "$SRC" \
    --piece-length 262144 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC" --stall
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

write_config <<'TOML'
[timeouts]
read_timeout_secs = 6
peer_discovery_wait_secs = 5
no_seeder_read_timeout_secs = 3
[cache]
cache_size = 1024
[connections]
listen_interfaces = "127.0.0.1:0"
[dht]
enabled = false
TOML
daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
wait_seeds "$MNT/data/seed.torrent/.stats" 1 20 || fail "the stalled seeder never showed up as connected"

rc=0
read_retry "$MNT/data/seed.torrent/seed.bin" "$W/out" 1 || rc=$?
[ "$rc" -ne 0 ] || fail "read larger than the cache cap succeeded, expected ENODATA"
grep -q "No data available" "$W/read.err" \
    || { cat "$W/read.err" >&2; fail "the failed read did not surface ENODATA"; }

grep -qE "read stalled on the on-disk cache \(cache_size=[0-9.]+ MiB, read span=[0-9.]+ MiB, read span exceeds cache\)" "$LOG_FILE" \
    || { grep -n "read stalled" "$LOG_FILE" >&2 || true; fail "cache-stall diagnostic with its evidence is missing"; }
LINE="$(grep -m1 'read stalled on the on-disk cache' "$LOG_FILE")"
echo "  $(echo "$LINE" | sed 's/^.*read stalled/read stalled/')"

# The stall must not wedge the observability surface.
grep -q '^  Status: ' "$MNT/data/seed.torrent/.stats" \
    || fail "leaf .stats unreadable after the cache stall"

echo "TC-cache-05: PASS (ENODATA + cache-stall prefix with the 'read span exceeds cache' evidence)"

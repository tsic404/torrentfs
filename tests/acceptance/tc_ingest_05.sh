#!/usr/bin/env bash
# Design acceptance case TC-ingest-05: the same torrent bytes in two
# `source_path`s share one content record and project the same state, while a
# same-info_hash / different-bytes torrent stays independent
# (`docs/03-acceptance.md` §A AC-05).
#
# Container-executable form: released binary; the unique-content counter is the
# observable "one content record", and both leaf `.stats` are compared verbatim
# on Status / Progress / DL and the Pieces run.
#
# Usage: ./tests/acceptance/tc_ingest_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-ingest-05
write_fast_config

SRC="$(payload_patterned payload.bin 131072)"
tracker_start
build_torrent "$TORRENT_FILES/t.torrent" --name t.bin --content "$SRC" \
    --piece-length 16384 --tracker "$TRACKER_URL"
SAME_BYTES="$TORRENT"
SAME_HASH_A="$INFOHASH"
# Same info dict (same info hash), different announce URL → different bytes.
build_torrent "$TORRENT_FILES/u.torrent" --name t.bin --content "$SRC" \
    --piece-length 16384 --tier "$TRACKER_URL,http://127.0.0.1:9/other"
OTHER_BYTES="$TORRENT"
[ "$INFOHASH" = "$SAME_HASH_A" ] || fail "fixture invariant broken: info hashes differ ($SAME_HASH_A vs $INFOHASH)"
[ "$(sha256sum < "$SAME_BYTES" | cut -d' ' -f1)" != "$(sha256sum < "$OTHER_BYTES" | cut -d' ' -f1)" ] \
    || fail "fixture invariant broken: the two torrents are byte-identical"

# The seeder announces before the client ingests, so the client's first
# announce discovers it; a peer that appears only after the client's announce
# stays invisible until the next announce interval and the read would time out.
peer_start "$SAME_HASH_A" 16384 "$SRC"
wait_peer_known "$SAME_HASH_A" 1 || fail "the seeder never registered with the tracker"

daemon_start
mkdir -p "$MNT/metadata/x" "$MNT/metadata/y" "$MNT/metadata/z"
ingest "$SAME_BYTES" "$MNT/metadata/x/t.torrent" >/dev/null
ingest "$SAME_BYTES" "$MNT/metadata/y/t.torrent" >/dev/null
X_STATS="$MNT/data/x/t.torrent/.stats"
Y_STATS="$MNT/data/y/t.torrent/.stats"

summarize() {
    {
        sed -n 's/^  \(Status:.*\)$/\1/p' "$1"
        sed -n 's/^  \(Pieces: .*\)$/\1/p' "$1"
    }
}

for _ in $(seq 1 40); do
    [ -f "$X_STATS" ] && [ -f "$Y_STATS" ] && break
    sleep 0.25
done
[ -f "$X_STATS" ] && [ -f "$Y_STATS" ] || fail "both projections must publish a leaf .stats"
[ -f "$MNT/data/x/t.torrent/t.bin" ] || fail "data/x/t.torrent/t.bin missing"
[ -f "$MNT/data/y/t.torrent/t.bin" ] || fail "data/y/t.torrent/t.bin missing"

# Two copies at zero progress are verbatim-equal even when the daemon kept two
# independent content records (both read `Status: Pending` with an all-`[]`
# run).  Read through data/x and require both leaves to carry a cached marker:
# only one shared content record can carry downloaded state into the second
# projection.
wait_peer_known "$SAME_HASH_A" 2 || fail "the client never announced alongside the seeder"
read_retry "$MNT/data/x/t.torrent/t.bin" "$W/x.out" 3 || fail "reading through data/x did not complete"
cmp -s "$W/x.out" "$SRC" || fail "the read through data/x returned the wrong bytes"
CACHED_X=0
CACHED_Y=0
for _ in $(seq 1 40); do
    grep -qE '\[(x|X [0-9]+)\]' "$X_STATS" && CACHED_X=1
    grep -qE '\[(x|X [0-9]+)\]' "$Y_STATS" && CACHED_Y=1
    [ "$CACHED_X" = 1 ] && [ "$CACHED_Y" = 1 ] && break
    sleep 0.25
done
[ "$CACHED_X" = 1 ] || fail "data/x reported no cached piece after its read"
[ "$CACHED_Y" = 1 ] \
    || fail "data/y carries no cached piece although data/x downloaded — the copies do not share one content record"

diff -u <(summarize "$X_STATS") <(summarize "$Y_STATS") > "$W/stats.diff" \
    || { cat "$W/stats.diff" >&2; fail "the two source copies do not report identical state"; }

TOTAL="$(sed -n 's/^  Total: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
UNIQUE="$(sed -n 's/^  Total: [0-9]*  Unique: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "$TOTAL" = 2 ] || fail "root total is $TOTAL, expected 2 sources"
# `Unique` counts distinct info hashes (C-6.2.4), not content records: both
# copies share one info hash, so 1 is only a keying sanity check.  The shared
# downloaded state in both leaves is the content-record evidence.
[ "$UNIQUE" = 1 ] || fail "root unique info-hash count is $UNIQUE, expected 1 (sanity: one info hash)"

# Same info hash, different bytes: its own directory and its own entry.
ingest "$OTHER_BYTES" "$MNT/metadata/z/t.torrent" >/dev/null
Z_STATS="$MNT/data/z/t.torrent/.stats"
for _ in $(seq 1 40); do
    [ -f "$Z_STATS" ] && break
    sleep 0.25
done
[ -f "$Z_STATS" ] || fail "the different-bytes torrent did not get its own projection"
[ -f "$MNT/data/z/t.torrent/t.bin" ] || fail "the different-bytes projection has no content entry"
TOTAL="$(sed -n 's/^  Total: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
UNIQUE="$(sed -n 's/^  Total: [0-9]*  Unique: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "$TOTAL" = 3 ] || fail "root total is $TOTAL after the third source, expected 3"
# Unique counts distinct info hashes (C-6.2.4): the third torrent shares one.
[ "$UNIQUE" = 1 ] || fail "root unique info-hash count is $UNIQUE, expected 1"
# Each source still has its own directory and its own leaf stats entry.
for d in x y z; do
    [ -f "$MNT/data/$d/t.torrent/.stats" ] || fail "data/$d/t.torrent lost its own .stats entry"
done

echo "TC-ingest-05: PASS (identical bytes: one shared content record — data/x downloaded and both leaves carry cached pieces with verbatim-equal state; different bytes same info_hash: its own dir and .stats, Total=3 Unique=1 by info_hash)"

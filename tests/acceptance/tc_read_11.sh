#!/usr/bin/env bash
# Design acceptance case TC-read-11: files of a multi-file torrent whose piece
# spans a file boundary read back correctly
# (`docs/03-acceptance.md` §B AC-10).
#
# Container-executable form: released binary, synthesized local swarm whose
# piece 0 covers the tail of file_A and the head of file_B.
#
# Usage: ./tests/acceptance/tc_read_11.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read11}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-11
write_fast_config

# 10000 bytes in file_A and 20000 in file_B, with 16384-byte pieces: piece 0 is
# file_A[0..10000) + file_B[0..6384).  The two files are consecutive slices of
# one patterned payload, so a piece mapped at the wrong offset (file_B[k..]) or
# the two files swapped cannot pass the digest comparison — a monotone fill
# would let a shifted read return the right byte value throughout.
SRC_DIR="$W/src"
mkdir -p "$SRC_DIR"
FULL="$(payload_patterned full.bin 30000)"
head -c 10000 "$FULL" > "$SRC_DIR/file_A"
tail -c 20000 "$FULL" > "$SRC_DIR/file_B"
tracker_start
build_torrent "$TORRENT_FILES/movie.torrent" --name movie --payload-dir "$SRC_DIR" \
    --path file_A --path file_B --piece-length 16384 --tracker "$TRACKER_URL"
[ "$TOTAL_SIZE" = 30000 ] || fail "torrent declares $TOTAL_SIZE bytes, expected 30000"
[ "$PIECE_COUNT" = 2 ] || fail "torrent declares $PIECE_COUNT pieces, expected 2"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC_DIR/file_A" "$SRC_DIR/file_B"
wait_peer_known "$INFOHASH" 1 || fail "peer never registered with the tracker"

daemon_start
ingest "$TORRENT" >/dev/null
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
ROOT="$MNT/data/movie.torrent/movie"
[ -d "$ROOT" ] || fail "multi-file seed root movie/ missing"
[ "$(stat -c '%a' "$ROOT")" = 555 ] || fail "seed root is not 0555"

for name in file_A file_B; do
    FILE="$ROOT/$name"
    [ -f "$FILE" ] || fail "$name not projected"
    [ "$(stat -c '%a' "$FILE")" = 444 ] || fail "$name is not 0444"
    declared="$(stat -c '%s' "$FILE")"
    want="$(stat -c '%s' "$SRC_DIR/$name")"
    [ "$declared" = "$want" ] || fail "$name declares $declared bytes, source has $want"
    read_retry "$FILE" "$W/$name.got" 3 || fail "$name read failed"
    got="$(stat -c '%s' "$W/$name.got")"
    [ "$got" = "$declared" ] || fail "$name returned $got bytes, stat says $declared"
    cmp -s "$W/$name.got" "$SRC_DIR/$name" || fail "$name content differs from the source"
    echo "  $name: $got bytes, sha256=$(digest "$W/$name.got")"
done

SERVED="$(peer_served)"
[ "${SERVED:-0}" -gt 0 ] || fail "no piece was ever served (fixture peer delivered nothing)"
echo "TC-read-11: PASS (file_A 10000B + file_B 20000B across a shared piece, both byte-exact, ${SERVED} pieces served)"

#!/usr/bin/env bash
# Design acceptance case TC-read-02: offset/length reads (head / middle / tail)
# return the requested byte count with correct content
# (`docs/03-acceptance.md` §B AC-11).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_read_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-02
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

# Head / middle / tail ranges, each a different piece-aligned-independent span.
RANGES=("0 131072 head" "2000000 131072 middle" "4063232 131072 tail")
for spec in "${RANGES[@]}"; do
    read -r off len name <<< "$spec"
    # An uncached range read waits for its pieces; retry a failed attempt.
    GOT=0
    for _ in 1 2 3; do
        slice "$FILE" "$off" "$len" "$W/$name.got"
        GOT="$(stat -c '%s' "$W/$name.got")"
        [ "$GOT" = "$len" ] && break
        sleep 1
    done
    [ "$GOT" = "$len" ] || fail "range $name returned $GOT bytes, requested $len"
    slice "$SRC" "$off" "$len" "$W/$name.want"
    cmp -s "$W/$name.got" "$W/$name.want" || fail "range $name content differs from the source"
    echo "  $name: offset=$off len=$len bytes=$GOT digest=$(digest "$W/$name.got")"
done

# A chunked read of the same region must concatenate to the source bytes.
: > "$W/joined"
for off in 0 65536 131072; do
    slice "$FILE" "$off" 65536 "$W/part.$off"
    cat "$W/part.$off" >> "$W/joined"
done
slice "$SRC" 0 196608 "$W/joined.want"
cmp -s "$W/joined" "$W/joined.want" || fail "concatenated chunk reads differ from the source"

echo "TC-read-02: PASS (3 independent ranges exact, 3x64KiB concatenation exact)"

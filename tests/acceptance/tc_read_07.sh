#!/usr/bin/env bash
# Design acceptance case TC-read-07: eight concurrent reads at different
# offsets (one past EOF) each return their requested bytes
# (`docs/03-acceptance.md` §B AC-15).
#
# Container-executable form: released binary, synthesized local swarm.
#
# Usage: ./tests/acceptance/tc_read_07.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read07}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-read-07
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
SIZE="$(stat -c '%s' "$FILE")"

OFFSETS=(0 524288 1048576 1572864 2097152 3145728 3800000 4194304)
LEN=131072
READERS=()
for i in "${!OFFSETS[@]}"; do
    off="${OFFSETS[$i]}"
    ( slice "$FILE" "$off" "$LEN" "$W/seg.$i" 2>"$W/err.$i"; echo $? > "$W/rc.$i" ) &
    READERS+=($!)
done
for pid in "${READERS[@]}"; do wait "$pid"; done

for i in "${!OFFSETS[@]}"; do
    off="${OFFSETS[$i]}"
    rc="$(cat "$W/rc.$i")"
    [ "$rc" = 0 ] || { cat "$W/err.$i" >&2; fail "reader at offset $off exited $rc"; }
    got="$(stat -c '%s' "$W/seg.$i")"
    if [ "$off" -ge "$SIZE" ]; then
        [ "$got" = 0 ] || fail "past-EOF read at $off returned $got bytes, expected 0"
        continue
    fi
    [ "$got" = "$LEN" ] || fail "reader at offset $off returned $got bytes, expected $LEN"
    slice "$SRC" "$off" "$LEN" "$W/want.$i"
    cmp -s "$W/seg.$i" "$W/want.$i" || fail "reader at offset $off content differs from source"
done

echo "TC-read-07: PASS (8 concurrent offset reads: 7 exact 131072-byte spans, 1 past-EOF 0 bytes)"

#!/usr/bin/env bash
# Design acceptance case TC-read-04: `stat` of a data file reports the full
# torrent-metadata size (not 0, not the downloaded size) with `0444`
# permissions (`docs/03-acceptance.md` §B AC-13).
#
# FUSE-level executable form.  Requires a FUSE-capable environment, the
# release binary, and a .torrent to ingest (this TC uses the shared valid
# fixture builder from tc_ingest_01).
#
# Usage: ./tests/acceptance/tc_read_04.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_read04}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

TORRENT_FILE="$(mktemp)"
trap 'rm -f "$TORRENT_FILE"; cleanup' EXIT
make_seed_torrent "$TORRENT_FILE"
cp "$TORRENT_FILE" "$MNT/metadata/seed.torrent"

for _ in $(seq 1 60); do
    [ -e "$MNT/data/seed.torrent/seed.txt" ] && break
    sleep 1
done
[ -e "$MNT/data/seed.torrent/seed.txt" ] || {
    echo "TC-read-04: FAIL — data/seed.torrent/seed.txt did not appear" >&2
    exit 1
}

FILE="$MNT/data/seed.torrent/seed.txt"
PERMS="$(stat -c '%a' "$FILE")"
SIZE="$(stat -c '%s' "$FILE")"
TYPE="$(stat -c '%F' "$FILE")"

# The stat size must equal the torrent-metadata declared size (33 bytes) —
# not 0 and not the downloaded size.
[ "$PERMS" = "444" ] || {
    echo "TC-read-04: FAIL — perms=$PERMS, expected 444" >&2
    exit 1
}
[ "$SIZE" = "33" ] || {
    echo "TC-read-04: FAIL — size=$SIZE, expected 33 (torrent metadata value)" >&2
    exit 1
}
[ "$TYPE" = "regular file" ] || {
    echo "TC-read-04: FAIL — type=$TYPE, expected regular file" >&2
    exit 1
}

echo "TC-read-04: PASS (stat: 444 regular file, 33 bytes from metadata)"

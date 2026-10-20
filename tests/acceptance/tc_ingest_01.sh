#!/usr/bin/env bash
# Design acceptance case TC-ingest-01: a `.torrent` copied into `metadata/`
# projects a `data/` directory named after the full filename (`.torrent`
# suffix included), with `0555` directory / `0444` file permissions and file
# sizes from the torrent metadata (`docs/03-acceptance.md` §A AC-01).
#
# FUSE-level executable form.  Requires a FUSE-capable environment and the
# release binary built (`cargo build --release`).
#
# Usage: ./tests/acceptance/tc_ingest_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

TORRENT_FILE="$(mktemp)"
trap 'rm -f "$TORRENT_FILE"; cleanup' EXIT
make_seed_torrent "$TORRENT_FILE"

cp "$TORRENT_FILE" "$MNT/metadata/seed.torrent"

# Wait for the projection to appear under data/.
for _ in $(seq 1 60); do
    [ -d "$MNT/data/seed.torrent" ] && break
    sleep 1
done
[ -d "$MNT/data/seed.torrent" ] || {
    echo "TC-ingest-01: FAIL — data/seed.torrent did not appear" >&2
    exit 1
}

# Directory name keeps the `.torrent` suffix; directory is 0555.
DIR_PERMS="$(stat -c '%a' "$MNT/data/seed.torrent")"
[ "$DIR_PERMS" = "555" ] || {
    echo "TC-ingest-01: FAIL — data/seed.torrent perms=$DIR_PERMS, expected 555" >&2
    exit 1
}

# The file inside is 0444 with the torrent-metadata size (33 bytes).
FILE="$MNT/data/seed.torrent/seed.txt"
FILE_PERMS="$(stat -c '%a' "$FILE")"
FILE_SIZE="$(stat -c '%s' "$FILE")"
[ "$FILE_PERMS" = "444" ] || {
    echo "TC-ingest-01: FAIL — seed.txt perms=$FILE_PERMS, expected 444" >&2
    exit 1
}
[ "$FILE_SIZE" = "33" ] || {
    echo "TC-ingest-01: FAIL — seed.txt size=$FILE_SIZE, expected 33" >&2
    exit 1
}

echo "TC-ingest-01: PASS (data/seed.torrent 555, seed.txt 444, 33 bytes)"

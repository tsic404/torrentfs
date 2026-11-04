#!/usr/bin/env bash
# Design acceptance case TC-ingest-02: the multi-level `metadata/` hierarchy is
# mirrored under `data/`, a root-level torrent lands at the root, and every
# intermediate directory and seed root is 0555
# (`docs/03-acceptance.md` §A AC-02).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_ingest_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-ingest-02
write_fast_config

tracker_start
build_torrent "$TORRENT_FILES/deep.torrent" --name deep.bin \
    --content "$(payload_patterned deep.bin 65536)" --piece-length 16384 --tracker "$TRACKER_URL"
DEEP="$TORRENT"
build_torrent "$TORRENT_FILES/root.torrent" --name root.bin \
    --content "$(payload_patterned root.bin 32768)" --piece-length 16384 --tracker "$TRACKER_URL"
ROOT_TORRENT="$TORRENT"

daemon_start
mkdir -p "$MNT/metadata/a/b/c"
ingest "$DEEP" "$MNT/metadata/a/b/c/deep.torrent" >/dev/null
ingest "$ROOT_TORRENT" "$MNT/metadata/root.torrent" >/dev/null

for dir in "$MNT/data/a" "$MNT/data/a/b" "$MNT/data/a/b/c" "$MNT/data/a/b/c/deep.torrent" "$MNT/data/root.torrent"; do
    [ -d "$dir" ] || fail "projection directory missing: ${dir#"$MNT"}"
    mode="$(stat -c '%a' "$dir")"
    [ "$mode" = 555 ] || fail "${dir#"$MNT"} has mode $mode, expected 555"
done
[ -f "$MNT/data/a/b/c/deep.torrent/deep.bin" ] || fail "nested file deep.bin missing"
[ -f "$MNT/data/root.torrent/root.bin" ] || fail "root-level seed content missing at data/root.torrent"
[ "$(stat -c '%s' "$MNT/data/a/b/c/deep.torrent/deep.bin")" = 65536 ] || fail "nested file size differs from metadata"
[ "$(stat -c '%s' "$MNT/data/root.torrent/root.bin")" = 32768 ] || fail "root-level file size differs from metadata"

echo "TC-ingest-02: PASS (a/b/c mirrored with 0555 at every level, root-level seed at data/root.torrent)"

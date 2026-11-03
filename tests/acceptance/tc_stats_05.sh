#!/usr/bin/env bash
# Design acceptance case TC-stats-05: no `.stats` appears inside a seed's own
# subdirectories or anywhere in the `metadata/` tree
# (`docs/03-acceptance.md` §E AC-44).
#
# Container-executable form: released binary, multi-file seed with a nested
# directory and a metadata subtree.
#
# Usage: ./tests/acceptance/tc_stats_05.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats05}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-05
write_fast_config

SRC_DIR="$W/src"
mkdir -p "$SRC_DIR/subdir"
head -c 16384 /dev/zero | tr '\0' 'a' > "$SRC_DIR/file_A"
head -c 16384 /dev/zero | tr '\0' 'c' > "$SRC_DIR/subdir/file_C"
tracker_start
build_torrent "$TORRENT_FILES/movie.torrent" --name movie --payload-dir "$SRC_DIR" \
    --path file_A --path subdir/file_C --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
mkdir -p "$MNT/metadata/deep/nested"
ingest "$TORRENT" "$MNT/metadata/deep/nested/movie.torrent" >/dev/null
SEED_DIR="$MNT/data/deep/nested/movie.torrent"
ROOT_DIR_PROJ="$SEED_DIR/movie"
wait_path "$ROOT_DIR_PROJ/subdir/file_C" 60 || fail "nested projection did not appear"

# Allowed stats files: root, data/, the source_path directory and the seed root
# (`data/<path>/<filename>.torrent/`), never inside the projected content tree.
grep -q -- "-- Status --" "$SEED_DIR/.stats" || fail "seed-root .stats missing"
for excluded in "$ROOT_DIR_PROJ" "$ROOT_DIR_PROJ/subdir" "$MNT/metadata" \
    "$MNT/metadata/deep" "$MNT/metadata/deep/nested"; do
    if [ -e "$excluded/.stats" ]; then
        fail "$excluded/.stats exists but must not"
    fi
done
HITS="$(find "$MNT/metadata" -name '.stats' 2>/dev/null | wc -l)"
[ "$HITS" = 0 ] || fail "metadata tree exposes $HITS .stats entries"
HITS="$(find "$SEED_DIR" -mindepth 2 -name '.stats' 2>/dev/null | wc -l)"
[ "$HITS" = 0 ] || fail "seed content tree exposes $HITS .stats entries below the seed root"

# A per-directory listing must not carry the virtual file either.
ls -a "$ROOT_DIR_PROJ/subdir" | grep -qx '.stats' && fail "seed subdirectory lists a .stats entry"
ls -a "$MNT/metadata/deep/nested" | grep -qx '.stats' && fail "metadata directory lists a .stats entry"

echo "TC-stats-05: PASS (no .stats under the seed subdirectory or the metadata tree; seed root has one)"

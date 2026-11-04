#!/usr/bin/env bash
# Design acceptance case TC-ingest-08: a multi-file torrent projects a
# first-level directory named by `info.name`, with every file path expanded
# beneath it, directories 0555 and files 0444
# (`docs/03-acceptance.md` §A AC-08).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_ingest_08.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest08}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-ingest-08
write_fast_config

SRC_DIR="$W/src"
mkdir -p "$SRC_DIR/sub/deeper"
head -c 10000 /dev/zero | tr '\0' 'a' > "$SRC_DIR/file_A"
head -c 20000 /dev/zero | tr '\0' 'b' > "$SRC_DIR/sub/file_B"
head -c 5000 /dev/zero | tr '\0' 'c' > "$SRC_DIR/sub/deeper/file_C"
tracker_start
build_torrent "$TORRENT_FILES/movie.torrent" --name movie --payload-dir "$SRC_DIR" \
    --path file_A --path sub/file_B --path sub/deeper/file_C \
    --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
ingest "$TORRENT" >/dev/null
SEED_ROOT="$MNT/data/movie.torrent/movie"
[ -d "$SEED_ROOT" ] || fail "first-level projection must be a directory named by info.name (movie)"
[ "$(stat -c '%a' "$SEED_ROOT")" = 555 ] || fail "seed root is not 0555"

for rel in file_A sub sub/file_B sub/deeper sub/deeper/file_C; do
    [ -e "$SEED_ROOT/$rel" ] || fail "declared path $rel missing from the projection"
done
for rel in sub sub/deeper; do
    [ "$(stat -c '%a' "$SEED_ROOT/$rel")" = 555 ] || fail "$rel is not a 0555 directory"
done
for rel in file_A sub/file_B sub/deeper/file_C; do
    [ "$(stat -c '%F' "$SEED_ROOT/$rel")" = "regular file" ] || fail "$rel is not a regular file"
    [ "$(stat -c '%a' "$SEED_ROOT/$rel")" = 444 ] || fail "$rel is not 0444"
    declared="$(stat -c '%s' "$SEED_ROOT/$rel")"
    want="$(stat -c '%s' "$SRC_DIR/$rel")"
    [ "$declared" = "$want" ] || fail "$rel declares $declared bytes, metadata says $want"
done

# The projected tree contains exactly the declared paths, nothing invented.
LISTED="$(cd "$SEED_ROOT" && find . -mindepth 1 ! -name '.stats' | sort)"
EXPECTED="$(printf '%s\n' ./file_A ./sub ./sub/deeper ./sub/deeper/file_C ./sub/file_B | sort)"
[ "$LISTED" = "$EXPECTED" ] || {
    diff <(printf '%s\n' "$EXPECTED") <(printf '%s\n' "$LISTED") >&2
    fail "projected tree does not match the declared name + file paths"
}

echo "TC-ingest-08: PASS (movie/ 0555 with file_A, sub/file_B, sub/deeper/file_C 0444 and metadata sizes)"

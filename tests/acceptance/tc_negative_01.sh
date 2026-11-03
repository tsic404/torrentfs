#!/usr/bin/env bash
# Design acceptance case TC-negative-01: F1 — the `data/` namespace never
# accepts a mutation; every attempt fails and leaves no trace in the projection
# (`docs/03-acceptance.md` §J AC-90).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_negative_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-01
write_fast_config

SRC_DIR="$W/src"
mkdir -p "$SRC_DIR/sub"
head -c 65536 /dev/zero | tr '\0' 'a' > "$SRC_DIR/file_A"
head -c 16384 /dev/zero | tr '\0' 'b' > "$SRC_DIR/sub/file_B"
tracker_start
build_torrent "$TORRENT_FILES/movie.torrent" --name movie --payload-dir "$SRC_DIR" \
    --path file_A --path sub/file_B --piece-length 16384 --tracker "$TRACKER_URL"

daemon_start
ingest "$TORRENT" >/dev/null
SEED_ROOT="$MNT/data/movie.torrent/movie"
[ -f "$SEED_ROOT/file_A" ] || fail "projection missing before the mutations"

snapshot() {
    ( cd "$MNT/data" && find . -mindepth 1 ! -name '.stats' -printf '%p %s %m %y\n' | sort )
}
BEFORE="$(snapshot)"

attempt() {
    local label="$1"
    shift
    local rc=0
    "$@" 2> "$W/$label.err" || rc=$?
    [ "$rc" -ne 0 ] || fail "$label succeeded but data/ must be read-only"
    grep -qE "Read-only file system|Operation not permitted" "$W/$label.err" \
        || { cat "$W/$label.err" >&2; fail "$label did not report a read-only errno"; }
}

attempt create-file sh -c 'printf x > "$1"' _ "$SEED_ROOT/new.txt"
attempt mkdir mkdir "$SEED_ROOT/newdir"
attempt unlink rm -f "$SEED_ROOT/file_A"
attempt rmdir rmdir "$SEED_ROOT/sub"
attempt symlink ln -s /etc/passwd "$SEED_ROOT/link"
attempt chmod chmod 777 "$SEED_ROOT/file_A"
attempt chown chown 12345:12345 "$SEED_ROOT/file_A"
attempt truncate truncate -s 0 "$SEED_ROOT/file_A"
attempt rename mv "$SEED_ROOT/file_A" "$SEED_ROOT/file_A2"

AFTER="$(snapshot)"
[ "$BEFORE" = "$AFTER" ] || {
    diff <(printf '%s\n' "$BEFORE") <(printf '%s\n' "$AFTER") >&2
    fail "data/ projection changed across the refused mutations"
}

# No hidden residue: the seed root lists exactly the declared content tree.
LISTED="$(cd "$SEED_ROOT" && find . -mindepth 1 ! -name '.stats' | sort)"
EXPECTED="$(printf '%s\n' ./file_A ./sub ./sub/file_B | sort)"
[ "$LISTED" = "$EXPECTED" ] || {
    diff <(printf '%s\n' "$EXPECTED") <(printf '%s\n' "$LISTED") >&2
    fail "refused mutations left entries behind in data/"
}

echo "TC-negative-01: PASS (10 mutation attempts all refused with a read-only errno; projection set, sizes and modes unchanged)"

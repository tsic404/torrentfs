#!/usr/bin/env bash
# Design acceptance case TC-rofs-01: writes to `data/`, directory creation
# under `data/`, and deletion of a projected `data/` entry are all rejected
# with `EROFS`, and the namespace is unchanged (`docs/03-acceptance.md` §C
# AC-20).
#
# FUSE-level executable form.  Requires a FUSE-capable environment and the
# release binary.  A valid torrent is ingested first so there is a real
# projected entry to attempt the deletion on.
#
# Usage: ./tests/acceptance/tc_rofs_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_rofs01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

fail() {
    echo "TC-rofs-01: FAIL — $1" >&2
    exit 1
}

ERR_FILE="$(mktemp)"
TORRENT_FILE="$(mktemp)"
trap 'rm -f "$ERR_FILE" "$TORRENT_FILE"; cleanup' EXIT

# Ingest one torrent so `data/` carries a projected entry (file + directory)
# that the deletion attempt can target.
make_seed_torrent "$TORRENT_FILE"
cp "$TORRENT_FILE" "$MNT/metadata/seed.torrent"
for _ in $(seq 1 60); do
    [ -e "$MNT/data/seed.torrent/seed.txt" ] && break
    sleep 1
done
[ -e "$MNT/data/seed.torrent/seed.txt" ] || fail "projected seed.txt did not appear"

# Snapshot the data/ namespace before the attempts: entry set must not change.
BEFORE="$(find "$MNT/data" -mindepth 1 | sort)"

# 1. Write to a path inside data/ → EROFS.  The failed redirection is
#    reported by the *shell* (bash prints its own diagnostic for a failed
#    open), so run a nested bash whose whole stderr is captured.
WRITE_MSG="$(bash -c 'echo x > "$1/data/test"' _ "$MNT" 2>&1)" || true
if [ -z "$WRITE_MSG" ]; then
    fail "write into data/ succeeded (must be EROFS)"
fi
if ! grep -qi "read-only file system" <<<"$WRITE_MSG"; then
    fail "write into data/ must report a read-only error (got: $WRITE_MSG)"
fi

# 2. mkdir under data/ → EROFS.
MKDIR_MSG="$(bash -c 'mkdir "$1/data/newdir"' _ "$MNT" 2>&1)" || true
if [ -z "$MKDIR_MSG" ]; then
    fail "mkdir under data/ succeeded (must be EROFS)"
fi
if ! grep -qi "read-only file system" <<<"$MKDIR_MSG"; then
    fail "mkdir under data/ must report a read-only error (got: $MKDIR_MSG)"
fi

# 3. Deleting a *projected entry* under data/ → EROFS (AC-20's deletion face;
#    not the `.stats` EPERM surface that belongs to AC-22).
RM_MSG="$(bash -c 'rm -f "$1/data/seed.torrent/seed.txt"' _ "$MNT" 2>&1)" || true
if [ -z "$RM_MSG" ]; then
    fail "rm of a projected data/ file succeeded (must be EROFS)"
fi
if ! grep -qi "read-only file system" <<<"$RM_MSG"; then
    fail "rm under data/ must report a read-only error (got: $RM_MSG)"
fi

# The namespace is unchanged: same entries, and the target file still reads.
AFTER="$(find "$MNT/data" -mindepth 1 | sort)"
[ "$BEFORE" = "$AFTER" ] || fail "data/ entry set changed: before=[$BEFORE] after=[$AFTER]"
[ -e "$MNT/data/seed.torrent/seed.txt" ] || fail "projected seed.txt disappeared after rejected rm"

echo "TC-rofs-01: PASS (data/ write, mkdir, and projected-entry rm all EROFS; namespace unchanged)"

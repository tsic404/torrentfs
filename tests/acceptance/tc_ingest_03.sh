#!/usr/bin/env bash
# Design acceptance case TC-ingest-03: renaming or deleting a `metadata/` seed
# makes the `data/` projection follow at once, without waiting out the kernel's
# 1s entry cache (`docs/03-acceptance.md` §A AC-03; contracts C-1.3.4 /
# C-3.3.8).
#
# Mount form.  The service-layer module (`test_tc_ingest_03.rs`) sees only the
# daemon's own resolver; it cannot tell whether the kernel was told to drop the
# old dentry.  Here every check runs with no sleep after the mutation, and the
# stale name was looked up first, so removing `inval_data_entry` /
# `Notifier::inval_entry` leaves a resolvable stale entry and fails the case.
# `test -e` (not `ls`) is the probe: readdir always comes back from the daemon,
# while the dentry cache is the layer the daemon must invalidate.
#
# A directory rename is asserted the same way: the old `data/<dir>` must stop
# resolving at once and the moved subtree must be visible immediately.  The
# daemon's rename branch calls `inval_data_entry`, so the old-name assertion is
# now a regression guard: a red result here means the active invalidation
# regressed, and no TTL tolerance is applied.
#
# Usage: ./tests/acceptance/tc_ingest_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skipped (no FUSE 3 mount capability).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest03}"

# Self-gate: a real mount is required.  libfuse2's fusermount rejects the
# daemon's non-empty mountpoint (it carries `.torrentfs.lock`), so only a
# fuse3 environment can run this carrier.
command -v fusermount3 >/dev/null 2>&1 || {
    echo "tc_ingest_03: SKIP — fusermount3 (libfuse3) is required for a mount" >&2
    exit 2
}
[ -e /dev/fuse ] || { echo "tc_ingest_03: SKIP — /dev/fuse is not available" >&2; exit 2; }
[ -x "$BIN" ] || { echo "tc_ingest_03: SKIP — binary not found: $BIN" >&2; exit 2; }

# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

TORRENT_FILE="$(mktemp)"
trap 'rm -f "$TORRENT_FILE"; cleanup' EXIT

fail() {
    echo "tc_ingest_03: FAIL — $1" >&2
    echo "  --- daemon log (tail) ---" >&2
    tail -n 20 "$LOG_FILE" >&2 2>/dev/null || true
    exit 1
}

make_seed_torrent "$TORRENT_FILE" ingest-03-seed.txt
cp "$TORRENT_FILE" "$MNT/metadata/seed.torrent"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -d "$MNT/data/seed.torrent" ] && break
    sleep 0.1
done
[ -d "$MNT/data/seed.torrent" ] || fail "data/seed.torrent did not appear"
stat "$MNT/data/seed.torrent" >/dev/null   # hold the dentry in the cache

# Rename: the old projected name must be gone at once.
mv "$MNT/metadata/seed.torrent" "$MNT/metadata/renamed.torrent"
if [ -e "$MNT/data/seed.torrent" ]; then
    fail "data/seed.torrent still resolves right after the rename (stale dentry)"
fi
[ -d "$MNT/data/renamed.torrent" ] || fail "data/renamed.torrent did not appear immediately"

# Delete: the projected name must be gone at once.
stat "$MNT/data/renamed.torrent" >/dev/null
rm "$MNT/metadata/renamed.torrent"
if [ -e "$MNT/data/renamed.torrent" ]; then
    fail "data/renamed.torrent still resolves right after the delete (stale dentry)"
fi

# Directory delete: the mirrored directory must vanish at once.
mkdir -p "$MNT/metadata/tree/deep"
cp "$TORRENT_FILE" "$MNT/metadata/tree/deep/leaf.torrent"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -d "$MNT/data/tree/deep/leaf.torrent" ] && break
    sleep 0.1
done
[ -d "$MNT/data/tree/deep/leaf.torrent" ] || fail "nested projection did not appear"
stat "$MNT/data/tree" >/dev/null
rm -rf "$MNT/metadata/tree"
if [ -e "$MNT/data/tree" ]; then
    fail "data/tree still resolves right after the subtree delete (stale dentry)"
fi

# Directory rename: the old mirrored directory must stop resolving at once,
# and the moved subtree must be visible immediately (regression guard for the
# rename branch's active invalidation).
mkdir -p "$MNT/metadata/renamedir/deep"
cp "$TORRENT_FILE" "$MNT/metadata/renamedir/deep/leaf.torrent"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -d "$MNT/data/renamedir/deep/leaf.torrent" ] && break
    sleep 0.1
done
[ -d "$MNT/data/renamedir/deep/leaf.torrent" ] || fail "nested projection did not appear"
stat "$MNT/data/renamedir" >/dev/null   # hold the old directory dentry
mv "$MNT/metadata/renamedir" "$MNT/metadata/moved"
if [ -e "$MNT/data/renamedir" ]; then
    fail "data/renamedir still resolves right after the directory rename (stale dentry)"
fi
[ -d "$MNT/data/moved/deep/leaf.torrent" ] || fail "data/moved/deep/leaf.torrent did not appear immediately"

echo "tc_ingest_03: PASS (seed rename/delete, subtree delete and directory rename reflect immediately)"

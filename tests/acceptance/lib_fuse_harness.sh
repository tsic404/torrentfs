#!/usr/bin/env bash
# Shared FUSE harness for the acceptance scripts.
#
# Mounts torrentfs on a fresh mountpoint with a fresh state/cache, waits for
# the mount, and tears it down on exit.  Source this file from an acceptance
# script after setting:
#
#   ROOT_DIR (repo root), BIN (torrentfs binary), MNT (mountpoint)
#
# After sourcing:
#   - $MNT is mounted (or the script already failed)
#   - $CACHE_DIR / $DB_DIR hold the daemon's state paths
#   - the EXIT trap unmounts and cleans up
#
# Usage from an acceptance script:
#   ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
#   BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
#   MNT="${2:-/tmp/torrentfs_acceptance_mnt}"
#   # shellcheck source=/dev/null
#   source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
set -euo pipefail

LOG_FILE="$(mktemp)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
TORRENTFS_PID=""

cleanup() {
    if [ -n "$TORRENTFS_PID" ]; then
        kill "$TORRENTFS_PID" 2>/dev/null || true
        # Graceful shutdown: give the daemon time to unmount.
        for _ in $(seq 1 20); do
            mountpoint -q "$MNT" 2>/dev/null || break
            sleep 0.5
        done
    fi
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    rm -rf "$MNT" "$LOG_FILE" "$CACHE_DIR" "$DB_DIR"
}
trap cleanup EXIT

command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
    echo "lib_fuse_harness: fusermount not found; need a FUSE-capable environment" >&2
    exit 1
}

[ -x "$BIN" ] || { echo "lib_fuse_harness: binary not found or not executable: $BIN" >&2; exit 1; }

mkdir -p "$MNT"

"$BIN" "$MNT" --cache "$CACHE_DIR" --db "$DB_DIR/metadata.db" > "$LOG_FILE" 2>&1 &
TORRENTFS_PID=$!

for _ in $(seq 1 60); do
    mountpoint -q "$MNT" && break
    sleep 1
done
mountpoint -q "$MNT" || {
    echo "lib_fuse_harness: FUSE mount did not appear" >&2
    echo "--- daemon log tail ---" >&2
    tail -n 20 "$LOG_FILE" >&2
    exit 1
}

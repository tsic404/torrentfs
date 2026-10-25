#!/usr/bin/env bash
# Shared FUSE harness for the acceptance scripts.
#
# The helpers at the top are the one implementation of "is there a FUSE mount
# here, and wait for one": cases that drive their own daemons or the container
# entrypoint source this file with TORRENTFS_FUSE_HARNESS_LIB_ONLY=1 and use
# them without the single-daemon harness below.
#
# Sourcing it plainly mounts torrentfs on a fresh mountpoint with a fresh
# state/cache, waits for the mount, and tears it down on exit.  Source this
# file from an acceptance script after setting:
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

# Print the FUSE connection minor of the mount at $1 (the id under
# /sys/fs/fuse/connections), empty when the path carries no FUSE mount.
fuse_minor_at() {
    awk -v mp="$1" '
        $5 == mp {
            for (i = 7; i < NF; i++) {
                if ($i == "-" && $(i + 1) ~ /^fuse/) {
                    split($3, a, ":")
                    print a[2]
                    exit
                }
            }
        }
    ' /proc/self/mountinfo
}

# Wait for a FUSE mount at $1, bounded by $2 seconds (default 30).
wait_for_fuse() {
    local target="$1" timeout="${2:-30}"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        [ -n "$(fuse_minor_at "$target")" ] && return 0
        sleep 0.2
    done
    return 1
}

# A case that manages its own daemons (or the entrypoint) wants the helpers
# above and owns its own state and EXIT trap: it sources this file with
# TORRENTFS_FUSE_HARNESS_LIB_ONLY=1, which stops before the harness mounts.
if [ "${TORRENTFS_FUSE_HARNESS_LIB_ONLY:-0}" = 1 ]; then
    return 0 2>/dev/null || exit 0
fi

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

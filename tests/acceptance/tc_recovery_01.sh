#!/usr/bin/env bash
# Design acceptance case TC-recovery-01: unmount → remount with the same state
# database keeps previously ingested torrents visible with unchanged
# structure, permissions and sizes (`docs/03-acceptance.md` §I AC-80).
#
# FUSE-level executable form.  Requires a FUSE-capable environment, the
# release binary.
#
# Usage: ./tests/acceptance/tc_recovery_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_recovery01}"
STATE_DIR="$(mktemp -d)"
CACHE_DIR="$(mktemp -d)"
LOG_FILE="$(mktemp)"
TORRENTFS_PID=""
trap 'kill "$TORRENTFS_PID" 2>/dev/null || true; fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true; rm -rf "$MNT" "$STATE_DIR" "$CACHE_DIR" "$LOG_FILE"' EXIT

command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
    echo "TC-recovery-01: fusermount not found; need a FUSE-capable environment" >&2
    exit 1
}

# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

TORRENT_FILE="$(mktemp)"
trap 'rm -f "$TORRENT_FILE"; kill "$TORRENTFS_PID" 2>/dev/null || true; fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true; sleep 1; rm -rf "$MNT" "$STATE_DIR" "$CACHE_DIR" "$LOG_FILE"' EXIT
make_seed_torrent "$TORRENT_FILE"

# ── First mount: ingest the torrent, snapshot the projection ──────────────
mkdir -p "$MNT"
"$BIN" "$MNT" --cache "$CACHE_DIR" --db "$STATE_DIR/metadata.db" > "$LOG_FILE" 2>&1 &
TORRENTFS_PID=$!
for _ in $(seq 1 60); do mountpoint -q "$MNT" && break; sleep 1; done
mountpoint -q "$MNT" || { echo "TC-recovery-01: FAIL — first mount did not appear" >&2; exit 1; }

cp "$TORRENT_FILE" "$MNT/metadata/seed.torrent"
for _ in $(seq 1 60); do
    [ -d "$MNT/data/seed.torrent" ] && break
    sleep 1
done
[ -d "$MNT/data/seed.torrent" ] || { echo "TC-recovery-01: FAIL — projection did not appear" >&2; exit 1; }

BEFORE="$(find "$MNT/data" -mindepth 1 | sort)"
BEFORE_PERMS="$(stat -c '%a %s' "$MNT/data/seed.torrent" "$MNT/data/seed.torrent/seed.txt")"

# Graceful shutdown: SIGTERM, wait for unmount.
kill "$TORRENTFS_PID"; TORRENTFS_PID=""
for _ in $(seq 1 30); do
    mountpoint -q "$MNT" 2>/dev/null || break
    sleep 0.5
done
mountpoint -q "$MNT" 2>/dev/null && {
    echo "TC-recovery-01: FAIL — mount did not unmount after SIGTERM" >&2
    exit 1
}

# ── Second mount: same state db, structure must be unchanged ──────────────
"$BIN" "$MNT" --cache "$CACHE_DIR" --db "$STATE_DIR/metadata.db" > "$LOG_FILE" 2>&1 &
TORRENTFS_PID=$!
for _ in $(seq 1 60); do mountpoint -q "$MNT" && break; sleep 1; done
mountpoint -q "$MNT" || { echo "TC-recovery-01: FAIL — second mount did not appear" >&2; exit 1; }

AFTER="$(find "$MNT/data" -mindepth 1 | sort)"
AFTER_PERMS="$(stat -c '%a %s' "$MNT/data/seed.torrent" "$MNT/data/seed.torrent/seed.txt")"

[ "$BEFORE" = "$AFTER" ] || {
    echo "TC-recovery-01: FAIL — projection changed across remount" >&2
    exit 1
}
[ "$BEFORE_PERMS" = "$AFTER_PERMS" ] || {
    echo "TC-recovery-01: FAIL — perms/sizes changed across remount" >&2
    exit 1
}

echo "TC-recovery-01: PASS (projection + perms/sizes unchanged across remount)"

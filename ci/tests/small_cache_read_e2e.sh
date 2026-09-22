#!/usr/bin/env bash
# End-to-end regression for the small-cache recheck spin: with the on-disk
# piece cache smaller than the torrent (and than a single dd block), a
# sequential full-file read must not degrade to a `force_recheck` spin.
#
# Reproduces the QA failure path: `--config` with `[cache] cache_size` <
# torrent size + a real FUSE mount + `dd bs=1M` (block > cache capacity).
# A healthy read is download-bound (a few seconds for a 4 MiB torrent on a
# local seeder); the bug made it re-run `force_recheck_and_wait` on nearly
# every FUSE read (~2s each), stretching a full `cat` into minutes.
#
# Requires a rootful/FUSE-capable environment (the CI Docker image, or a host
# with /dev/fuse).  Usage:
#   ./ci/tests/small_cache_read_e2e.sh [torrentfs_binary] [mountpoint]
#   env: SELFSEED_OUT (default /tmp/small_cache_selfseed), CACHE_MIB (default 1)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_small_cache_mnt}"
CACHE_MIB="${CACHE_MIB:-1}"
PAYLOAD_MIB="${PAYLOAD_MIB:-4}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/small_cache_selfseed}"
LOG_FILE="$(mktemp)"

cleanup() {
    # Unmount before removing the tree; the FUSE daemon holds the mountpoint.
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    rm -rf "$MNT" "$SELFSEED_OUT" "$LOG_FILE"
}
trap cleanup EXIT

command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
    echo "small_cache_read_e2e: fusermount not found; need a FUSE-capable environment" >&2
    exit 1
}

echo "[small_cache_read_e2e] building seeder (payload=${PAYLOAD_MIB} MiB)…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" >/dev/null 2>&1 &
SEEDER_PID=$!
trap 'kill "$SEEDER_PID" 2>/dev/null || true; cleanup' EXIT

# Wait for the seeder to finish building + start announcing.
for _ in $(seq 1 120); do
    [ -f "$SELFSEED_OUT/selfseed.torrent" ] && break
    sleep 1
done
[ -f "$SELFSEED_OUT/selfseed.torrent" ] || { echo "small_cache_read_e2e: seeder did not produce a torrent" >&2; exit 1; }

# Small cache config: below the torrent size (4 MiB) and below a 1 MiB dd block
# when CACHE_MIB < 1 — the "block > cache" condition from the QA repro.
CACHE_BYTES=$((CACHE_MIB * 1024 * 1024))
CONFIG_FILE="$(mktemp)"
printf '[cache]\ncache_size = %d\n' "$CACHE_BYTES" > "$CONFIG_FILE"

mkdir -p "$MNT"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"

echo "[small_cache_read_e2e] mounting torrentfs (cache=${CACHE_MIB} MiB)…"
"$BIN" "$MNT" --cache "$CACHE_DIR" --db "$DB_DIR/metadata.db" --config "$CONFIG_FILE" \
    > "$LOG_FILE" 2>&1 &
TORRENTFS_PID=$!
trap 'kill "$SEEDER_PID" "$TORRENTFS_PID" 2>/dev/null || true; cleanup' EXIT

# Wait for the mount to appear.
for _ in $(seq 1 60); do
    mountpoint -q "$MNT" && break
    sleep 1
done
mountpoint -q "$MNT" || { echo "small_cache_read_e2e: FUSE mount did not appear" >&2; exit 1; }

cp "$SELFSEED_OUT/selfseed.torrent" "$MNT/metadata/"
# Wait for the data/ directory to materialise after the torrent is persisted.
DATA_FILE=""
for _ in $(seq 1 60); do
    DATA_FILE="$(find "$MNT/data" -type f ! -name '.stats' 2>/dev/null | head -n1 || true)"
    [ -n "$DATA_FILE" ] && break
    sleep 1
done
[ -n "$DATA_FILE" ] || { echo "small_cache_read_e2e: data file did not appear" >&2; exit 1; }

echo "[small_cache_read_e2e] sequential full-file read (dd bs=1M)…"
START="$(date +%s)"
dd if="$DATA_FILE" of=/dev/null bs=1M status=none
END="$(date +%s)"
ELAPSED=$((END - START))

# The buggy path logged a `force_recheck` on nearly every FUSE read; a healthy
# small-cache read still evicts + re-downloads, but must not recheck-spin.
RECHECK_COUNT="$(grep -c 'stale libtorrent piece state detected' "$LOG_FILE" || true)"
echo "[small_cache_read_e2e] dd elapsed=${ELAPSED}s rechecks=${RECHECK_COUNT}"

# Bounds are deliberately loose to stay robust on slow CI: the seeder is
# local, so a 4 MiB read must finish in seconds, not the ~3-minute spin.  A
# handful of rechecks (one per stale-piece encounter) is acceptable; the bug
# produced dozens per read.
if [ "$ELAPSED" -gt 60 ]; then
    echo "small_cache_read_e2e: FAIL — full-file read took ${ELAPSED}s (>60s)" >&2
    exit 1
fi
# Explicit upper bound on the recheck count: the buggy path forced a recheck
# on nearly every FUSE read (dozens for a 4 MiB file); a healthy small-cache
# read re-downloads but only rechecks once per stale-piece encounter.
if [ "$RECHECK_COUNT" -gt 20 ]; then
    echo "small_cache_read_e2e: FAIL — ${RECHECK_COUNT} rechecks (>20)" >&2
    exit 1
fi
echo "small_cache_read_e2e: PASS (dd ${ELAPSED}s, ${RECHECK_COUNT} rechecks)"

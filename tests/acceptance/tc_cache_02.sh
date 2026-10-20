#!/usr/bin/env bash
# Design acceptance case TC-cache-02: unmount → remount with the same cache
# directory → the cache still hits; re-reading the same range does not
# re-download (`docs/03-acceptance.md` §D AC-31).
#
# FUSE-level executable form: engine restart alone cannot serve a restarted
# cache — the FUSE layer's background verification worker (`spawn_cache_verification`)
# is what re-validates on-disk pieces after a restart — so this case drives a
# real mount twice against one cache directory, with the seeder stopped before
# the second mount (a miss would have no peer and fail; success proves a hit).
#
# Requires a FUSE-capable environment, the release binary and the self-seed
# example built, plus `run_self_seed_env.sh`.
#
# Usage: ./tests/acceptance/tc_cache_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_cache02}"
STATE_DIR="$(mktemp -d)"
CACHE_DIR="$(mktemp -d)"
LOG_FILE="$(mktemp)"
SEED_OUT="$(mktemp -d)"
TORRENTFS_PID=""
SEEDER_PID=""
trap 'kill "$TORRENTFS_PID" 2>/dev/null || true; kill "$SEEDER_PID" 2>/dev/null || true; fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true; sleep 1; rm -rf "$MNT" "$STATE_DIR" "$CACHE_DIR" "$LOG_FILE" "$SEED_OUT"' EXIT

command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
    echo "TC-cache-02: fusermount not found; need a FUSE-capable environment" >&2
    exit 1
}

start_daemon() {
    "$BIN" "$MNT" --cache "$CACHE_DIR" --db "$STATE_DIR/metadata.db" > "$LOG_FILE" 2>&1 &
    TORRENTFS_PID=$!
    for _ in $(seq 1 60); do
        mountpoint -q "$MNT" && return 0
        sleep 1
    done
    echo "TC-cache-02: FAIL — FUSE mount did not appear" >&2
    tail -n 20 "$LOG_FILE" >&2
    exit 1
}

stop_daemon() {
    kill "$TORRENTFS_PID" 2>/dev/null || true
    TORRENTFS_PID=""
    for _ in $(seq 1 30); do
        mountpoint -q "$MNT" 2>/dev/null || return 0
        sleep 0.5
    done
    echo "TC-cache-02: FAIL — mount did not unmount after SIGTERM" >&2
    exit 1
}

# Start the self-seed environment: tracker + seeder + torrent, all local.
mkdir -p "$MNT"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SEED_OUT" >/dev/null 2>&1 &
SEEDER_PID=$!
for _ in $(seq 1 120); do
    [ -f "$SEED_OUT/selfseed.torrent" ] && break
    sleep 1
done
[ -f "$SEED_OUT/selfseed.torrent" ] || {
    echo "TC-cache-02: FAIL — seeder did not produce a torrent" >&2
    exit 1
}

# ── First mount: ingest and read the whole file (downloads + caches) ────
start_daemon
cp "$SEED_OUT/selfseed.torrent" "$MNT/metadata/selfseed.torrent"
for _ in $(seq 1 60); do
    [ -e "$MNT/data/selfseed.torrent/selfseed" ] && break
    sleep 1
done
[ -e "$MNT/data/selfseed.torrent/selfseed" ] || {
    echo "TC-cache-02: FAIL — projection did not appear on first mount" >&2
    exit 1
}
# Give the background verification worker time to mark the downloaded pieces.
sleep 3
FIRST="$(cat "$MNT/data/selfseed.torrent/selfseed")"
[ -n "$FIRST" ] || { echo "TC-cache-02: FAIL — first read returned empty" >&2; exit 1; }
[ "$(stat -c '%s' "$MNT/data/selfseed.torrent/selfseed")" = "$(wc -c < "$SEED_OUT/payload.txt")" ] || {
    echo "TC-cache-02: FAIL — first read size mismatch" >&2
    exit 1
}
stop_daemon

# Stop the seeder: the second mount's read must be served by the cache alone.
kill "$SEEDER_PID" 2>/dev/null || true
SEEDER_PID=""
sleep 2

# ── Second mount: same cache dir, no seeder — read must hit the cache ───
start_daemon
# The FUSE layer re-verifies on-disk pieces asynchronously on startup; wait
# until the torrent's Pieces markers show cached (verified) pieces (`[x]` /
# `[X n]`) instead of `[]` before issuing the read.
for _ in $(seq 1 60); do
    if grep -qE '^  Pieces: \[(x|X)' "$MNT/data/selfseed.torrent/.stats" 2>/dev/null; then
        break
    fi
    sleep 1
done
grep -qE '^  Pieces: \[(x|X)' "$MNT/data/selfseed.torrent/.stats" 2>/dev/null || {
    echo "TC-cache-02: FAIL — cached pieces were never verified after remount" >&2
    exit 1
}
# Compare against the authoritative payload and time the read: with the seeder
# gone, success itself proves the bytes came from the cache (a re-download has
# no peer), and a miss would stall for the 15s no-seeder window before failing
# — so a fast, byte-exact read pins the cache-hit path.
START_MS="$(date +%s%N)"
SECOND_PATH="$MNT/data/selfseed.torrent/selfseed"
cmp -s "$SEED_OUT/payload.txt" "$SECOND_PATH" || {
    echo "TC-cache-02: FAIL — re-read bytes differ from the authoritative payload" >&2
    exit 1
}
END_MS="$(date +%s%N)"
ELAPSED_MS=$(( (END_MS - START_MS) / 1000000 ))
[ "$ELAPSED_MS" -lt 10000 ] || {
    echo "TC-cache-02: FAIL — re-read took ${ELAPSED_MS}ms (a cache miss would stall on the no-seeder window)" >&2
    exit 1
}
# The cache must have served it: the swarm is empty (seeder stopped), so a
# cache miss would have waited out the no-seeder window and failed.
if grep -q "no seeder connected\|NoPeers\|ENODATA" "$LOG_FILE"; then
    echo "TC-cache-02: FAIL — second read fell back to the swarm (no-seeder error logged)" >&2
    exit 1
fi
# Keep the first read around as the cross-mount consistency check too.
SECOND="$(cat "$SECOND_PATH")"
[ "$SECOND" = "$FIRST" ] || {
    echo "TC-cache-02: FAIL — re-read bytes differ from the first read" >&2
    exit 1
}
stop_daemon

echo "TC-cache-02: PASS (remount served the authoritative bytes in ${ELAPSED_MS}ms from cache; no swarm fallback)"

#!/usr/bin/env bash
# Window-boundary e2e for the piece-priority access window.
#
# A read elevates the pieces it covers and descends through the step ladder to
# `window_edge_priority` (default 1) at the far edge of the access window, then
# drops to `rest_priority` (default 0) beyond it.  The stock 4 MiB selfseed
# torrent fits entirely inside the default 4096 MiB window, so the edge `[1]` /
# outside `[]` boundary is unobservable there.  This script seeds a >4 GiB
# sparse (all-zero, no physical allocation) single-file torrent via
# `ci/run_self_seed_env.sh --payload-gib` and asserts the boundary in `.stats`:
#
#   ... [7][6][5][4][3] [1][1]...[1] [][][] ...
#        ^ read range   ^ window edge  ^ beyond window
#
# The script pins `access_window_mb` (ACCESS_WINDOW_MB, default 4096 MiB) in
# the config and sizes the cache above it, so the access window — not the
# cache — sets the edge.  It then asserts the edge's exact position in the
# marker line, not just that `[1]` and `[]` both occur.  The in-window
# descending ladder (`[7][6][5][4][3]` down to the edge `[1]`) is already
# observable on the 4 MiB selfseed torrent; this script covers the edge `[1]` /
# outside `[]` boundary that only a >4 GiB seed can show.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/window_boundary_e2e.sh [torrentfs_binary] [mountpoint]
#   env: PAYLOAD_GIB (default 5), CACHE_GIB (default 5),
#        ACCESS_WINDOW_MB (default 4096), TRACKER_PORT (default 16969),
#        SELFSEED_OUT (default /tmp/window_selfseed)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_window_mnt}"
PAYLOAD_GIB="${PAYLOAD_GIB:-5}"
CACHE_GIB="${CACHE_GIB:-5}"
# Access window in MiB.  Defaults to the shipped `piece_priority` default
# (4096) and is written into the config below, so the value the assertions
# check against is the value the runtime uses.
ACCESS_WINDOW_MB="${ACCESS_WINDOW_MB:-4096}"
TRACKER_PORT="${TRACKER_PORT:-16969}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/window_selfseed}"
SEEDER_LOG="$SELFSEED_OUT/seeder.log"
TORRENTFS_LOG="$(mktemp)"
CONFIG_FILE="$(mktemp)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
SEEDER_PID=""
TORRENTFS_PID=""

cleanup() {
    # `run_self_seed_env.sh` runs the seeder binary as a child, so killing only
    # the wrapper would orphan the seeder still holding the tracker port.  Walk
    # the process tree via /proc (no procps dependency).
    kill_tree() {
        local pid="$1" child
        for child in $(cat "/proc/$pid/task/$pid/children" 2>/dev/null || true); do
            kill_tree "$child"
        done
        kill "$pid" 2>/dev/null || true
    }
    if [ -n "$TORRENTFS_PID" ]; then kill_tree "$TORRENTFS_PID"; fi
    if [ -n "$SEEDER_PID" ]; then kill_tree "$SEEDER_PID"; fi
    # Unmount before removing the tree; the FUSE daemon holds the mountpoint.
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    # The killed processes may still be flushing the cache when this runs; a
    # racing write leaves an unremovable `pieces/` entry, so retry briefly.
    # The payload and seed file are sparse (no physical blocks), but remove
    # them anyway so the runner's disk usage carries no nominal >4 GiB inode.
    for _ in 1 2 3 4 5; do
        rm -rf "$MNT" "$SELFSEED_OUT" "$CACHE_DIR" "$DB_DIR" "$TORRENTFS_LOG" "$CONFIG_FILE" 2>/dev/null && break
        sleep 1
    done
    return 0
}
trap cleanup EXIT

fail() {
    echo "window_boundary_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    if [ -n "${MARKERS:-}" ]; then
        local list trailing
        list="$(printf '%s' "$MARKERS" | grep -o '\[[^]]*\]' || true)"
        trailing="$(printf '%s\n' "$list" \
            | awk 'BEGIN { n = 0 } $0 == "[]" { n++; next } { n = 0 } END { print n }')"
        echo "  markers=$(printf '%s\n' "$list" | grep -c . || true) first16=$(printf '%s' "$list" | head -n16 | tr '\n' ' ')" >&2
        echo "  trailing_empty=${trailing} expected_outside=${EXPECTED_OUTSIDE:-?} window_pieces=${WINDOW_PIECES:-?}" >&2
    fi
    if [ -n "${TORRENTFS_PID:-}" ] && [ -n "${TORRENTFS_LOG:-}" ] && [ -f "$TORRENTFS_LOG" ]; then
        echo "  torrentfs log tail:" >&2
        tail -15 "$TORRENTFS_LOG" >&2 || true
    fi
    exit 1
}

command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 \
    || fail "fusermount not found; need a FUSE-capable environment"
[ -e /dev/fuse ] || fail "/dev/fuse not found; need a FUSE-capable environment"
[ -x "$BIN" ] || fail "torrentfs binary not found at $BIN; run 'cargo build --locked --release'"

# Positive-integer guard for the size env vars: a non-numeric or zero value
# fails here with a clear message instead of a bash arithmetic error deeper in.
require_positive_int() {
    local name="$1" value="$2"
    case "$value" in
        ''|*[!0-9]*) fail "$name must be a positive integer (got '${value}')" ;;
    esac
    [ "$(( 10#$value ))" -gt 0 ] || fail "$name must be > 0 (got '${value}')"
}
require_positive_int PAYLOAD_GIB "$PAYLOAD_GIB"
require_positive_int CACHE_GIB "$CACHE_GIB"
require_positive_int ACCESS_WINDOW_MB "$ACCESS_WINDOW_MB"

# `10#` accepts leading zeros, matching `run_self_seed_env.sh`'s validation.
PAYLOAD_BYTES=$(( 10#$PAYLOAD_GIB * 1024 * 1024 * 1024 ))
CACHE_BYTES=$(( 10#$CACHE_GIB * 1024 * 1024 * 1024 ))
ACCESS_WINDOW_BYTES=$(( 10#$ACCESS_WINDOW_MB * 1024 * 1024 ))

mkdir -p "$SELFSEED_OUT"
echo "[window_boundary_e2e] seeding ${PAYLOAD_GIB} GiB sparse payload…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-gib "$PAYLOAD_GIB" \
    --port "$TRACKER_PORT" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" > "$SEEDER_LOG" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 180); do
    [ -f "$SELFSEED_OUT/selfseed.torrent" ] && break
    kill -0 "$SEEDER_PID" 2>/dev/null \
        || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited during startup"; }
    sleep 1
done
[ -f "$SELFSEED_OUT/selfseed.torrent" ] || fail "seeder did not produce a torrent"

# Derive the piece length from the generated torrent (bencode
# `12:piece lengthi<N>e`) instead of restating `seeder_common::PIECE_LEN`, so
# the script has one source of truth for the piece geometry.  The info dict
# writes the piece length before the `pieces` blob, so the first match is it.
PIECE_LEN="$(grep -ao '12:piece lengthi[0-9]*e' "$SELFSEED_OUT/selfseed.torrent" \
    | head -n1 | sed 's/.*i\([0-9]*\)e/\1/' || true)"
[ -n "$PIECE_LEN" ] || fail "could not read the piece length from the generated torrent"
EXPECTED_PIECES=$(( (PAYLOAD_BYTES + PIECE_LEN - 1) / PIECE_LEN ))
# The access window is capped by the cache at `cache - 1 piece`; keep the cache
# above the window so the access window, not the cache, sets the edge under
# test (a smaller cache would silently shrink the window and still satisfy the
# marker assertions).
[ "$CACHE_BYTES" -ge "$(( ACCESS_WINDOW_BYTES + PIECE_LEN ))" ] \
    || fail "CACHE_GIB=${CACHE_GIB} is too small to expose the ${ACCESS_WINDOW_MB} MiB access window (need > $(( ACCESS_WINDOW_BYTES / 1024 / 1024 + 1 )) MiB)"
WINDOW_PIECES=$(( ACCESS_WINDOW_BYTES / PIECE_LEN ))
echo "[window_boundary_e2e] torrent: ${EXPECTED_PIECES} pieces x ${PIECE_LEN} B; window edge at piece ${WINDOW_PIECES}"

# The seeder hash-checks the full sparse seed file before announcing; wait for
# its "ready" line so the downloader's first read finds a live peer instead of
# spending its read-timeout window on peer discovery.
for _ in $(seq 1 180); do
    grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null && break
    kill -0 "$SEEDER_PID" 2>/dev/null \
        || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited before becoming ready"; }
    sleep 1
done
grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null || fail "seeder did not become ready"

# Pin both bounds the assertions depend on — the cache (kept above the window
# by the check above) and the access window itself — so the script owns the
# values it checks against instead of relying on the compiled defaults.
printf '[cache]\ncache_size = %d\n[piece_priority]\naccess_window_mb = %d\n' \
    "$CACHE_BYTES" "$(( 10#$ACCESS_WINDOW_MB ))" > "$CONFIG_FILE"
mkdir -p "$MNT"

echo "[window_boundary_e2e] mounting torrentfs (cache=${CACHE_GIB} GiB)…"
"$BIN" "$MNT" --cache "$CACHE_DIR" --db "$DB_DIR/metadata.db" --config "$CONFIG_FILE" \
    > "$TORRENTFS_LOG" 2>&1 &
TORRENTFS_PID=$!

for _ in $(seq 1 60); do
    mountpoint -q "$MNT" && break
    kill -0 "$TORRENTFS_PID" 2>/dev/null \
        || { tail -20 "$TORRENTFS_LOG" >&2; fail "torrentfs exited before mounting"; }
    sleep 1
done
mountpoint -q "$MNT" || { tail -20 "$TORRENTFS_LOG" >&2; fail "FUSE mount did not appear"; }

cp "$SELFSEED_OUT/selfseed.torrent" "$MNT/metadata/"
DATA_FILE=""
for _ in $(seq 1 60); do
    DATA_FILE="$(find "$MNT/data" -type f ! -name '.stats' 2>/dev/null | head -n1 || true)"
    [ -n "$DATA_FILE" ] && break
    sleep 1
done
[ -n "$DATA_FILE" ] || fail "data file did not appear"
STATS_FILE="$(dirname "$DATA_FILE")/.stats"

# A one-byte read at the head registers a reader on piece 0: the gradient is
# set on reader-added and retained as the prefetch window on release, so the
# boundary is observable without waiting for a full download.  The data entry
# can become visible a moment before its torrent id is final (the kernel caches
# the first lookup for ~1s), so retry the read briefly instead of failing on
# that transient.
echo "[window_boundary_e2e] reading the head to raise the gradient…"
HEAD_READ_OK=0
for _ in $(seq 1 15); do
    if dd if="$DATA_FILE" of=/dev/null bs=1 count=1 status=none 2>/dev/null; then
        HEAD_READ_OK=1
        break
    fi
    sleep 1
done
[ "$HEAD_READ_OK" = 1 ] || fail "head read failed after retries"

# Poll until `.stats` publishes the piece snapshot (the torrent can be
# persisted before its first piece-status snapshot).
MARKERS=""
PIECE_COUNT=""
for _ in $(seq 1 60); do
    PIECE_COUNT="$(sed -n 's/^  PieceCount: //p' "$STATS_FILE" 2>/dev/null || true)"
    MARKERS="$(sed -n 's/^  Pieces: //p' "$STATS_FILE" 2>/dev/null || true)"
    [ "$PIECE_COUNT" = "$EXPECTED_PIECES" ] && [ -n "$MARKERS" ] && break
    sleep 1
done

[ -n "$MARKERS" ] || fail "no Pieces marker line in .stats"
[ "$PIECE_COUNT" = "$EXPECTED_PIECES" ] \
    || fail "PieceCount ${PIECE_COUNT:-<none>} != expected $EXPECTED_PIECES"

# `[1]` is the window edge (window_edge_priority), `[]` is outside the window
# (rest_priority 0).  They must be distinguishable: the edge marker sits
# immediately before the first empty marker, and the file ends outside the
# window.
case "$MARKERS" in
    *"[1]"*) ;;
    *) fail "no window-edge [1] marker in the Pieces line" ;;
esac
case "$MARKERS" in
    *"[]"*) ;;
    *) fail "no outside-window [] marker in the Pieces line" ;;
esac
case "$MARKERS" in
    *"[1][]"*) ;;
    *) fail "no [1][] window-edge→outside boundary in the Pieces line" ;;
esac
case "$MARKERS" in
    *"[]") ;;
    *) fail "Pieces line does not end outside the window (last marker is not [])" ;;
esac

# The window edge must land exactly where the access window ends: pieces
# WINDOW_PIECES+1.. are never wanted, so the marker line has to end with
# exactly (EXPECTED_PIECES - 1 - WINDOW_PIECES) empty markers, immediately
# preceded by the last wanted piece.  Counting the stable tail — rather than
# the first `[]` — keeps the check independent of transient head states: a
# piece can briefly read `[]` between its priority being cleared on completion
# and its cache entry being registered.
MARKER_LIST="$(printf '%s' "$MARKERS" | grep -o '\[[^]]*\]' || true)"
TRAILING_EMPTY="$(printf '%s\n' "$MARKER_LIST" \
    | awk 'BEGIN { n = 0 } $0 == "[]" { n++; next } { n = 0 } END { print n }')"
EXPECTED_OUTSIDE=$(( EXPECTED_PIECES - 1 - WINDOW_PIECES ))
[ "$TRAILING_EMPTY" -eq "$EXPECTED_OUTSIDE" ] \
    || fail "trailing [] run ${TRAILING_EMPTY} != expected ${EXPECTED_OUTSIDE} (window edge misplaced)"

echo "[window_boundary_e2e] PASS (${EXPECTED_PIECES} pieces; window edge at piece ${WINDOW_PIECES})"

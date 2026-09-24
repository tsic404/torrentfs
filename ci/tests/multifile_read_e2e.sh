#!/usr/bin/env bash
# End-to-end regression for the self-seed environment's *multi-file* torrent.
#
# QA scenarios 15 and 19 (read one file of a multi-file torrent; read two files
# concurrently) used to be structure-only assertions: the repository shipped no
# seeded multi-file torrent, so `cat` returned ENODATA ~40 s in and the piece
# assertions could not be made.  `ci/run_self_seed_env.sh` now serves a
# multi-file `movie` torrent (movie/file_A, movie/file_B, movie/subdir/file_C)
# from the same tracker+session as the single-file payload, so this script
# pins both properties those scenarios assert, against a real FUSE mount:
#
#   1. reading one file downloads (and caches) only that file's pieces — the
#      other files' pieces stay `[]`;
#   2. two concurrent reads both land, each on its own pieces, without
#      disturbing the pieces no read touched.
#
# The payload is piece-aligned (256 KiB pieces): file_A is pieces 0-3, file_B is
# piece 4, subdir/file_C is pieces 5-6 — no piece is shared between two files,
# which is what makes "file_B stayed []" an unambiguous assertion.  Phase 1
# reads the last file (its last piece is also the torrent's last, and it lives
# in a subdirectory); phase 2 reads the first two concurrently, each from a
# fresh cache so the untouched pieces are genuinely untouched.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/multifile_read_e2e.sh [torrentfs_binary] [mountpoint]
#   env: SELFSEED_OUT (default /tmp/multifile_read_selfseed)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_multifile_mnt}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/multifile_read_selfseed}"
SEEDER_LOG="$SELFSEED_OUT/seeder.log"
TORRENTFS_LOG="$(mktemp)"
WORK_DIR="$(mktemp -d)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
SEEDER_PID=""
TORRENTFS_PID=""
STATS_FILE="$MNT/data/movie.torrent/.stats"

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
    # Reap the killed children before returning.  torrentfs unmounts $MNT as
    # its graceful shutdown completes; if a subsequent run (or a caller reusing
    # the default mountpoint) mounts there first, that lingering unmount tears
    # the fresh mount down ("FUSE session ended without a shutdown signal").
    [ -z "$TORRENTFS_PID" ] || wait "$TORRENTFS_PID" 2>/dev/null || true
    [ -z "$SEEDER_PID" ] || wait "$SEEDER_PID" 2>/dev/null || true
    # Unmount before removing the tree; the FUSE daemon holds the mountpoint.
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    # The killed processes may still be flushing the cache when this runs; a
    # racing write leaves an unremovable `pieces/` entry, so retry briefly.
    for _ in 1 2 3 4 5; do
        rm -rf "$MNT" "$SELFSEED_OUT" "$CACHE_DIR" "$DB_DIR" "$WORK_DIR" \
            "$TORRENTFS_LOG" 2>/dev/null && break
        sleep 1
    done
    return 0
}
trap cleanup EXIT

fail() {
    echo "multifile_read_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    if [ -n "${PHASE:-}" ]; then
        echo "  phase: ${PHASE}" >&2
        echo "  pieces: $(sed -n 's/^  Pieces: //p' "$STATS_FILE" 2>/dev/null || true)" >&2
    fi
    if [ -n "${SEEDER_PID:-}" ] && [ -f "$SEEDER_LOG" ]; then
        echo "  seeder log tail:" >&2
        tail -10 "$SEEDER_LOG" >&2 || true
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

# ── mount lifecycle ──────────────────────────────────────────────────────────

# Mount a daemon with a fresh cache+DB and register the multi-file torrent.
# Each phase gets its own state so "the pieces no read touched are still `[]`"
# is asserted against a cache that has never seen them.
mount_and_register() {
    mkdir -p "$MNT" "$CACHE_DIR" "$DB_DIR"
    "$BIN" "$MNT" --cache "$CACHE_DIR" --db "$DB_DIR/metadata.db" \
        > "$TORRENTFS_LOG" 2>&1 &
    TORRENTFS_PID=$!

    for _ in $(seq 1 60); do
        mountpoint -q "$MNT" && break
        kill -0 "$TORRENTFS_PID" 2>/dev/null \
            || { tail -20 "$TORRENTFS_LOG" >&2; fail "torrentfs exited before mounting"; }
        sleep 1
    done
    mountpoint -q "$MNT" || { tail -20 "$TORRENTFS_LOG" >&2; fail "FUSE mount did not appear"; }

    cp "$SELFSEED_OUT/movie.torrent" "$MNT/metadata/"
    # Wait for the torrent root to materialise after the async persist.
    for _ in $(seq 1 60); do
        [ -f "$STATS_FILE" ] && break
        sleep 1
    done
    [ -f "$STATS_FILE" ] || fail "data/movie.torrent/.stats did not appear"
}

# Stop the daemon and drop its cache+DB, so the next phase starts cold.
reset_mount() {
    if [ -n "$TORRENTFS_PID" ]; then
        kill "$TORRENTFS_PID" 2>/dev/null || true
        wait "$TORRENTFS_PID" 2>/dev/null || true
        TORRENTFS_PID=""
    fi
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    rm -rf "$CACHE_DIR" "$DB_DIR" "$MNT"
    CACHE_DIR="$(mktemp -d)"
    DB_DIR="$(mktemp -d)"
}

# ── assertions ───────────────────────────────────────────────────────────────

# Normalized per-piece download state from the torrent's `.stats` line:
# `cached` (in the piece cache: `[X n]` / `[x]`), `wanted` (prioritized but not
# downloaded: `[7]`..`[1]`), `absent` (`[]`: neither downloaded nor wanted).
# Splitting on `][` first keeps `[X n]`'s space intact.
piece_states() {
    sed -n 's/^  Pieces: //p' "$STATS_FILE" \
        | sed -e 's/\]\[/] [/g' \
        | sed -e 's/\[X [0-9]*\]/cached/g' -e 's/\[x\]/cached/g' \
              -e 's/\[\]/absent/g' -e 's/\[[0-9]*\]/wanted/g'
}

# Poll until the piece states settle on the expected sequence (the cache is
# updated asynchronously after the read returns).
expect_pieces() {
    local desc="$1" expected="$2" deadline=$(( SECONDS + 30 ))
    while [ "$SECONDS" -lt "$deadline" ]; do
        [ "$(piece_states)" = "$expected" ] && return 0
        sleep 0.5
    done
    fail "$desc: piece states '$(piece_states)', expected '$expected'"
}

# Read `rel` from the mounted torrent and compare it byte-for-byte with the
# payload the seeder is serving, so a wrong piece mapping cannot pass as a
# successful read.
read_file() {
    local rel="$1" out="$2"
    if ! timeout 120 cat "$MNT/data/movie.torrent/movie/$rel" > "$out"; then
        fail "reading $rel failed"
    fi
    if ! cmp -s "$out" "$SELFSEED_OUT/movie/$rel"; then
        fail "$rel: served bytes differ from the seeded payload"
    fi
}

# ── seeder ───────────────────────────────────────────────────────────────────

mkdir -p "$SELFSEED_OUT"
echo "[multifile_read_e2e] starting the self-seed environment…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" > "$SEEDER_LOG" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 300); do
    [ -f "$SELFSEED_OUT/movie.torrent" ] && break
    kill -0 "$SEEDER_PID" 2>/dev/null \
        || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited during startup"; }
    sleep 1
done
[ -f "$SELFSEED_OUT/movie.torrent" ] || fail "seeder did not produce movie.torrent"

# The piece-state expectations below encode the fixture geometry, so pin the
# sizes that produce them instead of failing later with an opaque mismatch.
for spec in "file_A 1048576" "file_B 262144" "subdir/file_C 524288"; do
    set -- $spec
    [ "$(stat -c %s "$SELFSEED_OUT/movie/$1")" = "$2" ] \
        || fail "multi-file payload $1 is not $2 bytes"
done

# The seeder hash-checks every payload before announcing; wait for its "ready"
# line so the reads find a live peer instead of spending their timeout on peer
# discovery.
for _ in $(seq 1 300); do
    grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null && break
    kill -0 "$SEEDER_PID" 2>/dev/null \
        || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited before becoming ready"; }
    sleep 1
done
grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null || fail "seeder did not become ready"

# ── phase 1: one file of three (QA scenario 15) ──────────────────────────────

PHASE="one file (subdir/file_C)"
echo "[multifile_read_e2e] phase 1: reading subdir/file_C only…"
mount_and_register
read_file "subdir/file_C" "$WORK_DIR/file_C.bin"
# file_C is pieces 5-6; file_A's and file_B's pieces must be untouched: not
# cached, and not even wanted (the read's prefetch window stops at the file).
expect_pieces "$PHASE" "absent absent absent absent absent cached cached"
reset_mount

# ── phase 2: two concurrent reads (QA scenario 19) ───────────────────────────

PHASE="two concurrent reads (file_A + file_B)"
echo "[multifile_read_e2e] phase 2: reading file_A and file_B concurrently…"
mount_and_register
read_file "file_A" "$WORK_DIR/file_A.bin" &
READ_A=$!
read_file "file_B" "$WORK_DIR/file_B.bin" &
READ_B=$!
if ! wait "$READ_A"; then fail "$PHASE: file_A read failed"; fi
if ! wait "$READ_B"; then fail "$PHASE: file_B read failed"; fi
# Both reads land on their own pieces (file_A 0-3, file_B 4) without disturbing
# each other, and file_C's pieces stay untouched by either.
expect_pieces "$PHASE" "cached cached cached cached cached absent absent"

echo "multifile_read_e2e: PASS (one-file and concurrent reads against the seeded multi-file torrent)"

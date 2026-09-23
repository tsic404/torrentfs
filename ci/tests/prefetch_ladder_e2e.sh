#!/usr/bin/env bash
# Prefetch-ladder e2e for the read-time forward prefetch transient.
#
# A read elevates the pieces it covers to `current_priority` (7) and the four
# pieces just past the read range descend through `step_priorities` to
# `[6][5][4][3]`; the rest of the access window stays at `window_edge_priority`
# (1).  QA scenario 21 samples `.stats` *while the read is in flight* to catch
# that `[6][5][4][3]` transient, but the stock 4 MiB selfseed torrent downloads
# in milliseconds on a local seeder, so the read (and with it the transient) is
# gone before the first ~300 ms poll lands — the ladder is only ever observed
# on a seed large enough that a read stays download-bound across a poll.
#
# This script seeds a >=512 MiB single-file torrent (2048 x 256 KiB pieces)
# with `ci/run_self_seed_env.sh --payload-mib`, reads a large chunk in the
# background, and samples `.stats` in a tight loop until it observes the
# descending ladder immediately after the read range:
#
#   ... [7][6][5][4][3] [1][1]...[1] ...
#        ^ read range  ^ step ladder  ^ window edge
#
# It complements `window_boundary_e2e.sh`: that script covers the >4 GiB window
# edge (`[1]` / outside `[]`) via a sparse `--payload-gib` seed; this one covers
# the in-window descending ladder at >=512 MiB, which the 4 MiB selfseed cannot
# hold in flight long enough to sample.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/prefetch_ladder_e2e.sh [torrentfs_binary] [mountpoint]
#   env: PAYLOAD_MIB (default 512, must be >= 512),
#        READ_MIB (default 256, the chunk read while sampling),
#        POLL_S (default 0.02), READ_TIMEOUT_S (default 300),
#        TRACKER_PORT (default 0 = OS-assigned free port),
#        SELFSEED_OUT (default /tmp/prefetch_ladder_selfseed)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_prefetch_ladder_mnt}"
PAYLOAD_MIB="${PAYLOAD_MIB:-512}"
READ_MIB="${READ_MIB:-256}"
POLL_S="${POLL_S:-0.02}"
READ_TIMEOUT_S="${READ_TIMEOUT_S:-300}"
TRACKER_PORT="${TRACKER_PORT:-0}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/prefetch_ladder_selfseed}"
SEEDER_LOG="$SELFSEED_OUT/seeder.log"
TORRENTFS_LOG="$(mktemp)"
CONFIG_FILE="$(mktemp)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
SEEDER_PID=""
TORRENTFS_PID=""
DD_PID=""

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
    if [ -n "$DD_PID" ]; then kill "$DD_PID" 2>/dev/null || true; fi
    if [ -n "$TORRENTFS_PID" ]; then kill_tree "$TORRENTFS_PID"; fi
    if [ -n "$SEEDER_PID" ]; then kill_tree "$SEEDER_PID"; fi
    # Reap the killed children before returning.  torrentfs unmounts $MNT as
    # its graceful shutdown completes; if a subsequent run (or a caller reusing
    # the default mountpoint) mounts there first, that lingering unmount tears
    # the fresh mount down ("FUSE session ended without a shutdown signal").
    [ -z "$TORRENTFS_PID" ] || wait "$TORRENTFS_PID" 2>/dev/null || true
    [ -z "$SEEDER_PID" ] || wait "$SEEDER_PID" 2>/dev/null || true
    [ -z "$DD_PID" ] || wait "$DD_PID" 2>/dev/null || true
    # Unmount before removing the tree; the FUSE daemon holds the mountpoint.
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    # The killed processes may still be flushing the cache when this runs; a
    # racing write leaves an unremovable `pieces/` entry, so retry briefly.
    for _ in 1 2 3 4 5; do
        rm -rf "$MNT" "$SELFSEED_OUT" "$CACHE_DIR" "$DB_DIR" "$TORRENTFS_LOG" "$CONFIG_FILE" 2>/dev/null && break
        sleep 1
    done
    return 0
}
trap cleanup EXIT

fail() {
    echo "prefetch_ladder_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    echo "  samples=${SAMPLES:-0} ladder_seen=${LADDER_SEEN:-0}" >&2
    if [ -n "${LADDER_SAMPLE:-}" ]; then
        echo "  ladder sample: $(printf '%s' "$LADDER_SAMPLE" | cut -c1-160)" >&2
    elif [ -n "${LAST_SAMPLE:-}" ]; then
        echo "  last sample  : $(printf '%s' "$LAST_SAMPLE" | cut -c1-160)" >&2
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

# Positive-integer guard: a non-numeric or zero value fails here with a clear
# message instead of a bash arithmetic error deeper in.
require_positive_int() {
    local name="$1" value="$2"
    case "$value" in
        ''|*[!0-9]*) fail "$name must be a positive integer (got '${value}')" ;;
    esac
    [ "$(( 10#$value ))" -gt 0 ] || fail "$name must be > 0 (got '${value}')"
}
require_positive_int PAYLOAD_MIB "$PAYLOAD_MIB"
require_positive_int READ_MIB "$READ_MIB"
require_positive_int READ_TIMEOUT_S "$READ_TIMEOUT_S"

# Positive-decimal guard for the poll interval, which takes fractional seconds
# (`0.02`) and so cannot use the integer guard above.  A non-numeric value
# would otherwise abort with a bare `sleep` error inside the sampling loop,
# bypassing the clean fail() diagnostics every other knob gets; a zero interval
# would spin that loop as fast as `.stats` can be read.
require_positive_number() {
    local name="$1" value="$2"
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || fail "$name must be a positive number (got '${value}')"
    [ -n "$(printf '%s' "$value" | tr -d '0.')" ] \
        || fail "$name must be > 0 (got '${value}')"
}
require_positive_number POLL_S "$POLL_S"
# The whole point of the >=512 MiB seed is that the read stays in flight long
# enough to sample; a smaller payload silently reintroduces the 4 MiB gap.
[ "$(( 10#$PAYLOAD_MIB ))" -ge 512 ] \
    || fail "PAYLOAD_MIB=${PAYLOAD_MIB} is below the 512 MiB floor this scenario needs"
[ "$(( 10#$READ_MIB ))" -le "$(( 10#$PAYLOAD_MIB ))" ] \
    || fail "READ_MIB=${READ_MIB} exceeds the ${PAYLOAD_MIB} MiB payload"

# `10#` accepts leading zeros, matching `run_self_seed_env.sh`'s validation.
PAYLOAD_BYTES=$(( 10#$PAYLOAD_MIB * 1024 * 1024 ))

mkdir -p "$SELFSEED_OUT"
echo "[prefetch_ladder_e2e] seeding ${PAYLOAD_MIB} MiB single-file payload…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --port "$TRACKER_PORT" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" > "$SEEDER_LOG" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 300); do
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
echo "[prefetch_ladder_e2e] torrent: ${EXPECTED_PIECES} pieces x ${PIECE_LEN} B"

# The seeder hash-checks the full payload before announcing; wait for its
# "ready" line so the read finds a live peer instead of spending its timeout
# window on peer discovery.
for _ in $(seq 1 300); do
    grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null && break
    kill -0 "$SEEDER_PID" 2>/dev/null \
        || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited before becoming ready"; }
    sleep 1
done
grep -q '\[seeder\] ready' "$SEEDER_LOG" 2>/dev/null || fail "seeder did not become ready"

# Pin the two bounds the ladder depends on: a cache large enough to hold the
# whole payload (so the read is never cache-bound) and the default 4096 MiB
# access window (so the ladder's four steps sit well inside the window and the
# `[1]` window edge follows them).
printf '[cache]\ncache_size = %d\n[piece_priority]\naccess_window_mb = 4096\n' \
    "$(( 1024 * 1024 * 1024 ))" > "$CONFIG_FILE"
mkdir -p "$MNT"

echo "[prefetch_ladder_e2e] mounting torrentfs…"
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

# The read must run from an unaccessed offset: the first reader's gradient is
# what carries the ladder, and pieces the prefetch already cached render as
# `[X n]`/`[x]` instead of `[6][5][4][3]`.  A fresh mount has nothing cached,
# so offset 0 is the guaranteed-fresh start.
echo "[prefetch_ladder_e2e] reading ${READ_MIB} MiB while sampling .stats (poll ${POLL_S}s)…"
LADDER_SEEN=0
LADDER_SAMPLE=""
LAST_SAMPLE=""
SAMPLES=0
START=$SECONDS
dd if="$DATA_FILE" of=/dev/null bs=1M count="$READ_MIB" status=none &
DD_PID=$!

while kill -0 "$DD_PID" 2>/dev/null; do
    [ $(( SECONDS - START )) -lt "$(( 10#$READ_TIMEOUT_S ))" ] \
        || fail "read did not finish within ${READ_TIMEOUT_S}s (${SAMPLES} samples)"
    LINE="$(sed -n 's/^  Pieces: //p' "$STATS_FILE" 2>/dev/null || true)"
    SAMPLES=$(( SAMPLES + 1 ))
    LAST_SAMPLE="$LINE"
    case "$LINE" in
        *"[7][6][5][4][3]"*)
            LADDER_SEEN=1
            LADDER_SAMPLE="$LINE"
            break
            ;;
    esac
    sleep "$POLL_S"
done

if [ "$LADDER_SEEN" -eq 1 ]; then
    # The ladder was sampled: stop the read.  The observed `[7][6][5][4][3]`
    # already proves a read range was active (the `[7]` it follows), so
    # draining the rest of the chunk would add CI time with no new signal.
    kill "$DD_PID" 2>/dev/null || true
    wait "$DD_PID" 2>/dev/null || true
    DD_PID=""
else
    # The read finished without the ladder.  Distinguish "the read itself
    # failed" from "the read succeeded but the transient was never sampled".
    if ! wait "$DD_PID"; then
        DD_PID=""
        fail "read failed (dd exited non-zero) after ${SAMPLES} samples"
    fi
    DD_PID=""
    fail "never observed the [7][6][5][4][3] prefetch ladder in ${SAMPLES} samples"
fi

# The matched substring already ties the four descending steps to the read
# range (the `[7]` it follows), so nothing further to assert: a `[6][5][4][3]`
# run can only come from the forward prefetch gradient.
echo "[prefetch_ladder_e2e] PASS (${EXPECTED_PIECES} pieces; ladder after read range; ${SAMPLES} samples)"

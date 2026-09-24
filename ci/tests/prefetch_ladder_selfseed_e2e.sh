#!/usr/bin/env bash
# Prefetch-ladder e2e on the *default* 4 MiB selfseed at the QA poll cadence.
#
# QA scenario 21 samples `.stats` while a read is in flight to catch the
# `[7][6][5][4][3]` ladder that read publishes, but a ~200ms poll misses it on
# the stock 4 MiB selfseed: a loopback seeder hands the four prefetched pieces
# over in milliseconds, so the transient is gone before the poll lands.
# `prefetch_ladder_e2e.sh` covers the same ladder with a >=512 MiB seed, where
# the read stays download-bound across a poll; this script keeps the default
# payload and stretches the transient instead, via the seeder's
# `--announce-delay` hold.
#
# That hold is the lever that works here: libtorrent does not rate-limit peers
# on the local network (a 256 KiB/s `[rate_limits] download_rate_limit` still
# moved 1 MiB in ~0.2s on loopback), so bandwidth throttling cannot slow a
# loopback swarm down.  Withholding the seeder can: the read parks in peer
# discovery with its gradient published, so `.stats` shows the ladder for as
# long as the swarm stays empty, and the seeder then joins and serves the read.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/prefetch_ladder_selfseed_e2e.sh [torrentfs_binary] [mountpoint]
#   env: ANNOUNCE_DELAY_S (default 6 — must outlast the mount→read startup but
#          stay inside the reader's ~9s peer-discovery window),
#        PAYLOAD_MIB (default 4), READ_MIB (default 1), POLL_S (default 0.2),
#        READ_TIMEOUT_S (default 60), TRACKER_PORT (default 0 = OS-assigned),
#        SELFSEED_OUT (default /tmp/ladder_selfseed_out)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_ladder_selfseed_mnt}"
PAYLOAD_MIB="${PAYLOAD_MIB:-4}"
READ_MIB="${READ_MIB:-1}"
POLL_S="${POLL_S:-0.2}"
ANNOUNCE_DELAY_S="${ANNOUNCE_DELAY_S:-6}"
READ_TIMEOUT_S="${READ_TIMEOUT_S:-60}"
TRACKER_PORT="${TRACKER_PORT:-0}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/ladder_selfseed_out}"
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
    # Reap the killed children before returning: torrentfs unmounts $MNT as its
    # graceful shutdown completes, and that lingering unmount would tear down a
    # fresh mount a later run makes there.
    [ -z "$DD_PID" ] || wait "$DD_PID" 2>/dev/null || true
    [ -z "$TORRENTFS_PID" ] || wait "$TORRENTFS_PID" 2>/dev/null || true
    [ -z "$SEEDER_PID" ] || wait "$SEEDER_PID" 2>/dev/null || true
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
    echo "prefetch_ladder_selfseed_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    echo "  samples=${SAMPLES:-0} ladder_samples=${LADDER_SAMPLES:-0} announce_delay=${ANNOUNCE_DELAY_S}s" >&2
    if [ -n "${LADDER_SAMPLE:-}" ]; then
        echo "  ladder sample: $(printf '%s' "$LADDER_SAMPLE" | cut -c1-160)" >&2
    elif [ -n "${LAST_SAMPLE:-}" ]; then
        echo "  last sample  : $(printf '%s' "$LAST_SAMPLE" | cut -c1-160)" >&2
    fi
    if [ -n "${SEEDER_LOG:-}" ] && [ -f "$SEEDER_LOG" ]; then
        echo "  seeder ready at read start: $(grep -c '\[seeder\] ready' "$SEEDER_LOG" || true)" >&2
        echo "  seeder log tail:" >&2
        tail -10 "$SEEDER_LOG" >&2 || true
    fi
    if [ -n "${TORRENTFS_LOG:-}" ] && [ -f "$TORRENTFS_LOG" ]; then
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
require_positive_int ANNOUNCE_DELAY_S "$ANNOUNCE_DELAY_S"
require_positive_int READ_TIMEOUT_S "$READ_TIMEOUT_S"

# Positive-decimal guard for the poll interval, which takes fractional seconds
# (`0.2`) and so cannot use the integer guard above.
require_positive_number() {
    local name="$1" value="$2"
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || fail "$name must be a positive number (got '${value}')"
    [ -n "$(printf '%s' "$value" | tr -d '0.')" ] \
        || fail "$name must be > 0 (got '${value}')"
}
require_positive_number POLL_S "$POLL_S"
[ "$(( 10#$READ_MIB ))" -le "$(( 10#$PAYLOAD_MIB ))" ] \
    || fail "READ_MIB=${READ_MIB} exceeds the ${PAYLOAD_MIB} MiB payload"

# Pin the two bounds the ladder depends on: a cache that leaves the whole
# payload inside the access window (the window is capped at `cache - 1 piece`,
# and the four ladder steps must fit inside it) and the shipped 4096 MiB
# window.  Both values are the compiled defaults; pinning them keeps the
# assertion independent of a future default change.  LSD and DHT are off so
# the swarm is only this script's tracker + seeder: either would let the daemon
# find an unrelated local seeder for the same deterministic info_hash (the
# self-seed payload is identical on every host) and serve the read before the
# delayed seeder joins.
printf '[cache]\ncache_size = %d\n[piece_priority]\naccess_window_mb = 4096\n[local_discovery]\nlsd_enabled = false\n[dht]\nenabled = false\n' \
    "$(( 1024 * 1024 * 1024 ))" > "$CONFIG_FILE"

# Mount the daemon *before* the seeder starts: the announce hold is measured
# from the seeder's start, so everything the read needs must already be up when
# it launches, or the hold would elapse before the read begins.
mkdir -p "$MNT"
echo "[prefetch_ladder_selfseed_e2e] mounting torrentfs…"
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

mkdir -p "$SELFSEED_OUT"
echo "[prefetch_ladder_selfseed_e2e] seeding ${PAYLOAD_MIB} MiB selfseed, held out of the swarm for ${ANNOUNCE_DELAY_S}s…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --announce-delay "$ANNOUNCE_DELAY_S" \
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
grep -q '\[seeder\] holding out of the swarm' "$SEEDER_LOG" \
    || { tail -20 "$SEEDER_LOG" >&2; fail "seeder did not report the announce hold"; }

cp "$SELFSEED_OUT/selfseed.torrent" "$MNT/metadata/"
# The data entry appears once the daemon persists the torrent; poll tightly so
# the read starts well inside the hold.
DATA_FILE=""
for _ in $(seq 1 600); do
    DATA_FILE="$(find "$MNT/data" -type f ! -name '.stats' 2>/dev/null | head -n1 || true)"
    [ -n "$DATA_FILE" ] && break
    sleep 0.05
done
[ -n "$DATA_FILE" ] || fail "data file did not appear"
STATS_FILE="$(dirname "$DATA_FILE")/.stats"

# The hold must still be in effect: a seeder that already joined would leave
# the transient at its ~200ms natural length, and this script would be timing
# the wrong window rather than the hold.
if grep -q '\[seeder\] ready' "$SEEDER_LOG"; then
    fail "seeder joined the swarm before the read started; raise ANNOUNCE_DELAY_S above ${ANNOUNCE_DELAY_S}s"
fi

echo "[prefetch_ladder_selfseed_e2e] reading ${READ_MIB} MiB while sampling .stats (poll ${POLL_S}s)…"
SAMPLES=0
LADDER_SAMPLES=0
LADDER_SAMPLE=""
LAST_SAMPLE=""
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
            LADDER_SAMPLES=$(( LADDER_SAMPLES + 1 ))
            LADDER_SAMPLE="$LINE"
            ;;
    esac
    sleep "$POLL_S"
done

DD_RC=0
wait "$DD_PID" || DD_RC=$?
DD_PID=""

# Two samples, not one: the ladder has to survive a poll interval for the QA
# cadence to catch it, and a single hit would also come from a sub-poll
# transient the hold is supposed to have removed.
[ "$LADDER_SAMPLES" -ge 2 ] \
    || fail "never observed the [7][6][5][4][3] prefetch ladder in ${SAMPLES} samples"
# The read still has to finish: the hold is a delay, not an outage.
[ "$DD_RC" -eq 0 ] \
    || fail "read failed (dd exited ${DD_RC}) — the seeder's announce hold outlasted the reader's peer-discovery window"

echo "[prefetch_ladder_selfseed_e2e] PASS (${LADDER_SAMPLES}/${SAMPLES} polls showed the ladder at ${POLL_S}s; read served after the ${ANNOUNCE_DELAY_S}s hold)"

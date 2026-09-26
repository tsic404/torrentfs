#!/usr/bin/env bash
# End-to-end regression for the self-seed *steady state* acceptance (QA skill
# scenario 21, small cache): with a 512 KiB piece cache a 1 MiB read must
# download, and the post-read steady state must show both the cached pieces
# (`[X n]`) and the retained prefetch gradient (`[7][6]`) — the reader's
# released gradient kept as the prefetch window, with the pieces already on
# disk dropped from it.
#
# The swarm is loopback-only (tracker + seeder on 127.0.0.1), the configuration
# that makes the daemon log one `skipping tracker announce (unreachable)` line
# per *non-loopback* listen socket: a wildcard `listen_interfaces` expands to
# one socket per local address, and libtorrent only announces to a tracker from
# a socket that can route to it, so exactly the loopback socket announces to a
# loopback tracker.  This script asserts the outcome that matters instead of
# that log noise — the idle handle sees the seeder (`Peers: n`, n >= 1) before
# any read, and the read completes — so a regression to "upload-only, zero
# peers, all-empty pieces" fails here rather than surfacing later as a QA
# environment block.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/small_cache_steady_state_e2e.sh [torrentfs_binary] [mountpoint]
#   env: CACHE_BYTES (default 524288 — 2 pieces, so the ladder is 1 step),
#        PAYLOAD_MIB (default 4), READ_MIB (default 1),
#        READ_SKIP_MIB (default 2 — read pieces 8-11 of 16),
#        IDLE_PEER_TIMEOUT_S (default 30), STEADY_SAMPLES (default 6),
#        STEADY_POLL_S (default 0.3), READ_TIMEOUT_S (default 60),
#        SELFSEED_OUT (default /tmp/small_cache_steady_selfseed)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_small_cache_steady_mnt}"
CACHE_BYTES="${CACHE_BYTES:-524288}"
PAYLOAD_MIB="${PAYLOAD_MIB:-4}"
READ_MIB="${READ_MIB:-1}"
READ_SKIP_MIB="${READ_SKIP_MIB:-2}"
IDLE_PEER_TIMEOUT_S="${IDLE_PEER_TIMEOUT_S:-30}"
STEADY_SAMPLES="${STEADY_SAMPLES:-6}"
STEADY_POLL_S="${STEADY_POLL_S:-0.3}"
READ_TIMEOUT_S="${READ_TIMEOUT_S:-60}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/small_cache_steady_selfseed}"
SEEDER_LOG="$SELFSEED_OUT/seeder.log"
TORRENTFS_LOG="$(mktemp)"
CONFIG_FILE="$(mktemp)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
SEEDER_PID=""
TORRENTFS_PID=""
# Declared before `fail` can run so its diagnostics can always expand the array
# (an unset array under `set -u` aborts instead of printing the samples).
SAMPLES=()

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
    # Reap the killed children before returning: torrentfs unmounts $MNT as its
    # graceful shutdown completes, and that lingering unmount would tear down a
    # fresh mount a later run makes there.
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
    echo "small_cache_steady_state_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    echo "  binary=$BIN cache_bytes=${CACHE_BYTES} read=${READ_MIB}MiB@${READ_SKIP_MIB}MiB" >&2
    if [ "${#SAMPLES[@]}" -gt 0 ]; then
        for sample in "${SAMPLES[@]}"; do
            echo "  pieces sample: $sample" >&2
        done
    fi
    if [ -n "${IDLE_LINE:-}" ]; then
        echo "  idle peers   : $IDLE_LINE" >&2
    fi
    if [ -n "${SEEDER_LOG:-}" ] && [ -f "$SEEDER_LOG" ]; then
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

# Positive-integer guards: a non-numeric or zero value fails here with a clear
# message instead of a bash arithmetic error deeper in.
require_positive_int() {
    local name="$1" value="$2"
    case "$value" in
        ''|*[!0-9]*) fail "$name must be a positive integer (got '${value}')" ;;
    esac
    [ "$(( 10#$value ))" -gt 0 ] || fail "$name must be > 0 (got '${value}')"
}
require_positive_int CACHE_BYTES "$CACHE_BYTES"
require_positive_int PAYLOAD_MIB "$PAYLOAD_MIB"
require_positive_int READ_MIB "$READ_MIB"
require_positive_int IDLE_PEER_TIMEOUT_S "$IDLE_PEER_TIMEOUT_S"
require_positive_int STEADY_SAMPLES "$STEADY_SAMPLES"
require_positive_int READ_TIMEOUT_S "$READ_TIMEOUT_S"
# Positive-decimal guard for the poll interval, which takes fractional seconds
# (`0.3`) and so cannot use the integer guard above.
require_positive_number() {
    local name="$1" value="$2"
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || fail "$name must be a positive number (got '${value}')"
    [ -n "$(printf '%s' "$value" | tr -d '0.')" ] \
        || fail "$name must be > 0 (got '${value}')"
}
require_positive_number STEADY_POLL_S "$STEADY_POLL_S"
# READ_SKIP_MIB may legitimately be 0 (read from the file head).
case "$READ_SKIP_MIB" in
    ''|*[!0-9]*) fail "READ_SKIP_MIB must be a non-negative integer (got '${READ_SKIP_MIB}')" ;;
esac
[ "$(( 10#$READ_SKIP_MIB + 10#$READ_MIB ))" -le "$(( 10#$PAYLOAD_MIB ))" ] \
    || fail "READ_MIB + READ_SKIP_MIB exceeds the ${PAYLOAD_MIB} MiB payload"

# The 2-piece cache is what makes the steady state interesting: the 1 MiB read
# covers 4 pieces, so the tail pieces stay queued in the retained gradient
# (`[7][6]` = 1 ladder step) while the first two land in the cache (`[X n]`).
# Pin the access window too, so the assertion does not depend on a future
# default change.  LSD and DHT are off so the swarm is only this script's
# tracker + seeder — either would let the daemon find an unrelated local seeder
# for the same deterministic info_hash (the self-seed payload is identical on
# every host) and serve the read without ever announcing.
printf '[cache]\ncache_size = %d\n[piece_priority]\naccess_window_mb = 4096\n[local_discovery]\nlsd_enabled = false\n[dht]\nenabled = false\n' \
    "$(( 10#$CACHE_BYTES ))" > "$CONFIG_FILE"

mkdir -p "$SELFSEED_OUT"
echo "[small_cache_steady_state_e2e] seeding ${PAYLOAD_MIB} MiB selfseed on loopback…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" > "$SEEDER_LOG" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 300); do
    [ -f "$SELFSEED_OUT/selfseed.torrent" ] && grep -q '\[seeder\] ready' "$SEEDER_LOG" && break
    kill -0 "$SEEDER_PID" 2>/dev/null || { tail -20 "$SEEDER_LOG" >&2; fail "seeder exited during startup"; }
    sleep 1
done
[ -f "$SELFSEED_OUT/selfseed.torrent" ] || fail "seeder did not produce a torrent"
grep -q '\[seeder\] ready' "$SEEDER_LOG" || fail "seeder never reported ready"

mkdir -p "$MNT"
echo "[small_cache_steady_state_e2e] mounting torrentfs (cache ${CACHE_BYTES} B)…"
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
for _ in $(seq 1 300); do
    DATA_FILE="$(find "$MNT/data" -type f ! -name '.stats' 2>/dev/null | head -n1 || true)"
    [ -n "$DATA_FILE" ] && break
    sleep 0.1
done
[ -n "$DATA_FILE" ] || fail "data file did not appear"
STATS_FILE="$(dirname "$DATA_FILE")/.stats"

# The handle is added in upload_mode (connect, request nothing).  It must still
# announce — from the loopback listen socket — and see the seeder before any
# read: a handle stuck at `Peers: 0` is the reported failure mode, and the
# acceptance's steady state is unobservable without a reachable seeder.
IDLE_LINE=""
for _ in $(seq 1 "$(( 10#$IDLE_PEER_TIMEOUT_S * 2 ))"); do
    IDLE_LINE="$(sed -n 's/^  Peers: //p' "$STATS_FILE" 2>/dev/null | head -n1 || true)"
    case "$IDLE_LINE" in
        "0  Seeds: 0") ;;
        "") ;;
        *) break ;;
    esac
    sleep 0.5
done
case "$IDLE_LINE" in
    ""|"0  Seeds: 0")
        fail "idle handle never saw the loopback seeder within ${IDLE_PEER_TIMEOUT_S}s (Peers: ${IDLE_LINE:-<no sample>})" ;;
esac
echo "[small_cache_steady_state_e2e] idle handle reached Peers: $IDLE_LINE"

echo "[small_cache_steady_state_e2e] reading ${READ_MIB} MiB at offset ${READ_SKIP_MIB} MiB…"
if ! timeout "$READ_TIMEOUT_S" dd if="$DATA_FILE" of=/dev/null \
    bs=1M count="$READ_MIB" skip="$READ_SKIP_MIB" status=none; then
    fail "read did not complete within ${READ_TIMEOUT_S}s"
fi

# Post-read steady state: sample the whole window before asserting anything.
# The retained gradient is transient (its queued pieces download, or leave the
# window as the cache fills), so the two halves of the acceptance are checked
# against different samples: the cached pieces and the retained gradient must
# coexist in at least one sample, and the cache must still hold a piece in the
# *final* sample — a read that downloaded and then lost every piece (cache
# miss on the re-read) is not the acceptance either.
SAMPLES=()
for _ in $(seq 1 "$STEADY_SAMPLES"); do
    LINE="$(sed -n 's/^  Pieces: //p' "$STATS_FILE" 2>/dev/null | head -n1 || true)"
    SAMPLES+=("$LINE")
    sleep "$STEADY_POLL_S"
done

STEADY_OK=""
for LINE in "${SAMPLES[@]}"; do
    case "$LINE" in
        *"[X "*)
            case "$LINE" in
                *"[7]"*) case "$LINE" in *"[6]"*) STEADY_OK="$LINE" ;; esac ;;
            esac
            ;;
    esac
    [ -n "$STEADY_OK" ] && break
done
[ -n "$STEADY_OK" ] || fail "steady state never showed cached pieces ([X n]) together with the retained prefetch gradient ([7][6])"

LAST_SAMPLE="${SAMPLES[${#SAMPLES[@]}-1]}"
case "$LAST_SAMPLE" in
    *"[X "*) ;;
    *) fail "cached pieces ([X n]) missing from the final steady state: $LAST_SAMPLE" ;;
esac

echo "[small_cache_steady_state_e2e] PASS — steady state: $STEADY_OK (final sample: $LAST_SAMPLE)"

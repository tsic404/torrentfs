#!/usr/bin/env bash
# End-to-end regression for the `.stats` slow-swarm alert (a *connected* seeder
# that makes no progress).  A loopback libtorrent seeder cannot hold that state
# — it serves every requested piece immediately — so the swarm here is a
# stalled peer (`run_self_seed_env.sh --stall-peer`): it handshakes, advertises
# a complete bitfield, then sends nothing while a read waits in the engine.
#
# The assertions are the alert's own contract:
#   * a sample taken before any read waits never renders `⚠ Slow swarm` (the
#     line reports a waiting reader, so an idle torrent cannot carry it);
#   * samples that already show the waiting read, the connected seeder and the
#     zero rate, but not yet the elapsed grace window, do not render it either;
#   * once the window has held, the line appears and names it — `no progress
#     for Ns` with N >= the engine's grace — and the same sample still shows
#     `Waiting: yes`, `Seeds: >= 1` and a zero rate.
#
# Expect the alert ~25s after the read starts, not after the grace alone: the
# read's own announce and peer traffic has to decay out of libtorrent's rate
# average before `download_rate == 0` holds, and only then does the grace
# window begin.
#
# Requires a FUSE-capable environment (/dev/fuse + fusermount3) and both
# binaries built: `cargo build --locked --release` and the seeder example
# (`cargo build --locked --release --example torrentfs-selfseed-env`, which
# `run_self_seed_env.sh` also builds).  Usage:
#   ./ci/tests/slow_swarm_e2e.sh [torrentfs_binary] [mountpoint]
#   env: PAYLOAD_MIB (default 4), READ_MIB (default 1),
#        PEER_TIMEOUT_S (default 60 — wait for the stalled peer to be counted
#        as a seeder), QUIET_TIMEOUT_S (default 90 — wait for libtorrent's rate
#        average to forget the connect handshake, the alert needs a zero rate),
#        HIT_TIMEOUT_S (default 45 — wait for the alert once the read starts;
#        it stays inside the pinned 60s read window the alert needs a parked
#        reader for),
#        SAMPLE_POLL_S (default 0.2), SLOW_SWARM_GRACE_S (default 5 — the
#        engine's `HEALTH_ALERT_SLOW_SWARM_GRACE_SECS`), READ_TIMEOUT_S
#        (default 90, the stalled read's own window), SELFSEED_OUT (default
#        /tmp/slow_swarm_selfseed)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_slow_swarm_mnt}"
PAYLOAD_MIB="${PAYLOAD_MIB:-4}"
READ_MIB="${READ_MIB:-1}"
PEER_TIMEOUT_S="${PEER_TIMEOUT_S:-60}"
QUIET_TIMEOUT_S="${QUIET_TIMEOUT_S:-90}"
HIT_TIMEOUT_S="${HIT_TIMEOUT_S:-45}"
SAMPLE_POLL_S="${SAMPLE_POLL_S:-0.2}"
SLOW_SWARM_GRACE_S="${SLOW_SWARM_GRACE_S:-5}"
READ_TIMEOUT_S="${READ_TIMEOUT_S:-90}"
SELFSEED_OUT="${SELFSEED_OUT:-/tmp/slow_swarm_selfseed}"
SEEDER_LOG="$SELFSEED_OUT/seeder.log"
TORRENTFS_LOG="$(mktemp)"
CONFIG_FILE="$(mktemp)"
CACHE_DIR="$(mktemp -d)"
DB_DIR="$(mktemp -d)"
SEEDER_PID=""
TORRENTFS_PID=""
DD_PID=""
# Declared before `fail` can run so its diagnostics can always expand the array
# (an unset array under `set -u` aborts instead of printing the samples).
SAMPLES=()

cleanup() {
    # `run_self_seed_env.sh` runs the seeder binary as a child, so killing only
    # the wrapper would orphan the stalled peer still holding its listen port.
    # Walk the process tree via /proc (no procps dependency).
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
    echo "slow_swarm_e2e: FAIL — $1" >&2
    # Diagnostics so a CI-only failure is debuggable from the job log alone.
    echo "  binary=$BIN payload=${PAYLOAD_MIB}MiB read=${READ_MIB}MiB grace=${SLOW_SWARM_GRACE_S}s" >&2
    if [ "${#SAMPLES[@]}" -gt 0 ]; then
        local i=0
        for sample in "${SAMPLES[@]}"; do
            i=$(( i + 1 ))
            echo "  sample $i: $sample" >&2
        done
    fi
    if [ -n "${IDLE_SAMPLE:-}" ]; then
        echo "  idle sample: $IDLE_SAMPLE" >&2
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
require_positive_int PAYLOAD_MIB "$PAYLOAD_MIB"
require_positive_int READ_MIB "$READ_MIB"
require_positive_int PEER_TIMEOUT_S "$PEER_TIMEOUT_S"
require_positive_int QUIET_TIMEOUT_S "$QUIET_TIMEOUT_S"
require_positive_int HIT_TIMEOUT_S "$HIT_TIMEOUT_S"
require_positive_int SLOW_SWARM_GRACE_S "$SLOW_SWARM_GRACE_S"
require_positive_int READ_TIMEOUT_S "$READ_TIMEOUT_S"
# Positive-decimal guard for the poll interval, which takes fractional seconds.
require_positive_number() {
    local name="$1" value="$2"
    [[ "$value" =~ ^[0-9]+([.][0-9]+)?$ ]] \
        || fail "$name must be a positive number (got '${value}')"
    [ -n "$(printf '%s' "$value" | tr -d '0.')" ] \
        || fail "$name must be > 0 (got '${value}')"
}
require_positive_number SAMPLE_POLL_S "$SAMPLE_POLL_S"
[ "$(( 10#$READ_MIB ))" -le "$(( 10#$PAYLOAD_MIB ))" ] \
    || fail "READ_MIB exceeds the ${PAYLOAD_MIB} MiB payload"

# True when VALUE is a positive decimal integer — the `.stats` fields are read
# from a file that may not carry them (yet), so every read is validated before
# it reaches arithmetic.
is_positive_int() {
    case "${1:-}" in
        ''|*[!0-9]*) return 1 ;;
        *) [ "$(( 10#$1 ))" -gt 0 ] ;;
    esac
}

# One `.stats` sample as `<peers>|<seeds>|<rate>|<waiting>|<alert>`.  `rate` is
# the value the alert's `download_rate == 0` condition reads, `alert` the whole
# health line when one is rendered (empty otherwise).
sample_stats() {
    awk '
        /^  Peers: /   { peers = $2; seeds = $4 }
        /^  Rate: /    { rate = $3 }
        /^  Waiting: / { waiting = $2 }
        /⚠/            { alert = $0 }
        END { printf "%s|%s|%s|%s|%s\n", peers, seeds, rate, waiting, alert }
    ' "$1" 2>/dev/null || true
}

# LSD and DHT are off so the swarm is only this script's tracker + stalled peer
# — either would let the daemon find another local peer for the same
# deterministic info hash and serve the read instead.  The read timeout is
# pinned: with a seeder connected, the piece wait is the full
# `read_timeout_secs`, which has to outlast the 5s grace plus a sample's slack.
printf '[local_discovery]\nlsd_enabled = false\n[dht]\nenabled = false\n[timeouts]\nread_timeout_secs = 60\npeer_discovery_wait_secs = 30\n' \
    > "$CONFIG_FILE"

mkdir -p "$SELFSEED_OUT"
echo "[slow_swarm_e2e] seeding ${PAYLOAD_MIB} MiB selfseed with a stalled peer on loopback…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 --stall-peer \
    --output-dir "$SELFSEED_OUT" > "$SEEDER_LOG" 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 300); do
    [ -f "$SELFSEED_OUT/selfseed.torrent" ] && grep -q '\[stall-peer\] ready' "$SEEDER_LOG" && break
    kill -0 "$SEEDER_PID" 2>/dev/null || { tail -20 "$SEEDER_LOG" >&2; fail "stalled peer exited during startup"; }
    sleep 1
done
[ -f "$SELFSEED_OUT/selfseed.torrent" ] || fail "stalled peer did not produce a torrent"
grep -q '\[stall-peer\] ready' "$SEEDER_LOG" || fail "stalled peer never reported ready"

mkdir -p "$MNT"
echo "[slow_swarm_e2e] mounting torrentfs…"
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

# Idle precondition, read from `.stats` rather than assumed: the stalled peer
# must be *counted as a seeder* (`num_seeds >= 1`, the alert's first condition)
# and libtorrent's rate average must have forgotten the connect handshake
# (`download_rate == 0`, its second).  A harness regression — a peer the
# tracker does not hand out, a peer that never completes the handshake — fails
# here with that diagnostic instead of as a puzzling missing-alert failure.
# Both waits are deadlines, not sample counts, so the poll interval stays a
# tuning knob rather than a timeout multiplier.
IDLE_SAMPLE=""
IDLE_DEADLINE=$(( $(date +%s) + 10#$PEER_TIMEOUT_S ))
while :; do
    IDLE_SAMPLE="$(sample_stats "$STATS_FILE")"
    IFS='|' read -r idle_peers idle_seeds idle_rate idle_waiting idle_alert <<<"$IDLE_SAMPLE"
    case "$idle_alert" in
        *"Slow swarm"*) fail "the slow-swarm alert rendered with no read waiting: $IDLE_SAMPLE" ;;
    esac
    is_positive_int "${idle_seeds:-}" && break
    [ "$(date +%s)" -lt "$IDLE_DEADLINE" ] || break
    sleep "$SAMPLE_POLL_S"
done
is_positive_int "${idle_seeds:-}" \
    || fail "the stalled peer was never counted as a seeder within ${PEER_TIMEOUT_S}s (idle sample: $IDLE_SAMPLE)"
# The connect burst (handshake, bitfield, tracker response) decays out of the
# rate average over ~20s; the alert's `download_rate == 0` condition is what
# this waits for.  The read issued below adds its own burst, which is why the
# alert still lands ~20s after it rather than immediately.
IDLE_DEADLINE=$(( $(date +%s) + 10#$QUIET_TIMEOUT_S ))
while :; do
    IDLE_SAMPLE="$(sample_stats "$STATS_FILE")"
    IFS='|' read -r idle_peers idle_seeds idle_rate idle_waiting idle_alert <<<"$IDLE_SAMPLE"
    [ "$idle_rate" = "0" ] && break
    [ "$(date +%s)" -lt "$IDLE_DEADLINE" ] || break
    sleep "$SAMPLE_POLL_S"
done
[ "$idle_rate" = "0" ] \
    || fail "the download rate never settled to 0 within ${QUIET_TIMEOUT_S}s (idle sample: $IDLE_SAMPLE)"
echo "[slow_swarm_e2e] idle swarm: Peers: $idle_peers  Seeds: $idle_seeds, rate 0 — starting the read"

# Sample *across* the parked read.  The first samples land inside the grace
# window (the engine needs the stalled condition to hold before it reports it),
# which is what makes the "no premature alert" assertion below observable.
SAMPLES=()
PRE_GRACE=0
LAST_PRE_GRACE=""
HIT=""
HIT_ALERT=""
timeout "$READ_TIMEOUT_S" dd if="$DATA_FILE" of=/dev/null bs=1M count="$READ_MIB" status=none &
DD_PID=$!
READ_STARTED="$(date +%s)"
while :; do
    SAMPLE="$(sample_stats "$STATS_FILE")"
    SAMPLES+=("$SAMPLE")
    IFS='|' read -r peers seeds rate waiting alert <<<"$SAMPLE"
    case "$alert" in
        *"Slow swarm"*) HIT="$SAMPLE"; HIT_ALERT="$alert"; break ;;
    esac
    # Pre-grace evidence: the waiting read, the connected seeder and the zero
    # rate are all rendered, and the alert is not — the window is still open.
    if [ "$waiting" = "yes" ] && [ "$rate" = "0" ] && is_positive_int "${seeds:-}" && [ -z "$alert" ]; then
        PRE_GRACE=$(( PRE_GRACE + 1 ))
        LAST_PRE_GRACE="$SAMPLE"
    fi
    [ "$(( $(date +%s) - READ_STARTED ))" -lt "$HIT_TIMEOUT_S" ] || break
    sleep "$SAMPLE_POLL_S"
done

[ -n "$HIT" ] || fail "the slow-swarm alert did not appear within ${HIT_TIMEOUT_S}s of the read starting (${#SAMPLES[@]} samples; reader exit: $(kill -0 "$DD_PID" 2>/dev/null && echo running || echo exited))"
# The alert's own text carries the elapsed window — parse it rather than trust
# the line's presence: the grace is the contract being regression-tested.
HIT_SECS="$(printf '%s' "$HIT_ALERT" | sed -n 's/.*no progress for \([0-9][0-9]*\)s.*/\1/p')"
[ -n "$HIT_SECS" ] || fail "the alert does not name the elapsed window: '$HIT_ALERT'"
[ "$(( 10#$HIT_SECS ))" -ge "$(( 10#$SLOW_SWARM_GRACE_S ))" ] \
    || fail "the alert fired before the ${SLOW_SWARM_GRACE_S}s grace window: '$HIT_ALERT'"
[ "$PRE_GRACE" -ge 1 ] \
    || fail "no sample showed the waiting read + connected seeder + zero rate without the alert, so 'it needs the grace window' is unproven (${#SAMPLES[@]} samples)"
# The facts the alert asserts must hold in the very sample that carries it:
# a seeded, zero-rate, waiting read — not a stale render of an earlier state.
IFS='|' read -r hit_peers hit_seeds hit_rate hit_waiting hit_alert <<<"$HIT"
[ "$hit_waiting" = "yes" ] || fail "the alert sample does not report a waiting read: $HIT"
[ "$hit_rate" = "0" ] || fail "the alert sample does not report a zero rate: $HIT"
is_positive_int "${hit_seeds:-}" || fail "the alert sample does not report a connected seeder: $HIT"

echo "[slow_swarm_e2e] PASS — alert after ${#SAMPLES[@]} samples ($PRE_GRACE pre-grace), window ${HIT_SECS}s >= grace ${SLOW_SWARM_GRACE_S}s: $HIT_ALERT"
echo "[slow_swarm_e2e] last pre-grace sample: $LAST_PRE_GRACE"

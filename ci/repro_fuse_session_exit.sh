#!/usr/bin/env bash
# Reproduction + regression harness for the silent FUSE-daemon exit.
#
# Repeated fresh mounts followed by an external `fusermount -u` used to leave
# the torrentfs daemon running as a mountless zombie: the FUSE session thread
# returned cleanly on ENODEV, the main thread stayed parked forever, and the
# next `.stats` read failed with ENOENT while the daemon still held the
# mountpoint lock.  The fix makes the daemon watch its session thread and exit
# with a clear error + status 102 when the mount is lost without a shutdown
# signal.  The harness also regresses the SIGTERM path (exit 0 + clean unmount)
# so the park-loop rewrite is exercised on both sides.
#
# Usage: ./ci/repro_fuse_session_exit.sh [cycles] [torrentfs-binary]
# Requires: /dev/fuse and `fusermount`/`fusermount3` (user_allow_other in
# /etc/fuse.conf for non-root).  Uses a temp dir; cleans up on exit.

set -euo pipefail

CYCLES="${1:-3}"
BINARY="${2:-}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ -z "$BINARY" ]; then
    for cand in "$ROOT_DIR/target/release/torrentfs" "$ROOT_DIR/target/debug/torrentfs"; do
        if [ -x "$cand" ]; then BINARY="$cand"; break; fi
    done
fi
if [ -z "$BINARY" ] || [ ! -x "$BINARY" ]; then
    echo "torrentfs binary not found — build it first (cargo build --release) or pass it as \$2" >&2
    exit 2
fi
if [ ! -c /dev/fuse ]; then
    echo "/dev/fuse not available — cannot exercise the FUSE mount" >&2
    exit 3
fi

case "$CYCLES" in
    ''|*[!0-9]*) echo "cycles must be a positive integer (got '$CYCLES')" >&2; exit 2 ;;
esac

# Exit status the fixed daemon reports when its FUSE session ends without a
# SIGINT/SIGTERM.  Must stay non-zero so the loss is never silent.
EXPECTED_EXIT=102

WORK="$(mktemp -d /tmp/torrentfs-session-exit.XXXXXX)"
MNT="$WORK/mnt"
CACHE="$WORK/cache"
DB="$WORK/db.sqlite"
mkdir -p "$MNT" "$CACHE"

DAEMON_PID=""
cleanup() {
    if [ -n "${DAEMON_PID:-}" ] && kill -0 "$DAEMON_PID" 2>/dev/null; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    fusermount -u -z "$MNT" 2>/dev/null || true
    fusermount3 -u -z "$MNT" 2>/dev/null || true
    rm -rf "$WORK"
}
trap cleanup EXIT

# Reuse the repository's standard FUSE-mount probe rather than re-deriving a
# mountinfo pattern.  entrypoint.sh runs `main` at the bottom, so source only
# the single self-contained `mountpoint_has_fuse` function — extracting the
# whole helper prefix would execute its top-level `parse_args "$@"` (and, if
# the "# ── main" marker ever moved, the entrypoint's own `main`).
HELPERS_FILE="$(mktemp)"
awk '/^mountpoint_has_fuse\(\) \{/{f=1} f{print} f && /^}/{exit}' \
    "$ROOT_DIR/entrypoint.sh" > "$HELPERS_FILE"
# shellcheck disable=SC1090
. "$HELPERS_FILE"
rm -f "$HELPERS_FILE"
if ! type mountpoint_has_fuse >/dev/null 2>&1; then
    echo "failed to extract mountpoint_has_fuse from entrypoint.sh" >&2
    exit 2
fi

# Wait for the daemon (pid $1) to exit and reap it so `wait` reports its real
# status.  `kill -0` still succeeds on a zombie (an exited-but-unreaped child),
# which would otherwise read as "still alive"; /proc/$pid/status reports
# "State: Z" for a zombie, so use it to tell an exited child from a live one.
# Returns the daemon's exit status, or 124 (like `timeout`) when it is still
# alive after the deadline — the caller must distinguish that from a real exit
# code instead of misreporting "expected exit N, got 124".
wait_and_reap() {
    local pid="$1" state deadline
    deadline=$(( SECONDS + 10 ))
    while :; do
        if kill -0 "$pid" 2>/dev/null; then
            state="$(awk '/^State:/{print $2}' "/proc/$pid/status" 2>/dev/null || true)"
            [ "$state" != "Z" ] || break
        else
            break
        fi
        if [ "$SECONDS" -ge "$deadline" ]; then
            return 124
        fi
        sleep 0.1
    done
    wait "$pid" 2>/dev/null
}

# Start the daemon on $MNT and block until mountpoint_has_fuse reports the FUSE
# mount is up.  Returns 0 on readiness, 1 otherwise.
start_and_wait_ready() {
    "$BINARY" --cache "$CACHE" --db "$DB" --log-level warn "$MNT" \
        > "$WORK/daemon.log" 2>&1 &
    DAEMON_PID=$!
    local i
    for i in $(seq 1 50); do
        if mountpoint_has_fuse "$MNT"; then
            return 0
        fi
        if ! kill -0 "$DAEMON_PID" 2>/dev/null; then
            echo "  FAIL: daemon exited before the mount came up (see $WORK/daemon.log)" >&2
            return 1
        fi
        sleep 0.2
    done
    echo "  FAIL: mount never became ready" >&2
    return 1
}

fail=0

# ── session-loss cycles ─────────────────────────────────────────────────────
i=1
while [ "$i" -le "$CYCLES" ]; do
    echo "[cycle $i/$CYCLES] external unmount on $MNT"
    start_and_wait_ready || { fail=1; break; }

    # Read .stats, then externally unmount the filesystem the way QA/containers
    # do between fresh-mount scenarios.
    head -c 16 "$MNT/.stats" >/dev/null
    fusermount -u "$MNT" 2>/dev/null || fusermount3 -u "$MNT"

    # The fixed daemon must notice the session end and exit non-zero — it must
    # NOT hang as a silent mountless zombie.
    rc=0
    wait_and_reap "$DAEMON_PID" || rc=$?
    DAEMON_PID=""
    if [ "$rc" -eq 124 ]; then
        echo "  FAIL: daemon still alive after external unmount (silent zombie)" >&2
        fail=1
        break
    elif [ "$rc" -ne "$EXPECTED_EXIT" ]; then
        echo "  FAIL: expected exit $EXPECTED_EXIT, got $rc" >&2
        fail=1
        break
    fi
    echo "  OK: daemon detected the session loss and exited $rc (not silent)"
    i=$(( i + 1 ))
done

# ── SIGTERM regression ──────────────────────────────────────────────────────
echo "[sigterm] clean shutdown on $MNT"
start_and_wait_ready || { fail=1; }
head -c 16 "$MNT/.stats" >/dev/null
kill -TERM "$DAEMON_PID"
rc=0
wait_and_reap "$DAEMON_PID" || rc=$?
DAEMON_PID=""
if [ "$rc" -eq 124 ]; then
    echo "  FAIL: daemon still alive after SIGTERM" >&2
    fail=1
elif [ "$rc" -ne 0 ]; then
    echo "  FAIL: expected SIGTERM exit 0, got $rc" >&2
    fail=1
elif mountpoint_has_fuse "$MNT"; then
    echo "  FAIL: mountpoint is still a FUSE mount after SIGTERM" >&2
    fail=1
else
    echo "  OK: SIGTERM exits 0 and unmounts cleanly"
fi

if [ "$fail" -ne 0 ]; then
    echo "reproduction FAILED" >&2
    exit 1
fi
echo "PASS: $CYCLES session-loss cycles exit non-zero; SIGTERM exits 0 and unmounts"

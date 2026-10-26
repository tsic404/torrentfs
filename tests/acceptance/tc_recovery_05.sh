#!/usr/bin/env bash
# Design acceptance case TC-recovery-05: a severed FUSE session whose mount is
# still attached makes the daemon exit 104; the container entry restarts it
# within budget (at most 3 recoveries, 1 s backoff each, a log line naming the
# recovery number) and exits 104 on the fourth consecutive loss; an external
# unmount makes the daemon exit 102 and is never restarted
# (`docs/03-acceptance.md` §I AC-84, contracts C-9.3 / C-10.7 / C-10.8).
#
# case TC-recovery-05
#
# FUSE- and process-level executable form: severing a connection means writing
# /sys/fs/fuse/connections/<minor>/abort, and the restart loop lives in
# `entrypoint.sh`, so neither is reachable from the in-process Rust harness.
#
# Each section reports its own environment requirement instead of weakening a
# criterion: the external-unmount matrix needs only a FUSE mount, while the
# severed-session matrix needs root so fusectl exposes the abort file.  A
# section the environment cannot run makes the whole case SKIP (exit 2).
#
# Usage: sudo ./tests/acceptance/tc_recovery_05.sh [torrentfs_binary]
# Exit: 0 = pass, 1 = fail, 2 = the environment cannot execute the case.
#
# The backoff is measured from the recovery log line to the restarted daemon's
# own start time (`/proc/<pid>/stat`), so the 1 s of C-10.7 is witnessed on its
# own instead of being padded by the mount that follows it.
#
# Every external wait is bounded: a wedged mount or a stuck entrypoint fails
# the case with a diagnostic instead of stalling the job.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"

WORK="$(mktemp -d)"
# The rootful entrypoint drops the daemon to TORRENTFS_UID (1000), so it must
# be able to traverse the work tree to reach the `torrentfs` placed on PATH.
chmod 0755 "$WORK"
ENTRY_MNT="$WORK/entry-mnt"
INTERNAL_MNT="/mnt-inner" # the entrypoint's two-stage publish target
DAEMON_PID=""
ENTRY_PID=""
FUSERMOUNT=""
INTERNAL_MNT_USED=0

# The mount helpers live in the shared harness; this case runs its own daemons
# and the entrypoint, so it takes the functions without the single-daemon
# harness (and its EXIT trap).
TORRENTFS_FUSE_HARNESS_LIB_ONLY=1
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"

skip() {
    echo "TC-recovery-05: SKIP — $*" >&2
    exit 2
}

fail() {
    echo "TC-recovery-05: FAIL — $*" >&2
    exit 1
}

cleanup() {
    if [ -n "$ENTRY_PID" ]; then
        kill "$ENTRY_PID" 2>/dev/null || true
        wait "$ENTRY_PID" 2>/dev/null || true
    fi
    if [ -n "$DAEMON_PID" ]; then
        kill "$DAEMON_PID" 2>/dev/null || true
        wait "$DAEMON_PID" 2>/dev/null || true
    fi
    for mnt in "$WORK/a-mnt" "$WORK/c-mnt" "$ENTRY_MNT"; do
        umount "$mnt" 2>/dev/null || true
        [ -n "$FUSERMOUNT" ] && "$FUSERMOUNT" -u -z "$mnt" >/dev/null 2>&1 || true
    done
    rm -rf "$WORK"
    # The fixed publish target is not this case's to reclaim: touch it only
    # after this run published through it.
    if [ "$INTERNAL_MNT_USED" -eq 1 ]; then
        if [ -n "$(fuse_minor_at "$INTERNAL_MNT")" ]; then
            umount "$INTERNAL_MNT" 2>/dev/null || true
            [ -n "$FUSERMOUNT" ] && "$FUSERMOUNT" -u -z "$INTERNAL_MNT" >/dev/null 2>&1 || true
        fi
        # The entrypoint leaves its mountpoint lock inside that target.
        rm -f "$INTERNAL_MNT/.torrentfs.lock" 2>/dev/null || true
        rmdir "$INTERNAL_MNT" 2>/dev/null || true
    fi
}
trap cleanup EXIT

# Wait for a fixed string to appear in $1, bounded by $2 seconds.
wait_for_log() {
    local log="$1" needle="$2" timeout="${3:-30}"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        grep -qF -- "$needle" "$log" 2>/dev/null && return 0
        sleep 0.05
    done
    return 1
}

# Wait until $2 occurrences of $3 are in $1, bounded by $4 seconds.
wait_for_log_count() {
    local log="$1" count="$2" needle="$3" timeout="${4:-60}"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        [ "$(grep -cF -- "$needle" "$log" 2>/dev/null || true)" -ge "$count" ] && return 0
        sleep 0.1
    done
    return 1
}

# Decide a mount that never appeared.  Only a status a mount attempt can
# produce (2 = the mount was refused, 3 = /dev/fuse vanished, 100 = the device
# could not be opened, C-1.2.3..C-1.2.5) together with a line only a mount
# attempt writes means this environment cannot mount FUSE.  A hang, a state
# database, engine or instance-lock failure is a failure, not a skip: the
# daemon warns about `/etc/fuse.conf` before every mount, so that line alone
# says nothing.
mount_unavailable() {
    local log="$1" pid="$2" who="$3" status=0
    if ! wait_pid "$pid" 5; then
        fail "$who kept running without mounting: $(tail -n 3 "$log" | tr '\n' ' ')"
    fi
    status="$WAIT_STATUS"
    if { [ "$status" -eq 2 ] || [ "$status" -eq 3 ] || [ "$status" -eq 100 ]; } &&
        grep -qE "Mount failed: Operation not permitted|Mount failed and /dev/fuse cannot be opened|mountpoint is not empty|fusermount:|/dev/fuse not found" \
            "$log" 2>/dev/null; then
        skip "this environment cannot mount FUSE ($who exited $status): $(tail -n 3 "$log" | tr '\n' ' ')"
    fi
    fail "$who exited $status before mounting: $(tail -n 8 "$log" | tr '\n' ' ')"
}

# Milliseconds since boot, with the fraction `/proc/stat`'s integer `btime`
# lacks.  Both endpoints of the backoff are read on this one clock: the stamp
# below and field 22 of `/proc/<pid>/stat` (clock ticks since boot).  Mixing
# epochs — `btime` floored to the second against an exact wall-clock stamp —
# placed the restart up to a second early and under-reported the interval.
uptime_ms() {
    awk '{ printf "%d\n", ($1 * 1000) + 0.5 }' /proc/uptime 2>/dev/null
}

# Milliseconds-since-boot at which process $1 started: field 22 of
# `/proc/<pid>/stat`, on the same boot clock as `uptime_ms`.  The kernel writes
# it at creation, so it is the restart moment itself rather than the later
# moment the mount is published.
proc_started_uptime_ms() {
    local pid="$1" stat starttime hz
    stat="$(cat "/proc/$pid/stat" 2>/dev/null)" || return 1
    starttime="$(printf '%s\n' "$stat" | awk '{ sub(/.*\) /, ""); split($0, f, " "); print f[20] }')"
    hz="$(getconf CLK_TCK 2>/dev/null || echo 100)"
    [ -n "$starttime" ] || return 1
    echo $(( starttime * 1000 / hz ))
}

# The daemon the entrypoint is running right now: `run_daemon` execs after
# setpriv, so torrentfs is the entrypoint's direct child.
entry_daemon_pid() {
    local child
    for child in $(cat "/proc/$ENTRY_PID/task/$ENTRY_PID/children" 2>/dev/null || true); do
        [ "$(cat "/proc/$child/comm" 2>/dev/null)" = "torrentfs" ] && { echo "$child"; return 0; }
    done
    return 1
}

# Milliseconds-since-boot at the first 10ms poll that finds no torrentfs child
# under the entrypoint; returns 1 when one is still there after $1 seconds.
# This is the start of the backoff window: the abort only marks the request,
# the daemon still runs its engine shutdown and cache flush before exiting,
# and the entrypoint prints the recovery line only after reaping that daemon —
# stamping at either of those would fold that shutdown into the interval and
# let a backoff shorter than the contract's 1s pass.
wait_for_daemon_exit() {
    local timeout="${1:-30}"
    local deadline=$((SECONDS + timeout))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! entry_daemon_pid >/dev/null; then
            uptime_ms
            return 0
        fi
        sleep 0.01
    done
    return 1
}

# Config shared by every daemon here: an ephemeral listen port (the runner may
# already own 6881) and no discovery channels besides the tracker.
CONFIG="$WORK/torrentfs.toml"
cat > "$CONFIG" <<'TOML'
[connections]
listen_interfaces = "127.0.0.1:0"

[dht]
enabled = false

[local_discovery]
lsd_enabled = false
upnp_enabled = false
natpmp_enabled = false
TOML

# Start a daemon on $1 with cache $2 and state db $3, logging to $4.  The
# caller reads `$!`: a command substitution would make the subshell the
# daemon's parent, and `wait` on a non-child reports 127 instead of the exit
# status the case judges.
start_daemon() {
    local mnt="$1" cache="$2" db="$3" log="$4"
    mkdir -p "$mnt" "$cache" "$(dirname "$db")"
    "$BIN" "$mnt" --cache "$cache" --db "$db" --config "$CONFIG" --log-level info >"$log" 2>&1 &
}

# Wait for a background pid, bounded by $2 seconds.  Its status lands in
# WAIT_STATUS; returns 1 on timeout.
wait_pid() {
    local pid="$1" timeout="${2:-60}"
    local deadline=$((SECONDS + timeout))
    WAIT_STATUS=0
    while kill -0 "$pid" 2>/dev/null; do
        if [ "$SECONDS" -ge "$deadline" ]; then
            return 1
        fi
        sleep 0.1
    done
    wait "$pid" 2>/dev/null || WAIT_STATUS=$?
    return 0
}

# The daemon's own mountpoint: the fixed internal target under the rootful
# entrypoint's two-stage publish, otherwise the entrypoint's mountpoint
# itself.  The fixed path counts only when this run is rootful — a rootless
# entrypoint never mounts there, so a FUSE mount found on it belongs to
# another run and must not be adopted (adopting it would detach a foreign
# mount in the external-unmount section and reclaim its lock directory).
# The caller records the choice in the main shell: a command substitution runs
# in a subshell, so anything set in here would never reach cleanup.
daemon_mountpoint() {
    if [ "$ROOTFUL" -eq 1 ] && [ -n "$(fuse_minor_at "$INTERNAL_MNT")" ]; then
        echo "$INTERNAL_MNT"
    else
        echo "$ENTRY_MNT"
    fi
}

[ -x "$BIN" ] || fail "torrentfs binary not found or not executable: $BIN"
# The entrypoint puts this binary on PATH through a symlink in the work tree,
# so a relative argument would resolve against $WORK/bin instead of the caller.
BIN="$(cd "$(dirname "$BIN")" && pwd)/$(basename "$BIN")"
[ -c /dev/fuse ] || skip "/dev/fuse is absent"
for helper in fusermount3 fusermount; do
    if command -v "$helper" >/dev/null 2>&1; then
        FUSERMOUNT="$helper"
        break
    fi
done
[ -n "$FUSERMOUNT" ] || skip "neither fusermount3 nor fusermount is on PATH"
command -v flock >/dev/null 2>&1 || skip "flock is required by the entrypoint"

# ── A. external unmount → daemon exits 102 ──────────────────────────────────
# `fusermount -u` removes the mount from the table; the session ends with no
# signal, which the contract classifies as final (102), not recoverable (104).
A_MNT="$WORK/a-mnt"
A_LOG="$WORK/a.log"
start_daemon "$A_MNT" "$WORK/a-cache" "$WORK/a-db/metadata.db" "$A_LOG"
DAEMON_PID=$!
wait_for_fuse "$A_MNT" 30 || mount_unavailable "$A_LOG" "$DAEMON_PID" "the daemon"
"$FUSERMOUNT" -u "$A_MNT" 2>/dev/null || fail "could not externally unmount $A_MNT"
wait_pid "$DAEMON_PID" 30 || fail "daemon still running 30s after the external unmount"
DAEMON_PID=""
[ "$WAIT_STATUS" -eq 102 ] || fail "external unmount made the daemon exit $WAIT_STATUS, expected 102"
[ -z "$(fuse_minor_at "$A_MNT")" ] || fail "the external unmount left a FUSE mount behind"
echo "[TC-recovery-05] external unmount → daemon exit 102"

# ── B. entrypoint-managed external unmount → 102, never restarted ───────────
ROOTFUL=0
if [ "$(id -u)" -eq 0 ]; then
    command -v setpriv >/dev/null 2>&1 || skip "setpriv is required by the rootful entrypoint path"
    ROOTFUL=1
fi
# The fixed publish target belongs to the rootful two-stage publish: a FUSE
# mount already sitting there is another run's, never this one's.  Rootful has
# nowhere else to publish and skips; a rootless run mounts directly on
# $ENTRY_MNT, so it only has to leave the foreign mount alone (which
# `daemon_mountpoint` enforces below).
if [ -n "$(fuse_minor_at "$INTERNAL_MNT")" ] && [ "$ROOTFUL" -eq 1 ]; then
    skip "$INTERNAL_MNT already carries a FUSE mount (another run)"
fi
mkdir -p "$WORK/bin" "$ENTRY_MNT" "$WORK/entry-cache" "$WORK/entry-db"
ln -sf "$BIN" "$WORK/bin/torrentfs"
ENTRY_LOG="$WORK/entry.log"
PATH="$WORK/bin:$PATH" bash "$ROOT_DIR/entrypoint.sh" "$ENTRY_MNT" \
    --cache "$WORK/entry-cache" --db "$WORK/entry-db/metadata.db" --config "$CONFIG" >"$ENTRY_LOG" 2>&1 &
ENTRY_PID=$!
wait_for_fuse "$ENTRY_MNT" 90 || mount_unavailable "$ENTRY_LOG" "$ENTRY_PID" "the entrypoint"
DAEMON_MNT="$(daemon_mountpoint)"
# Recorded here, in the main shell: the value above comes from a command
# substitution, so anything set inside it would never reach cleanup.
if [ "$DAEMON_MNT" = "$INTERNAL_MNT" ]; then
    INTERNAL_MNT_USED=1
fi
echo "[TC-recovery-05] entrypoint mountpoint $DAEMON_MNT (rootful=$ROOTFUL, internal target in use: $INTERNAL_MNT_USED)"
# Release the published bind first, then detach the daemon's mount: together
# they are the operator's external unmount, and with no mount left the daemon
# reports the final status instead of the recoverable one.
umount "$ENTRY_MNT" 2>/dev/null || umount -l "$ENTRY_MNT" 2>/dev/null || true
# On the direct path the published path *is* the daemon's mount, so the
# release above may already have removed it; only detach the internal mount
# when the two-stage publish left one.
if [ -n "$(fuse_minor_at "$DAEMON_MNT")" ]; then
    "$FUSERMOUNT" -u "$DAEMON_MNT" 2>/dev/null || "$FUSERMOUNT" -u -z "$DAEMON_MNT" 2>/dev/null ||
        fail "could not externally unmount the entrypoint-managed mount at $DAEMON_MNT"
fi
wait_pid "$ENTRY_PID" 60 || fail "the entrypoint still ran 60s after the external unmount"
ENTRY_PID=""
[ "$WAIT_STATUS" -eq 102 ] || fail "external unmount must make the entrypoint exit 102, got $WAIT_STATUS"
if grep -qF "restarting torrentfs" "$ENTRY_LOG"; then
    fail "the entrypoint restarted the daemon after an external unmount (102 is final)"
fi
echo "[TC-recovery-05] entrypoint external unmount → exit 102, no restart"

# ── C. severed session (mount still attached) → 104 + bounded recovery ──────
[ "$(id -u)" -eq 0 ] || skip "severing a FUSE connection needs root (writes /sys/fs/fuse/connections)"
if ! mountpoint -q /sys/fs/fuse/connections; then
    mount -t fusectl fusectl /sys/fs/fuse/connections 2>/dev/null ||
        skip "cannot mount fusectl on /sys/fs/fuse/connections"
fi

# C1: the daemon itself reports the loss as recoverable.
C_MNT="$WORK/c-mnt"
C_LOG="$WORK/c.log"
start_daemon "$C_MNT" "$WORK/c-cache" "$WORK/c-db/metadata.db" "$C_LOG"
DAEMON_PID=$!
wait_for_fuse "$C_MNT" 30 || mount_unavailable "$C_LOG" "$DAEMON_PID" "the daemon"
C_MINOR="$(fuse_minor_at "$C_MNT")"
[ -n "$C_MINOR" ] || fail "no FUSE connection at $C_MNT"
echo 1 >"/sys/fs/fuse/connections/$C_MINOR/abort" 2>/dev/null ||
    skip "cannot sever connection $C_MINOR (fusectl does not expose abort)"
wait_pid "$DAEMON_PID" 30 || fail "daemon still running 30s after its session was severed"
DAEMON_PID=""
[ "$WAIT_STATUS" -eq 104 ] || fail "a severed session with the mount attached exits 104, got $WAIT_STATUS"
echo "[TC-recovery-05] severed session → daemon exit 104"

# C2: the entrypoint recovers at most three times, one second apart, naming
# the recovery number, then exits 104 on the fourth consecutive loss.
ENTRY2_LOG="$WORK/entry2.log"
PATH="$WORK/bin:$PATH" bash "$ROOT_DIR/entrypoint.sh" "$ENTRY_MNT" \
    --cache "$WORK/entry-cache" --db "$WORK/entry-db/metadata.db" --config "$CONFIG" >"$ENTRY2_LOG" 2>&1 &
ENTRY_PID=$!
wait_for_fuse "$ENTRY_MNT" 90 || mount_unavailable "$ENTRY2_LOG" "$ENTRY_PID" "the entrypoint"

# C-10.7 is 1 s of backoff.  The window runs from the previous daemon's exit
# to the replacement's exec, so it contains the detach of the dead mounts, the
# backoff and the exec — not the aborted daemon's shutdown (that happens before
# the window opens) and not the mount that follows the restart.  The floor
# leaves ~90ms for the 10ms poll granularity and the 10ms tick rounding; the
# ceiling bounds the same window on a slow runner.
BACKOFF_MIN_MS=900
BACKOFF_MAX_MS=6000
for attempt in 1 2 3 4; do
    daemon_mnt="$(daemon_mountpoint)"
    if [ "$daemon_mnt" = "$INTERNAL_MNT" ]; then
        INTERNAL_MNT_USED=1
    fi
    minor="$(fuse_minor_at "$daemon_mnt")"
    [ -n "$minor" ] || fail "attempt $attempt: no FUSE connection at $daemon_mnt"
    echo 1 >"/sys/fs/fuse/connections/$minor/abort" 2>/dev/null ||
        fail "attempt $attempt: cannot sever connection $minor"

    if [ "$attempt" -le 3 ]; then
        recovery_line="restarting torrentfs (recovery $attempt/3)"
        # Window start: the daemon is gone (see `wait_for_daemon_exit`).  From
        # here C-10.7 schedules the detach of the dead mounts, the 1s backoff
        # and the exec of the new daemon — nothing else is in the interval.
        exit_at_ms="$(wait_for_daemon_exit 30)" ||
            fail "attempt $attempt: the daemon was still running 30s after the abort"
        wait_for_log "$ENTRY2_LOG" "$recovery_line" 30 ||
            fail "attempt $attempt: the entrypoint logged no '$recovery_line'"
        wait_for_log_count "$ENTRY2_LOG" "$((attempt + 1))" "available at $ENTRY_MNT" 60 ||
            fail "attempt $attempt: the recovered mount was never republished"
        restart_pid="$(entry_daemon_pid)" ||
            fail "attempt $attempt: the entrypoint is not running a torrentfs process after the recovery"
        restart_at_ms="$(proc_started_uptime_ms "$restart_pid")" ||
            fail "attempt $attempt: cannot read the start time of pid $restart_pid"
        backoff_ms=$((restart_at_ms - exit_at_ms))
        echo "[TC-recovery-05] recovery $attempt/3: daemon restarted ${backoff_ms}ms after the previous daemon exited"
        [ "$backoff_ms" -ge "$BACKOFF_MIN_MS" ] ||
            fail "attempt $attempt: restart came ${backoff_ms}ms after the previous daemon exited; C-10.7 requires a 1s backoff"
        [ "$backoff_ms" -le "$BACKOFF_MAX_MS" ] ||
            fail "attempt $attempt: restart came ${backoff_ms}ms after the previous daemon exited; C-10.7 requires a 1s backoff"
        kill -0 "$ENTRY_PID" 2>/dev/null || fail "attempt $attempt: the entrypoint exited instead of recovering"
    fi
done
# C2 tail: the fourth consecutive loss exhausts the budget.
wait_pid "$ENTRY_PID" 60 || fail "the entrypoint still ran after the fourth consecutive 104"
ENTRY_PID=""
[ "$WAIT_STATUS" -eq 104 ] || fail "after 3 recoveries a fourth 104 must make the entrypoint exit 104, got $WAIT_STATUS"
restarts="$(grep -cF "restarting torrentfs" "$ENTRY2_LOG" || true)"
[ "$restarts" -eq 3 ] || fail "the entrypoint restarted $restarts times, the budget is 3"
echo "[TC-recovery-05] 3 recoveries then exit 104 on the fourth severed session"

echo "TC-recovery-05: PASS (102 final on external unmount; 104 recovery budget, 1s backoff, exit 104 on budget exhaustion)"

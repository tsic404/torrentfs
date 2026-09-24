#!/usr/bin/env bash
# End-to-end regression for severed-FUSE-session recovery.
#
# A FUSE connection can be severed underneath a live mount (aborted connection,
# forced unmount): the kernel fails every in-flight request with ECONNABORTED
# and every later one with ENOTCONN, but it does not detach the mount.  The
# daemon used to report that loss with the same status as an intentional
# `fusermount -u`, so the container died and the mount stayed dead until a
# manual `docker start`.
#
# This harness severs the connection of a real, entrypoint-managed mount and
# asserts the container now recovers by itself: the daemon exits with the
# recoverable status, the entrypoint restarts it, and the published mount plus
# its content reads work again — with the entrypoint (container PID 1) never
# exiting.
#
# Requires root: severing a connection means writing to
# /sys/fs/fuse/connections/<minor>/abort, which only exists while fusectl is
# mounted on that directory.  The harness mounts it when absent (a container
# starts without it), which needs CAP_SYS_ADMIN.  The rootful entrypoint path is
# exercised, so the published bind mount is restored too.
#
# The daemon user (the entrypoint drops to TORRENTFS_UID, default 1000) must
# resolve through NSS or fusermount3 refuses to mount with "could not determine
# username"; the image creates that user in its Dockerfile, and the CI job that
# runs this harness creates the same one.
#
# Usage: sudo ./ci/tests/session_recovery_e2e.sh [torrentfs_binary]
#   env: SELFSEED_OUT (default <work>/selfseed), PAYLOAD_MIB (default 4)
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
PAYLOAD_MIB="${PAYLOAD_MIB:-4}"
INTERNAL_MNT="/mnt-inner" # the entrypoint's two-stage publish target

if [ "$(id -u)" -ne 0 ]; then
    echo "session_recovery_e2e: must run as root (severing a FUSE connection writes to sysfs)" >&2
    echo "  usage: sudo $0 [torrentfs_binary]" >&2
    exit 1
fi
command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 || {
    echo "session_recovery_e2e: fusermount not found; need a FUSE-capable environment" >&2
    exit 1
}
[ -c /dev/fuse ] || { echo "session_recovery_e2e: /dev/fuse is missing" >&2; exit 1; }
[ -x "$BIN" ] || { echo "session_recovery_e2e: torrentfs binary not found at $BIN" >&2; exit 1; }

# /sys/fs/fuse/connections is plain (empty) sysfs until fusectl is mounted on
# it, so without this the per-connection abort file below cannot be written.
if ! mountpoint -q /sys/fs/fuse/connections; then
    mount -t fusectl fusectl /sys/fs/fuse/connections 2>/dev/null || {
        echo "session_recovery_e2e: cannot mount fusectl on /sys/fs/fuse/connections (needs CAP_SYS_ADMIN)" >&2
        exit 1
    }
fi

WORK="$(mktemp -d /tmp/torrentfs-session-recovery.XXXXXX)"
# The rootful entrypoint drops the daemon to $TORRENTFS_UID (1000), so it must
# be able to traverse the work tree to reach the `torrentfs` on PATH.
chmod 0755 "$WORK"
MNT="$WORK/mnt"
SELFSEED_OUT="${SELFSEED_OUT:-$WORK/selfseed}"
LOG="$WORK/entrypoint.log"
ENTRYPOINT_PID=""
SEEDER_PID=""
READER_PID=""

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
    if [ -n "$ENTRYPOINT_PID" ]; then
        kill "$ENTRYPOINT_PID" 2>/dev/null || true
        wait "$ENTRYPOINT_PID" 2>/dev/null || true
    fi
    if [ -n "$SEEDER_PID" ]; then
        kill_tree "$SEEDER_PID"
        wait "$SEEDER_PID" 2>/dev/null || true
    fi
    if [ -n "$READER_PID" ]; then
        kill_tree "$READER_PID"
    fi
    fusermount3 -u -z "$MNT" >/dev/null 2>&1 || fusermount -u -z "$MNT" >/dev/null 2>&1 || true
    fusermount3 -u -z "$INTERNAL_MNT" >/dev/null 2>&1 || fusermount -u -z "$INTERNAL_MNT" >/dev/null 2>&1 || true
    umount "$MNT" 2>/dev/null || true
    rm -rf "$WORK" "$SELFSEED_OUT"
    # The entrypoint leaves its mountpoint lock file behind in the internal
    # mountpoint, so rmdir alone would not reclaim it.
    rm -f "$INTERNAL_MNT/.torrentfs.lock"
    rmdir "$INTERNAL_MNT" 2>/dev/null || true
}
trap cleanup EXIT

fail() {
    echo "session_recovery_e2e: FAIL — $1" >&2
    echo "  --- entrypoint log (tail) ---" >&2
    tail -40 "$LOG" >&2 2>/dev/null || true
    exit 1
}

# Print the FUSE connection minor (the /sys/fs/fuse/connections id) of the FUSE
# mount at $1, or nothing when the path carries no FUSE mount.
fuse_minor() {
    awk -v mp="$1" '
        $5 == mp {
            for (i = 7; i < NF; i++) {
                if ($i == "-" && $(i + 1) ~ /^fuse/) {
                    split($3, a, ":")
                    print a[2]
                    exit
                }
            }
        }
    ' /proc/self/mountinfo
}

wait_for_fuse_mount() {
    local target="$1" deadline=$((SECONDS + 30))
    while [ "$SECONDS" -lt "$deadline" ]; do
        [ -n "$(fuse_minor "$target")" ] && return 0
        sleep 0.5
    done
    return 1
}

wait_for_data_file() {
    local deadline=$((SECONDS + 60)) found=""
    while [ "$SECONDS" -lt "$deadline" ]; do
        found="$(find "$MNT/data" -type f ! -name '.stats' 2>/dev/null | head -n1 || true)"
        [ -n "$found" ] && { printf '%s\n' "$found"; return 0; }
        sleep 1
    done
    return 1
}

echo "[session_recovery_e2e] building seeder (payload=${PAYLOAD_MIB} MiB)…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --payload-mib "$PAYLOAD_MIB" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SELFSEED_OUT" >/dev/null 2>&1 &
SEEDER_PID=$!

for _ in $(seq 1 120); do
    [ -f "$SELFSEED_OUT/selfseed.torrent" ] && break
    sleep 1
done
[ -f "$SELFSEED_OUT/selfseed.torrent" ] || fail "seeder did not produce a torrent"
EXPECTED_MD5="$(md5sum "$SELFSEED_OUT/payload.txt" | awk '{print $1}')"

# The entrypoint launches `torrentfs` from PATH.
mkdir -p "$WORK/bin" "$MNT" "$WORK/cache" "$WORK/db"
ln -sf "$BIN" "$WORK/bin/torrentfs"

echo "[session_recovery_e2e] starting entrypoint (rootful two-stage publish)…"
PATH="$WORK/bin:$PATH" "$ROOT_DIR/entrypoint.sh" "$MNT" \
    --cache "$WORK/cache" --db "$WORK/db/metadata.db" >"$LOG" 2>&1 &
ENTRYPOINT_PID=$!

wait_for_fuse_mount "$MNT" || fail "FUSE mount at $MNT did not appear"
cp "$SELFSEED_OUT/selfseed.torrent" "$MNT/metadata/"
DATA_FILE="$(wait_for_data_file)" || fail "data file did not appear under $MNT/data"

BEFORE_MD5="$(md5sum "$DATA_FILE" | awk '{print $1}')"
[ "$BEFORE_MD5" = "$EXPECTED_MD5" ] || fail "content read before the abort was wrong ($BEFORE_MD5 != $EXPECTED_MD5)"
echo "[session_recovery_e2e] content read OK (md5=$BEFORE_MD5)"

# Hold the published path busy across the recovery, so a plain `umount` of the
# publish bind fails with EBUSY: a process whose working directory lives in the
# mount (a reader `cd`'d into data/, the sustained-read condition behind this
# bug) blocks a non-lazy unmount.  The open descriptor covers the fd case too.
(cd "$MNT" && exec 3<"$DATA_FILE" && sleep 120) &
READER_PID=$!
sleep 1
# Guard: without a busy mount the detach policy below is not exercised at all,
# so a silently-broken reader must fail the harness instead of passing it.
if umount "$MNT" 2>/dev/null; then
    fail "reader did not hold $MNT busy (a plain umount succeeded) — detach policy untested"
fi

MINOR="$(fuse_minor "$INTERNAL_MNT")"
[ -n "$MINOR" ] || fail "no FUSE connection found at $INTERNAL_MNT"
ABORT_FILE="/sys/fs/fuse/connections/$MINOR/abort"
echo "[session_recovery_e2e] severing the FUSE connection (minor=$MINOR)…"
# A bare redirect would end the harness under `set -e` with no diagnosis.
echo 1 >"$ABORT_FILE" 2>/dev/null || fail "cannot abort connection $MINOR: $ABORT_FILE is not writable (fusectl sees: $(ls /sys/fs/fuse/connections 2>/dev/null | tr '\n' ' '))"

# The daemon must report the loss as recoverable and the entrypoint must
# restart it — the old behavior exited the container instead.
RECOVERED=0
for _ in $(seq 1 60); do
    if grep -q "FUSE session severed (exit 104)" "$LOG" 2>/dev/null; then
        RECOVERED=1
        break
    fi
    sleep 1
done
[ "$RECOVERED" -eq 1 ] || fail "entrypoint did not restart the daemon after the severed session"

kill -0 "$ENTRYPOINT_PID" 2>/dev/null || fail "entrypoint exited instead of recovering"
echo "[session_recovery_e2e] daemon restarted; entrypoint still alive"

# The entrypoint logs one "available at" line per mount attempt, so the second
# one marks the recovered publish as complete.  Waiting for a FUSE entry instead
# would be satisfied by the dead mount, and the assertions below would then
# observe the pre-recovery state.
PUBLISHED=0
for _ in $(seq 1 60); do
    if [ "$(grep -c "available at $MNT" "$LOG" 2>/dev/null || true)" -ge 2 ]; then
        PUBLISHED=1
        break
    fi
    sleep 1
done
[ "$PUBLISHED" -eq 1 ] || fail "the recovered mount was never published"

# A recovery that stacked the new mount on the dead publish bind would leave two
# fuse entries at the published path, with the dead one surfacing again after a
# later unmount (and reachable on the host via rshared).
PUBLISHED_MOUNTS="$(awk -v mp="$MNT" '
    $5 == mp {
        for (i = 7; i < NF; i++) {
            if ($i == "-" && $(i + 1) ~ /^fuse/) { n++; break }
        }
    }
    END { print n + 0 }
' /proc/self/mountinfo)"
[ "$PUBLISHED_MOUNTS" -eq 1 ] || fail "expected exactly 1 published FUSE mount at $MNT after recovery, found $PUBLISHED_MOUNTS (stacked recovery)"
DATA_FILE="$(wait_for_data_file)" || fail "data file did not come back after recovery"
AFTER_MD5="$(md5sum "$DATA_FILE" | awk '{print $1}')"
[ "$AFTER_MD5" = "$EXPECTED_MD5" ] || fail "content read after recovery was wrong ($AFTER_MD5 != $EXPECTED_MD5)"

kill -0 "$ENTRYPOINT_PID" 2>/dev/null || fail "entrypoint exited after recovery"
echo "session_recovery_e2e: PASS (severed session recovered, content md5=$AFTER_MD5, entrypoint still running)"

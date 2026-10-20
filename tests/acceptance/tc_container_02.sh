#!/usr/bin/env bash
# Design acceptance case TC-container-02: a `.torrent` written on the host
# through the shared mount appears in `data/` in real time; host-side reads
# return the exact bytes; host-side writes / deletes / mkdir are rejected with
# a read-only errno (`docs/03-acceptance.md` §H AC-71).
#
# Container-level executable form.  Runs the production image (entrypoint +
# rshared mount) in a rootful FUSE-capable container with host networking, so
# the local tracker/seeder started here is reachable at loopback.  The
# container mountpoint is `/mnt` — the container-side path of the rshared bind
# — while `$HOST_DIR` is the host source directory.
#
# Requires: docker + /dev/fuse, the release binary and the self-seed example
# (`cargo build --release --example torrentfs-selfseed-env`).  Self-gating:
# SKIP (exit 2) when the environment cannot provide docker + /dev/fuse + a
# successful image build.
#
# The host directory must live on a shared mount for the FUSE mount to
# propagate to the host (see README "bind-propagation=rshared"); when the
# caller passes its own directory it is responsible for that.  A script-owned
# temp directory is made shared on a best-effort basis when running as root
# and is unmounted and removed on exit; a caller-supplied directory is never
# modified.
#
# Usage: ./tests/acceptance/tc_container_02.sh [image] [host-dir]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded (docker subcommands, host-side reads/writes
# through the propagated FUSE mount, mount/umount helpers).  A hung call fails
# the case with a diagnostic instead of stalling the job.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
# Wall-clock bound for one host-side read through the propagated FUSE mount.
HOST_IO_TIMEOUT=5

command -v docker >/dev/null 2>&1 || { echo "TC-container-02: SKIP — no docker" >&2; exit 2; }
[ -e /dev/fuse ] || { echo "TC-container-02: SKIP — no /dev/fuse" >&2; exit 2; }

# Every external call is bounded: a wedged daemon or a hung FUSE mount fails
# the case instead of stalling the job until the workflow timeout.
rc=0
IMAGE_ID="$(timeout 30 docker images -q "$IMAGE" 2>/dev/null)" || rc=$?
if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
    echo "TC-container-02: FAIL — docker images hung (rc=$rc)" >&2
    exit 1
fi
[ "$rc" -eq 0 ] || {
    echo "TC-container-02: FAIL — docker images failed (rc=$rc)" >&2
    exit 1
}

if [ -z "$IMAGE_ID" ]; then
    echo "TC-container-02: building image $IMAGE…"
    rc=0
    timeout 900 docker build -q -t "$IMAGE" "$ROOT_DIR" >/dev/null || rc=$?
    if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
        echo "TC-container-02: FAIL — docker build hung (rc=$rc)" >&2
        exit 1
    fi
    [ "$rc" -eq 0 ] || {
        echo "TC-container-02: SKIP — docker build failed (rc=$rc)" >&2
        exit 2
    }
fi

if [ -n "${2:-}" ]; then
    HOST_DIR="$2"
    OWN_HOST_DIR=0
else
    HOST_DIR="$(mktemp -d)"
    OWN_HOST_DIR=1
fi
HOST_DIR_SHARED=0
SEED_OUT="$(mktemp -d)"

CONTAINER="tc-container-02-$$"
SEEDER_PID=""
RUN_DIR="$HOST_DIR/run.$$.${RANDOM}"
MOUNT_HOST="$RUN_DIR/mnt"
PROBE_OUT="$RUN_DIR/probe.out"
timeout 10 mkdir -p "$MOUNT_HOST"

# Only a script-owned temp directory is shared: the container bind is
# `bind-propagation=rshared`, which a private host parent would swallow.
if [ "$OWN_HOST_DIR" -eq 1 ] && [ "$(id -u)" -eq 0 ]; then
    if timeout 10 mount --bind "$HOST_DIR" "$HOST_DIR" 2>/dev/null; then
        HOST_DIR_SHARED=1
        timeout 10 mount --make-shared "$HOST_DIR" 2>/dev/null || true
    fi
fi

cleanup() {
    # Stop gracefully first: the entrypoint's trap unmounts the internal FUSE
    # mount and releases the publish bind, which also drops the host-side
    # propagated mount (a hard kill leaves a stale ENOTCONN mount).
    if [ -n "$(timeout 5 docker ps -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]; then
        timeout 30 docker stop -t 20 "$CONTAINER" >/dev/null 2>&1 || true
    fi
    timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [ -n "$SEEDER_PID" ]; then
        for child in $(pgrep -P "$SEEDER_PID" 2>/dev/null || true); do
            kill "$child" 2>/dev/null || true
        done
        kill "$SEEDER_PID" 2>/dev/null || true
    fi
    for _ in $(seq 1 10); do
        # mountinfo lists stale (ENOTCONN) mounts too, unlike `mountpoint`.
        grep -q " $MOUNT_HOST " /proc/self/mountinfo 2>/dev/null || break
        timeout 10 fusermount3 -u -z "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 10 fusermount -u -z "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 10 umount -l "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 15 sudo -n umount -l "$MOUNT_HOST" >/dev/null 2>&1 \
            || true
        sleep 1
    done
    timeout 30 rm -rf "$RUN_DIR" "$SEED_OUT" >/dev/null 2>&1 || true

    # Release and remove only a script-owned host directory; a caller-supplied
    # directory is left untouched.
    if [ "$OWN_HOST_DIR" -eq 1 ]; then
        if [ "$HOST_DIR_SHARED" -eq 1 ]; then
            timeout 15 umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || timeout 20 sudo -n umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || true
        fi
        timeout 30 rm -rf "$HOST_DIR" >/dev/null 2>&1 || true
    fi
}

# Every docker call is bounded so a wedged daemon fails the case instead of
# hanging the job until the workflow timeout.
container_running() {
    [ "$(timeout 5 docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null)" = "true" ]
}

# A host-side timeout means the propagated FUSE mount is not serviced: fail at
# once with diagnostics instead of treating it as "not ready yet".
host_io_failed() {
    local op="$1" code="$2"
    if [ "$code" -eq 124 ] || [ "$code" -ge 128 ]; then
        echo "TC-container-02: FAIL — host-side FUSE $op hung for ${HOST_IO_TIMEOUT}s (mount not serviced)" >&2
    else
        echo "TC-container-02: FAIL — host-side FUSE $op failed (rc=$code)" >&2
    fi
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

# 0 = condition holds, 1 = does not; a timeout or any other error is fatal.
host_test() {
    local code=0
    timeout -k 2 "$HOST_IO_TIMEOUT" test "$@" 2>/dev/null || code=$?
    case "$code" in
        0|1) return "$code" ;;
    esac
    host_io_failed "test $*" "$code"
}

# Bounded host-side command that must succeed.
host_run() {
    local limit="$1" op="$2" code=0
    shift 2
    timeout -k 5 "$limit" "$@" || code=$?
    [ "$code" -eq 0 ] || host_io_failed "$op" "$code"
}

# Bounded host-side command: output goes to a file (never a pipe, so a killed
# child cannot keep the caller waiting) and a timeout exits with diagnostics.
probe() {
    local limit="$1" op="$2" out="$3" code=0
    shift 3
    timeout -k 5 "$limit" "$@" >"$out" 2>&1 || code=$?
    if [ "$code" -eq 124 ] || [ "$code" -ge 128 ]; then
        host_io_failed "$op" "$code"
    fi
    return "$code"
}

trap cleanup EXIT

# Local tracker + seeder (loopback); the container joins the host network so
# the announce URL resolves inside it.
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SEED_OUT" >/dev/null 2>&1 &
SEEDER_PID=$!
DEADLINE=$((SECONDS + 180))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -f "$SEED_OUT/selfseed.torrent" ] && break
    kill -0 "$SEEDER_PID" 2>/dev/null || {
        echo "TC-container-02: FAIL — self-seed environment exited before producing a torrent" >&2
        exit 1
    }
    sleep 1
done
[ -f "$SEED_OUT/selfseed.torrent" ] || {
    echo "TC-container-02: FAIL — self-seed environment did not produce a torrent within 180s" >&2
    exit 1
}

timeout 120 docker run -d --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN --network host \
    --security-opt apparmor=unconfined \
    --mount "type=bind,source=$MOUNT_HOST,target=/mnt,bind-propagation=rshared" \
    "$IMAGE" /mnt >/dev/null || {
    echo "TC-container-02: FAIL — docker run failed or did not return within 120s" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    container_running && break
    sleep 1
done
container_running || {
    echo "TC-container-02: FAIL — container did not stay running" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

# Wait until the entrypoint published the FUSE mount on both the container
# path (/mnt) and the host-side shared directory before writing the torrent
# through the host side — the two-stage rootful mount publishes after the
# daemon is up, so an immediate check races it on a loaded runner.
INNER=""
READY=0
DEADLINE=$((SECONDS + 120))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if ! container_running; then
        echo "TC-container-02: FAIL — container exited before publishing the mount" >&2
        { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
        exit 1
    fi
    INNER="$(timeout 5 docker exec "$CONTAINER" ls /mnt 2>/dev/null || true)"
    if grep -qx "data" <<<"$INNER" && grep -qx "metadata" <<<"$INNER" \
        && host_test -d "$MOUNT_HOST/metadata" && host_test -d "$MOUNT_HOST/data"; then
        READY=1
        break
    fi
    sleep 1
done
[ "$READY" -eq 1 ] || {
    echo "TC-container-02: FAIL — mount not visible on both sides within the 120s deadline" >&2
    echo "  container /mnt: [$INNER]" >&2
    echo "  host metadata dir exists: $(timeout -k 2 "$HOST_IO_TIMEOUT" test -d "$MOUNT_HOST/metadata" && echo yes || echo no)" >&2
    echo "  (host visibility needs the host directory on a shared mount:" >&2
    echo "   mount --bind <dir> <dir> && mount --make-shared <dir>)" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

# Host-side write: copy the torrent into the host-visible metadata/.
host_run 60 "cp $MOUNT_HOST/metadata/selfseed.torrent" \
    cp "$SEED_OUT/selfseed.torrent" "$MOUNT_HOST/metadata/selfseed.torrent"

# The data/ projection must appear through the host side in real time.
PROJECTION="$MOUNT_HOST/data/selfseed.torrent/selfseed"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if host_test -e "$PROJECTION"; then break; fi
    sleep 1
done
host_test -e "$PROJECTION" || {
    echo "TC-container-02: FAIL — host-side projection did not appear within 60s" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

# Host-side read: bytes must equal the authoritative payload, and the stat
# size must match it.
AUTHORITATIVE_SIZE="$(stat -c '%s' "$SEED_OUT/payload.txt")"
rc=0
probe "$HOST_IO_TIMEOUT" "stat $PROJECTION" "$PROBE_OUT" stat -c '%s' "$PROJECTION" || rc=$?
[ "$rc" -eq 0 ] || host_io_failed "stat $PROJECTION" "$rc"
SIZE="$(<"$PROBE_OUT")"
[ "$SIZE" = "$AUTHORITATIVE_SIZE" ] || {
    echo "TC-container-02: FAIL — host-side stat size=$SIZE, expected $AUTHORITATIVE_SIZE" >&2
    exit 1
}
rc=0
timeout -k 5 120 cmp -s "$SEED_OUT/payload.txt" "$PROJECTION" || rc=$?
if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
    host_io_failed "cmp $PROJECTION" "$rc"
fi
[ "$rc" -eq 0 ] || {
    echo "TC-container-02: FAIL — host-side read bytes differ from the authoritative payload" >&2
    exit 1
}

# Host-side mutations must be rejected with a read-only errno (EROFS) — the
# shell surfaces it as "Read-only file system" — and leave the projection
# untouched.
WRITE_RC=0
probe 30 "write $MOUNT_HOST/data/forbidden" "$PROBE_OUT" \
    bash -c 'echo x > "$1/data/forbidden"' _ "$MOUNT_HOST" || WRITE_RC=$?
WRITE_MSG="$(<"$PROBE_OUT")"
[ "$WRITE_RC" -ne 0 ] || {
    echo "TC-container-02: FAIL — host-side write into data/ succeeded" >&2
    exit 1
}
grep -qi "read-only file system" <<<"$WRITE_MSG" || {
    echo "TC-container-02: FAIL — host-side write must fail with a read-only errno (got: $WRITE_MSG)" >&2
    exit 1
}

MKDIR_RC=0
probe 30 "mkdir $MOUNT_HOST/data/forbidden_dir" "$PROBE_OUT" \
    bash -c 'mkdir "$1/data/forbidden_dir"' _ "$MOUNT_HOST" || MKDIR_RC=$?
MKDIR_MSG="$(<"$PROBE_OUT")"
[ "$MKDIR_RC" -ne 0 ] || {
    echo "TC-container-02: FAIL — host-side mkdir under data/ succeeded" >&2
    exit 1
}
grep -qi "read-only file system" <<<"$MKDIR_MSG" || {
    echo "TC-container-02: FAIL — host-side mkdir must fail with a read-only errno (got: $MKDIR_MSG)" >&2
    exit 1
}

RM_RC=0
probe 30 "delete $PROJECTION" "$PROBE_OUT" \
    bash -c 'rm -f "$1/data/selfseed.torrent/selfseed"' _ "$MOUNT_HOST" || RM_RC=$?
RM_MSG="$(<"$PROBE_OUT")"
[ "$RM_RC" -ne 0 ] || {
    echo "TC-container-02: FAIL — host-side delete under data/ succeeded" >&2
    exit 1
}
grep -qi "read-only file system" <<<"$RM_MSG" || {
    echo "TC-container-02: FAIL — host-side delete must fail with a read-only errno (got: $RM_MSG)" >&2
    exit 1
}

# The projection is still intact after the rejected mutations.
host_test -e "$PROJECTION" || {
    echo "TC-container-02: FAIL — projection lost after rejected mutations" >&2
    exit 1
}

echo "TC-container-02: PASS (host write visible, host read byte-exact, host mutations EROFS)"

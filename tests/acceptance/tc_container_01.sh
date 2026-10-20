#!/usr/bin/env bash
# Design acceptance case TC-container-01: a container running torrentfs
# exposes `metadata/` and `data/` inside; under a rootful + rshared mount the
# host sees the same directories (`docs/03-acceptance.md` §H AC-70).
#
# Container-level executable form.  Runs the production image (built with
# `docker build` so the entrypoint's two-stage rshared mount is exercised) in
# a rootful FUSE-capable container.  The container mountpoint is `/mnt` — the
# container-side path of the rshared bind — while `$HOST_DIR` is the host
# source directory.  Self-gating: SKIP (exit 2) when the environment cannot
# provide docker + /dev/fuse + a successful image build.
#
# The host directory must live on a shared mount for the FUSE mount to
# propagate to the host (see README "bind-propagation=rshared"); when the
# caller passes its own directory it is responsible for that.  A script-owned
# temp directory is made shared on a best-effort basis when running as root
# and is unmounted and removed on exit; a caller-supplied directory is never
# modified.
#
# Usage: ./tests/acceptance/tc_container_01.sh [image] [host-dir]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded (docker subcommands, host-side reads through
# the propagated FUSE mount, mount/umount helpers).  A hung call fails the
# case with a diagnostic instead of stalling the job.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
# Wall-clock bound for one host-side read through the propagated FUSE mount.
HOST_IO_TIMEOUT=5

command -v docker >/dev/null 2>&1 || { echo "TC-container-01: SKIP — no docker" >&2; exit 2; }
[ -e /dev/fuse ] || { echo "TC-container-01: SKIP — no /dev/fuse" >&2; exit 2; }

# Every external call is bounded: a wedged daemon or a hung FUSE mount fails
# the case instead of stalling the job until the workflow timeout.
rc=0
IMAGE_ID="$(timeout 30 docker images -q "$IMAGE" 2>/dev/null)" || rc=$?
if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
    echo "TC-container-01: FAIL — docker images hung (rc=$rc)" >&2
    exit 1
fi
[ "$rc" -eq 0 ] || {
    echo "TC-container-01: FAIL — docker images failed (rc=$rc)" >&2
    exit 1
}

if [ -z "$IMAGE_ID" ]; then
    echo "TC-container-01: building image $IMAGE…"
    rc=0
    timeout 900 docker build -q -t "$IMAGE" "$ROOT_DIR" >/dev/null || rc=$?
    if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
        echo "TC-container-01: FAIL — docker build hung (rc=$rc)" >&2
        exit 1
    fi
    [ "$rc" -eq 0 ] || {
        echo "TC-container-01: SKIP — docker build failed (rc=$rc)" >&2
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

CONTAINER="tc-container-01-$$"
# Run-scoped scratch directory: the script only ever removes this one, never
# a caller-supplied host directory.
RUN_DIR="$HOST_DIR/run.$$.${RANDOM}"
MOUNT_HOST="$RUN_DIR/mnt"
HOST_SIDE_FILE="$RUN_DIR/host-side.out"
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
    # propagated mount.  A hard `rm -f` would leave a stale ENOTCONN mount on
    # the host that `rm -rf` can no longer remove.
    if [ -n "$(timeout 5 docker ps -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]; then
        timeout 30 docker stop -t 20 "$CONTAINER" >/dev/null 2>&1 || true
    fi
    timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true

    # Detach any residual host-side publish mount (stale mounts are ENOTCONN;
    # the runner user may need sudo for this).
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
    timeout 30 rm -rf "$RUN_DIR" >/dev/null 2>&1 || true

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

# A host-side read timeout means the propagated FUSE mount is not serviced:
# fail at once with diagnostics instead of treating it as "not ready yet".
host_io_failed() {
    local op="$1" code="$2"
    if [ "$code" -eq 124 ] || [ "$code" -ge 128 ]; then
        echo "TC-container-01: FAIL — host-side FUSE $op hung for ${HOST_IO_TIMEOUT}s (mount not serviced)" >&2
    else
        echo "TC-container-01: FAIL — host-side FUSE $op failed (rc=$code)" >&2
    fi
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
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

timeout 120 docker run -d --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    --mount "type=bind,source=$MOUNT_HOST,target=/mnt,bind-propagation=rshared" \
    "$IMAGE" /mnt >/dev/null || {
    echo "TC-container-01: FAIL — docker run failed or did not return within 120s" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    container_running && break
    sleep 1
done
container_running || {
    echo "TC-container-01: FAIL — container did not stay running" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

# The rootful entrypoint mounts FUSE on /mnt-inner first and then
# bind-publishes it at /mnt; on a loaded runner that publish lags container
# start, so poll (bounded) until both the container-facing mount and the
# host-side shared directory list the filesystem instead of checking once
# right after `docker run`.
INNER=""
HOST_SIDE=""
READY=0
DEADLINE=$((SECONDS + 120))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if ! container_running; then
        echo "TC-container-01: FAIL — container exited before publishing the mount" >&2
        { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
        exit 1
    fi
    INNER="$(timeout 5 docker exec "$CONTAINER" ls /mnt 2>/dev/null || true)"
    rc=0
    probe "$HOST_IO_TIMEOUT" "ls $MOUNT_HOST" "$HOST_SIDE_FILE" ls "$MOUNT_HOST" || rc=$?
    [ "$rc" -eq 0 ] || host_io_failed "ls $MOUNT_HOST" "$rc"
    HOST_SIDE="$(<"$HOST_SIDE_FILE")"
    if grep -qx "data" <<<"$INNER" && grep -qx "metadata" <<<"$INNER" \
        && grep -qx "data" <<<"$HOST_SIDE" && grep -qx "metadata" <<<"$HOST_SIDE"; then
        READY=1
        break
    fi
    sleep 1
done
[ "$READY" -eq 1 ] || {
    echo "TC-container-01: FAIL — mount not visible on both sides within the 120s deadline" >&2
    echo "  container /mnt: [$INNER]" >&2
    echo "  host dir:       [$HOST_SIDE]" >&2
    echo "  (host visibility needs the host directory on a shared mount:" >&2
    echo "   mount --bind <dir> <dir> && mount --make-shared <dir>)" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

echo "TC-container-01: PASS (container /mnt and host-side mount both list metadata/ and data/)"

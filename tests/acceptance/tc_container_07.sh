#!/usr/bin/env bash
# Design acceptance case TC-container-07: a SIGKILL of the container leaves the
# host-side propagated FUSE mount in the connection-cut (ENOTCONN) state and
# reproduces it; the documented host-side recovery step reclaims it so the
# container starts again; a normal stop leaves no host-side mount
# (`docs/03-acceptance.md` §H AC-76; `docs/02-contracts.md` C-10.10, C-10.12;
# `docs/adr/ADR-012-container-mount-propagation.md`).
#
# Container-level executable form.  Rootful Docker with a rshared bind of a host
# directory is required for the host to see the FUSE mount at all; when the
# host directory cannot be seen on the host side (no shared propagation, e.g.
# an unprivileged nested runner) the case SKIPs (exit 2) instead of pretending
# to judge residue.  When the environment also cannot run the documented
# host-side recovery (no root and no passwordless sudo), the case SKIPs after
# recording that the residue was reproducible — the step itself is a host root
# operation, not a product behaviour.
#
# Usage: ./tests/acceptance/tc_container_07.sh [image] [host-dir]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded; a hung FUSE path or docker subcommand fails
# the case with a diagnostic instead of stalling the job.
set -euo pipefail

CASE="TC-container-07"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
HOST_IO_TIMEOUT=5
CONTAINER="tc-container-07-$$"
OWN_HOST_DIR=0
HOST_DIR_SHARED=0
if [ -n "${2:-}" ]; then
    HOST_DIR="$2"
else
    HOST_DIR="$(mktemp -d)"
    OWN_HOST_DIR=1
fi
RUN_DIR="$HOST_DIR/run.$$.$RANDOM"
MOUNT_HOST="$RUN_DIR/mnt"

fail() {
    echo "$CASE: FAIL — $*" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

skip() {
    echo "$CASE: SKIP — $*" >&2
    exit 2
}

# Detach a host-side mount (the documented recovery: `sudo umount -l <dir>` /
# `sudo fusermount -uz <dir>`).  Bounded per attempt; returns non-zero when the
# mount is still present.
detach_host_mount() {
    local attempt
    for attempt in $(seq 1 10); do
        host_residue_present || return 0
        timeout 10 fusermount3 -u -z "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 10 fusermount -u -z "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 10 umount -l "$MOUNT_HOST" >/dev/null 2>&1 \
            || timeout 15 sudo -n umount -l "$MOUNT_HOST" >/dev/null 2>&1 \
            || true
        sleep 1
    done
    ! host_residue_present
}

cleanup() {
    if [ -n "$(timeout 5 docker ps -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]; then
        timeout 30 docker stop -t 30 "$CONTAINER" >/dev/null 2>&1 || true
    fi
    timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [ -n "$MOUNT_HOST" ]; then
        detach_host_mount 2>/dev/null || true
        timeout 30 rm -rf "$RUN_DIR" >/dev/null 2>&1 || true
    fi
    if [ "$OWN_HOST_DIR" -eq 1 ]; then
        if [ "$HOST_DIR_SHARED" -eq 1 ]; then
            timeout 15 umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || timeout 20 sudo -n umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || true
        fi
        timeout 30 rm -rf "$HOST_DIR" >/dev/null 2>&1 || true
    fi
    timeout 30 rm -rf "$WORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || skip "no docker"
[ -e /dev/fuse ] || skip "no /dev/fuse"

timeout 10 mkdir -p "$MOUNT_HOST"
# Only a script-owned temp directory is shared: the container bind is
# `bind-propagation=rshared`, which a private host parent would swallow.
if [ "$OWN_HOST_DIR" -eq 1 ] && [ "$(id -u)" -eq 0 ]; then
    if timeout 10 mount --bind "$HOST_DIR" "$HOST_DIR" 2>/dev/null; then
        HOST_DIR_SHARED=1
        timeout 10 mount --make-shared "$HOST_DIR" 2>/dev/null || true
    fi
fi

rc=0
IMAGE_ID="$(timeout 30 docker images -q "$IMAGE" 2>/dev/null)" || rc=$?
if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
    fail "docker images hung (rc=$rc)"
fi
[ "$rc" -eq 0 ] || fail "docker images failed (rc=$rc)"
if [ -z "$IMAGE_ID" ]; then
    echo "$CASE: building image $IMAGE…"
    rc=0
    timeout 900 docker build -q -t "$IMAGE" "$ROOT_DIR" >/dev/null || rc=$?
    if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
        fail "docker build hung (rc=$rc)"
    fi
    [ "$rc" -eq 0 ] || skip "docker build failed (rc=$rc)"
fi

container_running() {
    [ "$(timeout 5 docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

host_residue_present() {
    # mountinfo lists stale (ENOTCONN) mounts too, unlike `mountpoint`.
    grep -q " $MOUNT_HOST " /proc/self/mountinfo 2>/dev/null
}

host_residue_enotconn() {
    local err
    err="$(timeout -k 2 "$HOST_IO_TIMEOUT" stat -c '%f' "$MOUNT_HOST" 2>&1 >/dev/null || true)"
    case "$err" in
        *'Transport endpoint is not connected'*) return 0 ;;
    esac
    return 1
}

wait_for_internal_mount() {
    local limit="$1" deadline inner
    deadline=$((SECONDS + limit))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! container_running "$CONTAINER"; then
            return 1
        fi
        inner="$(timeout 5 docker exec "$CONTAINER" ls /mnt 2>/dev/null || true)"
        if grep -qx "data" <<<"$inner" && grep -qx "metadata" <<<"$inner"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# The host-side view requires both rshared propagation and a shared host
# directory; an environment without it cannot judge AC-76 at all.
wait_for_host_mount() {
    local limit="$1" deadline
    deadline=$((SECONDS + limit))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if timeout -k 2 "$HOST_IO_TIMEOUT" test -d "$MOUNT_HOST/metadata" 2>/dev/null \
            && timeout -k 2 "$HOST_IO_TIMEOUT" test -d "$MOUNT_HOST/data" 2>/dev/null; then
            return 0
        fi
        if ! container_running "$CONTAINER"; then
            return 1
        fi
        sleep 1
    done
    return 1
}

echo "$CASE: starting a rootful container with a rshared host bind…"
timeout 120 docker run -d --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    --mount "type=bind,source=$MOUNT_HOST,target=/mnt,bind-propagation=rshared" \
    "$IMAGE" /mnt >/dev/null || fail "docker run failed or did not return within 120s"
wait_for_internal_mount 120 || fail "the container-side FUSE mount did not become ready"
if ! wait_for_host_mount 60; then
    if container_running "$CONTAINER"; then
        skip "host-side rshared propagation is unavailable (the host directory never shows the mount); start from a shared host mount"
    fi
    fail "the container exited before publishing the mount"
fi
HOST_LIST="$(timeout -k 2 "$HOST_IO_TIMEOUT" ls "$MOUNT_HOST" 2>/dev/null || true)"
grep -qx "data" <<<"$HOST_LIST" || fail "the host-side mount does not list data/"
grep -qx "metadata" <<<"$HOST_LIST" || fail "the host-side mount does not list metadata/"

echo "$CASE: SIGKILL — the host-side mount must survive as ENOTCONN…"
timeout 30 docker kill -s KILL "$CONTAINER" >/dev/null || fail "docker kill -s KILL failed"
DEADLINE=$((SECONDS + 30))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    container_running "$CONTAINER" || break
    sleep 1
done
container_running "$CONTAINER" && fail "the container survived SIGKILL"

RESIDUE=0
DEADLINE=$((SECONDS + 20))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if host_residue_present && host_residue_enotconn; then
        RESIDUE=1
        break
    fi
    sleep 1
done
[ "$RESIDUE" -eq 1 ] || fail "SIGKILL did not leave a reproducible ENOTCONN mount on $MOUNT_HOST"

echo "$CASE: applying the documented host-side recovery (umount -l / fusermount -uz)…"
if ! detach_host_mount; then
    if [ "$(id -u)" -eq 0 ]; then
        fail "the documented host-side recovery step did not reclaim the stale mount"
    fi
    skip "the residue was reproducible but this environment cannot run the host root recovery step (no root, no passwordless sudo)"
fi
host_residue_present && fail "the stale host mount is still present after the recovery step"
timeout -k 2 "$HOST_IO_TIMEOUT" test -d "$MOUNT_HOST" \
    || fail "the host path is not usable after the recovery step"

echo "$CASE: restarting the container after recovery…"
timeout 120 docker start "$CONTAINER" >/dev/null || fail "docker start failed after the documented recovery"
wait_for_internal_mount 120 || fail "the container-side mount did not become ready after restart"
wait_for_host_mount 120 || fail "the host-side mount did not come back after restart"
HOST_LIST="$(timeout -k 2 "$HOST_IO_TIMEOUT" ls "$MOUNT_HOST" 2>/dev/null || true)"
grep -qx "data" <<<"$HOST_LIST" || fail "the restarted host-side mount does not list data/"
grep -qx "metadata" <<<"$HOST_LIST" || fail "the restarted host-side mount does not list metadata/"

echo "$CASE: normal stop — no host-side residue…"
timeout 60 docker stop -t 30 "$CONTAINER" >/dev/null || fail "docker stop failed or hung"
DEADLINE=$((SECONDS + 20))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    host_residue_present || break
    sleep 1
done
host_residue_present && fail "a normal stop left a mount on $MOUNT_HOST"
timeout -k 2 "$HOST_IO_TIMEOUT" test -d "$MOUNT_HOST" \
    || fail "the host path is not usable after a normal stop"

echo "$CASE: PASS (SIGKILL residue reproduced as ENOTCONN; documented host recovery allowed the restart; normal stop left no residue)"

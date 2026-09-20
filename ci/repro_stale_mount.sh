#!/usr/bin/env bash
# Reproduction harness for the stale host mount left by a SIGKILLed container.
#
# A container killed with SIGKILL (`docker kill -s KILL`, or an OOM kill) never
# runs torrentfs's shutdown unmount, so the FUSE mount it propagated to the host
# via rshared bind propagation survives the container and reports ENOTCONN
# ("Transport endpoint is not connected"). The next `docker start` then fails
# before the entrypoint can run — the engine cannot re-establish a bind mount
# whose source path is a dead FUSE mount. Recovery must happen on the host:
#
#     umount -l <host-mountpoint>       # or: fusermount -uz <host-mountpoint>
#
# then restart the container. The entrypoint's in-container recovery
# (`recover_stale_mountpoint`) only covers container-only mounts (rootless
# podman / non-root --user), where the stale mount lives inside the container
# and the engine can still start it.
#
# Usage: sudo ./ci/repro_stale_mount.sh [image] [engine]
# Requires: root, /dev/fuse, and docker or podman. Cleans up after itself.

set -euo pipefail

IMAGE="${1:-ghcr.io/tsic404/torrentfs:main}"
ENGINE="${2:-docker}"

case "$ENGINE" in
    docker|podman) ;;
    *) echo "engine must be 'docker' or 'podman' (got '$ENGINE')" >&2; exit 2 ;;
esac

if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root (sudo)" >&2
    exit 1
fi
if [ ! -c /dev/fuse ]; then
    echo "error: /dev/fuse not present on the host" >&2
    exit 1
fi
if ! command -v "$ENGINE" >/dev/null 2>&1; then
    echo "error: $ENGINE not found on PATH" >&2
    exit 1
fi

WORK="$(mktemp -d /tmp/torrentfs-stale-mount.XXXXXX)"
HOST_MNT="$WORK/mnt"
STATE="$WORK/state"
NAME="torrentfs-stale-mount-repro"
mkdir -p "$HOST_MNT" "$STATE"

# host_mount_stale: 0 when $HOST_MNT reports ENOTCONN (a dead FUSE mount), else 1.
host_mount_stale() {
    local err
    if stat -c '%f' "$HOST_MNT" >/dev/null 2>&1; then
        return 1
    fi
    err="$(stat -c '%f' "$HOST_MNT" 2>&1 >/dev/null || true)"
    case "$err" in
        *'Transport endpoint is not connected'*) return 0 ;;
        *) return 1 ;;
    esac
}

cleanup() {
    "$ENGINE" rm -f "$NAME" >/dev/null 2>&1 || true
    # Lazy-detach any mount still stacked on the work tree, then drop the tree.
    umount -l "$HOST_MNT" >/dev/null 2>&1 || true
    sleep 0.2
    umount -l "$HOST_MNT" >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

# The bind-mount source must itself be a mount before it can be made shared
# (mirrors ci/deploy_rootful.sh prepare_mountpoint).
if ! mountpoint -q "$HOST_MNT" 2>/dev/null; then
    mount --bind "$HOST_MNT" "$HOST_MNT"
fi
mount --make-shared "$HOST_MNT"

echo "[repro] starting $ENGINE container '$NAME' (image $IMAGE)" >&2
"$ENGINE" run -d \
    --name "$NAME" \
    --device /dev/fuse \
    --cap-add SYS_ADMIN \
    --mount "type=bind,source=$HOST_MNT,target=/mnt,bind-propagation=rshared" \
    --mount "type=bind,source=$STATE,target=/home/torrentfs/.local/share/torrentfs" \
    --stop-timeout 30 \
    "$IMAGE" /mnt >/dev/null

# Wait for the FUSE mount to propagate to the host.
ready=0
for _ in $(seq 1 60); do
    if ls "$HOST_MNT/metadata/" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.5
done
if [ "$ready" -ne 1 ]; then
    echo "FAIL: FUSE mount never became visible at $HOST_MNT" >&2
    exit 1
fi
echo "[repro] FUSE mount live at $HOST_MNT" >&2

# ── SIGKILL ──────────────────────────────────────────────────────────────────
echo "[repro] SIGKILLing the container" >&2
"$ENGINE" kill -s KILL "$NAME" >/dev/null
sleep 1

if ! host_mount_stale; then
    echo "FAIL: expected a stale ENOTCONN mount at $HOST_MNT after SIGKILL, found none" >&2
    echo "      (stat output: $(stat -c '%f' "$HOST_MNT" 2>&1 || true))" >&2
    exit 1
fi
echo "[repro] confirmed: $HOST_MNT reports ENOTCONN after SIGKILL" >&2

# The engine's restart path is observed, not asserted: it varies by engine and
# version. Docker fails here because it cannot re-bind-mount a dead source path.
echo "[repro] attempting '$ENGINE start' on the stale mount (may fail — that is the documented symptom)" >&2
if "$ENGINE" start "$NAME" >/dev/null 2>&1; then
    echo "[repro]   engine restarted the container without host-side recovery" >&2
    "$ENGINE" kill -s KILL "$NAME" >/dev/null 2>&1 || true
else
    echo "[repro]   engine refused to restart (stale bind source) — host-side recovery required" >&2
fi

# ── recovery ─────────────────────────────────────────────────────────────────
echo "[repro] host-side recovery: umount -l $HOST_MNT" >&2
umount -l "$HOST_MNT"
sleep 0.5

if host_mount_stale; then
    echo "FAIL: $HOST_MNT still reports ENOTCONN after umount -l" >&2
    exit 1
fi

echo "[repro] restarting the container after host-side recovery" >&2
"$ENGINE" start "$NAME" >/dev/null

ready=0
for _ in $(seq 1 60); do
    if ls "$HOST_MNT/metadata/" >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 0.5
done
if [ "$ready" -ne 1 ]; then
    echo "FAIL: FUSE mount did not return after restart" >&2
    exit 1
fi

echo "PASS: SIGKILL leaves a stale ENOTCONN host mount; host-side umount -l recovers and restart succeeds" >&2

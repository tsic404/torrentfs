#!/usr/bin/env bash
# One-click rootful container deployment for host-visible FUSE mounts.
#
# Rootless podman cannot propagate a container FUSE mount to the host; this
# script prepares the host mountpoint as a shared mount and starts torrentfs
# in a rootful engine (podman or docker) with rshared bind propagation. See
# `usage()` below for flags and defaults.

set -euo pipefail

ENGINE=""
MOUNTPOINT="/host/torrentfs"
STATE_DIR="/var/lib/torrentfs"
# The repo publishes per-arch tags (`main-amd64`, `main-arm64`) plus a merged
# multi-arch `main`; plain `latest` is only emitted for v-tag pushes and does
# not exist today. Pin an explicit pullable tag by default — override with
# `--image` (e.g. `--image ghcr.io/tsip404/torrentfs:main-amd64`).
IMAGE="ghcr.io/tsip404/torrentfs:main"
NAME="torrentfs"
DRY_RUN=0

usage() {
    cat <<'EOF'
One-click rootful container deployment for host-visible FUSE mounts.

Rootless podman cannot propagate a container FUSE mount to the host: shared
mount propagation (rshared) is unsupported inside user namespaces, so the
FUSE filesystem stays visible only inside the container (see the README
"Container Deployment" section). This script prepares a host mountpoint as a
shared mount and starts torrentfs in a rootful engine (podman or docker) with
rshared bind propagation, making the FUSE filesystem directly accessible on
the host at <mountpoint>.

Usage:
  sudo ./ci/deploy_rootful.sh [--engine podman|docker] [--mountpoint PATH]
                              [--state PATH] [--image IMAGE] [--name NAME]
                              [--dry-run]

Defaults:
  engine     first of podman/docker found on PATH (podman preferred)
  mountpoint /host/torrentfs (host-visible FUSE mount)
  state      /var/lib/torrentfs (persistent db + piece cache)
  image      ghcr.io/tsip404/torrentfs:main
  name       torrentfs
EOF
}

parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --engine|--mountpoint|--state|--image|--name)
                if [ $# -lt 2 ]; then
                    echo "error: $1 requires a value" >&2
                    usage >&2
                    return 2
                fi
                case "$1" in
                    --engine) ENGINE="$2" ;;
                    --mountpoint) MOUNTPOINT="$2" ;;
                    --state) STATE_DIR="$2" ;;
                    --image) IMAGE="$2" ;;
                    --name) NAME="$2" ;;
                esac
                shift 2
                ;;
            --dry-run) DRY_RUN=1; shift ;;
            -h|--help) usage; exit 0 ;;
            *) echo "unknown option: $1" >&2; usage >&2; return 2 ;;
        esac
    done
}

detect_engine() {
    if command -v podman >/dev/null 2>&1; then
        printf '%s\n' podman
    elif command -v docker >/dev/null 2>&1; then
        printf '%s\n' docker
    else
        return 1
    fi
}

# The bind-mount source must itself be a mount before it can be made shared;
# `mount --bind dir dir` turns a plain directory into one (idempotent: skipped
# when it is already a mount). `mount --make-shared` is likewise idempotent.
prepare_mountpoint() {
    mkdir -p "$MOUNTPOINT"
    if ! mountpoint -q "$MOUNTPOINT" 2>/dev/null; then
        mount --bind "$MOUNTPOINT" "$MOUNTPOINT"
    fi
    mount --make-shared "$MOUNTPOINT"
}

# Echo the exact command sequence a real run would execute, for --dry-run.
# Kept as a separate function so it can be tested without root or /dev/fuse.
print_dry_run() {
    echo "[deploy_rootful] dry run — no host mounts or container started"
    echo "[deploy_rootful] would run:"
    echo "  mkdir -p $MOUNTPOINT"
    echo "  mount --bind $MOUNTPOINT $MOUNTPOINT   # if not already a mount"
    echo "  mount --make-shared $MOUNTPOINT"
    echo "  mkdir -p $STATE_DIR"
    echo "  $ENGINE run -d --name $NAME \\"
    echo "    --device /dev/fuse \\"
    echo "    --cap-add SYS_ADMIN \\"
    echo "    --mount type=bind,source=$MOUNTPOINT,target=/mnt,bind-propagation=rshared \\"
    echo "    --mount type=bind,source=$STATE_DIR,target=/root/.local/share/torrentfs \\"
    echo "    --stop-timeout 30 \\"
    echo "    $IMAGE"
}

run_deploy() {
    prepare_mountpoint
    mkdir -p "$STATE_DIR"

    echo "[deploy_rootful] starting $ENGINE container '$NAME' (image $IMAGE)" >&2
    echo "[deploy_rootful] host-visible FUSE mount: $MOUNTPOINT" >&2
    echo "[deploy_rootful] persistent state: $STATE_DIR" >&2

    "$ENGINE" run -d \
        --name "$NAME" \
        --device /dev/fuse \
        --cap-add SYS_ADMIN \
        --mount "type=bind,source=$MOUNTPOINT,target=/mnt,bind-propagation=rshared" \
        --mount "type=bind,source=$STATE_DIR,target=/root/.local/share/torrentfs" \
        --stop-timeout 30 \
        "$IMAGE"

    echo "[deploy_rootful] verify with: ls $MOUNTPOINT/metadata/" >&2
}

# ── main ─────────────────────────────────────────────────────────────────────

parse_args "$@" || exit $?

# Rootful deployment requires root: the engine needs CAP_SYS_ADMIN for the
# FUSE mount and the host-side shared-mount setup calls `mount`.
if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root (sudo)" >&2
    exit 1
fi

if [ ! -c /dev/fuse ]; then
    echo "error: /dev/fuse not present on the host" >&2
    exit 1
fi

if [ -z "$ENGINE" ]; then
    ENGINE="$(detect_engine)" || {
        echo "error: neither podman nor docker found on PATH" >&2
        exit 1
    }
fi
if ! command -v "$ENGINE" >/dev/null 2>&1; then
    echo "error: $ENGINE not found on PATH" >&2
    exit 1
fi

if [ "$DRY_RUN" -eq 1 ]; then
    print_dry_run
    exit 0
fi

run_deploy

#!/bin/bash
# torrentfs container entrypoint — handles FUSE device setup and mount visibility.
#
#   1. Detects container + FUSE availability
#   2. Attempts to create /dev/fuse when running as root
#   3. Provides actionable diagnostics when FUSE is unavailable
#   4. Two-stage FUSE mount: mounts on internal path (/mnt-inner), then
#      bind-mounts to the container-facing path for host visibility via
#      shared mount propagation (rshared).
#      See: https://docs.docker.com/engine/storage/bind-mounts/#configure-bind-propagation
#   5. Rootless podman detection: shared propagation is unsupported in user
#      namespaces, so the two-stage bind mount is skipped and torrentfs mounts
#      directly on the container path. When the mountpoint is a bind mount
#      (e.g. -v /host:/mnt:shared), an explicit warning is emitted that the
#      ':shared' flag is ineffective and the host will not see the FUSE mount.

set -euo pipefail

# ── helpers ──────────────────────────────────────────────────────────────────

# Commands that do not require a FUSE mount — pass these straight through.
needs_fuse() {
    for arg in "$@"; do
        case "$arg" in
            --help|-h|help|--version|-V)
                return 1  # does NOT need FUSE
                ;;
        esac
    done
    return 0  # needs FUSE
}

# Config errors are fatal: torrentfs exits non-zero on a bad --config, and
# we surface that immediately instead of failing later at the FUSE mount
# stage with a misleading diagnostic.

# Validate one --config value via the CLI's --config-check. Exits non-zero
# (propagating torrentfs's own exit code) on an invalid or unreadable file.
# The `|| rc=$?` pattern keeps the failure reachable under `set -e`.
validate_config() {
    local config_path="$1" rc=0
    torrentfs --config-check --config "$config_path" || rc=$?
    if [ "$rc" -ne 0 ]; then
        echo "[entrypoint] ERROR: invalid config file '$config_path' (torrentfs exit code $rc)" >&2
        exit "$rc"
    fi
}

for arg in "$@"; do
    case "$arg" in
        --config)
            config_flag_next=1
            ;;
        --config=*)
            validate_config "${arg#--config=}"
            ;;
        *)
            if [ "${config_flag_next:-0}" -eq 1 ]; then
                validate_config "$arg"
                config_flag_next=0
            fi
            ;;
    esac
done

in_container() {
    # Heuristics: cgroup mount, /.dockerenv, /run/.containerenv (podman)
    grep -q ':/docker/' /proc/1/cgroup 2>/dev/null && return 0
    grep -q ':/libpod-' /proc/1/cgroup 2>/dev/null && return 0
    test -f /.dockerenv && return 0
    test -f /run/.containerenv && return 0
    return 1
}

is_root() {
    test "$(id -u)" -eq 0
}

is_rootless_podman() {
    # Rootless podman: container detected via podman signatures AND the
    # user namespace maps container UID 0 to a non-zero host UID.
    # (/proc/self/uid_map row 1 col 2 is the host-side UID; != 0 means
    # we are in a user namespace, i.e. rootless.)
    # In rootless podman, shared mount propagation (rshared) is not supported
    # because the user namespace lacks the necessary mount privileges.
    # Ref: https://lists.podman.io/archives/list/podman@lists.podman.io/thread/YOGMR5I2M4MLCMQGZGFUV3NOJYJKZZ2X/
    if in_container; then
        # Podman signatures: /run/.containerenv or libpod- cgroup
        if test -f /run/.containerenv || grep -q ':/libpod-' /proc/1/cgroup 2>/dev/null; then
            if awk 'NR==1 { exit !($2 != 0) }' /proc/self/uid_map 2>/dev/null; then
                return 0
            fi
        fi
    fi
    return 1
}

# Check whether $1 is a bind mount (root field != "/" in /proc/self/mountinfo).
# Used to detect `-v <host>:<container>:shared` style bind mounts so we can
# warn that shared propagation is ineffective under rootless podman.
is_bind_mount() {
    local target="$1"
    # Field 4 is the root within the source filesystem; "/" means it is the
    # root of that filesystem (not a subpath bind). Field 5 is the mount point.
    awk -v mp="$target" '$5 == mp && $4 != "/" { found=1 } END { exit !found }' \
        /proc/self/mountinfo 2>/dev/null
}

fuse_device_exists() {
    test -c /dev/fuse
}

ensure_fuse_device() {
    if fuse_device_exists; then
        return 0
    fi

    if is_root; then
        echo "[entrypoint] /dev/fuse missing — attempting to create device node" >&2
        if mknod /dev/fuse c 10 229 2>/dev/null; then
            echo "[entrypoint] /dev/fuse created successfully" >&2
            return 0
        fi
        echo "[entrypoint] mknod /dev/fuse failed" >&2
    fi

    return 1
}

# ── main ─────────────────────────────────────────────────────────────────────

echo "[entrypoint] torrentfs container startup" >&2

if in_container; then
    echo "[entrypoint] detected container environment" >&2
else
    echo "[entrypoint] bare-metal or VM environment" >&2
fi

if ! needs_fuse "$@"; then
    # Help, version, and other diagnostic commands don't need FUSE.
    echo "[entrypoint] skipping FUSE check for diagnostic command — starting torrentfs" >&2
    exec torrentfs "$@"
fi

if ! ensure_fuse_device; then
    cat >&2 <<'DIAG'
╔══════════════════════════════════════════════════════════════════════╗
║  FUSE kernel device (/dev/fuse) is not available in this container.  ║
║  torrentfs requires FUSE to mount the virtual filesystem.            ║
║                                                                      ║
║  Solutions:                                                          ║
║                                                                      ║
║  1. podman (recommended):                                            ║
║     podman run --device /dev/fuse --cap-add SYS_ADMIN ...            ║
║                                                                      ║
║  2. podman (alternative — full privileges):                          ║
║     podman run --privileged ...                                      ║
║                                                                      ║
║  3. docker:                                                          ║
║     docker run --device /dev/fuse --cap-add SYS_ADMIN ...            ║
║                                                                      ║
║  4. docker compose:                                                  ║
║     services:                                                        ║
║       torrentfs:                                                     ║
║         devices:                                                     ║
║           - /dev/fuse:/dev/fuse                                      ║
║         cap_add:                                                     ║
║           - SYS_ADMIN                                                ║
║                                                                      ║
║  Rootless podman caveat: shared mount propagation (rshared) is not   ║
║  supported in rootless podman. Even with /dev/fuse available, the    ║
║  FUSE mount will only be visible inside the container — the host     ║
║  cannot access it via a bind mount. Use rootful podman or Docker     ║
║  for host-visible FUSE mounts.                                       ║
║                                                                      ║
║  If you cannot grant these privileges, torrentfs cannot mount its    ║
║  FUSE filesystem inside a container. Run torrentfs on the host or    ║
║  in a privileged container instead.                                  ║
╚══════════════════════════════════════════════════════════════════════╝
DIAG
    exit 100
fi

# ── FUSE mount with host visibility ──────────────────────────────────────────
# Runs torrentfs on an internal mount point, then bind-mounts to the
# container-facing path (typically a shared bind mount). This allows
# the FUSE filesystem to propagate to the host via shared mount propagation.
# For the host to see the mount, the container must be started with:
#   --mount type=bind,source=<host-path>,target=<container-path>,bind-propagation=rshared
# and the host source directory must itself be a shared mount:
#   mount --bind <host-path> <host-path> && mount --make-shared <host-path>
#
# Rootless podman does not support shared mount propagation, so the
# two-stage bind mount is skipped: torrentfs mounts directly on $mountpoint
# and the FUSE filesystem is only visible inside the container.
start_torrentfs() {
    local mountpoint="$1"
    shift

    if is_rootless_podman; then
        start_torrentfs_rootless "$mountpoint" "$@"
    else
        start_torrentfs_rootful "$mountpoint" "$@"
    fi
}

# Rootless podman path: direct FUSE mount, no bind mount (propagation not supported).
start_torrentfs_rootless() {
    local mountpoint="$1"
    shift

    mkdir -p "$mountpoint"

    # If the mountpoint is a bind mount (e.g. -v /host:/mnt:shared), warn
    # explicitly that shared propagation cannot work under rootless podman —
    # the host will NOT see the FUSE filesystem even though the bind mount
    # itself is present.
    if is_bind_mount "$mountpoint"; then
        echo "[entrypoint] WARNING: $mountpoint is a bind mount, but rootless podman cannot propagate" >&2
        echo "[entrypoint]   FUSE mounts to the host — ':shared' / 'rshared' is ineffective here." >&2
        echo "[entrypoint]   The FUSE filesystem will only be visible inside the container." >&2
        echo "[entrypoint]   For host-visible mounts, use rootful podman (sudo podman) or Docker." >&2
    fi

    echo "[entrypoint] rootless podman detected — FUSE mount will only be visible inside the container" >&2
    echo "[entrypoint] starting torrentfs directly on $mountpoint" >&2

    local torrentfs_pid=""
    cleanup() {
        echo "[entrypoint] shutting down" >&2
        if [ -n "${torrentfs_pid:-}" ]; then
            kill "$torrentfs_pid" 2>/dev/null || true
            wait "$torrentfs_pid" 2>/dev/null || true
        fi
        umount "${mountpoint:-}" 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    torrentfs "$mountpoint" "$@" &
    torrentfs_pid=$!

    # Wait for FUSE mount to become ready (up to 30s)
    local ready=0
    for i in $(seq 1 60); do
        if mountpoint -q "$mountpoint" 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.5
    done

    if [ "$ready" -eq 0 ]; then
        echo "[entrypoint] FUSE mount did not become ready within 30s" >&2
        kill "$torrentfs_pid" 2>/dev/null || true
        wait "$torrentfs_pid" 2>/dev/null || true
        exit 1
    fi

    echo "[entrypoint] torrentfs running (pid=$torrentfs_pid), available at $mountpoint (container-only)" >&2

    local rc=0
    wait "$torrentfs_pid" || rc=$?
    exit "$rc"
}

# Rootful container (or Docker) path: two-stage bind mount for host visibility.
start_torrentfs_rootful() {
    local mountpoint="$1"
    shift
    local internal_mnt="/mnt-inner"

    mkdir -p "$internal_mnt"
    mkdir -p "$mountpoint"

    echo "[entrypoint] starting torrentfs on internal mount $internal_mnt" >&2

    local torrentfs_pid=""
    cleanup() {
        echo "[entrypoint] shutting down" >&2
        if [ -n "${torrentfs_pid:-}" ]; then
            kill "$torrentfs_pid" 2>/dev/null || true
            wait "$torrentfs_pid" 2>/dev/null || true
        fi
        umount "${mountpoint:-}" 2>/dev/null || true
    }
    trap cleanup EXIT INT TERM

    torrentfs "$internal_mnt" "$@" &
    torrentfs_pid=$!

    local ready=0
    for i in $(seq 1 60); do
        if mountpoint -q "$internal_mnt" 2>/dev/null; then
            ready=1
            break
        fi
        sleep 0.5
    done

    if [ "$ready" -eq 0 ]; then
        echo "[entrypoint] FUSE mount did not become ready within 30s" >&2
        kill "$torrentfs_pid" 2>/dev/null || true
        wait "$torrentfs_pid" 2>/dev/null || true
        exit 1
    fi

    echo "[entrypoint] FUSE mount ready — publishing to $mountpoint" >&2
    mount --bind "$internal_mnt" "$mountpoint"

    echo "[entrypoint] torrentfs running (pid=$torrentfs_pid), available at $mountpoint" >&2

    local rc=0
    wait "$torrentfs_pid" || rc=$?
    exit "$rc"
}

echo "[entrypoint] /dev/fuse is available" >&2
start_torrentfs "$@"

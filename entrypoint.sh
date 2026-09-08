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
#   6. Concurrent-mountpoint conflict handling: two containers sharing one
#      host directory via rshared propagation would stack FUSE mounts and
#      sever each other's mount (ENOTCONN). The entrypoint takes an exclusive
#      flock on the mountpoint directory (same inode across containers) held
#      for the container lifetime, then refuses to start if $mountpoint
#      already carries a foreign FUSE mount.

set -euo pipefail

# ── helpers ──────────────────────────────────────────────────────────────────

# Commands that do not require a FUSE mount — pass these straight through.
needs_fuse() {
    for arg in "$@"; do
        case "$arg" in
            --help|-h|help|--version|-V|--config-check)
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

# ── argument parsing ──────────────────────────────────────────────────────
# torrentfs's CLI is `torrentfs [OPTIONS] <mountpoint>` (clap): options may
# precede or follow the single positional mountpoint. The entrypoint needs the
# mountpoint separately (to mkdir/recover/probe it before mounting), so we parse
# the command line once here — validating any --config files and separating the
# mountpoint from the arguments forwarded to torrentfs. This makes
# `--config <path> /mnt` and `/mnt --config <path>` equivalent.

# First positional argument (the mountpoint); empty when none was given.
mountpoint=""
# Every argument except the mountpoint, forwarded to torrentfs in order.
torrentfs_args=()

parse_args() {
    local arg expect_value="" options_ended=0
    mountpoint=""
    torrentfs_args=()

    for arg in "$@"; do
        if [ "$options_ended" -eq 1 ]; then
            # After `--`, every remaining argument is positional.
            if [ -z "$mountpoint" ]; then
                mountpoint="$arg"
            else
                torrentfs_args+=("$arg")
            fi
            continue
        fi

        if [ -n "$expect_value" ]; then
            # Value of the preceding --config/--db/--cache option.
            if [ "$expect_value" = "--config" ]; then
                validate_config "$arg"
            fi
            torrentfs_args+=("$arg")
            expect_value=""
            continue
        fi

        case "$arg" in
            --)
                # End of options: everything after this is positional.
                options_ended=1
                ;;
            --config=*)
                validate_config "${arg#--config=}"
                torrentfs_args+=("$arg")
                ;;
            --config|--db|--cache)
                torrentfs_args+=("$arg")
                expect_value="$arg"
                ;;
            --db=*|--cache=*)
                torrentfs_args+=("$arg")
                ;;
            -*)
                # Any other option is forwarded verbatim. Flags that bypass
                # the FUSE mount (--help, --version, --config-check) are
                # handled by needs_fuse before this forwarding is consumed.
                torrentfs_args+=("$arg")
                ;;
            *)
                if [ -z "$mountpoint" ]; then
                    mountpoint="$arg"
                else
                    torrentfs_args+=("$arg")
                fi
                ;;
        esac
    done
}

parse_args "$@"

# Reject a missing or unusable mountpoint with an actionable diagnostic (exit 2,
# matching clap's usage-error code) before the FUSE device check runs, so the
# specific error is not masked by the FUSE diagnostic (exit 100). Also refuses a
# `-`-prefixed path: the downstream mkdir/stat/umount/mount commands — and
# torrentfs itself — would parse such a name as an option rather than a path.
validate_mountpoint() {
    local mp="${1:-}"
    if [ -z "$mp" ]; then
        echo "[entrypoint] ERROR: no mount point specified" >&2
        echo "[entrypoint] Usage: torrentfs [OPTIONS] <MOUNTPOINT>" >&2
        exit 2
    fi
    case "$mp" in
        -*)
            echo "[entrypoint] ERROR: mount point '$mp' must not begin with '-'" >&2
            echo "[entrypoint] Prefix with './' to use a literal '-'-prefixed name." >&2
            exit 2
            ;;
    esac
}

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

# Try a metadata-only stat on $1 and report whether it fails with ENOTCONN.
# Used to detect a stale FUSE mount left behind by a previous container run
# (the kernel reports ENOTCONN — "Transport endpoint is not connected" — when
# the FUSE daemon that owned the mount is gone).
mountpoint_enotconn() {
    local target="$1" err
    if stat -c '%f' "$target" >/dev/null 2>&1; then
        return 1
    fi
    # stat failed — distinguish ENOTCONN (stale FUSE mount) from other errors
    # (ENOENT, EACCES, …) via stderr. `|| true` keeps the failing stat from
    # tripping `set -e`; a stat|grep pipeline would report the stat failure
    # under `pipefail` even when grep matched.
    err="$(stat -c '%f' "$target" 2>&1 >/dev/null || true)"
    case "$err" in
        *'Transport endpoint is not connected'*) return 0 ;;
        *) return 1 ;;
    esac
}
# print an actionable recovery note. Returns 0 when the path is usable
# (never ENOTCONN again), 1 when it remains a dead mount we cannot reclaim.
recover_stale_mountpoint() {
    local target="$1"

    if ! mountpoint_enotconn "$target"; then
        return 0
    fi

    echo "[entrypoint] WARNING: $target is a stale FUSE mount (ENOTCONN)" >&2
    echo "[entrypoint]   This happens when a previous container was SIGKILLed before" >&2
    echo "[entrypoint]   torrentfs could unmount cleanly." >&2
    echo "[entrypoint]   Attempting lazy unmount (umount -l) and retrying…" >&2

    if ! umount -l "$target" 2>/dev/null; then
        echo "[entrypoint] ERROR: could not lazy-unmount stale mountpoint $target" >&2
        cat >&2 <<'RECOVERY'
╔══════════════════════════════════════════════════════════════════════╗
║  Stale FUSE mount detected at the mountpoint and it could not be     ║
║  reclaimed automatically.                                            ║
║                                                                      ║
║  Recover manually, then start the container again:                   ║
║                                                                      ║
║  1. Inside the container (or on the host that owns the mount):       ║
║       fusermount -uz /mnt                                            ║
║       umount -l /mnt                                                 ║
║                                                                      ║
║  2. Verify the mountpoint is gone:                                   ║
║       mountpoint -q /mnt && echo "still mounted" || echo "clean"     ║
║                                                                      ║
║  3. Start the container again.                                       ║
╚══════════════════════════════════════════════════════════════════════╝
RECOVERY
        return 1
    fi

    # Give the lazy detach a moment, then confirm the path no longer fails
    # with ENOTCONN before we let mkdir/torrentfs touch it.
    sleep 0.2
    if mountpoint_enotconn "$target"; then
        echo "[entrypoint] ERROR: $target still reports ENOTCONN after lazy unmount" >&2
        return 1
    fi

    echo "[entrypoint] stale mount reclaimed — $target is usable again" >&2
    return 0
}

# Wait up to 30s for torrentfs (pid $1) to publish its FUSE mount at $2.
# Returns 0 once the mount is ready. On failure — torrentfs exiting before the
# mount appeared, or the 30s deadline passing — prints an actionable diagnostic
# and returns a non-zero exit code to propagate (torrentfs's own code when it
# exited first, otherwise 1). Callers must `exit` with the returned code.
wait_for_fuse_mount() {
    local torrentfs_pid="$1" target="$2"

    local ready=0 exited=0 i
    for i in $(seq 1 60); do
        if mountpoint -q "$target" 2>/dev/null; then
            ready=1
            break
        fi
        # torrentfs died before the mount came up (e.g. fusermount3 was
        # blocked from opening /dev/fuse by Docker's seccomp profile) — stop
        # waiting so we surface its exit code instead of a bare timeout.
        if ! kill -0 "$torrentfs_pid" 2>/dev/null; then
            exited=1
            break
        fi
        sleep 0.5
    done

    if [ "$ready" -eq 1 ]; then
        return 0
    fi

    local rc=1
    if [ "$exited" -eq 1 ]; then
        # Reap torrentfs so its non-zero status is not lost; `wait` under
        # `set -e` would otherwise swallow it.
        wait "$torrentfs_pid" 2>/dev/null || rc=$?
    else
        # Still alive but the mount never appeared — stop it.
        kill "$torrentfs_pid" 2>/dev/null || true
        wait "$torrentfs_pid" 2>/dev/null || true
    fi
    # A clean (0) exit with no mount is still a crash from the container's view.
    [ "$rc" -ne 0 ] || rc=1

    if [ "$exited" -eq 1 ]; then
        echo "[entrypoint] ERROR: torrentfs exited with code $rc before the FUSE mount became ready" >&2
    else
        echo "[entrypoint] ERROR: FUSE mount at $target did not become ready within 30s" >&2
    fi
    echo "[entrypoint]   The FUSE filesystem could not be mounted. Common causes:" >&2
    echo "[entrypoint]     - missing --device /dev/fuse (or the fuse kernel module)" >&2
    echo "[entrypoint]     - missing --cap-add SYS_ADMIN (required for the mount syscall)" >&2
    echo "[entrypoint]     - Docker seccomp blocking /dev/fuse access even when the" >&2
    echo "[entrypoint]       device node exists: add --security-opt seccomp=unconfined" >&2
    echo "[entrypoint]       (or run --privileged)" >&2
    return "$rc"
}

# Detect whether $1 already has a FUSE mount stacked on it. Returns 0 when a
# fuse-type entry exists at the target, 1 otherwise.
#
# Two containers sharing one host directory via rshared bind propagation can
# stack FUSE mounts on top of each other: container2's `mount --bind` on its
# /mnt propagates onto container1's /mnt, and the later mount severs the
# earlier container's FUSE connection (ENOTCONN). Before this entrypoint
# mounts anything, a fuse entry at $1 can only belong to another container —
# so this probe doubles as the "another torrentfs already owns this
# mountpoint" conflict detector. A plain bind mount (`-v host:/mnt`) is not a
# FUSE mount and does not trip it.
#
# The mountinfo source is injectable via TORRENTFS_MOUNTINFO_PATH so tests can
# exercise this production function directly against a fixture.
mountpoint_has_fuse() {
    local target="$1" mountinfo="${TORRENTFS_MOUNTINFO_PATH:-/proc/self/mountinfo}"
    # Field 5 is the mount point. The filesystem type is the first field after
    # the "-" separator: the optional-fields column (field 7) holds 0..N
    # entries (`shared:X master:Y` on rshared propagation trees), so fstype is
    # NOT a fixed column index and `$9` would miss a fuse mount.
    awk -v mp="$target" '
        $5 == mp {
            for (i = 7; i < NF; i++) {
                if ($i == "-" && $(i + 1) ~ /^fuse/) { found = 1; exit }
            }
        }
        END { exit !found }
    ' "$mountinfo" 2>/dev/null
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

validate_mountpoint "$mountpoint"

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

    # A previous run that was SIGKILLed (stop_timeout too short) can leave a
    # stale FUSE mount on $mountpoint: the kernel reports ENOTCONN because the
    # FUSE daemon that owned the mount is gone. mkdir -p would then fail with
    # ENOTCONN and podman start never recovers. Probe and reclaim before
    # mounting so a restart self-heals.
    if ! recover_stale_mountpoint "$mountpoint"; then
        exit 100
    fi

    # Serialize mount ownership across containers. The lock is taken on the
    # mountpoint directory itself: two containers bind-mounting the same host
    # directory resolve it to the same inode, so flock on that inode gives
    # cross-container mutual exclusion. Held for the container's lifetime (fd 9
    # stays open), it closes the check-then-mount TOCTOU window for
    # simultaneous starts — exactly one container proceeds to mount.
    mkdir -p "$mountpoint"
    exec 9<"$mountpoint"
    if ! flock -n 9; then
        echo "[entrypoint] ERROR: $mountpoint is locked by another running torrentfs" >&2
        echo "[entrypoint]   container. One host directory supports a single torrentfs" >&2
        echo "[entrypoint]   container — stop the other one before starting this one." >&2
        exit 101
    fi

    # A live FUSE mount already on $mountpoint means another torrentfs
    # container propagated its mount here (rshared shared host directory).
    # Stacking a second FUSE mount would sever the other container's mount —
    # ENOTCONN on both sides — so refuse instead of clobbering it.
    if mountpoint_has_fuse "$mountpoint"; then
        echo "[entrypoint] ERROR: $mountpoint is already a FUSE mount owned by another" >&2
        echo "[entrypoint]   running torrentfs instance." >&2
        echo "[entrypoint]   Two containers bind-mounted the same host directory with" >&2
        echo "[entrypoint]   rshared propagation; stacking a second FUSE mount here would" >&2
        echo "[entrypoint]   sever the first container's mount (ENOTCONN)." >&2
        echo "[entrypoint]   One host directory supports a single torrentfs container." >&2
        echo "[entrypoint]   Fix: stop the other container, or give this container a" >&2
        echo "[entrypoint]   different host directory / mountpoint." >&2
        exit 101
    fi

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

    local mount_rc=0
    wait_for_fuse_mount "$torrentfs_pid" "$mountpoint" || mount_rc=$?
    if [ "$mount_rc" -ne 0 ]; then
        exit "$mount_rc"
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

    local mount_rc=0
    wait_for_fuse_mount "$torrentfs_pid" "$internal_mnt" || mount_rc=$?
    if [ "$mount_rc" -ne 0 ]; then
        exit "$mount_rc"
    fi

    echo "[entrypoint] FUSE mount ready — publishing to $mountpoint" >&2
    mount --bind "$internal_mnt" "$mountpoint"

    echo "[entrypoint] torrentfs running (pid=$torrentfs_pid), available at $mountpoint" >&2

    local rc=0
    wait "$torrentfs_pid" || rc=$?
    exit "$rc"
}

echo "[entrypoint] /dev/fuse is available" >&2

start_torrentfs "$mountpoint" "${torrentfs_args[@]}"

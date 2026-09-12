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
#      (e.g. -v /host:/mnt:shared) — the host-visibility recipe — the entrypoint
#      fails fast (exit 102) instead of silently mounting container-only: the
#      ':shared' flag is ineffective and the host will never see the FUSE mount.
#   6. Concurrent-mountpoint conflict handling: two containers sharing one
#      host directory via rshared propagation would stack FUSE mounts and
#      sever each other's mount (ENOTCONN). The entrypoint takes an exclusive
#      flock on the mountpoint directory (same inode across containers) held
#      for the container lifetime, then refuses to start if $mountpoint
#      already carries a foreign FUSE mount.

set -euo pipefail

# ── privilege drop ────────────────────────────────────────────────────────────
# torrentfs is a network-facing daemon (untrusted .torrent input + libtorrent).
# A rootful container holds real root and drops to this dedicated non-root user
# before the daemon starts; rootless podman maps container UID 0 to the invoking
# host user (user namespace), so its "root" is already unprivileged on the host
# and a drop to a subuid would sever /dev/fuse and bind-mounted state access —
# no drop occurs there.
TORRENTFS_UID=1000
TORRENTFS_GID=1000
TORRENTFS_HOME=/home/torrentfs

# Resolved at startup by resolve_daemon_ids: who the daemon runs as, and who
# the state tree is re-homed to.
daemon_uid=""
daemon_gid=""
daemon_home=""

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
# --cache / --db values, captured so the state-ownership fix can cover custom
# state volumes in addition to the default XDG directory.
cache_arg=""
db_arg=""

parse_args() {
    local arg expect_value="" options_ended=0
    mountpoint=""
    torrentfs_args=()
    cache_arg=""
    db_arg=""

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
            case "$expect_value" in
                --config) validate_config "$arg" ;;
                --db) db_arg="$arg" ;;
                --cache) cache_arg="$arg" ;;
            esac
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
            --db=*)
                db_arg="${arg#--db=}"
                torrentfs_args+=("$arg")
                ;;
            --cache=*)
                cache_arg="${arg#--cache=}"
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

# Drop privileges only in a rootful container (real root). Rootless podman's
# "root" is the invoking host user inside a user namespace: keep it, since a
# drop would sever /dev/fuse and state-volume access.
should_drop_privileges() {
    is_root && ! is_rootless_podman
}

# Resolve the daemon identity once, so the state-ownership fix and the launch
# both target the same owner torrentfs will actually run as.
resolve_daemon_ids() {
    if should_drop_privileges; then
        daemon_uid="$TORRENTFS_UID"
        daemon_gid="$TORRENTFS_GID"
        daemon_home="$TORRENTFS_HOME"
    else
        daemon_uid="$(id -u)"
        daemon_gid="$(id -g)"
        daemon_home="${HOME:-/}"
    fi
}

# Launch torrentfs under the correct identity. Always called in the background;
# `exec` keeps the background subshell's PID equal to the daemon's PID so the
# caller's `kill`/`wait` track torrentfs itself (an intermediate subshell would
# orphan the daemon on shutdown).
run_daemon() {
    if should_drop_privileges; then
        exec setpriv --reuid="$daemon_uid" --regid="$daemon_gid" --clear-groups \
            env HOME="$daemon_home" "$@"
    fi
    exec "$@"
}
# Check whether $1 is a bind mount (root field != "/" in /proc/self/mountinfo).
# Used to detect `-v <host>:<container>:shared` style bind mounts: under rootless
# podman the ':shared' propagation is silently downgraded, so the FUSE mount can
# never reach the host — the entrypoint fails fast rather than degrade silently.
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
            # A non-root daemon (privilege drop in rootful containers) needs
            # read-write /dev/fuse. Only chmod the node we just created (0600
            # root): chmod-ing a --device-provided node would mutate the host's
            # device permissions through the rootful bind mount.
            chmod a+rw /dev/fuse
            echo "[entrypoint] /dev/fuse created successfully" >&2
            return 0
        fi
        echo "[entrypoint] mknod /dev/fuse failed" >&2
    fi

    return 1
}

# torrentfs persists its piece cache and SQLite DB under the XDG data
# directory ($XDG_DATA_HOME or ~/.local/share), or under --cache / --db when
# those overrides are given.  A previous container run — e.g. an older image
# that ran as a non-root user — can leave that tree owned by a different UID
# (nobody:nogroup).  The current user then cannot write the DB (SQLite
# ReadOnly) or the cache metadata, which silently disables the download
# engine so every data read returns EIO.  Re-home the tree to the current
# user before starting so a reused state volume is always writable.

# Re-home one path to the current user.  Recurses only when the tree's
# ownership does not already match the current user throughout.  A reused
# volume can have a correct top-level owner but leftover nobody:nogroup files
# underneath (old image state) — a top-level-only probe misses those, leaving
# cache_metadata.txt unwritable and failing the download engine.  `find`
# exits at the first mismatch, so a consistent GB-scale piece cache is only
# walked read-only (no chown syscalls) on every cold start.  Skipped when the
# path does not exist yet: torrentfs creates it fresh with correct ownership.
rehome_ownership() {
    local target="$1" probe
    [ -e "$target" ] || return 0
    # resolve_daemon_ids() runs first (main calls it before
    # fix_state_dir_ownership); an empty daemon_uid would make `find ! -user ""`
    # a probe failure and `chown -R ":"` a silent no-op that leaves the tree
    # foreign-owned. Fail loudly if that contract is broken.
    if [ -z "${daemon_uid:-}" ] || [ -z "${daemon_gid:-}" ]; then
        echo "[entrypoint] ERROR: daemon identity not resolved; refusing to rehome $target" >&2
        return 1
    fi
    # Capture find's exit status separately from its output: a consistent tree
    # is status==0 AND empty output.  A non-zero status (unreadable subtree,
    # unmapped nobody dir in a rootless userns, I/O error, faulty mount) is a
    # probe failure — not a clean bill of health — so fall through to chown
    # rather than silently skipping a leftover foreign-owned tree.
    if probe="$(find "$target" \( ! -user "$daemon_uid" -o ! -group "$daemon_gid" \) -print -quit 2>/dev/null)"; then
        if [ -z "$probe" ]; then
            return 0
        fi
        # A foreign-owned entry was found. Warn before re-homing so a reused
        # state volume's chown to the daemon user is visible rather than
        # silent: the host-side owner (`torrentfs:torrentfs`) is a consequence
        # of this re-home, not an image defect.
        echo "[entrypoint] WARNING: foreign-owned files found in $target (first: $probe); re-homing to $daemon_uid:$daemon_gid" >&2
    fi
    if ! chown -R "$daemon_uid:$daemon_gid" "$target" 2>/dev/null; then
        echo "[entrypoint] WARNING: could not chown $target to $daemon_uid:$daemon_gid" >&2
    fi
}

fix_state_dir_ownership() {
    # Only root can re-home a foreign-owned tree; non-root runs (bare metal)
    # cannot chown and should not try.
    if ! is_root; then
        return 0
    fi

    rehome_ownership "${XDG_DATA_HOME:-$daemon_home/.local/share}/torrentfs"

    if [ -n "${cache_arg:-}" ]; then
        rehome_ownership "$cache_arg"
    fi
    if [ -n "${db_arg:-}" ]; then
        # --db names the SQLite file; SQLite writes its -wal / -shm sidecars
        # into the file's parent directory, so re-home that too (never `.` or
        # `/`, which a bare filename or root path would produce).
        local db_dir
        db_dir="$(dirname "$db_arg")"
        case "$db_dir" in
            .|/|'') : ;;
            *) rehome_ownership "$db_dir" ;;
        esac
        rehome_ownership "$db_arg"
    fi
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
        # `mountpoint -q` reports any mountpoint as mounted, so a bind mount at
        # $target (e.g. -v /host:/mnt) would make it succeed before torrentfs
        # has published its FUSE filesystem. Probe mountinfo for a fuse-type
        # entry instead, so readiness means the FUSE mount itself is up.
        if mountpoint_has_fuse "$target"; then
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
# The target is canonicalized before comparison because mountinfo records the
# kernel-normalized mount point (symlinks resolved, `.`/`..` merged, trailing
# `/` dropped) — the raw CLI path would never match for relative, symlink, or
# non-canonical inputs.
#
# The mountinfo source is injectable via TORRENTFS_MOUNTINFO_PATH so tests can
# exercise this production function directly against a fixture.
mountpoint_has_fuse() {
    local target="$1" mountinfo="${TORRENTFS_MOUNTINFO_PATH:-/proc/self/mountinfo}"
    # Canonicalize to the kernel's representation. The mountpoint exists by the
    # time we probe (mkdir -p ran first); if it can't be resolved, report "no
    # FUSE mount" rather than risk a false ready.
    target="$(readlink -f "$target" 2>/dev/null)" || return 1
    # Field 5 is the mount point. The filesystem type is the first field after
    # the "-" separator: the optional-fields column (field 7) holds 0..N
    # entries (`shared:X master:Y` on rshared propagation trees), so fstype is
    # NOT a fixed column index and `$9` would miss a fuse mount.
    #
    # Field 5 octal-escapes space/tab/newline/backslash (`\040`/`\011`/`\012`/
    # `\134`); decode before comparing against the canonical target. Backslash
    # is decoded last so an escaped backslash (`\134`) isn't re-read as the
    # start of another escape sequence (`\134012` must stay backslash+"012").
    # The canonical target is passed via the environment (ENVIRON) rather than
    # `awk -v`, which would interpret backslash escapes in the value and mangle
    # a mountpoint whose path contains a literal backslash.
    TORRENTFS_TARGET="$target" awk '
        BEGIN { mp = ENVIRON["TORRENTFS_TARGET"] }
        {
            p = $5
            gsub(/\\040/, " ", p)
            gsub(/\\011/, "\t", p)
            gsub(/\\012/, "\n", p)
            gsub(/\\134/, "\\", p)
            if (p == mp) {
                for (i = 7; i < NF; i++) {
                    if ($i == "-" && $(i + 1) ~ /^fuse/) { found = 1; exit }
                }
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

    if should_drop_privileges; then
        start_torrentfs_rootful "$mountpoint" "$@"
    else
        start_torrentfs_rootless "$mountpoint" "$@"
    fi
}
# Direct mount path (no bind mount): torrentfs mounts directly on the
# mountpoint, so the FUSE filesystem is only visible inside the container.
# Used whenever host visibility is impossible — rootless podman (user
# namespace) and non-root `--user` runs. Under rootless podman running as
# container root, a bind mount on the mountpoint (the ':shared' host-visibility
# recipe) fails fast (exit 102) instead of silently degrading.
start_torrentfs_rootless() {
    local mountpoint="$1"
    shift

    mkdir -p "$mountpoint"

    # A bind mount on the mountpoint signals host-visibility intent:
    # `-v <host>:<container>:shared` is the standard startup recipe. Under
    # rootless podman that ':shared' flag is silently downgraded to a private
    # bind mount — the user namespace cannot create shared mount events — so
    # the FUSE mount can never reach the host. Fail fast instead of silently
    # mounting container-only: the user asked for a host-visible mount that we
    # cannot deliver, and a confusing "host cannot see data/" is worse than a
    # clear refusal.
    #
    # Scoped to container root (is_root): only root under rootless podman is
    # the `:shared` host-visibility recipe. A non-root `--user` run (Docker or
    # rootless podman) bind-mounts /mnt for mountpoint writability, not host
    # visibility — that keeps the container-only path with a warning below.
    if is_bind_mount "$mountpoint"; then
        if is_rootless_podman && is_root; then
            echo "[entrypoint] ERROR: $mountpoint is a bind mount, but shared propagation" >&2
            echo "[entrypoint]   (rshared) is unsupported under rootless podman — the" >&2
            echo "[entrypoint]   ':shared' flag is silently ignored, so the FUSE mount" >&2
            echo "[entrypoint]   cannot reach the host. Refusing to start container-only" >&2
            echo "[entrypoint]   instead of silently degrading." >&2
            echo "[entrypoint]   Fixes:" >&2
            echo "[entrypoint]     - Host-visible mount: run rootful — Docker, or 'sudo" >&2
            echo "[entrypoint]       podman' with --mount type=bind,bind-propagation=rshared." >&2
            echo "[entrypoint]     - Container-only access: drop the ':shared' bind mount and" >&2
            echo "[entrypoint]       reach data/ via 'podman exec <container> ls /mnt/…'." >&2
            exit 102
        fi
        # Non-root `--user` run (Docker or rootless podman): a bind mount here
        # is for mountpoint writability (the image's /mnt is root-owned), not
        # host visibility. Still surface the host-visibility limitation in case
        # ':shared' was intended, but proceed container-only as documented.
        echo "[entrypoint] WARNING: $mountpoint is a bind mount, but shared propagation is" >&2
        echo "[entrypoint]   unavailable for a non-root user — ':shared' / 'rshared' is" >&2
        echo "[entrypoint]   ineffective here. The FUSE filesystem will only be visible" >&2
        echo "[entrypoint]   inside the container. For host-visible mounts, run as root" >&2
        echo "[entrypoint]   via Docker or rootful podman (sudo podman)." >&2
    fi

    echo "[entrypoint] direct mount (no host propagation) — FUSE mount will only be visible inside the container" >&2
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

    run_daemon torrentfs "$mountpoint" "$@" &
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
    if should_drop_privileges; then
        chown "$daemon_uid:$daemon_gid" "$internal_mnt"
    fi
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

    run_daemon torrentfs "$internal_mnt" "$@" &
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

resolve_daemon_ids

fix_state_dir_ownership

start_torrentfs "$mountpoint" "${torrentfs_args[@]}"

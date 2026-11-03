#!/usr/bin/env bash
# Unit tests for entrypoint.sh detection helpers — the pure-logic functions that
# determine container environment and mount-propagation capabilities, without a
# real FUSE mount or a running torrentfs process.
# The entrypoint script runs main at the bottom, so we cannot `source` it;
# extract the helper functions (before the "# ── main" marker) into a temp file
# and source that prefix.
# Usage: ./ci/tests/entrypoint_test.sh   Exit: 0 = pass, 1 = fail.

set -euo pipefail

ENTRYPOINT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/entrypoint.sh"
PASS=0
FAIL=0

# ── extract helpers ──────────────────────────────────────────────────────────

# Extract everything before the main section marker.
HELPERS_FILE="$(mktemp)"
awk '/^# ── main/{exit} {print}' "$ENTRYPOINT" > "$HELPERS_FILE"
# Also extract the start_torrentfs function (defined after the main marker) so
# the integration test can exercise its exit-101 conflict paths without
# running the real FUSE mount or dispatch functions.
awk '/^start_torrentfs\(\) \{/{f=1} f{print} f && /^}/{exit}' "$ENTRYPOINT" >> "$HELPERS_FILE"
# Also extract start_torrentfs_rootless so the fail-fast (exit 102) branch can
# be exercised without a real FUSE mount or torrentfs binary.
awk '/^start_torrentfs_rootless\(\) \{/{f=1} f{print} f && /^}/{exit}' "$ENTRYPOINT" >> "$HELPERS_FILE"
# Also extract start_torrentfs_rootful so the publish-bind stop race can be
# exercised with a fake mount/umount.
awk '/^start_torrentfs_rootful\(\) \{/{f=1} f{print} f && /^}/{exit}' "$ENTRYPOINT" >> "$HELPERS_FILE"
trap 'rm -f "$HELPERS_FILE"' EXIT

# Append a test-only override helper to the extracted functions so it is
# available alongside is_bind_mount in every subshell that sources
# HELPERS_FILE.  This keeps the test bodies free of duplicated mktemp +
# printf logic.
cat >> "$HELPERS_FILE" <<'EOF'

# Write a fake mountinfo to a temp file, point production mountpoint_has_fuse
# at it via TORRENTFS_MOUNTINFO_PATH, and redefine is_bind_mount to read from
# it instead of /proc/self/mountinfo.
setup_mountinfo() {
    local content="$1"
    local tmpfile
    tmpfile="$(mktemp)"
    printf '%s\n' "$content" > "$tmpfile"
    MOUNTINFO_FAKE="$tmpfile"
    # mountpoint_has_fuse reads its source from TORRENTFS_MOUNTINFO_PATH (the
    # production function, not a redefined copy), so point it at the fixture.
    export TORRENTFS_MOUNTINFO_PATH="$tmpfile"
    trap 'rm -f "$MOUNTINFO_FAKE"' EXIT
    is_bind_mount() {
        local target="$1"
        awk -v mp="$target" '$5 == mp && $4 != "/" { found=1 } END { exit !found }' \
            "$MOUNTINFO_FAKE" 2>/dev/null
    }
}

# Define a fake `stat` shell function for mountpoint_enotconn tests. `stat`
# is a shell builtin in this environment, so a PATH stub is never consulted;
# a function shadows the builtin instead.
# $1 = exit code (integer), $2 = stderr text (verbatim).
setup_stat() {
    # `stat` is a shell builtin here, so a PATH stub is never consulted. A
    # shell function shadows the builtin; it reads globals because a nested
    # function cannot see `local`s after the defining function returns.
    STAT_FAKE_EXIT="$1"
    STAT_FAKE_STDERR="$2"
    stat() {
        printf '%s\n' "$STAT_FAKE_STDERR" >&2
        return "$STAT_FAKE_EXIT"
    }
}

# Install a fake `setpriv` on PATH that prints its argv (one per line) to
# stdout and exits 0. Lets run_daemon tests assert the exact privilege-drop
# invocation without a real setpriv or a drop to an existing UID.
install_fake_setpriv() {
    local dir
    dir="$(mktemp -d)"
    # The interpreter is resolved, not hard-coded to /bin/bash: the test host may
    # have bash only under another prefix (merged-usr images do, some sandboxes
    # do not), and a missing interpreter would make every run_daemon assertion
    # fail for a reason unrelated to the code under test.
    printf '%s\n' "#!$(command -v bash)" 'printf "%s\n" "$@"' > "$dir/setpriv"
    chmod +x "$dir/setpriv"
    PATH="$dir:$PATH"
}

# Stub out validate_config so parse_args tests can pass --config without
# invoking the real torrentfs binary (unavailable in this test environment).
# parse_args is only exercised for argument structure here; config validity
# itself is covered by the Rust --config-check tests.
validate_config() { :; }

# Fake `find` that reports every path as foreign-owned.  The probe's
# `-print -quit` form emits one plain line (read by command substitution); the
# re-home walk's `-print0` form must emit NUL separators, because rehome_tree
# reads records with `read -d ''` and a newline-only stream is one partial
# record.
stub_find_mismatch() {
    find() {
        case "$*" in
            *-print0*) printf '%s\0' "/mismatch" ;;
            *) echo "/mismatch" ;;
        esac
    }
}
EOF

# ── test harness ─────────────────────────────────────────────────────────────

# Run a test in a subshell with helpers sourced.
# $1 = description, rest = command to execute (must exit 0 on pass, non-0 on fail).
run_test() {
    local desc="$1"; shift
    local result=0
    (
        set -eu
        # shellcheck source=/dev/null
        source "$HELPERS_FILE"
        eval "$*"
    ) || result=$?

    # There is no skip status: 77 fails like any other non-zero exit, so a
    # case that re-introduces `exit 77` because its environment could not
    # supply the state turns this job red instead of quietly proving nothing.
    if [ "$result" -eq 0 ]; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (exit $result)"
        FAIL=$((FAIL + 1))
    fi
}

# ── tests ────────────────────────────────────────────────────────────────────

echo "Running entrypoint detection tests..."
echo ""

# --- needs_fuse ---

run_test "needs_fuse --help does not need FUSE" \
    'if needs_fuse --help; then exit 1; else exit 0; fi'

run_test "needs_fuse -h does not need FUSE" \
    'if needs_fuse -h; then exit 1; else exit 0; fi'

run_test "needs_fuse --version does not need FUSE" \
    'if needs_fuse --version; then exit 1; else exit 0; fi'

run_test "needs_fuse -V does not need FUSE" \
    'if needs_fuse -V; then exit 1; else exit 0; fi'

run_test "needs_fuse --config-check does not need FUSE" \
    'if needs_fuse --config-check; then exit 1; else exit 0; fi'

run_test "needs_fuse --config-check --config /foo.toml does not need FUSE" \
    'if needs_fuse --config-check --config /foo.toml; then exit 1; else exit 0; fi'

run_test "needs_fuse /mnt needs FUSE" \
    'if needs_fuse /mnt; then exit 0; else exit 1; fi'

run_test "needs_fuse --config /foo.toml /mnt needs FUSE" \
    'if needs_fuse --config /foo.toml /mnt; then exit 0; else exit 1; fi'

# --- has_config_check ---
# has_config_check drives the --config-check diagnostic short-circuit: unlike
# --help/--version, --config-check conflicts with the mountpoint positional in
# clap, so the entrypoint must forward the parsed args (mountpoint stripped)
# instead of "$@".

run_test "has_config_check --config-check is true" \
    'has_config_check --config-check'

run_test "has_config_check --config /foo.toml --config-check /mnt is true" \
    'has_config_check --config /foo.toml --config-check /mnt'

run_test "has_config_check --help is false" \
    'if has_config_check --help; then exit 1; else exit 0; fi'

run_test "has_config_check /mnt is false" \
    'if has_config_check /mnt; then exit 1; else exit 0; fi'

# --- parse_args (mountpoint identification) ---
# parse_args splits the command line into $mountpoint (first positional) and
# $torrentfs_args (everything else), validating --config values. It must skip
# option values so the mountpoint may precede or follow --config/--db/--cache.

run_test "parse_args /mnt sets mountpoint=/mnt" \
    'parse_args /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args --config /foo.toml /mnt sets mountpoint=/mnt" \
    'parse_args --config /foo.toml /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args /mnt --config /foo.toml sets mountpoint=/mnt" \
    'parse_args /mnt --config /foo.toml; [ "$mountpoint" = /mnt ]'

run_test "parse_args --config=/foo.toml /mnt sets mountpoint=/mnt" \
    'parse_args --config=/foo.toml /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args --db /db/path /mnt sets mountpoint=/mnt" \
    'parse_args --db /db/path /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args --cache /cache/dir /mnt sets mountpoint=/mnt" \
    'parse_args --cache /cache/dir /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args --db /db/path captures db_arg" \
    'parse_args --db /db/path /mnt; [ "$db_arg" = /db/path ]'

run_test "parse_args --cache /cache/dir captures cache_arg" \
    'parse_args --cache /cache/dir /mnt; [ "$cache_arg" = /cache/dir ]'

run_test "parse_args --db=/db/path captures db_arg inline" \
    'parse_args --db=/db/path /mnt; [ "$db_arg" = /db/path ]'

run_test "parse_args --cache=/cache/dir captures cache_arg inline" \
    'parse_args --cache=/cache/dir /mnt; [ "$cache_arg" = /cache/dir ]'

run_test "parse_args forwards non-mountpoint args to torrentfs_args" \
    'parse_args --config /foo.toml /mnt; [ "${#torrentfs_args[@]}" -eq 2 ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = /foo.toml ]'

run_test "parse_args --config /foo.toml --config-check /mnt separates mountpoint" \
    'parse_args --config /foo.toml --config-check /mnt; [ "$mountpoint" = /mnt ] && [ "${#torrentfs_args[@]}" -eq 3 ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = /foo.toml ] && [ "${torrentfs_args[2]}" = --config-check ]'

run_test "parse_args -- /mnt sets mountpoint=/mnt" \
    'parse_args -- /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args -- --mnt sets mountpoint=--mnt" \
    'parse_args -- --mnt; [ "$mountpoint" = "--mnt" ]'

run_test "parse_args -- --config-check treats it as mountpoint (escape)" \
    'parse_args -- --config-check; [ "$mountpoint" = "--config-check" ] && [ "${#torrentfs_args[@]}" -eq 0 ]'

run_test "parse_args --log-file /log/x.log /mnt captures log_file_arg and mountpoint" \
    'parse_args --log-file /log/x.log /mnt; [ "$mountpoint" = /mnt ] && [ "$log_file_arg" = /log/x.log ]'

run_test "parse_args --log-file=/log/x.log captures log_file_arg inline" \
    'parse_args --log-file=/log/x.log /mnt; [ "$mountpoint" = /mnt ] && [ "$log_file_arg" = /log/x.log ]'

run_test "parse_args --log-level debug /mnt does not treat debug as mountpoint" \
    'parse_args --log-level debug /mnt; [ "$mountpoint" = /mnt ] && [ "${#torrentfs_args[@]}" -eq 2 ]'

run_test "parse_args --log-level=debug /mnt keeps mountpoint" \
    'parse_args --log-level=debug /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args --cache-size 4194304 /mnt rejects with exit 2" \
    'rc=0; ( parse_args --cache-size 4194304 /mnt ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "parse_args --config /foo.toml /mnt captures config_arg" \
    'parse_args --config /foo.toml /mnt; [ "$config_arg" = /foo.toml ]'

run_test "parse_args --config=/foo.toml /mnt captures config_arg inline" \
    'parse_args --config=/foo.toml /mnt; [ "$config_arg" = /foo.toml ]'

run_test "parse_args -- /mnt leaves config_arg empty" \
    'parse_args -- /mnt; [ -z "$config_arg" ]'

# --- apply_config_override ---
# apply_config_override resolves the config file in precedence order — CLI
# --config (captured by parse_args) > TORRENTFS_CONFIG env > the mounted default
# path — and injects the winner as a leading --config argument. Tests override
# TORRENTFS_DEFAULT_CONFIG with a temp file (or unset it) so the well-known
# container path (/etc/torrentfs.toml) is never read from the test host.

run_test "apply_config_override injects --config from TORRENTFS_CONFIG" \
    'TORRENTFS_DEFAULT_CONFIG=/nonexistent/torrentfs-default.toml; export TORRENTFS_CONFIG=/env.toml; parse_args /mnt; apply_config_override; [ "$config_arg" = /env.toml ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = /env.toml ] && [ "${#torrentfs_args[@]}" -eq 2 ]'

run_test "apply_config_override defers to CLI --config over TORRENTFS_CONFIG" \
    'TORRENTFS_DEFAULT_CONFIG=/nonexistent/torrentfs-default.toml; export TORRENTFS_CONFIG=/env.toml; parse_args --config /cli.toml /mnt; apply_config_override; [ "$config_arg" = /cli.toml ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = /cli.toml ] && [ "${#torrentfs_args[@]}" -eq 2 ]'

run_test "apply_config_override defers to CLI --config= over TORRENTFS_CONFIG" \
    'TORRENTFS_DEFAULT_CONFIG=/nonexistent/torrentfs-default.toml; export TORRENTFS_CONFIG=/env.toml; parse_args --config=/cli.toml /mnt; apply_config_override; [ "$config_arg" = /cli.toml ] && [ "${#torrentfs_args[@]}" -eq 1 ]'

run_test "apply_config_override no-ops when TORRENTFS_CONFIG is unset" \
    'unset TORRENTFS_CONFIG; TORRENTFS_DEFAULT_CONFIG=/nonexistent/torrentfs-default.toml; parse_args /mnt; apply_config_override; [ -z "$config_arg" ] && [ "${#torrentfs_args[@]}" -eq 0 ]'

run_test "apply_config_override applies the mounted default path when no env/CLI" \
    'unset TORRENTFS_CONFIG; d="$(mktemp -d)"; touch "$d/cfg.toml"; TORRENTFS_DEFAULT_CONFIG="$d/cfg.toml"; parse_args /mnt; apply_config_override
rc=0
[ "$config_arg" = "$d/cfg.toml" ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = "$d/cfg.toml" ] && [ "${#torrentfs_args[@]}" -eq 2 ] || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "apply_config_override prefers TORRENTFS_CONFIG over the mounted default" \
    'd="$(mktemp -d)"; touch "$d/cfg.toml"; TORRENTFS_DEFAULT_CONFIG="$d/cfg.toml"; export TORRENTFS_CONFIG=/env.toml; parse_args /mnt; apply_config_override
rc=0
[ "$config_arg" = /env.toml ] && [ "${torrentfs_args[1]}" = /env.toml ] || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "apply_config_override prefers CLI --config over the mounted default" \
    'd="$(mktemp -d)"; touch "$d/cfg.toml"; TORRENTFS_DEFAULT_CONFIG="$d/cfg.toml"; unset TORRENTFS_CONFIG; parse_args --config /cli.toml /mnt; apply_config_override
rc=0
[ "$config_arg" = /cli.toml ] && [ "${#torrentfs_args[@]}" -eq 2 ] || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "apply_config_override set-but-empty TORRENTFS_CONFIG opts out of the mounted default" \
    'd="$(mktemp -d)"; touch "$d/cfg.toml"; TORRENTFS_DEFAULT_CONFIG="$d/cfg.toml"; export TORRENTFS_CONFIG=; parse_args /mnt; apply_config_override
rc=0
[ -z "$config_arg" ] && [ "${#torrentfs_args[@]}" -eq 0 ] || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "apply_config_override no-ops when the default path file is absent" \
    'unset TORRENTFS_CONFIG; TORRENTFS_DEFAULT_CONFIG=/no/such/config.toml; parse_args /mnt; apply_config_override; [ -z "$config_arg" ] && [ "${#torrentfs_args[@]}" -eq 0 ]'

# --- should_apply_config_override ---
# Config override resolution is applied only when the command consumes the
# parsed args — the mount path or --config-check. Diagnostic commands that
# forward the raw "$@" (--help/--version) skip it, so a bad config path cannot
# block their output.

run_test "should_apply_config_override true for a mount command" \
    'parse_args /mnt; should_apply_config_override /mnt'

run_test "should_apply_config_override false for --help" \
    'parse_args --help; if should_apply_config_override --help; then exit 1; else exit 0; fi'

run_test "should_apply_config_override false for --version" \
    'parse_args --version; if should_apply_config_override --version; then exit 1; else exit 0; fi'

run_test "should_apply_config_override true for --config-check" \
    'parse_args --config-check; should_apply_config_override --config-check'

# --- revalidate_config_as_daemon ---
# Re-checks the effective config as the daemon user after the privilege drop,
# so a config readable by root but not by UID 1000 fails at validation time
# instead of "validated, then daemon exits with Failed to load config".
# `setpriv` is a PATH binary, so a shell-function stub shadows it (no real drop
# and no torrentfs invocation).

run_test "revalidate_config_as_daemon no-ops without a config" \
    'config_arg=""; should_drop_privileges() { return 0; }; revalidate_config_as_daemon'

run_test "revalidate_config_as_daemon no-ops when not dropping privileges" \
    'config_arg=/x.toml; should_drop_privileges() { return 1; }; revalidate_config_as_daemon'

run_test "revalidate_config_as_daemon passes when the daemon can read the config" \
    'config_arg=/x.toml; daemon_uid=1000; daemon_gid=1000; should_drop_privileges() { return 0; }; setpriv() { return 0; }; revalidate_config_as_daemon'

run_test "revalidate_config_as_daemon fails fast when the daemon cannot read the config" \
    'config_arg=/x.toml; daemon_uid=1000; daemon_gid=1000; should_drop_privileges() { return 0; }; setpriv() { return 1; }
rc=0
( revalidate_config_as_daemon ) 2>/dev/null || rc=$?
[ "$rc" -ne 0 ]'

# --- validate_mountpoint (mountpoint guard) ---
# validate_mountpoint rejects a missing or `-`-prefixed mountpoint with exit 2
# before the FUSE device check. The nested subshell captures the exit code so
# `set -e` does not abort the test on the expected non-zero status.

run_test "validate_mountpoint /mnt accepts a normal path" \
    'validate_mountpoint /mnt'

run_test "validate_mountpoint empty rejects with exit 2" \
    'rc=0; ( validate_mountpoint "" ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "validate_mountpoint --mnt rejects dash-prefixed with exit 2" \
    'rc=0; ( validate_mountpoint --mnt ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

# --- in_container (on a non-container test system, should return false) ---

run_test "in_container returns false on bare metal" \
    'if in_container; then exit 1; else exit 0; fi'

# --- is_rootless_podman (on a non-container test system, should return false) ---

run_test "is_rootless_podman returns false on bare metal" \
    'if is_rootless_podman; then exit 1; else exit 0; fi'

# --- is_bind_mount ---
# is_bind_mount reads /proc/self/mountinfo directly. The setup_mountinfo
# helper (appended to HELPERS_FILE above) writes a fake mountinfo to a temp
# file and redefines is_bind_mount to read from it instead.

run_test "is_bind_mount detects bind mount (root field != /)" \
    'setup_mountinfo "1 0 0:1 / /proc rw shared:1 - proc proc rw
2 1 0:2 /host/path /mnt rw shared:2 - none none rw"; if is_bind_mount /mnt; then exit 0; else exit 1; fi'

run_test "is_bind_mount returns false for non-bind mount (root = /)" \
    'setup_mountinfo "1 0 0:1 / /proc rw shared:1 - proc proc rw
2 1 0:2 / /mnt rw shared:2 - tmpfs tmpfs rw"; if is_bind_mount /mnt; then exit 1; else exit 0; fi'

run_test "is_bind_mount returns false for non-existent mountpoint" \
    'setup_mountinfo "1 0 0:1 / /proc rw shared:1 - proc proc rw
2 1 0:2 /host/path /mnt rw shared:2 - none none rw"; if is_bind_mount /nonexistent; then exit 1; else exit 0; fi'

run_test "is_bind_mount returns false for empty mountinfo" \
    'setup_mountinfo ""; if is_bind_mount /mnt; then exit 1; else exit 0; fi'

# --- mountpoint_has_fuse ---
# Detects a FUSE mount at the target. The filesystem type is the first field
# after the "-" separator (0..N optional fields precede it), so fixtures vary
# the optional-field count to prove fstype is not read from a fixed column.

run_test "mountpoint_has_fuse true with 0 optional fields" \
    'setup_mountinfo "36 35 98:0 /mnt-inner /mnt rw - fuse.torrentfs torrentfs rw"; if mountpoint_has_fuse /mnt; then exit 0; else exit 1; fi'

run_test "mountpoint_has_fuse true with 2 optional fields (shared master)" \
    'setup_mountinfo "36 35 98:0 /mnt-inner /mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"; if mountpoint_has_fuse /mnt; then exit 0; else exit 1; fi'

run_test "mountpoint_has_fuse true when fstype is bare fuse" \
    'setup_mountinfo "36 35 98:0 /mnt-inner /mnt rw shared:1 master:2 - fuse torrentfs rw"; if mountpoint_has_fuse /mnt; then exit 0; else exit 1; fi'

run_test "mountpoint_has_fuse false for non-fuse mount (ext4, 2 optional fields)" \
    'setup_mountinfo "36 35 98:0 / /mnt rw shared:1 master:2 - ext4 /dev/sda1 rw"; if mountpoint_has_fuse /mnt; then exit 1; else exit 0; fi'

run_test "mountpoint_has_fuse false for fuse mount at another target" \
    'setup_mountinfo "36 35 98:0 /mnt-inner /mnt-inner rw shared:1 master:2 - fuse.torrentfs torrentfs rw"; if mountpoint_has_fuse /mnt; then exit 1; else exit 0; fi'

run_test "mountpoint_has_fuse false for empty mountinfo" \
    'setup_mountinfo ""; if mountpoint_has_fuse /mnt; then exit 1; else exit 0; fi'

# --- mountpoint_has_fuse canonicalization / escape decoding ---
# mountinfo records the kernel-normalized mount point (symlinks resolved,
# `.`/`..` merged, trailing `/` dropped) with space/tab/newline/backslash
# octal-escaped.
# These fixtures exercise a real target path against its mountinfo-escaped
# form, proving mountpoint_has_fuse canonicalizes and decodes before comparing.

run_test "mountpoint_has_fuse canonicalizes a relative target" \
    'd="$(mktemp -d)"; mkdir -p "$d/sub"
canon="$(readlink -f "$d/sub")"
setup_mountinfo "36 35 98:0 / $canon rw - fuse.torrentfs torrentfs rw"
rc=0
( cd "$d" && mountpoint_has_fuse sub ) || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse matches a target containing a space" \
    'd="$(mktemp -d)"; mkdir -p "$d/with space"
escaped="$d/with\040space"
setup_mountinfo "36 35 98:0 / $escaped rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/with space" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse matches a target with a trailing slash" \
    'd="$(mktemp -d)"; mkdir -p "$d/sub"
canon="$(readlink -f "$d/sub")"
setup_mountinfo "36 35 98:0 / $canon rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/sub/" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse canonicalizes dot-dot path components" \
    'd="$(mktemp -d)"; mkdir -p "$d/sub"
canon="$(readlink -f "$d/sub")"
setup_mountinfo "36 35 98:0 / $canon rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/../$(basename "$d")/sub" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse resolves a symlink target" \
    'd="$(mktemp -d)"; mkdir -p "$d/sub"; ln -s "$d/sub" "$d/link"
canon="$(readlink -f "$d/sub")"
setup_mountinfo "36 35 98:0 / $canon rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/link" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse matches a target containing a backslash" \
    'd="$(mktemp -d)"; mkdir -p "$d/back\slash"
escaped="$d/back\134slash"
setup_mountinfo "36 35 98:0 / $escaped rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/back\slash" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

run_test "mountpoint_has_fuse matches a target containing a newline" \
    'd="$(mktemp -d)"; mkdir -p "$d/line
break"
escaped="$d/line\012break"
setup_mountinfo "36 35 98:0 / $escaped rw - fuse.torrentfs torrentfs rw"
rc=0
mountpoint_has_fuse "$d/line
break" || rc=$?
rm -rf "$d"
[ "$rc" -eq 0 ]'

# --- mountpoint_has_fuse dead-mount fallback ---
# A dead (ENOTCONN) FUSE mount cannot be canonicalized: readlink -f fails even
# though the mount still appears in mountinfo. The exit-side detach depends on
# this probe seeing a stale mount it must detach, so when readlink fails the
# raw target (trailing slash stripped) is matched against mountinfo field 5.
# `readlink` is stubbed to fail; the fixture carries the raw absolute path.

run_test "mountpoint_has_fuse detects a dead mount when readlink -f fails" \
    'readlink() { return 1; }
mnt="/tmp/dead-mnt"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
if mountpoint_has_fuse "$mnt"; then exit 0; else exit 1; fi'

run_test "mountpoint_has_fuse strips a trailing slash in the dead-mount fallback" \
    'readlink() { return 1; }
mnt="/tmp/dead-mnt"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
if mountpoint_has_fuse "$mnt/"; then exit 0; else exit 1; fi'

run_test "mountpoint_has_fuse returns false when dead-mount raw target is absent" \
    'readlink() { return 1; }
setup_mountinfo "36 35 98:0 /mnt-inner /other rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
if mountpoint_has_fuse /tmp/absent; then exit 1; else exit 0; fi'

# --- start_torrentfs exit-101 integration ---
# start_torrentfs must refuse (exit 101) when a FUSE mount already exists at
# the mountpoint. recover_stale_mountpoint and flock are stubbed so the test
# exercises the detection branch; the mountpoint is a throwaway temp dir and
# the mountinfo source is injected via TORRENTFS_MOUNTINFO_PATH.

run_test "start_torrentfs exits 101 on FUSE conflict" \
    'mnt="$(mktemp -d)"
export TORRENTFS_MOUNTINFO_PATH="$mnt/mountinfo"
printf "%s\n" "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw" > "$TORRENTFS_MOUNTINFO_PATH"
recover_stale_mountpoint() { return 0; }
flock() { return 0; }
rc=0
( start_torrentfs "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 101'

run_test "start_torrentfs exits 101 when mountpoint is locked" \
    'mnt="$(mktemp -d)"
recover_stale_mountpoint() { return 0; }
flock() { return 1; }
rc=0
( start_torrentfs "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 101'

# --- start_torrentfs dispatch ---
# The two-stage bind mount (host visibility) needs real root in a non-userns
# container; everything else — rootless podman (userns root) and non-root
# --user runs — mounts directly. recover_stale_mountpoint/flock/
# mountpoint_has_fuse are stubbed to pass the guards, and the two dispatch
# targets record which one ran.

run_test "start_torrentfs dispatches to rootful for rootful root" \
    'mnt="$(mktemp -d)"
recover_stale_mountpoint() { return 0; }
flock() { return 0; }
mountpoint_has_fuse() { return 1; }
is_root() { return 0; }; is_rootless_podman() { return 1; }
start_torrentfs_rootful() { echo ROOTFUL > "$mnt/dispatch"; }
start_torrentfs_rootless() { echo ROOTLESS > "$mnt/dispatch"; }
start_torrentfs "$mnt"
grep -qx ROOTFUL "$mnt/dispatch" && ! grep -qx ROOTLESS "$mnt/dispatch"
rc=$?
rm -rf "$mnt"
# The status is exited explicitly: inside `( ... ) || result=$?` bash does not
# enable set -e, so a failing grep before the cleanup is otherwise swallowed.
exit "$rc"'

# Dispatch keys on the kernel uid_map alone, so this case has to supply that
# state.  It cannot enter a foreign namespace to get one: `unshare --user
# --map-root-user` run inside an existing user namespace (the CI job runs as a
# container step) keeps the inherited row (`0 0 1`) first and maps the caller on
# a later row, so the process is not "container root = this namespace's
# invoker" -- the previous version tested the runner's containment instead of
# the predicate.
#
# The state is therefore a fixture: the production pure predicate is fed a real
# uid_map body ("inside outside length"), and should_drop_privileges and the
# dispatch branch run unmodified.  A wrong predicate makes the rootful case
# below pass the wrong branch, so this pair cannot both be satisfied by a
# broken implementation.  The kernel-read layer is covered separately by
# `uid_map_is_rootless` data cases and `should_drop_privileges` cases, which run
# on any host; nothing here is skipped.
run_test "start_torrentfs dispatches to direct mount under rootless podman" \
    'mnt="$(mktemp -d)"
# The rootless shape: container UID 0 maps to the invoking host UID.  is_root
# is stubbed so the predicate alone decides: without it a non-root test host
# would take the rootless branch for the wrong reason.
is_root() { return 0; }
in_rootless_user_namespace() { uid_map_is_rootless "         0       1000          1"; }
recover_stale_mountpoint() { return 0; }
flock() { return 0; }
mountpoint_has_fuse() { return 1; }
start_torrentfs_rootful() { echo ROOTFUL > "$mnt/dispatch"; }
start_torrentfs_rootless() { echo ROOTLESS > "$mnt/dispatch"; }
start_torrentfs "$mnt"
rc=0
grep -qx ROOTLESS "$mnt/dispatch" && ! grep -qx ROOTFUL "$mnt/dispatch" || rc=1
[ "$rc" -eq 0 ] || echo "  DISPATCH -> [$(cat "$mnt/dispatch" 2>/dev/null)]" >&2
rm -rf "$mnt"
exit "$rc"'

# The nested-userns shape: the inherited row maps container 0 to outside 0, and
# only a later row names the caller.  UID 0 there is not the invoker, so the
# entrypoint must keep the drop (rootful branch).  This pins the multi-row
# semantics of uid_map_is_rootless; the data cases below cover the variants.
run_test "start_torrentfs keeps the drop for a map inherited from a parent namespace" \
    'mnt="$(mktemp -d)"
is_root() { return 0; }
in_rootless_user_namespace() { uid_map_is_rootless "         0          0          1
      1000       1000          1"; }
recover_stale_mountpoint() { return 0; }
flock() { return 0; }
mountpoint_has_fuse() { return 1; }
start_torrentfs_rootful() { echo ROOTFUL > "$mnt/dispatch"; }
start_torrentfs_rootless() { echo ROOTLESS > "$mnt/dispatch"; }
start_torrentfs "$mnt"
rc=0
grep -qx ROOTFUL "$mnt/dispatch" && ! grep -qx ROOTLESS "$mnt/dispatch" || rc=1
[ "$rc" -eq 0 ] || echo "  DISPATCH -> [$(cat "$mnt/dispatch" 2>/dev/null)]" >&2
rm -rf "$mnt"
exit "$rc"'


# --- start_torrentfs_rootless fail-fast ---
# Under rootless podman running as container root (is_root), a bind mount on
# the mountpoint is the ':shared' host-visibility recipe whose propagation
# cannot be honored — the entrypoint must exit 102 instead of silently mounting
# container-only. Non-root `--user` runs (Docker or rootless podman) bind-mount
# /mnt for writability and keep the warning + container-only path;
# bind-mount-free rootless runs also proceed. run_daemon/wait_for_fuse_mount are
# stubbed so the "proceeds" cases short-circuit without a real mount.

run_test "start_torrentfs_rootless fails fast on bind mount under rootless podman (root)" \
    'mnt="$(mktemp -d)"
is_root() { return 0; }
is_rootless_podman() { return 0; }
is_bind_mount() { return 0; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 102'

run_test "start_torrentfs_rootless warns but proceeds for non-root docker --user bind mount" \
    'mnt="$(mktemp -d)"
is_root() { return 1; }
is_rootless_podman() { return 1; }
is_bind_mount() { return 0; }
run_daemon() { :; }
wait_for_fuse_mount() { return 1; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 1'

run_test "start_torrentfs_rootless warns but proceeds for rootless podman --user bind mount" \
    'mnt="$(mktemp -d)"
is_root() { return 1; }
is_rootless_podman() { return 0; }
is_bind_mount() { return 0; }
run_daemon() { :; }
wait_for_fuse_mount() { return 1; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 1'

run_test "start_torrentfs_rootless proceeds without bind mount under rootless podman" \
    'mnt="$(mktemp -d)"
is_root() { return 0; }
is_rootless_podman() { return 0; }
is_bind_mount() { return 1; }
run_daemon() { :; }
wait_for_fuse_mount() { return 1; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 1'

run_test "start_torrentfs_rootless exits 103 when stale detach fails after clean exit" \
    'mnt="$(mktemp -d)"
is_bind_mount() { return 1; }
run_daemon() { exit 0; }
wait_for_fuse_mount() { return 0; }
force_unmount_fuse() { return 1; }
umount() { return 0; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
rm -rf "$mnt"
test "$rc" -eq 103'

# --- stop signals outside the steady state ---
# A stop request must work in every window. The trap only records it, so the
# recovery loop and the readiness wait unwind instead of restarting the daemon:
# a restarted daemon would inherit PID 1's SIG_IGN and later stops would be
# unanswerable until SIGKILL left a stale mount behind.

run_test "start_torrentfs_rootless exits cleanly on SIGTERM during the recovery backoff" \
    'mnt="$(mktemp -d)"
log="$(mktemp)"
is_bind_mount() { return 1; }
SESSION_RECOVERY_BACKOFF_SECS=5
# The fake daemon reports a severed session; the loop reaches its backoff.
run_daemon() { exit 104; }
wait_for_fuse_mount() { return 0; }
force_unmount_fuse() { return 0; }
umount() { return 0; }
( start_torrentfs_rootless "$mnt" ) 2>"$log" &
ep=$!
for _ in $(seq 1 50); do
    grep -q "restarting torrentfs" "$log" 2>/dev/null && break
    sleep 0.1
done
if ! grep -q "restarting torrentfs" "$log"; then
    kill -9 "$ep" 2>/dev/null || true
    wait "$ep" 2>/dev/null || true
    rm -rf "$mnt"; rm -f "$log"
    exit 1
fi
kill -TERM "$ep"
rc=0
wait "$ep" || rc=$?
rm -rf "$mnt"; rm -f "$log"
[ "$rc" -eq 0 ]'

run_test "start_torrentfs_rootless exits on SIGTERM during the startup wait" \
    'mnt="$(mktemp -d)"
log="$(mktemp)"
is_bind_mount() { return 1; }
run_daemon() { exec sleep 30; }
mountpoint_has_fuse() { return 1; }
force_unmount_fuse() { return 0; }
umount() { return 0; }
( start_torrentfs_rootless "$mnt" ) 2>"$log" &
ep=$!
sleep 0.5
kill -TERM "$ep"
rc=0
wait "$ep" || rc=$?
# 143 is the fake daemon status killed by the trap. The misleading
# "exited with code 127" path (waiting again for a pid the trap reaped) must
# not run, and the readiness wait must observe the flag — status 143 alone
# cannot tell a working trap from an entrypoint killed by the signal.
[ "$rc" -eq 143 ] \
    && grep -q "shutting down" "$log" \
    && grep -q "stop requested during startup" "$log" \
    && ! grep -q "exited with code 127" "$log"
rc=$?
rm -rf "$mnt"; rm -f "$log"
exit "$rc"'

run_test "start_torrentfs_rootless reports the daemon status under repeated SIGTERM" \
    'mnt="$(mktemp -d)"
log="$(mktemp)"
is_bind_mount() { return 1; }
# A daemon that holds its TERM for ~1s (like torrentfs draining), then exits 0,
# so the second signal lands while the trap is still collecting its status.
run_daemon() { exec bash -c "trap \"sleep 1; exit 0\" TERM; while :; do sleep 0.05; done"; }
wait_for_fuse_mount() { return 0; }
force_unmount_fuse() { return 0; }
umount() { return 0; }
( start_torrentfs_rootless "$mnt" ) 2>"$log" &
ep=$!
sleep 0.4
kill -TERM "$ep"
sleep 0.2
# The second signal must reach a still-running entrypoint: a failed kill means
# the first stop already finished and this test verified nothing.
if ! kill -TERM "$ep" 2>/dev/null; then
    wait "$ep" 2>/dev/null || true
    rm -rf "$mnt"; rm -f "$log"
    exit 1
fi
rc=0
wait "$ep" || rc=$?
rm -rf "$mnt"; rm -f "$log"
[ "$rc" -eq 0 ]'

run_test "start_torrentfs exits cleanly when a stop was recorded before startup" \
    'stopping=1
rc=0; ( start_torrentfs /nonexistent-mountpoint ) 2>/dev/null || rc=$?
[ "$rc" -eq 0 ]'

run_test "start_torrentfs_rootless keeps a stop recorded before startup" \
    'stopping=1
mnt="$(mktemp -d)"
calls="$(mktemp)"
run_daemon() { echo started >> "$calls"; }
rc=0
( start_torrentfs_rootless "$mnt" ) 2>/dev/null || rc=$?
ok=0
[ "$rc" -eq 0 ] && [ ! -s "$calls" ] && ok=1
rm -rf "$mnt"; rm -f "$calls"
[ "$ok" -eq 1 ]'

# --- rootful publish bind ---
# The stop trap runs at a command boundary, so the publish bind must re-check
# the flag on both sides: a stop that lands between the loop's check and the
# bind otherwise publishes an empty directory that no detach path recognizes.

run_test "start_torrentfs_rootful withdraws a bind published after a stop request" \
    'mnt="$(mktemp -d)"
calls="$(mktemp)"
should_drop_privileges() { return 1; }
mkdir() { :; }
run_daemon() { :; }
wait_for_fuse_mount() { return 0; }
force_unmount_fuse() { return 0; }
detach_publish_bind() { return 0; }
# The stop trap lands between the pre-bind check and the bind.
mount() { stopping=1; echo "mount $*" >> "$calls"; }
umount() { echo "umount $*" >> "$calls"; return 0; }
rc=0
( start_torrentfs_rootful "$mnt" ) 2>/dev/null || rc=$?
ok=0
grep -q "^mount --bind " "$calls" && grep -q "^umount $mnt$" "$calls" && [ "$rc" -eq 0 ] && ok=1
rm -rf "$mnt"; rm -f "$calls"
[ "$ok" -eq 1 ]'

# --- detach_publish_bind ---
# The publish bind is released with a direct unmount when possible and with the
# lazy form when a reader still holds the mount (the sustained-read case); the
# mountinfo probe verifies the result instead of assuming it.

run_test "detach_publish_bind lazy-detaches when the mount is busy" \
    'mnt="$(mktemp -d)"
mounted=1
mountpoint_has_fuse() { [ "$mounted" -eq 1 ]; }
umount_calls="$(mktemp)"
umount() {
    echo "$*" >> "$umount_calls"
    case "$1" in
        -l) mounted=0; return 0 ;;
        *) return 32 ;;
    esac
}
rc=0
detach_publish_bind "$mnt" 2>/dev/null || rc=$?
grep -q -- "-l $mnt$" "$umount_calls" && [ "$rc" -eq 0 ]
rc=$?
rm -rf "$mnt"; rm -f "$umount_calls"
exit "$rc"'

run_test "detach_publish_bind uses a direct unmount when nothing holds the mount" \
    'mnt="$(mktemp -d)"
mounted=1
mountpoint_has_fuse() { [ "$mounted" -eq 1 ]; }
umount_calls="$(mktemp)"
umount() { echo "$*" >> "$umount_calls"; mounted=0; return 0; }
detach_publish_bind "$mnt" 2>/dev/null
[ "$(cat "$umount_calls")" = "$mnt" ]
rc=$?
rm -rf "$mnt"; rm -f "$umount_calls"
exit "$rc"'

run_test "detach_publish_bind reports a mount that survives the lazy detach" \
    'mnt="$(mktemp -d)"
mountpoint_has_fuse() { return 0; }
umount() { return 1; }
rc=0
detach_publish_bind "$mnt" 2>/dev/null || rc=$?
rm -rf "$mnt"
[ "$rc" -ne 0 ]'

run_test "detach_publish_bind no-ops when no FUSE mount is published" \
    'mnt="$(mktemp -d)"
mountpoint_has_fuse() { return 1; }
called=""
umount() { called="umount $*"; return 1; }
detach_publish_bind "$mnt" 2>/dev/null
rm -rf "$mnt"
[ -z "$called" ]'

# --- withdraw_publish_bind ---
# The stop trap may already have detached the publish layer; only a target that
# still resolves to the internal directory means the bind is still ours.

run_test "withdraw_publish_bind unmounts the layer and reports success" \
    'mnt="$(mktemp -d)"; internal="$(mktemp -d)"
calls="$(mktemp)"
umount() { echo "$*" >> "$calls"; return 0; }
withdraw_publish_bind "$mnt" "$internal" && grep -qx "$mnt" "$calls"
rc=$?
rm -rf "$mnt" "$internal"; rm -f "$calls"
exit "$rc"'

run_test "withdraw_publish_bind accepts a layer the stop trap already detached" \
    'mnt="$(mktemp -d)"; internal="$(mktemp -d)"
umount() { return 1; }
withdraw_publish_bind "$mnt" "$internal"
rc=$?
rm -rf "$mnt" "$internal"
exit "$rc"'

run_test "withdraw_publish_bind reports a bind that is still published" \
    'mnt="$(mktemp -d)"; internal="$(mktemp -d)"
umount() { return 1; }
# A live bind makes the target stat to the internal directory inode.
rmdir "$mnt"
ln -s "$internal" "$mnt"
rc=0
withdraw_publish_bind "$mnt" "$internal" || rc=$?
rm -f "$mnt"; rm -rf "$internal"
[ "$rc" -ne 0 ]'

# --- mountpoint_enotconn ---
# setup_stat defines a fake `stat` function (a shell builtin here, so a PATH
# stub would never be consulted) with the given exit code and stderr text.

run_test "mountpoint_enotconn true on ENOTCONN stderr" \
    'setup_stat 1 "stat: cannot stat '\''/mnt'\'': Transport endpoint is not connected"; if mountpoint_enotconn /mnt; then exit 0; else exit 1; fi'

run_test "mountpoint_enotconn false when stat succeeds" \
    'setup_stat 0 ""; if mountpoint_enotconn /mnt; then exit 1; else exit 0; fi'

run_test "mountpoint_enotconn false on ENOENT" \
    'setup_stat 1 "stat: cannot stat '\''/mnt'\'': No such file or directory"; if mountpoint_enotconn /mnt; then exit 1; else exit 0; fi'

# --- force_unmount_fuse ---
# The exit-side stale-mount detach: after the daemon exits, if a FUSE mount is
# still present at the target, detach it via fusermount3 → fusermount → umount -l.
# `setup_mountinfo` points mountpoint_has_fuse at a fixture; the detach commands
# are shell-function stubs so no real fusermount/umount runs on the test host.

run_test "force_unmount_fuse no-ops when nothing is mounted" \
    'mnt="$(mktemp -d)"
setup_mountinfo ""
called=""
fusermount3() { called="fusermount3"; }
fusermount() { called="fusermount"; }
umount() { called="umount"; }
force_unmount_fuse "$mnt" 2>/dev/null
rm -rf "$mnt"
[ -z "$called" ]'

run_test "force_unmount_fuse detaches via fusermount3 when a FUSE mount lingers" \
    'mnt="$(mktemp -d)"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
fusermount3_called=""
fusermount3() { fusermount3_called="$*"; return 0; }
fusermount() { return 99; }
umount() { return 99; }
force_unmount_fuse "$mnt" 2>/dev/null
rm -rf "$mnt"
[ "$fusermount3_called" = "-u -q -z -- $mnt" ]'

run_test "force_unmount_fuse falls back to fusermount when fusermount3 fails" \
    'mnt="$(mktemp -d)"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
fusermount3() { return 1; }
fusermount_called=""
fusermount() { fusermount_called="$*"; return 0; }
umount() { return 99; }
force_unmount_fuse "$mnt" 2>/dev/null
rm -rf "$mnt"
[ "$fusermount_called" = "-u -q -z -- $mnt" ]'

run_test "force_unmount_fuse falls back to umount -l when both helpers fail" \
    'mnt="$(mktemp -d)"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
fusermount3() { return 1; }
fusermount() { return 1; }
umount_called=""
umount() { umount_called="$*"; return 0; }
force_unmount_fuse "$mnt" 2>/dev/null
rm -rf "$mnt"
[ "$umount_called" = "-l $mnt" ]'

run_test "force_unmount_fuse returns non-zero when every detach attempt fails" \
    'mnt="$(mktemp -d)"
setup_mountinfo "36 35 98:0 /mnt-inner $mnt rw shared:1 master:2 - fuse.torrentfs torrentfs rw"
fusermount3() { return 1; }
fusermount() { return 1; }
umount() { return 1; }
rc=0
force_unmount_fuse "$mnt" 2>/dev/null || rc=$?
rm -rf "$mnt"
[ "$rc" -ne 0 ]'

# --- fuse_device_exists (just verify it doesn't crash) ---

run_test "fuse_device_exists does not crash" \
    'fuse_device_exists 2>/dev/null || true; exit 0'

# --- fix_state_dir_ownership ---
# Re-homes the dedicated state tree (default XDG dir, or the daemon owner's
# home) to the daemon user when a previous container run left any part of it
# owned by a different UID.  The ownership probe walks *recursively* via
# `find`, so a correct top-level directory with a leftover nobody:nogroup file
# underneath still triggers the re-home; the recursive pass itself stays on one
# filesystem (`-xdev`).  --cache and --db are never walked recursively: only
# their leaves are handed over (prepare_state_paths / fix_db_file_ownership), so
# a `--db /var/lib/x.db` cannot re-own /var/lib.  is_root, find, and chown are
# stubbed (except the real-`find` tests), and daemon_uid/daemon_gid/daemon_home
# are set directly (resolve_daemon_ids is covered separately) so no real
# uid/chown runs on the test host.

run_test "fix_state_dir_ownership skips when not root" \
    'is_root() { return 1; }
called=""
chown() { called="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership
rm -rf "$data_home"
[ -z "$called" ]'

run_test "fix_state_dir_ownership skips when state dir is missing" \
    'is_root() { return 0; }
daemon_uid=0; daemon_gid=0
called=""
chown() { called="$*"; }
data_home="$(mktemp -d)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership
rm -rf "$data_home"
[ -z "$called" ]'

run_test "fix_state_dir_ownership re-homes a mismatched tree entry by entry" \
    'is_root() { return 0; }
daemon_uid=0; daemon_gid=0
stub_find_mismatch
chown_args=""
chown() { chown_args="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
warn_file="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>"$warn_file"
[ "$chown_args" = "-h 0:0 -- /mismatch" ] \
    && grep -q "WARNING" "$warn_file" \
    && grep -q "$data_home/torrentfs" "$warn_file" \
    && grep -q "re-homing to 0:0" "$warn_file"
rc=$?
rm -rf "$data_home"
rm -f "$warn_file"
exit "$rc"'

run_test "fix_state_dir_ownership warns before it chowns" \
    'is_root() { return 0; }
daemon_uid=1000; daemon_gid=1000
stub_find_mismatch
chown() { echo "CHOWN $*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
log="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership >"$log" 2>&1
rm -rf "$data_home"
warn_line="$(grep -m1 -n "WARNING" "$log" | cut -d: -f1)"
chown_line="$(grep -m1 -n "CHOWN" "$log" | cut -d: -f1)"
rm -f "$log"
[ -n "$warn_line" ] && [ -n "$chown_line" ] && [ "$warn_line" -lt "$chown_line" ]'

run_test "fix_state_dir_ownership warns once and names only the state tree" \
    'is_root() { return 0; }
daemon_uid=1000; daemon_gid=1000
find() { echo "/mismatch"; }
chown() { :; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs" "$data_home/cache"
cache_arg="$data_home/cache"
warn_file="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>"$warn_file"
[ "$(grep -c "WARNING" "$warn_file")" -eq 1 ] \
    && grep -q "$data_home/torrentfs" "$warn_file" \
    && ! grep -q "  - $data_home/cache " "$warn_file"
rc=$?
rm -rf "$data_home"
rm -f "$warn_file"
exit "$rc"'

run_test "fix_state_dir_ownership walks the state tree once for a --db inside it" \
    'is_root() { return 0; }
daemon_uid=1000; daemon_gid=1000
stub_find_mismatch
calls="$(mktemp)"
chown() { echo "$*" >> "$calls"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
: > "$data_home/torrentfs/metadata.db"
db_arg="$data_home/torrentfs/metadata.db"
warn_file="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>"$warn_file"
[ "$(grep -c -- "-h 1000:1000 -- /mismatch$" "$calls")" -eq 1 ] \
    && [ "$(grep -c -- "-h 1000:1000 -- $data_home/torrentfs/metadata.db$" "$calls")" -eq 1 ] \
    && [ "$(grep -c "  - $data_home/torrentfs " "$warn_file")" -eq 1 ]
rc=$?
rm -rf "$data_home"
rm -f "$calls" "$warn_file"
exit "$rc"'

run_test "fix_state_dir_ownership chowns cache and db leaves without recursing" \
    'is_root() { return 0; }
daemon_uid=1000; daemon_gid=1000
find() { :; }
calls="$(mktemp)"
chown() { echo "$*" >> "$calls"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/cache" "$data_home/db"
: > "$data_home/db/metadata.db"
: > "$data_home/db/metadata.db-wal"
cache_arg="$data_home/cache"
db_arg="$data_home/db/metadata.db"
XDG_DATA_HOME="$data_home/nonexistent" fix_state_dir_ownership 2>/dev/null
[ "$(wc -l < "$calls")" -eq 4 ] \
    && grep -q -- "1000:1000 -- $data_home/cache$" "$calls" \
    && grep -q -- "1000:1000 -- $data_home/db$" "$calls" \
    && grep -q -- "-h 1000:1000 -- $data_home/db/metadata.db$" "$calls" \
    && grep -q -- "-h 1000:1000 -- $data_home/db/metadata.db-wal$" "$calls" \
    && ! grep -q -- "-R" "$calls"
rc=$?
rm -rf "$data_home"
rm -f "$calls"
exit "$rc"'

run_test "fix_state_dir_ownership skips chown when find detects no mismatch" \
    'is_root() { return 0; }
daemon_uid=0; daemon_gid=0
find() { :; }
called=""
chown() { called="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
warn_file="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>"$warn_file"
rm -rf "$data_home"
[ -z "$called" ] && [ ! -s "$warn_file" ]
rc=$?
rm -f "$warn_file"
exit "$rc"'

run_test "fix_state_dir_ownership warns instead of chowning when the walk fails" \
    'is_root() { return 0; }
daemon_uid=0; daemon_gid=0
find() { return 1; }
called=""
chown() { called="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
warn_file="$(mktemp)"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>"$warn_file"
[ -z "$called" ] \
    && grep -q "WARNING" "$warn_file" \
    && grep -q "could not walk $data_home/torrentfs" "$warn_file"
rc=$?
rm -rf "$data_home"
rm -f "$warn_file"
exit "$rc"'

run_test "fix_state_dir_ownership detects nested ownership mismatch via real find" \
    'is_root() { return 0; }
daemon_uid=12345; daemon_gid=12345
calls="$(mktemp)"
chown() { echo "$*" >> "$calls"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs/sub"
touch "$data_home/torrentfs/sub/cache_metadata.txt"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership 2>/dev/null
[ "$(grep -c -- "-h 12345:12345 -- $data_home/torrentfs$" "$calls")" -eq 1 ] \
    && grep -q -- "-h 12345:12345 -- $data_home/torrentfs/sub$" "$calls" \
    && grep -q -- "-h 12345:12345 -- $data_home/torrentfs/sub/cache_metadata.txt$" "$calls"
rc=$?
rm -rf "$data_home"
rm -f "$calls"
exit "$rc"'

run_test "fix_state_dir_ownership skips chown on a consistent tree (real find)" \
    'is_root() { return 0; }
daemon_uid="$(id -u)"; daemon_gid="$(id -g)"
called=""
chown() { called="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs/sub"
touch "$data_home/torrentfs/sub/cache_metadata.txt"
XDG_DATA_HOME="$data_home" fix_state_dir_ownership
rm -rf "$data_home"
[ -z "$called" ]'

run_test "fix_state_dir_ownership respects daemon_home when XDG unset" \
    'is_root() { return 0; }
daemon_uid=0; daemon_gid=0
stub_find_mismatch
chown_args=""
chown() { chown_args="$*"; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/.local/share/torrentfs"
unset XDG_DATA_HOME
daemon_home="$data_home" fix_state_dir_ownership
rc=0
[ "$chown_args" = "-h 0:0 -- /mismatch" ] || rc=1
rm -rf "$data_home"
exit "$rc"'

# --- normalize_state_path / is_system_state_path ---
# A state path is canonicalized before any chown, so `..` forms cannot smuggle a
# system directory past the guard; the guard only refuses the exact system
# directory, leaving a dedicated subdirectory (e.g. /var/lib/torrentfs) usable.

run_test "is_system_state_path refuses the root and system directories" \
    'for p in / /var /var/lib /etc /usr /tmp; do is_system_state_path "$p" || exit 1; done
if is_system_state_path /var/lib/torrentfs; then exit 1; fi
if is_system_state_path /state/cache; then exit 1; fi'

run_test "normalize_state_path rejects a --cache that resolves to the root" \
    'rc=0; ( normalize_state_path --cache /a/.. ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "normalize_state_path rejects a --cache that resolves to a system directory" \
    'rc=0; ( normalize_state_path --cache /var/lib ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "normalize_state_path accepts a dedicated state subdirectory" \
    'normalize_state_path --cache /var/lib/torrentfs/cache \
        && [ "$normalized_path" = /var/lib/torrentfs/cache ]'

run_test "normalize_state_path canonicalizes dot-dot forms" \
    'normalize_state_path --db /var/lib/torrentfs/../torrentfs/metadata.db \
        && [ "$normalized_path" = /var/lib/torrentfs/metadata.db ]'

run_test "is_system_state_path refuses canonical usrmerged system directories" \
    'for p in /usr/bin /usr/sbin /usr/lib /usr/lib64 /usr/libexec /usr/share /usr/src /usr/include; do
    is_system_state_path "$p" || exit 1
done
if is_system_state_path /usr/bin/torrentfs; then exit 1; fi'

run_test "normalize_state_path rejects a usrmerged alias of a system directory" \
    'for p in /bin /sbin /lib /lib64; do
    rc=0; ( normalize_state_path --cache "$p" ) 2>/dev/null || rc=$?
    [ "$rc" -eq 2 ] || exit 1
done
# The canonical form the alias resolves to must be refused as well.
rc=0; ( normalize_state_path --cache /usr/bin ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ]'

run_test "normalize_state_path rejects a relative path as a usage error" \
    'rc=0; ( normalize_state_path --db metadata.db ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ]'

# --- rehome_tree / fix_db_file_ownership ---
# The recursive pass stays on one filesystem (-xdev) and only re-owns foreign
# entries; the DB re-home touches the file and its SQLite sidecars, nothing else.

run_test "rehome_tree confines its walk to one filesystem" \
    'daemon_uid=12345; daemon_gid=12345
find_calls="$(mktemp)"
find() { echo "$*" >> "$find_calls"; :; }
chown() { :; }
data_home="$(mktemp -d)"
mkdir -p "$data_home/torrentfs"
rehome_tree "$data_home/torrentfs"
rc=0
[ -s "$find_calls" ] || rc=1
while IFS= read -r call; do
    case "$call" in
        *"$data_home/torrentfs -xdev"*) : ;;
        *) rc=1 ;;
    esac
done < "$find_calls"
rm -rf "$data_home"
rm -f "$find_calls"
exit "$rc"'

run_test "probe_foreign_ownership confines its walk to one filesystem" \
    'daemon_uid=12345; daemon_gid=12345
find_calls="$(mktemp)"
find() { echo "$*" >> "$find_calls"; }
probe_foreign_ownership /state/torrentfs >/dev/null
rc=0
grep -q -- "/state/torrentfs -xdev" "$find_calls" || rc=1
rm -f "$find_calls"
exit "$rc"'

run_test "fix_db_file_ownership chowns the file and sidecars without recursing" \
    'daemon_uid=1000; daemon_gid=1000
calls="$(mktemp)"
chown() { echo "$*" >> "$calls"; }
state="$(mktemp -d)"
db="$state/metadata.db"
: > "$db"
: > "$db-wal"
: > "$db-shm"
fix_db_file_ownership "$db"
[ "$(wc -l < "$calls")" -eq 3 ] \
    && grep -q -- "-h 1000:1000 -- $db$" "$calls" \
    && grep -q -- "-h 1000:1000 -- $db-wal$" "$calls" \
    && grep -q -- "-h 1000:1000 -- $db-shm$" "$calls" \
    && ! grep -q -- "-R" "$calls"
rc=$?
rm -rf "$state"
rm -f "$calls"
exit "$rc"'

run_test "fix_db_file_ownership skips files that do not exist" \
    'daemon_uid=1000; daemon_gid=1000
called=""
chown() { called="$*"; }
state="$(mktemp -d)"
fix_db_file_ownership "$state/absent.db"
rc=$?
rm -rf "$state"
[ -z "$called" ] && [ "$rc" -eq 0 ]'

# --- prepare_log_file_parent ---
# --log-file's parent must exist and be daemon-owned before the setpriv drop.
# A relative path (bare filename or subdir) resolves against the root-owned
# WORKDIR and is rejected; an absolute path gets its parent mkdir -p'd and the
# leaf directory chown'd (never -R, never the container root).

run_test "prepare_log_file_parent rejects a relative --log-file" \
    'rc=0; ( prepare_log_file_parent "logs/torrentfs.log" ) 2>/dev/null || rc=$?; [ "$rc" -eq 1 ]'

# The README states these statuses: --cache/--db are 2 (C-10.9), --log-file is
# 1.  Pin them here so a doc/code drift shows up as a failure.
run_test "validate_state_paths exits 2 for a relative --cache and --db" \
    'rc=0; ( cache_arg=rel; db_arg=""; log_file_arg="" ; validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ] || exit 1
rc=0; ( cache_arg=""; db_arg="rel.db"; log_file_arg="" ; validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ]'

# The two --log-file inputs below are not duplicates: "/torrentfs.log" has
# parent "/", an exact SYSTEM_STATE_PATHS entry, and is rejected by name;
# "/var/lib/torrentfs.log" has parent "/var/lib", also an exact entry, and is
# accepted -- this check judges the parent shape only, not membership in the
# state-block list.  Feeding the list entry itself is what makes the pair
# falsifiable: a parent-based rejection added to validate_log_file_path fails
# the second assertion, while the first would still pass.  (A path merely
# *under* a system directory, e.g. /var/log/torrentfs/x.log, exercises neither
# rule: /var/log/torrentfs is not a list entry, so it cannot tell them apart.)
run_test "validate_state_paths judges --log-file by its parent shape, not by the state-path list" \
    'rc=0; ( cache_arg=""; db_arg=""; log_file_arg="/torrentfs.log" ; validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 1 ] || exit 1
rc=0; ( cache_arg=""; db_arg=""; log_file_arg="/var/lib/torrentfs.log" ; validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 0 ]'


run_test "prepare_log_file_parent rejects a root-parent --log-file" \
    'rc=0; ( prepare_log_file_parent "/torrentfs.log" ) 2>/dev/null || rc=$?; [ "$rc" -eq 1 ]'

run_test "prepare_log_file_parent mkdir -p's and chowns only the leaf directory" \
    'daemon_uid=1000; daemon_gid=1000
mkdir_calls=""; chown_calls=""
mkdir() { mkdir_calls="$*"; }
chown() { chown_calls="$*"; }
prepare_log_file_parent "/logs/sub/torrentfs.log"
[ "$mkdir_calls" = "-p /logs/sub" ] && [ "$chown_calls" = "1000:1000 /logs/sub" ]'

run_test "prepare_log_file_parent warns when chown fails" \
    'daemon_uid=1000; daemon_gid=1000
mkdir() { return 0; }
chown() { return 1; }
warn_file="$(mktemp)"
prepare_log_file_parent "/logs/torrentfs.log" 2>"$warn_file"
grep -q "could not chown log directory" "$warn_file"
rc=$?
rm -f "$warn_file"
exit "$rc"'

run_test "prepare_log_file_parent normalizes // and rejects ..-traversal to root" \
    'daemon_uid=1000; daemon_gid=1000
mkdir() { :; }
chown_calls=""
chown() { chown_calls="$*"; }
# `//` collapses to a single slash: the leaf is chowned at /logs, not //logs.
prepare_log_file_parent "//logs/x.log"
[ "$chown_calls" = "1000:1000 /logs" ] || exit 1
# `..` traversal normalizes to a `/` parent, which the guard must reject.
rc=0
( prepare_log_file_parent "/logs/../x" ) 2>/dev/null || rc=$?
[ "$rc" -eq 1 ]'

# --- prepare_state_paths / create_daemon_dir ---
# The post-drop daemon creates the --cache directory and the --db file itself,
# so a path that does not exist yet under a root-owned mount must be mkdir -p'd
# as root and handed to the daemon user — the failure mode is
# `docker run -v /host/state:/state … --cache /state/cache` exiting at startup.

run_test "create_daemon_dir mkdir -p's the tree and chowns only the leaf" \
    'daemon_uid=1000; daemon_gid=1000
mkdir_calls=""; chown_calls=""
mkdir() { mkdir_calls="$*"; }
chown() { chown_calls="$*"; }
create_daemon_dir "/state/cache" "--cache"
[ "$mkdir_calls" = "-p /state/cache" ] && [ "$chown_calls" = "1000:1000 -- /state/cache" ]'

run_test "create_daemon_dir refuses the root, a relative path and system dirs" \
    'daemon_uid=1000; daemon_gid=1000
# The stubs write to a file, not to shell variables: create_daemon_dir runs in
# a subshell for the exit code, so a variable set there never reaches us.
calls="$(mktemp)"
chown() { echo "chown $*" >> "$calls"; }
mkdir() { echo "mkdir $*" >> "$calls"; }
for target in / . "/state/.." /var/lib /usr/bin; do
    rc=0
    ( create_daemon_dir "$target" "--cache" ) 2>/dev/null || rc=$?
    [ "$rc" -eq 2 ] || { rm -f "$calls"; exit 1; }
done
if [ -s "$calls" ]; then
    rm -f "$calls"
    exit 1
fi
rm -f "$calls"'

run_test "create_daemon_dir fails when the directory cannot be created" \
    'daemon_uid=1000; daemon_gid=1000
mkdir() { return 1; }
rc=0; ( create_daemon_dir "/state/cache" "--cache" ) 2>/dev/null || rc=$?
[ "$rc" -eq 1 ]'

run_test "create_daemon_dir warns when chown fails" \
    'daemon_uid=1000; daemon_gid=1000
mkdir() { return 0; }
chown() { return 1; }
warn_file="$(mktemp)"
create_daemon_dir "/state/cache" "--cache" 2>"$warn_file"
grep -q "could not chown --cache directory" "$warn_file"
rc=$?
rm -f "$warn_file"
exit "$rc"'

run_test "prepare_state_paths creates and chowns the --cache dir and --db parent" \
    'daemon_uid="$(id -u)"; daemon_gid="$(id -g)"
state_dir="$(mktemp -d)"
cache_arg="$state_dir/cache"
db_arg="$state_dir/db/metadata.db"
prepare_state_paths
[ -d "$cache_arg" ] && [ -d "$state_dir/db" ] || exit 1
[ "$(stat -c %u:%g "$cache_arg")" = "$daemon_uid:$daemon_gid" ] || exit 1
[ "$(stat -c %u:%g "$state_dir/db")" = "$daemon_uid:$daemon_gid" ] || exit 1
rm -rf "$state_dir"'

run_test "prepare_state_paths rejects a relative --cache/--db with exit 2" \
    'daemon_uid=1000; daemon_gid=1000
for which in cache db; do
    cache_arg=""; db_arg=""
    if [ "$which" = cache ]; then cache_arg="cache"; else db_arg="metadata.db"; fi
    rc=0
    ( prepare_state_paths ) 2>/dev/null || rc=$?
    [ "$rc" -eq 2 ] || exit 1
done'

run_test "prepare_state_paths is a no-op without --cache/--db" \
    'cache_arg=""; db_arg=""
create_daemon_dir() { echo "create_daemon_dir called without a state path" >&2; return 1; }
prepare_state_paths'

# --- validate_state_paths ---
# The path guards are usage errors, not ownership work: they must fire for every
# run, including `--user <uid>` runs where fix_state_dir_ownership (the chown
# branch) returns early.  A regression that puts them back under is_root makes
# the first three cases fail.

run_test "validate_state_paths rejects a relative --cache as non-root" \
    'is_root() { return 1; }
cache_arg="cache"; db_arg=""; log_file_arg=""
rc=0
( validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ]'

run_test "validate_state_paths rejects a --db whose path resolves to a system directory" \
    'is_root() { return 1; }
cache_arg=""; db_arg="/state/../var/lib"; log_file_arg=""
rc=0
( validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 2 ]'

run_test "validate_state_paths rejects a relative --log-file as non-root" \
    'is_root() { return 1; }
cache_arg=""; db_arg=""; log_file_arg="logs/torrentfs.log"
rc=0
( validate_state_paths ) 2>/dev/null || rc=$?
[ "$rc" -eq 1 ]'

run_test "validate_state_paths accepts absolute paths without creating them" \
    'is_root() { return 1; }
state_dir="$(mktemp -d)"
cache_arg="$state_dir/cache"; db_arg="$state_dir/db/metadata.db"
log_file_arg="$state_dir/logs/torrentfs.log"
validate_state_paths
rc=$?
[ "$rc" -eq 0 ] && [ ! -d "$cache_arg" ] && [ ! -d "$state_dir/db" ] && [ ! -d "$state_dir/logs" ]
rc=$?
rm -rf "$state_dir"
exit "$rc"'

run_test "validate_state_paths accepts a dedicated subdirectory of a system tree" \
    'is_root() { return 1; }
cache_arg="/var/lib/torrentfs/cache"; db_arg=""; log_file_arg=""
validate_state_paths'

run_test "fix_state_dir_ownership prepares a --cache dir that does not exist yet" \
    'is_root() { return 0; }
daemon_uid="$(id -u)"; daemon_gid="$(id -g)"
state_dir="$(mktemp -d)"
cache_arg="$state_dir/cache"
XDG_DATA_HOME="$state_dir/nonexistent" fix_state_dir_ownership
[ -d "$cache_arg" ] && [ "$(stat -c %u:%g "$cache_arg")" = "$daemon_uid:$daemon_gid" ]
rc=$?
rm -rf "$state_dir"
exit "$rc"'

# --- should_drop_privileges / resolve_daemon_ids / run_daemon ---
# The privilege-drop decision: drop only when root AND not in a user namespace
# (rootless podman or rootless docker, where container UID 0 already maps to the
# invoking host user).

run_test "should_drop_privileges true as root outside a rootless container" \
    'is_root() { return 0; }; in_rootless_user_namespace() { return 1; }; should_drop_privileges'

run_test "should_drop_privileges false as root in a rootless container" \
    'is_root() { return 0; }; in_rootless_user_namespace() { return 0; }; if should_drop_privileges; then exit 1; else exit 0; fi'

# The mapping shapes, as data.  These are the production predicate's own
# inputs: a fixture here is a uid_map body, not a stubbed decision.
run_test "uid_map_is_rootless accepts a rootless container map" \
    'uid_map_is_rootless "         0       1000          1"'

run_test "uid_map_is_rootless accepts a rootless map with several rows" \
    'uid_map_is_rootless "         0       1000          1
      1000       1001        999"'

run_test "uid_map_is_rootless rejects the identity map of a rootful container" \
    'if uid_map_is_rootless "         0          0 4294967295"; then exit 1; fi'

run_test "uid_map_is_rootless rejects a map whose uid 0 is not the inside id" \
    'if uid_map_is_rootless "      1000       1000          1"; then exit 1; fi'

run_test "uid_map_is_rootless rejects an empty map and a truncated row" \
    'if uid_map_is_rootless ""; then exit 1; fi
if uid_map_is_rootless "         0       1000"; then exit 1; fi'

# The multi-row shapes.  A rootless container's map has the root row plus the
# subuid range; a namespace nested inside another one inherits `0 0 1` first and
# maps the caller on a later row, and UID 0 there is not the caller -- so that
# shape must stay rootful.
run_test "uid_map_is_rootless accepts a root row followed by a subuid range" \
    'uid_map_is_rootless "         0       1000          1
         1     100000      65536"'

run_test "uid_map_is_rootless rejects an inherited root row above the caller row" \
    'map="         0          0          1
      1000       1000          1"
if uid_map_is_rootless "$map"; then exit 1; fi'

run_test "uid_map_is_rootless answers rootful when the map has no root row" \
    'if uid_map_is_rootless "      1000       1000          1
      2000       2000          1"; then exit 1; fi'

run_test "uid_map_is_rootless finds the root row when it is not first" \
    'uid_map_is_rootless "      1000       1000          1
         0       1000          1"'

run_test "uid_map_is_rootless decides on the root row alone, not on a caller row" \
    'uid_map_is_rootless "         0       1000          1
      1000       1000          1"'

run_test "uid_map_is_rootless compares the outside id numerically" \
    'if uid_map_is_rootless "         0         00          1"; then exit 1; fi
uid_map_is_rootless "         0        010          1"'

run_test "uid_map_is_rootless rejects a malformed row instead of guessing" \
    'if uid_map_is_rootless "         0        abc          1"; then exit 1; fi
if uid_map_is_rootless "         0       1000          x1"; then exit 1; fi'

# The reader feeds the kernel map into the pure predicate.  Comparing against
# the same predicate on the live map fails if the reader stops consulting
# /proc/self/uid_map (or reads a caller-controlled path) and passes instead.
run_test "in_rootless_user_namespace agrees with the kernel map it reads" \
    'live="$(cat /proc/self/uid_map)"
if uid_map_is_rootless "$live"; then
    in_rootless_user_namespace
else
    if in_rootless_user_namespace; then exit 1; fi
fi'

run_test "in_rootless_user_namespace ignores a caller-supplied map path" \
    'map="$(mktemp)"
printf "%s\n" "         0       1000          1" > "$map"
live="$(cat /proc/self/uid_map)"
got=0
TORRENTFS_UID_MAP_PATH="$map" in_rootless_user_namespace || got=1
want=1
uid_map_is_rootless "$live" && want=0
rm -f "$map"
[ "$got" -eq "$want" ]'

run_test "should_drop_privileges false as non-root" \
    'is_root() { return 1; }; is_rootless_podman() { return 1; }; if should_drop_privileges; then exit 1; else exit 0; fi'

run_test "resolve_daemon_ids picks torrentfs user when dropping" \
    'is_root() { return 0; }; is_rootless_podman() { return 1; }
resolve_daemon_ids
[ "$daemon_uid" = 1000 ] && [ "$daemon_gid" = 1000 ] && [ "$daemon_home" = /home/torrentfs ]'

run_test "resolve_daemon_ids keeps current user when not dropping" \
    'is_root() { return 1; }; is_rootless_podman() { return 0; }
id() { case "$1" in -u) echo 4242 ;; -g) echo 4343 ;; esac; }
HOME=/some/home resolve_daemon_ids
[ "$daemon_uid" = 4242 ] && [ "$daemon_gid" = 4343 ] && [ "$daemon_home" = /some/home ]'

run_test "resolve_daemon_ids falls back to / when HOME is unset" \
    'is_root() { return 1; }; is_rootless_podman() { return 0; }
id() { case "$1" in -u) echo 4242 ;; -g) echo 4343 ;; esac; }
unset HOME
resolve_daemon_ids
[ "$daemon_uid" = 4242 ] && [ "$daemon_gid" = 4343 ] && [ "$daemon_home" = / ]'

run_test "fix_state_dir_ownership refuses when daemon identity is unresolved" \
    'is_root() { return 0; }
unset daemon_uid daemon_gid
called=""
chown() { called="$*"; }
d="$(mktemp -d)"
mkdir -p "$d/torrentfs"
rc=0
( XDG_DATA_HOME="$d" fix_state_dir_ownership ) 2>/dev/null || rc=$?
rm -rf "$d"
[ "$rc" -ne 0 ] && [ -z "$called" ]'

run_test "run_daemon drops to torrentfs via setpriv" \
    'should_drop_privileges() { return 0; }
daemon_uid=1000; daemon_gid=1000; daemon_home=/home/torrentfs
install_fake_setpriv
out="$( ( run_daemon torrentfs /mnt --db /db ) 2>/dev/null )"
printf "%s\n" "$out" | grep -q -- "--reuid=1000"
printf "%s\n" "$out" | grep -q -- "--regid=1000"
printf "%s\n" "$out" | grep -q -- "--clear-groups"
printf "%s\n" "$out" | grep -q -- "HOME=/home/torrentfs"'

# --- wait_for_fuse_mount ---
# The helper polls `mountpoint_has_fuse` and watches torrentfs (via `kill -0`
# / `wait`). Mock all three so the 30s deadline never actually elapses:
# `mountpoint_has_fuse` controls readiness, `kill -0` controls liveness, `wait`
# supplies the exit code. `kill`/`wait` are shell builtins here, so shell
# functions shadow them.

run_test "wait_for_fuse_mount returns 0 when FUSE mount becomes ready" \
    'mountpoint_has_fuse() { return 0; }; if wait_for_fuse_mount 99999 /mnt 2>/dev/null; then exit 0; else exit 1; fi'

run_test "wait_for_fuse_mount propagates torrentfs exit code on premature exit" \
    'mountpoint_has_fuse() { return 1; }; kill() { return 1; }; wait() { return 7; }; rc=0; wait_for_fuse_mount 99999 /mnt 2>/dev/null || rc=$?; [ "$rc" -eq 7 ]'

run_test "wait_for_fuse_mount treats clean exit without mount as failure (rc 1)" \
    'mountpoint_has_fuse() { return 1; }; kill() { return 1; }; wait() { return 0; }; rc=0; wait_for_fuse_mount 99999 /mnt 2>/dev/null || rc=$?; [ "$rc" -eq 1 ]'

# --- should_recover_session ---
# Only a severed session (the daemon's EXIT_SESSION_SEVERED) is recoverable, and
# only while the recovery budget lasts. A detached mount must stay final: the
# entrypoint must not resurrect a filesystem the operator unmounted.

run_test "should_recover_session restarts a severed session" \
    'should_recover_session "$DAEMON_EXIT_SESSION_SEVERED" 0'

run_test "should_recover_session allows the last recovery in the budget" \
    'should_recover_session "$DAEMON_EXIT_SESSION_SEVERED" "$((MAX_SESSION_RECOVERIES - 1))"'

run_test "should_recover_session stops once the budget is spent" \
    '! should_recover_session "$DAEMON_EXIT_SESSION_SEVERED" "$MAX_SESSION_RECOVERIES"'

run_test "should_recover_session does not restart an external unmount" \
    '! should_recover_session 102 0'

run_test "should_recover_session does not restart a real failure" \
    '! should_recover_session 1 0 && ! should_recover_session 101 0 && ! should_recover_session 0 0'

run_test "should_recover_session codes do not collide with the other exits" \
    'for code in 0 1 2 3 100 101 102 103; do [ "$DAEMON_EXIT_SESSION_SEVERED" -ne "$code" ] || exit 1; done'

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1

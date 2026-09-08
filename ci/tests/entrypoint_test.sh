#!/usr/bin/env bash
# Unit tests for entrypoint.sh detection helpers (TSI-2469).
#
# Tests the pure-logic functions that determine container environment and
# mount-propagation capabilities — without requiring a real FUSE mount or a
# running torrentfs process.
#
# The entrypoint script has `set -euo pipefail` and executes main at the
# bottom, so we cannot simply `source` it. Instead we extract the helper
# functions (everything before the "# ── main" marker) into a temp file and
# source that prefix.
#
# Usage: ./ci/tests/entrypoint_test.sh
# Exit code: 0 = all pass, 1 = any failure.

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

# Stub out validate_config so parse_args tests can pass --config without
# invoking the real torrentfs binary (unavailable in this test environment).
# parse_args is only exercised for argument structure here; config validity
# itself is covered by the Rust --config-check tests.
validate_config() { :; }
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

# --- parse_args (mountpoint identification, TSI-2902) ---
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

run_test "parse_args forwards non-mountpoint args to torrentfs_args" \
    'parse_args --config /foo.toml /mnt; [ "${#torrentfs_args[@]}" -eq 2 ] && [ "${torrentfs_args[0]}" = --config ] && [ "${torrentfs_args[1]}" = /foo.toml ]'

run_test "parse_args -- /mnt sets mountpoint=/mnt" \
    'parse_args -- /mnt; [ "$mountpoint" = /mnt ]'

run_test "parse_args -- --mnt sets mountpoint=--mnt" \
    'parse_args -- --mnt; [ "$mountpoint" = "--mnt" ]'

# --- validate_mountpoint (mountpoint guard, TSI-2902) ---
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

# --- mountpoint_enotconn ---
# setup_stat defines a fake `stat` function (a shell builtin here, so a PATH
# stub would never be consulted) with the given exit code and stderr text.

run_test "mountpoint_enotconn true on ENOTCONN stderr" \
    'setup_stat 1 "stat: cannot stat '\''/mnt'\'': Transport endpoint is not connected"; if mountpoint_enotconn /mnt; then exit 0; else exit 1; fi'

run_test "mountpoint_enotconn false when stat succeeds" \
    'setup_stat 0 ""; if mountpoint_enotconn /mnt; then exit 1; else exit 0; fi'

run_test "mountpoint_enotconn false on ENOENT" \
    'setup_stat 1 "stat: cannot stat '\''/mnt'\'': No such file or directory"; if mountpoint_enotconn /mnt; then exit 1; else exit 0; fi'

# --- fuse_device_exists (just verify it doesn't crash) ---

run_test "fuse_device_exists does not crash" \
    'fuse_device_exists 2>/dev/null || true; exit 0'

# --- wait_for_fuse_mount ---
# The helper polls `mountpoint` and watches torrentfs (via `kill -0` / `wait`).
# Mock all three so the 30s deadline never actually elapses: `mountpoint`
# controls readiness, `kill -0` controls liveness, `wait` supplies the exit
# code. `kill`/`wait` are shell builtins here, so shell functions shadow them.

run_test "wait_for_fuse_mount returns 0 when mount becomes ready" \
    'mountpoint() { return 0; }; if wait_for_fuse_mount 99999 /mnt 2>/dev/null; then exit 0; else exit 1; fi'

run_test "wait_for_fuse_mount propagates torrentfs exit code on premature exit" \
    'mountpoint() { return 1; }; kill() { return 1; }; wait() { return 7; }; rc=0; wait_for_fuse_mount 99999 /mnt 2>/dev/null || rc=$?; [ "$rc" -eq 7 ]'

run_test "wait_for_fuse_mount treats clean exit without mount as failure (rc 1)" \
    'mountpoint() { return 1; }; kill() { return 1; }; wait() { return 0; }; rc=0; wait_for_fuse_mount 99999 /mnt 2>/dev/null || rc=$?; [ "$rc" -eq 1 ]'

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1

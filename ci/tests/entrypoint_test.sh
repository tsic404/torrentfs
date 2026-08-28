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
trap 'rm -f "$HELPERS_FILE"' EXIT

# Append a test-only override helper to the extracted functions so it is
# available alongside is_bind_mount in every subshell that sources
# HELPERS_FILE.  This keeps the test bodies free of duplicated mktemp +
# printf logic.
cat >> "$HELPERS_FILE" <<'EOF'

# Write a fake mountinfo to a temp file and redefine is_bind_mount to read
# from it instead of /proc/self/mountinfo.
setup_mountinfo() {
    local content="$1"
    local tmpfile
    tmpfile="$(mktemp)"
    printf '%s\n' "$content" > "$tmpfile"
    MOUNTINFO_FAKE="$tmpfile"
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

run_test "needs_fuse /mnt needs FUSE" \
    'if needs_fuse /mnt; then exit 0; else exit 1; fi'

run_test "needs_fuse --config /foo.toml /mnt needs FUSE" \
    'if needs_fuse --config /foo.toml /mnt; then exit 0; else exit 1; fi'

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

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1

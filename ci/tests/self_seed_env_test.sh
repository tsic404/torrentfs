#!/usr/bin/env bash
# Regression tests for ci/run_self_seed_env.sh payload-size and
# --tracker-bind/--announce-host validation.
# The script runs top-to-bottom (no "# ── main" marker), so extract
# validate_size_arg and exercise its bounds directly, then run the real script
# with invalid values to assert they fail before creating the output dir or
# building the seeder (the "no side effects" guarantee this validation exists
# for).  Bounds match the signed-64-bit payload caps: 2^43-1 MiB / 2^33-1 GiB.
# The bind/announce cases assert the same no-side-effect contract: an
# unreachable pair, an IPv6 literal, or an unknown option exits 2 before any
# directory is created (no real seeder is started).
# Usage: ./ci/tests/self_seed_env_test.sh   Exit: 0 = pass, 1 = fail.

set -euo pipefail

SCRIPT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/ci/run_self_seed_env.sh"
PASS=0
FAIL=0

# ── extract helpers ──────────────────────────────────────────────────────────

# Extract validate_size_arg (defined and closed at column 0).
HELPERS_FILE="$(mktemp)"
awk '/^validate_size_arg\(\) \{/{f=1} f{print} f && /^}/{exit}' "$SCRIPT" > "$HELPERS_FILE"
trap 'rm -f "$HELPERS_FILE"' EXIT

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

echo "Running self-seed env validation tests..."
echo ""

# --- validate_size_arg: --payload-gib accept ---

run_test "payload-gib accepts 1" \
    'validate_size_arg --payload-gib 1 8589934591 GiB'

run_test "payload-gib accepts max 8589934591" \
    'validate_size_arg --payload-gib 8589934591 8589934591 GiB'

run_test "payload-gib accepts leading zeros (00008 = 8)" \
    'validate_size_arg --payload-gib 00008 8589934591 GiB'

# --- validate_size_arg: --payload-gib reject ---

run_test "payload-gib rejects non-numeric" \
    'rc=0; ( validate_size_arg --payload-gib abc 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects zero" \
    'rc=0; ( validate_size_arg --payload-gib 0 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects all-zeros" \
    'rc=0; ( validate_size_arg --payload-gib 000 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects negative" \
    'rc=0; ( validate_size_arg --payload-gib -1 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects max+1 (8589934592)" \
    'rc=0; ( validate_size_arg --payload-gib 8589934592 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects 2^64+1 without arithmetic wrap" \
    'rc=0; ( validate_size_arg --payload-gib 18446744073709551617 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-gib rejects 2^64+8589934586 without arithmetic wrap" \
    'rc=0; ( validate_size_arg --payload-gib 18446744073710632187 8589934591 GiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

# --- validate_size_arg: --payload-mib accept ---

run_test "payload-mib accepts default 4" \
    'validate_size_arg --payload-mib 4 8796093022207 MiB'

run_test "payload-mib accepts max 8796093022207" \
    'validate_size_arg --payload-mib 8796093022207 8796093022207 MiB'

# --- validate_size_arg: --payload-mib reject ---

run_test "payload-mib rejects non-numeric" \
    'rc=0; ( validate_size_arg --payload-mib abc 8796093022207 MiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-mib rejects zero" \
    'rc=0; ( validate_size_arg --payload-mib 0 8796093022207 MiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

run_test "payload-mib rejects max+1 (8796093022208)" \
    'rc=0; ( validate_size_arg --payload-mib 8796093022208 8796093022207 MiB ) 2>/dev/null || rc=$?; [ "$rc" -eq 2 ]'

# --- end-to-end: invalid value produces no output dir ---

run_test "invalid --payload-gib exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --payload-gib 18446744073709551617 --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

run_test "invalid --payload-mib exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --payload-mib 0 --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

# --- end-to-end: --tracker-bind/--announce-host validation ---

run_test "mismatched --announce-host exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --tracker-bind 127.0.0.1 --announce-host 10.0.0.1 --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

run_test "IPv6 --announce-host exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --announce-host "::1" --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

run_test "IPv6 --tracker-bind exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --tracker-bind "::1" --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

run_test "unknown option exits 2 without creating output dir" \
    'd="$(mktemp -d)"
rc=0
( "$SCRIPT" --bogus --output-dir "$d/out" ) 2>/dev/null || rc=$?
created=1
[ ! -e "$d/out" ] && created=0
rm -rf "$d"
[ "$rc" -eq 2 ] && [ "$created" -eq 0 ]'

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1

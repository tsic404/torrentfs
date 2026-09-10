#!/usr/bin/env bash
# Unit tests for ci/deploy_rootful.sh (TSI-2759).
#
# Tests the pure logic of the rootful deployment helper — argument parsing,
# engine probing, and the --dry-run command sequence — without requiring root,
# /dev/fuse, or a running container engine.
#
# The script has `set -euo pipefail` and executes main at the bottom, so we
# cannot simply `source` it. Instead we extract everything before the
# "# ── main" marker into a temp file and source that prefix, mirroring
# entrypoint_test.sh.
#
# Usage: ./ci/tests/deploy_rootful_test.sh
# Exit code: 0 = all pass, 1 = any failure.

set -euo pipefail

DEPLOY="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)/ci/deploy_rootful.sh"
PASS=0
FAIL=0

# ── extract helpers ──────────────────────────────────────────────────────────

# Extract everything before the main section marker.
HELPERS_FILE="$(mktemp)"
awk '/^# ── main/{exit} {print}' "$DEPLOY" > "$HELPERS_FILE"

# Fake engine executables for detect_engine tests. `command -v` consults
# PATH, so pointing PATH at these directories controls what the probe sees.
FAKE_DIR="$(mktemp -d)"
# Single EXIT trap cleans both temp artifacts — later trap calls overwrite
# earlier ones in bash, so both must live in the same handler.
trap 'rm -f "$HELPERS_FILE"; rm -rf "$FAKE_DIR"' EXIT
mkdir -p "$FAKE_DIR/both" "$FAKE_DIR/docker" "$FAKE_DIR/none"
printf '#!/bin/sh\nexit 0\n' > "$FAKE_DIR/both/podman"
printf '#!/bin/sh\nexit 0\n' > "$FAKE_DIR/both/docker"
printf '#!/bin/sh\nexit 0\n' > "$FAKE_DIR/docker/docker"
chmod +x \
    "$FAKE_DIR/both/podman" "$FAKE_DIR/both/docker" "$FAKE_DIR/docker/docker"

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

# Assert that invoking parse_args with the given missing-value option exits 2.
# $1 = option (e.g. --engine)
expect_missing_value_exit_2() {
    local opt="$1"
    run_test "$opt without a value exits 2" \
        "if parse_args $opt 2>/dev/null; then exit 1; else rc=\$?; [ \"\$rc\" -eq 2 ] && exit 0 || exit \"\$rc\"; fi"
}

# ── tests ────────────────────────────────────────────────────────────────────

echo "Running deploy_rootful tests..."
echo ""

# --- parse_args: missing values ---

expect_missing_value_exit_2 --engine
expect_missing_value_exit_2 --mountpoint
expect_missing_value_exit_2 --state
expect_missing_value_exit_2 --image
expect_missing_value_exit_2 --name

# --- parse_args: value assignment ---

run_test "parse_args assigns all value options and --dry-run" \
    'parse_args --engine docker --mountpoint /mnt/tf --state /srv/tf --image ghcr.io/tsic404/torrentfs:main-amd64 --name tf-qa --dry-run; [ "$ENGINE" = docker ] && [ "$MOUNTPOINT" = /mnt/tf ] && [ "$STATE_DIR" = /srv/tf ] && [ "$IMAGE" = ghcr.io/tsic404/torrentfs:main-amd64 ] && [ "$NAME" = tf-qa ] && [ "$DRY_RUN" -eq 1 ]'

run_test "parse_args default IMAGE is explicitly tagged" \
    '[ "$IMAGE" = ghcr.io/tsic404/torrentfs:main ]'
# --- parse_args: unknown option ---

run_test "unknown option exits 2" \
    'if parse_args --bogus 2>/dev/null; then exit 1; else rc=$?; [ "$rc" -eq 2 ] && exit 0 || exit "$rc"; fi'

# --- parse_args: help ---

run_test "--help exits 0" \
    'parse_args --help >/dev/null 2>&1'

# --- detect_engine ---

run_test "detect_engine prefers podman when both present" \
    'out=$(PATH="$FAKE_DIR/both" detect_engine); [ "$out" = podman ]'

run_test "detect_engine falls back to docker" \
    'out=$(PATH="$FAKE_DIR/docker" detect_engine); [ "$out" = docker ]'

run_test "detect_engine exits non-zero when neither present" \
    'if PATH="$FAKE_DIR/none" detect_engine >/dev/null 2>&1; then exit 1; else exit 0; fi'

# --- print_dry_run ---

run_test "dry-run sequence includes state dir mkdir" \
    'ENGINE=podman; out=$(print_dry_run); printf "%s\n" "$out" | grep -q "mkdir -p /var/lib/torrentfs"'

run_test "dry-run sequence includes explicit image tag" \
    'ENGINE=podman; out=$(print_dry_run); printf "%s\n" "$out" | grep -q "ghcr.io/tsic404/torrentfs:main"'

run_test "dry-run sequence includes engine command" \
    'ENGINE=podman; out=$(print_dry_run); printf "%s\n" "$out" | grep -q "podman run -d --name torrentfs"'

run_test "dry-run mounts state at the non-root daemon home" \
    'ENGINE=podman; out=$(print_dry_run); printf "%s\n" "$out" | grep -q "target=/home/torrentfs/.local/share/torrentfs"'

# ── summary ──────────────────────────────────────────────────────────────────

echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1

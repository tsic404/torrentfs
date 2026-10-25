#!/usr/bin/env bash
# Design acceptance case TC-container-06: the entrypoint's config-source
# precedence is observable — a non-empty `TORRENTFS_CONFIG` overrides the
# mounted default path, a set-but-empty value is an explicit "no config" that
# does NOT fall back to the mounted default, and an explicit `--config` wins
# over the environment (`docs/03-acceptance.md` §H AC-75;
# `docs/02-contracts.md` C-10.2).
#
# Container-level executable form.  Every source points at a TOML file with a
# distinct `[cache] cache_size`; the root `.stats` cache cap is the observable
# that names which file won.  A control run with the environment unset proves
# the mounted default file is actually applied (so the empty-value run's
# "default cap" is not vacuous).  Self-gating: SKIP (exit 2) when docker,
# /dev/fuse or a successful image build is unavailable.
#
# Usage: ./tests/acceptance/tc_container_06.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded; a hung docker subcommand fails the case.
set -euo pipefail

CASE="TC-container-06"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
CONF_DIR="$WORK/conf"
DEFAULT_CONF="$WORK/default.toml"
CONTAINERS=()

fail() {
    echo "$CASE: FAIL — $*" >&2
    local name
    for name in "${CONTAINERS[@]}"; do
        { timeout 10 docker logs "$name" 2>&1 | tail -20 >&2; } || true
    done
    exit 1
}

skip() {
    echo "$CASE: SKIP — $*" >&2
    exit 2
}

cleanup() {
    local name
    for name in "${CONTAINERS[@]}"; do
        if [ -n "$(timeout 5 docker ps -q -f "name=^${name}$" 2>/dev/null)" ]; then
            timeout 30 docker stop -t 30 "$name" >/dev/null 2>&1 || true
        fi
        timeout 20 docker rm -f "$name" >/dev/null 2>&1 || true
    done
    timeout 30 rm -rf "$WORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || skip "no docker"
[ -e /dev/fuse ] || skip "no /dev/fuse"

mkdir -p "$CONF_DIR"
chmod 755 "$CONF_DIR"
cat >"$CONF_DIR/env.toml" <<'TOML'
[cache]
cache_size = 67108864
TOML
cat >"$DEFAULT_CONF" <<'TOML'
[cache]
cache_size = 33554432
TOML
cat >"$CONF_DIR/cli.toml" <<'TOML'
[cache]
cache_size = 16777216
TOML
chmod 644 "$CONF_DIR/env.toml" "$CONF_DIR/cli.toml" "$DEFAULT_CONF"

rc=0
IMAGE_ID="$(timeout 30 docker images -q "$IMAGE" 2>/dev/null)" || rc=$?
if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
    fail "docker images hung (rc=$rc)"
fi
[ "$rc" -eq 0 ] || fail "docker images failed (rc=$rc)"
if [ -z "$IMAGE_ID" ]; then
    echo "$CASE: building image $IMAGE…"
    rc=0
    timeout 900 docker build -q -t "$IMAGE" "$ROOT_DIR" >/dev/null || rc=$?
    if [ "$rc" -eq 124 ] || [ "$rc" -ge 128 ]; then
        fail "docker build hung (rc=$rc)"
    fi
    [ "$rc" -eq 0 ] || skip "docker build failed (rc=$rc)"
fi

container_running() {
    [ "$(timeout 5 docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

wait_for_mount() {
    local name="$1" limit="$2" deadline inner
    deadline=$((SECONDS + limit))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if ! container_running "$name"; then
            return 1
        fi
        inner="$(timeout 5 docker exec "$name" ls /mnt 2>/dev/null || true)"
        if grep -qx "data" <<<"$inner" && grep -qx "metadata" <<<"$inner"; then
            return 0
        fi
        sleep 1
    done
    return 1
}

stop_container() {
    local name="$1" code=0
    timeout 60 docker stop -t 30 "$name" >/dev/null || code=$?
    [ "$code" -eq 0 ] || fail "docker stop $name failed or hung (rc=$code)"
}

start_case() {
    local name="$1"
    shift
    timeout 120 docker run -d --name "$name" \
        --device /dev/fuse --cap-add SYS_ADMIN \
        --security-opt apparmor=unconfined \
        "$@" >/dev/null || fail "docker run failed for $name"
    CONTAINERS+=("$name")
    wait_for_mount "$name" 120 || fail "the FUSE mount did not become ready for $name"
}

# Poll the root `.stats` until the cache cap equals EXPECTED and assert it.
# The stats snapshot refreshes about once per second, so poll instead of
# trusting the first read after mount.
assert_cache_max() {
    local name="$1" expected="$2" label="$3" deadline text=""
    deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        text="$(timeout 10 docker exec "$name" cat /mnt/.stats 2>/dev/null || true)"
        if grep -q "Cache Usage:.* / $expected (" <<<"$text"; then
            return 0
        fi
        sleep 1
    done
    grep '^  Cache Usage:' <<<"$text" >&2 || true
    fail "$label: expected cache cap / $expected, got: $(grep '^  Cache Usage:' <<<"$text")"
}

MOUNTS=(-v "$CONF_DIR:/configs:ro" -v "$DEFAULT_CONF:/etc/torrentfs.toml:ro")

# Control first: with the environment unset, the bind-mounted default path is
# applied (32 MiB).  This proves the default path is live, which is what makes
# the empty-value case below meaningful.
NAME_DEFAULT="tc-container-06-default-$$"
echo "$CASE: control — mounted default path applies (env unset)…"
start_case "$NAME_DEFAULT" "${MOUNTS[@]}" "$IMAGE" /mnt
assert_cache_max "$NAME_DEFAULT" "32.00 MB" "mounted default path"
stop_container "$NAME_DEFAULT"

# A non-empty environment variable overrides the mounted default path.
NAME_ENV="tc-container-06-env-$$"
echo "$CASE: non-empty TORRENTFS_CONFIG wins over the mounted default…"
start_case "$NAME_ENV" "${MOUNTS[@]}" \
    -e TORRENTFS_CONFIG=/configs/env.toml "$IMAGE" /mnt
assert_cache_max "$NAME_ENV" "64.00 MB" "non-empty TORRENTFS_CONFIG"
stop_container "$NAME_ENV"

# A set-but-empty variable is an explicit "no config": nothing is injected and
# the mounted default path must NOT be used (default 1 GiB cap instead).
NAME_EMPTY="tc-container-06-empty-$$"
echo "$CASE: empty TORRENTFS_CONFIG injects nothing and does not fall back…"
start_case "$NAME_EMPTY" "${MOUNTS[@]}" \
    -e TORRENTFS_CONFIG= "$IMAGE" /mnt
assert_cache_max "$NAME_EMPTY" "1.00 GB" "empty TORRENTFS_CONFIG (no fallback)"
stop_container "$NAME_EMPTY"

# An explicit --config wins over the environment.
NAME_CLI="tc-container-06-cli-$$"
echo "$CASE: --config wins over TORRENTFS_CONFIG…"
start_case "$NAME_CLI" "${MOUNTS[@]}" \
    -e TORRENTFS_CONFIG=/configs/env.toml \
    "$IMAGE" /mnt --config /configs/cli.toml
assert_cache_max "$NAME_CLI" "16.00 MB" "CLI --config over the environment"
stop_container "$NAME_CLI"

echo "$CASE: PASS (mounted default 32.00 MB; env 64.00 MB wins over it; empty env falls back to the 1.00 GB default; CLI 16.00 MB wins over env)"

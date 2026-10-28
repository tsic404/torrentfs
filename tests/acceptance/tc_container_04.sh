#!/usr/bin/env bash
# Design acceptance case TC-container-04: the container entrypoint forwards the
# state-db / cache / config / log parameters to the daemon (each one observable
# at the path it names), a run with no extra arguments behaves like the image
# default, and an invalid config path exits non-zero with the reason
# (`docs/03-acceptance.md` §H AC-73; `docs/02-contracts.md` C-10.2, C-10.3,
# C-10.9).
#
# Container-level executable form.  The passthrough run points `--cache`,
# `--db`, `--config`, `--log-file` and `--log-level` at host-mounted paths and
# asserts the effects (cache cap from the config in the root `.stats`, the DB
# and cache metadata appearing at the named paths, the structured log landing
# in the named file, and `warn` suppressing the INFO startup lines).  The
# default run uses the image CMD with no extra arguments and asserts the
# default state paths and the 1 GiB cache cap.  The failure runs assert the
# usage/exit-code contract: an invalid config exits 1 with the entrypoint's
# reason and the daemon's own reason on stderr, `--cache-size` alone, a missing
# mountpoint and a `-`-prefixed mountpoint exit 2.
#
# Usage: ./tests/acceptance/tc_container_04.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded; a hung docker subcommand fails the case.
set -euo pipefail

CASE="TC-container-04"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
STATE_HOST="$WORK/state"
LOG_HOST="$WORK/logs"
CONF_DIR="$WORK/conf"
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

mkdir -p "$STATE_HOST" "$LOG_HOST" "$CONF_DIR"
chmod 755 "$STATE_HOST" "$LOG_HOST" "$CONF_DIR"
cat >"$CONF_DIR/passthrough.toml" <<'TOML'
# Cache cap distinctly different from both the 1 GiB default and the 32 MiB
# default-path fixture used by the config-source case.
[cache]
cache_size = 67108864
TOML
chmod 644 "$CONF_DIR/passthrough.toml"

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

# Poll the root `.stats` until the cache cap equals the expected rendering.
# The stats snapshot refreshes about once per second, so a single early read
# can still carry the previous view.
wait_cache_max() {
    local name="$1" expected="$2" deadline=0 text=""
    deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        text="$(timeout 10 docker exec "$name" cat /mnt/.stats 2>/dev/null || true)"
        if grep -q "Cache Usage:.* / $expected (" <<<"$text"; then
            return 0
        fi
        sleep 1
    done
    grep '^  Cache Usage:' <<<"$text" >&2 || true
    return 1
}

# The fmt subscriber colorizes the level even when writing to a file (the file
# is not a TTY but ANSI is on by default), so level assertions strip the escape
# sequences before matching.
log_text() {
    sed -E 's/\x1b\[[0-9;]*[A-Za-z]//g' "$1"
}

# Run a short-lived invocation that must exit with EXPECTED and print NEEDLE on
# stderr.  The remaining arguments are the full `docker run` argument list
# (docker options, image, then the container command).
expect_rc() {
    local expected="$1" needle="$2" desc="$3"
    shift 3
    local code=0
    timeout 120 docker run --rm "$@" >"$WORK/expect.out" 2>"$WORK/expect.err" || code=$?
    [ "$code" -eq "$expected" ] \
        || fail "$desc exited $code, expected $expected: $(tail -n 5 "$WORK/expect.err")"
    grep -qF "$needle" "$WORK/expect.err" \
        || fail "$desc stderr lacks '$needle': $(tail -n 5 "$WORK/expect.err")"
}

# ── parameter passthrough ────────────────────────────────────────────────────
NAME_A="tc-container-04-a-$$"
echo "$CASE: passthrough run (--cache/--db/--config/--log-file/--log-level)…"
timeout 120 docker run -d --name "$NAME_A" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    -v "$STATE_HOST:/state" -v "$LOG_HOST:/logs" -v "$CONF_DIR:/configs:ro" \
    "$IMAGE" /mnt --config /configs/passthrough.toml \
    --cache /state/cache --db /state/db/metadata.db \
    --log-file /logs/torrentfs.log --log-level debug >/dev/null || {
    fail "docker run failed or did not return within 120s"
}
CONTAINERS+=("$NAME_A")
wait_for_mount "$NAME_A" 120 || fail "the FUSE mount did not become ready"

wait_cache_max "$NAME_A" "64.00 MB" \
    || fail "the config's cache_size did not reach the daemon (expected / 64.00 MB in the root .stats)"
timeout 10 docker exec "$NAME_A" test -f /state/cache/cache_metadata.txt \
    || fail "--cache did not place the cache metadata at /state/cache"
timeout 10 docker exec "$NAME_A" test -f /state/db/metadata.db \
    || fail "--db did not place the SQLite DB at /state/db/metadata.db"
if timeout 10 docker exec "$NAME_A" test -e /home/torrentfs/.local/share/torrentfs; then
    fail "the default state directory was created even though --cache/--db were given"
fi

DEADLINE=$((SECONDS + 30))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    grep -q "torrentfs mounted" "$LOG_HOST/torrentfs.log" 2>/dev/null && break
    sleep 1
done
[ -f "$LOG_HOST/torrentfs.log" ] || fail "--log-file was not created on the host mount"
grep -q "torrentfs mounted" "$LOG_HOST/torrentfs.log" \
    || fail "--log-file did not receive the daemon's structured log"
# Here-string, not a pipe: `grep -q` closing the pipe early would SIGPIPE sed
# and `pipefail` would report 141 for a log that does carry the line.
grep -q " INFO " <<<"$(log_text "$LOG_HOST/torrentfs.log")" \
    || fail "the log file carries no INFO line at --log-level debug"
if grep -q "torrentfs mounted" <<<"$(timeout 10 docker logs "$NAME_A" 2>&1)"; then
    fail "--log-file did not redirect the structured log away from the container streams"
fi
stop_container "$NAME_A"

# ── --log-level is forwarded: warn suppresses the INFO startup lines ─────────
NAME_B="tc-container-04-b-$$"
echo "$CASE: --log-level warn run…"
timeout 120 docker run -d --name "$NAME_B" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    -v "$LOG_HOST:/logs" \
    "$IMAGE" /mnt --log-file /logs/warn.log --log-level warn >/dev/null || {
    fail "docker run failed for the --log-level warn run"
}
CONTAINERS+=("$NAME_B")
wait_for_mount "$NAME_B" 120 || fail "the FUSE mount did not become ready for the warn run"
DEADLINE=$((SECONDS + 30))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -f "$LOG_HOST/warn.log" ] && break
    sleep 1
done
[ -f "$LOG_HOST/warn.log" ] || fail "--log-file was not created for the warn run"
if grep -q " INFO " <<<"$(log_text "$LOG_HOST/warn.log")"; then
    fail "--log-level warn did not suppress the INFO lines in the log file"
fi
stop_container "$NAME_B"

# ── no extra arguments: image default behaviour ─────────────────────────────
NAME_C="tc-container-04-c-$$"
echo "$CASE: default run (no extra arguments)…"
timeout 120 docker run -d --name "$NAME_C" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    "$IMAGE" >/dev/null || fail "docker run failed for the default run"
CONTAINERS+=("$NAME_C")
wait_for_mount "$NAME_C" 120 || fail "the FUSE mount did not become ready for the default run"
wait_cache_max "$NAME_C" "1.00 GB" \
    || fail "the default run did not use the 1 GiB cache default (/ 1.00 GB)"
timeout 10 docker exec "$NAME_C" test -f /home/torrentfs/.local/share/torrentfs/db/metadata.db \
    || fail "the default state DB path was not used"
timeout 10 docker exec "$NAME_C" test -f /home/torrentfs/.local/share/torrentfs/cache/cache_metadata.txt \
    || fail "the default cache path was not used"
if ! grep -q "torrentfs mounted" <<<"$(timeout 10 docker logs "$NAME_C" 2>&1)"; then
    fail "the default run does not log to the container streams"
fi
stop_container "$NAME_C"

# ── invalid config: non-zero exit with the reason ───────────────────────────
echo "$CASE: invalid config paths…"
expect_rc 1 "invalid config file '/configs/missing.toml'" "an invalid --config path" \
    "$IMAGE" /mnt --config /configs/missing.toml
grep -qF "Invalid configuration file" "$WORK/expect.out" \
    || fail "the daemon's own invalid-config reason is missing from stdout"
grep -qF "(torrentfs exit code 1)" "$WORK/expect.err" \
    || fail "the entrypoint did not propagate the config validation exit code"

expect_rc 1 "invalid config file '/configs/missing-env.toml'" "an invalid TORRENTFS_CONFIG path" \
    -e TORRENTFS_CONFIG=/configs/missing-env.toml "$IMAGE" /mnt

# ── usage errors: exit code 2 (C-10.3 / C-10.9) ─────────────────────────────
echo "$CASE: usage errors…"
expect_rc 2 "cache-size is not a CLI option" "a standalone --cache-size" \
    "$IMAGE" --cache-size 1024
expect_rc 2 "no mount point specified" "a missing mountpoint" \
    --entrypoint /usr/local/bin/entrypoint.sh "$IMAGE"
expect_rc 2 "must not begin with '-'" "a '-' prefixed mountpoint after --" \
    "$IMAGE" -- -mnt

echo "$CASE: PASS (config/cache/db/log parameters take effect; default run matches the image default; invalid config exits 1 with its reason; usage errors exit 2)"

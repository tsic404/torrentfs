#!/usr/bin/env bash
# Design acceptance case TC-recovery-03: SIGTERM stops the daemon with exit
# code 0, drains the download engine, flushes the cache metadata and unmounts
# FUSE (`docs/03-acceptance.md` §I AC-82; `docs/02-contracts.md` C-10.9:
# `0` = normal shutdown).
#
# case TC-recovery-03
#
# Container-level executable form.  The observation that regressed is the
# guest's own exit code: the daemon drains, flushes and unmounts, then the
# entrypoint reports 143 because the SIGTERM interrupted its `wait`.  A
# daemon-level assertion cannot see that — the daemon's status is 0 either
# way — so this carrier runs the production image, lets the entrypoint reach
# its steady state, stops the container through the runtime, and asserts
# `docker inspect .State.ExitCode` is 0 together with the daemon's own
# shutdown sequence in the container log.
#
# Scenarios cover the entrypoint's shutdown paths: the rootful two-stage bind
# publish (the image default) and the container-only direct mount (`--user`,
# the rootless form), both stopped by the runtime; a third repeats the SIGTERM
# 150 ms into the shutdown, where a re-entering trap would replace the reaped
# daemon status with 128+signo.
#
# Each scenario waits for the entrypoint's own "torrentfs running" line before
# stopping: stopping earlier would exercise the startup path, not the steady
# state SIGTERM contract this case is about.
#
# Usage: ./tests/acceptance/tc_recovery_03.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.  A scenario
# the environment refuses (no usable FUSE device) counts as 2 as well: a case
# that asserted nothing must never read as passed.
#
# Every external call is bounded (docker subcommands, the readiness poll); a
# hung call fails the case with a diagnostic instead of stalling the job.
set -euo pipefail

CASE="TC-recovery-03"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
CONTAINERS=()
SCENARIOS_RUN=0
SCENARIOS_SKIPPED=0

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

# Container log (stdout+stderr), bounded like every other external call.
container_log() {
    timeout 10 docker logs "$1" 2>&1 || true
}

container_running() {
    [ "$(timeout 5 docker inspect -f '{{.State.Running}}' "$1" 2>/dev/null)" = "true" ]
}

# Wait up to $2 seconds for the entrypoint to report its steady state.
# Returns 0 once "torrentfs running" is in the log, 1 on timeout, 2 when the
# container died first — the caller decides whether that is an environment
# refusal or a failure.
wait_for_steady_state() {
    local name="$1" limit="$2" deadline
    deadline=$((SECONDS + limit))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if container_log "$name" | grep -qF '[entrypoint] torrentfs running (pid='; then
            return 0
        fi
        container_running "$name" || return 2
        sleep 0.5
    done
    return 1
}

# The entrypoint's documented environment refusal (C-10.9 status 100): the FUSE
# device is missing or unusable, so no mount was ever attempted.  Exit 100
# alone is not enough — the daemon's own device-unusable code is passed through
# as 100 too — so the entrypoint's own diagnostic must be in the log.  Every
# other early exit (invalid config, unwritable state directory, lock conflict,
# panic) is a failure of this case, never an environment limitation.
environment_refused_mount() {
    local name="$1"
    [ "$(timeout 10 docker inspect -f '{{.State.ExitCode}}' "$name" 2>/dev/null || true)" = "100" ] \
        || return 1
    container_log "$name" | grep -qF 'FUSE kernel device (/dev/fuse) is not available in this container'
}

# Stop the container with the runtime's SIGTERM twice: the second one lands
# while the entrypoint's trap is reaping the daemon.  A clean stop must survive
# it — a trap that re-enters would replace the reaped status with 128+signo.
stop_with_repeated_sigterm() {
    local name="$1" desc="$2" code=0 deadline
    timeout 30 docker kill -s TERM "$name" >/dev/null || code=$?
    [ "$code" -eq 0 ] || fail "$desc: docker kill -s TERM failed (rc=$code)"
    sleep 0.15
    code=0
    timeout 30 docker kill -s TERM "$name" >/dev/null 2>&1 || code=$?
    # The daemon's teardown takes ~1s (FUSE session join); a container that is
    # already gone means the second signal never reached the shutdown window.
    [ "$code" -eq 0 ] \
        || fail "$desc: the container exited before the second SIGTERM — the re-entrancy window was not exercised"
    deadline=$((SECONDS + 60))
    while [ "$SECONDS" -lt "$deadline" ]; do
        container_running "$name" || return 0
        sleep 0.2
    done
    fail "$desc: the container did not stop within 60s of two SIGTERMs"
}

# Run one container and assert the guest exits 0 when the runtime stops it.
# $1 container name, $2 scenario description, $3 stop mode (`stop` or
# `repeat-term`), then the extra `docker run` options.
run_scenario() {
    local name="$1" desc="$2" mode="$3"
    shift 3
    local code=0 exit_code listing ready

    echo "$CASE: $desc — starting container…"
    code=0
    timeout 120 docker run -d --name "$name" \
        --device /dev/fuse --cap-add SYS_ADMIN \
        --security-opt apparmor=unconfined \
        "$@" "$IMAGE" /mnt >/dev/null || code=$?
    [ "$code" -eq 0 ] || fail "$desc: docker run failed or hung (rc=$code)"
    CONTAINERS+=("$name")

    ready=0
    wait_for_steady_state "$name" 120 || ready=$?
    case "$ready" in
        0) ;;
        2)
            if environment_refused_mount "$name"; then
                echo "$CASE: $desc — SKIP (the environment refuses the FUSE device; no assertion ran)" >&2
                SCENARIOS_SKIPPED=$((SCENARIOS_SKIPPED + 1))
                return 0
            fi
            fail "$desc: the container exited before the entrypoint reached its steady state"
            ;;
        *) fail "$desc: the entrypoint did not report a running daemon within 120s" ;;
    esac

    # A daemon that is "running" must also be serving its mount: the exit-code
    # assertion below is only meaningful for a container that actually ran.
    listing="$(timeout 15 docker exec "$name" ls /mnt 2>/dev/null || true)"
    if ! grep -qx 'data' <<<"$listing" || ! grep -qx 'metadata' <<<"$listing"; then
        fail "$desc: the FUSE mount at /mnt does not expose data/ and metadata/"
    fi

    case "$mode" in
        stop)
            code=0
            timeout 60 docker stop -t 30 "$name" >/dev/null || code=$?
            [ "$code" -eq 0 ] || fail "$desc: docker stop failed or hung (rc=$code)"
            ;;
        repeat-term) stop_with_repeated_sigterm "$name" "$desc" ;;
        *) fail "$desc: unknown stop mode '$mode'" ;;
    esac

    exit_code="$(timeout 10 docker inspect -f '{{.State.ExitCode}}' "$name" 2>/dev/null || true)"
    [ -n "$exit_code" ] || fail "$desc: docker inspect did not report the container's exit code"
    # C-10.9: a runtime SIGTERM is a normal shutdown, so the guest must report
    # 0. 143 here means the entrypoint reported the status of its own
    # interrupted `wait` instead of the daemon's.
    [ "$exit_code" = "0" ] \
        || fail "$desc: container exit code is $exit_code, expected 0 (C-10.9: a SIGTERM stop is a normal shutdown)"

    # The exit code alone cannot distinguish a clean stop from a guest that
    # exited 0 for another reason: the daemon's own shutdown sequence must be
    # in the log, i.e. the SIGTERM reached it and its teardown ran.
    container_log "$name" | grep -qF 'unmounting FUSE filesystem' \
        || fail "$desc: the daemon's shutdown sequence is missing from the log"

    SCENARIOS_RUN=$((SCENARIOS_RUN + 1))
    echo "$CASE: $desc — PASS (container exit code 0 after the runtime's SIGTERM)"
}

run_scenario "tc-recovery-03-rootful-$$" "rootful two-stage publish" stop

# The container-only direct mount runs as a non-root user: the entrypoint keeps
# the container UID and mounts $mountpoint directly (no bind publish).  The
# image's /mnt belongs to root, so the non-root daemon gets a writable bind for
# its mountpoint; the daemon's state directory under the image's torrentfs home
# is already writable for that UID.
MNT_HOST="$WORK/mnt"
mkdir -p "$MNT_HOST"
chmod 0777 "$MNT_HOST"
run_scenario "tc-recovery-03-rootless-$$" "container-only direct mount" stop \
    --user 1000:1000 -v "$MNT_HOST:/mnt"

# The stop-supervisor case: a second SIGTERM arrives while the entrypoint's
# trap is still reaping the daemon.
run_scenario "tc-recovery-03-repeat-term-$$" "repeated SIGTERM during shutdown" repeat-term

if [ "$SCENARIOS_SKIPPED" -ne 0 ]; then
    echo "$CASE: SKIP — $SCENARIOS_RUN scenario(s) passed, $SCENARIOS_SKIPPED could not run here; the exit-code criterion is unverified" >&2
    exit 2
fi

echo "$CASE: PASS (SIGTERM stops the container with exit code 0 in every scenario)"

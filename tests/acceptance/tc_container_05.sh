#!/usr/bin/env bash
# Design acceptance case TC-container-05: two containers bound to the same host
# directory cannot both mount it — the later starter exits 101 while the first
# one's mount keeps working, for both sequential and simultaneous starts
# (`docs/03-acceptance.md` §H AC-74; `docs/02-contracts.md` C-10.9,
# `docs/adr/ADR-012-container-mount-propagation.md` 防叠加与互斥).
#
# Container-level executable form.  Both containers bind the same host
# directory with `bind-propagation=rshared` (the host-visibility recipe); the
# entrypoint's instance lock lives on the mountpoint directory inode, which is
# the same inode across both containers, and its "already a FUSE mount" probe
# catches a propagated mount.  The order case starts the second container
# after the first one is serving; the simultaneous case starts both at once
# and requires exactly one winner.  In both cases the loser must exit 101 and
# the winner must still list `metadata/` and `data/` without an ENOTCONN
# error.  Host-side propagation is not required for the conflict itself; when
# the caller supplies a host directory it is responsible for it being shared
# (run layer's rshared recipe), and a script-owned temp directory is made
# shared on a best-effort basis when running as root.
#
# Usage: ./tests/acceptance/tc_container_05.sh [image] [host-dir]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded; a hung docker subcommand fails the case.
set -euo pipefail

CASE="TC-container-05"
ACCEPTANCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$ACCEPTANCE_DIR/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
# shellcheck source=/dev/null
source "$ACCEPTANCE_DIR/lib_seed_torrent.sh"

CONTAINERS=()
SEQUENTIAL_DIR=""
PARALLEL_DIR=""
OWN_HOST_DIR=0
HOST_DIR_SHARED=0
if [ -n "${2:-}" ]; then
    HOST_DIR="$2"
else
    HOST_DIR="$(mktemp -d)"
    OWN_HOST_DIR=1
fi
RUN_DIR="$HOST_DIR/run.$$.$RANDOM"
SEQUENTIAL_DIR="$RUN_DIR/sequential"
PARALLEL_DIR="$RUN_DIR/parallel"

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

# Detach a host-side mount propagated through rshared (stale or live).  A
# caller-supplied host directory is never removed, only its mounts are.
detach_host_mount() {
    local target="$1" attempt
    for attempt in $(seq 1 10); do
        grep -q " $target " /proc/self/mountinfo 2>/dev/null || return 0
        timeout 10 fusermount3 -u -z "$target" >/dev/null 2>&1 \
            || timeout 10 fusermount -u -z "$target" >/dev/null 2>&1 \
            || timeout 10 umount -l "$target" >/dev/null 2>&1 \
            || timeout 15 sudo -n umount -l "$target" >/dev/null 2>&1 \
            || true
        sleep 1
    done
    ! grep -q " $target " /proc/self/mountinfo 2>/dev/null
}

cleanup() {
    local name
    for name in "${CONTAINERS[@]}"; do
        if [ -n "$(timeout 5 docker ps -q -f "name=^${name}$" 2>/dev/null)" ]; then
            timeout 30 docker stop -t 30 "$name" >/dev/null 2>&1 || true
        fi
        timeout 20 docker rm -f "$name" >/dev/null 2>&1 || true
    done
    if [ -n "$RUN_DIR" ]; then
        detach_host_mount "$SEQUENTIAL_DIR" 2>/dev/null || true
        detach_host_mount "$PARALLEL_DIR" 2>/dev/null || true
        timeout 30 rm -rf "$RUN_DIR" >/dev/null 2>&1 || true
    fi
    if [ "$OWN_HOST_DIR" -eq 1 ]; then
        if [ "$HOST_DIR_SHARED" -eq 1 ]; then
            timeout 15 umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || timeout 20 sudo -n umount -l "$HOST_DIR" >/dev/null 2>&1 \
                || true
        fi
        timeout 30 rm -rf "$HOST_DIR" >/dev/null 2>&1 || true
    fi
    timeout 30 rm -rf "$WORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || skip "no docker"
[ -e /dev/fuse ] || skip "no /dev/fuse"

timeout 10 mkdir -p "$SEQUENTIAL_DIR" "$PARALLEL_DIR"
# rshared propagation only reaches the host when the bind source lives on a
# shared mount; a script-owned temp directory is shared on a best-effort basis
# when running as root.  A caller-supplied directory is left untouched.
if [ "$OWN_HOST_DIR" -eq 1 ] && [ "$(id -u)" -eq 0 ]; then
    if timeout 10 mount --bind "$HOST_DIR" "$HOST_DIR" 2>/dev/null; then
        HOST_DIR_SHARED=1
        timeout 10 mount --make-shared "$HOST_DIR" 2>/dev/null || true
    fi
fi

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

start_bound_container() {
    local name="$1" source_dir="$2"
    timeout 120 docker run -d --name "$name" \
        --device /dev/fuse --cap-add SYS_ADMIN \
        --security-opt apparmor=unconfined \
        --mount "type=bind,source=$source_dir,target=/mnt,bind-propagation=rshared" \
        "$IMAGE" /mnt >/dev/null || fail "docker run failed for $name"
    CONTAINERS+=("$name")
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

# Poll until the container stops; print its exit code.  Returns 1 when it is
# still running at the deadline.
wait_exit_code() {
    local name="$1" limit="$2" deadline
    deadline=$((SECONDS + limit))
    while [ "$SECONDS" -lt "$deadline" ]; do
        if [ "$(timeout 5 docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null || echo unknown)" = "false" ]; then
            timeout 10 docker inspect -f '{{.State.ExitCode}}' "$name"
            return 0
        fi
        sleep 1
    done
    return 1
}

assert_conflict_message() {
    local name="$1" logs
    logs="$(timeout 10 docker logs "$name" 2>&1 || true)"
    if ! grep -qE "locked by another running torrentfs|already a FUSE mount owned by another" <<<"$logs"; then
        fail "$name exited 101 without the documented conflict diagnostic: $(tail -n 5 <<<"$logs")"
    fi
}

assert_winner_intact() {
    local name="$1" label="$2" out err_file="$WORK/winner.err"
    out="$(timeout 10 docker exec "$name" ls /mnt 2>"$err_file")" \
        || fail "$label cannot list /mnt: $(cat "$err_file")"
    grep -qx "data" <<<"$out" || fail "$label is missing data/ in /mnt"
    grep -qx "metadata" <<<"$out" || fail "$label is missing metadata/ in /mnt"
    if grep -qi "transport endpoint is not connected" "$err_file"; then
        fail "$label's mount was severed (ENOTCONN)"
    fi
    out="$(timeout 10 docker exec "$name" ls /mnt/data 2>"$err_file")" \
        || fail "$label cannot list /mnt/data: $(cat "$err_file")"
    if grep -qi "transport endpoint is not connected" "$err_file"; then
        fail "$label's data/ listing hit a severed connection (ENOTCONN)"
    fi
}

# ── sequential start: the first container owns the lock ─────────────────────
echo "$CASE: sequential start on one host directory…"
FIRST="tc-container-05-a-$$"
SECOND="tc-container-05-b-$$"
start_bound_container "$FIRST" "$SEQUENTIAL_DIR"
wait_for_mount "$FIRST" 120 || fail "the first container's mount did not become ready"

TORRENT_FILE="$WORK/seed.torrent"
make_seed_torrent "$TORRENT_FILE"
timeout 30 docker exec -i --user 1000:1000 "$FIRST" \
    sh -c 'cat > /mnt/metadata/seed.torrent' < "$TORRENT_FILE" \
    || fail "could not write the fixture torrent into the first container's metadata/"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if timeout 5 docker exec "$FIRST" test -d /mnt/data/seed.torrent 2>/dev/null; then
        break
    fi
    sleep 1
done
timeout 5 docker exec "$FIRST" test -d /mnt/data/seed.torrent \
    || fail "the first container's projection did not appear"

start_bound_container "$SECOND" "$SEQUENTIAL_DIR"
CODE="$(wait_exit_code "$SECOND" 120)" \
    || fail "the second container did not refuse the shared mount within 120s (still running)"
[ "$CODE" = "101" ] || fail "the second container exited $CODE, expected 101"
assert_conflict_message "$SECOND"
container_running "$FIRST" || fail "the first container stopped after the conflict"
assert_winner_intact "$FIRST" "the first container"
OUT="$(timeout 10 docker exec "$FIRST" ls /mnt/data 2>/dev/null || true)"
grep -qx "seed.torrent" <<<"$OUT" \
    || fail "the first container's projection disappeared after the conflict"

# ── simultaneous start: exactly one winner ──────────────────────────────────
echo "$CASE: simultaneous start on one host directory…"
RACER_A="tc-container-05-c-$$"
RACER_B="tc-container-05-d-$$"
PIDS=()
for name in "$RACER_A" "$RACER_B"; do
    timeout 120 docker run -d --name "$name" \
        --device /dev/fuse --cap-add SYS_ADMIN \
        --security-opt apparmor=unconfined \
        --mount "type=bind,source=$PARALLEL_DIR,target=/mnt,bind-propagation=rshared" \
        "$IMAGE" /mnt >/dev/null &
    PIDS+=("$!")
    CONTAINERS+=("$name")
done
RC_A=0
RC_B=0
wait "${PIDS[0]}" || RC_A=$?
wait "${PIDS[1]}" || RC_B=$?
[ "$RC_A" -eq 0 ] || fail "the first simultaneous docker run failed (rc=$RC_A)"
[ "$RC_B" -eq 0 ] || fail "the second simultaneous docker run failed (rc=$RC_B)"

WINNER=""
LOSER=""
DEADLINE=$((SECONDS + 120))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    RUN_A="$(timeout 5 docker inspect -f '{{.State.Running}}' "$RACER_A" 2>/dev/null || echo unknown)"
    RUN_B="$(timeout 5 docker inspect -f '{{.State.Running}}' "$RACER_B" 2>/dev/null || echo unknown)"
    if [ "$RUN_A" = "true" ] && [ "$RUN_B" = "false" ]; then
        WINNER="$RACER_A"
        LOSER="$RACER_B"
        break
    fi
    if [ "$RUN_A" = "false" ] && [ "$RUN_B" = "true" ]; then
        WINNER="$RACER_B"
        LOSER="$RACER_A"
        break
    fi
    if [ "$RUN_A" = "false" ] && [ "$RUN_B" = "false" ]; then
        fail "both simultaneous containers refused the same host directory"
    fi
    sleep 1
done
[ -n "$WINNER" ] || fail "the simultaneous start never settled to one winner and one refusal"
CODE="$(timeout 10 docker inspect -f '{{.State.ExitCode}}' "$LOSER")"
[ "$CODE" = "101" ] || fail "the losing simultaneous container exited $CODE, expected 101"
assert_conflict_message "$LOSER"
wait_for_mount "$WINNER" 120 || fail "the winning container's mount did not become ready"
assert_winner_intact "$WINNER" "the winning container"

echo "$CASE: PASS (later starter exits 101 in both orders; the first mount stays usable without ENOTCONN)"

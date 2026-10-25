#!/usr/bin/env bash
# Design acceptance case TC-container-08: the container form of the failure
# diagnostics is collection-equivalent — the entrypoint neither rewrites,
# merges nor swallows the daemon's error stream, so in a non-TTY container a
# frozen-prefix cause is collected unchanged and counted at least once
# (`docs/03-acceptance.md` §H AC-77; `docs/02-contracts.md` C-10.1, C-4.4).
#
# Container-level executable form.  Evidence is collected from the live
# attached `docker run` of this run — never from a replayed log file: without
# a TTY, the Docker CLI demultiplexes the container's stdout and stderr into
# its own stdout and stderr, so the two streams are captured separately.  The
# collection channel is self-proven first with a same-shaped positive control
# (a frozen-prefix-form line written to stderr must arrive verbatim on stderr
# and not on stdout).  The measured run then starts the real entrypoint and a
# real daemon with a sourceless swarm and short wait windows, fails a read
# with ENODATA, and requires >= 1 line matching the frozen no-seeder prefix on
# the container's stderr stream while the daemon's INFO line arrives on the
# stdout stream — a merge in either direction fails the case.
#
# Usage: ./tests/acceptance/tc_container_08.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded; a hung read or docker subcommand fails the
# case instead of stalling the job.
set -euo pipefail

CASE="TC-container-08"
ACCEPTANCE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$ACCEPTANCE_DIR/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
CONF_DIR="$WORK/conf"
CONTAINER="tc-container-08-$$"
DOCKER_PID=""
# shellcheck source=/dev/null
source "$ACCEPTANCE_DIR/lib_seed_torrent.sh"

fail() {
    echo "$CASE: FAIL — $*" >&2
    { timeout 10 docker logs "$CONTAINER" 2>&1 | tail -20 >&2; } || true
    exit 1
}

skip() {
    echo "$CASE: SKIP — $*" >&2
    exit 2
}

cleanup() {
    if [ -n "$(timeout 5 docker ps -q -f "name=^${CONTAINER}$" 2>/dev/null)" ]; then
        timeout 30 docker stop -t 30 "$CONTAINER" >/dev/null 2>&1 || true
    fi
    timeout 20 docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    if [ -n "$DOCKER_PID" ]; then
        kill "$DOCKER_PID" 2>/dev/null || true
    fi
    timeout 30 rm -rf "$WORK" >/dev/null 2>&1 || true
}
trap cleanup EXIT

command -v docker >/dev/null 2>&1 || skip "no docker"
[ -e /dev/fuse ] || skip "no /dev/fuse"

mkdir -p "$CONF_DIR"
chmod 755 "$CONF_DIR"
cat >"$CONF_DIR/short.toml" <<'TOML'
# A synthetic swarm that never appears: short windows keep the read bounded
# while still settling through the discovery/ no-seeder phases that emit the
# frozen cause.
[dht]
enabled = false

[local_discovery]
lsd_enabled = false

[timeouts]
read_timeout_secs = 4
peer_discovery_wait_secs = 3
no_seeder_read_timeout_secs = 3
TOML
chmod 644 "$CONF_DIR/short.toml"

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

# ── positive control: the non-TTY collection channel itself ─────────────────
# Same image, same non-TTY `docker run` with stdout/stderr redirected apart; a
# line shaped like the frozen prefix is written to stderr.  The control proves
# the channel delivers a stderr line of that shape verbatim and does not merge
# streams — it bypasses the entrypoint, which the measured run exercises.
CTL_MARKER='no seeder connected (Peers:0 Seeds:0) within the 3s peer-discovery window; channel positive control'
CTL_STDOUT='control-stdout-sentinel'
echo "$CASE: positive control of the non-TTY collection channel…"
timeout 120 docker run --rm --entrypoint /bin/sh \
    -e "CTL_MARKER=$CTL_MARKER" -e "CTL_STDOUT=$CTL_STDOUT" \
    "$IMAGE" -c 'printf "%s\n" "$CTL_MARKER" >&2; printf "%s\n" "$CTL_STDOUT"' \
    >"$WORK/control.out" 2>"$WORK/control.err" \
    || fail "the positive-control container failed"
grep -Fx "$CTL_MARKER" "$WORK/control.err" >/dev/null \
    || fail "the collection channel did not deliver the frozen-prefix-shaped control line on stderr"
if grep -qF "$CTL_MARKER" "$WORK/control.out"; then
    fail "the collection channel merged the control line into stdout"
fi
grep -Fx "$CTL_STDOUT" "$WORK/control.out" >/dev/null \
    || fail "the collection channel did not deliver the control stdout sentinel"
if grep -qF "$CTL_STDOUT" "$WORK/control.err"; then
    fail "the collection channel merged stdout into stderr"
fi

# ── measured run: entrypoint + daemon, sourceless swarm ─────────────────────
TORRENT_FILE="$WORK/seed.torrent"
make_seed_torrent "$TORRENT_FILE"
echo "$CASE: starting the production entrypoint attached without a TTY…"
timeout 300 docker run --rm --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined \
    -v "$CONF_DIR:/configs:ro" \
    "$IMAGE" /mnt --config /configs/short.toml \
    >"$WORK/stdout.log" 2>"$WORK/stderr.log" &
DOCKER_PID=$!

DEADLINE=$((SECONDS + 120))
READY=0
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if ! kill -0 "$DOCKER_PID" 2>/dev/null; then
        { tail -n 20 "$WORK/stderr.log" >&2; } || true
        fail "the attached docker run exited before the mount became ready"
    fi
    INNER="$(timeout 5 docker exec "$CONTAINER" ls /mnt 2>/dev/null || true)"
    if grep -qx "data" <<<"$INNER" && grep -qx "metadata" <<<"$INNER"; then
        READY=1
        break
    fi
    sleep 1
done
[ "$READY" -eq 1 ] || fail "the FUSE mount did not become ready within 120s"

timeout 30 docker exec -i --user 1000:1000 "$CONTAINER" \
    sh -c 'cat > /mnt/metadata/seed.torrent' < "$TORRENT_FILE" \
    || fail "could not write the fixture torrent into metadata/"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if timeout 5 docker exec "$CONTAINER" test -e /mnt/data/seed.torrent/seed.txt 2>/dev/null; then
        break
    fi
    sleep 1
done
timeout 5 docker exec "$CONTAINER" test -e /mnt/data/seed.torrent/seed.txt \
    || fail "the data/ projection did not appear within 60s"

echo "$CASE: failing a read on the sourceless swarm…"
READ_RC=0
timeout 60 docker exec "$CONTAINER" cat /mnt/data/seed.torrent/seed.txt \
    >"$WORK/read.out" 2>"$WORK/read.err" || READ_RC=$?
[ "$READ_RC" -ne 124 ] || fail "the read hung past 60s instead of settling through the wait windows"
[ "$READ_RC" -ne 0 ] || fail "the read unexpectedly succeeded on a sourceless swarm"
[ ! -s "$WORK/read.out" ] || fail "the failed read returned bytes"
grep -qi "no data available" "$WORK/read.err" \
    || fail "the failed read did not surface ENODATA: $(cat "$WORK/read.err")"

# The engine writes the classified cause to its own stderr at failure time; it
# must reach this run's stderr capture.  No replay: these files were created
# fresh by the attached `docker run` above.
COUNT=0
DEADLINE=$((SECONDS + 30))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    COUNT="$(grep -cE '^no seeder connected \(Peers:[0-9]+ Seeds:[0-9]+\)' "$WORK/stderr.log" || true)"
    [ "$COUNT" -ge 1 ] && break
    sleep 1
done
[ "$COUNT" -ge 1 ] || fail "no frozen no-seeder prefix reached the container's stderr stream (count=$COUNT)"
grep -qE '^no seeder connected \(Peers:[0-9]+ Seeds:[0-9]+\) (within the [0-9]+s peer-discovery window;|after the [0-9]+s no-seeder piece wait; no seeder is present in the swarm)' "$WORK/stderr.log" \
    || fail "the matched cause does not carry a documented frozen-prefix variant"

# C-10.1: the streams stay separate through the entrypoint.  The diagnostic
# must not appear on stdout, the daemon's structured INFO log must not appear
# on stderr, and the entrypoint's own diagnostics share the stderr capture
# without replacing the daemon's line.
if grep -qE '^no seeder connected \(Peers:' "$WORK/stdout.log"; then
    fail "the daemon's error-stream cause was merged into the container's stdout stream"
fi
grep -q "torrentfs mounted" "$WORK/stdout.log" \
    || fail "the daemon's structured stdout log is missing from the container's stdout stream"
if grep -q "torrentfs mounted" "$WORK/stderr.log"; then
    fail "the daemon's stdout log was merged into the container's stderr stream"
fi
grep -q '^\[entrypoint\]' "$WORK/stderr.log" \
    || fail "the entrypoint's own stderr diagnostics are missing from the captured stream"

timeout 60 docker stop -t 30 "$CONTAINER" >/dev/null 2>&1 || true
wait "$DOCKER_PID" 2>/dev/null || true
DOCKER_PID=""

echo "$CASE: PASS (frozen no-seeder prefix collected ${COUNT}× on the non-TTY stderr stream; stdout/stderr stayed separate; channel positive control passed)"

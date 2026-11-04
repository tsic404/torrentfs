#!/usr/bin/env bash
# Design acceptance case TC-container-03: restarting a container against the
# same state directory keeps the ingested torrent and the piece cache — after
# `docker stop` → `docker start` the projection is still listed and a second
# full read is served from the cache instead of re-downloading
# (`docs/03-acceptance.md` §H AC-72; `docs/02-contracts.md` C-10.11, C-5.3.1).
#
# Container-level executable form.  Runs the production image with a host
# directory bind-mounted as the state volume and passes `--cache` / `--db`
# inside it, so the SQLite DB and the piece cache survive the stop/start.  The
# swarm is synthesized inside that container from the shipped fixtures
# (`lib_container_swarm.sh`, `lib_swarm.pl`), so the host needs docker only —
# no build step on the host and no seeder binary to build.  A container stop kills the
# tracker and the seeder with the daemon, so the second read has no peer at
# all: a byte-exact read that finishes fast can only come from the cache.  On
# restart the daemon's own cache-verification summary must report every cached
# piece verified from disk, and the second read must stay inside the cache-hit
# time bound with no no-seeder fallback.  The global cache counters are printed
# as non-gating evidence only: they are a non-blocking snapshot and may render
# as zero while another reader holds the cache lock (C-6.5.1).
# Self-gating: SKIP (exit 2) when docker, /dev/fuse or a successful image build
# is unavailable.
#
# Usage: ./tests/acceptance/tc_container_03.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded (docker subcommands, the swarm startup, the
# in-container reads).  A hung call fails the case with a diagnostic instead
# of stalling the job.
set -euo pipefail

CASE="TC-container-03"
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
IMAGE="${1:-torrentfs:acceptance-ci}"
WORK="$(mktemp -d)"
STATE_HOST="$WORK/state"
SEED_OUT="$WORK/seed"
CONTAINER="tc-container-03-$$"
CONTAINERS=()
# shellcheck source=/dev/null
SWARM_HOST_DIR="$SEED_OUT"
source "$ROOT_DIR/tests/acceptance/lib_container_swarm.sh"

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

mkdir -p "$STATE_HOST" "$SEED_OUT"

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

stop_gracefully() {
    local name="$1" code=0
    timeout 60 docker stop -t 30 "$name" >/dev/null || code=$?
    [ "$code" -eq 0 ] || fail "docker stop $name failed or hung (rc=$code)"
    if container_running "$name"; then
        fail "$name is still running after docker stop"
    fi
}

# Root `.stats` is a snapshot refreshed about once per second; reading it twice
# with a one-second gap returns the current view (C-5.2.4's polling rule).
stats_text() {
    local name="$1" text
    text="$(timeout 10 docker exec "$name" cat /mnt/.stats 2>/dev/null || true)"
    sleep 1
    text="$(timeout 10 docker exec "$name" cat /mnt/.stats 2>/dev/null || true)"
    printf '%s' "$text"
}

# "hits misses" from the root `.stats` global cache counters.
cache_counts() {
    local text
    text="$(stats_text "$1")"
    sed -n 's/^  Hits: \([0-9,]*\)  Misses: \([0-9,]*\).*/\1 \2/p' <<<"$text" \
        | head -n1 | tr -d ','
}

# All piece markers cached (`[x]` / `[X n]`), none uncached or downloading.
all_pieces_cached() {
    local line marker
    line="$(sed -n 's/^  Pieces: //p' <<<"$1" | head -n1)"
    [ -n "$line" ] || return 1
    while IFS= read -r marker; do
        case "$marker" in
            "[x]"|"[X "*) : ;;
            *) return 1 ;;
        esac
    done < <(grep -oE '\[[^]]*\]' <<<"$line")
    return 0
}

echo "$CASE: first run — ingest, download and cache…"
timeout 120 docker run -d --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined --network host \
    -v "$STATE_HOST:/state" \
    -v "$SEED_OUT:/seed" \
    -v "$ROOT_DIR/tests/acceptance:/fixtures:ro" \
    "$IMAGE" /mnt --cache /state/cache --db /state/db/metadata.db >/dev/null || {
    fail "docker run failed or did not return within 120s"
}
CONTAINERS+=("$CONTAINER")
wait_for_mount "$CONTAINER" 120 || fail "the FUSE mount did not become ready on the first run"

# The swarm lives inside the container under test (its own Perl + the shipped
# fixtures), so stopping the container leaves the second read with no peer.
echo "$CASE: starting the local swarm inside the container…"
swarm_start "$CONTAINER" || fail "the in-container swarm fixtures did not start"
echo "  swarm: tracker=$SWARM_TRACKER_URL pieces=$SWARM_PIECE_COUNT"

timeout 30 docker exec -i --user 1000:1000 "$CONTAINER" \
    sh -c 'cat > /mnt/metadata/selfseed.torrent' < "$SWARM_TORRENT" \
    || fail "could not write the torrent into metadata/"
DEADLINE=$((SECONDS + 60))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    if timeout 5 docker exec "$CONTAINER" test -e /mnt/data/selfseed.torrent/selfseed 2>/dev/null; then
        break
    fi
    sleep 1
done
timeout 5 docker exec "$CONTAINER" test -e /mnt/data/selfseed.torrent/selfseed \
    || fail "the data/ projection did not appear within 60s"

READ_RC=0
timeout 240 docker exec "$CONTAINER" cat /mnt/data/selfseed.torrent/selfseed \
    >"$WORK/first.bin" 2>"$WORK/first.err" || READ_RC=$?
[ "$READ_RC" -eq 0 ] || fail "the first read failed (rc=$READ_RC): $(tail -n 3 "$WORK/first.err")"
cmp -s "$SWARM_PAYLOAD" "$WORK/first.bin" \
    || fail "the first read bytes differ from the authoritative payload"
[ -f "$STATE_HOST/db/metadata.db" ] \
    || fail "no SQLite DB under the state volume after the first run"
[ -f "$STATE_HOST/cache/cache_metadata.txt" ] \
    || fail "no cache metadata under the state volume after the first run"
CACHE_FILES="$(find "$STATE_HOST/cache/pieces" -type f 2>/dev/null | wc -l)"
[ "$CACHE_FILES" -gt 0 ] || fail "the cache volume holds no piece files after the first read"
LOG_OFFSET="$(timeout 10 docker logs "$CONTAINER" 2>&1 | wc -l)"

echo "$CASE: graceful stop…"
stop_gracefully "$CONTAINER"

# The tracker and the seeder were processes inside the container just stopped,
# so the restart leaves an empty swarm: a cache miss has no peer to download
# from and would stall on the no-seeder window.
sleep 2

echo "$CASE: restart against the same state volume…"
timeout 120 docker start "$CONTAINER" >/dev/null || fail "docker start failed after the graceful stop"
wait_for_mount "$CONTAINER" 120 || fail "the FUSE mount did not become ready after the restart"

# The projection comes from the persisted DB: the torrent file is not copied
# in again, so a missing state DB would leave data/ empty here.
timeout 10 docker exec "$CONTAINER" test -d /mnt/data/selfseed.torrent \
    || fail "the torrent is no longer visible after the restart"
[ -f "$STATE_HOST/db/metadata.db" ] \
    || fail "the state DB disappeared across stop/start"
[ -f "$STATE_HOST/cache/cache_metadata.txt" ] \
    || fail "the cache metadata disappeared across stop/start"
SIZE="$(timeout 10 docker exec "$CONTAINER" stat -c '%s' /mnt/data/selfseed.torrent/selfseed 2>/dev/null || true)"
EXPECTED_SIZE="$(stat -c '%s' "$SWARM_PAYLOAD")"
[ "$SIZE" = "$EXPECTED_SIZE" ] \
    || fail "projection size after restart is $SIZE, expected $EXPECTED_SIZE"

# Readiness only: the FUSE layer re-verifies on-disk pieces asynchronously on
# startup.  Wait (bounded) for the markers to settle, then judge on the listed
# observables; a timeout here is not itself a verdict.
DEADLINE=$((SECONDS + 90))
STATS=""
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    STATS="$(timeout 10 docker exec "$CONTAINER" cat /mnt/data/selfseed.torrent/.stats 2>/dev/null || true)"
    if all_pieces_cached "$STATS"; then
        break
    fi
    sleep 1
done

# AC-72 / C-5.3.1: the retained pieces are re-verified from disk on restart.
# The daemon's own verification summary is the observable that every cached
# piece (4 MiB payload / 256 KiB pieces) survived instead of being re-fetched.
VERIFIED=0
DEADLINE=$((SECONDS + 60))
RESTART_LOGS=""
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    RESTART_LOGS="$(timeout 10 docker logs "$CONTAINER" 2>&1 | tail -n +$((LOG_OFFSET + 1)) || true)"
    if grep -q "Cache piece verification complete: $SWARM_PIECE_COUNT verified" <<<"$RESTART_LOGS"; then
        VERIFIED=1
        break
    fi
    sleep 1
done
if [ "$VERIFIED" -ne 1 ]; then
    fail "the restart did not verify the $SWARM_PIECE_COUNT cached pieces from disk: $(grep -m1 'Cache piece verification' <<<"$RESTART_LOGS" || echo 'no verification line')"
fi

# Evidence only, never a verdict: the global cache counters are a non-blocking
# snapshot and may render as zero while a reader holds the cache lock
# (C-6.5.1), so they cannot carry AC-72.
BEFORE="$(cache_counts "$CONTAINER")"

echo "$CASE: second read — must be served from the cache…"
START_NS="$(date +%s%N)"
READ_RC=0
timeout 60 docker exec "$CONTAINER" cat /mnt/data/selfseed.torrent/selfseed \
    >"$WORK/second.bin" 2>"$WORK/second.err" || READ_RC=$?
END_NS="$(date +%s%N)"
[ "$READ_RC" -eq 0 ] || fail "the second read failed with no seeder (rc=$READ_RC): $(tail -n 3 "$WORK/second.err")"
cmp -s "$SWARM_PAYLOAD" "$WORK/second.bin" \
    || fail "the second read bytes differ from the authoritative payload"
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
[ "$ELAPSED_MS" -lt 10000 ] \
    || fail "the second read took ${ELAPSED_MS}ms; a cache miss would stall on the no-seeder window"

AFTER="$(cache_counts "$CONTAINER")"
NEW_LOGS="$(timeout 10 docker logs "$CONTAINER" 2>&1 | tail -n +$((LOG_OFFSET + 1)) || true)"
if grep -q "no seeder connected" <<<"$NEW_LOGS"; then
    fail "the second read fell back to the swarm (a no-seeder cause was logged after the restart)"
fi

stop_gracefully "$CONTAINER"
echo "$CASE: PASS (torrent and cache survived stop/start; $SWARM_PIECE_COUNT pieces verified from disk; second read ${ELAPSED_MS}ms from cache; cache counters evidence only: before=${BEFORE:-unavailable} after=${AFTER:-unavailable})"

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
# swarm is the local self-seed environment; the seeder is stopped before the
# restart, so a byte-exact read that finishes fast can only come from the
# cache.  The root `.stats` cache hit/miss counters confirm the hit: hits must
# grow and misses must not.  Self-gating: SKIP (exit 2) when docker, /dev/fuse
# or a successful image build is unavailable.
#
# Usage: ./tests/acceptance/tc_container_03.sh [image]
# Exit: 0 = pass, 1 = fail, 2 = environment cannot run this case.
#
# Every external call is bounded (docker subcommands, the seeder startup, the
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
SEEDER_PID=""
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
    local name child
    for name in "${CONTAINERS[@]}"; do
        if [ -n "$(timeout 5 docker ps -q -f "name=^${name}$" 2>/dev/null)" ]; then
            timeout 30 docker stop -t 30 "$name" >/dev/null 2>&1 || true
        fi
        timeout 20 docker rm -f "$name" >/dev/null 2>&1 || true
    done
    if [ -n "$SEEDER_PID" ]; then
        for child in $(pgrep -P "$SEEDER_PID" 2>/dev/null || true); do
            kill "$child" 2>/dev/null || true
        done
        kill "$SEEDER_PID" 2>/dev/null || true
    fi
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

echo "$CASE: starting the local self-seed swarm…"
"$ROOT_DIR/ci/run_self_seed_env.sh" \
    --tracker-bind 127.0.0.1 --announce-host 127.0.0.1 \
    --output-dir "$SEED_OUT" >"$WORK/seeder.log" 2>&1 &
SEEDER_PID=$!
DEADLINE=$((SECONDS + 240))
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    [ -f "$SEED_OUT/selfseed.torrent" ] && break
    if ! kill -0 "$SEEDER_PID" 2>/dev/null; then
        { tail -n 20 "$WORK/seeder.log" >&2; } || true
        fail "the self-seed environment exited before producing a torrent"
    fi
    sleep 1
done
[ -f "$SEED_OUT/selfseed.torrent" ] || fail "the self-seed environment produced no torrent within 240s"

echo "$CASE: first run — ingest, download and cache…"
timeout 120 docker run -d --name "$CONTAINER" \
    --device /dev/fuse --cap-add SYS_ADMIN \
    --security-opt apparmor=unconfined --network host \
    -v "$STATE_HOST:/state" \
    "$IMAGE" /mnt --cache /state/cache --db /state/db/metadata.db >/dev/null || {
    fail "docker run failed or did not return within 120s"
}
CONTAINERS+=("$CONTAINER")
wait_for_mount "$CONTAINER" 120 || fail "the FUSE mount did not become ready on the first run"

timeout 30 docker exec -i --user 1000:1000 "$CONTAINER" \
    sh -c 'cat > /mnt/metadata/selfseed.torrent' < "$SEED_OUT/selfseed.torrent" \
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
cmp -s "$SEED_OUT/payload.txt" "$WORK/first.bin" \
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

# The seeder (and its tracker) is gone before the restart: a cache miss would
# have no peer to download from and would stall on the no-seeder window.
if [ -n "$SEEDER_PID" ]; then
    for child in $(pgrep -P "$SEEDER_PID" 2>/dev/null || true); do
        kill "$child" 2>/dev/null || true
    done
    kill "$SEEDER_PID" 2>/dev/null || true
    SEEDER_PID=""
fi
sleep 2
if pgrep -f "torrentfs-selfseed-env" >/dev/null 2>&1; then
    echo "$CASE: WARNING — a self-seed process is still alive; the cache-hit read may see a peer" >&2
fi

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
EXPECTED_SIZE="$(stat -c '%s' "$SEED_OUT/payload.txt")"
[ "$SIZE" = "$EXPECTED_SIZE" ] \
    || fail "projection size after restart is $SIZE, expected $EXPECTED_SIZE"

# The FUSE layer re-verifies on-disk pieces asynchronously on startup: wait
# until every piece is marked cached before reading.
DEADLINE=$((SECONDS + 90))
PIECES_OK=0
STATS=""
while [ "$SECONDS" -lt "$DEADLINE" ]; do
    STATS="$(timeout 10 docker exec "$CONTAINER" cat /mnt/data/selfseed.torrent/.stats 2>/dev/null || true)"
    if all_pieces_cached "$STATS"; then
        PIECES_OK=1
        break
    fi
    sleep 1
done
if [ "$PIECES_OK" -ne 1 ]; then
    fail "cached pieces were not re-verified after the restart: $(grep -m1 '^  Pieces: ' <<<"$STATS" || echo 'no Pieces line')"
fi

BEFORE="$(cache_counts "$CONTAINER")"
[ -n "$BEFORE" ] || fail "the root .stats carries no cache hit/miss counters"
HITS_BEFORE="${BEFORE%% *}"
MISSES_BEFORE="${BEFORE##* }"

echo "$CASE: second read — must be served from the cache…"
START_NS="$(date +%s%N)"
READ_RC=0
timeout 60 docker exec "$CONTAINER" cat /mnt/data/selfseed.torrent/selfseed \
    >"$WORK/second.bin" 2>"$WORK/second.err" || READ_RC=$?
END_NS="$(date +%s%N)"
[ "$READ_RC" -eq 0 ] || fail "the second read failed with no seeder (rc=$READ_RC): $(tail -n 3 "$WORK/second.err")"
cmp -s "$SEED_OUT/payload.txt" "$WORK/second.bin" \
    || fail "the second read bytes differ from the authoritative payload"
ELAPSED_MS=$(( (END_NS - START_NS) / 1000000 ))
[ "$ELAPSED_MS" -lt 10000 ] \
    || fail "the second read took ${ELAPSED_MS}ms; a cache miss would stall on the no-seeder window"

AFTER="$(cache_counts "$CONTAINER")"
[ -n "$AFTER" ] || fail "the root .stats lost its cache hit/miss counters"
HITS_AFTER="${AFTER%% *}"
MISSES_AFTER="${AFTER##* }"
[ "$HITS_AFTER" -gt "$HITS_BEFORE" ] \
    || fail "cache hits did not grow on the second read (before=$HITS_BEFORE after=$HITS_AFTER)"
[ "$MISSES_AFTER" -eq "$MISSES_BEFORE" ] \
    || fail "the second read recorded cache misses (before=$MISSES_BEFORE after=$MISSES_AFTER)"
NEW_LOGS="$(timeout 10 docker logs "$CONTAINER" 2>&1 | tail -n +$((LOG_OFFSET + 1)) || true)"
if grep -q "no seeder connected" <<<"$NEW_LOGS"; then
    fail "the second read fell back to the swarm (a no-seeder cause was logged after the restart)"
fi

stop_gracefully "$CONTAINER"
echo "$CASE: PASS (torrent and cache survived stop/start; second read ${ELAPSED_MS}ms from cache, hits ${HITS_BEFORE}→${HITS_AFTER}, misses ${MISSES_BEFORE})"

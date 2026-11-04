#!/usr/bin/env bash
# Local swarm fixtures running inside the container under test.
#
# The container-level carriers execute the production image, whose runtime
# already carries Perl and coreutils (the same tools `lib_swarm.pl` and
# `lib_torrent_fixture.pl` need).  Starting the tracker and the seeder inside
# that container keeps the host side dependent on docker only: no build step
# on the host and no host-side Perl.  The daemon still talks to a real tracker
# and a real seeding peer over loopback.
#
# The container must bind two host directories when it is started:
#   $SWARM_HOST_DIR → /seed      read-write: payload, torrent, state files
#   <tests/acceptance> → /fixtures  read-only: the two fixture scripts
#
# Usage from a carrier (host side, once the container is running):
#   SWARM_HOST_DIR="$SEED_OUT"
#   source "$ROOT_DIR/tests/acceptance/lib_container_swarm.sh"
#   swarm_start "$CONTAINER" || fail "the local swarm fixtures did not start"
#   ... $SWARM_TORRENT / $SWARM_PAYLOAD / $SWARM_INFOHASH ...
#
# swarm_start sets: SWARM_PAYLOAD, SWARM_TORRENT, SWARM_INFOHASH,
# SWARM_PIECE_LENGTH, SWARM_PIECE_COUNT, SWARM_TOTAL_SIZE, SWARM_TRACKER_URL.
#
# The fixtures are processes inside the container, so they die with it.  A
# stop/start therefore restarts the daemon against an empty swarm, which is
# exactly what the cache-hit cases need.
# (No `set` here: the carriers that source this file own their own options.)

SWARM_FIXTURES="${SWARM_FIXTURES:-/fixtures}"
SWARM_SEED="${SWARM_SEED:-/seed}"
# 4 MiB in 256 KiB pieces: 16 pieces, the geometry the cache cases reason about.
SWARM_MIB="${SWARM_MIB:-4}"
SWARM_PIECE_LENGTH="${SWARM_PIECE_LENGTH:-262144}"

# swarm_start CONTAINER — synthesize payload, torrent, tracker and seeder
# inside CONTAINER.  Non-zero on any failure; the caller owns the message.
swarm_start() {
    local container="$1" i env_out

    [ -n "${SWARM_HOST_DIR:-}" ] || {
        echo "swarm fixture: FAIL — SWARM_HOST_DIR (the /seed bind source) is unset" >&2
        return 1
    }
    [ -n "$SWARM_FIXTURES" ] && [ -n "$SWARM_SEED" ] || {
        echo "swarm fixture: FAIL — SWARM_FIXTURES / SWARM_SEED must be non-empty" >&2
        return 1
    }
    SWARM_PAYLOAD="$SWARM_HOST_DIR/payload.bin"
    SWARM_TORRENT="$SWARM_HOST_DIR/selfseed.torrent"
    rm -f "$SWARM_PAYLOAD" "$SWARM_TORRENT" \
        "$SWARM_HOST_DIR/tracker.port" "$SWARM_HOST_DIR/tracker.state" \
        "$SWARM_HOST_DIR/peer.port" "$SWARM_HOST_DIR/peer.state" \
        "$SWARM_HOST_DIR/tracker.log" "$SWARM_HOST_DIR/peer.log"

    # Deterministic non-repeating payload, the same generator the in-container
    # carriers use: a read from the wrong offset cannot compare equal.
    timeout 120 docker exec "$container" perl -e '
        my $n = $ARGV[0];
        for my $i (0 .. $n - 1) {
            print chr((($i * 37) ^ ($i >> 8) ^ ($i >> 16)) & 0xFF);
        }' "$((SWARM_MIB * 1024 * 1024))" > "$SWARM_PAYLOAD" || {
        echo "swarm fixture: FAIL — payload generation failed" >&2
        return 1
    }
    [ -s "$SWARM_PAYLOAD" ] || {
        echo "swarm fixture: FAIL — the payload is empty" >&2
        return 1
    }

    # Tracker first: the torrent's announce URL needs the port it publishes.
    timeout 30 docker exec -d "$container" sh -c '
        fixtures="$1"; seed="$2"
        exec perl "$fixtures/lib_swarm.pl" tracker \
            --port-file "$seed/tracker.port" --state-file "$seed/tracker.state" \
            --interval 5 > "$seed/tracker.log" 2>&1
    ' sh "$SWARM_FIXTURES" "$SWARM_SEED" || return 1
    for ((i = 0; i < 100; i++)); do
        [ -s "$SWARM_HOST_DIR/tracker.port" ] && break
        sleep 0.1
    done
    [ -s "$SWARM_HOST_DIR/tracker.port" ] || {
        echo "swarm fixture: FAIL — the tracker published no port" >&2
        tail -n 5 "$SWARM_HOST_DIR/tracker.log" >&2 2>/dev/null || true
        return 1
    }
    SWARM_TRACKER_URL="$(sed -n 's/^URL=//p' "$SWARM_HOST_DIR/tracker.port")"
    [ -n "$SWARM_TRACKER_URL" ] || {
        echo "swarm fixture: FAIL — the tracker published no announce URL" >&2
        return 1
    }

    # The torrent declares the payload's piece hashes and the tracker above, so
    # the daemon verifies every byte it downloads against it.
    env_out="$(timeout 120 docker exec "$container" \
        perl "$SWARM_FIXTURES/lib_torrent_fixture.pl" make \
        --out "$SWARM_SEED/selfseed.torrent" --name selfseed \
        --content "$SWARM_SEED/payload.bin" \
        --piece-length "$SWARM_PIECE_LENGTH" \
        --tracker "$SWARM_TRACKER_URL")" || {
        echo "swarm fixture: FAIL — torrent construction failed" >&2
        return 1
    }
    eval "$env_out"
    SWARM_INFOHASH="$INFOHASH"
    SWARM_PIECE_COUNT="$PIECE_COUNT"
    SWARM_TOTAL_SIZE="$TOTAL_SIZE"
    SWARM_PIECE_LENGTH="$PIECE_LENGTH"
    [ -s "$SWARM_TORRENT" ] || {
        echo "swarm fixture: FAIL — no torrent at $SWARM_TORRENT" >&2
        return 1
    }

    timeout 30 docker exec -d "$container" sh -c '
        fixtures="$1"; seed="$2"; hash="$3"; plen="$4"; announce="$5"
        exec perl "$fixtures/lib_swarm.pl" peer --info-hash "$hash" \
            --piece-length "$plen" --content "$seed/payload.bin" \
            --port-file "$seed/peer.port" --state-file "$seed/peer.state" \
            --announce "$announce" > "$seed/peer.log" 2>&1
    ' sh "$SWARM_FIXTURES" "$SWARM_SEED" "$SWARM_INFOHASH" "$SWARM_PIECE_LENGTH" "$SWARM_TRACKER_URL" \
        || return 1
    for ((i = 0; i < 100; i++)); do
        [ -s "$SWARM_HOST_DIR/peer.port" ] && break
        sleep 0.1
    done
    [ -s "$SWARM_HOST_DIR/peer.port" ] || {
        echo "swarm fixture: FAIL — the seeder published no port" >&2
        tail -n 5 "$SWARM_HOST_DIR/peer.log" >&2 2>/dev/null || true
        return 1
    }

    # The seeder announces itself on start; wait until the tracker holds it, so
    # the daemon's own announce is answered with a reachable peer.
    for ((i = 0; i < 200; i++)); do
        grep -q "^peer $SWARM_INFOHASH " "$SWARM_HOST_DIR/tracker.state" 2>/dev/null \
            && return 0
        sleep 0.1
    done
    echo "swarm fixture: FAIL — the seeder never registered with the tracker" >&2
    return 1
}

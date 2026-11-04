#!/usr/bin/env bash
# Shared environment for the container-driven acceptance carriers.
#
# The containers run the released binary at /usr/local/bin/torrentfs only: no
# toolchain build, no python.  This file gives those carriers the pieces every case
# needs — a scratch tree, a daemon lifecycle, a config file, the synthesized
# .torrent shapes and the local tracker/peer swarm (lib_torrent_fixture.pl,
# lib_swarm.pl) — so each carrier keeps its own scenario and assertions.
#
# Usage from a carrier:
#   ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
#   BIN="${1:-/usr/local/bin/torrentfs}"
#   MNT="${2:-/tmp/torrentfs_acceptance_<tc>}"
#   source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
#   env_init <tc-id>
#
# Exit codes: 0 pass, 1 fail, 2 skip (no FUSE in this environment).
set -uo pipefail

ACCEPTANCE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TORRENT_FIXTURE="$ACCEPTANCE_LIB_DIR/lib_torrent_fixture.pl"
SWARM_FIXTURE="$ACCEPTANCE_LIB_DIR/lib_swarm.pl"

: "${ROOT_DIR:?ROOT_DIR must be set before sourcing lib_acceptance_env.sh}"
: "${BIN:=/usr/local/bin/torrentfs}"
: "${MNT:=/tmp/torrentfs_acceptance_mnt}"

fail() {
    echo "${TC_ID:-acceptance}: FAIL — $*" >&2
    exit 1
}

skip() {
    echo "${TC_ID:-acceptance}: SKIP — $*" >&2
    exit 2
}

# ── Environment gate ────────────────────────────────────────────────────
require_fuse() {
    [ -c /dev/fuse ] || skip "no /dev/fuse in this environment"
    command -v fusermount3 >/dev/null 2>&1 || command -v fusermount >/dev/null 2>&1 \
        || skip "fusermount not found"
    [ -x "$BIN" ] || fail "binary not executable: $BIN"
}

unmount_path() {
    fusermount3 -u -z "$1" >/dev/null 2>&1 \
        || fusermount -u -z "$1" >/dev/null 2>&1 || true
}

# ── Scratch tree ────────────────────────────────────────────────────────
PIDS=()
PEER_PIDS=()
PEER_STATES=()

env_init() {
    TC_ID="${1:?env_init needs the TC id}"
    W="$(mktemp -d)"
    MNT="${MNT:-/tmp/torrentfs_acceptance_$TC_ID}"
    CACHE_DIR="$W/cache"
    STATE_DIR="$W/state"
    DB_FILE="$STATE_DIR/metadata.db"
    LOG_FILE="$W/daemon.log"
    CONFIG_FILE="$W/config.toml"
    PAYLOAD="$W/payload"
    TORRENT_FILES="$W/torrents"
    mkdir -p "$MNT" "$CACHE_DIR" "$STATE_DIR" "$PAYLOAD" "$TORRENT_FILES"
    trap env_cleanup EXIT
}

env_cleanup() {
    local pid dir
    for pid in "${PIDS[@]:-}"; do
        [ -n "$pid" ] && kill "$pid" >/dev/null 2>&1 || true
    done
    # Cases that drive several daemons keep each mount under $W; unmount them
    # all before the scratch tree is removed.
    for dir in "$MNT" "$W"/mnt*; do
        [ -d "$dir" ] && unmount_path "$dir"
    done
    sleep 0.3
    rm -rf "$W" "$MNT"
}

track_pid() {
    PIDS+=("$1")
}

wait_mount() {
    local mnt="${1:-$MNT}" limit="${2:-120}" i
    for ((i = 0; i < limit; i++)); do
        mountpoint -q "$mnt" && return 0
        sleep 0.25
    done
    return 1
}

wait_unmount() {
    local mnt="${1:-$MNT}" limit="${2:-120}" i
    for ((i = 0; i < limit; i++)); do
        mountpoint -q "$mnt" || return 0
        sleep 0.25
    done
    return 1
}

# wait_path PATH SECONDS — first wait is short because a projected entry shows
# up as soon as the torrent is persisted.
wait_path() {
    local path="$1" limit="${2:-60}" i
    for ((i = 0; i < limit * 4; i++)); do
        [ -e "$path" ] && return 0
        sleep 0.25
    done
    return 1
}

# ── Config ──────────────────────────────────────────────────────────────
# write_config <<TOML ... TOML — the file lands at $CONFIG_FILE.
write_config() {
    cat > "$CONFIG_FILE"
}

# Short waits keep each case bounded; the peer-discovery window still outlasts
# a local tracker's first announce.
write_fast_config() {
    write_config <<'TOML'
[timeouts]
read_timeout_secs = 20
peer_discovery_wait_secs = 12
no_seeder_read_timeout_secs = 4
[connections]
listen_interfaces = "127.0.0.1:0"
[local_discovery]
lsd_enabled = false
upnp_enabled = false
natpmp_enabled = false
[dht]
enabled = false
TOML
}

# ── Daemon ──────────────────────────────────────────────────────────────
# daemon_start [extra daemon args...]
daemon_start() {
    local args=(--cache "$CACHE_DIR" --db "$DB_FILE")
    [ -f "$CONFIG_FILE" ] && args+=(--config "$CONFIG_FILE")
    : > "$LOG_FILE"
    "$BIN" "$MNT" "${args[@]}" "$@" > "$LOG_FILE" 2>&1 &
    DAEMON_PID=$!
    track_pid "$DAEMON_PID"
    wait_mount "$MNT" || {
        tail -n 20 "$LOG_FILE" >&2
        fail "FUSE mount did not appear on $MNT"
    }
}

# daemon_start_bare [daemon args...] — no cache/db/config injection, so a case
# can assert the daemon's own default/derived behaviour.
daemon_start_bare() {
    : > "$LOG_FILE"
    "$BIN" "$MNT" "$@" > "$LOG_FILE" 2>&1 &
    DAEMON_PID=$!
    track_pid "$DAEMON_PID"
    wait_mount "$MNT" || {
        tail -n 20 "$LOG_FILE" >&2
        fail "FUSE mount did not appear on $MNT"
    }
}

# daemon_stop [signal] — stops the daemon and waits for the mount to go away.
daemon_stop() {
    local sig="${1:-TERM}"
    [ -n "${DAEMON_PID:-}" ] || return 0
    kill "-$sig" "$DAEMON_PID" >/dev/null 2>&1 || true
    wait_unmount "$MNT" 120 || fail "mount did not unmount after SIG$sig"
    wait "$DAEMON_PID" >/dev/null 2>&1 || true
    DAEMON_PID=""
}

# Remount on the same state/cache: the case keeps its payload and re-ingests.
daemon_restart() {
    daemon_stop
    daemon_start
}

# ── Torrent fixtures ────────────────────────────────────────────────────
# build_torrent <out> [fixture args...] — sets TORRENT, INFOHASH,
# PIECE_LENGTH, TOTAL_SIZE, PIECE_COUNT.
build_torrent() {
    local out="$1"
    shift
    local out_json
    out_json="$(perl "$TORRENT_FIXTURE" make --out "$out" "$@")" \
        || fail "torrent fixture build failed"
    eval "$out_json"
    TORRENT="$out"
}

# payload_file NAME BYTES CHAR — deterministic content file.
payload_file() {
    local path="$PAYLOAD/$1"
    mkdir -p "$(dirname "$path")"
    head -c "$2" /dev/zero | tr '\0' "$3" > "$path"
    printf '%s\n' "$path"
}

# ── Swarm ───────────────────────────────────────────────────────────────
# tracker_start [--interval N] — sets TRACKER_URL, TRACKER_STATE.
tracker_start() {
    TRACKER_STATE="$W/tracker.state"
    local port_file="$W/tracker.port"
    rm -f "$port_file" "$TRACKER_STATE"
    perl "$SWARM_FIXTURE" tracker --port-file "$port_file" \
        --state-file "$TRACKER_STATE" "$@" &
    TRACKER_PID=$!
    track_pid "$TRACKER_PID"
    local i
    for ((i = 0; i < 100; i++)); do
        [ -s "$port_file" ] && break
        sleep 0.1
    done
    [ -s "$port_file" ] || fail "local tracker did not start"
    TRACKER_URL="$(sed -n 's/^URL=//p' "$port_file")"
    [ -n "$TRACKER_URL" ] || fail "local tracker published no announce URL"
}

# tracker_announces — announces handled so far.
tracker_announces() {
    sed -n 's/^announces=//p' "$TRACKER_STATE" 2>/dev/null | head -n1
}

# tracker_peers <info_hash> — peer entries the tracker holds for that hash.
tracker_peers() {
    grep -c "^peer $1 " "$TRACKER_STATE" 2>/dev/null || true
}

# peer_start <info_hash> <piece_length> <content...> [--peer options...]
# Sets PEER_PORT_FILE / PEER_STATE.  The peer announces itself to $TRACKER_URL.
peer_start() {
    local hash="$1" plen="$2"
    shift 2
    local content=() arg
    while [ $# -gt 0 ] && [ "${1#--}" = "$1" ]; do
        content+=("$1")
        shift
    done
    PEER_STATE="$W/peer.$$.$RANDOM.state"
    PEER_PORT_FILE="$W/peer.$$.$RANDOM.port"
    local args=(--info-hash "$hash" --piece-length "$plen"
        --port-file "$PEER_PORT_FILE" --state-file "$PEER_STATE")
    if [ -n "${TRACKER_URL:-}" ]; then
        args+=(--announce "$TRACKER_URL")
    fi
    for arg in "${content[@]}"; do
        args+=(--content "$arg")
    done
    perl "$SWARM_FIXTURE" peer "${args[@]}" "$@" &
    PEER_PID=$!
    PEER_PIDS+=("$PEER_PID")
    PEER_STATES+=("$PEER_STATE")
    track_pid "$PEER_PID"
    local i
    for ((i = 0; i < 100; i++)); do
        [ -s "$PEER_PORT_FILE" ] && break
        sleep 0.1
    done
    [ -s "$PEER_PORT_FILE" ] || fail "fixture peer did not start"
    PEER_PORT="$(sed -n 's/^PORT=//p' "$PEER_PORT_FILE")"
}

peer_stop() {
    local pid
    for pid in "${PEER_PIDS[@]:-}"; do
        [ -n "$pid" ] || continue
        kill "$pid" >/dev/null 2>&1 || true
        wait "$pid" >/dev/null 2>&1 || true
    done
    PEER_PIDS=()
    PEER_STATES=()
    PEER_PID=""
}

# peer_served — PIECE replies the fixture peer sent so far.
peer_served() {
    sed -n 's/^served=//p' "$PEER_STATE" 2>/dev/null | head -n1
}

# wait_peer_known <info_hash> <count> — the tracker holds at least `count`
# peers for the hash (the fixture announced and the client can discover it).
wait_peer_known() {
    local hash="$1" want="${2:-1}" i got
    for ((i = 0; i < 100; i++)); do
        got="$(tracker_peers "$hash")"
        [ "${got:-0}" -ge "$want" ] && return 0
        sleep 0.1
    done
    return 1
}

# wait_projection REL — the projected torrent's leaf `.stats` has rendered its
# whole block and the entry set below the root is unchanged for a full second.
# The projection publishes the root first and fills its entries a few ms later,
# and can rebuild the subtree once the torrent's handle settles; a carrier that
# starts right after `ingest` would otherwise see a missing entry (ENOENT), a
# half-written `.stats` (empty fields), or a subtree that is briefly gone.  The
# snapshot's *content* is not compared: live counters change between reads.
wait_projection() {
    local rel="$1" root="$MNT/data/$1" i prev_tree="" tree now stable_since=""
    wait_path "$root/.stats" 60 || fail "projection $rel/.stats did not appear"
    for ((i = 0; i < 200; i++)); do
        grep -q '^  PieceCount: ' "$root/.stats" 2>/dev/null && break
        sleep 0.05
    done
    grep -q '^  PieceCount: ' "$root/.stats" 2>/dev/null \
        || fail "projection $rel/.stats never finished rendering"
    for ((i = 0; i < 400; i++)); do
        tree="$(find "$root" -mindepth 1 -maxdepth 2 ! -name .stats 2>/dev/null | wc -l)"
        now="$(date +%s%N)"
        if [ "$tree" -gt 0 ] && [ "$tree" = "$prev_tree" ]; then
            [ -n "$stable_since" ] || stable_since="$now"
            if [ $(( (now - stable_since) / 1000000 )) -ge 1000 ]; then
                return 0
            fi
        else
            stable_since=""
        fi
        prev_tree="$tree"
        sleep 0.05
    done
    return 1
}

# ingest <torrent-file> [dest] — copy a torrent into metadata/ and wait for the
# projected content root.  The projection mirrors the destination's path below
# metadata/, so a dest in a subtree is waited for at the same relative path.
ingest() {
    local src="$1" dest="${2:-$MNT/metadata/$(basename "$1")}" rel
    mkdir -p "$(dirname "$dest")"
    cp "$src" "$dest"
    rel="${dest#"$MNT/metadata"/}"
    wait_path "$MNT/data/$rel" 120 || fail "projection data/$rel did not appear"
    wait_projection "$rel" || fail "projection data/$rel did not finish publishing"
    printf '%s\n' "$MNT/data/$rel"
}

# read_retry SRC DST [ATTEMPTS] — read until it succeeds.  A read issued while
# the swarm is still delivering fails with ENODATA; retrying models the caller.
read_retry() {
    local src="$1" dest="$2" attempts="${3:-3}" i
    for ((i = 0; i < attempts; i++)); do
        if cat "$src" > "$dest" 2>"$W/read.err"; then return 0; fi
        sleep 1
    done
    return 1
}

# slice SRC OFFSET LEN DST — the authoritative byte range OFFSET..OFFSET+LEN.
slice() {
    dd if="$1" of="$4" bs=65536 skip="$2" count="$3" \
        iflag=skip_bytes,count_bytes status=none
}

# digest FILE — sha256 of a file.
digest() {
    sha256sum < "$1" | cut -d' ' -f1
}

# payload_patterned NAME BYTES — deterministic non-repeating bytes, so a read
# from the wrong offset cannot pass a digest comparison.
payload_patterned() {
    local path="$PAYLOAD/$1"
    mkdir -p "$(dirname "$path")"
    perl -e 'for my $i (0 .. $ARGV[0] - 1) { print chr((($i * 37) ^ ($i >> 8) ^ ($i >> 16)) & 0xFF) }' \
        "$2" > "$path"
    printf '%s\n' "$path"
}

# piece_markers STATS_FILE — print the `-- Pieces --` marker run, one token per
# line, so a case can assert the four-state grammar without a JSON parser.
piece_markers() {
    perl -e '
        local $/;
        my $text = <STDIN>;
        for my $line (split /\n/, $text) {
            next unless $line =~ /^  Pieces: (.*)$/;
            my $run = $1;
            print "$_\n" for $run =~ /(\[[^\]]*\])/g;
            last;
        }' < "$1"
}

# to_bytes "4.00 MB" — a rendered byte count as an integer, for growth checks.
to_bytes() {
    local value="$1" num unit
    num="${value%% *}"
    unit="${value##* }"
    case "$unit" in
        B) printf '%d\n' "${num%%.*}" ;;
        KB) awk -v n="$num" 'BEGIN { printf "%d", n * 1024 }' ;;
        MB) awk -v n="$num" 'BEGIN { printf "%d", n * 1048576 }' ;;
        GB) awk -v n="$num" 'BEGIN { printf "%d", n * 1073741824 }' ;;
        *) printf '0\n' ;;
    esac
}

# wait_seeds LEAF_STATS MIN SECONDS — block until the leaf `.stats` reports at
# least MIN connected seeds.  Classification branches on live seed counts, so a
# case that depends on a seeder being connected must see it connected first.
wait_seeds() {
    local leaf="$1" want="${2:-1}" limit="${3:-15}" i seeds
    for ((i = 0; i < limit * 4; i++)); do
        seeds="$(sed -n 's/^  Peers: [0-9]*  Seeds: \([0-9]*\).*/\1/p' "$leaf" 2>/dev/null | head -n1)"
        [ "${seeds:-0}" -ge "$want" ] && return 0
        sleep 0.25
    done
    return 1
}

# ── Two-scale contrast preconditions ────────────────────────────────────
# `docs/03-acceptance.md` §验收范围规则: the AC-91 scale contrast only has
# judgement power when the per-request fixed overhead is calibrated on the
# target machine with a small directory whose scan cost is negligible, and the
# full scan's added cost is at least twice that overhead.  The scale cases
# calibrate inside the instance under test (same mount, same walker) and gate
# on it; a measurement short of the margin is aborted, never recorded as PASS.
scan_added_ms() {
    local fixed="$1" scan="$2"
    [ "$fixed" -gt 0 ] || fixed=1
    echo $(( scan - fixed ))
}

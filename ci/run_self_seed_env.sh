#!/usr/bin/env bash
# Self-seeding QA environment for torrentfs. Public sample torrents usually
# have no reachable seeders, so this builds a deterministic local swarm —
# a local HTTP tracker, a single-file 4 MiB torrent, and a libtorrent seeder —
# for real downloads without external infrastructure (no DHT/LSD/UPnP/NAT-PMP/
# public trackers). By default the tracker binds 0.0.0.0 and the announce host
# is the host's primary non-loopback IPv4, so libtorrent's per-interface
# announces (it expands a wildcard listen interface into one socket per local
# address) can reach the tracker on multi-interface hosts; a loopback-only
# fallback (127.0.0.1/127.0.0.1) is used when no non-loopback address is
# detectable. NOTE: binding 0.0.0.0 exposes the unauthenticated tracker to the
# LAN (any host may query or inject peers); acceptable for a synthesized QA
# payload, but pass --tracker-bind 127.0.0.1 to stay loopback-only.
# Usage: ./ci/run_self_seed_env.sh [--payload-mib N] [--payload-gib N] [--port PORT]
#        [--tracker-bind IP] [--announce-host IP] [--output-dir DIR]
#        (--port defaults to 0: the OS picks a free port, published in tracker.url)
#        → outputs under ci/selfseed/output, or DIR when --output-dir is given

set -euo pipefail

# Announce host written into the .torrent (and reached by the seeder/client).
# First probe: the default-route source IPv4 — the interface carrying the
# default route (skipping VPN/bridge interfaces that don't). Second probe: the
# first global IPv4 (best-effort when there is no default route; may itself be
# a VPN/bridge address). Loopback as a last resort when `ip` is absent or no
# non-loopback address exists.
detect_announce_host() {
    local ip=""
    if command -v ip >/dev/null 2>&1; then
        ip="$(ip -4 route show default 2>/dev/null \
            | sed -n 's/.* src \([0-9][0-9.]*\).*/\1/p' | head -n1)"
        [ -n "$ip" ] || ip="$(ip -4 -o addr show scope global 2>/dev/null \
            | sed -n 's/.* inet \([0-9][0-9.]*\)\/.*/\1/p' | head -n1)"
    fi
    [ -n "$ip" ] || ip="127.0.0.1"
    printf '%s\n' "$ip"
}

# Validate VALUE is a positive decimal integer ≤ MAX. Leading zeros are
# stripped and the bound is compared by digit count then lexicographic order,
# so out-of-range values never reach Bash's signed 64-bit arithmetic (which
# wraps silently). Invalid input prints a self-seed: error and exits 2.
validate_size_arg() {
    local flag="$1" value="$2" max="$3" unit="$4" digits
    case "$value" in
        *[!0-9]*) echo "self-seed: $flag must be a positive integer" >&2; exit 2 ;;
    esac
    digits="${value#"${value%%[!0]*}"}"
    if [ -z "$digits" ]; then
        echo "self-seed: $flag must be > 0" >&2; exit 2
    fi
    if [ "${#digits}" -gt "${#max}" ] \
        || { [ "${#digits}" -eq "${#max}" ] && [[ "$digits" > "$max" ]]; }; then
        echo "self-seed: $flag too large (max $max $unit)" >&2; exit 2
    fi
}

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_DIR="$SCRIPT_DIR/selfseed"
OUTPUT_DIR="$ENV_DIR/output"
PAYLOAD_MIB=4
PAYLOAD_GIB=""
# Caps keep payload bytes within signed 64-bit (2^43 MiB = 2^33 GiB = 2^63
# bytes), so the later * 1024*1024 / * 1024*1024*1024 arithmetic cannot wrap.
MAX_PAYLOAD_MIB=8796093022207
MAX_PAYLOAD_GIB=8589934591
# 0 = OS-assigned free ephemeral port, reported in the announce URL the seeder
# writes.  A fixed port would collide with a tracker left behind by a killed
# run (the stale process holds it in LISTEN, which no SO_REUSEADDR relaxes).
TRACKER_PORT=0
TRACKER_BIND=""
ANNOUNCE_HOST=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --payload-mib) PAYLOAD_MIB="$2"; shift 2 ;;
        --payload-gib) PAYLOAD_GIB="$2"; shift 2 ;;
        --port) TRACKER_PORT="$2"; shift 2 ;;
        --tracker-bind) TRACKER_BIND="$2"; shift 2 ;;
        --announce-host) ANNOUNCE_HOST="$2"; shift 2 ;;
        --output-dir)
            # `$# -lt 2` catches a missing value (would otherwise trip `set -u`
            # as an unbound-variable exit 1); `-z` catches an explicit empty
            # value, which must not silently fall back to the caller's cwd.
            if [[ $# -lt 2 || -z "$2" ]]; then
                echo "self-seed: --output-dir requires a non-empty path" >&2
                exit 2
            fi
            # Anchor a relative path to the invocation cwd now, before the
            # `cd "$ROOT_DIR"` below, so the seeder (which runs from ROOT_DIR)
            # writes to the same place mkdir created.
            case "$2" in
                /*) OUTPUT_DIR="$2" ;;
                *) OUTPUT_DIR="$PWD/$2" ;;
            esac
            shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

# Resolve the tracker-bind/announce-host pair so the announce URL always points
# at an address the tracker actually listens on. A concrete --tracker-bind
# (e.g. 127.0.0.1) pins the announce host to the same address; a wildcard/unset
# bind listens on every interface and announces a detected primary IPv4 (the
# loopback fallback keeps the old loopback-only default when undetectable).
if [ -n "$TRACKER_BIND" ] && [ "$TRACKER_BIND" != "0.0.0.0" ]; then
    if [ -z "$ANNOUNCE_HOST" ]; then
        ANNOUNCE_HOST="$TRACKER_BIND"
    elif [ "$ANNOUNCE_HOST" != "$TRACKER_BIND" ]; then
        echo "self-seed: --announce-host '$ANNOUNCE_HOST' is unreachable at --tracker-bind '$TRACKER_BIND'; the announce host must match a concrete tracker bind" >&2
        exit 2
    fi
else
    if [ -z "$ANNOUNCE_HOST" ]; then
        ANNOUNCE_HOST="$(detect_announce_host)"
    fi
    if [ -z "$TRACKER_BIND" ]; then
        if [ "$ANNOUNCE_HOST" = "127.0.0.1" ]; then
            TRACKER_BIND="127.0.0.1"
        else
            TRACKER_BIND="0.0.0.0"
        fi
    fi
fi

# The self-seed tracker is IPv4-only (`ci/selfseed_env.rs`): its announce
# handler accepts only V4 peer addresses and the announce URL is written
# without IPv6 brackets, so an IPv6 literal would silently produce a broken
# swarm.  Reject it up front instead of letting the passthrough fail downstream.
if [[ "$TRACKER_BIND" == *:* ]]; then
    echo "IPv6 not supported by the self-seed tracker: --tracker-bind '$TRACKER_BIND'" >&2
    exit 2
fi
if [[ "$ANNOUNCE_HOST" == *:* ]]; then
    echo "IPv6 not supported by the self-seed tracker: --announce-host '$ANNOUNCE_HOST'" >&2
    exit 2
fi

# Validate payload sizes before any side effect (creating OUTPUT_DIR or
# building the seeder) so an invalid value fails with no directory/build work.
validate_size_arg "--payload-mib" "$PAYLOAD_MIB" "$MAX_PAYLOAD_MIB" "MiB"
if [ -n "$PAYLOAD_GIB" ]; then
    validate_size_arg "--payload-gib" "$PAYLOAD_GIB" "$MAX_PAYLOAD_GIB" "GiB"
fi

# Resolve a *working* cargo by probing `--version` on every PATH candidate (in
# order) and then the rustup default location.  `--version` rejects broken
# rustup shims (toolchain without the cargo component) that shadow a working
# cargo further down PATH, and rejects directories mistaken for executables.
CARGO=""
try_cargo() {
    local candidate="$1"
    [ -n "$candidate" ] && [ -f "$candidate" ] && [ -x "$candidate" ] || return 1
    "$candidate" --version >/dev/null 2>&1 || return 1
    CARGO="$candidate"
}

old_ifs=$IFS
IFS=:
for dir in ${PATH:-}; do
    [ -n "$dir" ] || continue
    if try_cargo "$dir/cargo"; then break; fi
done
IFS=$old_ifs
if [ -z "$CARGO" ]; then try_cargo "$HOME/.cargo/bin/cargo" \
    || { echo "no working cargo in PATH or $HOME/.cargo/bin/cargo" >&2; exit 1; }; fi

mkdir -p "$OUTPUT_DIR"
cd "$ROOT_DIR"

echo "[selfseed] building seeder (release)…"
"$CARGO" build --locked --release --example torrentfs-selfseed-env --quiet

if [ -n "$PAYLOAD_GIB" ]; then
    echo "[selfseed] generating ${PAYLOAD_GIB} GiB sparse (all-zero) payload…"
    truncate -s $((10#$PAYLOAD_GIB * 1024 * 1024 * 1024)) "$OUTPUT_DIR/payload.txt"
else
    echo "[selfseed] generating ${PAYLOAD_MIB} MiB deterministic payload…"
    head -c $((10#$PAYLOAD_MIB * 1024 * 1024)) /dev/zero \
        | tr '\0' 'a' > "$OUTPUT_DIR/payload.txt"
fi

echo "[selfseed] creating torrent + starting tracker…"
SEED_ARGS=( \
    --payload "$OUTPUT_DIR/payload.txt" \
    --tracker-port "$TRACKER_PORT" \
)
if [ -n "$TRACKER_BIND" ]; then
    SEED_ARGS+=(--tracker-bind "$TRACKER_BIND")
fi
if [ -n "$ANNOUNCE_HOST" ]; then
    SEED_ARGS+=(--announce-host "$ANNOUNCE_HOST")
fi
SEED_ARGS+=( \
    --torrent-out "$OUTPUT_DIR/selfseed.torrent" \
    --url-out "$OUTPUT_DIR/tracker.url" \
)
"$ROOT_DIR/target/release/examples/torrentfs-selfseed-env" "${SEED_ARGS[@]}"

echo ""
echo "════════════════════════════════════════════════════════════"
echo " Self-seed environment ready"
echo "   torrent : $OUTPUT_DIR/selfseed.torrent"
echo "   payload : $OUTPUT_DIR/payload.txt ($(wc -c < "$OUTPUT_DIR/payload.txt") bytes)"
echo "   tracker : $(cat "$OUTPUT_DIR/tracker.url")"
echo ""
echo " Next steps:"
echo "   1. cp '$OUTPUT_DIR/selfseed.torrent' <mountpoint>/metadata/"
echo "   2. cat <mountpoint>/data/selfseed/selfseed   # served by the local seeder"
echo ""
echo " Stop with Ctrl-C (or kill this shell)."
echo "════════════════════════════════════════════════════════════"
echo ""

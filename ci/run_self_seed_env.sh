#!/usr/bin/env bash
# Self-seeding QA environment for torrentfs (TSI-2418).
#
# The public Ubuntu/Debian .torrent samples under /workspace/testdata/torrents/
# routinely have zero reachable seeders on the current network, which makes any
# read against them fail with the (correct) ENODATA/NoPeers error.  This script
# builds a deterministic, self-contained test swarm so QA can exercise real
# downloads without depending on external infrastructure:
#
#   - a local HTTP tracker on 127.0.0.1:<TRACKER_PORT>
#   - a single-file torrent built from a fixed 4 MiB payload
#   - a libtorrent seeder serving that payload from ci/selfseed/seed_data/
#
# Everything is loopback-only: no DHT, no LSD, no UPnP, no NAT-PMP, no public
# trackers.  Run this before starting torrentfs, then drop
# ./output/selfseed.torrent into the mounted torrentfs directory and read files
# through the mount — pieces are served by the local seeder.
#
# Usage:
#   ./ci/run_self_seed_env.sh [--payload-mib N] [--port PORT]
#
# Outputs (relative to the repo's ci/selfseed/ directory):
#   output/selfseed.torrent — the .torrent to copy into torrentfs
#   output/payload.txt      — exact expected content (for diffing)
#   output/tracker.url      — announce URL of the local tracker

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
ENV_DIR="$SCRIPT_DIR/selfseed"
OUTPUT_DIR="$ENV_DIR/output"
PAYLOAD_MIB=4
TRACKER_PORT=16969

while [[ $# -gt 0 ]]; do
    case "$1" in
        --payload-mib) PAYLOAD_MIB="$2"; shift 2 ;;
        --port) TRACKER_PORT="$2"; shift 2 ;;
        *) echo "unknown option: $1" >&2; exit 2 ;;
    esac
done

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

echo "[selfseed] generating ${PAYLOAD_MIB} MiB deterministic payload…"
head -c $((PAYLOAD_MIB * 1024 * 1024)) /dev/zero \
    | tr '\0' 'a' > "$OUTPUT_DIR/payload.txt"

echo "[selfseed] creating torrent + starting tracker…"
"$ROOT_DIR/target/release/examples/torrentfs-selfseed-env" \
    --payload "$OUTPUT_DIR/payload.txt" \
    --tracker-port "$TRACKER_PORT" \
    --torrent-out "$OUTPUT_DIR/selfseed.torrent" \
    --url-out "$OUTPUT_DIR/tracker.url"

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

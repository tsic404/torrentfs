#!/usr/bin/env bash
# Design acceptance case TC-stats-02: a torrent-root `.stats` exists with the
# single-torrent title and Status / Rates / Peers / Cache / `info_hash`
# sections; two different torrents produce different content
# (`docs/03-acceptance.md` §E AC-41).
#
# FUSE-level executable form.  Requires a FUSE-capable environment, the
# release binary.
#
# Usage: ./tests/acceptance/tc_stats_02.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats02}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

TORRENT_A="$(mktemp)"
TORRENT_B="$(mktemp)"
trap 'rm -f "$TORRENT_A" "$TORRENT_B"; cleanup' EXIT
make_seed_torrent "$TORRENT_A" "alpha.bin"
make_seed_torrent "$TORRENT_B" "beta.bin"

cp "$TORRENT_A" "$MNT/metadata/alpha.torrent"
cp "$TORRENT_B" "$MNT/metadata/beta.torrent"

for _ in $(seq 1 60); do
    [ -e "$MNT/data/alpha.torrent/.stats" ] && [ -e "$MNT/data/beta.torrent/.stats" ] && break
    sleep 1
done
[ -e "$MNT/data/alpha.torrent/.stats" ] || { echo "TC-stats-02: FAIL — alpha .stats missing" >&2; exit 1; }
[ -e "$MNT/data/beta.torrent/.stats" ] || { echo "TC-stats-02: FAIL — beta .stats missing" >&2; exit 1; }

ALPHA="$(cat "$MNT/data/alpha.torrent/.stats")"
BETA="$(cat "$MNT/data/beta.torrent/.stats")"

# Title line for the single torrent.
grep -q '^===== torrent: alpha.bin =====' <<<"$ALPHA" || {
    echo "TC-stats-02: FAIL — alpha .stats missing torrent title" >&2
    exit 1
}

# Section headers present.  The torrent .stats carries `-- Status --`,
# `-- Rates --`, `-- Peers --` headers, a `Cache:` line and `info_hash:`.
for section in "-- Status --" "-- Rates --" "-- Peers --" "Cache:" "info_hash:"; do
    if ! grep -qFe "$section" <<<"$ALPHA"; then
        echo "TC-stats-02: FAIL — alpha .stats missing section '$section'" >&2
        exit 1
    fi
done

# Two distinct torrents → different .stats content.
[ "$ALPHA" != "$BETA" ] || {
    echo "TC-stats-02: FAIL — two torrents' .stats must differ" >&2
    exit 1
}

echo "TC-stats-02: PASS (torrent title + Status/Rates/Peers/Cache/info_hash; two torrents differ)"

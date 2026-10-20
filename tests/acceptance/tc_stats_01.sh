#!/usr/bin/env bash
# Design acceptance case TC-stats-01: the root `.stats` exists as a `0444`
# regular file carrying the Overview / Global Rates / Connections / Torrents /
# Cache sections, with no single-torrent title line (`docs/03-acceptance.md`
# §E AC-40).
#
# FUSE-level executable form.  Requires a FUSE-capable environment and the
# release binary.
#
# Usage: ./tests/acceptance/tc_stats_01.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats01}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"

STATS="$MNT/.stats"

[ -f "$STATS" ] || { echo "TC-stats-01: FAIL — root .stats missing" >&2; exit 1; }

PERMS="$(stat -c '%a' "$STATS")"
[ "$PERMS" = "444" ] || { echo "TC-stats-01: FAIL — .stats perms=$PERMS, expected 444" >&2; exit 1; }

CONTENT="$(cat "$STATS")"

# Section headers must all be present.
for section in "-- Overview --" "-- Global Rates --" "-- Connections --" "-- Torrents --" "-- Cache --"; do
    if ! grep -qFe "$section" <<<"$CONTENT"; then
        echo "TC-stats-01: FAIL — root .stats missing section '$section'" >&2
        exit 1
    fi
done

# No single-torrent title line (`===== torrent: ... =====`).
if grep -qE '^===== torrent:' <<<"$CONTENT"; then
    echo "TC-stats-01: FAIL — root .stats must not carry a torrent title line" >&2
    exit 1
fi

echo "TC-stats-01: PASS (0444; Overview/Global Rates/Connections/Torrents/Cache; no torrent title)"

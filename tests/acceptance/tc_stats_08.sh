#!/usr/bin/env bash
# Design acceptance case TC-stats-08: the Trackers section lists entries by tier
# in order, states the DHT/LSD fallback when there is no tracker list, and
# appends the session-wide DHT node count when session stats exist
# (`docs/03-acceptance.md` §E AC-47).
#
# Container-executable form: released binary; two torrents, one with a two-tier
# announce list and one with no trackers at all.
#
# Usage: ./tests/acceptance/tc_stats_08.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_stats08}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-stats-08
write_fast_config

tracker_start
TIER1_A="http://127.0.0.1:9/first-tier-one"
TIER1_B="http://127.0.0.1:9/second-tier-one"
build_torrent "$TORRENT_FILES/tiered.torrent" --name tiered.bin \
    --content "$(payload_patterned tiered.bin 65536)" --piece-length 16384 \
    --tracker "$TRACKER_URL" --tier "$TIER1_A,$TIER1_B"
TIERED="$TORRENT"
build_torrent "$TORRENT_FILES/bare.torrent" --name bare.bin \
    --content "$(payload_patterned bare.bin 65536)" --piece-length 16384
BARE="$TORRENT"

daemon_start
ingest "$TIERED" >/dev/null
ingest "$BARE" >/dev/null
TIERED_STATS="$MNT/data/tiered.torrent/.stats"
BARE_STATS="$MNT/data/bare.torrent/.stats"

for _ in $(seq 1 40); do
    grep -q -- '-- Trackers --' "$TIERED_STATS" && break
    sleep 0.25
done
grep -q -- '-- Trackers --' "$TIERED_STATS" || fail "tiered torrent has no Trackers section"

# Tier 0 before tier 1, and both tier-1 entries in their declared order.
LINE0="$(grep -nF "  tier 0  $TRACKER_URL" "$TIERED_STATS" | cut -d: -f1)"
LINE1A="$(grep -nF "  tier 1  $TIER1_A" "$TIERED_STATS" | cut -d: -f1)"
LINE1B="$(grep -nF "  tier 1  $TIER1_B" "$TIERED_STATS" | cut -d: -f1)"
[ -n "$LINE0" ] || fail "tier 0 entry missing from the Trackers section"
[ -n "$LINE1A" ] || fail "first tier 1 entry missing from the Trackers section"
[ -n "$LINE1B" ] || fail "second tier 1 entry missing from the Trackers section"
[ "$LINE0" -lt "$LINE1A" ] || fail "tier 1 was listed before tier 0"
[ "$LINE1A" -lt "$LINE1B" ] || fail "tier 1 entries were not listed in their declared order"

grep -qE '^  DHT Nodes: [0-9]+ \(global\)$' "$TIERED_STATS" \
    || fail "the session-wide DHT node line is missing"

# No tracker list: the section states the DHT/LSD fallback instead.
for _ in $(seq 1 40); do
    grep -q -- '-- Trackers --' "$BARE_STATS" && break
    sleep 0.25
done
grep -qF '  No trackers — relying on DHT/LSD' "$BARE_STATS" \
    || { sed -n '/-- Trackers --/,/^$/p' "$BARE_STATS" >&2; fail "the no-tracker fallback line is missing"; }

echo "  tiered: $(sed -n '/-- Trackers --/,/^$/p' "$TIERED_STATS" | tr '\n' '|')"
echo "  bare:   $(sed -n '/-- Trackers --/,/^$/p' "$BARE_STATS" | tr '\n' '|')"
echo "TC-stats-08: PASS (tier order 0 < 1, tier-1 order kept, DHT node line present, no-tracker fallback stated)"

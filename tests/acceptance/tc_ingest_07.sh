#!/usr/bin/env bash
# Design acceptance case TC-ingest-07: a batch of 20 torrents projects one
# directory each, deleting a `metadata/` subtree cleans the matching projection,
# and the structure survives a remount (`docs/03-acceptance.md` §A AC-07).
#
# Container-executable form: released binary, no swarm needed.
#
# Usage: ./tests/acceptance/tc_ingest_07.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest07}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-ingest-07
write_fast_config

tracker_start
mkdir -p "$TORRENT_FILES/batch"
for i in $(seq -w 0 19); do
    build_torrent "$TORRENT_FILES/batch/t$i.torrent" --name "t$i.bin" \
        --content "$(payload_patterned "t$i.bin" 16384)" --piece-length 16384 \
        --tracker "$TRACKER_URL"
done

daemon_start
mkdir -p "$MNT/metadata/batch/g1" "$MNT/metadata/batch/g2" "$MNT/metadata/batch/g3"
for i in $(seq -w 0 19); do
    n=$((10#$i))
    if [ "$n" -le 6 ]; then group=g1; elif [ "$n" -le 13 ]; then group=g2; else group=g3; fi
    cp "$TORRENT_FILES/batch/t$i.torrent" "$MNT/metadata/batch/$group/t$i.torrent"
done

for i in $(seq -w 0 19); do
    n=$((10#$i))
    if [ "$n" -le 6 ]; then group=g1; elif [ "$n" -le 13 ]; then group=g2; else group=g3; fi
    wait_path "$MNT/data/batch/$group/t$i.torrent" 120 || fail "projection for t$i.torrent missing"
done

META_LIST="$(cd "$MNT/metadata" && find . -name '*.torrent' | sort)"
DATA_LIST="$(cd "$MNT/data" && find . -maxdepth 4 -name '*.torrent' | sort)"
[ "$META_LIST" = "$DATA_LIST" ] || {
    diff <(printf '%s\n' "$META_LIST") <(printf '%s\n' "$DATA_LIST") >&2
    fail "metadata seeds and projected directories do not correspond one-to-one"
}
[ "$(printf '%s\n' "$DATA_LIST" | wc -l)" = 20 ] || fail "expected 20 projections, found $(printf '%s\n' "$DATA_LIST" | wc -l)"

snapshot() {
    ( cd "$MNT/data" && find . -type f ! -name '.stats' -printf '%p %s %m\n' | sort )
}

# Delete one metadata subtree: only its projection disappears.
rm -rf "$MNT/metadata/batch/g2"
for _ in $(seq 1 80); do
    [ -e "$MNT/data/batch/g2" ] || break
    sleep 0.25
done
[ -e "$MNT/data/batch/g2" ] && fail "data/batch/g2 still present after deleting its metadata subtree"
[ -d "$MNT/data/batch/g1" ] || fail "data/batch/g1 disappeared with an unrelated subtree"
[ -d "$MNT/data/batch/g3" ] || fail "data/batch/g3 disappeared with an unrelated subtree"
REMAINING="$(cd "$MNT/data" && find . -maxdepth 4 -name '*.torrent' | wc -l)"
[ "$REMAINING" = 13 ] || fail "expected 13 projections after the subtree delete, found $REMAINING"

BEFORE="$(snapshot)"
daemon_stop
daemon_start
AFTER="$(snapshot)"
[ "$BEFORE" = "$AFTER" ] || {
    diff <(printf '%s\n' "$BEFORE") <(printf '%s\n' "$AFTER") >&2
    fail "projection structure or sizes changed across the remount"
}

echo "TC-ingest-07: PASS (20 seeds ↔ 20 projections, g2 subtree cleaned on delete, 13 projections with identical paths/sizes/permissions after remount)"

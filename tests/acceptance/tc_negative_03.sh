#!/usr/bin/env bash
# Design acceptance case TC-negative-03: F4 — persistent state does not live
# inside the FUSE mount by default, and pointing it there produces no
# persistence illusion across a remount (`docs/03-acceptance.md` §J AC-92).
#
# Container-executable form: released binary with a local seeder, so the
# configured cache path is exercised by a real read instead of only existing.
#
# Usage: ./tests/acceptance/tc_negative_03.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skip (no FUSE in this environment).
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-/usr/local/bin/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_negative03}"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_acceptance_env.sh"
require_fuse
env_init TC-negative-03

SRC_FILE="$(payload_patterned seed.bin 65536)"
tracker_start
build_torrent "$TORRENT_FILES/seed.torrent" --name seed.bin \
    --content "$SRC_FILE" --piece-length 16384 --tracker "$TRACKER_URL"
peer_start "$INFOHASH" "$PIECE_LENGTH" "$SRC_FILE"
wait_peer_known "$INFOHASH" 1 || fail "the seeder never registered with the tracker"

# ── Default state location is outside the mountpoint ─────────────────────
HOME_DIR="$W/home"
mkdir -p "$HOME_DIR"
mkdir -p "$MNT"
HOME="$HOME_DIR" "$BIN" "$MNT" > "$LOG_FILE" 2>&1 &
DEFAULT_PID=$!
track_pid "$DEFAULT_PID"
wait_mount "$MNT" || { tail -n 20 "$LOG_FILE" >&2; fail "daemon did not mount with the default state paths"; }
DEFAULT_STATE="$HOME_DIR/.local/share/torrentfs"
[ -d "$DEFAULT_STATE" ] || fail "default state directory $DEFAULT_STATE was not created"
case "$DEFAULT_STATE" in
    "$MNT"/*) fail "default state directory lies inside the mountpoint" ;;
esac
echo "  default state: ${DEFAULT_STATE#"$W"/} (outside the mountpoint)"
kill "$DEFAULT_PID" >/dev/null 2>&1 || true
wait_unmount "$MNT" 120 || fail "default-path daemon did not unmount"
wait "$DEFAULT_PID" >/dev/null 2>&1 || true

# ── State pointed inside the mount: usable-looking, not persistent ───────
write_fast_config
IN_MNT_DB="$MNT/metadata/state/metadata.db"
IN_MNT_CACHE="$MNT/metadata/state/cache"
# The carrier pre-creates nothing here: whatever ends up on the underlying
# filesystem must be what the daemon itself wrote, otherwise the observation is
# an artifact of the layout rather than the daemon's behaviour.
"$BIN" "$MNT" --db "$IN_MNT_DB" --cache "$IN_MNT_CACHE" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
INSIDE_PID=$!
track_pid "$INSIDE_PID"
wait_mount "$MNT" || fail "daemon did not mount with state paths inside the mountpoint"
ingest "$TORRENT" >/dev/null
TOTAL="$(sed -n 's/^  Total: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"
[ "$TOTAL" = 1 ] || fail "the in-mount state did not produce a usable-looking projection (Total=$TOTAL)"

# Exercise the cache path: a real read is the only way pieces land where the
# configured cache points, and AC-92's cache half is about where those pieces
# end up.  The criterion is "no persistence illusion", not a particular
# outcome: the read may be served (pieces then have to be genuinely on the
# underlying filesystem) or may fail with ENODATA (nothing to be served from),
# and the remount below is judged against whichever happened.
wait_peer_known "$INFOHASH" 2 || fail "client never announced to the tracker"
FIRST_RC=0
read_retry "$MNT/data/seed.torrent/seed.bin" "$W/inside.out" 3 || FIRST_RC=$?
if [ "$FIRST_RC" = 0 ]; then
    cmp -s "$W/inside.out" "$SRC_FILE" || fail "the in-mount-state read returned the wrong bytes"
else
    grep -q "No data available" "$W/read.err" \
        || { cat "$W/read.err" >&2; fail "the in-mount-state read failed without ENODATA"; }
fi
SERVED_FIRST="$(peer_served)"

kill "$INSIDE_PID" >/dev/null 2>&1 || true
wait_unmount "$MNT" 120 || fail "in-mount-state daemon did not unmount"
wait "$INSIDE_PID" >/dev/null 2>&1 || true

# What is actually on the underlying filesystem after the unmount.  `$MNT`
# resolves to the host directory here, so these are real files, not the
# daemon's projection.
UNDER_DB="$MNT/metadata/state/metadata.db"
UNDER_CACHE="$MNT/metadata/state/cache"
REAL_DB=0
[ -f "$UNDER_DB" ] && REAL_DB=1
CACHE_PRESENT=0
[ -d "$UNDER_CACHE/pieces/$INFOHASH" ] && CACHE_PRESENT=1
echo "  underlying state after unmount: db_present=$REAL_DB cache_pieces=$CACHE_PRESENT"

"$BIN" "$MNT" --db "$IN_MNT_DB" --cache "$IN_MNT_CACHE" --config "$CONFIG_FILE" > "$LOG_FILE" 2>&1 &
AGAIN_PID=$!
track_pid "$AGAIN_PID"
wait_mount "$MNT" || fail "remount with inside-mount state failed"
TOTAL="$(sed -n 's/^  Total: \([0-9]*\).*/\1/p' "$MNT/.stats" | head -n1)"

if [ "$REAL_DB" = 1 ]; then
    # The database really is on the underlying filesystem, so the projection
    # must be genuinely usable — no illusion either way.
    [ "$TOTAL" = 1 ] || fail "the underlying database exists but the remount reports Total=$TOTAL"
    [ -f "$MNT/data/seed.torrent/seed.bin" ] || fail "the underlying database exists but the projection is missing"
    [ "$(stat -c '%s' "$MNT/data/seed.torrent/seed.bin")" = 65536 ] || fail "the projection's size differs after the remount"
else
    [ "$TOTAL" = 0 ] || fail "no underlying database exists but the remount still reports Total=$TOTAL — a persistence illusion"
    [ -e "$MNT/data/seed.torrent" ] && fail "the projection survived without any underlying state"
fi

# Cache half: what the remount serves must match what actually survived.  A
# cache that really is on the underlying filesystem must serve the re-read
# without downloading; one that is gone may leave the read failing or make it
# download again — but it must never be served as if pieces were there.
SECOND_RC=0
read_retry "$MNT/data/seed.torrent/seed.bin" "$W/again.out" 3 || SECOND_RC=$?
if [ "$SECOND_RC" = 0 ]; then
    cmp -s "$W/again.out" "$SRC_FILE" || fail "the remounted read returned the wrong bytes"
fi
SERVED_SECOND="$(peer_served)"
DELTA=$(( ${SERVED_SECOND:-0} - ${SERVED_FIRST:-0} ))
if [ "$CACHE_PRESENT" = 1 ]; then
    [ "$SECOND_RC" = 0 ] || fail "cache pieces exist on the underlying filesystem but the remount could not serve the read"
    [ "$DELTA" -eq 0 ] \
        || fail "cache pieces survived on the underlying filesystem yet the remount re-downloaded $DELTA pieces — a persistence illusion"
elif [ "$SECOND_RC" = 0 ] && [ "$DELTA" -le 0 ]; then
    fail "no cache pieces survived on the underlying filesystem yet the remount served the read without downloading — a persistence illusion"
fi

echo "TC-negative-03: PASS (default state outside the mount; in-mount state recorded as db_present=$REAL_DB cache_pieces=$CACHE_PRESENT read_ok=$([ "$FIRST_RC" = 0 ] && echo yes || echo no) again_ok=$([ "$SECOND_RC" = 0 ] && echo yes || echo no), remount consistent, no illusion: $DELTA pieces re-downloaded)"

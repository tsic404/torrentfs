#!/usr/bin/env bash
# Design acceptance case TC-ingest-06: a non-bencode `.torrent` write is
# rejected at close with `EINVAL` and leaves no `data/` projection; a same-name
# retry started at once is judged by its content alone — still invalid stays
# rejected, valid content is accepted and its projection appears
# (`docs/03-acceptance.md` §A AC-06; contracts C-2.4.2 / C-2.4.4).
#
# Mount form.  The service-layer module (`test_tc_ingest_06.rs`) can only see
# the daemon's verdict; the kernel-side residue (the rejected file's dentry and
# the `data/` name looked up while it was absent) is visible only on a mount.
# Here the `data/` name is probed while the write is rejected — caching a
# negative dentry — and the valid retry must project with no sleep, so removing
# the rejection path's `inval_data_entry` leaves the stale negative entry in
# place and fails the case.
#
# Usage: ./tests/acceptance/tc_ingest_06.sh [torrentfs_binary] [mountpoint]
# Exit: 0 = pass, 1 = fail, 2 = skipped (no FUSE 3 mount capability).
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="${1:-$ROOT_DIR/target/release/torrentfs}"
MNT="${2:-/tmp/torrentfs_acceptance_ingest06}"

# Self-gate: libfuse2's fusermount rejects the daemon's non-empty mountpoint
# (it carries `.torrentfs.lock`); only a fuse3 environment can run this.
command -v fusermount3 >/dev/null 2>&1 || {
    echo "tc_ingest_06: SKIP — fusermount3 (libfuse3) is required for a mount" >&2
    exit 2
}
[ -e /dev/fuse ] || { echo "tc_ingest_06: SKIP — /dev/fuse is not available" >&2; exit 2; }
[ -x "$BIN" ] || { echo "tc_ingest_06: SKIP — binary not found: $BIN" >&2; exit 2; }

# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_fuse_harness.sh"
# shellcheck source=/dev/null
source "$ROOT_DIR/tests/acceptance/lib_seed_torrent.sh"

BAD="$(mktemp)"
GOOD="$(mktemp)"
trap 'rm -f "$BAD" "$GOOD"; cleanup' EXIT

fail() {
    echo "tc_ingest_06: FAIL — $1" >&2
    echo "  --- daemon log (tail) ---" >&2
    tail -n 20 "$LOG_FILE" >&2 2>/dev/null || true
    exit 1
}

printf 'not a bencoded torrent\n' > "$BAD"
make_seed_torrent "$GOOD" candidate.txt

# cp checks close(2); its error text carries the EINVAL witness.
if err="$(cp "$BAD" "$MNT/metadata/candidate.torrent" 2>&1)"; then
    fail "a non-bencode write must be rejected, cp exited 0"
fi
case "$err" in
    *"Invalid argument"*) ;;
    *) fail "expected EINVAL (Invalid argument) from close, got: $err" ;;
esac
if [ -e "$MNT/data/candidate.torrent" ]; then
    fail "a rejected write must not project a directory"
fi

# Same-name retry, still invalid: decided by content, same verdict.
if err="$(cp "$BAD" "$MNT/metadata/candidate.torrent" 2>&1)"; then
    fail "an immediate same-name retry must be rejected by content"
fi
case "$err" in
    *"Invalid argument"*) ;;
    *) fail "the retry must report the same EINVAL, got: $err" ;;
esac
if [ -e "$MNT/data/candidate.torrent" ]; then
    fail "the rejected retry must not project a directory"
fi

# Same-name retry, now valid: accepted, and the projection appears inside the
# design's 200ms window.  A stale negative dentry from the probes above would
# hide it for the full 1s TTL.
cp "$GOOD" "$MNT/metadata/candidate.torrent" || fail "valid content must be accepted"
# 20 x 10ms = the design's 200ms window.
for _ in $(seq 1 20); do
    [ -d "$MNT/data/candidate.torrent" ] && break
    sleep 0.01
done
[ -d "$MNT/data/candidate.torrent" ] ||
    fail "the accepted retry's projection did not appear within 200ms"

# A rejected overwrite of an existing seed must also clear its projection: the
# rejection removes the row, so the `data/` name (whose dentry was just looked
# up) must stop resolving without waiting for the TTL.  The kernel sends
# FUSE_RELEASE asynchronously (`fuse_simple_background`), so the daemon's
# row-removal and invalidation may land just after `cp` returns; wait for it
# within the design's 200ms freshness bound.  A missing invalidation leaves the
# stale name resolving for the full 1s entry TTL and still fails here.
make_seed_torrent "$GOOD" victim.txt
cp "$GOOD" "$MNT/metadata/victim.torrent" || fail "seed setup write failed"
for _ in $(seq 1 60); do
    [ -d "$MNT/data/victim.torrent" ] && break
    sleep 0.1
done
[ -d "$MNT/data/victim.torrent" ] || fail "seed setup did not project"
stat "$MNT/data/victim.torrent" >/dev/null   # hold the positive dentry
if err="$(cp "$BAD" "$MNT/metadata/victim.torrent" 2>&1)"; then
    fail "a non-bencode overwrite must be rejected"
fi
case "$err" in
    *"Invalid argument"*) ;;
    *) fail "the overwrite must report EINVAL, got: $err" ;;
esac
# 20 x 10ms = the design's 200ms window, five times below the 1s TTL.
for _ in $(seq 1 20); do
    [ -e "$MNT/data/victim.torrent" ] || break
    sleep 0.01
done
if [ -e "$MNT/data/victim.torrent" ]; then
    fail "the rejected overwrite left the old projection resolvable (stale dentry)"
fi

echo "tc_ingest_06: PASS (EINVAL on close, no projection, content-judged retry)"

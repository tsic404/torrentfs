#!/usr/bin/env bash
# Deterministic torrent fixture builder for the acceptance scripts.
#
# Builds a minimal single-file, single-piece `.torrent` without any
# interpreter beyond coreutils: a fixed 33-byte payload ("Hello torrent
# content for testing"), piece length 16384, the given internal name.  The
# announce URL points at a loopback tracker that need not exist — these
# fixtures exercise the ingest/projection/stat surface, not downloads.
#
# Usage (source, then call):
#   make_seed_torrent <output-path> [internal-name]
#
# The internal name selects the info hash (it is inside the info dict), so
# two calls with different names produce two distinct torrents.

make_seed_torrent() {
    local out="$1" name="${2:-seed.txt}"
    local content="Hello torrent content for testing"
    local hex raw
    hex="$(printf '%s' "$content" | sha1sum | cut -d' ' -f1)"
    raw=""
    local i
    for ((i = 0; i < ${#hex}; i += 2)); do
        raw+="\\x${hex:i:2}"
    done
    {
        printf 'd8:announce30:http://127.0.0.1:6969/announce4:infod'
        printf '6:lengthi33e'
        printf '4:name%d:%s' "${#name}" "$name"
        printf '12:piece lengthi16384e'
        printf '6:pieces20:'
        printf '%b' "$raw"
        printf 'ee'
    } > "$out"
}

# The size in bytes the fixture's metadata declares for its single file.
SEED_TORRENT_FILE_SIZE=33

# The exact payload the fixture's single file holds (the torrent metadata
# declares this content's SHA-1; the projection serves these bytes).
SEED_TORRENT_CONTENT="Hello torrent content for testing"

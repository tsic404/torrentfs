#!/usr/bin/env bash
# Enable user_allow_other in /etc/fuse.conf so non-root users can mount
# torrentfs with the allow_other option.
#
# Idempotent: uncomments an existing `#user_allow_other` line, or appends
# `user_allow_other` when the line is absent. Safe to run repeatedly.
set -euo pipefail

CONF=/etc/fuse.conf

if [ "$(id -u)" -ne 0 ]; then
    echo "error: run as root (sudo)" >&2
    exit 1
fi

if [ ! -f "$CONF" ]; then
    touch "$CONF"
fi

# Match the Dockerfile's unlock logic so host and image stay consistent.
sed -i 's/^#\s*user_allow_other\s*$/user_allow_other/' "$CONF"
grep -q '^user_allow_other$' "$CONF" || echo 'user_allow_other' >> "$CONF"

echo "user_allow_other enabled in $CONF:"
grep -n '^user_allow_other$' "$CONF"

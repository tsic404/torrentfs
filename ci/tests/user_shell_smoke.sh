#!/usr/bin/env bash
# Host-side smoke test for the torrentfs daemon user's login shell (TSI-3043).
#
# Runs on the CI runner AFTER the image is built, alongside entrypoint_smoke.sh.
# It starts a container with a plain sleep (no /dev/fuse, no mount needed), then
# asserts the observable behaviors the shell fix delivers:
#
#   1. /etc/passwd gives the torrentfs daemon user a login shell (/bin/sh),
#      not /usr/sbin/nologin.
#   2. `docker exec --user torrentfs <c> /bin/sh -c 'id -u; id -g'` succeeds
#      and runs as UID/GID 1000 — the operator debugging workflow from the
#      issue.
#   3. `su - torrentfs` (login shell) succeeds inside the container. This is
#      what /usr/sbin/nologin actually breaks: nologin makes su/login reject
#      the account ("This account is currently not available.").
#
# Usage:
#   ./ci/tests/user_shell_smoke.sh [IMAGE]
#
# Exit code: 0 = pass, non-zero = fail.

set -euo pipefail

IMAGE="${1:-torrentfs:ci}"
NAME="torrentfs-shell-smoke-$$"

cleanup() {
    docker rm -f "$NAME" >/dev/null 2>&1 || true
}
trap cleanup EXIT

# A long sleep keeps the container alive without /dev/fuse or a mountpoint.
docker run -d --name "$NAME" --entrypoint /bin/sh "$IMAGE" -c 'sleep 300' >/dev/null

# 1. The daemon user must carry a login shell, not nologin.
shell="$(docker exec --user root "$NAME" getent passwd torrentfs | cut -d: -f7)"
[ "$shell" = "/bin/sh" ] || {
    echo "[smoke] FAIL: torrentfs shell is '$shell' (want /bin/sh)" >&2
    exit 1
}

# 2. `docker exec --user torrentfs` must start a shell as UID/GID 1000.
out="$(docker exec --user torrentfs "$NAME" /bin/sh -c 'id -u; id -g')"
[ "$out" = "1000
1000" ] || {
    echo "[smoke] FAIL: exec as torrentfs reported '$out' (want 1000/1000)" >&2
    exit 1
}

# 3. `su - torrentfs` (login shell) must succeed — the behavior nologin broke.
out="$(docker exec --user root "$NAME" su - torrentfs -c 'id -u; id -g')"
[ "$out" = "1000
1000" ] || {
    echo "[smoke] FAIL: su - torrentfs reported '$out' (want 1000/1000)" >&2
    exit 1
}

echo "[smoke] torrentfs login shell OK ($shell), exec + su identity 1000/1000"

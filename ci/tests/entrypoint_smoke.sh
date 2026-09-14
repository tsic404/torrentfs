#!/usr/bin/env bash
# End-to-end smoke test for the entrypoint privilege-drop path. Runs INSIDE
# the built image as real root in a rootful container, asserting two behaviors
# the host-side unit tests cannot reach: (1) resolve_daemon_ids targets the
# non-root `torrentfs` user, (2) run_daemon really execs setpriv and drops to
# UID/GID 1000 (not the stubbed argv shape). Catches a missing/broken setpriv
# or a silent root-drop. Deliberately does NOT mount FUSE (unreliable on CI).
# Usage: docker run --rm -v "$PWD/ci/tests/entrypoint_smoke.sh:/smoke.sh:ro" \
#        --entrypoint /bin/bash torrentfs:ci /smoke.sh    Exit: 0 = pass.

set -euo pipefail

ENTRYPOINT=/usr/local/bin/entrypoint.sh

# The entrypoint has `set -euo pipefail` and executes main at the bottom, so we
# cannot simply `source` it — extract the helpers (everything before "# ── main").
HELPERS_FILE="$(mktemp)"
awk '/^# ── main/{exit} {print}' "$ENTRYPOINT" > "$HELPERS_FILE"
trap 'rm -f "$HELPERS_FILE"' EXIT

# shellcheck source=/dev/null
source "$HELPERS_FILE"

# This smoke test is only meaningful as real root in a non-userns container;
# refuse loudly otherwise instead of asserting the wrong thing.
is_root || { echo "[smoke] FAIL: must run as root (got uid $(id -u))" >&2; exit 1; }
is_rootless_podman && { echo "[smoke] FAIL: must run rootful, not in a user namespace" >&2; exit 1; }

echo "[smoke] starting as $(id -u):$(id -g)"

# 1. In a rootful container the daemon identity resolves to torrentfs.
resolve_daemon_ids
[ "$daemon_uid" = 1000 ]  || { echo "[smoke] FAIL: daemon_uid=$daemon_uid (want 1000)" >&2; exit 1; }
[ "$daemon_gid" = 1000 ]  || { echo "[smoke] FAIL: daemon_gid=$daemon_gid (want 1000)" >&2; exit 1; }
[ "$daemon_home" = /home/torrentfs ] || { echo "[smoke] FAIL: daemon_home=$daemon_home (want /home/torrentfs)" >&2; exit 1; }

# 2. run_daemon must drop to the torrentfs user. `exec` replaces this shell, so
#    the assertion below runs AS the dropped user and its exit code becomes this
#    script's final status.
run_daemon sh -c 'test "$(id -u)" = 1000 && test "$(id -g)" = 1000 && echo "[smoke] dropped to $(id -u):$(id -g)"'

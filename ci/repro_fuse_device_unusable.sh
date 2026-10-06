#!/usr/bin/env bash
# Regression gate for the container "device cannot be ensured" exit path.
#
# A container started without `--device /dev/fuse` still lets the entrypoint's
# `mknod` create the node, but the runtime's device cgroup / seccomp profile
# denies `open("/dev/fuse")` with EPERM.  The daemon must then log the
# contract's 100 branch line and exit 100 (`EXIT_FUSE_DEVICE_UNUSABLE`) — before
# the fix it died of SIGSEGV instead (139): `exit()` runs OpenSSL's
# `OPENSSL_cleanup`, which freed the library globals while libtorrent's session
# threads were still inside OpenSSL, and only the entrypoint's fallback turned
# that into the expected 100.
#
# That defect is a non-deterministic race, so the gate's discriminating power
# comes from what the harness itself supplies:
#   * `--network host` — the container layout this path was found in;
#   * CPU saturation for the whole run (`nproc` busy loops).
# Measured against the published pre-fix image (torrentfs:main@sha256:e3464287),
# default parameters catch 7 of 32 cycles; 32 cycles without load catch none
# (0/40 measured), which would make this file a gate in name only.  It refuses
# to run (exit 4) when it cannot start that load.
#
# Usage: ./ci/repro_fuse_device_unusable.sh [image] [cycles]
# Requires: docker (the image is built by ci/docker.yml from this tree).
# Exit codes: 0 = all cycles pass, 1 = a cycle failed, 2 = bad usage,
# 3 = environment cannot exercise the path, 4 = no load capability (refuse).
set -euo pipefail

IMAGE="${1:-torrentfs:test}"
CYCLES="${2:-32}"

case "$CYCLES" in
    ''|*[!0-9]*|0) echo "cycles must be a positive integer (got '$CYCLES')" >&2; exit 2 ;;
esac

command -v docker >/dev/null 2>&1 || { echo "docker not available" >&2; exit 3; }
docker image inspect "$IMAGE" >/dev/null 2>&1 || {
    echo "image '$IMAGE' not found — build it first (docker build -t $IMAGE .)" >&2
    exit 3
}

# The daemon-side line is the one and only evidence that the device node
# existed and the runtime blocked open() on it (the entrypoint's own
# "[entrypoint] /dev/fuse missing" branch never reaches the daemon).
LOG_100="/dev/fuse cannot be opened"
# Upper bound on one container's whole run; a hang is a gate failure, not a
# "still running" state.
CYCLE_TIMEOUT_SECS=120

LOAD_PIDS=()
stop_load() {
    local pid
    for pid in "${LOAD_PIDS[@]:-}"; do
        kill "$pid" 2>/dev/null || true
    done
    wait 2>/dev/null || true
}
trap stop_load EXIT

# Saturate every CPU for the whole run: the race needs the session thread to
# still be inside OpenSSL when the daemon runs its exit-time destructors, which
# only happens while the machine is oversubscribed.
start_load() {
    local cores
    cores=$(nproc 2>/dev/null || echo 0)
    if [ "$cores" -lt 1 ]; then
        echo "cannot determine CPU count (nproc) — this gate needs to impose CPU load" >&2
        exit 4
    fi
    local i pid
    for ((i = 0; i < cores; i++)); do
        ( while :; do :; done ) &
        LOAD_PIDS+=("$!")
    done
    sleep 1
    local alive=0
    for pid in "${LOAD_PIDS[@]}"; do
        if kill -0 "$pid" 2>/dev/null; then alive=$((alive + 1)); fi
    done
    if [ "$alive" -eq 0 ]; then
        echo "cannot keep CPU load running (spawned $cores busy loops, none alive)" >&2
        echo "refusing to gate on this harness without load — the defect is load-dependent" >&2
        exit 4
    fi
    echo "CPU load: $alive busy loop(s)"
}

FAILURES=0
start_load

for cycle in $(seq 1 "$CYCLES"); do
    # `--cap-add SYS_ADMIN` + default seccomp + no `--device /dev/fuse`: the
    # exact environment the contract's 100 branch describes.
    out=""
    code=0
    out=$(timeout "$CYCLE_TIMEOUT_SECS" docker run --rm --network host --cap-add SYS_ADMIN \
        "$IMAGE" 2>&1) || code=$?

    reason=""
    if grep -q "\[entrypoint\] /dev/fuse missing" <<<"$out"; then
        # mknod could not create the node at all: this host cannot exercise
        # the daemon-side probe, which is what this harness asserts.
        echo "cycle $cycle: SKIP — entrypoint could not create /dev/fuse on this host"
        echo "$out" | tail -5
        exit 3
    fi
    if [ "$code" -eq 124 ]; then
        reason="container did not exit within ${CYCLE_TIMEOUT_SECS}s"
    fi
    if [ -z "$reason" ] && [ "$code" -ne 100 ]; then
        reason="container exited $code, expected 100"
    fi
    if [ -z "$reason" ] && ! grep -q "$LOG_100" <<<"$out"; then
        reason="missing contract log line '$LOG_100'"
    fi
    if [ -z "$reason" ] && grep -qE "Segmentation fault|core dumped|exited with code 139" <<<"$out"; then
        reason="daemon died of SIGSEGV (entrypoint fallback masked it as 100)"
    fi

    if [ -n "$reason" ]; then
        FAILURES=$((FAILURES + 1))
        echo "cycle $cycle: FAIL — $reason"
        echo "$out" | tail -10
    else
        echo "cycle $cycle: PASS — exit 100, contract log line present, no core dump"
    fi
done

if [ "$FAILURES" -ne 0 ]; then
    echo "FAIL: $FAILURES/$CYCLES cycles failed" >&2
    exit 1
fi
echo "PASS: $CYCLES/$CYCLES cycles exited 100 without a core dump"

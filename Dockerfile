# Stage 1: Build
FROM debian:sid-slim AS builder

# Install Rust toolchain + the build chain: libtorrent and the Boost headers are
# fetched and compiled by the crate's build script, so only a C++17 compiler,
# openssl and libfuse development files are needed here.
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential libssl-dev libfuse-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

WORKDIR /build

COPY . .

RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release && \
    cp target/release/torrentfs /torrentfs

# Stage 2: Runtime
FROM debian:sid-slim

# libtorrent, its C ABI wrapper and the C++ runtime are statically linked into
# /usr/local/bin/torrentfs, so no libtorrent / Boost package is needed here: the
# runtime carries openssl (still a runtime dependency), FUSE and the base
# image's own dependencies.
#
# The daemon user gets /bin/sh (not /usr/sbin/nologin) as its login shell:
# a container has no login(1)/PAM path for nologin to guard, and operators
# need `docker exec --user torrentfs sh` to debug the mount.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libssl3t64 \
    libstdc++6 \
    libfuse2 \
    fuse3 \
    ca-certificates \
    passwd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 1000 torrentfs \
    && useradd --uid 1000 --gid 1000 --home-dir /home/torrentfs \
        --create-home --shell /bin/sh torrentfs

# Enable allow_other so non-root users can access the shared mount.
# fuse3 ships /etc/fuse.conf with `#user_allow_other` commented out; uncomment
# it (falling back to appending) so torrentfs detects user_allow_other at startup.
RUN touch /etc/fuse.conf \
    && sed -i 's/^#\s*user_allow_other\s*$/user_allow_other/' /etc/fuse.conf \
    && (grep -q '^user_allow_other$' /etc/fuse.conf || echo 'user_allow_other' >> /etc/fuse.conf)

COPY --from=builder /torrentfs /usr/local/bin/torrentfs
COPY entrypoint.sh /usr/local/bin/entrypoint.sh

# stop_timeout must be raised at the runtime layer (see README Container
# Deployment): the image cannot override podman/docker's default 10s
# SIGTERM→SIGKILL grace period, and a forced kill leaves a stale ENOTCONN
# mount that blocks the next `podman start`.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/mnt"]

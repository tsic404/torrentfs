# Stage 1: Build
FROM debian:sid-slim AS builder

# Install Rust toolchain + build deps for libtorrent (built from source below).
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates build-essential pkg-config \
    cmake ninja-build \
    libboost-dev libssl-dev \
    clang libclang-dev libfuse-dev \
    && rm -rf /var/lib/apt/lists/* \
    && curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y

ENV PATH="/root/.cargo/bin:${PATH}"

# Build libtorrent-rasterbar from source, statically, with -fno-gnu-unique:
# Debian's prebuilt libtorrent emits the error-category statics as GNU-unique
# symbols; linking torrentfs against it coalesces them wrongly, so
# `system_category()` dereferences a null vtable → SIGSEGV on first disk I/O.
# Building from source with -fno-gnu-unique (WEAK) plus static linking removes
# the cross-DSO conflict.
ARG LIBTORRENT_VERSION=2.1.1
# SHA256 of the release tarball, pinned for supply-chain integrity. Bump together
# with LIBTORRENT_VERSION (digest from the GitHub release asset metadata).
ARG LIBTORRENT_SHA256=0f163516ecef2e3331500266751de3098835a3c3ae0c2290448046c632bc0e93
RUN curl --proto '=https' --tlsv1.2 --retry 5 --retry-delay 2 --retry-connrefused -sSfL -o /tmp/libtorrent.tar.gz \
      "https://github.com/arvidn/libtorrent/releases/download/v${LIBTORRENT_VERSION}/libtorrent-rasterbar-${LIBTORRENT_VERSION}.tar.gz" \
    && echo "${LIBTORRENT_SHA256}  /tmp/libtorrent.tar.gz" | sha256sum -c - \
    && mkdir -p /tmp/libtorrent-src \
    && tar xzf /tmp/libtorrent.tar.gz -C /tmp/libtorrent-src --strip-components=1 \
    && cmake -S /tmp/libtorrent-src -B /tmp/libtorrent-build -G Ninja \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_SHARED_LIBS=OFF \
        -DCMAKE_CXX_FLAGS="-fno-gnu-unique" \
        -DCMAKE_INSTALL_PREFIX=/usr/local \
        -Dwebtorrent=OFF \
        -Dbuild_tests=OFF \
        -Dbuild_examples=OFF \
        -Dbuild_tools=OFF \
        -Dpython-bindings=OFF \
    && cmake --build /tmp/libtorrent-build -j"$(nproc)" \
    && cmake --install /tmp/libtorrent-build \
    && rm -rf /tmp/libtorrent-src /tmp/libtorrent-build /tmp/libtorrent.tar.gz

WORKDIR /build

COPY . .

RUN --mount=type=cache,target=/root/.cargo/registry \
    cargo build --release && \
    cp target/release/torrentfs /torrentfs

# Stage 2: Runtime
FROM debian:sid-slim

# libtorrent is statically linked into /usr/local/bin/torrentfs; only its
# runtime deps (OpenSSL, C++ runtime) and FUSE are needed here.
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
#
# The QA-verified rootful run used `--network host` (see README "Docker
# (rootful, host-visible mount)"): in that environment the default bridge
# network failed while creating the container's veth pair (`failed to add the
# host (veth…) pair interfaces: operation not supported`), before any process
# started.
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
CMD ["/mnt"]

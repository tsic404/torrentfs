# torrentfs

A FUSE-based virtual filesystem for BitTorrent management. Mount `.torrent` files, browse their structure, and read file contents on-demand via the BitTorrent network.

Copy a `.torrent` file into the filesystem and torrentfs generates the corresponding data directory automatically; you browse the seed's structure as a normal directory tree and read any file — pieces are fetched from the swarm only when you read them, then cached and re-seeded.

## Features

- **Drop-in `.torrent` ingestion** — copy a `.torrent` file into the `metadata/` directory; torrentfs parses it and exposes its file tree under `data/` automatically.
- **On-demand reads** — file contents are downloaded from the BitTorrent network only when read, with piece priority boosted for the active read.
- **Automatic caching** — downloaded pieces are cached in memory and on disk (LRU), so repeated reads skip the network.
- **Automatic seeding** — cached/downloaded pieces are re-seeded back to the swarm.
- **Persistent metadata** — torrent metadata and directory structure are stored in SQLite and survive restarts.
- **Virtual statistics** — a `.stats` file at the root, per directory, and per torrent reports piece lifecycle, cache hit rates, and session status.
- **TOML configuration** — proxy, DHT, rate limits, tracker, encryption, and ~15 other sections; every key is optional and falls back to libtorrent defaults.
- **Docker image** — `ghcr.io/tsic404/torrentfs` with an entrypoint that handles FUSE device setup and mount visibility (rootful/rootless).

## Installation

### Build from source

Requirements:

- Rust toolchain (stable)
- `libtorrent-rasterbar` 2.1.x with pkg-config metadata
- `openssl` development files
- `libfuse` development files
- `clang` / `libclang` (for FFI bindings)
- A C++17 compiler (`gcc` or `clang`)

Build the binary:

```bash
cargo build --release
# binary at target/release/torrentfs
```

Or install to your cargo bin path:

```bash
cargo install --path .
```

The FFI crate (`libtorrent-sys`) probes `libtorrent-rasterbar` and `openssl` via pkg-config and compiles the C++ wrapper against the ABI definitions the installed libtorrent was built with. See `Dockerfile` for the exact dependency set used in the shipped image.

### Docker image

```bash
docker pull ghcr.io/tsic404/torrentfs:main
```

The image builds libtorrent from source (statically, `-fno-gnu-unique`) and includes the entrypoint that configures FUSE and mount visibility. See [Container Deployment](#container-deployment).

## Quick Start

### Local

The steps below run the binary built in [Installation](#installation) as `./target/release/torrentfs`. If you ran `cargo install --path .`, the bare `torrentfs` name is on your `PATH` and can be used instead.

1. Ensure `/dev/fuse` exists and the FUSE kernel module is loaded:

   ```bash
   modprobe fuse
   ls -l /dev/fuse
   ```

2. Enable `allow_other` for non-root users (torrentfs mounts with `allow_other`; without this line a non-root mount fails with `Operation not permitted`):

   ```bash
   sudo ./ci/enable_fuse_allow_other.sh
   ```

3. Mount:

   ```bash
   mkdir -p /mnt/torrentfs
   ./target/release/torrentfs /mnt/torrentfs
   ```

4. Copy a `.torrent` in and read its contents:

   ```bash
   cp ubuntu-24.04.iso.torrent /mnt/torrentfs/metadata/
   ls /mnt/torrentfs/data/
   cat /mnt/torrentfs/data/<name>/README
   ```

### Docker (rootful, host-visible mount)

Prepare a shared host mount, then run with `rshared` bind propagation:

```bash
mkdir -p /host/torrentfs
mount --bind /host/torrentfs /host/torrentfs
mount --make-shared /host/torrentfs

docker run --rm \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs:main
```

The filesystem is then visible on the host at `/host/torrentfs`. For podman and rootless variants, see [Container Deployment](#container-deployment).

### Rootless podman (container-only access)

Rootless podman cannot create shared mounts, so the `rshared` / `:shared` bind-mount recipe above is rejected by the entrypoint (exit `102`) instead of silently mounting container-only. Run **without** a bind mount on the mountpoint and access the filesystem inside the container via `podman exec` — see [Rootless podman](#rootless-podman) under [Container Deployment](#container-deployment) for the command, which includes `--stop-timeout 30` for clean shutdown. For host-visible mounts, use rootful Docker or `sudo podman`.

## Usage

### Adding a torrent

Copy a `.torrent` file into the `metadata/` directory (any subdirectory works; each `.torrent` generates a matching tree under `data/`):

```bash
cp some.iso.torrent /mnt/torrentfs/metadata/
```

torrentfs parses the torrent on `release` (file close) and creates the data directory in the background — the `data/` mirror is populated within a fraction of a second.

### Browsing and reading

```bash
ls /mnt/torrentfs/data/
ls /mnt/torrentfs/data/<torrent-name>/
cat /mnt/torrentfs/data/<torrent-name>/path/to/file
```

The `data/` tree is read-only (`EROFS` for writes); `metadata/` holds your `.torrent` files. Read progress and piece state are visible in the virtual `.stats` files.

### Statistics

- `/mnt/torrentfs/.stats` — global session overview.
- `/mnt/torrentfs/data/.stats` and per-torrent/per-directory `.stats` — piece lifecycle and cache metrics for that subtree.

See [`.stats` Pieces block](#stats-pieces-block) for the marker semantics.

### Configuration

Pass a TOML file with `--config`:

```bash
./target/release/torrentfs /mnt/torrentfs --config torrentfs-config.toml
```

Every key is optional; missing keys use libtorrent defaults. Example:

```toml
# torrentfs-config.toml
[connections]
listen_interfaces = "0.0.0.0:6881"

[proxy]
host = "127.0.0.1"
port = 1080
type = "socks5"
```

Validate a config without mounting:

```bash
./target/release/torrentfs --config torrentfs-config.toml --config-check
```

CLI flags: `torrentfs <mountpoint> [--db <path>] [--cache <dir>] [--config <file>] [--config-check]`.

#### `[proxy]` key naming

The `[proxy]` section accepts both `type` and `proxy_type` for the proxy kind (e.g. `socks5`). `type` matches libtorrent's `settings_pack` key and is the canonical name; `proxy_type` is accepted as an alias for users who find it more intuitive. Both set the same value.

#### SOCKS5 UDP ASSOCIATE probe (socks5 proxy)

When a SOCKS5 proxy is configured, libtorrent tries to open a UDP tunnel through the proxy by sending a SOCKS5 UDP ASSOCIATE request (`cmd=3`). The request carries `host='0.0.0.0' port=0` — libtorrent's default send-local endpoint when none is set, not a real connect target. A relay with no UDP endpoint to associate cannot answer, so the relay log shows an unclosed `cmd=3` request.

This is expected libtorrent behavior, not a configuration error:

- It is triggered by the SOCKS5 proxy configuration and an unanswered UDP ASSOCIATE — it does not depend on the listen port being `0`.
- It is not a fixed pair: one UDP socket is opened per listening socket, and libtorrent retries with exponential backoff when the association fails, so the `cmd=3` request reappears periodically in the log.
- Tracker announce and existing TCP peer connections use CONNECT (`cmd=1`) and are unaffected.
- UDP-dependent paths (uTP peer connections, UDP trackers, DHT) rely on this tunnel; whether they work when the association is not established is not covered here, so do not assume those paths are unaffected.
- When no relay peer can answer, no action is needed — do not treat the unclosed UDP ASSOCIATE request as a libtorrent or proxy misconfiguration.

## Architecture

```
main → fuse → services → domain/infrastructure
```

| Layer | Role |
|-------|------|
| `main` | Entry point: CLI args, FUSE mount, bootstrap |
| `fuse` | FUSE protocol adapter: `Filesystem` trait impl + inode management. No DB/download/seeding logic |
| `services` | Orchestration: `TorrentService` (torrent lifecycle), `DownloadService` (piece download), `SeedingService` (seeding management) |
| `domain` | Pure data models and repository traits (`Torrent`, `TorrentFile`, `TorrentRepository`) |
| `infrastructure` | Concrete implementations: `db` (SQLite), `download` (libtorrent session), `cache` (LRU piece cache), `config` (TOML), `metadata` (.torrent parsing) |

Dependency direction: `domain` has no dependency on `infrastructure`; `infrastructure` implements `domain` traits.

## Container Deployment

torrentfs ships a Docker image (`ghcr.io/tsic404/torrentfs`) with a smart entrypoint that handles FUSE device setup and mount visibility. Whether the FUSE filesystem is visible on the **host** (a bind-mounted host directory sees the mount created inside the container) depends on the container engine and its root/user namespace mode. The container always runs torrentfs correctly — the difference is whether the mount propagates out to the host.

### FUSE visibility by container engine

| Engine | Mode | Host-visible FUSE mount? | Notes |
|---|---|---|---|
| Docker | rootful (default) | ✅ Yes | Use `--mount ...,bind-propagation=rshared` and prepare a shared host mount (see Quick Start) |
| podman | rootful (`sudo podman run ...`) | ✅ Yes | Same shared-propagation recipe as Docker |
| podman | rootless (default) | ❌ No — mount stays inside the container | Use `podman exec` to access the filesystem, or run torrentfs directly on the host |

If you need host-visible FUSE mounts, use **rootful Docker or rootful podman**. Rootless podman cannot create shared mounts — a fundamental user-namespace limitation, not a torrentfs or entrypoint bug.

For QA, the same split governs where content reads are verified: host-side under rootful engines, inside the container under rootless podman.

### One host directory per container

The `rshared` recipe above shares a host directory across containers. Do not bind-mount the **same** host directory into two torrentfs containers: the second container's `mount --bind` stacks a second FUSE mount on top of the first and severs the first container's mount — both sides then report `ENOTCONN`. The entrypoint guards against this in two ways: it takes an exclusive `flock` on the mountpoint directory (the same inode across containers, so the lock is mutually exclusive and held for the container's lifetime), and it detects a FUSE mount already present at the mountpoint. Either conflict makes it refuse to start (exit `101`) instead of clobbering the other container's mount.

Give each container its own host directory / mountpoint, or run a single container per host directory.

### Reaching host loopback services (`--network host`)

The bundled self-seed QA environment (`ci/run_self_seed_env.sh`) runs its tracker and seeder on the **host**, bound to `127.0.0.1` (loopback-only; see [Offline QA](#offline-qa-self-seeding-test-swarm)). A container runs in its own network namespace, so `127.0.0.1` inside the container is the container itself, not the host — torrentfs cannot reach the host's seeder and its announces fail.

To reach a host loopback service, run the container in the host network namespace with `--network host`:

```bash
# Docker (rootful) — host network so the container's 127.0.0.1 = host loopback
docker run --rm --network host \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsic404/torrentfs:main
```

`--network host` is orthogonal to FUSE mount visibility: combine it with the `rshared` recipe above for host-visible mounts, or with `podman exec` access for rootless podman. It applies to rootful and rootless containers alike — the isolation that matters here is the network namespace, not the user namespace.

### Port conflict with the host seeder (`listen_interfaces`)

Under `--network host` the container shares the host's network namespace, so torrentfs and the host's self-seed seeder must not bind the same port. torrentfs listens on `0.0.0.0:6881` by default (the libtorrent default; see `[connections] listen_interfaces`), and the self-seed seeder (`ci/run_self_seed_env.sh`) is also a libtorrent session that defaults to the same `6881`. With Docker's default `bridge` network the two live in separate network namespaces; `--network host` puts them in the same namespace and triggers the collision.

Give torrentfs a distinct listen port via a TOML config file passed to `--config`:

```toml
# torrentfs-config.toml
[connections]
listen_interfaces = "0.0.0.0:6882"
```

```bash
docker run --rm --network host \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  -v "$PWD/torrentfs-config.toml:/torrentfs-config.toml:ro" \
  ghcr.io/tsic404/torrentfs:main /mnt --config /torrentfs-config.toml
```

`--config` may precede or follow the mountpoint — `ghcr.io/tsic404/torrentfs:main --config /torrentfs-config.toml /mnt` is equivalent to the form above. The entrypoint parses the command line and mounts on the first positional argument regardless of where `--config` appears.

The seeder stays on `6881`; torrentfs moves to `6882`. The same applies to any other BitTorrent peer already bound to `6881` on the host — the collision is a property of the shared network namespace, not of the self-seed environment specifically.

### Rootless podman

Rootless podman **does not support shared mount propagation** (`rshared`). This is a fundamental limitation of user namespaces — the container cannot create mount events that propagate to the host.

**What works**: torrentfs mounts and operates correctly inside the container. Use `podman exec` to access the filesystem:

```bash
podman run -d --name torrentfs \
  --stop-timeout 30 \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsic404/torrentfs:main

podman exec torrentfs ls /mnt/metadata/
```

**What does not work**: the host cannot access the FUSE mount through a bind-mounted directory. Passing `-v /host:/mnt:shared` or `--mount ...,bind-propagation=rshared` is silently ineffective — rootless user namespaces cannot create shared mounts, so no mount event reaches the host. The entrypoint detects a bind mount on the mountpoint under rootless podman and **fails fast** (exit `102`) instead of silently mounting container-only, so the standard `-v …:/mnt:shared` recipe can never leave you with a "host cannot see `data/`" surprise. If you need host-visible FUSE mounts:

- Use rootful podman (`sudo podman run ...`) or Docker
- Run the bundled one-click helper `sudo ./ci/deploy_rootful.sh` — it prepares the host shared mount and starts the container with `rshared` bind propagation and a persistent state directory
- Run torrentfs directly on the host without a container

For container-only access under rootless podman, run **without** a bind mount on the mountpoint (as in the `podman exec` recipe above): the entrypoint then mounts directly on the mountpoint, container-only.

### Non-root execution (UID downgrade)

torrentfs is a network-facing daemon — it ingests untrusted `.torrent` files and links libtorrent — so the image does not run it as real root where that is avoidable:

- The image ships a dedicated non-root user `torrentfs` (UID/GID `1000`, home `/home/torrentfs`).
- In a **rootful** container (Docker, `sudo podman`), the entrypoint performs the privileged setup as root — `/dev/fuse`, the state directory, and the `rshared` bind mount — then drops the torrentfs daemon itself to `torrentfs` via `setpriv` before it starts.
- Under **rootless podman**, container UID `0` already maps to the invoking host user through the user namespace: the process is unprivileged on the host, and dropping to a subuid would sever access to `/dev/fuse` and bind-mounted state volumes. The entrypoint therefore keeps the mapped root and does not drop.

The daemon's state directory follows the running user's XDG data dir — in a rootful container that is `/home/torrentfs/.local/share/torrentfs` (the old root-running image used `/root/.local/share/torrentfs`); bind-mount a persistent volume there (see `ci/deploy_rootful.sh`). You can also run as a non-root user directly (`podman run --user 1000:1000 --userns keep-id ...`): the entrypoint skips the root-only setup and mounts directly on the mountpoint, container-only. The mountpoint must be writable by that user — rootless podman's user namespace maps the image's root-owned `/mnt` to the invoking user, but `docker run --user 1000:1000` keeps `/mnt` root-owned, so pass a writable bind mount (`-v /path:/mnt`) for the FUSE mount to succeed.

Non-root FUSE mounting needs `/dev/fuse` to be world-accessible (the udev default is `0666`) or the daemon user to be in the host's `fuse` group; the entrypoint widens `/dev/fuse` only when it creates the node itself, never a `--device`-provided one. `user_allow_other` is already enabled in `/etc/fuse.conf` at image build time.

### Shutdown and restart (`stop_timeout`)

`podman stop` / `docker stop` send SIGTERM and then SIGKILL after a grace period. The container engine default is **10 seconds**, which is not enough for torrentfs to stop the download engine, drain its worker queue, flush the cache, and unmount the FUSE filesystem. The forced SIGKILL then leaves a stale FUSE mount that reports `ENOTCONN` ("Transport endpoint is not connected") on the next start, and `mkdir /mnt` in the entrypoint would fail.

The image cannot raise that timeout — the grace period is a container-runtime setting, not an image property — so raise it at runtime:

```bash
# podman
podman run --stop-timeout 30 ... ghcr.io/tsic404/torrentfs:main

# docker
docker run --stop-timeout 30 ... ghcr.io/tsic404/torrentfs:main

# compose (both engines)
services:
  torrentfs:
    image: ghcr.io/tsic404/torrentfs:main
    stop_grace_period: 30s

# quadlet / systemd
[Container]
TimeoutStopSec=30
```

Even with a sufficient timeout, an externally killed container (or a host crash) can still leave a stale mount. The entrypoint now probes the mountpoint for `ENOTCONN` at startup and, when it finds one, lazy-unmounts it (`umount -l`) and retries automatically — printing recovery steps only if the auto-recovery itself fails.

`ENOTCONN` has a second cause: two containers sharing one host directory (see "One host directory per container" above). The entrypoint distinguishes the two — a live FUSE mount propagated in from another container is refused at startup (exit `101`), never lazily unmounted, because unmounting it would sever the *other* container's healthy mount.

## Filesystem Semantics

### rename — refusal to overwrite (non-POSIX)

torrentfs diverges from POSIX `rename(2)` overwrite semantics. Renaming a `.torrent` file or metadata directory onto an **existing** target path returns `EEXIST` and leaves both entries untouched — it does **not** atomically replace the destination the way POSIX `rename` does.

| `rename(old, new)` where `new` exists | POSIX | torrentfs |
|---|---|---|
| `new` is a file, `old` is a file | overwrites `new` | `EEXIST` (refused) |
| `new` is a directory, `old` is a directory | replaces empty dir / `ENOTEMPTY` | `EEXIST` (refused) |
| `new` resolves to the same inode as `old` | no-op (`0`) | no-op (`0`) |

Rationale: a `.torrent` file is the durable handle to a downloaded swarm; a silent overwrite would discard the replaced entry's cached pieces, seeding state, and database record without warning. Forcing the caller to remove the destination first makes the destructive step explicit. To replace `B` with `A`:

- **`.torrent` file**: `unlink B && rename A B` (`unlink` only accepts `*.torrent` names; any other name returns `EACCES`, a directory returns `EISDIR`).
- **metadata directory**: `rmdir B && rename A B` (`rmdir` requires `B` to be empty; a non-empty directory returns `ENOTEMPTY`, so unlink its `.torrent` contents first).

Source: `src/fuse/fs_service.rs` — `rename()` returns `FsError::AlreadyExists` (`EEXIST`) when the destination name already resolves to a different inode.

### `dd skip` past EOF

Reading at or beyond the end of a `data/` file returns 0 bytes (EOF), not an error — an out-of-bounds `read` is served as an empty result, exactly like a regular file. Source: `src/fuse/fs_service.rs` — `read()` returns `ReadOutcome::Ready(Vec::new())` when `offset >= file_size`.

GNU `dd` therefore behaves against a torrentfs file exactly as it does against any non-empty seekable file whose size is smaller than the skip offset:

```console
$ dd if=/mnt/torrentfs/data/<name>/file skip=999999999 count=1
dd: /mnt/torrentfs/data/<name>/file: cannot skip to specified offset
0+0 records in
0+0 records out
0 bytes copied, 0.000… s, 0.0 kB/s
```

The `cannot skip to specified offset` line is a GNU coreutils/Linux `dd` behavior (POSIX does not mandate the diagnostic wording) for a `skip` that reaches past EOF: it is emitted only when `fstat()` reports a non-empty `st_size` smaller than the requested skip offset. It is **not** an error from torrentfs:

- The `lseek` succeeds — the kernel serves it through the generic file-offset path once the FUSE `lseek` operation is reported as unsupported.
- The subsequent `read` returns 0 bytes (EOF), as above.
- `dd` exits 0.

The same command against a non-empty regular file on any Linux filesystem prints the identical message and exits 0; against a zero-byte file `dd` prints only `0+0 records` and exits 0, no diagnostic. Suppressing the warning from the filesystem side would require misreporting the file's `st_size` (or its type) to `fstat()`, which would break legitimate size queries — so torrentfs leaves the warning intact as the correct, informative signal that the requested skip exceeded the file.

### `.stats` Pieces block

Per-torrent `.stats` renders the piece lifecycle as a header line, a labelled marker line, and structured piece-metadata lines:

```text
-- Pieces (16 pieces, 256.00 KB each) --
  Pieces: [x][7][1][]...
  PieceSize: 256.00 KB
  PieceCount: 16
```

The `-- Pieces (N pieces, X each) --` header is human-readable prose; the data line is the one starting with the `Pieces:` label. Machine parsers should key on `Pieces:` rather than the `Pieces (` literal in the header, and read piece dimensions from the `PieceSize:` / `PieceCount:` key-value lines instead of regex-parsing the prose header. Each bracketed token in the `Pieces:` marker line is one piece's state, rendered back-to-back with no separator by `piece_marker()` (`src/fuse/stats.rs`):

| Marker | Meaning |
|--------|---------|
| `[x]` | cached but never accessed (`hit_count == 0`) |
| `[X n]` | cached and accessed `n` times (`hit_count > 0`) |
| `[N]` | wanted for download but not cached yet (`!is_cached && priority > 0`) |
| `[]` | not wanted and not cached (`!is_cached && priority == 0`) |

Here *cached* means the piece is present in the disk cache (`PieceStatus::is_cached`), *wanted* means a reader has requested it (`PieceStatus::priority > 0`), and the access count is `PieceStatus::hit_count`.

Source: `src/fuse/stats.rs` — `piece_block()`.

## Troubleshooting

### `cp` to the mountpoint fails with EIO (Input/output error)

A sporadic `EIO` on `cp` (or any I/O) into the mountpoint usually means a previous torrentfs instance was not fully cleaned up: the old mount is still lazily attached (or the old process still holds the FUSE device), so writes race against a half-torn-down mount.

Clean up the environment before retrying:

```bash
# 1. Force-detach any stale mount (-u unmount, -z also detach a busy mount).
fusermount -uz /path/to/mountpoint

# 2. Confirm no leftover torrentfs process still holds the mount, then kill it.
ps aux | grep -E '[t]orrentfs'
pkill -f 'torrentfs.*<mountpoint>'   # only if a stale instance is listed above

# 3. Verify the mountpoint is really gone before re-mounting.
mountpoint -q /path/to/mountpoint && echo "still mounted" || echo "clean"
```

Retry the operation only after step 3 reports the mountpoint clean. If the mountpoint lives inside a container with bind propagation (see Container Deployment), run the same steps on the host as well — a stale mount can persist on both sides of the bind.

### Non-root mount fails with `Operation not permitted` (EPERM)

torrentfs mounts with `allow_other` so non-root users can access the mount. That option only works when the host's `/etc/fuse.conf` enables `user_allow_other`. Distributions ship that line commented out (`#user_allow_other`), which makes `torrentfs /mnt --config ...` return `Operation not permitted` for a non-root user. The container image already uncomments it at build time; the **host** running the container (or a bare development machine) needs the same change:

```bash
sudo sed -i 's/^#\s*user_allow_other\s*$/user_allow_other/' /etc/fuse.conf
sudo sh -c 'grep -q "^user_allow_other$" /etc/fuse.conf || echo user_allow_other >> /etc/fuse.conf'
```

Verify the line is active, then remount:

```bash
grep '^user_allow_other$' /etc/fuse.conf && echo enabled
./target/release/torrentfs /mnt --config /path/to/config.toml
```

On a bare development machine you can instead run the idempotent helper:

```bash
sudo ./ci/enable_fuse_allow_other.sh
```

It converges to one active line when the option is absent or commented; a padded active line is not recognized by the exact-match check and a duplicate is appended. Pre-existing duplicate active lines are left as-is. `main.rs` detects the line at startup: when it is present the mount includes `allow_other`; when it is missing, a non-root mount fails with `Operation not permitted` and the error hints at `/etc/fuse.conf` — the kernel requires `user_allow_other` for any unprivileged FUSE mount, so there is no owner-only fallback.

## Offline QA: self-seeding test swarm

Public sample torrents (e.g. the Ubuntu/Debian `.torrent` files commonly used in QA) often have **no reachable seeders** on a given network. Reads through the mount then fail with `ENODATA` ("No data available") — this is correct, healthy-warn behavior, not a bug. To exercise real on-demand downloads without external infrastructure, run the bundled self-seed environment:

```bash
./ci/run_self_seed_env.sh                 # builds + starts tracker & seeder
# in another shell / container:
cp ci/selfseed/output/selfseed.torrent <mountpoint>/metadata/
cat <mountpoint>/data/selfseed/selfseed    # served by the local seeder
```

- The payload (`ci/selfseed/output/payload.txt`) is deterministic; diff it against what you read through the mount to verify integrity.
- The swarm is loopback-only (tracker `127.0.0.1:16969`, no DHT/LSD/UPnP), so it never touches public trackers. Running torrentfs inside a container while the seeder stays on the host requires `--network host` — see [Container Deployment](#container-deployment).
- Source: `ci/selfseed_env.rs` (cargo example `torrentfs-selfseed-env`).

## License

No license is currently declared: the repository has no `LICENSE` file and `Cargo.toml` sets no `license` field.

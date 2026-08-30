# torrentfs

A FUSE-based virtual filesystem for BitTorrent management. Mount `.torrent` files, browse their structure, and read file contents on-demand via the BitTorrent network.

## Documentation

Supplementary docs live in [`docs/`](docs/README.md) — see its index for
focused, discoverable guides such as test-environment network prerequisites.

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

### Key modules

- `src/fuse/` — FUSE protocol (`mod.rs`), inode management (`inodes.rs`), data resolution (`lookup.rs`), stats generation (`stats.rs`)
- `src/services/` — `torrent.rs` (DB delegation for torrent CRUD), `download.rs` (piece download orchestration), `seeding.rs` (seeding lifecycle)
- `src/domain/` — `types.rs` (data models), `repository.rs` (traits), `error.rs` (error types)
- `src/infrastructure/` — `db/` (SQLite persistence), `download/` (libtorrent session + piece management), `cache/` (LRU cache), `config/` (TOML config), `metadata/` (.torrent parsing)
- `src/seeding.rs` — `SeedingManager` (peer seeding with cache eviction callbacks)
- `src/error.rs` — re-exports from `domain::error`

Dependency direction: `domain` has no dependency on `infrastructure`; `infrastructure` implements `domain` traits.

### `[proxy]` key naming

The `[proxy]` section accepts both `type` and `proxy_type` for the proxy
kind (e.g. `socks5`). `type` matches libtorrent's `settings_pack` key and is
the canonical name; `proxy_type` is accepted as an alias for users who find
it more intuitive. Both set the same value.

## Container Deployment

torrentfs ships a Docker image (`ghcr.io/tsip404/torrentfs`) with a smart entrypoint that handles FUSE device setup and mount visibility.

### Quick Start

```bash
# Docker (rootful) — host-visible FUSE mount via shared propagation
docker run --rm \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsip404/torrentfs
```

On the host, prepare the shared mount first:
```bash
mkdir -p /host/torrentfs
mount --bind /host/torrentfs /host/torrentfs
mount --make-shared /host/torrentfs
```

### Rootless podman

Rootless podman **does not support shared mount propagation** (`rshared`). This is a fundamental limitation of user namespaces — the container cannot create mount events that propagate to the host.

**What works**: torrentfs mounts and operates correctly inside the container. Use `podman exec` to access the filesystem:

```bash
podman run -d --name torrentfs \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsip404/torrentfs

podman exec torrentfs ls /mnt/metadata/
```

**What does not work**: the host cannot access the FUSE mount through a bind-mounted directory. Passing `-v /host:/mnt:shared` or `--mount ...,bind-propagation=rshared` is silently ineffective — rootless user namespaces cannot create shared mounts, so no mount event reaches the host. The entrypoint detects bind mounts on the mountpoint in rootless mode and emits an explicit warning at startup. If you need host-visible FUSE mounts:

- Use rootful podman (`sudo podman run ...`) or Docker
- Run torrentfs directly on the host without a container

The entrypoint automatically detects rootless podman and runs in container-only mode, skipping the unsupported bind mount step.

### Shutdown and restart (`stop_timeout`)

`podman stop` / `docker stop` send SIGTERM and then SIGKILL after a grace
period. The container engine default is **10 seconds**, which is not enough
for torrentfs to stop the download engine, drain its worker queue, flush the
cache, and unmount the FUSE filesystem. The forced SIGKILL then leaves a stale
FUSE mount that reports `ENOTCONN` ("Transport endpoint is not connected") on
the next start, and `mkdir /mnt` in the entrypoint would fail.

The image cannot raise that timeout — the grace period is a container-runtime
setting, not an image property — so raise it at runtime:

```bash
# podman
podman run --stop-timeout 30 ... ghcr.io/tsip404/torrentfs

# docker
docker run --stop-timeout 30 ... ghcr.io/tsip404/torrentfs

# compose (both engines)
services:
  torrentfs:
    image: ghcr.io/tsip404/torrentfs
    stop_grace_period: 30s

# quadlet / systemd
[Container]
TimeoutStopSec=30
```

Even with a sufficient timeout, an externally killed container (or a host
crash) can still leave a stale mount. The entrypoint now probes the mountpoint
for `ENOTCONN` at startup and, when it finds one, lazy-unmounts it
(`umount -l`) and retries automatically — printing recovery steps only if the
auto-recovery itself fails.

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

## Troubleshooting

### `cp` to the mountpoint fails with EIO (Input/output error)

A sporadic `EIO` on `cp` (or any I/O) into the mountpoint usually means a
previous torrentfs instance was not fully cleaned up: the old mount is still
lazily attached (or the old process still holds the FUSE device), so writes
race against a half-torn-down mount.

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

Retry the operation only after step 3 reports the mountpoint clean. If the
mountpoint lives inside a container with bind propagation (see Container
Deployment), run the same steps on the host as well — a stale mount can
persist on both sides of the bind.

### Non-root mount fails with `Operation not permitted` (EPERM)

torrentfs mounts with `allow_other` so non-root users can access the mount.
That option only works when the host's `/etc/fuse.conf` enables
`user_allow_other`. Distributions ship that line commented out
(`#user_allow_other`), which makes `torrentfs /mnt --config ...` return
`Operation not permitted` for a non-root user. The container image already
uncomments it at build time; the **host** running the container (or a bare
development machine) needs the same change:

```bash
sudo sed -i 's/^#\s*user_allow_other\s*$/user_allow_other/' /etc/fuse.conf
sudo sh -c 'grep -q "^user_allow_other$" /etc/fuse.conf || echo user_allow_other >> /etc/fuse.conf'
```

Verify the line is active, then remount:

```bash
grep '^user_allow_other$' /etc/fuse.conf && echo enabled
torrentfs /mnt --config /path/to/config.toml
```

On a bare development machine you can instead run the idempotent helper:

```bash
sudo ./ci/enable_fuse_allow_other.sh
```

It is safe to run repeatedly: it uncomments an existing
`#user_allow_other` line or appends `user_allow_other` when the line is
absent. `main.rs` detects the line at startup and falls back to owner-only
mounting with a warning when it is missing — so a mount that succeeds
silently but only for the mounting user is this issue too.

## Offline QA: self-seeding test swarm

Public sample torrents (e.g. the Ubuntu/Debian `.torrent` files commonly used in
QA) often have **no reachable seeders** on a given network. Reads through the
mount then fail with `ENODATA` ("No data available") — this is correct,
healthy-warn behavior, not a bug. To exercise real on-demand downloads without
external infrastructure, run the bundled self-seed environment:

```bash
./ci/run_self_seed_env.sh                 # builds + starts tracker & seeder
# in another shell / container:
cp ci/selfseed/output/selfseed.torrent <mountpoint>/metadata/
cat <mountpoint>/data/selfseed/selfseed    # served by the local seeder
```

- The payload (`ci/selfseed/output/payload.txt`) is deterministic; diff it
  against what you read through the mount to verify integrity.
- The swarm is loopback-only (tracker `127.0.0.1:16969`, no DHT/LSD/UPnP), so
  it never touches public trackers.
- Source: `ci/selfseed_env.rs` (cargo example `torrentfs-selfseed-env`).

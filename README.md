# torrentfs

A FUSE-based virtual filesystem for BitTorrent management. Mount `.torrent` files, browse their structure, and read file contents on-demand via the BitTorrent network.

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

**What does not work**: the host cannot access the FUSE mount through a bind-mounted directory. If you need host-visible FUSE mounts:

- Use rootful podman (`sudo podman run ...`) or Docker
- Run torrentfs directly on the host without a container

The entrypoint automatically detects rootless podman and runs in container-only mode, skipping the unsupported bind mount step.

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

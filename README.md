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

### SOCKS5 UDP ASSOCIATE probe (socks5 proxy)

When a SOCKS5 proxy is configured, libtorrent tries to open a UDP tunnel
through the proxy by sending a SOCKS5 UDP ASSOCIATE request (`cmd=3`). The
request carries `host='0.0.0.0' port=0` — libtorrent's default send-local
endpoint when none is set, not a real connect target. A relay with no UDP
endpoint to associate cannot answer, so the relay log shows an unclosed
`cmd=3` request.

This is expected libtorrent behavior, not a configuration error:

- It is triggered by the SOCKS5 proxy configuration and an unanswered UDP
  ASSOCIATE — it does not depend on the listen port being `0`.
- It is not a fixed pair: one UDP socket is opened per listening socket, and
  libtorrent retries with exponential backoff when the association fails, so
  the `cmd=3` request reappears periodically in the log.
- Tracker announce and existing TCP peer connections use CONNECT (`cmd=1`)
  and are unaffected.
- UDP-dependent paths (uTP peer connections, UDP trackers, DHT) rely on this
  tunnel; whether they work when the association is not established is not
  covered here, so do not assume those paths are unaffected.
- When no relay peer can answer, no action is needed — do not treat the
  unclosed UDP ASSOCIATE request as a libtorrent or proxy misconfiguration.

## Container Deployment

torrentfs ships a Docker image (`ghcr.io/tsic404/torrentfs`) with a smart entrypoint that handles FUSE device setup and mount visibility. Whether the FUSE filesystem is visible on the **host** (a bind-mounted host directory sees the mount created inside the container) depends on the container engine and its root/user namespace mode. The container always runs torrentfs correctly — the difference is whether the mount propagates out to the host.

### FUSE visibility by container engine

| Engine | Mode | Host-visible FUSE mount? | Notes |
|---|---|---|---|
| Docker | rootful (default) | ✅ Yes | Use `--mount ...,bind-propagation=rshared` and prepare a shared host mount (see Quick Start) |
| podman | rootful (`sudo podman run ...`) | ✅ Yes | Same shared-propagation recipe as Docker |
| podman | rootless (default) | ❌ No — mount stays inside the container | Use `podman exec` to access the filesystem, or run torrentfs directly on the host |

If you need host-visible FUSE mounts, use **rootful Docker or rootful podman**. Rootless podman cannot create shared mounts — a fundamental user-namespace limitation, not a torrentfs or entrypoint bug.

For QA, the same split governs where content reads are verified: host-side under
rootful engines, inside the container under rootless podman — see
[`docs/qa-fuse-content-read.md`](docs/qa-fuse-content-read.md).

### Quick Start (rootful)

```bash
# Docker (rootful) — host-visible FUSE mount via shared propagation
docker run --rm \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs
```

On the host, prepare the shared mount first:

```bash
mkdir -p /host/torrentfs
mount --bind /host/torrentfs /host/torrentfs
mount --make-shared /host/torrentfs
```

### Reaching host loopback services (`--network host`)

The bundled self-seed QA environment (`ci/run_self_seed_env.sh`) runs its
tracker and seeder on the **host**, bound to `127.0.0.1` (loopback-only; see
[Offline QA](#offline-qa-self-seeding-test-swarm)). A container runs in its own
network namespace, so `127.0.0.1` inside the container is the container itself,
not the host — torrentfs cannot reach the host's seeder and its announces fail.

To reach a host loopback service, run the container in the host network
namespace with `--network host`:

```bash
# Docker (rootful) — host network so the container's 127.0.0.1 = host loopback
docker run --rm --network host \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsic404/torrentfs
```

`--network host` is orthogonal to FUSE mount visibility: combine it with the
`rshared` recipe above for host-visible mounts, or with `podman exec` access
for rootless podman. It applies to rootful and rootless containers alike — the
isolation that matters here is the network namespace, not the user namespace.

### Port conflict with the host seeder (`listen_interfaces`)

Under `--network host` the container shares the host's network namespace, so
torrentfs and the host's self-seed seeder must not bind the same port.
torrentfs listens on `0.0.0.0:6881` by default (the libtorrent default; see
`[connections] listen_interfaces`), and the self-seed seeder
(`ci/run_self_seed_env.sh`) is also a libtorrent session that defaults to the
same `6881`. With Docker's default `bridge` network the two live in separate
network namespaces; `--network host` puts them in the same namespace and
triggers the collision.

Give torrentfs a distinct listen port via a TOML config file passed to
`--config`:

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
  ghcr.io/tsic404/torrentfs /mnt --config /torrentfs-config.toml
```

The seeder stays on `6881`; torrentfs moves to `6882`. The same applies to any
other BitTorrent peer already bound to `6881` on the host — the collision is a
property of the shared network namespace, not of the self-seed environment
specifically.

### Rootless podman

Rootless podman **does not support shared mount propagation** (`rshared`). This is a fundamental limitation of user namespaces — the container cannot create mount events that propagate to the host.

**What works**: torrentfs mounts and operates correctly inside the container. Use `podman exec` to access the filesystem:

```bash
podman run -d --name torrentfs \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsic404/torrentfs

podman exec torrentfs ls /mnt/metadata/
```

**What does not work**: the host cannot access the FUSE mount through a bind-mounted directory. Passing `-v /host:/mnt:shared` or `--mount ...,bind-propagation=rshared` is silently ineffective — rootless user namespaces cannot create shared mounts, so no mount event reaches the host. The entrypoint detects bind mounts on the mountpoint in rootless mode and emits an explicit warning at startup. If you need host-visible FUSE mounts:

- Use rootful podman (`sudo podman run ...`) or Docker
- Run the bundled one-click helper `sudo ./ci/deploy_rootful.sh` — it prepares
  the host shared mount and starts the container with `rshared` bind
  propagation and a persistent state directory
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
podman run --stop-timeout 30 ... ghcr.io/tsic404/torrentfs

# docker
docker run --stop-timeout 30 ... ghcr.io/tsic404/torrentfs

# compose (both engines)
services:
  torrentfs:
    image: ghcr.io/tsic404/torrentfs
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

### `.stats` Pieces block

Per-torrent `.stats` renders the piece lifecycle as a header line, a labelled
marker line, and structured piece-metadata lines:

```text
-- Pieces (16 pieces, 256.00 KB each) --
  Pieces: [x][7][1][]...
  PieceSize: 256.00 KB
  PieceCount: 16
```

The `-- Pieces (N pieces, X each) --` header is human-readable prose; the data
line is the one starting with the `Pieces:` label. Machine parsers should key
on `Pieces:` rather than the `Pieces (` literal in the header, and read piece
dimensions from the `PieceSize:` / `PieceCount:` key-value lines instead of
regex-parsing the prose header.
Each bracketed token in the `Pieces:` marker line is one piece's state,
rendered back-to-back with no separator by `piece_marker()`
(`src/fuse/stats.rs`):

| Marker | Meaning |
|--------|---------|
| `[x]` | cached but never accessed (`hit_count == 0`) |
| `[X n]` | cached and accessed `n` times (`hit_count > 0`) |
| `[N]` | wanted for download but not cached yet (`!is_cached && priority > 0`) |
| `[]` | not wanted and not cached (`!is_cached && priority == 0`) |

Here *cached* means the piece is present in the disk cache
(`PieceStatus::is_cached`), *wanted* means a reader has requested it
(`PieceStatus::priority > 0`), and the access count is `PieceStatus::hit_count`.

Source: `src/fuse/stats.rs` — `piece_block()`.

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

It converges to one active line when the option is absent or commented; a
padded active line is not recognized by the exact-match check and a duplicate
is appended. Pre-existing duplicate active lines are left as-is. `main.rs` detects
the line at startup: when it is present the mount
includes `allow_other`; when it is missing, a non-root mount fails with
`Operation not permitted` and the error hints at `/etc/fuse.conf` — the
kernel requires `user_allow_other` for any unprivileged FUSE mount, so there
is no owner-only fallback.

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
  it never touches public trackers. Running torrentfs inside a container while
  the seeder stays on the host requires `--network host` — see
  [Container Deployment](#container-deployment).
- Source: `ci/selfseed_env.rs` (cargo example `torrentfs-selfseed-env`).

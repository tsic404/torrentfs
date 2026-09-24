# torrentfs

A FUSE-based virtual filesystem for BitTorrent management: mount `.torrent` files, browse their structure, and read file contents on-demand — pieces are fetched from the swarm only when read, then cached and re-seeded.

## Features

- **Drop-in `.torrent` ingestion** — copy a `.torrent` into `metadata/`; torrentfs parses it and exposes its tree under `data/`.
- **On-demand reads** — content is downloaded only when read, with piece priority boosted for the active read.
- **Automatic caching** — pieces are cached in memory and on disk (LRU); repeated reads skip the network.
- **Automatic seeding** — cached/downloaded pieces are re-seeded to the swarm.
- **Persistent metadata** — metadata and directory structure live in SQLite and survive restarts.
- **Virtual statistics** — `.stats` files report piece lifecycle, cache hit rates, and session status.
- **TOML configuration** — proxy, DHT, rate limits, tracker, encryption, and 15 other sections; every key is optional and falls back to libtorrent defaults.
- **Docker image** — `ghcr.io/tsic404/torrentfs` with an entrypoint handling FUSE device setup, mount visibility, and privilege drop — rootful runs a two-stage rshared bind mount for host visibility; rootless podman skips it (no UID downgrade, container-only mount) and, as container root, fails fast (exit 102) on any bind mount at the mountpoint.

## Installation

### Build from source

Requirements: Rust toolchain (stable), `libtorrent-rasterbar` 2.1.x with pkg-config metadata, `openssl` development files, `libfuse` development files, `clang` / `libclang` (for FFI bindings), and a C++17 compiler (`gcc` or `clang`).

```bash
cargo build --release       # binary at ./target/release/torrentfs
cargo install --path .      # or install to your cargo bin path
```

### Docker image

```bash
docker pull ghcr.io/tsic404/torrentfs:main
```

## Quick Start

### Local

The examples run `./target/release/torrentfs` (after `cargo install --path .`, the bare `torrentfs` name is on your `PATH`). Run `sudo modprobe fuse` and `sudo ./ci/enable_fuse_allow_other.sh` once, then:

```bash
mkdir -p /mnt/torrentfs
./target/release/torrentfs /mnt/torrentfs
cp ubuntu-24.04.iso.torrent /mnt/torrentfs/metadata/
ls /mnt/torrentfs/data/
cat /mnt/torrentfs/data/<name>/README
```

### Docker (rootful, host-visible mount)

```bash
sudo mkdir -p /host/torrentfs
sudo mount --bind /host/torrentfs /host/torrentfs && sudo mount --make-shared /host/torrentfs
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs:main
```

`/mnt` is the FUSE mountpoint, not a persistence location: the entrypoint
mounts the filesystem over it, so its tree exists only while the FUSE mount is
alive and a volume mounted at `/mnt` is shadowed by it. torrentfs persists its
state — the SQLite metadata DB and the on-disk piece cache — under the XDG data
directory (`$XDG_DATA_HOME/torrentfs`, defaulting to
`/home/torrentfs/.local/share/torrentfs` for the image's UID-1000 daemon user),
or under `--db` / `--cache` when those overrides are given. To survive
`docker stop` / restart, mount a persistent volume over that state directory:

```bash
sudo mkdir -p /host/torrentfs /host/torrentfs-state
sudo mount --bind /host/torrentfs /host/torrentfs && sudo mount --make-shared /host/torrentfs
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  -v /host/torrentfs-state:/home/torrentfs/.local/share/torrentfs \
  ghcr.io/tsic404/torrentfs:main
```

The daemon runs as UID/GID 1000 in a rootful container, so a state volume owned
by anyone else is re-homed on startup. The entrypoint probes the tree
(recursively, stopping at the first foreign entry), prints a `WARNING` block
naming every affected path, then `chown -R`s it to `1000:1000` — without the
chown the daemon cannot write its DB or cache metadata and the download engine
silently disables. A bind-mounted host directory changes ownership on the host
too, which locks out a host user whose UID differs from 1000. To keep the host
ownership, pre-own the directory (`sudo chown -R 1000:1000
/host/torrentfs-state`) or run with `--user <uid>:<gid>` so the daemon user
already matches.

### Shutdown, restart, and stale mounts

`docker stop` / `podman stop` send SIGTERM first: torrentfs drains the download
engine, unmounts its FUSE filesystem, and the entrypoint releases the rshared
bind mount, so a graceful stop also unmounts the host-visible mountpoint —
`findmnt` shows no leftover entry and `docker start` restores it cleanly.

A forced kill skips that path. `docker kill -s KILL` (or an OOM kill) terminates
the daemon with no chance to unmount, and the FUSE mount it propagated to the
host via rshared bind propagation survives the container as a stale mount that
reports `ENOTCONN` ("Transport endpoint is not connected"). The next
`docker start` then fails before the entrypoint can run — the engine cannot
re-establish a bind mount whose source path is a dead FUSE mount:

```text
invalid mount config for type "bind": stat /host/torrentfs: transport endpoint is not connected
```

Recover on the host, then restart:

```bash
sudo umount -l /host/torrentfs        # or: sudo fusermount -uz /host/torrentfs
docker start torrentfs
```

Two defenses live inside the image:

- **Exit-side detach.** On shutdown the entrypoint detaches any FUSE mount the
  daemon failed to unmount itself (`fusermount3 -u` / `fusermount -u`, then
  `umount -l`), so a daemon that exits without a clean unmount does not leave a
  stale host mount behind. If the detach itself fails after a clean daemon exit
  (status 0), the entrypoint exits `103` so the cleanup failure is not mistaken
  for a clean shutdown.
- **Startup probe.** For container-only mounts (rootless podman / non-root
  `--user`, where the stale mount lives inside the container and the engine can
  still start it), the entrypoint probes the mountpoint for `ENOTCONN` at
  startup, lazy-unmounts a stale mount, and retries automatically.
- **Severed-session recovery.** A FUSE connection can also be severed
  underneath a *live* mount (kernel abort, forced unmount): every in-flight
  request fails with `ECONNABORTED`, every later one with `ENOTCONN`, and the
  mount is dead while the daemon still runs. The daemon reports that loss with
  status `104` — distinct from `102`, which stays reserved for an external
  `fusermount -u` — and the entrypoint restarts it (bounded to 3 attempts),
  re-mounting and re-publishing the bind mount, so the container recovers
  without a `docker restart`. An intentional unmount still stops the container.

None of these can clear a host-side stale mount left by a `SIGKILL`: the entrypoint
never runs because the engine refuses the restart first, so the host-side
`umount -l` above is required. Give torrentfs enough time to stop to avoid the
situation — `docker run --stop-timeout 30`, `podman run --stop-timeout 30`, or
`stop_grace_period: 30s` in compose.

## Usage

### Adding a torrent

Copy a `.torrent` into the `metadata/` directory (any subdirectory works); each `.torrent` generates a matching tree under `data/`.

The `data/` mirror preserves the `metadata/` directory layout: a `.torrent` at `metadata/<source_path>/<name>.torrent` shows up at `data/<source_path>/<name>.torrent`. `source_path` is the path relative to `metadata/` (empty for the root).

Duplicate `.torrent` files are mapped by content, not by `info_hash` alone:

- Every `(source_path, filename)` gets its own `data/` entry — the same `info_hash` dropped under `metadata/big/` and `metadata/small/` shows in both `data/big/` and `data/small/`.
- When the `.torrent` files are byte-for-byte identical, the database stores one shared content row (metadata, file list, raw bytes) plus one source row per directory; both directories still list the shared content.
- When the `.torrent` files differ (for example, the same `info_hash` with different tracker URLs), each stores its own content row, and each directory shows its own entry.

Identical files share one `info_hash` and therefore one download state: the shared content row keeps the download progress (`resume_data`) and `created_at` of the first-inserted copy — the folded copies' resume data is the same logical value, never an independent download.

### Browsing and reading

```bash
ls /mnt/torrentfs/data/
cat /mnt/torrentfs/data/<torrent-name>/path/to/file   # data/ is read-only (EROFS for writes)
```

A read at or past a file's end returns 0 bytes (standard EOF), never an error
errno — the `read` handler short-circuits `offset >= file_size` to an empty
reply before any piece is fetched from cache or the swarm. `dd` can still print
a warning on such a read:

```bash
dd if=/mnt/torrentfs/data/<name>/file bs=1 skip=999999999 count=1
# dd: /mnt/...: cannot skip to specified offset
# 0+0 records in
# 0+0 records out
# exit status 0
```

This is coreutils `dd`'s own "skip past EOF" notice, not a torrentfs error: it
is emitted whenever the `skip=` distance exceeds the file size, on any regular
file (ext4, tmpfs, …) and not only on FUSE. Nothing is read, the exit status
stays 0, and no errno from the filesystem is involved. Pass `status=none` to
silence it (`dd … status=none`).

### Configuration

```bash
./target/release/torrentfs /mnt/torrentfs --config torrentfs-config.toml
./target/release/torrentfs --config torrentfs-config.toml --config-check
```

Every key is optional (libtorrent defaults). Example `torrentfs-config.toml`:

```toml
[connections]
listen_interfaces = "0.0.0.0:6881"

[timeouts]
read_timeout_secs = 60

[cache]
cache_size = 67108864
```

The FUSE read timeout (`[timeouts] read_timeout_secs`, in seconds) sets the per-phase wait applied to torrent state transitions and piece downloads during a read. It defaults to 60s — raise it when reading the first piece of a large cold file on a slow-but-healthy swarm, or lower it to fail fast on dead torrents. It is a torrentfs-level timeout and is not passed to libtorrent.

A read's worst-case wait exceeds this value: the engine waits up to `read_timeout_secs` for the state transition, up to 10s for a stale-piece recheck, up to 9s for peer discovery, and up to `read_timeout_secs` again for the piece download — ~139s at the default, plus a 5s FUSE dispatch margin before the read surfaces `ENODATA`.

The on-disk piece cache size (`[cache] cache_size`, in bytes) defaults to 1 GiB. Set it below the torrent's total size to force LRU eviction and re-download on repeated reads.

A read the cache cannot serve — the piece it waits on was there and is gone (evicted, purged after a failed check, or removed outside the cache), or its range is larger than the whole cache — times out with `ENODATA` like a missing seeder does, so the daemon names the cause on its own stderr: `read stalled on the on-disk cache (cache_size=1.00 MiB, read span=0.12 MiB, piece the read waits on is gone from cache); raise [cache] cache_size if the cache is evicting data the read needs`. Size `cache_size` to at least the size of the file being read so its pieces stay resident; the message also states whether a seeder is connected, since the re-download needs one. A genuine swarm problem is reported separately as `no seeder connected (Peers:N Seeds:M)`.

Note: `[rate_limits] download_rate_limit` / `upload_rate_limit` (bytes per second, `0` = unlimited) do not apply to peers on the local network — libtorrent leaves loopback/local peers unthrottled by default. Use a peer address outside the local network (routable public address) to exercise rate limits.

CLI flags: `torrentfs <mountpoint> [--db <path>] [--cache <dir>] [--config <file>] [--log-level <level>] [--log-file <path>] [--config-check]`.

In a container, the entrypoint resolves an external config file in precedence
order and injects it as `--config`, so TOML-only options such as
`[cache] cache_size` are configurable without a CLI flag:

1. an explicit `--config` CLI option;
2. the `TORRENTFS_CONFIG` environment variable;
3. a config file bind-mounted at `/etc/torrentfs.toml` (no env var needed).

The winning file is validated at startup (a bad file fails fast). It must be
readable by the daemon user (UID 1000): a rootful container re-validates the
config after the privilege drop, so a root-only `0600` mount fails fast with an
actionable error — `chmod 644` it. Setting `TORRENTFS_CONFIG` to an empty value
disables the override (no config is injected; the mounted default is not used).
After `--` (end of options) `--config` is a positional argument, not the
option, so it does not suppress `TORRENTFS_CONFIG` or the default mount path.

Pure mount override — no env var required:

```bash
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  -v /host/torrentfs-small-cache.toml:/etc/torrentfs.toml:ro \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs:main /mnt
```

Environment-variable override — any mounted path:

```bash
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  -v /host/torrentfs-small-cache.toml:/etc/torrentfs/config.toml:ro \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  -e TORRENTFS_CONFIG=/etc/torrentfs/config.toml \
  ghcr.io/tsic404/torrentfs:main /mnt
```

### Logging

torrentfs logs to stdout at `info` level by default. Verbosity follows
`--log-level` (which overrides the `RUST_LOG` environment variable) across
`error|warn|info|debug|trace`:

```bash
./target/release/torrentfs --log-level debug /mnt/torrentfs
```

`--log-file <path>` redirects logs to a file (appended; parent directories are
created on first use) instead of stdout, so the log can be bind-mounted out of
a container:

```bash
./target/release/torrentfs --log-file /var/log/torrentfs.log /mnt/torrentfs
```

Docker: mount a log directory and point `--log-file` at an absolute path inside
it. The entrypoint creates the parent directory (as root) and re-owns it to the
daemon user (UID 1000), so a root-owned bind mount stays writable after the
privilege drop. `--log-file` must be an absolute path — a relative path resolves
against the container WORKDIR (`/`), which the daemon user cannot write:

```bash
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  -v /host/logs:/logs \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs:main /mnt --log-file /logs/torrentfs.log --log-level debug
```

## Architecture

| Layer | Role |
|-------|------|
| `main` | Entry point: CLI args, FUSE mount, bootstrap |
| `fuse` | FUSE protocol adapter: `Filesystem` trait impl + inode management. No DB/download/seeding logic |
| `services` | Orchestration: `TorrentService` (torrent lifecycle), `DownloadService` (piece download), `SeedingService` (seeding management) |
| `domain` | Pure data models and repository traits (`Torrent`, `TorrentFile`, `TorrentRepository`) |
| `infrastructure` | Concrete implementations: `db` (SQLite), `download` (libtorrent session), `cache` (LRU piece cache), `config` (TOML), `metadata` (.torrent parsing) |

Dependency direction: `domain` has no dependency on `infrastructure`; `infrastructure` implements `domain` traits.

## License

No license is currently declared: the repository has no `LICENSE` file and `Cargo.toml` sets no `license` field.

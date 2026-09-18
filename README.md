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

Note: `[rate_limits] download_rate_limit` / `upload_rate_limit` (bytes per second, `0` = unlimited) do not apply to peers on the local network — libtorrent leaves loopback/local peers unthrottled by default. Use a peer address outside the local network (routable public address) to exercise rate limits.

CLI flags: `torrentfs <mountpoint> [--db <path>] [--cache <dir>] [--config <file>] [--log-level <level>] [--log-file <path>] [--config-check]`.

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

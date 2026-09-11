# torrentfs

A FUSE-based virtual filesystem for BitTorrent management: mount `.torrent` files, browse their structure, and read file contents on-demand — pieces are fetched from the swarm only when read, then cached and re-seeded.

## Features

- **Drop-in `.torrent` ingestion** — copy a `.torrent` into `metadata/`; torrentfs parses it and exposes its tree under `data/`.
- **On-demand reads** — content is downloaded only when read, with piece priority boosted for the active read.
- **Automatic caching** — pieces are cached in memory and on disk (LRU); repeated reads skip the network.
- **Automatic seeding** — cached/downloaded pieces are re-seeded to the swarm.
- **Persistent metadata** — metadata and directory structure live in SQLite and survive restarts.
- **Virtual statistics** — `.stats` files report piece lifecycle, cache hit rates, and session status.
- **TOML configuration** — proxy, DHT, rate limits, tracker, encryption, and ~15 other sections; every key is optional and falls back to libtorrent defaults.
- **Docker image** — `ghcr.io/tsic404/torrentfs` with an entrypoint handling FUSE device setup and mount visibility (rootful/rootless).

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
```

CLI flags: `torrentfs <mountpoint> [--db <path>] [--cache <dir>] [--config <file>] [--config-check]`.

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

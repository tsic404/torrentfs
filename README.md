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

`--cache <dir>` and `--db <file>` go through the same handover for paths that do
not exist yet: torrentfs creates both itself, but only after the privilege drop,
so a fresh root-owned state volume would make it exit with `EACCES`. The
entrypoint `mkdir -p`s the cache directory and the `--db` parent as root and
chowns those leaves to the daemon user, which is what makes a custom state path
usable in a container:

```bash
docker run --rm --device /dev/fuse --cap-add SYS_ADMIN \
  --mount type=bind,source=/host/torrentfs,target=/mnt,bind-propagation=rshared \
  -v /host/torrentfs-state:/state \
  ghcr.io/tsic404/torrentfs:main /mnt \
  --cache /state/cache --db /state/db/metadata.db
```

A `--user <uid>:<gid>` run cannot chown: there the daemon user must already be
able to write the paths it is given.

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

### Low-peer troubleshooting

A read that cannot be served inside the wait budget returns the prefix of
pieces that did complete as a short read, and fails with `ENODATA`
("No data available") when nothing completed. On a swarm with few peers that
is expected behaviour, not a filesystem fault — and the errno never carries
the reason. The cause is observable in the per-torrent `.stats` while the read
is parked, and in the daemon log and stderr after it fails. Nothing inside the
mount reports it: read-failure details go to the log and stderr only, with no
per-torrent diagnostics file (the `.read-errors` design was dropped).

| What the swarm is doing | `.stats` (`cat /mnt/torrentfs/data/<name>/.stats`) | Daemon log / stderr |
|---|---|---|
| **0 seeders** — no connected peer holds the data | `Seeds: 0`; a parked read shows `Waiting: yes`, `Waited: <n>s`, `Active readers: <k>`; an empty swarm with a parked read alerts as `⚠ Waiting for peers (<n>s) — no connected peers; a reader is waiting` | `read_file_range <hash>: swarm empty — waiting up to <w>s for a peer to appear (peer discovery)` while it waits; on failure `no seeder connected (Peers:0 Seeds:0) within the <w>s peer-discovery window; …` (nothing appeared) or `… after the <w>s no-seeder piece wait; no seeder is present in the swarm` (peers connected, none a seeder) |
| **1 slow seeder** — connected but delivering nothing | `Seeds: ≥1` with `Rate: ↓ 0 B/s`; `⚠ Slow swarm: seeder connected but no progress for <n>s` while a reader waits | no dedicated stderr line — the failure is logged at WARN: `Failed to read torrent file data (async): Timeout("Timed out waiting for piece <p> after <w>s. Torrent progress: <p>%")` |
| **tracker not answering** — announces yield no peers | `Peers: 0  Seeds: 0`; `-- Trackers --` renders the announce targets — one `tier 0  <url>` row per URL, `No trackers — relying on DHT/LSD` when the torrent has none, or `(unavailable — tracker list not read)` when the list could not be read — followed by the session-wide `DHT Nodes: <n> (global)`; a sustained empty swarm with no parked read adds `⚠ Health: 0 peers / 0 seeds — no connected peers; tracker may be reachable` (it claims no reachability, only that no peer is connected) | the same `no seeder connected (Peers:0 Seeds:0) …` line as the 0-seeder case. Neither surface reports the tracker's reply — torrentfs exposes no announce results, so "tracker answered with 0 peers" and "no announce arrived" are indistinguishable here; both take the same next step: check that the announce target is reachable from the daemon (host network, firewall — see below) and that the swarm is actually seeding |

While a read is parked, the per-torrent `-- Peers --` block reports the wait:

```text
  Peers: 0  Seeds: 0
  Waiting: yes
  Waited: 42s
  Active readers: 1
  ⚠ Waiting for peers (51s) — no connected peers; a reader is waiting
```

`Waiting: yes` means the engine is holding at least one read for this torrent
right now; `Waited: <n>s` is the oldest parked read's own age (it keeps growing
while that read stays parked, and the line is dropped — rather than reporting
zero — once none is); `Active readers:` counts the reads parked for this
torrent. All three come from one snapshot, so they cannot disagree. The seconds
inside an alert are a different clock — `⚠ Waiting for peers (<n>s)` counts how
long the swarm has been continuously empty, and `⚠ Slow swarm: … for <n>s` how
long the connected seeder has delivered nothing — so neither is expected to
match `Waited:`. The alert line names what a bare peer count cannot: a blocked
reader on an empty swarm (`⚠ Waiting for peers`), a connected seeder that has
delivered nothing (`⚠ Slow swarm`), or a sustained empty swarm with no reader
waiting and no download in progress (`⚠ Health`); every alert is suppressed
once all pieces are cached. `.stats` is a snapshot refreshed about once per
second while it is being read, so the first read after an idle period can
return the previous snapshot and the next one is current — read it twice when
the value matters.

To read the failure cause, grep the daemon's streams (direct run: the terminal,
or `--log-file`):

```bash
docker logs <container> 2>&1 | grep -E 'no seeder connected|read stalled on the on-disk cache|Failed to read'
```

The engine writes two of the causes to its own stderr at the moment the read
fails (the FUSE client only ever sees `ENODATA`, so this is where the reason
exists):

- `no seeder connected (Peers:<N> Seeds:<M>) within the <w>s peer-discovery window; <advice>` — the discovery window ran out with nothing that could serve the read; `<advice>` names the timeout that actually caps that window (`[timeouts] read_timeout_secs` when it is the smaller of the two, `peer_discovery_wait_secs` when it is, or both when they are equal).
- `no seeder connected (Peers:<N> Seeds:<M>) after the <w>s no-seeder piece wait; no seeder is present in the swarm` — peers are connected but none seeds (or the only seeder joined after the window and left). This window derives from `read_timeout_secs`; a larger `peer_discovery_wait_secs` cannot widen it.
- `read stalled on the on-disk cache (cache_size=…, read span=…, <evidence>); raise [cache] cache_size if the cache is evicting data the read needs` — the swarm is not the problem; the read outgrew the cache (`piece the read waits on is gone from cache` or `read span exceeds cache`). Size `cache_size` to at least the file being read.

Otherwise the failure is logged at WARN (`Failed to read from torrent file
…` / `Failed to read torrent file data (async): …`); the slow-seeder case
appears only there.

For libtorrent-level detail (piece reads, writes and hashes), restart the
daemon with `TORRENTFS_DIAG=1` set — the gate is read once, so it must be set
when the daemon starts:

```bash
docker run … -e TORRENTFS_DIAG=1 … ghcr.io/tsic404/torrentfs:main /mnt
TORRENTFS_DIAG=1 ./target/release/torrentfs /mnt/torrentfs
```

Those `[DIAG]` lines are off by default because they flood stderr during an
active download; turn them off once they have answered whether blocks arrive
at all.

A container runs in its own network namespace, so `127.0.0.1` inside it is the
container, not the host: a swarm whose tracker or seeder lives on the host's
loopback never connects and looks exactly like a dead swarm (empty `.stats`,
`no seeder connected`). Run the container with `--network host` to share the
host's namespace — that is what a host-side self-seed setup needs. Sharing the
namespace also shares the host's ports: torrentfs leaves
`[connections] listen_interfaces` unset (libtorrent's `0.0.0.0:6881`), so move
it to another port when the host seeder already listens on 6881. On a multi-NIC
host the kernel can pick a non-loopback source address even for a connection
configured to use `127.0.0.1`, leaving it stalled before the handshake
(`ss -tn` shows `SYN-SENT`) and making a healthy host-local tracker/seeder look
unreachable; that is host routing, not a torrentfs setting — re-run on a
single-NIC host, or one with working loopback routing.

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
peer_discovery_wait_secs = 30

[cache]
cache_size = 67108864
```

The FUSE read timeout (`[timeouts] read_timeout_secs`, in seconds) sets the per-phase wait applied to torrent state transitions and piece downloads during a read. It defaults to 60s — raise it when reading the first piece of a large cold file on a slow-but-healthy swarm, or lower it to fail fast on dead torrents. It is a torrentfs-level timeout and is not passed to libtorrent.

The peer-discovery wait (`[timeouts] peer_discovery_wait_secs`, in seconds) is how long a read that finds an empty swarm waits for a peer or seeder to appear before the swarm counts as sourceless. It defaults to 30s, which covers a cold start — right after the mount, the tracker's first announce and the peer connect can take several seconds (measured ~9s in a container), and a shorter window would fail that first `cat` with `ENODATA` even though the torrent is healthy. The effective wait is `min(read_timeout_secs, peer_discovery_wait_secs)`, so a short read timeout still bounds the whole read; raise both to wait longer for a slow tracker or DHT bootstrap. Like `read_timeout_secs` it is torrentfs-level and is not passed to libtorrent. A read that does run out its window reports the elapsed discovery wait in the daemon's own stderr and names this key, so the wait is tuned rather than guessed. Once a probe declares the swarm sourceless, the reads that follow it inherit that verdict for one window instead of each spending a window of their own — a whole-file `cat` reaches the engine as one read per FUSE chunk, and it must not cost one discovery window per chunk. The verdict is dropped as soon as a peer or seed connects and it lapses one window after the probe, so a seeder that comes online later is still found by a retry (the probe's announce cadence keeps running while the verdict is live).

A read's worst-case wait exceeds `read_timeout_secs`: the engine waits up to `read_timeout_secs` for the state transition, up to 10s for a stale-piece recheck, up to `min(read_timeout_secs, peer_discovery_wait_secs)` for peer discovery (30s at the defaults), up to 15s more for the no-seeder piece wait, and up to `read_timeout_secs` again for the piece download — ~175s at the defaults, plus a 5s FUSE dispatch margin before the read surfaces `ENODATA`.

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
| `services` | Orchestration: `TorrentService` (torrent lifecycle), `DownloadService` (piece download + seeding via the shared session) |
| `domain` | Pure data models and repository traits (`Torrent`, `TorrentFile`, `TorrentRepository`) |
| `infrastructure` | Concrete implementations: `db` (SQLite), `download` (libtorrent session), `cache` (LRU piece cache), `config` (TOML), `metadata` (.torrent parsing) |

Dependency direction: `domain` has no dependency on `infrastructure`; `infrastructure` implements `domain` traits.

## License

No license is currently declared: the repository has no `LICENSE` file and `Cargo.toml` sets no `license` field.

# QA: verifying content reads across container engines

Content-read scenarios (torrent data contents, file reads) are the acceptance
checks that depend on FUSE mount visibility. The FUSE mount is created inside
the container; whether a **host-side** path (`docker cp`, `podman cp`, or a
bind-mounted host directory) can read it depends entirely on the container
engine and its root/user namespace mode.

## Limitation

Under **rootless podman** (the default `podman run`) the FUSE mount is visible
only inside the container. Shared mount propagation (`rshared`) is unsupported
inside user namespaces, so a `:shared` bind mount is silently ineffective and
the mount never reaches the host. Consequently a host-side
`docker cp torrentfs-qa:/mnt/data/...` cannot read the FUSE content path, and a
host bind-mounted directory stays empty. This is a platform limitation, not a
torrentfs or entrypoint bug — the entrypoint already emits an explicit warning
at startup when it detects a bind mount on the mountpoint in rootless mode.

## Verification paths

| Mode | Where content reads are verified | Mechanism |
|---|---|---|
| rootful (Docker / `sudo podman`) | host | shared mount + `rshared` bind propagation exposes the FUSE filesystem to the host |
| rootless podman | inside the container | `podman exec` runs structure + content checks against the container-local mount |

### rootful path

Prepare a shared host mount and start the container with `rshared` bind
propagation (see the project README's "Container Deployment" section, or run
`sudo ./ci/deploy_rootful.sh`):

```bash
docker run -d --name torrentfs-qa \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  --mount type=bind,source=/tmp/torrentfs-qa-mnt,target=/mnt,bind-propagation=rshared \
  ghcr.io/tsic404/torrentfs:main /mnt

cat /tmp/torrentfs-qa-mnt/data/<torrent>/<file>   # host-side content read
```

### rootless path

The mount stays inside the container. Run every structure and content check
with `podman exec`:

```bash
podman run -d --name torrentfs-qa \
  --device /dev/fuse \
  --cap-add SYS_ADMIN \
  ghcr.io/tsic404/torrentfs:main /mnt

podman exec torrentfs-qa ls /mnt/data/<torrent>/          # structure
podman exec torrentfs-qa cat /mnt/data/<torrent>/<file>   # content
```

Do not attempt `docker cp torrentfs-qa:/mnt/data/...` under rootless podman —
the host cannot see the FUSE filesystem, so the copy reads nothing.

## References

- TSI-2806 — this issue.

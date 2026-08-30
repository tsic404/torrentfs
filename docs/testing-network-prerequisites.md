# Test environment network prerequisites

End-to-end byte verification through the SOCKS5 proxy is only trustworthy on
a host whose loopback routing is unambiguous.

## Blocker

On multi-NIC hosts (e.g. `enp3s0` + `tailscale0`), the kernel may select a
non-loopback source address even when libtorrent is told to use `127.0.0.1`
(`outgoing_interfaces = "127.0.0.1"`). The SYN leaves from the physical or
tailscale interface and stalls in `SYN-SENT`, so libtorrent never completes
connections to `127.0.0.1` — not even the bundled seeder's announce to its own
local tracker. The proxy end-to-end byte comparison therefore cannot run.

## Requirement

Re-run the end-to-end byte verification on a single-NIC host, or on a host
with working loopback connectivity. Do not attempt to work around kernel
routing or source-address selection in application code.

## References

- TSI-2638 — this issue.
- TSI-2584 QA comment `01a0504d-36be-71db-b0ed-02534dbd2896` (out-of-scope
  gap #1).

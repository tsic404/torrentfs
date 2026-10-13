#!/usr/bin/env bash
# Regenerates the committed FFI declarations in src/bindings.rs. ADR-002 freezes
# bindgen as the generator and requires its output to be pre-generated and
# committed: the build only includes it, so this script is the single
# regeneration entry (CI fails when regenerating produces a diff).
#
# Needs libclang (set LIBCLANG_PATH when it is not on the default search path)
# and one network fetch of the pinned bindgen CLI on first use. Formatting stays
# with bindgen instead of rustfmt, and the allowlist keeps the output to the
# wrapper's own surface: both would otherwise make the committed file depend on
# the generating machine rather than on the header.
set -euo pipefail

BINDGEN_VERSION="=0.69.5"

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tools="$(mktemp -d)"
trap 'rm -rf "$tools"' EXIT

cargo install bindgen-cli --version "$BINDGEN_VERSION" --locked --root "$tools"

"$tools/bin/bindgen" \
    "$here/wrapper/libtorrent_wrapper.h" \
    --no-rustfmt-bindings \
    --allowlist-type 'lt_.*' \
    --allowlist-function 'lt_.*' \
    --allowlist-var 'lt_.*' \
    --output "$here/src/bindings.rs"

# bindgen writes no trailing newline; the committed file ends like any source file.
printf '\n' >> "$here/src/bindings.rs"

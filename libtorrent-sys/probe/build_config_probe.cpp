// Compile-time checks on the define table (design §4 #5). Compiled twice: with
// the shared tier only (the wrapper's view) and with the shared plus the
// library-only tier (libtorrent's own view), so a define that leaks between the
// tiers or drops out of either fails the build.
#include <libtorrent/config.hpp>
#include <libtorrent/version.hpp>

#if defined(TORRENT_LINKING_SHARED) || defined(TORRENT_BUILDING_SHARED)
#error "the static form must not define TORRENT_LINKING_SHARED or TORRENT_BUILDING_SHARED"
#endif
#if defined(TORRENT_DISABLE_ENCRYPTION)
#error "encryption must stay enabled: TORRENT_DISABLE_ENCRYPTION must not be defined"
#endif
#if !defined(TORRENT_USE_OPENSSL) || !defined(TORRENT_USE_LIBCRYPTO) || !defined(TORRENT_SSL_PEERS)
#error "the shared tier must carry the whole encryption/TLS define family"
#endif
#if !defined(TORRENT_ABI_VERSION) || TORRENT_ABI_VERSION != 2
#error "the shared tier must define TORRENT_ABI_VERSION=2"
#endif
#if !defined(TORRENT_USE_I2P) || TORRENT_USE_I2P != 1
#error "the shared tier must keep I2P enabled (TORRENT_USE_I2P=1)"
#endif
#if !defined(TORRENT_USE_RTC) || TORRENT_USE_RTC != 0
#error "the shared tier must keep WebTorrent disabled (TORRENT_USE_RTC=0)"
#endif

#ifdef LT_PROBE_LIBRARY_TIER
#if !defined(TORRENT_BUILDING_LIBRARY)
#error "the library tier must define TORRENT_BUILDING_LIBRARY"
#endif
#else
#if defined(TORRENT_BUILDING_LIBRARY)
#error "library-only defines must not reach the wrapper tier"
#endif
#endif

// libtorrent's own config.hpp pins this too; the point is that the 64-bit
// offset invariant holds whichever tier is compiled.
#if !defined(_FILE_OFFSET_BITS) || _FILE_OFFSET_BITS != 64
#error "the build must keep _FILE_OFFSET_BITS=64"
#endif

static_assert(LIBTORRENT_VERSION_NUM == LT_PINNED_LIBTORRENT_VERSION_NUM,
    "fetched libtorrent source is not the pinned 2.1.1 (LIBTORRENT_VERSION_NUM 20101)");

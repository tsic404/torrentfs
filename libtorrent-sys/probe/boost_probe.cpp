// Self-sufficiency probe for the fetched Boost header tree (design §4 #2): it
// includes only boost/version.hpp and boost/asio.hpp, so it fails to compile
// unless the unpacked tree is complete, and fails its static assertion unless
// the tree is the pinned release.
#include <boost/asio.hpp>
#include <boost/version.hpp>

static_assert(BOOST_VERSION == LT_PINNED_BOOST_VERSION,
    "fetched Boost headers are not the pinned 1.86.0 (BOOST_VERSION 108600)");

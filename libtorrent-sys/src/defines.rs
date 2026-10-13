// Build pins and the compile-configuration table shared by libtorrent's own
// translation units and the C ABI wrapper (design `05-modules/libtorrent-sys.md`
// §4 #2/#5). Included by both `build.rs` and `lib.rs` so the table has exactly
// one source: a second copy is a contract violation.

/// Pins and the two-tier compile-configuration table.
pub mod defines {
    #![allow(dead_code)]

    /// Pinned libtorrent release; the fetched source must report this version.
    pub const LIBTORRENT_VERSION: &str = "2.1.1";
    /// Upstream `LIBTORRENT_VERSION_NUM` encoding `MMmmtt` (2.1.1 -> 20101).
    pub const LIBTORRENT_VERSION_NUM: u32 = 20101;
    /// Upstream release asset name, hardcoded in the pin.
    pub const LIBTORRENT_ASSET: &str = "libtorrent-rasterbar-2.1.1.tar.gz";
    /// Default source URL; overridden by `LIBTORRENT_SRC_URL`.
    pub const LIBTORRENT_SRC_URL: &str =
        "https://github.com/arvidn/libtorrent/releases/download/v2.1.1/libtorrent-rasterbar-2.1.1.tar.gz";
    /// SHA256 of the pinned asset; any mismatch fails the build.
    pub const LIBTORRENT_SHA256: &str =
        "0f163516ecef2e3331500266751de3098835a3c3ae0c2290448046c632bc0e93";
    /// Top-level directory inside the libtorrent asset.
    pub const LIBTORRENT_SRC_DIR: &str = "libtorrent-rasterbar-2.1.1";

    /// Pinned Boost release, headers only.
    pub const BOOST_VERSION: &str = "1.86.0";
    /// Upstream `BOOST_VERSION` encoding `major*100000 + minor*100 + patch`.
    pub const BOOST_VERSION_NUM: u32 = 108600;
    /// Upstream release asset name, hardcoded in the pin.
    pub const BOOST_ASSET: &str = "boost-1.86.0-b2-nodocs.tar.gz";
    /// Default source URL; overridden by `BOOST_SRC_URL`.
    pub const BOOST_SRC_URL: &str =
        "https://github.com/boostorg/boost/releases/download/boost-1.86.0/boost-1.86.0-b2-nodocs.tar.gz";
    /// SHA256 of the pinned asset; any mismatch fails the build.
    pub const BOOST_SHA256: &str =
        "2128a4c96862b5c0970c1e34d76b1d57e4a1016b80df85ad39667f30b1deba26";
    /// Top-level directory inside the Boost asset; only `boost/**` is kept.
    pub const BOOST_SRC_DIR: &str = "boost-1.86.0";

    /// Compile definitions the library and the wrapper must share verbatim.
    pub const SHARED_DEFINES: &[(&str, Option<&str>)] = &[
        ("TORRENT_ABI_VERSION", Some("2")),
        ("BOOST_ASIO_NO_DEPRECATED", None),
        ("BOOST_SYSTEM_USE_UTF8", None),
        ("BOOST_ASIO_ENABLE_CANCELIO", None),
        ("TORRENT_USE_I2P", Some("1")),
        ("TORRENT_USE_RTC", Some("0")),
        ("TORRENT_USE_OPENSSL", None),
        ("TORRENT_USE_LIBCRYPTO", None),
        ("TORRENT_SSL_PEERS", None),
        ("OPENSSL_NO_SSL2", None),
        ("OPENSSL_NO_SSL3", None),
        ("OPENSSL_NO_TLS1", None),
        ("OPENSSL_NO_TLS1_1", None),
        ("OPENSSL_NO_DTLS1", None),
    ];

    /// Compile definitions for libtorrent's own translation units only.
    pub const LIBRARY_DEFINES: &[(&str, Option<&str>)] = &[
        ("TORRENT_BUILDING_LIBRARY", None),
        ("_FILE_OFFSET_BITS", Some("64")),
        ("BOOST_ASIO_HAS_STD_CHRONO", None),
        ("BOOST_EXCEPTION_DISABLE", None),
    ];

    /// Upstream encoding used by both version pins: three dot-separated parts
    /// weighted by the caller (libtorrent `MMmmtt`, Boost `MMmmmtt`).
    pub const fn version_num(version: &str, weights: [u32; 3]) -> u32 {
        let bytes = version.as_bytes();
        let mut parts = [0u32; 3];
        let mut index = 0;
        let mut part = 0;
        while index < bytes.len() {
            let byte = bytes[index];
            if byte == b'.' {
                part += 1;
            } else {
                parts[part] = parts[part] * 10 + (byte - b'0') as u32;
            }
            index += 1;
        }
        parts[0] * weights[0] + parts[1] * weights[1] + parts[2] * weights[2]
    }
}

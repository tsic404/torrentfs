use std::env;
use std::path::PathBuf;

fn main() {
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());

    println!("cargo:rerun-if-changed=wrapper/libtorrent_wrapper.h");
    println!("cargo:rerun-if-changed=wrapper/libtorrent_wrapper.cpp");

    let libtorrent_cflags = pkg_config::Config::new()
        .probe("libtorrent-rasterbar")
        .expect("Could not find libtorrent-rasterbar");

    let openssl_cflags = pkg_config::Config::new()
        .probe("openssl")
        .expect("Could not find openssl");

    let mut cpp_build = cc::Build::new();
    cpp_build
        .cpp(true)
        .file("wrapper/libtorrent_wrapper.cpp")
        .include("wrapper")
        .flag("-std=c++17")
        .flag("-fexceptions")
        .flag("-O1")
        // Emit the boost::system error-category statics as WEAK, not
        // GNU-unique: Debian's prebuilt libtorrent emits them GNU-unique, and
        // when both this wrapper and the library do so the dynamic linker
        // coalesces them wrongly → `system_category()` dereferences a null
        // vtable → SIGSEGV in `PieceStorageDiskIO::new_torrent`. Clang already
        // emits WEAK and rejects `-fno-gnu-unique`, so probe for support:
        // GCC applies the flag, Clang skips it (already correct).
        .flag_if_supported("-fno-gnu-unique");

    // Apply libtorrent's ABI-relevant compile definitions from its pkg-config
    // Cflags (e.g. TORRENT_ABI_VERSION, TORRENT_LINKING_SHARED for shared
    // builds, TORRENT_USE_RTC=0 for static builds, plus the boost/openssl
    // feature flags). Reading them here — instead of hardcoding — keeps the
    // wrapper ABI consistent with however libtorrent was actually built.
    for (key, value) in &libtorrent_cflags.defines {
        cpp_build.define(key, value.as_deref());
    }

    for include_path in libtorrent_cflags.include_paths.iter() {
        cpp_build.include(include_path);
    }

    for include_path in openssl_cflags.include_paths.iter() {
        cpp_build.include(include_path);
    }

    cpp_build.compile("libtorrent_wrapper");

    for lib in libtorrent_cflags.libs.iter() {
        println!("cargo:rustc-link-lib={}", lib);
    }

    for lib in openssl_cflags.libs.iter() {
        println!("cargo:rustc-link-lib={}", lib);
    }

    let bindings = bindgen::Builder::default()
        .header("wrapper/libtorrent_wrapper.h")
        .generate()
        .expect("Unable to generate bindings");

    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}

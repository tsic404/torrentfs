//! Build script: fetch the pinned libtorrent and Boost sources at build time,
//! compile libtorrent plus the C ABI wrapper statically, and guard the
//! pre-generated bindings. Contract: design `05-modules/libtorrent-sys.md` §4.

use std::env;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use sha2::{Digest, Sha256};

include!("src/defines.rs");
use defines::*;

/// Sources the parse must find, so a truncated upstream CMake block fails the
/// build instead of silently dropping translation units.
const REQUIRED_SOURCES: &[&str] = &["session.cpp", "session_impl.cpp", "torrent_info.cpp"];

/// A read must make progress within this window, so a peer that stops sending
/// cannot hang the build (design §8).
const READ_TIMEOUT_S: u64 = 60;
/// Connect attempts per asset: the Dockerfile fetch this replaces used
/// `curl --retry 5`.
const FETCH_ATTEMPTS: u32 = 5;
/// First retry delay; doubled per attempt.
const RETRY_BACKOFF_MS: u64 = 1_000;
/// Response ceiling per asset: the pinned libtorrent asset is ~6.7 MiB and the
/// Boost asset ~64.6 MiB, so a response above these is not the pinned asset.
const MAX_LIBTORRENT_BYTES: u64 = 64 * 1024 * 1024;
const MAX_BOOST_BYTES: u64 = 256 * 1024 * 1024;
/// Scratch names carry this counter as well, so two files created in the same
/// process never share a path.
static SCRATCH_COUNTER: AtomicU64 = AtomicU64::new(0);

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    for watched in [
        "wrapper/libtorrent_wrapper.h",
        "wrapper/libtorrent_wrapper.cpp",
        "probe/boost_probe.cpp",
        "probe/build_config_probe.cpp",
        "src/bindings.rs",
        "src/defines.rs",
    ] {
        println!("cargo:rerun-if-changed={watched}");
    }
    for variable in [
        "LIBTORRENT_SRC_URL",
        "BOOST_SRC_URL",
        "LIBTORRENT_SYS_CACHE",
        "BOOST_HEADERS_DIR",
        "CARGO_NET_OFFLINE",
    ] {
        println!("cargo:rerun-if-env-changed={variable}");
    }

    check_bindings(&manifest_dir);

    let cache = cache_dir();
    let libtorrent_src = libtorrent_source(&cache);
    let boost_headers = boost_headers(&cache);

    // The wrapper archive must precede the libtorrent archive on the link line:
    // the linker resolves the libtorrent symbols the wrapper pulls in only from
    // archives that come after it.
    compile_wrapper(&manifest_dir, &libtorrent_src, &boost_headers);
    compile_libtorrent(&libtorrent_src, &boost_headers);
    compile_probes(&manifest_dir, &libtorrent_src, &boost_headers);

    // openssl resolves from the default system paths — headers from the
    // compiler's include path, libraries by name (no pkg-config).
    println!("cargo:rustc-link-lib=ssl");
    println!("cargo:rustc-link-lib=crypto");
}

/// Aborts the build with an actionable message (design §8).
fn fail(message: impl std::fmt::Display) -> ! {
    eprintln!("libtorrent-sys build failed: {message}");
    std::process::exit(1);
}

/// Cache for fetched sources: `LIBTORRENT_SYS_CACHE` or `$CARGO_HOME/libtorrent-sys-cache`.
fn cache_dir() -> PathBuf {
    if let Ok(dir) = env::var("LIBTORRENT_SYS_CACHE") {
        return PathBuf::from(dir);
    }
    let cargo_home = match env::var("CARGO_HOME") {
        Ok(dir) => PathBuf::from(dir),
        Err(_) => {
            let home = env::var("HOME")
                .unwrap_or_else(|_| fail("set CARGO_HOME, HOME or LIBTORRENT_SYS_CACHE"));
            PathBuf::from(home).join(".cargo")
        }
    };
    cargo_home.join("libtorrent-sys-cache")
}

/// URL override for one pinned asset; the pin itself stays authoritative.
fn source_url(variable: &str, default: &str) -> String {
    env::var(variable).unwrap_or_else(|_| default.to_string())
}

/// Extracted libtorrent source tree, downloading and verifying it if the cache
/// does not already hold it.
fn libtorrent_source(cache: &Path) -> PathBuf {
    let source = cache.join(LIBTORRENT_SRC_DIR);
    if source.join("CMakeLists.txt").is_file() {
        verify_cached_tree(
            cache,
            LIBTORRENT_ASSET,
            LIBTORRENT_SRC_DIR,
            LIBTORRENT_SHA256,
        );
        return source;
    }
    let url = source_url("LIBTORRENT_SRC_URL", LIBTORRENT_SRC_URL);
    let archive = fetch_asset(
        cache,
        LIBTORRENT_ASSET,
        &url,
        LIBTORRENT_SHA256,
        MAX_LIBTORRENT_BYTES,
    );
    unpack_asset(&archive, cache, LIBTORRENT_SRC_DIR, None, "CMakeLists.txt");
    if !source.join("CMakeLists.txt").is_file() {
        fail(format!(
            "{} does not contain {LIBTORRENT_SRC_DIR}/CMakeLists.txt",
            archive.display()
        ));
    }
    write_digest_marker(cache, LIBTORRENT_SRC_DIR, LIBTORRENT_SHA256);
    source
}

/// Boost header root: `BOOST_HEADERS_DIR` when given, else the unpacked asset.
fn boost_headers(cache: &Path) -> PathBuf {
    if let Ok(dir) = env::var("BOOST_HEADERS_DIR") {
        let dir = PathBuf::from(dir);
        if !dir.join("boost/version.hpp").is_file() {
            fail(format!(
                "BOOST_HEADERS_DIR {} has no boost/version.hpp",
                dir.display()
            ));
        }
        return dir;
    }
    let headers = cache.join(BOOST_SRC_DIR);
    if headers.join("boost/version.hpp").is_file() {
        verify_cached_tree(cache, BOOST_ASSET, BOOST_SRC_DIR, BOOST_SHA256);
        return headers;
    }
    let url = source_url("BOOST_SRC_URL", BOOST_SRC_URL);
    let archive = fetch_asset(cache, BOOST_ASSET, &url, BOOST_SHA256, MAX_BOOST_BYTES);
    // Only the header tree is unpacked: no binaries, no documentation.
    unpack_asset(
        &archive,
        cache,
        BOOST_SRC_DIR,
        Some("boost"),
        "boost/version.hpp",
    );
    if !headers.join("boost/version.hpp").is_file() {
        fail(format!(
            "{} does not contain {BOOST_SRC_DIR}/boost/version.hpp",
            archive.display()
        ));
    }
    write_digest_marker(cache, BOOST_SRC_DIR, BOOST_SHA256);
    headers
}

/// Digest marker recording the pin a cached extracted tree was unpacked from.
fn digest_marker(cache: &Path, src_dir: &str) -> PathBuf {
    cache.join(format!("{src_dir}.sha256"))
}

/// Trust check for a cached extracted tree (design §4 #2: a mismatch fails the
/// build, there is no silent skip). With the archive still in the cache the tree
/// is accepted once that archive matches the pin; without it the marker written
/// at unpack time must record the same pin.
fn verify_cached_tree(cache: &Path, asset: &str, src_dir: &str, sha256: &str) {
    let archive = cache.join(asset);
    if archive.is_file() {
        verify_sha256(&archive, sha256);
        return;
    }
    let marker = digest_marker(cache, src_dir);
    let recorded = fs::read_to_string(&marker).unwrap_or_default();
    if recorded.trim() == sha256 {
        return;
    }
    fail(format!(
        "{} is present but not verifiable: {asset} is missing from the cache and {} records {}; \
         expected SHA256 {sha256} — remove the tree and its marker, or restore the archive",
        cache.join(src_dir).display(),
        marker.display(),
        if recorded.trim().is_empty() {
            "nothing"
        } else {
            recorded.trim()
        }
    ));
}

/// Records the verified pin next to an extracted tree, so the tree stays
/// verifiable after the archive is cleaned out of the cache.
fn write_digest_marker(cache: &Path, src_dir: &str, sha256: &str) {
    let marker = digest_marker(cache, src_dir);
    fs::write(&marker, format!("{sha256}\n"))
        .unwrap_or_else(|error| fail(format!("cannot write {}: {error}", marker.display())));
}

/// Downloads one pinned asset into the cache and returns its path. A cached copy
/// is re-verified against the pin instead of re-downloaded; a mismatch fails
/// closed (design §4 #2, §8).
fn fetch_asset(cache: &Path, asset: &str, url: &str, sha256: &str, max_bytes: u64) -> PathBuf {
    fs::create_dir_all(cache).unwrap_or_else(|error| {
        fail(format!(
            "cannot create source cache {}: {error}",
            cache.display()
        ))
    });
    let archive = cache.join(asset);
    if archive.is_file() {
        verify_sha256(&archive, sha256);
        return archive;
    }
    if env::var("CARGO_NET_OFFLINE").as_deref() == Ok("true") {
        fail(format!(
            "offline (CARGO_NET_OFFLINE) and {asset} is missing from {}: expected SHA256 {sha256}; \
             run one online build or pre-populate the cache",
            cache.display()
        ));
    }
    // The partial file carries a per-process suffix, so two cargo builds sharing
    // one cache never truncate or delete each other's download.
    let partial = cache.join(format!("{asset}.part-{}", scratch_suffix()));
    let agent = ureq::builder()
        .timeout_read(Duration::from_secs(READ_TIMEOUT_S))
        .timeout_connect(Duration::from_secs(READ_TIMEOUT_S))
        .build();
    eprintln!("libtorrent-sys: fetching {asset} from {}", redact_url(url));
    let mut attempt = 1;
    loop {
        // A retry starts from scratch; this also clears the previous partial.
        fs::remove_file(&partial).ok();
        match download_once(&agent, &partial, asset, url, max_bytes) {
            Ok(()) => break,
            Err((is_transient, message)) if is_transient && attempt < FETCH_ATTEMPTS => {
                let backoff_ms = RETRY_BACKOFF_MS << (attempt - 1);
                eprintln!(
                    "libtorrent-sys: fetching {asset} from {} failed ({message}); \
                     retry {attempt}/{} in {backoff_ms} ms",
                    redact_url(url),
                    FETCH_ATTEMPTS - 1
                );
                std::thread::sleep(Duration::from_millis(backoff_ms));
                attempt += 1;
            }
            Err((_, message)) => fail(format!(
                "cannot fetch {asset} from {}: {message}; expected SHA256 {sha256}; \
                 point the source URL override at a reachable mirror or pre-populate {}",
                redact_url(url),
                cache.display()
            )),
        }
    }
    verify_sha256(&partial, sha256);
    fs::rename(&partial, &archive).unwrap_or_else(|error| {
        fail(format!(
            "cannot move {} to {}: {error}",
            partial.display(),
            archive.display()
        ))
    });
    archive
}

/// One download attempt; `Err((true, message))` is worth retrying.
fn download_once(
    agent: &ureq::Agent,
    target: &Path,
    asset: &str,
    url: &str,
    max_bytes: u64,
) -> Result<(), (bool, String)> {
    let response = agent.get(url).call().map_err(|error| {
        let is_transient = match &error {
            ureq::Error::Status(code, _) => *code >= 500 || *code == 408 || *code == 429,
            ureq::Error::Transport(_) => true,
        };
        (is_transient, error.to_string())
    })?;
    if let Some(announced) = response
        .header("Content-Length")
        .and_then(|value| value.parse::<u64>().ok())
    {
        if announced > max_bytes {
            return Err((
                false,
                format!(
                    "the response announces {announced} bytes, above the {max_bytes}-byte ceiling for {asset}"
                ),
            ));
        }
    }
    // `create_new` refuses a path another file already occupies, so the download
    // cannot be redirected through a symlink into truncating another file.
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(target)
        .map_err(|error| (false, format!("cannot write {}: {error}", target.display())))?;
    let mut body = response.into_reader();
    let mut buffer = [0u8; 64 * 1024];
    let mut written: u64 = 0;
    loop {
        match body.read(&mut buffer) {
            Ok(0) => break,
            Ok(read) => {
                written += read as u64;
                if written > max_bytes {
                    return Err((
                        false,
                        format!("{asset} exceeded the {max_bytes}-byte ceiling while downloading"),
                    ));
                }
                file.write_all(&buffer[..read]).map_err(|error| {
                    (false, format!("cannot write {}: {error}", target.display()))
                })?;
            }
            Err(error) => return Err((true, error.to_string())),
        }
    }
    file.sync_all()
        .map_err(|error| (false, format!("cannot flush {}: {error}", target.display())))?;
    Ok(())
}

/// Per-process scratch suffix (pid, clock, counter): cache files must not collide
/// with another cargo build's, and an unpredictable name cannot be pre-planted as
/// a symlink (design §4 #2 shared cache).
fn scratch_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let counter = SCRATCH_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{nanos}-{counter}", std::process::id())
}

/// Log-safe form of a source URL: scheme, host and path only. The override exists
/// for internal mirrors, whose userinfo (`user:pass@`) and query (signed or
/// token-bearing parameters) must not reach CI or build logs.
fn redact_url(url: &str) -> String {
    let (scheme, remainder) = match url.split_once("://") {
        Some((scheme, remainder)) => (format!("{scheme}://"), remainder),
        None => (String::new(), url),
    };
    let remainder = remainder.split(['?', '#']).next().unwrap_or_default();
    let (authority, path) = match remainder.find('/') {
        Some(index) => (&remainder[..index], &remainder[index..]),
        None => (remainder, ""),
    };
    let host = authority.rsplit('@').next().unwrap_or(authority);
    format!("{scheme}{host}{path}")
}

fn verify_sha256(path: &Path, expected: &str) {
    let actual = sha256_of(path);
    if actual != expected {
        fail(format!(
            "SHA256 mismatch for {}: expected {expected}, got {actual}; \
             remove the file from the cache and retry",
            path.display()
        ));
    }
}

fn sha256_of(path: &Path) -> String {
    let mut file = fs::File::open(path)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", path.display())));
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file
            .read(&mut buffer)
            .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", path.display())));
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// Unpacks `<cache>/<src_dir>` out of a verified archive; `keep` restricts the
/// extraction to one sub-tree (the Boost asset carries more than headers) and
/// `published` is the file whose presence proves the tree is complete.
fn unpack_asset(archive: &Path, cache: &Path, src_dir: &str, keep: Option<&str>, published: &str) {
    let target = cache.join(src_dir);
    // Staging is unique per process: a concurrent build unpacking the same asset
    // must not remove the directory this one is filling (design §4 #2).
    let staging = cache.join(format!("staging-{src_dir}-{}", scratch_suffix()));
    fs::create_dir_all(&staging)
        .unwrap_or_else(|error| fail(format!("cannot create {}: {error}", staging.display())));
    let file = fs::File::open(archive)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", archive.display())));
    let mut tar = tar::Archive::new(flate2::read::GzDecoder::new(file));
    match keep {
        None => tar
            .unpack(&staging)
            .unwrap_or_else(|error| fail(format!("cannot unpack {}: {error}", archive.display()))),
        Some(subtree) => {
            let prefix = Path::new(src_dir).join(subtree);
            let entries = tar.entries().unwrap_or_else(|error| {
                fail(format!("cannot read {}: {error}", archive.display()))
            });
            for entry in entries {
                let mut entry = entry.unwrap_or_else(|error| {
                    fail(format!("cannot read {}: {error}", archive.display()))
                });
                let path = entry
                    .path()
                    .unwrap_or_else(|error| {
                        fail(format!(
                            "cannot read an entry of {}: {error}",
                            archive.display()
                        ))
                    })
                    .into_owned();
                if !path.starts_with(&prefix) {
                    continue;
                }
                entry.unpack_in(&staging).unwrap_or_else(|error| {
                    fail(format!("cannot unpack {}: {error}", archive.display()))
                });
            }
        }
    }
    // A concurrent build may have published the same tree already; both copies
    // come from the same verified archive, so keep the one in place.
    if target.join(published).is_file() {
        fs::remove_dir_all(&staging).ok();
        return;
    }
    fs::remove_dir_all(&target).ok();
    let unpacked = staging.join(src_dir);
    match fs::rename(&unpacked, &target) {
        Ok(()) => {}
        Err(error) => {
            if !target.join(published).is_file() {
                fail(format!(
                    "cannot move {} into place as {}: {error}",
                    unpacked.display(),
                    target.display()
                ));
            }
        }
    }
    fs::remove_dir_all(&staging).ok();
}

/// Compiles the wrapper with the shared tier only (design §4 #5).
fn compile_wrapper(manifest_dir: &Path, libtorrent_src: &Path, boost_headers: &Path) {
    let mut build = cc::Build::new();
    build
        .cpp(true)
        .opt_level(1)
        .flag("-std=c++17")
        .flag("-fexceptions")
        .flag_if_supported("-fno-gnu-unique")
        .include(manifest_dir.join("wrapper"))
        .include(libtorrent_src.join("include"))
        .include(boost_headers)
        .file(manifest_dir.join("wrapper/libtorrent_wrapper.cpp"));
    apply_defines(&mut build, SHARED_DEFINES);
    build.compile("libtorrent_wrapper");
}

/// Compiles libtorrent's own translation units with the shared plus the
/// library-only tier, from the source list the upstream CMake file declares.
fn compile_libtorrent(libtorrent_src: &Path, boost_headers: &Path) {
    let cmake_path = libtorrent_src.join("CMakeLists.txt");
    let cmake = fs::read_to_string(&cmake_path)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", cmake_path.display())));
    let sources = cmake_list(&cmake, "sources");
    for required in REQUIRED_SOURCES {
        if !sources.iter().any(|source| source.as_str() == *required) {
            fail(format!(
                "the upstream CMake source list in {} has no {required}; the parse is stale",
                cmake_path.display()
            ));
        }
    }

    let mut build = cc::Build::new();
    build
        .cpp(true)
        .opt_level(1)
        .flag("-std=c++17")
        .flag("-fexceptions")
        .flag_if_supported("-fno-gnu-unique")
        // Upstream code compiled with a flag set that is not upstream's own:
        // its warnings are not actionable here, its errors still fail the build.
        .warnings(false)
        .include(libtorrent_src.join("include"))
        .include(libtorrent_src.join("deps/try_signal"))
        .include(boost_headers);
    apply_defines(&mut build, SHARED_DEFINES);
    apply_defines(&mut build, LIBRARY_DEFINES);
    for source in &sources {
        build.file(libtorrent_src.join("src").join(source));
    }
    for source in cmake_list(&cmake, "try_signal_sources") {
        build.file(libtorrent_src.join("deps/try_signal").join(source));
    }
    // The two conditional additions of design §4 #10: encryption and DHT are
    // both enabled, so their source sets join the upstream list.
    for source in cmake_list(&cmake, "kademlia_sources") {
        build.file(libtorrent_src.join("src/kademlia").join(source));
    }
    for source in cmake_list(&cmake, "ed25519_sources") {
        build.file(libtorrent_src.join("src/ed25519").join(source));
    }
    build.file(libtorrent_src.join("src/pe_crypto.cpp"));
    build.compile("torrent-rasterbar");
}

/// Compile-only probes: the Boost header tree must be self-sufficient, and the
/// define table must hold for both tiers (design §4 #2, #5).
fn compile_probes(manifest_dir: &Path, libtorrent_src: &Path, boost_headers: &Path) {
    let boost_pin = BOOST_VERSION_NUM.to_string();
    let libtorrent_pin = LIBTORRENT_VERSION_NUM.to_string();

    let mut boost_probe = cc::Build::new();
    boost_probe
        .cpp(true)
        .opt_level(1)
        .flag("-std=c++17")
        .flag("-fexceptions")
        .include(boost_headers)
        .define("LT_PINNED_BOOST_VERSION", Some(boost_pin.as_str()))
        .file(manifest_dir.join("probe/boost_probe.cpp"));
    apply_defines(&mut boost_probe, SHARED_DEFINES);
    boost_probe.compile_intermediates();

    for library_tier in [false, true] {
        let mut probe = cc::Build::new();
        probe
            .cpp(true)
            .opt_level(1)
            .flag("-std=c++17")
            .flag("-fexceptions")
            .include(libtorrent_src.join("include"))
            .include(boost_headers)
            .define(
                "LT_PINNED_LIBTORRENT_VERSION_NUM",
                Some(libtorrent_pin.as_str()),
            )
            .file(manifest_dir.join("probe/build_config_probe.cpp"));
        apply_defines(&mut probe, SHARED_DEFINES);
        if library_tier {
            probe.define("LT_PROBE_LIBRARY_TIER", None);
            apply_defines(&mut probe, LIBRARY_DEFINES);
        }
        probe.compile_intermediates();
    }
}

fn apply_defines(build: &mut cc::Build, defines: &[(&str, Option<&str>)]) {
    for (name, value) in defines {
        build.define(name, *value);
    }
}

/// Reads one `set(<variable> ...)` block from the upstream CMake file: the
/// pinned layout lists one source per line, with comments and blanks interleaved.
fn cmake_list(cmake: &str, variable: &str) -> Vec<String> {
    let header = format!("set({variable}");
    let mut sources = Vec::new();
    let mut collecting = false;
    for line in cmake.lines() {
        let line = line.trim();
        if !collecting {
            collecting = line == header || line.starts_with(&format!("{header} "));
            continue;
        }
        if line == ")" {
            break;
        }
        if line.ends_with(".cpp") {
            sources.push(line.to_string());
        }
    }
    if sources.is_empty() {
        fail(format!(
            "the upstream CMake file declares no sources in its `set({variable} ...)` block"
        ));
    }
    sources
}

/// Fails when the committed bindings no longer cover every `lt_*` name the
/// wrapper header declares: the header is the only declaration source, so a
/// stale bindings file would silently break the ABI (ADR-002).
fn check_bindings(manifest_dir: &Path) {
    let header_path = manifest_dir.join("wrapper/libtorrent_wrapper.h");
    let header = fs::read_to_string(&header_path)
        .unwrap_or_else(|error| fail(format!("cannot read {}: {error}", header_path.display())));
    let bindings_path = manifest_dir.join("src/bindings.rs");
    let bindings = fs::read_to_string(&bindings_path).unwrap_or_else(|error| {
        fail(format!(
            "cannot read {}: {error}; regenerate it with libtorrent-sys/regenerate-bindings.sh",
            bindings_path.display()
        ))
    });

    let mut missing = Vec::new();
    for name in declared_lt_names(&header) {
        if !bindings_declare(&bindings, &name) && !missing.contains(&name) {
            missing.push(name);
        }
    }
    if !missing.is_empty() {
        fail(format!(
            "{} is stale, {} declared name(s) missing: {}; \
             regenerate it with libtorrent-sys/regenerate-bindings.sh",
            bindings_path.display(),
            missing.len(),
            missing.join(", ")
        ));
    }
}

/// Whether the bindings file declares `name` as a whole identifier.
fn bindings_declare(bindings: &str, name: &str) -> bool {
    let bytes = bindings.as_bytes();
    let mut from = 0;
    while let Some(offset) = bindings[from..].find(name) {
        let end = from + offset + name.len();
        let boundary = bytes
            .get(end)
            .map(|byte| !(byte.is_ascii_alphanumeric() || *byte == b'_'))
            .unwrap_or(true);
        if boundary {
            return true;
        }
        from = end;
    }
    false
}

/// Collects the `lt_*` identifiers of the header's declarations, ignoring its
/// comment lines.
fn declared_lt_names(header: &str) -> Vec<String> {
    let mut names = Vec::new();
    for line in header.lines() {
        let line = line.trim();
        if line.starts_with("//") || line.starts_with("/*") || line.starts_with('*') {
            continue;
        }
        let bytes = line.as_bytes();
        let mut index = 0;
        while let Some(offset) = line[index..].find("lt_") {
            let start = index + offset;
            if start > 0 && (bytes[start - 1].is_ascii_alphanumeric() || bytes[start - 1] == b'_') {
                index = start + 3;
                continue;
            }
            let end = line[start..]
                .find(|character: char| !(character.is_ascii_alphanumeric() || character == '_'))
                .map(|offset| start + offset)
                .unwrap_or(line.len());
            names.push(line[start..end].to_string());
            index = end;
        }
    }
    names
}

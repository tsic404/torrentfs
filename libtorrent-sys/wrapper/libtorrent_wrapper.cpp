#include "libtorrent_wrapper.h"
#include <libtorrent/torrent_info.hpp>
#include <libtorrent/file_storage.hpp>
#include <libtorrent/sha1_hash.hpp>
#include <libtorrent/bdecode.hpp>
#include <libtorrent/span.hpp>
#include <libtorrent/session.hpp>
#include <libtorrent/add_torrent_params.hpp>
#include <libtorrent/torrent_handle.hpp>
#include <libtorrent/torrent_status.hpp>
#include <libtorrent/alert_types.hpp>
#include <libtorrent/torrent_flags.hpp>
#include <libtorrent/settings_pack.hpp>
#include <libtorrent/disk_interface.hpp>
#include <libtorrent/disk_buffer_holder.hpp>
#include <libtorrent/disk_observer.hpp>
#include <libtorrent/hasher.hpp>
#include <libtorrent/session_params.hpp>
#include <libtorrent/peer_request.hpp>
#include <libtorrent/storage_defs.hpp>
#include <libtorrent/aux_/vector.hpp>
#include <cstring>
#include <cstdio>
#include <cstdlib>
#include <cerrno>
#include <stdexcept>
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <chrono>
#include <atomic>
#include <thread>
#include <deque>
#include <cctype>
#include <map>
#include <set>
#include <unistd.h>
#include <fcntl.h>
#include <shared_mutex>
#include <unordered_map>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <fstream>
#include <openssl/sha.h>
// ── DIAG logging gate ──────────────────────────────────────────────────────
// Verbose per-piece/per-operation `[DIAG]` logging floods stderr during active
// download (one line per `async_write` / `async_hash` / `write_piece` block).
// Gate it behind `TORRENTFS_DIAG=1` so it is off by default (TSI-2119).
static bool torrentfs_diag_enabled() {
    static const bool enabled = [] {
        const char* v = std::getenv("TORRENTFS_DIAG");
        return v && (*v == '1' || *v == 't' || *v == 'T');
    }();
    return enabled;
}

#define TORRENTFS_DIAG(...) \
    do { if (torrentfs_diag_enabled()) { fprintf(stderr, __VA_ARGS__); } } while (0)

// ── Disk I/O diagnostics ──────────────────────────────────────────────────
// Tests read these to prove two properties of the piece write path: one fsync
// per completed piece (not one per block), and no file I/O executed on the
// libtorrent network thread. `disk_io_note_call` is invoked by every file
// operation in the piece storage; `DiskCallbackScope` marks the disk_interface
// callbacks, so I/O performed inline inside one is counted as a violation.
static std::atomic<uint64_t> g_disk_piece_fsyncs{0};
static std::atomic<uint64_t> g_disk_piece_markers{0};
static std::atomic<uint64_t> g_disk_write_blocks{0};
static std::atomic<uint64_t> g_disk_net_thread_io_calls{0};
static std::atomic<uint64_t> g_disk_callback_calls{0};
static std::atomic<uint64_t> g_disk_callback_total_us{0};
static std::atomic<uint64_t> g_disk_callback_max_us{0};
static std::atomic<uint64_t> g_read_buffers_allocated{0};
static std::atomic<uint64_t> g_read_buffers_released{0};
static std::atomic<uint64_t> g_read_buffers_live{0};
static std::atomic<uint64_t> g_read_buffers_dropped{0};
static std::atomic<uint64_t> g_piece_publish_failures{0};
static std::atomic<uint64_t> g_write_completion_errors{0};
static std::atomic<uint64_t> g_write_completion_errors_delivered{0};

static thread_local bool tl_disk_callback_active = false;
static thread_local bool tl_disk_worker_thread = false;

static void disk_io_note_call() {
    if (tl_disk_callback_active && !tl_disk_worker_thread) {
        g_disk_net_thread_io_calls.fetch_add(1, std::memory_order_relaxed);
    }
}

struct DiskCallbackScope {
    std::chrono::steady_clock::time_point m_start
        = std::chrono::steady_clock::now();
    DiskCallbackScope() { tl_disk_callback_active = true; }
    ~DiskCallbackScope() {
        tl_disk_callback_active = false;
        uint64_t const us = static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::steady_clock::now() - m_start).count());
        g_disk_callback_calls.fetch_add(1, std::memory_order_relaxed);
        g_disk_callback_total_us.fetch_add(us, std::memory_order_relaxed);
        uint64_t prev = g_disk_callback_max_us.load(std::memory_order_relaxed);
        while (us > prev
            && !g_disk_callback_max_us.compare_exchange_weak(
                prev, us, std::memory_order_relaxed)) {
        }
    }
};

// ── FFI exception fence ────────────────────────────────────────────────────
// ADR-002: no C++ exception may cross the C ABI. Every exported function runs
// its libtorrent-touching body inside this fence; the catch arms turn the
// exception into the documented error channel — the error struct when the
// signature carries one, the function's failure value otherwise — instead of
// letting it reach std::terminate.
// Outside the fence: only `free`/`delete` of objects whose destructors are
// implicitly noexcept (a throwing member destructor would call std::terminate
// before any catch arm could see it) and the compile-time constant
// `lt_torrent_i2p_enabled`.

// The single translation point from a C++ failure to `lt_error_t`: noexcept,
// and allocation-free when the message itself cannot be stored. Callers that
// carry a specific code write it back afterwards.
static void lt_ffi_error_set(lt_error_t* error, const char* message) noexcept {
    if (!error) return;
    static thread_local std::string err_msg;
    error->code = -1;
    try {
        err_msg = message ? message : "";
        error->message = err_msg.c_str();
    } catch (...) {
        // Recording the failure must not allocate: a second bad_alloc would
        // escape this noexcept function and call std::terminate — the outcome
        // the fence exists to prevent. A string literal needs no allocation.
        error->message = "out of memory while recording the error message";
    }
}

#define LT_FFI_BEGIN try {
#define LT_FFI_END(error, failure)                                          \
    } catch (const std::exception& e) {                                     \
        lt_ffi_error_set((error), e.what());                                \
        fprintf(stderr, "[wrapper] C++ exception in %s: %s\n", __func__, e.what()); \
        return failure;                                                     \
    } catch (...) {                                                         \
        lt_ffi_error_set((error), "unknown C++ exception");                  \
        fprintf(stderr, "[wrapper] unknown C++ exception in %s\n", __func__); \
        return failure;                                                     \
    }
#define LT_FFI_END_VOID(error)                                              \
    } catch (const std::exception& e) {                                     \
        lt_ffi_error_set((error), e.what());                                \
        fprintf(stderr, "[wrapper] C++ exception in %s: %s\n", __func__, e.what()); \
    } catch (...) {                                                         \
        lt_ffi_error_set((error), "unknown C++ exception");                  \
        fprintf(stderr, "[wrapper] unknown C++ exception in %s\n", __func__); \
    }
// Appended to a function that keeps its own typed catch arms (one that maps a
// libtorrent error code, for instance): the generic arm still has to exist, or
// a non-std exception would escape.
#define LT_FFI_TAIL(error, failure)                                         \
    catch (...) {                                                           \
        lt_ffi_error_set((error), "unknown C++ exception");                  \
        fprintf(stderr, "[wrapper] unknown C++ exception in %s\n", __func__); \
        return failure;                                                     \
    }

// ── Per-info-hash shared mutex for read/write synchronization (TSI-2262) ──
// The C++ PieceStorage::write_piece acquires an exclusive lock before writing
// blocks to a piece file. The Rust PieceStore::read_piece acquires a shared
// lock before reading. This prevents the write-during-read race that caused
// concurrent readers to get inconsistent (partial) data during active
// downloads.
//
// The mutexes are stored as shared_ptr<shared_mutex> and are intentionally
// NEVER erased from the map. This avoids two use-after-free scenarios:
//   1. get_piece_lock returns a shared_ptr copy (not a reference), so even
//      if the map entry were erased, the mutex stays alive until the last
//      shared_ptr is released.
//   2. lt_unlock_piece_read uses .find() (not operator[]), so it never
//      creates a spurious new mutex to unlock.
// The memory cost is ~48 bytes per torrent ever added — negligible for a
// FUSE filesystem that processes a bounded number of torrents.
static std::mutex g_piece_lock_map_mutex;
static std::unordered_map<std::string, std::shared_ptr<std::shared_mutex>> g_piece_locks;

// Look up or create the shared_mutex for the given info_hash (hex string).
// Returns a shared_ptr copy so the mutex outlives the map entry.
static std::shared_ptr<std::shared_mutex> get_piece_lock(const std::string& info_hash_hex) {
    std::lock_guard<std::mutex> map_lock(g_piece_lock_map_mutex);
    auto& ptr = g_piece_locks[info_hash_hex];
    if (!ptr) {
        ptr = std::make_shared<std::shared_mutex>();
    }
    return ptr;
}

// Find the shared_mutex without creating a new entry.
// Returns nullptr if the info_hash has no entry (e.g. no libtorrent session
// was ever created for it — happens in unit tests with synthetic keys).
static std::shared_ptr<std::shared_mutex> find_piece_lock(const std::string& info_hash_hex) {
    std::lock_guard<std::mutex> map_lock(g_piece_lock_map_mutex);
    auto it = g_piece_locks.find(info_hash_hex);
    if (it == g_piece_locks.end()) return nullptr;
    return it->second;
}

struct lt_error {
    std::string message;
    int code;
};

struct lt_file_entry_inner {
    std::string path;
    uint64_t size;
};

struct lt_torrent_metadata {
    std::string name;
    uint64_t total_size;
    uint32_t piece_length;
    uint32_t num_pieces;
    uint32_t num_files;
    std::vector<lt_file_entry_inner> files;
    lt::sha1_hash info_hash;
};

struct lt_session_wrapper {
    lt::session* session;
    std::mutex mutex;
    // `disable_hash_checks` as baked into this session's settings.  With it
    // on, a full check cannot reject a missing piece (the comparison is
    // skipped), so the cached set has to be declared from the piece files.
    bool disable_hash_checks = false;
    // Piece-cache root of a custom-storage session; empty for the plain
    // session, which keeps libtorrent's own storage.
    std::string piece_cache_dir;
    // Rate sampling for the session-stats snapshot (C-6.2.2): libtorrent 2.1
    // reports cumulative byte counters only, so the rates are the deltas
    // between two samples over the elapsed time. Only the single alert-consumer
    // thread (C-11.9) touches these.
    int64_t stats_last_recv_bytes = -1;
    int64_t stats_last_sent_bytes = -1;
    std::chrono::steady_clock::time_point stats_last_sample{};
    // Last computed one-second rates; a sample posted before the next full
    // interval reuses them (see the session-stats branch).
    int64_t stats_last_download_rate = 0;
    int64_t stats_last_upload_rate = 0;
};

namespace {

std::string sha1_to_hex(lt::sha1_hash const& h) {
    char hex[41];
    for (int i = 0; i < 20; i++) {
        snprintf(hex + i * 2, 3, "%02x", static_cast<unsigned char>(h.data()[i]));
    }
    return std::string(hex, 40);
}

// Piece files live at <pieces>/<info_hash_hex>/<info_hash_hex>:piece:<index>.
// Only a name whose suffix is all digits is a piece file: a leftover
// `<index>.incomplete` marker (a write that never finished) is not.
static bool parse_piece_file_name(char const* name, std::string const& prefix,
    int& index)
{
    std::string s(name);
    if (s.size() <= prefix.size() || s.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    for (size_t i = prefix.size(); i < s.size(); ++i) {
        if (s[i] < '0' || s[i] > '9') return false;
    }
    try {
        index = std::stoi(s.substr(prefix.size()));
    } catch (...) {
        return false;
    }
    return true;
}

// Whether any piece file of this torrent exists on disk — the trigger for a
// full check (which needs a cache to check; an empty one would hash nothing).
static bool has_cached_piece_file(std::string const& dir,
    std::string const& info_hash_hex)
{
    disk_io_note_call();
    std::string prefix = info_hash_hex + ":piece:";
    DIR* dp = opendir(dir.c_str());
    if (!dp) return false;
    bool found = false;
    while (struct dirent* entry = readdir(dp)) {
        int index = 0;
        if (parse_piece_file_name(entry->d_name, prefix, index)) {
            found = true;
            break;
        }
    }
    closedir(dp);
    return found;
}

// Declare the cached pieces in `params.have_pieces`.  Only used when hash
// checks are disabled: a full check then marks every piece as passed, so a
// partial cache would be broadcast as complete.  A piece counts only when its
// file is exactly one piece long and no `.incomplete` marker is left — the
// bitmap cannot detect a file of the right length holding wrong bytes, which
// is why the checked path keeps the full check instead.
static void declare_cached_pieces(lt::add_torrent_params& params,
    lt::torrent_info const& ti, std::string const& piece_cache_dir,
    std::string const& info_hash_hex)
{
    std::string dir = piece_cache_dir + "/" + info_hash_hex;
    std::string prefix = info_hash_hex + ":piece:";
    DIR* dp = opendir(dir.c_str());
    if (!dp) return;
    bool sized = false;
    while (struct dirent* entry = readdir(dp)) {
        int index = 0;
        if (!parse_piece_file_name(entry->d_name, prefix, index)) continue;
        if (index < 0 || index >= ti.num_pieces()) continue;
        std::string path = dir + "/" + entry->d_name;
        struct stat st;
        if (stat(path.c_str(), &st) != 0) continue;
        if (st.st_size != ti.piece_size(lt::piece_index_t(index))) continue;
        if (access((path + ".incomplete").c_str(), F_OK) == 0) continue;
        // Only a non-empty bitmap is resume data; an empty cache must not look
        // like a declaration (libtorrent then treats the torrent as new).
        if (!sized) {
            params.have_pieces.resize(ti.num_pieces(), false);
            sized = true;
        }
        params.have_pieces.set_bit(lt::piece_index_t(index));
    }
    closedir(dp);
}

// Declare the cached set on a freshly built add_torrent_params when the
// session has hash checks off and keeps pieces in the custom cache.
static void declare_cached_pieces_if_needed(lt_session_wrapper const& wrapper,
    lt::add_torrent_params& params)
{
    if (!wrapper.disable_hash_checks || wrapper.piece_cache_dir.empty()) return;
    if (!params.ti) return;
    declare_cached_pieces(params, *params.ti, wrapper.piece_cache_dir,
        sha1_to_hex(params.ti->info_hashes().get_best()));
}

} // namespace

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error) {
    try {
        auto ti = new lt::torrent_info(std::string(filepath));
        return static_cast<lt_torrent_info_t>(ti);
    } catch (const lt::system_error& e) {
        // Never record the message by hand here: an allocation failing inside
        // a handler is not caught by the later handlers of the same try, so it
        // would leave the C ABI. `lt_ffi_error_set` is noexcept and falls back
        // to a literal; the code is restored after it.
        int code = e.code().value();
        lt_ffi_error_set(error, e.what());
        if (error) error->code = code;
        return nullptr;
    } catch (const std::exception& e) {
        lt_ffi_error_set(error, e.what());
        return nullptr;
    }
    LT_FFI_TAIL(error, nullptr)
}

lt_torrent_info_t lt_torrent_info_create_from_buffer(const uint8_t* data, size_t size, lt_error_t* error) {
    try {
        lt::error_code ec;
        // Use bdecode + torrent_info(bdecode_node) instead of the deprecated
        // torrent_info(span, ec, from_span) which may not properly copy data
        // on libtorrent 2.0.x, causing stack smashing on later session use.
        lt::bdecode_node node;
        lt::bdecode(reinterpret_cast<const char*>(data),
                    reinterpret_cast<const char*>(data) + size, node, ec);
        if (ec) {
            int code = ec.value();
            lt_ffi_error_set(error, ec.message().c_str());
            if (error) error->code = code;
            return nullptr;
        }
        lt::torrent_info ti(node, ec);
        if (ec) {
            int code = ec.value();
            lt_ffi_error_set(error, ec.message().c_str());
            if (error) error->code = code;
            return nullptr;
        }
        return static_cast<lt_torrent_info_t>(new lt::torrent_info(std::move(ti)));
    } catch (const lt::system_error& e) {
        int code = e.code().value();
        lt_ffi_error_set(error, e.what());
        if (error) error->code = code;
        return nullptr;
    } catch (const std::exception& e) {
        lt_ffi_error_set(error, e.what());
        return nullptr;
    }
    LT_FFI_TAIL(error, nullptr)
}

void lt_torrent_info_destroy(lt_torrent_info_t info) {
    if (info) {
        auto ti = static_cast<lt::torrent_info*>(info);
        delete ti;
    }
}

const char* lt_torrent_info_name(lt_torrent_info_t info) {
    if (!info) return nullptr;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    static thread_local std::string name;
    name = ti->name();
    return name.c_str();
    LT_FFI_END(nullptr, nullptr)
}

uint64_t lt_torrent_info_total_size(lt_torrent_info_t info) {
    if (!info) return 0;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    return ti->total_size();
    LT_FFI_END(nullptr, 0)
}

uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info) {
    if (!info) return 0;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->piece_length());
    LT_FFI_END(nullptr, 0)
}

uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info) {
    if (!info) return 0;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_pieces());
    LT_FFI_END(nullptr, 0)
}

uint32_t lt_torrent_info_num_files(lt_torrent_info_t info) {
    if (!info) return 0;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_files());
    LT_FFI_END(nullptr, 0)
}

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count) {
    if (!info || !files || !count) return -1;

    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    const lt::file_storage& fs = ti->files();
    auto n = static_cast<uint32_t>(fs.num_files());

    auto* out = static_cast<lt_file_entry_t*>(std::calloc(n, sizeof(lt_file_entry_t)));
    if (!out) return -1;

    static thread_local std::vector<std::string> paths;
    paths.clear();
    paths.reserve(n);

    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        auto idx = static_cast<int>(i);
        paths.emplace_back(fs.file_path(i));
        out[idx].path = paths.back().c_str();
        out[idx].size = static_cast<uint64_t>(fs.file_size(i));
    }

    *files = out;
    *count = n;
    return 0;
    LT_FFI_END(nullptr, -1)
}

void lt_files_free(lt_file_entry_t* files) {
    std::free(files);
}

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out) {
    if (!info || !hash_out) return -1;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    auto h = ti->info_hashes();
    auto sha1 = h.get_best();
    std::memcpy(hash_out, sha1.data(), 20);
    return 0;
    LT_FFI_END(nullptr, -1)
}

int lt_torrent_info_hash_for_piece(lt_torrent_info_t info, int piece_index, uint8_t* hash_out) {
    if (!info || !hash_out || piece_index < 0) return -1;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    if (piece_index >= ti->num_pieces()) return -1;
    auto h = ti->hash_for_piece(lt::piece_index_t(piece_index));
    if (h.is_all_zeros()) return -1;
    std::memcpy(hash_out, h.data(), 20);
    return 0;
    LT_FFI_END(nullptr, -1)
}

// TSI-2277: Extract the private flag from the torrent info dict.
// libtorrent's torrent_info::priv() returns true when the 'private'
// field in the info dict is set to 1 (BEP-27 / PT isolation).
// Returns 1 if private, 0 if not, -1 on error (null handle).
int lt_torrent_info_is_private(lt_torrent_info_t info) {
    if (!info) return -1;
    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    return ti->priv() ? 1 : 0;
    LT_FFI_END(nullptr, -1)
}

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info) {
    if (!info) return nullptr;

    LT_FFI_BEGIN
    auto ti = static_cast<lt::torrent_info*>(info);
    auto* meta = new lt_torrent_metadata();

    meta->name = ti->name();
    meta->total_size = ti->total_size();
    meta->piece_length = static_cast<uint32_t>(ti->piece_length());
    meta->num_pieces = static_cast<uint32_t>(ti->num_pieces());
    meta->num_files = static_cast<uint32_t>(ti->num_files());

    const lt::file_storage& fs = ti->files();
    for (lt::file_index_t i(0); i < fs.end_file(); ++i) {
        meta->files.push_back({fs.file_path(i), static_cast<uint64_t>(fs.file_size(i))});
    }

    auto h = ti->info_hashes().get_best();
    meta->info_hash = h;

    return reinterpret_cast<lt_torrent_metadata_t*>(meta);
    LT_FFI_END(nullptr, nullptr)
}

void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata) {
    if (metadata) {
        auto* meta = reinterpret_cast<lt_torrent_metadata*>(metadata);
        delete meta;
    }
}

lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error) {
    LT_FFI_BEGIN
        auto wrapper = new lt_session_wrapper();
        wrapper->session = new lt::session();

        lt::settings_pack settings;
        if (listen_interface && strlen(listen_interface) > 0) {
            settings.set_str(lt::settings_pack::listen_interfaces, listen_interface);
        }
        settings.set_int(lt::settings_pack::alert_mask,
            lt::alert_category::error | lt::alert_category::status |
                lt::alert_category::piece_progress);
        // TSI-2622: keep upload↔upload connections. torrentfs adds its idle
        // handles in upload_mode, so a torrentfs↔torrentfs pair has both
        // sides upload-only; libtorrent's default
        // close_redundant_connections=true tears that connection down before
        // .stats can observe it. Match lt_session_create_with_custom_storage
        // (TSI-2467). Session::new has no user settings JSON on this path, so
        // the unconditional false is correct — user config is applied later
        // via lt_session_apply_settings.
        settings.set_bool(lt::settings_pack::close_redundant_connections, false);
        // A seeder must serve peers that share one public address — peers
        // behind the same NAT, or a foreign client on the same host as the
        // seeder it is downloading from.  With upstream's default (false)
        // libtorrent keeps a single peer connection per address: peers that
        // share an address compete for that one slot, and the side that is
        // not kept receives no piece.
        settings.set_bool(lt::settings_pack::allow_multiple_connections_per_ip, true);
        wrapper->session->apply_settings(settings);

        return static_cast<lt_session_t>(wrapper);
    LT_FFI_END(error, nullptr)
}

void lt_session_destroy(lt_session_t session) {
    if (session) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        delete wrapper->session;
        delete wrapper;
    }
}

lt_torrent_handle_t lt_session_add_torrent(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        lt_ffi_error_set(error, "Invalid session or torrent info");
        return nullptr;
    }

    LT_FFI_BEGIN
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        lt::add_torrent_params params;
        params.ti = std::make_shared<lt::torrent_info>(*ti);
        if (save_path) {
            params.save_path = save_path;
        } else {
            params.save_path = "/tmp/torrentfs-cache";
        }
        // Clear paused flag: default_flags includes paused, which prevents
        // tracker announces and peer connections. Torrent download is
        // triggered on-demand via set_piece_deadline.
        params.flags &= ~lt::torrent_flags::paused;
        declare_cached_pieces_if_needed(*wrapper, params);

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    LT_FFI_END(error, nullptr)
}

lt_torrent_handle_t lt_session_add_torrent_upload_mode(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        lt_ffi_error_set(error, "Invalid session or torrent info");
        return nullptr;
    }

    LT_FFI_BEGIN
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        lt::add_torrent_params params;
        params.ti = std::make_shared<lt::torrent_info>(*ti);
        if (save_path) {
            params.save_path = save_path;
        } else {
            params.save_path = "/tmp/torrentfs-cache";
        }
        // upload_mode: torrent will connect to trackers/peers but never request pieces.
        // Clear paused (so tracker announces work) and auto_managed (so libtorrent
        // does NOT periodically take the torrent out of upload_mode on its own —
        // torrentfs switches to download mode explicitly on the first read).
        params.flags &= ~(lt::torrent_flags::paused | lt::torrent_flags::auto_managed);
        params.flags |= lt::torrent_flags::upload_mode;
        declare_cached_pieces_if_needed(*wrapper, params);

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    LT_FFI_END(error, nullptr)
}

void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files) {
    if (!session || !handle) return;
    LT_FFI_BEGIN
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    auto h = static_cast<lt::torrent_handle*>(handle);
    lt::remove_flags_t flags = remove_files ? lt::session::delete_files : lt::remove_flags_t{};
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    wrapper->session->remove_torrent(*h, flags);
    // The handle object is the caller's: destroying it here would turn every
    // later call on the same pointer into a use-after-free instead of the
    // `invalid_torrent_handle` error the fence reports. The Rust owner drops it
    // through `lt_torrent_handle_destroy` right after this call.
    LT_FFI_END_VOID(nullptr)
}

void lt_torrent_handle_destroy(lt_torrent_handle_t handle) {
    if (handle) {
        auto h = static_cast<lt::torrent_handle*>(handle);
        delete h;
    }
}

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle) {
    if (!handle) return 0;
    LT_FFI_BEGIN
    auto h = static_cast<lt::torrent_handle*>(handle);
    return h->is_valid() ? 1 : 0;
    LT_FFI_END(nullptr, 0)
}

int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total,
    int64_t* dl_rate, int64_t* ul_rate, int64_t* total_dl, int64_t* total_ul,
    int32_t* peers, int32_t* seeds) {
    if (!handle || !state || !progress || !total_done || !total) return -1;

    LT_FFI_BEGIN
    auto h = static_cast<lt::torrent_handle*>(handle);
    // No is_valid() pre-check: a torrent removed between the check and the
    // call makes status() throw `invalid_torrent_handle`, and the fence turns
    // that into this function's documented -1. A pre-check would only narrow
    // the window, not close it.
    auto status = h->status();
    *state = static_cast<int>(status.state);
    *progress = status.progress;
    *total_done = static_cast<uint64_t>(status.total_done);
    *total = static_cast<uint64_t>(status.total);
    if (dl_rate) *dl_rate = static_cast<int64_t>(status.download_rate);
    if (ul_rate) *ul_rate = static_cast<int64_t>(status.upload_rate);
    if (total_dl) *total_dl = static_cast<int64_t>(status.total_download);
    if (total_ul) *total_ul = static_cast<int64_t>(status.total_upload);
    if (peers) *peers = status.num_peers;
    if (seeds) *seeds = status.num_seeds;
    return 0;
    LT_FFI_END(nullptr, -1)
}

void lt_piece_data_free(uint8_t* data) {
    if (data) {
        std::free(data);
    }
}

int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset) {
    if (!handle || !first_piece || !num_pieces || !file_offset) return -1;

    LT_FFI_BEGIN
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    auto t = h->torrent_file();
    if (!t) return -1;

    const auto& fs = t->files();

    // `file_index` is an int at the ABI edge: a negative value compared as
    // `lt::file_index_t` never satisfies `>= end_file()`, so it has to be
    // rejected before the conversion, and the upper bound is checked against
    // the torrent's own file count.
    if (file_index < 0) return -1;
    lt::file_index_t fi(file_index);
    if (fi >= fs.end_file()) return -1;

    auto file_size = fs.file_size(fi);
    auto piece_length = t->piece_length();
    if (piece_length <= 0) return -1;
    auto file_offset_val = fs.file_offset(fi);
    
    int64_t start_piece = file_offset_val / piece_length;
    int64_t end_offset = file_offset_val + file_size;
    int64_t end_piece = (end_offset + piece_length - 1) / piece_length;
    
    *first_piece = start_piece;
    *num_pieces = end_piece - start_piece;
    *file_offset = file_offset_val;
    
    return 0;
    LT_FFI_END(nullptr, -1)
}

int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces) {
    if (!handle || !piece_length || !num_pieces) return -1;

    LT_FFI_BEGIN
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    auto t = h->torrent_file();
    if (!t) return -1;

    *piece_length = t->piece_length();
    *num_pieces = t->num_pieces();

    return 0;
    LT_FFI_END(nullptr, -1)
}

int lt_torrent_handle_have_piece(lt_torrent_handle_t handle, int piece_index) {
    // A negative index used to reach `status.pieces[-1]` — an out-of-range
    // read. Anything outside the torrent's piece range is "not have".
    if (!handle || piece_index < 0) return 0;

    LT_FFI_BEGIN
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return 0;

    auto t = h->torrent_file();
    if (!t) return 0;
    if (piece_index >= t->num_pieces()) return 0;

    // `have_piece()` reads the piece-picker bit directly. The old
    // `h->status()` built a whole torrent_status — including the complete
    // piece bitfield — on every poll of every piece.
    return h->have_piece(lt::piece_index_t(piece_index)) ? 1 : 0;
    LT_FFI_END(nullptr, 0)
}

int lt_torrent_handle_set_piece_deadline(lt_torrent_handle_t handle, int piece_index, int deadline_ms) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        h->set_piece_deadline(lt::piece_index_t(piece_index), deadline_ms);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

int lt_torrent_handle_set_piece_priority(lt_torrent_handle_t handle, int piece_index, int priority) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        std::vector<std::pair<lt::piece_index_t, lt::download_priority_t>> pieces;
        pieces.emplace_back(lt::piece_index_t(piece_index), static_cast<lt::download_priority_t>(priority));
        h->prioritize_pieces(pieces);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

int lt_torrent_handle_set_all_piece_priorities(lt_torrent_handle_t handle, int priority) {
    if (!handle) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    try {
        auto t = h->torrent_file();
        if (!t) return -1;
        
        int num_pieces = t->num_pieces();
        std::vector<std::pair<lt::piece_index_t, lt::download_priority_t>> pieces;
        pieces.reserve(num_pieces);
        for (int i = 0; i < num_pieces; i++) {
            pieces.emplace_back(lt::piece_index_t(i), static_cast<lt::download_priority_t>(priority));
        }
        h->prioritize_pieces(pieces);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

int lt_torrent_handle_unset_flags(lt_torrent_handle_t handle, uint64_t flags) {
    if (!handle) return -1;

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    try {
        h->unset_flags(static_cast<lt::torrent_flags_t>(flags));
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

int lt_torrent_handle_set_flags(lt_torrent_handle_t handle, uint64_t flags) {
    if (!handle) return -1;

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    try {
        h->set_flags(static_cast<lt::torrent_flags_t>(flags));
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

int lt_torrent_handle_force_recheck(lt_torrent_handle_t handle) {
    if (!handle) return -1;

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    try {
        h->force_recheck();
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

// Minimal JSON parser for flat settings objects
// Handles: {"key1": "str", "key2": 123, "key3": true, "key4": false}
static void skip_json_ws(const char*& p) {
    while (*p && std::isspace(static_cast<unsigned char>(*p))) p++;
}

static std::string parse_json_string(const char*& p) {
    // p points to opening '"'
    p++;
    std::string result;
    while (*p && *p != '"') {
        if (*p == '\\' && *(p + 1)) {
            p++;
            char c = *p;
            switch (c) {
                case 'n': result += '\n'; break;
                case 't': result += '\t'; break;
                case 'r': result += '\r'; break;
                case 'b': result += '\b'; break;
                case 'f': result += '\f'; break;
                case '\\': result += '\\'; break;
                case '"': result += '"'; break;
                case '/': result += '/'; break;
                case 'u': {
                    // \uXXXX — decode 4 hex digits into UTF-8.
                    // Covers BMP plane; surrogate pairs are not handled
                    // (tracker URLs never contain astral-plane chars).
                    if (p[1] && p[2] && p[3] && p[4]) {
                        char hex[5] = {p[1], p[2], p[3], p[4], 0};
                        unsigned int cp = static_cast<unsigned int>(strtoul(hex, nullptr, 16));
                        p += 4; // advance past 4 hex digits (loop's p++ covers the 5th)
                        if (cp < 0x80) {
                            result += static_cast<char>(cp);
                        } else if (cp < 0x800) {
                            result += static_cast<char>(0xC0 | (cp >> 6));
                            result += static_cast<char>(0x80 | (cp & 0x3F));
                        } else {
                            result += static_cast<char>(0xE0 | (cp >> 12));
                            result += static_cast<char>(0x80 | ((cp >> 6) & 0x3F));
                            result += static_cast<char>(0x80 | (cp & 0x3F));
                        }
                    }
                    break;
                }
                default: result += c; break;
            }
        } else {
            result += *p;
        }
        p++;
    }
    if (*p == '"') p++;
    return result;
}

static int64_t parse_json_int(const char*& p) {
    bool negative = false;
    if (*p == '-') { negative = true; p++; }
    int64_t val = 0;
    while (*p && std::isdigit(static_cast<unsigned char>(*p))) {
        val = val * 10 + (*p - '0');
        p++;
    }
    return negative ? -val : val;
}

// Map a config `proxy_type` string to libtorrent's `proxy_type_t` enum value.
// Returns true and writes the value when `val` names a real proxy kind;
// false when it does not. Enum values mirror `proxy_type_t`
// (none=0/socks4=1/socks5=2/socks5_pw=3/http=4/http_pw=5/i2p_proxy=6).
// Rust `ProxyConfig::validate()` already rejects unknown kinds, but the
// wrapper stays defensive so a bad JSON key can never assert or misroute.
static bool proxy_type_from_string(const std::string& val, int* out) {
    if (val == "none") {
        *out = lt::settings_pack::none;
    } else if (val == "socks4") {
        *out = lt::settings_pack::socks4;
    } else if (val == "socks5") {
        *out = lt::settings_pack::socks5;
    } else if (val == "socks5_pw") {
        *out = lt::settings_pack::socks5_pw;
    } else if (val == "http") {
        *out = lt::settings_pack::http;
    } else if (val == "http_pw") {
        *out = lt::settings_pack::http_pw;
#if TORRENT_USE_I2P
    } else if (val == "i2p_proxy") {
        *out = lt::settings_pack::i2p_proxy;
#endif
    } else {
        return false;
    }
    return true;
}

static void apply_str_setting(lt::settings_pack& pack, const std::string& key, const std::string& val) {
    // Phase 1: core string settings
    if (key == "listen_interfaces") {
        pack.set_str(lt::settings_pack::listen_interfaces, val);
    } else if (key == "outgoing_interfaces") {
        pack.set_str(lt::settings_pack::outgoing_interfaces, val);
    } else if (key == "user_agent") {
        pack.set_str(lt::settings_pack::user_agent, val);
    } else if (key == "peer_fingerprint") {
        pack.set_str(lt::settings_pack::peer_fingerprint, val);
    } else if (key == "handshake_client_version") {
        // TSI-2954: peer wire-protocol handshake client version string; PT
        // trackers may inspect it to identify the client, so surface it for
        // spoofing alongside user_agent / peer_fingerprint.
        pack.set_str(lt::settings_pack::handshake_client_version, val);
    } else if (key == "proxy_hostname" || key == "host") {
        // Canonical key is `proxy_hostname`; `host` is accepted as a legacy
        // alias so any already-serialized config keeps working.
        pack.set_str(lt::settings_pack::proxy_hostname, val);
    } else if (key == "proxy_type") {
        // `proxy_type` is an int_types setting in libtorrent; the config
        // models it as a free-form string (TSI-2529). Convert it here so
        // `type = "socks5"` actually reaches the session instead of being
        // silently dropped as an unknown string key.
        int proxy_type = 0;
        if (proxy_type_from_string(val, &proxy_type)) {
            pack.set_int(lt::settings_pack::proxy_type, proxy_type);
        }
    }
    // Unknown keys are silently ignored
}

static void apply_int_setting(lt::settings_pack& pack, const std::string& key, int val) {
    // Phase 1: core integer settings
    if (key == "max_connections") {
        // libtorrent 2.0: max_connections is not directly settable via settings_pack
        // silently ignored
    } else if (key == "max_uploads") {
        // libtorrent 2.0: max_uploads is not directly settable via settings_pack
        // silently ignored
    } else if (key == "connection_speed") {
        pack.set_int(lt::settings_pack::connection_speed, val);
    } else if (key == "peer_connect_timeout") {
        pack.set_int(lt::settings_pack::peer_connect_timeout, val);
    } else if (key == "listen_queue_size") {
        pack.set_int(lt::settings_pack::listen_queue_size, val);
    } else if (key == "min_reconnect_time") {
        pack.set_int(lt::settings_pack::min_reconnect_time, val);
    } else if (key == "max_peerlist_size") {
        pack.set_int(lt::settings_pack::max_peerlist_size, val);
    } else if (key == "max_paused_peerlist_size") {
        pack.set_int(lt::settings_pack::max_paused_peerlist_size, val);
    } else if (key == "dht_announce_interval") {
        pack.set_int(lt::settings_pack::dht_announce_interval, val);
    } else if (key == "max_dht_items") {
        pack.set_int(lt::settings_pack::dht_max_dht_items, val);
    } else if (key == "max_active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "download_rate_limit") {
        pack.set_int(lt::settings_pack::download_rate_limit, val);
    } else if (key == "upload_rate_limit") {
        pack.set_int(lt::settings_pack::upload_rate_limit, val);
    } else if (key == "disk_io_write_mode") {
        pack.set_int(lt::settings_pack::disk_io_write_mode, val);
    } else if (key == "disk_io_read_mode") {
        pack.set_int(lt::settings_pack::disk_io_read_mode, val);
    } else if (key == "file_pool_size") {
        pack.set_int(lt::settings_pack::file_pool_size, val);
    } else if (key == "max_queued_disk_bytes") {
        pack.set_int(lt::settings_pack::max_queued_disk_bytes, val);
    } else if (key == "max_queued_disk_bytes_low_watermark") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "cache_size") {
        // libtorrent 2.0: cache_size removed, silently ignored
    } else if (key == "cache_expiry") {
        // libtorrent 2.0: cache_expiry removed, silently ignored
    } else if (key == "default_cache_min_age") {
        // libtorrent 2.0: default_cache_min_age removed, silently ignored
    } else if (key == "whole_pieces_threshold") {
        pack.set_int(lt::settings_pack::whole_pieces_threshold, val);
    } else if (key == "piece_timeout") {
        pack.set_int(lt::settings_pack::piece_timeout, val);
    } else if (key == "request_timeout") {
        pack.set_int(lt::settings_pack::request_timeout, val);
    } else if (key == "max_out_request_queue") {
        pack.set_int(lt::settings_pack::max_out_request_queue, val);
    } else if (key == "max_allowed_in_request_queue") {
        pack.set_int(lt::settings_pack::max_allowed_in_request_queue, val);
    } else if (key == "max_suggest_pieces") {
        pack.set_int(lt::settings_pack::max_suggest_pieces, val);
    } else if (key == "seeding_piece_quota") {
        pack.set_int(lt::settings_pack::seeding_piece_quota, val);
    } else if (key == "max_sparse_regions") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "peer_timeout") {
        pack.set_int(lt::settings_pack::peer_timeout, val);
    } else if (key == "urlseed_timeout") {
        pack.set_int(lt::settings_pack::urlseed_timeout, val);
    } else if (key == "urlseed_pipeline_size") {
        pack.set_int(lt::settings_pack::urlseed_pipeline_size, val);
    } else if (key == "stop_tracker_timeout") {
        pack.set_int(lt::settings_pack::stop_tracker_timeout, val);
    } else if (key == "tracker_completion_timeout") {
        pack.set_int(lt::settings_pack::tracker_completion_timeout, val);
    } else if (key == "tracker_receive_timeout") {
        pack.set_int(lt::settings_pack::tracker_receive_timeout, val);
    } else if (key == "inactivity_timeout") {
        pack.set_int(lt::settings_pack::inactivity_timeout, val);
    } else if (key == "tracker_backoff") {
        pack.set_int(lt::settings_pack::tracker_backoff, val);
    } else if (key == "tracker_maximum_response_length") {
        pack.set_int(lt::settings_pack::tracker_maximum_response_length, val);
    } else if (key == "min_announce_interval") {
        pack.set_int(lt::settings_pack::min_announce_interval, val);
    } else if (key == "udp_tracker_token_expiry") {
        pack.set_int(lt::settings_pack::udp_tracker_token_expiry, val);
    } else if (key == "choking_algorithm") {
        pack.set_int(lt::settings_pack::choking_algorithm, val);
    } else if (key == "seed_choking_algorithm") {
        pack.set_int(lt::settings_pack::seed_choking_algorithm, val);
    } else if (key == "mixed_mode_algorithm") {
        pack.set_int(lt::settings_pack::mixed_mode_algorithm, val);
    } else if (key == "suggest_mode") {
        pack.set_int(lt::settings_pack::suggest_mode, val);
    } else if (key == "active_downloads") {
        pack.set_int(lt::settings_pack::active_downloads, val);
    } else if (key == "active_seeds") {
        pack.set_int(lt::settings_pack::active_seeds, val);
    } else if (key == "active_checking") {
        pack.set_int(lt::settings_pack::active_checking, val);
    } else if (key == "active_limit") {
        pack.set_int(lt::settings_pack::active_limit, val);
    } else if (key == "active_tracker_limit") {
        pack.set_int(lt::settings_pack::active_tracker_limit, val);
    } else if (key == "active_lsd_limit") {
        pack.set_int(lt::settings_pack::active_lsd_limit, val);
    } else if (key == "active_dht_limit") {
        pack.set_int(lt::settings_pack::active_dht_limit, val);
    } else if (key == "auto_manage_interval") {
        pack.set_int(lt::settings_pack::auto_manage_interval, val);
    } else if (key == "auto_manage_startup") {
        pack.set_int(lt::settings_pack::auto_manage_startup, val);
    } else if (key == "share_ratio_limit") {
        pack.set_int(lt::settings_pack::share_ratio_limit, val);
    } else if (key == "seed_time_ratio_limit") {
        pack.set_int(lt::settings_pack::seed_time_ratio_limit, val);
    } else if (key == "seed_time_limit") {
        pack.set_int(lt::settings_pack::seed_time_limit, val);
    } else if (key == "encryption_policy") {
        pack.set_int(lt::settings_pack::out_enc_policy, val);
    } else if (key == "allowed_encryption_level") {
        pack.set_int(lt::settings_pack::allowed_enc_level, val);
    } else if (key == "proxy_port" || key == "port") {
        // Canonical key is `proxy_port`; `port` is accepted as a legacy alias.
        pack.set_int(lt::settings_pack::proxy_port, val);
    } else if (key == "alert_mask") {
        pack.set_int(lt::settings_pack::alert_mask, val);
    } else if (key == "alert_queue_size") {
        pack.set_int(lt::settings_pack::alert_queue_size, val);
    } else if (key == "aio_threads") {
        pack.set_int(lt::settings_pack::aio_threads, val);
    } else if (key == "network_threads") {
        // libtorrent 2.0: network_threads removed, silently ignored
    } else if (key == "checking_mem_usage") {
        pack.set_int(lt::settings_pack::checking_mem_usage, val);
    } else if (key == "tick_interval") {
        pack.set_int(lt::settings_pack::tick_interval, val);
    } else if (key == "send_buffer_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_watermark, val);
    } else if (key == "send_buffer_watermark_factor") {
        pack.set_int(lt::settings_pack::send_buffer_watermark_factor, val);
    } else if (key == "send_buffer_low_watermark") {
        pack.set_int(lt::settings_pack::send_buffer_low_watermark, val);
    } else if (key == "recv_socket_buffer_size") {
        pack.set_int(lt::settings_pack::recv_socket_buffer_size, val);
    } else if (key == "send_socket_buffer_size") {
        pack.set_int(lt::settings_pack::send_socket_buffer_size, val);
    } else if (key == "optimistic_disk_retry") {
        pack.set_int(lt::settings_pack::optimistic_disk_retry, val);
    } else if (key == "num_optimistic_unchoke_slots") {
        pack.set_int(lt::settings_pack::num_optimistic_unchoke_slots, val);
    } else if (key == "max_failcount") {
        pack.set_int(lt::settings_pack::max_failcount, val);
    } else if (key == "max_rejects") {
        pack.set_int(lt::settings_pack::max_rejects, val);
    } else if (key == "share_mode_target") {
        pack.set_int(lt::settings_pack::share_mode_target, val);
    } else if (key == "local_service_announce_interval") {
        pack.set_int(lt::settings_pack::local_service_announce_interval, val);
    } else if (key == "read_job_every") {
        // libtorrent 2.0: not available, silently ignored
    }
    // Unknown keys are silently ignored
}

static void apply_bool_setting(lt::settings_pack& pack, const std::string& key, bool val) {
    // Phase 1: core boolean settings
    if (key == "smooth_connects") {
        pack.set_bool(lt::settings_pack::smooth_connects, val);
    } else if (key == "allow_multiple_connections_per_ip") {
        pack.set_bool(lt::settings_pack::allow_multiple_connections_per_ip, val);
    } else if (key == "enable_dht") {
        pack.set_bool(lt::settings_pack::enable_dht, val);
    } else if (key == "enable_lsd") {
        pack.set_bool(lt::settings_pack::enable_lsd, val);
    } else if (key == "enable_upnp") {
        pack.set_bool(lt::settings_pack::enable_upnp, val);
    } else if (key == "enable_natpmp") {
        pack.set_bool(lt::settings_pack::enable_natpmp, val);
    } else if (key == "rate_limit_utp") {
        // libtorrent 2.0: rate_limit_utp removed, silently ignored
    } else if (key == "rate_limit_ip_overhead") {
        pack.set_bool(lt::settings_pack::rate_limit_ip_overhead, val);
    } else if (key == "use_disk_read_ahead") {
        // libtorrent 2.0: use_disk_read_ahead removed, silently ignored
    } else if (key == "lock_disk_cache") {
        // libtorrent 2.0: lock_disk_cache removed, silently ignored
    } else if (key == "no_atime_storage") {
        pack.set_bool(lt::settings_pack::no_atime_storage, val);
    } else if (key == "low_prio_disk") {
        // libtorrent 2.0: low_prio_disk removed, silently ignored
    } else if (key == "use_read_cache") {
        // libtorrent 2.0: use_read_cache removed, silently ignored
    } else if (key == "use_disk_cache_pool") {
        // libtorrent 2.0: use_disk_cache_pool removed, silently ignored
    } else if (key == "volatile_read_cache") {
        // libtorrent 2.0: volatile_read_cache deprecated, silently ignored
    } else if (key == "guided_read_cache") {
        // libtorrent 2.0: guided_read_cache removed, silently ignored
    } else if (key == "prioritize_partial_pieces") {
        pack.set_bool(lt::settings_pack::prioritize_partial_pieces, val);
    } else if (key == "drop_skipped_requests") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "announce_to_all_trackers") {
        pack.set_bool(lt::settings_pack::announce_to_all_trackers, val);
    } else if (key == "announce_to_all_tiers") {
        pack.set_bool(lt::settings_pack::announce_to_all_tiers, val);
    } else if (key == "prefer_udp_trackers") {
        pack.set_bool(lt::settings_pack::prefer_udp_trackers, val);
    } else if (key == "auto_manage_prefer_seeds") {
        pack.set_bool(lt::settings_pack::auto_manage_prefer_seeds, val);
    } else if (key == "dont_count_slow_torrents") {
        pack.set_bool(lt::settings_pack::dont_count_slow_torrents, val);
    } else if (key == "proxy_hostnames") {
        pack.set_bool(lt::settings_pack::proxy_hostnames, val);
    } else if (key == "proxy_peer_connections") {
        pack.set_bool(lt::settings_pack::proxy_peer_connections, val);
    } else if (key == "proxy_tracker_connections") {
        pack.set_bool(lt::settings_pack::proxy_tracker_connections, val);
    } else if (key == "anonymous_mode") {
        pack.set_bool(lt::settings_pack::anonymous_mode, val);
    } else if (key == "force_proxy") {
        // libtorrent 2.0: force_proxy removed, silently ignored
    } else if (key == "always_send_user_agent") {
        pack.set_bool(lt::settings_pack::always_send_user_agent, val);
    } else if (key == "ignore_resume_timestamps") {
        // libtorrent 2.0: ignore_resume_timestamps removed, silently ignored
    } else if (key == "no_recheck_incomplete_resume") {
        pack.set_bool(lt::settings_pack::no_recheck_incomplete_resume, val);
    } else if (key == "disable_hash_checks") {
        pack.set_bool(lt::settings_pack::disable_hash_checks, val);
    } else if (key == "allow_i2p_mixed") {
        pack.set_bool(lt::settings_pack::allow_i2p_mixed, val);
    } else if (key == "incoming_starts_queued") {
        // libtorrent 2.0: not available, silently ignored
    } else if (key == "ban_web_seeds") {
        pack.set_bool(lt::settings_pack::ban_web_seeds, val);
    } else if (key == "report_web_seed_downloads") {
        pack.set_bool(lt::settings_pack::report_web_seed_downloads, val);
    } else if (key == "apply_ip_filter_to_trackers") {
        pack.set_bool(lt::settings_pack::apply_ip_filter_to_trackers, val);
    } else if (key == "announce_double_nat") {
        // libtorrent 2.0: announce_double_nat removed, silently ignored
    } else if (key == "lock_files") {
        // libtorrent 2.0: lock_files removed, silently ignored
    } else if (key == "strict_super_seeding") {
        // libtorrent 2.0: strict_super_seeding removed, silently ignored
    } else if (key == "enable_os_cache") {
        pack.set_bool(lt::settings_pack::enable_os_cache, val);
    } else if (key == "close_redundant_connections") {
        pack.set_bool(lt::settings_pack::close_redundant_connections, val);
    }
    // Unknown keys are silently ignored
}

// Build a settings_pack from a JSON string (shared by session creation and runtime apply)
static lt::settings_pack build_settings_pack(const char* settings_json) {
    lt::settings_pack pack;
    if (!settings_json || !*settings_json) return pack;

    const char* p = settings_json;
    skip_json_ws(p);
    if (*p != '{') return pack;
    p++;

    while (*p) {
        skip_json_ws(p);
        if (*p == '}') { p++; break; }
        if (*p == ',') { p++; continue; }

        // Parse key
        if (*p != '"') break;
        std::string key = parse_json_string(p);

        skip_json_ws(p);
        if (*p != ':') break;
        p++;

        skip_json_ws(p);

        // Parse value
        if (*p == '"') {
            std::string val = parse_json_string(p);
            apply_str_setting(pack, key, val);
        } else if (*p == 't' || *p == 'f') {
            bool val = (*p == 't');
            while (*p && *p != ',' && *p != '}' && !std::isspace(static_cast<unsigned char>(*p))) p++;
            apply_bool_setting(pack, key, val);
        } else if (*p == '-' || std::isdigit(static_cast<unsigned char>(*p))) {
            int64_t val = parse_json_int(p);
            apply_int_setting(pack, key, static_cast<int>(val));
        } else {
            // Unknown value type, skip
            while (*p && *p != ',' && *p != '}') p++;
        }
    }

    return pack;
}

void lt_session_apply_settings(lt_session_t session, const char* settings_json) {
    if (!session) return;

    LT_FFI_BEGIN
    auto pack = build_settings_pack(settings_json);
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    wrapper->session->apply_settings(pack);
    LT_FFI_END_VOID(nullptr)
}

static bool get_session_bool_setting_impl(lt::settings_pack const& settings, const std::string& key, int* out) {
    if (key == "allow_multiple_connections_per_ip") {
        if (settings.has_val(lt::settings_pack::allow_multiple_connections_per_ip)) {
            *out = settings.get_bool(lt::settings_pack::allow_multiple_connections_per_ip) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_dht") {
        if (settings.has_val(lt::settings_pack::enable_dht)) {
            *out = settings.get_bool(lt::settings_pack::enable_dht) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_lsd") {
        if (settings.has_val(lt::settings_pack::enable_lsd)) {
            *out = settings.get_bool(lt::settings_pack::enable_lsd) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_upnp") {
        if (settings.has_val(lt::settings_pack::enable_upnp)) {
            *out = settings.get_bool(lt::settings_pack::enable_upnp) ? 1 : 0;
            return true;
        }
    } else if (key == "enable_natpmp") {
        if (settings.has_val(lt::settings_pack::enable_natpmp)) {
            *out = settings.get_bool(lt::settings_pack::enable_natpmp) ? 1 : 0;
            return true;
        }
    } else if (key == "smooth_connects") {
        if (settings.has_val(lt::settings_pack::smooth_connects)) {
            *out = settings.get_bool(lt::settings_pack::smooth_connects) ? 1 : 0;
            return true;
        }
    } else if (key == "close_redundant_connections") {
        if (settings.has_val(lt::settings_pack::close_redundant_connections)) {
            *out = settings.get_bool(lt::settings_pack::close_redundant_connections) ? 1 : 0;
            return true;
        }
    }
    return false;
}

static bool get_session_int_setting_impl(lt::settings_pack const& settings, const std::string& key, int* out) {
    if (key == "proxy_type") {
        if (settings.has_val(lt::settings_pack::proxy_type)) {
            *out = settings.get_int(lt::settings_pack::proxy_type);
            return true;
        }
    } else if (key == "proxy_port") {
        if (settings.has_val(lt::settings_pack::proxy_port)) {
            *out = settings.get_int(lt::settings_pack::proxy_port);
            return true;
        }
    }
    return false;
}

int lt_session_get_bool_setting(lt_session_t session, const char* key, int* out) {
    if (!session || !key || !out) return -1;
    LT_FFI_BEGIN
    // The lock lives inside the fence with the settings copy: `lock()` itself
    // throws `std::system_error`, which was the exception still escaping.
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    auto settings = wrapper->session->get_settings();
    if (get_session_bool_setting_impl(settings, std::string(key), out)) {
        return 0;
    }
    return -1;
    LT_FFI_END(nullptr, -1)
}

static bool get_session_str_setting_impl(lt::settings_pack const& settings, const std::string& key, std::string& out) {
    // TSI-2798: `listen_interfaces` / `outgoing_interfaces` are wired to the
    // settings_pack by `apply_str_setting` (connections.rs → JSON → wrapper),
    // but had no readback path, so nothing could verify they actually reached
    // the live session. Read them back so tests can prove the wiring.
    if (key == "listen_interfaces") {
        if (settings.has_val(lt::settings_pack::listen_interfaces)) {
            out = settings.get_str(lt::settings_pack::listen_interfaces);
            return true;
        }
    } else if (key == "outgoing_interfaces") {
        if (settings.has_val(lt::settings_pack::outgoing_interfaces)) {
            out = settings.get_str(lt::settings_pack::outgoing_interfaces);
            return true;
        }
    } else if (key == "proxy_hostname") {
        if (settings.has_val(lt::settings_pack::proxy_hostname)) {
            out = settings.get_str(lt::settings_pack::proxy_hostname);
            return true;
        }
    } else if (key == "peer_fingerprint") {
        if (settings.has_val(lt::settings_pack::peer_fingerprint)) {
            out = settings.get_str(lt::settings_pack::peer_fingerprint);
            return true;
        }
    } else if (key == "handshake_client_version") {
        if (settings.has_val(lt::settings_pack::handshake_client_version)) {
            out = settings.get_str(lt::settings_pack::handshake_client_version);
            return true;
        }
    }
    return false;
}

int lt_session_get_str_setting(lt_session_t session, const char* key, char** out) {
    if (!session || !key || !out) return -1;
    LT_FFI_BEGIN
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    auto settings = wrapper->session->get_settings();
    std::string val;
    if (get_session_str_setting_impl(settings, std::string(key), val)) {
        *out = strdup(val.c_str());
        return *out ? 0 : -1;
    }
    return -1;
    LT_FFI_END(nullptr, -1)
}

int lt_session_get_int_setting(lt_session_t session, const char* key, int* out) {
    if (!session || !key || !out) return -1;
    LT_FFI_BEGIN
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    auto settings = wrapper->session->get_settings();
    if (get_session_int_setting_impl(settings, std::string(key), out)) {
        return 0;
    }
    return -1;
    LT_FFI_END(nullptr, -1)
}

// TSI-2547: report whether I2P support is compiled into this libtorrent build.
// Mirrors the TORRENT_USE_I2P guard that wraps the `i2p_proxy` mapping in
// apply_str_setting; Rust-side config validation calls this so the accepted
// proxy_type domain matches the wrapper's actual capability.
int lt_torrent_i2p_enabled(void) {
#if TORRENT_USE_I2P
    return 1;
#else
    return 0;
#endif
}
// Include session_stats_alert header
#include <libtorrent/session_stats.hpp>

// TSI-2344: fire-and-forget `post_session_stats()`. The resulting
// `session_stats_alert` is drained exclusively by the alert-consumer thread
// (`lt_session_pop_alerts`), preserving the single-consumer invariant — the
// previous `lt_session_get_stats` polled `pop_alerts` inline and could steal
// alerts from the consumer.
void lt_session_post_session_stats(lt_session_t session) {
    if (!session) return;
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    try {
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->post_session_stats();
    } catch (const std::exception&) {
        // Fire-and-forget: a failed stats request is not an error the
        // caller can act on; the next tick retries.
    } catch (...) {
        fprintf(stderr, "[wrapper] unknown C++ exception in %s\n", __func__);
    }
}

// TSI-2468: fire-and-forget `post_torrent_updates()`. Requests libtorrent
// to refresh per-torrent statistics (num_peers, num_seeds, etc.) and emit
// `torrent_update_alert` for each torrent. Without this, `torrent_handle::
// status()` may return stale peer counts — the internal peer list is only
// refreshed when the session processes a tick or `post_torrent_updates`.
void lt_session_post_torrent_updates(lt_session_t session) {
    if (!session) return;
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    try {
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->post_torrent_updates();
    } catch (const std::exception&) {
        // Fire-and-forget
    } catch (...) {
        fprintf(stderr, "[wrapper] unknown C++ exception in %s\n", __func__);
    }
}
// ── Helper: convert sha1_hash to hex string ──
static std::string alert_info_hash_to_hex(const lt::sha1_hash& h) {
    char buf[41];
    for (int i = 0; i < 20; i++) {
        snprintf(buf + i * 2, 3, "%02x", static_cast<unsigned char>(h[i]));
    }
    buf[40] = '\0';
    return std::string(buf, 40);
}

// ── Helper: extract info_hash from a torrent_handle ──
static void alert_fill_info_hash_from_handle(const lt::torrent_handle& h, char* out) {
    if (!h.is_valid()) {
        out[0] = '\0';
        return;
    }
    auto ti = h.torrent_file();
    if (!ti) {
        out[0] = '\0';
        return;
    }
    auto hashes = ti->info_hashes();
    auto best = hashes.get_best();
    std::string hex = alert_info_hash_to_hex(best);
    std::memcpy(out, hex.c_str(), hex.size() + 1);
}

// ── Helper: distinguish a full completion from a selective-download one ──
// torrent_finished fires when every *wanted* piece (priority > 0) is
// downloaded. With selective piece priorities, filtered pieces (priority 0)
// may still be missing — a partial completion, not the whole torrent. Check
// the have-pieces bitfield: all pieces set means the full torrent downloaded.
static int alert_torrent_finished_is_complete(const lt::torrent_handle& h) {
    if (!h.is_valid()) return 0;
    auto ti = h.torrent_file();
    if (!ti) return 1; // no metadata yet; nothing filtered to judge
    int num_pieces = static_cast<int>(ti->num_pieces());
    auto st = h.status();
    if (static_cast<int>(st.pieces.size()) < num_pieces) return 0;
    for (int i = 0; i < num_pieces; ++i) {
        if (!st.pieces[lt::piece_index_t(i)]) return 0;
    }
    return 1;
}

void lt_session_set_alert_notify(lt_session_t session, void (*callback)(void* user_data), void* user_data) {
    if (!session) return;
    LT_FFI_BEGIN
    auto wrapper = static_cast<lt_session_wrapper*>(session);

    if (!callback) {
        // Clear the notify hook.
        wrapper->session->set_alert_notify(std::function<void()>());
        return;
    }

    // `callback` is a plain C function pointer; capture `user_data` by value
    // so the closure is copyable and the notify hook stays valid. The hook is
    // invoked from libtorrent's internal thread(s) on the 0→1 alert-queue
    // transition — it must not block, pop alerts, or re-enter the session.
    wrapper->session->set_alert_notify([callback, user_data]() {
        callback(user_data);
    });
    LT_FFI_END_VOID(nullptr)
}

lt_alert_list_t* lt_session_pop_alerts(lt_session_t session) {
    if (!session) return nullptr;

    auto wrapper = static_cast<lt_session_wrapper*>(session);

    LT_FFI_BEGIN
    std::vector<lt::alert*> alerts;
    {
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->pop_alerts(&alerts);
    }

    if (alerts.empty()) return nullptr;

    auto* list = static_cast<lt_alert_list_t*>(std::malloc(sizeof(lt_alert_list_t)));
    if (!list) return nullptr;
    list->count = static_cast<int>(alerts.size());
    list->alerts = static_cast<lt_alert_data_t*>(
        std::calloc(alerts.size(), sizeof(lt_alert_data_t)));
    if (!list->alerts) {
        std::free(list);
        return nullptr;
    }

    for (size_t i = 0; i < alerts.size(); i++) {
        auto& out = list->alerts[i];
        auto* alert = alerts[i];

        try {
            // ── read_piece_alert ──
            if (auto* rp = lt::alert_cast<lt::read_piece_alert>(alert)) {
                out.type = LT_ALERT_READ_PIECE;
                alert_fill_info_hash_from_handle(rp->handle, out.info_hash);
                out.piece_index = static_cast<int>(rp->piece);
                out.error_code = rp->error ? rp->error.value() : 0;
                if (rp->error) {
                    std::string msg = rp->error.message();
                    out.message = strdup(msg.empty() ? "" : msg.c_str());
                } else if (rp->size > 0 && rp->buffer.get()) {
                    out.piece_data_size = rp->size;
                    out.piece_data = static_cast<uint8_t*>(std::malloc(rp->size));
                    if (out.piece_data) {
                        std::memcpy(out.piece_data, rp->buffer.get(), rp->size);
                    }
                }
            }
            // ── session_stats_alert ──
            else if (auto* sa = lt::alert_cast<lt::session_stats_alert>(alert)) {
                out.type = LT_ALERT_SESSION_STATS;
                lt::span<std::int64_t const> counters = sa->counters();
                // `session_stats_metrics()` returns the metric table **by
                // value**.  Binding that temporary straight to a span would
                // leave the span dangling once the full expression ends, and
                // the loop below would then read freed memory — an
                // intermittent SIGSEGV in the alert consumer.  The table is
                // static data, so capture it once.
                static std::vector<lt::stats_metric> const metrics =
                    lt::session_stats_metrics();
                int64_t recv_bytes = -1;
                int64_t sent_bytes = -1;
                for (auto const& m : metrics) {
                    int idx = m.value_index;
                    if (idx < 0 || idx >= static_cast<int>(counters.size())) continue;
                    const char* name = m.name;
                    if (!name) continue;
                    std::string namestr(name);
                    if (namestr == "net.recv_bytes") recv_bytes = counters[idx];
                    else if (namestr == "net.sent_bytes") sent_bytes = counters[idx];
                    else if (namestr == "dht.dht_nodes") out.dht_nodes = static_cast<int32_t>(counters[idx]);
                    else if (namestr == "peer.num_peers_connected") out.peers_connected = static_cast<int32_t>(counters[idx]);
                    else if (namestr == "peer.num_peers_half_open") out.half_open_connections = static_cast<int32_t>(counters[idx]);
                }
                // libtorrent 2.1 publishes cumulative byte counters only —
                // `net.recv_rate` / `net.sent_rate` do not exist, which is why
                // the rate rows used to read 0 forever. The rate is the delta
                // between two samples over the elapsed time.
                //
                // The rate is a one-second rate: a sample posted closer than
                // that to the previous one reuses the last value instead of
                // dividing a near-zero delta by a near-zero interval. Without
                // that floor a second alert carrying the same counters (the
                // engine posts per loop iteration while a read is parked)
                // overwrote a valid rate with 0 in the same batch.
                constexpr double RATE_SAMPLE_SECONDS = 1.0;
                out.total_downloaded = recv_bytes > 0 ? recv_bytes : 0;
                out.total_uploaded = sent_bytes > 0 ? sent_bytes : 0;
                if (recv_bytes >= 0 && sent_bytes >= 0) {
                    auto const now = std::chrono::steady_clock::now();
                    bool const have_baseline =
                        wrapper->stats_last_recv_bytes >= 0 && wrapper->stats_last_sent_bytes >= 0;
                    double const seconds = have_baseline
                        ? std::chrono::duration<double>(now - wrapper->stats_last_sample).count()
                        : 0.0;
                    if (have_baseline && seconds >= RATE_SAMPLE_SECONDS) {
                        int64_t const recv_delta = recv_bytes - wrapper->stats_last_recv_bytes;
                        int64_t const sent_delta = sent_bytes - wrapper->stats_last_sent_bytes;
                        // A negative delta means the counters restarted with a
                        // new session; report 0 rather than a bogus rate.
                        wrapper->stats_last_download_rate =
                            recv_delta >= 0 ? static_cast<int64_t>(recv_delta / seconds) : 0;
                        wrapper->stats_last_upload_rate =
                            sent_delta >= 0 ? static_cast<int64_t>(sent_delta / seconds) : 0;
                        wrapper->stats_last_recv_bytes = recv_bytes;
                        wrapper->stats_last_sent_bytes = sent_bytes;
                        wrapper->stats_last_sample = now;
                    } else if (!have_baseline) {
                        wrapper->stats_last_recv_bytes = recv_bytes;
                        wrapper->stats_last_sent_bytes = sent_bytes;
                        wrapper->stats_last_sample = now;
                    }
                    out.download_rate = wrapper->stats_last_download_rate;
                    out.upload_rate = wrapper->stats_last_upload_rate;
                }
            }
            // ── torrent_finished_alert ──
            else if (auto* tf = lt::alert_cast<lt::torrent_finished_alert>(alert)) {
                out.type = LT_ALERT_TORRENT_FINISHED;
                alert_fill_info_hash_from_handle(tf->handle, out.info_hash);
                out.finished_complete = alert_torrent_finished_is_complete(tf->handle);
            }
            // ── piece_finished_alert ──
            else if (auto* pf = lt::alert_cast<lt::piece_finished_alert>(alert)) {
                out.type = LT_ALERT_PIECE_FINISHED;
                alert_fill_info_hash_from_handle(pf->handle, out.info_hash);
                out.piece_index = static_cast<int>(pf->piece_index);
            }
            // ── torrent_removed_alert ──
            else if (auto* tr = lt::alert_cast<lt::torrent_removed_alert>(alert)) {
                out.type = LT_ALERT_TORRENT_REMOVED;
                std::string hex = alert_info_hash_to_hex(tr->info_hash);
                std::memcpy(out.info_hash, hex.c_str(), hex.size() + 1);
            }
            // ── other alerts ──
            else {
                out.type = LT_ALERT_OTHER;
                out.category = static_cast<int>(static_cast<unsigned int>(alert->category()));
                std::string msg = alert->message();
                out.message = strdup(msg.empty() ? "" : msg.c_str());
            }
        } catch (const std::exception& e) {
            // Any exception during alert processing (e.g. null string
            // construction) is caught per-alert so other alerts are not lost.
            out.type = LT_ALERT_OTHER;
            out.category = 0;
            out.message = strdup(e.what());
        }
    }

    return list;
    LT_FFI_END(nullptr, nullptr)
}

void lt_alert_list_destroy(lt_alert_list_t* list) {
    if (!list) return;
    if (list->alerts) {
        for (int i = 0; i < list->count; i++) {
            auto& a = list->alerts[i];
            if (a.piece_data) std::free(a.piece_data);
            if (a.message) std::free(const_cast<char*>(a.message));
        }
        std::free(list->alerts);
    }
    std::free(list);
}

// ============================================================================
// PieceStorage: per-torrent piece file storage backend
// Stores piece data in cache/pieces/<info_hash>/<info_hash>:piece:N format
// m_base_path already includes "pieces/", so paths are relative to it.
// ============================================================================

namespace {

static bool ensure_dir_recursive(const std::string& path) {
    disk_io_note_call();
    if (path.empty()) return false;
    size_t pos = 0;
    while (pos < path.size()) {
        pos = path.find('/', pos + 1);
        std::string sub = path.substr(0, pos);
        if (!sub.empty()) {
            if (mkdir(sub.c_str(), 0755) != 0 && errno != EEXIST) {
                fprintf(stderr, "[DIAG] ensure_dir_recursive mkdir(%s) failed: %s\n",
                        sub.c_str(), strerror(errno));
                return false;
            }
        }
        if (pos == std::string::npos) break;
    }
    return true;
}

static constexpr int kDiskBlockSize = lt::default_block_size;

// Writes the whole block at the requested offset. pwrite keeps the descriptor
// positioned independently, so two blocks of one piece can never share a seek.
static bool pwrite_all(int fd, const char* buf, int size, int64_t offset) {
    int64_t position = offset;
    int remaining = size;
    while (remaining > 0) {
        ssize_t const written = ::pwrite(
            fd, buf, static_cast<size_t>(remaining), position);
        if (written < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (written == 0) return false;
        buf += written;
        remaining -= static_cast<int>(written);
        position += written;
    }
    return true;
}

// ============================================================================
// CompletionMailbox: hand-off point from disk workers back to the network
// thread.
//
// The disk_interface contract wants completions to run on libtorrent's
// network thread. Posting each one straight into the io_context would put the
// handler — and the torrent reference it may hold — into a queue that can
// outlive this disk object: during session shutdown the io_context destroys
// its pending handlers after the disk object is gone, and releasing such a
// handler then calls back into freed state. Instead the pending completions
// live here, and the io_context only holds one wake-up at a time that carries
// a shared reference to this mailbox and nothing else.
// ============================================================================
class CompletionMailbox : public std::enable_shared_from_this<CompletionMailbox> {
public:
    explicit CompletionMailbox(lt::io_context& ios) : m_ios(ios) {}

    // Test hook: keep queued completions in the mailbox instead of draining
    // them, so the shutdown discard path can be exercised deliberately.
    void set_paused(bool paused) {
        bool should_post = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_paused = paused;
            if (!m_paused && !m_in_flight && !m_queue.empty()) {
                m_in_flight = true;
                should_post = true;
            }
        }
        if (should_post) {
            auto self = shared_from_this();
            boost::asio::post(m_ios, [self] { self->drain(); });
        }
    }

    // Called from a worker thread (or from the network thread for the inline
    // early-return paths). Never blocks, never runs the completion here.
    void enqueue(std::function<void()> done) {
        bool should_post = false;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_closed) {
                // The owner is shutting down and will not run completions any
                // more. Dropping the functor here releases its buffer and
                // torrent references while the owner is still alive.
                return;
            }
            m_queue.push_back(std::move(done));
            if (!m_in_flight) {
                m_in_flight = true;
                should_post = true;
            }
        }
        if (should_post) {
            auto self = shared_from_this();
            boost::asio::post(m_ios, [self] { self->drain(); });
        }
    }

    // Network thread: run the completions queued so far. A handler may submit
    // new disk work, whose completion then re-arms the wake-up.
    void drain() {
        std::deque<std::function<void()>> work;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (m_paused) return;
            m_in_flight = false;
            work.swap(m_queue);
            m_delivered += work.size();
        }
        for (auto& done : work) {
            if (done) done();
        }
    }

    // Test observation: the queue depth at this instant, and how many
    // completions have been drained or discarded so far.
    void stats(uint64_t& pending, uint64_t& discarded, uint64_t& delivered) {
        std::lock_guard<std::mutex> lock(m_mutex);
        pending = static_cast<uint64_t>(m_queue.size());
        discarded = m_discarded;
        delivered = m_delivered;
    }

    // Owner destructor, after its workers have been joined: no completion can
    // arrive any more, so the queued ones are released here, on the owner's
    // still-live state.
    void close() {
        std::deque<std::function<void()>> dropped;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_closed = true;
            dropped.swap(m_queue);
            m_discarded += dropped.size();
        }
        // `dropped` destroys the completions here, outside the lock.
    }

private:
    lt::io_context& m_ios;
    std::mutex m_mutex;
    std::deque<std::function<void()>> m_queue;
    bool m_in_flight = false;
    bool m_closed = false;
    bool m_paused = false;
    uint64_t m_delivered = 0;
    uint64_t m_discarded = 0;
};

// ============================================================================
// DiskWorkerPool: the only executor for piece file I/O.
//
// libtorrent calls disk_interface on its network thread. Every callback here
// hands its arguments to the pool and returns, so that thread never opens,
// reads, writes, fsyncs or unlinks a piece file. Jobs of one torrent run in
// submission order (PieceStorage serializes its own lane), so a delete or a
// hash cannot overtake the block writes queued before it; different torrents
// run in parallel up to the worker count.
//
// The queue is bounded by QUEUE_HIGH_WATER. Crossing it makes async_write
// return libtorrent's "pause this peer" signal and parks the peer's observer
// until the queue drains to QUEUE_LOW_WATER. The network thread is never
// blocked: a full queue throttles peers instead.
// ============================================================================
class DiskWorkerPool {
public:
    // `completion_sink` hands a functor to the disk object's completion
    // mailbox; it is called only from a worker thread.
    explicit DiskWorkerPool(std::function<void(std::function<void()>)> completion_sink)
        : m_completion_sink(std::move(completion_sink))
    {
        size_t const workers = worker_thread_count();
        for (size_t i = 0; i < workers; ++i) {
            m_threads.emplace_back([this] { worker_loop(); });
        }
    }

    ~DiskWorkerPool() { stop_and_join(); }

    // Count one submitted job. Returns true once the overall backlog reaches
    // the high watermark; the caller then reports back-pressure to libtorrent.
    bool track_enqueue(std::shared_ptr<lt::disk_observer> observer) {
        uint64_t const depth = m_pending.fetch_add(1, std::memory_order_relaxed) + 1;
        if (depth < QUEUE_HIGH_WATER) return false;
        std::lock_guard<std::mutex> lock(m_observer_mutex);
        m_exceeded = true;
        if (observer) m_waiting.push_back(std::move(observer));
        return true;
    }

    void track_complete() {
        uint64_t const depth = m_pending.fetch_sub(1, std::memory_order_relaxed) - 1;
        if (depth > QUEUE_LOW_WATER) return;
        std::vector<std::shared_ptr<lt::disk_observer>> release;
        {
            std::lock_guard<std::mutex> lock(m_observer_mutex);
            if (!m_exceeded) return;
            m_exceeded = false;
            release.swap(m_waiting);
        }
        if (release.empty()) return;
        // stop_and_join() clears the sink; nothing can observe the release
        // after that point.
        if (!m_completion_sink) return;
        m_completion_sink([observers = std::move(release)] {
            for (auto& observer : observers) {
                if (observer) observer->on_disk();
            }
        });
    }

    void dispatch(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            if (!m_stopping) {
                m_tasks.push_back(std::move(task));
                m_cv.notify_one();
                return;
            }
        }
        // Shutdown: the pool no longer accepts work. Drop the job instead of
        // running it on the calling thread — the caller is libtorrent's
        // network thread, and a disk callback must never perform file I/O
        // there. The storage's destructor closes any descriptor the dropped
        // job would have closed.
        m_pending.fetch_sub(1, std::memory_order_relaxed);
    }

    void stop_and_join() {
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            m_stopping = true;
        }
        m_cv.notify_all();
        for (auto& thread : m_threads) {
            if (thread.joinable()) thread.join();
        }
        m_threads.clear();
        m_completion_sink = nullptr;
    }

private:
    static size_t worker_thread_count() {
        unsigned const ncpu = std::thread::hardware_concurrency();
        if (ncpu < 2) return 2;
        if (ncpu > 4) return 4;
        return ncpu;
    }

    void worker_loop() {
        for (;;) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(m_mutex);
                m_cv.wait(lock, [this] { return m_stopping || !m_tasks.empty(); });
                if (m_tasks.empty()) {
                    if (m_stopping) return;
                    continue;
                }
                task = std::move(m_tasks.front());
                m_tasks.pop_front();
            }
            try {
                task();
            } catch (std::exception const& e) {
                fprintf(stderr, "[wrapper] disk worker exception: %s\n", e.what());
            } catch (...) {
                fprintf(stderr, "[wrapper] disk worker unknown exception\n");
            }
        }
    }

    static constexpr uint64_t QUEUE_HIGH_WATER = 256;
    static constexpr uint64_t QUEUE_LOW_WATER = 64;

    std::mutex m_mutex;
    std::condition_variable m_cv;
    std::deque<std::function<void()>> m_tasks;
    std::vector<std::thread> m_threads;
    bool m_stopping = false;

    std::atomic<uint64_t> m_pending{0};
    std::function<void(std::function<void()>)> m_completion_sink;
    std::mutex m_observer_mutex;
    bool m_exceeded = false;
    std::vector<std::shared_ptr<lt::disk_observer>> m_waiting;
};

class PieceStorageDiskIO;

// Process-wide lookup of the custom disk object by cache directory. The test
// hooks at the end of this file need to reach the mailbox while the owning
// session is alive.
static std::mutex g_disk_io_registry_mutex;
static std::map<std::string, PieceStorageDiskIO*> g_disk_io_registry;

// One serial lane per info hash, shared across every PieceStorage built for
// that hash during the process lifetime. A torrent can be removed and re-added
// while the old storage still has queued work (libtorrent drops it from the
// session before the asynchronous delete completes), so ordering has to span
// storage lifetimes: an old async delete must run before the new storage's
// directory creation and block writes, never after them.
struct TorrentLane {
    std::mutex mutex;
    std::deque<std::function<void()>> jobs;
    bool draining = false;
};

class PieceStorage : public std::enable_shared_from_this<PieceStorage> {
public:
    PieceStorage(const std::string& base_path, const std::string& info_hash_hex,
        lt::file_storage const* files, lt::io_context& ios,
        std::shared_ptr<DiskWorkerPool> pool,
        std::shared_ptr<TorrentLane> lane, bool v1, bool v2)
        : m_base_path(base_path), m_info_hash_hex(info_hash_hex), m_files(files),
          m_ios(ios), m_pool(std::move(pool)), m_lane(std::move(lane)),
          m_v1(v1), m_v2(v2)
    {
        // The per-torrent directory is created on this torrent's lane. Doing
        // it here would run mkdir on libtorrent's network thread, which the
        // disk callbacks must not do.
    }

    ~PieceStorage() { close_write_handles(); }

    // Queue one disk job on the serial lane for this info hash. Returns true
    // when the pool's overall backlog is over its high watermark, which the
    // caller turns into libtorrent's per-peer back-pressure signal. The job
    // owns this storage until it runs: the lane is shared across storage
    // lifetimes, so it cannot hold the storage that happened to dispatch it.
    bool submit(std::function<void()> job,
        std::shared_ptr<lt::disk_observer> observer = nullptr) {
        auto self = shared_from_this();
        bool should_dispatch = false;
        {
            std::lock_guard<std::mutex> lock(m_lane->mutex);
            m_lane->jobs.push_back(
                [job = std::move(job), self]() mutable { job(); });
            if (!m_lane->draining) {
                m_lane->draining = true;
                should_dispatch = true;
            }
        }
        bool const over_water = m_pool->track_enqueue(std::move(observer));
        if (should_dispatch) {
            auto lane = m_lane;
            auto pool = m_pool;
            m_pool->dispatch([lane, pool] { drain_lane(lane, pool); });
        }
        return over_water;
    }

    static void drain_lane(std::shared_ptr<TorrentLane> const& lane,
        std::shared_ptr<DiskWorkerPool> const& pool) {
        for (;;) {
            std::function<void()> job;
            {
                std::lock_guard<std::mutex> lock(lane->mutex);
                if (lane->jobs.empty()) {
                    lane->draining = false;
                    return;
                }
                job = std::move(lane->jobs.front());
                lane->jobs.pop_front();
            }
            // One failing job must not strand the rest of this torrent's lane.
            try {
                job();
            } catch (std::exception const& e) {
                fprintf(stderr, "[wrapper] disk job exception: %s\n", e.what());
            } catch (...) {
                fprintf(stderr, "[wrapper] disk job unknown exception\n");
            }
            pool->track_complete();
        }
    }

    // v2 piece length in bytes, i.e. excluding the v1 tail padding (BEP 52).
    // The torrent's file layout owns the value; the storage only mirrors it.
    int piece_size2(int piece_index) const {
        if (!m_files || piece_index < 0
            || lt::piece_index_t(piece_index) >= m_files->end_piece()) {
            return -1;
        }
        return m_files->piece_size2(lt::piece_index_t(piece_index));
    }

    std::string get_info_hash_hex() const { return m_info_hash_hex; }

    std::string pieces_dir() const {
        return m_base_path + "/" + m_info_hash_hex;
    }

    // Create the per-torrent directory. Called from this torrent's lane when
    // the torrent is added: the directory exists just as it did before the
    // write path moved off libtorrent's network thread, but the mkdir itself
    // never runs on that thread.
    void ensure_pieces_dir() {
        if (!ensure_dir_recursive(pieces_dir())) {
            fprintf(stderr, "[DIAG] PieceStorage: failed to create directory %s\n",
                    pieces_dir().c_str());
        }
    }

    std::string piece_path(int piece_index) const {
        return pieces_dir() + "/" + m_info_hash_hex + ":piece:" + std::to_string(piece_index);
    }

    // Per-piece in-progress marker. It is written once the piece's blocks have
    // all reached disk and the data has been fsynced, and is cleared by the
    // Rust cache layer once the piece is registered as complete.
    std::string piece_marker_path(int piece_index) const {
        return piece_path(piece_index) + ".incomplete";
    }

    // Blocks are staged here until the piece is complete. The name is not a
    // piece file for either restart scan, so a crash mid-piece leaves bytes
    // that can never be mistaken for a complete piece. It is kept separate
    // from `.incomplete`: the Rust cache creates, truncates and removes that
    // marker on its own schedule.
    std::string piece_staging_path(int piece_index) const {
        return piece_path(piece_index) + ".part";
    }

    bool read_piece(int piece_index, int offset, char* buf, int size) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        int const fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) return false;
        char* cursor = buf;
        int remaining = size;
        int64_t position = offset;
        while (remaining > 0) {
            ssize_t const got = ::pread(
                fd, cursor, static_cast<size_t>(remaining), position);
            if (got < 0) {
                if (errno == EINTR) continue;
                ::close(fd);
                return false;
            }
            if (got == 0) break;
            cursor += got;
            remaining -= static_cast<int>(got);
            position += got;
        }
        ::close(fd);
        bool const ok = remaining == 0;
        TORRENTFS_DIAG("[DIAG] read_piece piece=%d offset=%d size=%d ok=%d first4=%02x%02x%02x%02x\n",
                piece_index, offset, size, ok,
                (unsigned char)(size>0?buf[0]:0), (unsigned char)(size>1?buf[1]:0),
                (unsigned char)(size>2?buf[2]:0), (unsigned char)(size>3?buf[3]:0));
        return ok;
    }

    // One 16 KiB block of a piece. Blocks are written to the piece's staging
    // path, never to the published piece path: a crash can leave a full-length
    // sparse staging file, but the restart scan only accepts published piece
    // files, so a half-written piece cannot be mistaken for a complete one
    // (notably with hash checks disabled). The staging file stays open until
    // every block has arrived; the last block fsyncs it once and publishes it
    // under the final name, then writes the empty marker the Rust cache clears
    // on registration. That keeps the documented write/flush/fsync/mark order
    // at piece granularity instead of once per block.
    bool write_block(int piece_index, int offset, const char* buf, int size) {
        disk_io_note_call();
        g_disk_write_blocks.fetch_add(1, std::memory_order_relaxed);
        int64_t const extent = expected_write_extent(piece_index);
        if (extent <= 0 || offset < 0 || size <= 0
            || static_cast<int64_t>(offset) + size > extent) {
            return false;
        }

        auto state_it = m_write_states.find(piece_index);
        if (state_it == m_write_states.end()) {
            if (!ensure_dir_recursive(pieces_dir())) {
                fprintf(stderr, "[DIAG] write_block: ensure_dir_recursive failed for %s\n",
                        pieces_dir().c_str());
                return false;
            }
            int const fd = ::open(piece_staging_path(piece_index).c_str(),
                O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
            if (fd < 0) return false;
            PieceWriteState state;
            state.fd = fd;
            // Only blocks that carry payload are requested. A hybrid piece
            // may also receive a final block that extends into the padding;
            // that block still ends the piece.
            int64_t const payload = expected_payload_size(piece_index);
            if (payload <= 0) {
                ::close(fd);
                return false;
            }
            size_t const block_count = static_cast<size_t>(
                (payload + kDiskBlockSize - 1) / kDiskBlockSize);
            state.blocks_seen.assign(block_count, 0);
            state_it = m_write_states.emplace(piece_index, std::move(state)).first;
        }

        PieceWriteState& state = state_it->second;
        {
            // Shared with the Rust read path: a reader must not observe a
            // half-written piece file (the lock is per info hash, not per
            // piece, exactly as before).
            auto piece_lock = get_piece_lock(m_info_hash_hex);
            std::unique_lock<std::shared_mutex> write_lock(*piece_lock);
            if (!pwrite_all(state.fd, buf, size, offset)) {
                ::close(state.fd);
                m_write_states.erase(state_it);
                ::unlink(piece_staging_path(piece_index).c_str());
                return false;
            }
        }

        TORRENTFS_DIAG("[DIAG] write_block piece=%d offset=%d size=%d\n",
                piece_index, offset, size);

        size_t const block = static_cast<size_t>(offset) / kDiskBlockSize;
        if (block < state.blocks_seen.size() && !state.blocks_seen[block]) {
            state.blocks_seen[block] = 1;
            ++state.blocks_seen_count;
        }
        if (state.blocks_seen_count == state.blocks_seen.size()) {
            int const fd = state.fd;
            m_write_states.erase(state_it);
            return finish_piece(piece_index, fd);
        }
        return true;
    }

    // A hash job may run on a piece that is still being written. A still-open
    // descriptor therefore means the piece is incomplete: close it without
    // publishing, so the hash sees no final file (a mismatch) and the staged
    // bytes stay invisible to both the restart scan and the published path.
    void flush_open_piece(int piece_index) {
        auto it = m_write_states.find(piece_index);
        if (it == m_write_states.end()) return;
        ::close(it->second.fd);
        m_write_states.erase(it);
    }

    void clear_piece(int piece_index) {
        auto it = m_write_states.find(piece_index);
        if (it == m_write_states.end()) return;
        ::close(it->second.fd);
        m_write_states.erase(it);
        // Drop the partial staging bytes; the marker is left alone because a
        // published piece that libtorrent is clearing must stay tagged
        // incomplete until a later successful publish replaces it.
        ::unlink(piece_staging_path(piece_index).c_str());
    }

    void close_write_handles() {
        for (auto& entry : m_write_states) {
            if (entry.second.fd >= 0) ::close(entry.second.fd);
        }
        m_write_states.clear();
    }

    bool has_piece(int piece_index) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        return file.is_open();
    }

    // Size libtorrent will write for this piece, from the torrent layout. The
    // on-disk file is empty until the first block arrives, so the write path
    // needs the expected size rather than a stat.
    int64_t expected_piece_size(int piece_index) const {
        if (!m_files || piece_index < 0
            || lt::piece_index_t(piece_index) >= m_files->end_piece()) {
            return -1;
        }
        return m_files->piece_size(lt::piece_index_t(piece_index));
    }

    // Bytes libtorrent may write for this piece. v1 metadata (a v1-only or
    // hybrid torrent) describes the full padded piece, so a final block may
    // run into the padding. A v2-only piece is requested only up to its
    // payload.
    int64_t expected_write_extent(int piece_index) const {
        return m_v1 ? expected_piece_size(piece_index)
                    : expected_payload_size(piece_index);
    }

    // Payload bytes of this piece. The piece is complete once every block
    // covering these bytes has arrived. Padding is not v2-specific: a v1
    // torrent may align files with explicit pad files, and libtorrent
    // pre-marks blocks made only of padding as finished without requesting
    // them. Counting the whole layout would strand such a piece forever.
    int64_t expected_payload_size(int piece_index) const {
        if (!m_files || piece_index < 0
            || lt::piece_index_t(piece_index) >= m_files->end_piece()) {
            return -1;
        }
        int64_t const piece_start
            = static_cast<int64_t>(piece_index) * m_files->piece_length();
        int64_t const piece_end
            = piece_start + m_files->piece_size(lt::piece_index_t(piece_index));
        int64_t payload_end = piece_start;
        lt::file_index_t const first = m_files->file_index_at_offset(piece_start);
        for (lt::file_index_t f = first; f < m_files->end_file(); ++f) {
            int64_t const offset = m_files->file_offset(f);
            if (offset >= piece_end) break;
            if (m_files->pad_file_at(f)) continue;
            int64_t const end
                = std::min<int64_t>(offset + m_files->file_size(f), piece_end);
            if (end > payload_end) payload_end = end;
        }
        return payload_end - piece_start;
    }

    // On-disk size; -1 when the file is missing. The hash paths use this to
    // distinguish an absent piece from a present one.
    int64_t piece_size(int piece_index) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return -1;
        return static_cast<int64_t>(file.tellg());
    }

    lt::sha1_hash hash_piece(int piece_index, int piece_size) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return lt::sha1_hash();

        lt::hasher h;
        std::vector<char> buf(16 * 1024);
        int remaining = piece_size;
        while (remaining > 0) {
            int to_read = std::min(remaining, static_cast<int>(buf.size()));
            file.read(buf.data(), to_read);
            int actual = static_cast<int>(file.gcount());
            if (actual == 0) break;
            h.update(buf.data(), actual);
            remaining -= actual;
        }
        return h.final();
    }

    // SHA-256 of one v2 block: `size` bytes read from `offset` inside the
    // piece file. A short read (piece truncated on disk) yields the zero hash,
    // which never matches the torrent's expected block hash.
    lt::sha256_hash hash_block_sha256(int piece_index, int offset, int size) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open() || offset < 0 || size <= 0) return lt::sha256_hash();
        file.seekg(offset);
        std::vector<char> buf(static_cast<size_t>(size));
        file.read(buf.data(), size);
        if (file.gcount() != size) return lt::sha256_hash();

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        SHA256_Update(&ctx, buf.data(), buf.size());
        lt::sha256_hash result;
        SHA256_Final(reinterpret_cast<unsigned char*>(result.data()), &ctx);
        return result;
    }

    // Read the full raw piece data into a vector. Returns empty vector if the
    // piece file does not exist or is empty. Needed so that async_hash can
    // compute SHA256 sub-range hashes for v2 blocks in hybrid torrents.
    std::vector<char> read_piece_data(int piece_index) {
        disk_io_note_call();
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return {};
        std::streamsize sz = file.tellg();
        if (sz <= 0) return {};
        file.seekg(0);
        std::vector<char> data(sz);
        file.read(data.data(), sz);
        if (!file) return {};
        return data;
    }

    // Drop the torrent's piece directory: every piece file and `.incomplete`
    // marker first, then the directory itself. A plain rmdir() on a directory
    // that still holds piece files always failed, so the cache of a deleted
    // torrent was never reclaimed.
    void delete_piece_files() {
        disk_io_note_call();
        close_write_handles();
        auto piece_lock = get_piece_lock(m_info_hash_hex);
        std::unique_lock<std::shared_mutex> write_lock(*piece_lock);
        std::string dir = m_base_path + "/" + m_info_hash_hex;
        DIR* dp = opendir(dir.c_str());
        if (!dp) return;
        while (struct dirent* entry = readdir(dp)) {
            std::string name(entry->d_name);
            if (name == "." || name == "..") continue;
            ::unlink((dir + "/" + name).c_str());
        }
        closedir(dp);
        rmdir(dir.c_str());
    }

private:
    struct PieceWriteState {
        int fd = -1;
        std::vector<uint8_t> blocks_seen;
        size_t blocks_seen_count = 0;
    };

    // Flush the staged piece, publish it under its final name, then write the
    // in-progress marker. The fsync is the only one this piece gets: blocks
    // accumulate in the open staging descriptor until the last one arrives.
    // Publishing is a rename, so the final path never exists before every
    // block has landed. A failed fsync or rename returns false so the last
    // block's completion carries a storage error: the piece is not published,
    // and libtorrent must not record it as written.
    bool finish_piece(int piece_index, int fd) {
        // A v1 piece (including a hybrid torrent and a v1 torrent with
        // explicit alignment pads) is checked against a SHA-1 that covers the
        // padded piece, so fill the pad tail before the one fsync. v2-only
        // pieces are hashed and read only up to their payload and stay
        // payload-sized.
        int64_t const payload = expected_payload_size(piece_index);
        if (m_v1 && payload >= 0) {
            int64_t const full = expected_piece_size(piece_index);
            if (full <= 0 || (full > payload
                    && ::ftruncate(fd, static_cast<off_t>(full)) != 0)) {
                TORRENTFS_DIAG("[DIAG] pad failed for piece %d: %s\n",
                        piece_index, strerror(errno));
                ::close(fd);
                ::unlink(piece_staging_path(piece_index).c_str());
                g_piece_publish_failures.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
        }
        std::string const staging = piece_staging_path(piece_index);
        if (::fsync(fd) != 0) {
            TORRENTFS_DIAG("[DIAG] fsync failed for piece %d: %s\n",
                    piece_index, strerror(errno));
            ::close(fd);
            ::unlink(staging.c_str());
            g_piece_publish_failures.fetch_add(1, std::memory_order_relaxed);
            return false;
        }
        g_disk_piece_fsyncs.fetch_add(1, std::memory_order_relaxed);
        ::close(fd);

        if (::rename(staging.c_str(), piece_path(piece_index).c_str()) != 0) {
            fprintf(stderr, "[DIAG] publish rename failed for %s: %s\n",
                    staging.c_str(), strerror(errno));
            ::unlink(staging.c_str());
            g_piece_publish_failures.fetch_add(1, std::memory_order_relaxed);
            return false;
        }

        // The marker is deliberately not fsynced: one fsync per piece is the
        // durability point for the data, and the marker only has to be
        // visible to the restart scan that runs after the process exits
        // cleanly enough for the page cache to have it.
        std::string const marker_path = piece_marker_path(piece_index);
        int const marker = ::open(marker_path.c_str(),
            O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (marker < 0) {
            fprintf(stderr, "[DIAG] create marker failed for %s: %s\n",
                    marker_path.c_str(), strerror(errno));
            // The piece itself is published and durable; only the marker is
            // missing, which a restart scan reads as "complete" — correctly.
            return true;
        }
        g_disk_piece_markers.fetch_add(1, std::memory_order_relaxed);
        ::close(marker);
        return true;
    }

    std::string m_base_path;
    std::string m_info_hash_hex;
    // Torrent layout, owned by the torrent_info of the torrent this storage
    // belongs to. Only read for v2 piece geometry.
    lt::file_storage const* m_files;
    lt::io_context& m_ios;
    std::shared_ptr<DiskWorkerPool> m_pool;
    // Shared with every other storage of the same info hash; see TorrentLane.
    std::shared_ptr<TorrentLane> m_lane;
    // Protocol layout flags. v1 requests cover the padded piece; a v2 layout
    // pads each file to a piece boundary and only requests the payload.
    bool m_v1;
    bool m_v2;
    // Open descriptors for pieces whose blocks are still arriving. Guarded by
    // the lane: every disk job for this torrent runs on one worker at a time.
    std::map<int, PieceWriteState> m_write_states;
};

// ============================================================================
// PieceStorageDiskIO: implements disk_interface for piece-level storage
// ============================================================================

class PieceStorageDiskIO : public lt::disk_interface, public lt::buffer_allocator_interface {
public:
    PieceStorageDiskIO(lt::io_context& ios, const std::string& piece_cache_dir,
        bool disable_hash_checks)
        : m_ios(ios), m_piece_cache_dir(piece_cache_dir),
          m_disable_hash_checks(disable_hash_checks)
    {
        if (piece_cache_dir.empty()) {
            throw std::runtime_error("PieceStorageDiskIO: piece_cache_dir is empty");
        }
        if (!ensure_dir_recursive(piece_cache_dir)) {
            fprintf(stderr, "[DIAG] PieceStorageDiskIO: failed to create cache dir %s\n",
                    piece_cache_dir.c_str());
            throw std::runtime_error("Failed to create piece cache directory: " + piece_cache_dir);
        }
        auto mailbox = m_mailbox;
        m_pool = std::make_shared<DiskWorkerPool>(
            [mailbox](std::function<void()> done) {
                mailbox->enqueue(std::move(done));
            });
        std::lock_guard<std::mutex> lock(g_disk_io_registry_mutex);
        g_disk_io_registry[m_piece_cache_dir] = this;
    }

    ~PieceStorageDiskIO() override {
        {
            std::lock_guard<std::mutex> lock(g_disk_io_registry_mutex);
            auto it = g_disk_io_registry.find(m_piece_cache_dir);
            if (it != g_disk_io_registry.end() && it->second == this) {
                g_disk_io_registry.erase(it);
            }
        }
        // Order matters. Joining the workers first means no new completion can
        // be produced; closing the mailbox then releases the ones still
        // queued while this object is still alive. The io_context may later
        // destroy its single pending wake-up, but that one carries only the
        // mailbox, never a torrent.
        m_destroying.store(true, std::memory_order_release);
        if (m_pool) m_pool->stop_and_join();
        if (m_mailbox) m_mailbox->close();
        // A job that was enqueued while the pool was already stopping has no
        // drainer left. It owns its storage through the wrapper `submit`
        // installs, so drop those jobs here to break the lane -> job ->
        // storage -> lane cycle and let the storage close its descriptors.
        std::vector<std::shared_ptr<TorrentLane>> lanes;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            for (auto& entry : m_lanes) {
                if (auto lane = entry.second.lock()) lanes.push_back(lane);
            }
            m_lanes.clear();
        }
        for (auto& lane : lanes) {
            std::lock_guard<std::mutex> lock(lane->mutex);
            lane->jobs.clear();
            lane->draining = false;
        }
        m_storages.clear();
        // Safety net: a read buffer that no completion owned any more would
        // leak for the process lifetime otherwise. Dropped completions have
        // already released theirs, so this normally frees nothing. This is
        // not the completion-discard path and must not be counted as one.
        std::vector<char*> leftovers;
        {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            leftovers.assign(m_allocated.begin(), m_allocated.end());
        }
        for (char* buf : leftovers) {
            release_read_buffer(buf, false);
        }
    }

    // buffer_allocator_interface
    void free_disk_buffer(char* b) override {
        // A second free for the same pointer is ignored: m_allocated is the
        // single ownership record, so release happens exactly once.
        release_read_buffer(b, false);
    }
#if LIBTORRENT_VERSION_NUM >= 20100
    void free_multiple_buffers(lt::span<char*> bufs) override {
        for (auto* b : bufs) {
            release_read_buffer(b, false);
        }
    }
#endif

    // Test hooks used by the read-buffer teardown regression: park the
    // completions the workers produced, then discard them the way shutdown
    // does, while this object and its allocator are still alive.
    void test_pause_completion_mailbox(bool paused) {
        if (m_mailbox) m_mailbox->set_paused(paused);
    }

    void test_close_completion_mailbox() {
        if (m_mailbox) m_mailbox->close();
    }

    void test_completion_mailbox_stats(uint64_t* pending, uint64_t* discarded,
        uint64_t* delivered) {
        if (!m_mailbox) return;
        m_mailbox->stats(*pending, *discarded, *delivered);
    }

    // disk_interface: new_torrent
    lt::storage_holder new_torrent(lt::storage_params const& p,
        std::shared_ptr<void> const& /*torrent*/) override
    {
        std::shared_ptr<PieceStorage> storage;
        lt::storage_index_t idx;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            std::string info_hash_hex = sha1_to_hex(p.info_hash);
            TORRENTFS_DIAG("[DIAG] new_torrent info_hash=%s\n",
                    info_hash_hex.c_str());
            for (auto it = m_lanes.begin(); it != m_lanes.end();) {
                if (it->second.expired()) {
                    it = m_lanes.erase(it);
                } else {
                    ++it;
                }
            }
            // Reuse the lane of an earlier storage for the same hash: its
            // queued delete must run before this storage creates or writes
            // anything, even though the old torrent already left the session.
            auto& lane_slot = m_lanes[info_hash_hex];
            std::shared_ptr<TorrentLane> lane = lane_slot.lock();
            if (!lane) {
                lane = std::make_shared<TorrentLane>();
                lane_slot = lane;
            }
            // The layout (and with it the v2 piece geometry) is owned by the
            // torrent's torrent_info, which outlives this storage.
            storage = std::make_shared<PieceStorage>(
                m_piece_cache_dir, info_hash_hex, &p.files, m_ios, m_pool,
                std::move(lane), p.v1, p.v2);
            idx = m_next_index;
            ++m_next_index;
            m_storages[idx] = storage;
        }
        // Claiming the directory is file I/O, so it runs on this torrent's
        // serial lane instead of the network thread that called us. On a
        // re-add the lane still holds the old storage's delete, so mkdir is
        // ordered after it and the recreated directory is not wiped.
        PieceStorage* raw = storage.get();
        storage->submit([raw] { raw->ensure_pieces_dir(); });
        return lt::storage_holder(idx, *this);
    }

    // disk_interface: remove_torrent
    void remove_torrent(lt::storage_index_t idx) override {
        // Releasing a queued completion inside our own destructor can drop the
        // last torrent reference, whose storage_holder then calls back in
        // here. The storages are already being torn down; nothing to do.
        if (m_destroying.load(std::memory_order_acquire)) return;
        std::shared_ptr<PieceStorage> storage;
        {
            std::lock_guard<std::mutex> lock(m_mutex);
            auto it = m_storages.find(idx);
            if (it == m_storages.end()) return;
            storage = std::move(it->second);
            m_storages.erase(it);
        }
        // Close any still-open piece descriptor on the pool rather than on
        // this (network) thread; the queued job owns the storage until it
        // runs.
        PieceStorage* raw = storage.get();
        storage->submit([raw] { raw->close_write_handles(); });
        // TSI-2262: the per-info-hash shared_mutex is intentionally NOT
        // erased from g_piece_locks here. A concurrent lt_unlock_piece_read
        // may still hold a shared_ptr to it; erasing would invalidate the
        // map entry's shared_ptr, but since get_piece_lock returns a copy,
        // the mutex stays alive until the last reference drops. Leaving
        // the entry avoids the erase-during-use UB entirely.
    }

    // Hand a completion to the mailbox instead of posting it into the
    // io_context directly: the handler then never outlives this disk object,
    // even when the session is torn down with completions still queued.
    void enqueue_completion(std::function<void()> done) {
        if (m_mailbox) {
            m_mailbox->enqueue(std::move(done));
            return;
        }
        // No mailbox (construction failure path): running it here keeps the
        // disk_interface contract, and no worker exists to post from.
        if (done) done();
    }

    // Ownership of an async_read buffer until the completion either hands it
    // to libtorrent's disk_buffer_holder or is destroyed. Holding it in a
    // shared_ptr means a completion dropped by CompletionMailbox::close()
    // still releases the buffer here, exactly once.
    struct ReadBufferLease {
        PieceStorageDiskIO* owner = nullptr;
        char* data = nullptr;

        ~ReadBufferLease() {
            if (data && owner) owner->release_read_buffer(data, true);
        }

        char* release() {
            char* const out = data;
            data = nullptr;
            return out;
        }
    };

    // Free a read buffer owned by this disk object. `dropped` distinguishes a
    // buffer released because its completion was discarded from one delivered
    // through disk_buffer_holder. Membership in m_allocated makes the free
    // exactly-once even if both paths race.
    void release_read_buffer(char* buf, bool dropped) {
        if (!buf) return;
        bool freed = false;
        {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            auto it = m_allocated.find(buf);
            if (it != m_allocated.end()) {
                m_allocated.erase(it);
                freed = true;
            }
        }
        if (!freed) return;
        std::free(buf);
        g_read_buffers_released.fetch_add(1, std::memory_order_relaxed);
        g_read_buffers_live.fetch_sub(1, std::memory_order_relaxed);
        if (dropped) {
            g_read_buffers_dropped.fetch_add(1, std::memory_order_relaxed);
        }
    }

    // disk_interface: async_read
    void async_read(lt::storage_index_t storage, lt::peer_request const& r,
        std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        DiskCallbackScope callback_scope;
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            handler(lt::disk_buffer_holder(),
                lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            return;
        }

        char* buf = static_cast<char*>(std::malloc(r.length));
        if (!buf) {
            handler(lt::disk_buffer_holder(),
                lt::storage_error(lt::error_code(boost::system::errc::not_enough_memory, boost::system::generic_category())));
            return;
        }
        {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            m_allocated.insert(buf);
        }
        g_read_buffers_allocated.fetch_add(1, std::memory_order_relaxed);
        g_read_buffers_live.fetch_add(1, std::memory_order_relaxed);
        auto lease = std::make_shared<ReadBufferLease>();
        lease->owner = this;
        lease->data = buf;

        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, r, lease, handler = std::move(handler)]() mutable {
            if (ps->read_piece(static_cast<int>(r.piece), r.start, lease->data, r.length)) {
#if LIBTORRENT_VERSION_NUM >= 20100
                enqueue_completion([h = std::move(handler), this, lease]() mutable {
                    h(lt::disk_buffer_holder(*this, lease->release()),
                        lt::storage_error());
                });
#else
                enqueue_completion([h = std::move(handler), this, lease, len = r.length]() mutable {
                    h(lt::disk_buffer_holder(*this, lease->release(), len),
                        lt::storage_error());
                });
#endif
            } else {
                release_read_buffer(lease->release(), false);
                enqueue_completion([h = std::move(handler)] {
                    h(lt::disk_buffer_holder(),
                        lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
                });
            }
        });
    }

    // disk_interface: async_write
    bool async_write(lt::storage_index_t storage, lt::peer_request const& r,
        char const* buf, std::shared_ptr<lt::disk_observer> o,
        std::function<void(lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        DiskCallbackScope callback_scope;
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            handler(lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            return false;
        }

        // libtorrent's receive buffer is reused as soon as this call returns,
        // so the block must be copied here. The copy is the whole cost on the
        // network thread; opening, writing and fsyncing happen on the pool.
        std::shared_ptr<char> block(
            static_cast<char*>(std::malloc(static_cast<size_t>(r.length))),
            [](char* p) { std::free(p); });
        if (!block) {
            handler(lt::storage_error(lt::error_code(
                boost::system::errc::not_enough_memory, boost::system::generic_category())));
            return false;
        }
        std::memcpy(block.get(), buf, static_cast<size_t>(r.length));

        PieceStorage* ps = storage_ptr.get();
        bool const over_water = storage_ptr->submit(
            [this, ps, r, block = std::move(block), handler = std::move(handler)]() mutable {
                bool const ok = ps->write_block(static_cast<int>(r.piece), r.start,
                    block.get(), r.length);
                TORRENTFS_DIAG("[DIAG] async_write piece=%d offset=%d len=%d => %s\n",
                        static_cast<int>(r.piece), r.start, r.length, ok ? "OK" : "FAIL");
                lt::storage_error error;
                if (!ok) {
                    g_write_completion_errors.fetch_add(1, std::memory_order_relaxed);
                    error = lt::storage_error(lt::error_code(
                        boost::system::errc::io_error, boost::system::generic_category()));
                }
                enqueue_completion([h = std::move(handler), error]() mutable {
                    h(error);
                    // The handler returned, so libtorrent has observed the
                    // error. Distinct from an error that was only constructed.
                    if (error) {
                        g_write_completion_errors_delivered.fetch_add(
                            1, std::memory_order_relaxed);
                    }
                });
            },
            std::move(o));
        return over_water;
    }

    // disk_interface: async_hash
    void async_hash(lt::storage_index_t storage, lt::piece_index_t piece,
        lt::span<lt::sha256_hash> v2,
        lt::disk_job_flags_t flags,
        std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler) override
    {
        DiskCallbackScope callback_scope;
        (void)flags;
        TORRENTFS_DIAG("[DIAG] async_hash CALLED piece=%d v2_size=%zu\n",
                static_cast<int>(piece), v2.size());
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d => no storage\n", static_cast<int>(piece));
            // Zero out v2 hashes for hybrid torrents — no data available
            for (auto& h : v2) h = lt::sha256_hash();
            enqueue_completion([h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, piece, v2, handler = std::move(handler)]() mutable {
            int const piece_idx = static_cast<int>(piece);
            // A piece that finished writing may still own an open descriptor
            // when the hash job arrives first; flush it once so the hash reads
            // durable bytes.
            ps->flush_open_piece(piece_idx);

            int64_t const sz = ps->piece_size(piece_idx);
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d file_size=%ld\n", piece_idx, (long)sz);
            if (sz <= 0) {
                TORRENTFS_DIAG("[DIAG] async_hash piece=%d => file_size<=0, empty hash\n", piece_idx);
                // A missing piece is a mismatch to this storage, not a file
                // error: `no_such_file_or_directory` here makes libtorrent
                // skip the rest of the torrent's pieces.
                for (auto& h : v2) h = lt::sha256_hash();
                enqueue_completion([h = std::move(handler), p = piece] {
                    h(p, lt::sha1_hash(), lt::storage_error());
                });
                return;
            }

            // Read full piece data so we can compute v2 sub-range hashes
            auto data = ps->read_piece_data(piece_idx);
            if (data.empty()) {
                TORRENTFS_DIAG("[DIAG] async_hash piece=%d => read_piece_data empty\n", piece_idx);
                for (auto& h : v2) h = lt::sha256_hash();
                enqueue_completion([h = std::move(handler), p = piece] {
                    h(p, lt::sha1_hash(), lt::storage_error());
                });
                return;
            }

            // Compute SHA1 over the full piece
            lt::hasher hasher;
            hasher.update(data.data(), static_cast<int>(data.size()));
            lt::sha1_hash hash = hasher.final();

            // Compute SHA256 for each v2 block in hybrid torrents.
            // BEP 52 hashes the piece in fixed 16 KiB blocks; only the final
            // block is short, and the range stops at `piece_size2` (the v2
            // piece length, which excludes v1 tail padding). Slicing the piece
            // evenly folded padding bytes into the last block, so every padded
            // piece failed v2 verification.
            if (!v2.empty()) {
                int const piece_size2 = ps->piece_size2(piece_idx);
                if (piece_size2 < 0 || static_cast<size_t>(piece_size2) > data.size()) {
                    TORRENTFS_DIAG("[DIAG] async_hash piece=%d v2_blocks=%zu piece_size2=%d => zero hashes\n",
                            piece_idx, v2.size(), piece_size2);
                    for (auto& h : v2) h = lt::sha256_hash();
                } else {
                    size_t const blocks = v2.size();
                    size_t const block_size = static_cast<size_t>(lt::default_block_size);
                    size_t const v2_size = static_cast<size_t>(piece_size2);
                    for (size_t j = 0; j < blocks; ++j) {
                        size_t const start = j * block_size;
                        if (start >= v2_size) {
                            v2[j] = lt::sha256_hash();
                            continue;
                        }
                        size_t const len = std::min(block_size, v2_size - start);
                        SHA256_CTX ctx;
                        SHA256_Init(&ctx);
                        SHA256_Update(&ctx, data.data() + start, len);
                        SHA256_Final(reinterpret_cast<unsigned char*>(v2[j].data()), &ctx);
                    }
                    TORRENTFS_DIAG("[DIAG] async_hash piece=%d v2_blocks=%zu piece_size2=%zu => populated\n",
                            piece_idx, blocks, v2_size);
                }
            }

            TORRENTFS_DIAG("[DIAG] async_hash piece=%d size=%d hash=%02x%02x%02x%02x... => posted\n",
                    piece_idx, static_cast<int>(data.size()),
                    (unsigned char)hash[0], (unsigned char)hash[1],
                    (unsigned char)hash[2], (unsigned char)hash[3]);
            enqueue_completion([h = std::move(handler), p = piece, hash] {
                h(p, hash, lt::storage_error());
            });
        });
    }

    // disk_interface: async_hash2 — SHA-256 of the single v2 block at `offset`
    // inside the piece. The offset is part of the contract: hashing the whole
    // piece (the previous behaviour) returned a hash no block can match.
    void async_hash2(lt::storage_index_t storage, lt::piece_index_t piece,
        int offset, lt::disk_job_flags_t /*flags*/,
        std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler) override
    {
        DiskCallbackScope callback_scope;
        TORRENTFS_DIAG("[DIAG] async_hash2 CALLED piece=%d offset=%d\n",
                static_cast<int>(piece), offset);
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d => no storage\n", static_cast<int>(piece));
            enqueue_completion([h = std::move(handler), p = piece] {
                h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, piece, offset, handler = std::move(handler)]() mutable {
            int const piece_idx = static_cast<int>(piece);
            int const piece_size2 = ps->piece_size2(piece_idx);
            int64_t const sz = ps->piece_size(piece_idx);
            TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d file_size=%ld offset=%d piece_size2=%d\n",
                    piece_idx, (long)sz, offset, piece_size2);
            if (sz <= 0) {
                TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d => file_size<=0, empty hash\n", piece_idx);
                enqueue_completion([h = std::move(handler), p = piece] {
                    h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                        boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
                });
                return;
            }

            if (piece_size2 <= 0 || offset < 0 || offset >= piece_size2) {
                TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d offset=%d outside piece_size2=%d\n",
                        piece_idx, offset, piece_size2);
                enqueue_completion([h = std::move(handler), p = piece] {
                    h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                        boost::system::errc::invalid_argument, boost::system::generic_category())));
                });
                return;
            }

            int const block_len = std::min<int>(lt::default_block_size, piece_size2 - offset);
            lt::sha256_hash hash = ps->hash_block_sha256(piece_idx, offset, block_len);
            TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d offset=%d len=%d hash=%02x%02x%02x%02x... => posted\n",
                    piece_idx, offset, block_len,
                    (unsigned char)hash[0], (unsigned char)hash[1],
                    (unsigned char)hash[2], (unsigned char)hash[3]);
            enqueue_completion([h = std::move(handler), p = piece, hash] {
                h(p, hash, lt::storage_error());
            });
        });
    }

    // disk_interface: async_move_storage
    void async_move_storage(lt::storage_index_t /*storage*/, std::string /*p*/,
        lt::move_flags_t /*flags*/,
        std::function<void(lt::status_t, std::string const&, lt::storage_error const&)> handler) override
    {
#if LIBTORRENT_VERSION_NUM >= 20100
        handler(lt::disk_status::fatal_disk_error, std::string(),
#else
        // libtorrent 2.0.x: disk_status namespace not available, use default status_t
        handler(lt::status_t{}, std::string(),
#endif
            lt::storage_error(lt::error_code(boost::system::errc::not_supported, boost::system::generic_category())));
    }

    // disk_interface: async_release_files
    void async_release_files(lt::storage_index_t storage,
        std::function<void()> handler) override
    {
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            if (handler) handler();
            return;
        }
        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, handler = std::move(handler)]() mutable {
            ps->close_write_handles();
            if (handler) {
                enqueue_completion([h = std::move(handler)]() mutable { h(); });
            }
        });
    }

    // disk_interface: async_check_files
    void async_check_files(lt::storage_index_t storage,
        lt::add_torrent_params const* /*resume_data*/,
        lt::aux::vector<std::string, lt::file_index_t> /*links*/,
        std::function<void(lt::status_t, lt::storage_error const&)> handler) override
    {
        DiskCallbackScope callback_scope;
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            handler(lt::status_t{},
                lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory,
                    boost::system::generic_category())));
            return;
        }

        PieceStorage* ps = storage_ptr.get();
        bool const disable_hash_checks = m_disable_hash_checks;
        storage_ptr->submit([this, ps, disable_hash_checks,
            handler = std::move(handler)]() mutable {
            // After FUSE remount, PieceStorageDiskIO must re-check which piece
            // files exist on disk so that libtorrent can correctly identify
            // cached pieces. With hash checks off a full check cannot tell a
            // missing piece from a present one (the comparison is skipped and
            // every piece passes), so it would broadcast a partial cache as
            // complete; the cached set is declared in `have_pieces` when the
            // torrent is added instead.
            if (disable_hash_checks) {
                TORRENTFS_DIAG("[DIAG] async_check_files: hash checks disabled, "
                               "keeping the declared bitmap\n");
                enqueue_completion([h = std::move(handler)]() mutable {
                    h(lt::status_t{}, lt::storage_error());
                });
                return;
            }

            // Cached piece files exist but carry no resume data (every torrent
            // is re-added on start), so `no_error` would tell libtorrent the
            // storage is empty: the session then advertises an empty bitfield
            // and serves nothing. `need_full_check` makes it hash the cache
            // through async_hash and rebuild the bitfield from pieces on disk.
            lt::status_t status{};
            if (has_cached_piece_file(ps->pieces_dir(), ps->get_info_hash_hex())) {
                TORRENTFS_DIAG("[DIAG] async_check_files: cached pieces => need_full_check\n");
                status = lt::disk_status::need_full_check;
            }
            enqueue_completion([h = std::move(handler), status]() mutable {
                h(status, lt::storage_error());
            });
        });
    }

    // disk_interface: async_stop_torrent
    void async_stop_torrent(lt::storage_index_t storage,
        std::function<void()> handler) override
    {
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            if (handler) handler();
            return;
        }
        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, handler = std::move(handler)]() mutable {
            ps->close_write_handles();
            if (handler) {
                enqueue_completion([h = std::move(handler)]() mutable { h(); });
            }
        });
    }

    // disk_interface: async_rename_file
    void async_rename_file(lt::storage_index_t /*storage*/,
        lt::file_index_t /*index*/, std::string /*name*/,
        std::function<void(std::string const&, lt::file_index_t, lt::storage_error const&)> handler) override
    {
        handler(std::string(), lt::file_index_t(0),
            lt::storage_error(lt::error_code(boost::system::errc::not_supported, boost::system::generic_category())));
    }

    // disk_interface: async_delete_files
    void async_delete_files(lt::storage_index_t storage,
        lt::remove_flags_t /*options*/,
        std::function<void(lt::storage_error const&)> handler) override
    {
        DiskCallbackScope callback_scope;
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            handler(lt::storage_error());
            return;
        }
        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, handler = std::move(handler)]() mutable {
            ps->delete_piece_files();
            enqueue_completion([h = std::move(handler)]() mutable {
                h(lt::storage_error());
            });
        });
    }

    // disk_interface: async_set_file_priority
    void async_set_file_priority(lt::storage_index_t /*storage*/,
        lt::aux::vector<lt::download_priority_t, lt::file_index_t> prio,
        std::function<void(lt::storage_error const&,
            lt::aux::vector<lt::download_priority_t, lt::file_index_t>)> handler) override
    {
        handler(lt::storage_error(), std::move(prio));
    }

    // disk_interface: async_clear_piece
    // TSI-2467: Fixed to pass actual piece index instead of hardcoded
    // piece_index_t(0). No dedicated test: this is a pure C++ disk_interface
    // callback with no FFI exposure or Rust call path, and libtorrent uses
    // the handler only as a completion signal (does not validate the index).
    void async_clear_piece(lt::storage_index_t storage,
        lt::piece_index_t index,
        std::function<void(lt::piece_index_t)> handler) override
    {
        auto storage_ptr = get_storage(storage);
        if (!storage_ptr) {
            if (handler) handler(index);
            return;
        }
        PieceStorage* ps = storage_ptr.get();
        storage_ptr->submit([this, ps, index, handler = std::move(handler)]() mutable {
            ps->clear_piece(static_cast<int>(index));
            if (handler) {
                enqueue_completion([h = std::move(handler), index]() mutable {
                    h(index);
                });
            }
        });
    }

    // disk_interface: update_stats_counters
    void update_stats_counters(lt::counters& /*c*/) const override {
    }

    // disk_interface: get_status
    std::vector<lt::open_file_state> get_status(lt::storage_index_t) const override {
        return {};
    }

    // disk_interface: abort
    void abort(bool wait) override {
        // The workers post completions back to this io_context; join them
        // before libtorrent tears the session down.
        if (wait && m_pool) m_pool->stop_and_join();
    }

    // disk_interface: submit_jobs
    void submit_jobs() override {
    }

    // disk_interface: settings_updated
    void settings_updated() override {
    }

private:
    std::shared_ptr<PieceStorage> get_storage(lt::storage_index_t idx) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_storages.find(idx);
        if (it == m_storages.end()) return nullptr;
        return it->second;
    }

    lt::io_context& m_ios;
    std::string m_piece_cache_dir;
    // Session setting snapshotted at construction: with it on, a full check
    // marks every piece as passed, so the cached set must come from the
    // declared bitmap instead.
    bool m_disable_hash_checks;
    std::mutex m_mutex;
    std::map<lt::storage_index_t, std::shared_ptr<PieceStorage>> m_storages;
    // Serial lanes keyed by info hash, shared by successive storages of the
    // same torrent (a removal and re-add must not interleave file deletion
    // with the new storage's writes). Weak so an idle hash does not keep one.
    std::map<std::string, std::weak_ptr<TorrentLane>> m_lanes;
    // Created before any storage and stopped before they are released; jobs
    // hold shared references to both the pool and their storage.
    std::shared_ptr<CompletionMailbox> m_mailbox{
        std::make_shared<CompletionMailbox>(m_ios)};
    std::shared_ptr<DiskWorkerPool> m_pool;
    // Set first thing in the destructor; guards the re-entrant remove_torrent
    // that releasing queued completions can trigger.
    std::atomic<bool> m_destroying{false};
    lt::storage_index_t m_next_index{0};
    std::set<char*> m_allocated;
    std::mutex m_alloc_mutex;
};

} // anonymous namespace

// ============================================================================
// C API: lt_session_create_with_custom_storage
// Creates a session with PieceStorageDiskIO from the start, avoiding the
// session_params crash on libtorrent 2.0.x that occurs during session
// recreation in add_torrent_with_custom_storage.
// ============================================================================

lt_session_t lt_session_create_with_custom_storage(
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!piece_cache_dir || !*piece_cache_dir) {
        lt_ffi_error_set(error, "piece_cache_dir is required");
        return nullptr;
    }

    LT_FFI_BEGIN
        auto wrapper = new lt_session_wrapper();

        // TSI-2068: Always pass a non-empty JSON to build_settings_pack so
        // the pack is properly initialized via JSON parsing.  Calling
        // mutating methods (set_int, has_val) on a completely
        // default-constructed settings_pack corrupts settings / causes
        // SIGSEGV on libtorrent 2.1.x (TSI-2061).
        // When settings_json is NULL, construct a minimal JSON with
        // alert_mask — build_settings_pack will parse it and properly
        // initialize the pack via apply_int_setting.
        std::string effective_json;
        if (settings_json && strlen(settings_json) > 0) {
            effective_json = settings_json;
        } else {
            // Minimal JSON so build_settings_pack initializes the
            // pack through normal parsing (apply_int_setting).
            // alert_category::error (1) | alert_category::status (64)
            // | alert_category::piece_progress (4194304) = 4194369.
            effective_json = "{\"alert_mask\":4194369}";
        }
        lt::session_params params;
        params.settings = build_settings_pack(effective_json.c_str());

        // Inject alert_mask into user-provided settings when not already
        // present.  build_settings_pack has already initialized the pack
        // via JSON parsing, so set_int is safe here — unlike the
        // TSI-2061 scenario where it was called on a default pack.
        if (settings_json && strlen(settings_json) > 0) {
            bool user_set_alert_mask =
                strstr(settings_json, "\"alert_mask\"") != nullptr;
            if (!user_set_alert_mask) {
                params.settings.set_int(lt::settings_pack::alert_mask,
                    lt::alert_category::error | lt::alert_category::status |
                        lt::alert_category::piece_progress);
            }
        }
        // TSI-2467: Disable close_redundant_connections by default. torrentfs
        // uses selective piece priorities (priority 0 = dont_download for
        // non-read-range pieces). When the last piece in a read range passes
        // hash check, libtorrent's is_finished() returns true (because all
        // non-filtered pieces are "have"), triggering torrent_finished
        // prematurely. With the default close_redundant_connections=true,
        // finished() disconnects all seed peers, causing download rate to
        // drop to zero until peers reconnect on the next read. Disabling it
        // keeps peers connected through the transient finished state, so the
        // next read's resume_download() can immediately resume downloading.
        // Respect explicit user configuration (close_redundant_connections in
        // the JSON settings) — only inject the default when unset.
        bool user_set_close_redundant = settings_json
            && strstr(settings_json, "\"close_redundant_connections\"") != nullptr;
        if (!user_set_close_redundant) {
            params.settings.set_bool(
                lt::settings_pack::close_redundant_connections, false);
        }
        // Same reasoning as close_redundant_connections above, and as the plain
        // session path: this session is the seeder, so it must accept a peer
        // whose address it already holds a connection to (peers behind one
        // NAT, or a foreign client on the seeder's own host).  Upstream's
        // address-only match would drop that peer as a duplicate before it
        // receives a block.  An explicit user value still wins.
        bool user_set_multiple_connections = settings_json
            && strstr(settings_json, "\"allow_multiple_connections_per_ip\"") != nullptr;
        if (!user_set_multiple_connections) {
            params.settings.set_bool(
                lt::settings_pack::allow_multiple_connections_per_ip, true);
        }

        std::string cache_dir(piece_cache_dir);
        // Snapshot the setting the piece accounting depends on: with hash
        // checks off, the cached set has to be declared from the piece files
        // (see async_check_files), so both the add path and the disk_io need
        // the same value.
        wrapper->disable_hash_checks =
            params.settings.get_bool(lt::settings_pack::disable_hash_checks);
        wrapper->piece_cache_dir = cache_dir;
        const bool disable_hash_checks = wrapper->disable_hash_checks;
        params.disk_io_constructor = [cache_dir, disable_hash_checks](lt::io_context& ios,
            lt::settings_interface const&, lt::counters&) -> std::unique_ptr<lt::disk_interface> {
            return std::make_unique<PieceStorageDiskIO>(ios, cache_dir, disable_hash_checks);
        };
        wrapper->session = new lt::session(std::move(params));

        return static_cast<lt_session_t>(wrapper);
    LT_FFI_END(error, nullptr)
}

// ============================================================================
// C API: lt_session_add_torrent_with_custom_storage
// Adds a torrent to an existing custom-storage session (created via
// lt_session_create_with_custom_storage). No longer recreates the session.
// ============================================================================

lt_torrent_handle_t lt_session_add_torrent_with_custom_storage(
    lt_session_t session, lt_torrent_info_t info,
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!session || !info) {
        lt_ffi_error_set(error, "Invalid session or torrent info");
        return nullptr;
    }

    LT_FFI_BEGIN
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        (void)settings_json; // Settings are now baked into session at creation time
        std::string cache_dir(piece_cache_dir ? piece_cache_dir : "/tmp/torrentfs-cache");

        // Add the torrent to the existing session (custom storage already set up)
        lt::add_torrent_params atp;
        atp.ti = std::make_shared<lt::torrent_info>(*ti);
        atp.save_path = cache_dir;
        // Clear paused: default_flags includes paused, preventing tracker
        // announces. Download is triggered on-demand via set_piece_deadline.
        atp.flags &= ~lt::torrent_flags::paused;
        declare_cached_pieces_if_needed(*wrapper, atp);

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    LT_FFI_END(error, nullptr)
}

lt_torrent_handle_t lt_session_add_torrent_with_custom_storage_upload_mode(
    lt_session_t session, lt_torrent_info_t info,
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!session || !info) {
        lt_ffi_error_set(error, "Invalid session or torrent info");
        return nullptr;
    }

    LT_FFI_BEGIN
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto ti = static_cast<lt::torrent_info*>(info);

        (void)settings_json; // Settings are now baked into session at creation time
        std::string cache_dir(piece_cache_dir ? piece_cache_dir : "/tmp/torrentfs-cache");

        // Add the torrent in upload_mode to the existing session
        lt::add_torrent_params atp;
        atp.ti = std::make_shared<lt::torrent_info>(*ti);
        atp.save_path = cache_dir;
        // Clear paused so tracker announces and peer connections work.
        atp.flags &= ~lt::torrent_flags::paused;
        atp.flags |= lt::torrent_flags::upload_mode;
        declare_cached_pieces_if_needed(*wrapper, atp);

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    LT_FFI_END(error, nullptr)
}

// ============================================================================
// C API: TSI-2262 — Per-info-hash shared read lock for Rust PieceStore
// Allows the Rust read path (PieceStore::read_piece) to acquire a shared
// (read) lock on the same per-info-hash shared_mutex that the C++
// PieceStorage::write_piece holds exclusively. This prevents concurrent
// readers from reading partially-written piece files during active
// downloads.
// ============================================================================

// Acquire a shared (read) lock on the per-info-hash mutex.
// Blocks if a writer (write_piece) currently holds the exclusive lock.
// The caller MUST pair this with lt_unlock_piece_read before accessing
// the piece file via Rust's std::fs::read.
void lt_lock_piece_read(const char* info_hash_hex) {
    if (!info_hash_hex || !*info_hash_hex) return;
    LT_FFI_BEGIN
    // get_piece_lock returns a shared_ptr copy, so the mutex outlives
    // the map entry even if a concurrent remove_torrent happens.
    auto lock = get_piece_lock(std::string(info_hash_hex));
    lock->lock_shared();
    LT_FFI_END_VOID(nullptr)
}

// Release a previously-acquired shared (read) lock.
// Uses find_piece_lock (not operator[]) so it never creates a spurious
// new mutex. If no entry exists (e.g. torrent never had a write_piece
// call), the unlock is a no-op — the matching lock would also have been
// a no-op on a freshly created empty mutex, but find avoids the UB of
// unlocking a mutex that was never locked.
void lt_unlock_piece_read(const char* info_hash_hex) {
    if (!info_hash_hex || !*info_hash_hex) return;
    LT_FFI_BEGIN
    auto lock = find_piece_lock(std::string(info_hash_hex));
    if (lock) {
        lock->unlock_shared();
    }
    LT_FFI_END_VOID(nullptr)
}

// ============================================================================
// TSI-2276: Tracker manipulation FFI — extract / replace / re-announce
// ============================================================================

// Build a JSON array string from a vector of (tier, url) pairs.
// Format: [{"tier":0,"url":"http://..."},{"tier":1,"url":"udp://..."}]
static std::string trackers_to_json(const std::vector<std::pair<int, std::string>>& trackers) {
    std::string json = "[";
    for (size_t i = 0; i < trackers.size(); i++) {
        if (i > 0) json += ",";
        json += "{\"tier\":";
        json += std::to_string(trackers[i].first);
        json += ",\"url\":\"";
        // Escape JSON-special and control characters per RFC 8259.
        for (unsigned char uc : trackers[i].second) {
            switch (uc) {
                case '"': json += "\\\""; break;
                case '\\': json += "\\\\"; break;
                case '\b': json += "\\b"; break;
                case '\f': json += "\\f"; break;
                case '\n': json += "\\n"; break;
                case '\r': json += "\\r"; break;
                case '\t': json += "\\t"; break;
                default:
                    if (uc < 0x20) {
                        // U+0000–U+001F: \u00XX
                        char buf[8];
                        snprintf(buf, sizeof(buf), "\\u%04x", uc);
                        json += buf;
                    } else {
                        json += static_cast<char>(uc);
                    }
                    break;
            }
        }
        json += "\"}";
    }
    json += "]";
    return json;
}

int lt_torrent_info_trackers(lt_torrent_info_t info, char** out_json, lt_error_t* error) {
    if (!info || !out_json) {
        lt_ffi_error_set(error, "Invalid arguments");
        return -1;
    }

    LT_FFI_BEGIN
        auto ti = static_cast<lt::torrent_info*>(info);

        // torrent_info::trackers() returns the merged list from both
        // 'announce' and 'announce-list', with tier numbers preserved.
        std::vector<std::pair<int, std::string>> trackers;
        const auto& announce_list = ti->trackers();
        for (const auto& t : announce_list) {
            trackers.emplace_back(static_cast<int>(t.tier), t.url);
        }
        std::string json = trackers_to_json(trackers);
        *out_json = strdup(json.c_str());
        if (!*out_json) {
            lt_ffi_error_set(error, "strdup failed");
            return -1;
        }
        return 0;
    LT_FFI_END(error, -1)
}

// Minimal JSON array parser for tracker replacement.
// Parses: [{"tier":N,"url":"..."},...]
// Uses the existing skip_json_ws / parse_json_string helpers defined above.
// Invariant: parse_json_string fully consumes quoted strings (including any
// '}' or '{' characters inside the string), so the object-end check (*p != '}')
// only triggers on the actual closing brace — never on a brace inside a URL.
static int parse_trackers_json(const char* json, std::vector<lt::announce_entry>& out) {
    if (!json) return -1;
    const char* p = json;
    skip_json_ws(p);
    if (*p != '[') return -1;
    p++; // skip '['
    skip_json_ws(p);

    while (*p && *p != ']') {
        if (*p != '{') return -1;
        p++; // skip '{'
        skip_json_ws(p);

        lt::announce_entry entry;
        entry.tier = 0;
        bool have_tier = false;
        bool have_url = false;

        while (*p && *p != '}') {
            skip_json_ws(p);
            if (*p != '"') return -1;
            std::string key = parse_json_string(p);
            skip_json_ws(p);
            if (*p != ':') return -1;
            p++; // skip ':'
            skip_json_ws(p);

            if (key == "tier") {
                entry.tier = static_cast<int>(parse_json_int(p));
                have_tier = true;
            } else if (key == "url") {
                if (*p != '"') return -1;
                entry.url = parse_json_string(p);
                have_url = true;
            } else {
                // Unknown key — skip its value.
                if (*p == '"') {
                    parse_json_string(p);
                } else {
                    parse_json_int(p);
                }
            }
            skip_json_ws(p);
            if (*p == ',') { p++; skip_json_ws(p); }
        }

        if (!have_url) return -1; // url is mandatory
        if (!have_tier) entry.tier = 0;
        out.push_back(std::move(entry));

        if (*p != '}') return -1;
        p++; // skip '}'
        skip_json_ws(p);
        if (*p == ',') { p++; skip_json_ws(p); }
    }

    if (*p != ']') return -1;
    return 0;
}

int lt_torrent_handle_replace_trackers(lt_torrent_handle_t handle, const char* trackers_json, lt_error_t* error) {
    if (!handle || !trackers_json) {
        lt_ffi_error_set(error, "Invalid arguments");
        return -1;
    }

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) {
        lt_ffi_error_set(error, "Invalid torrent handle");
        return -1;
    }

    LT_FFI_BEGIN
        std::vector<lt::announce_entry> trackers;
        if (parse_trackers_json(trackers_json, trackers) != 0) {
            lt_ffi_error_set(error, "Failed to parse trackers JSON");
            return -1;
        }
        h->replace_trackers(trackers);
        return 0;
    LT_FFI_END(error, -1)
}

int lt_torrent_handle_force_reannounce(lt_torrent_handle_t handle) {
    if (!handle) return -1;

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;

    try {
        h->force_reannounce();
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
    LT_FFI_TAIL(nullptr, -1)
}

// TSI-2277: Extract the current tracker list from a torrent_handle.
// Returns the live trackers (after any replace_trackers calls) as a JSON
// array string, same format as lt_torrent_info_trackers. Used by the
// tracker merge logic to get the existing handle's trackers for dedup.
int lt_torrent_handle_trackers(lt_torrent_handle_t handle, char** out_json, lt_error_t* error) {
    if (!handle || !out_json) {
        lt_ffi_error_set(error, "Invalid arguments");
        return -1;
    }

    LT_FFI_BEGIN
        auto h = static_cast<lt::torrent_handle*>(handle);
        if (!h->is_valid()) {
            lt_ffi_error_set(error, "Invalid torrent handle");
            return -1;
        }

        // torrent_handle::trackers() returns the current announce_entry list
        // (reflecting any prior replace_trackers calls).
        std::vector<std::pair<int, std::string>> trackers;
        const auto& announce_list = h->trackers();
        for (const auto& t : announce_list) {
            trackers.emplace_back(static_cast<int>(t.tier), t.url);
        }
        std::string json = trackers_to_json(trackers);
        *out_json = strdup(json.c_str());
        if (!*out_json) {
            lt_ffi_error_set(error, "strdup failed");
            return -1;
        }
        return 0;
    LT_FFI_END(error, -1)
}

void lt_string_free(char* str) {
    if (str) {
        std::free(str);
    }
}

void lt_disk_io_counters_get(lt_disk_io_counters_t* out) {
    if (!out) return;
    out->piece_fsyncs = g_disk_piece_fsyncs.load(std::memory_order_relaxed);
    out->piece_markers = g_disk_piece_markers.load(std::memory_order_relaxed);
    out->write_blocks = g_disk_write_blocks.load(std::memory_order_relaxed);
    out->net_thread_io_calls
        = g_disk_net_thread_io_calls.load(std::memory_order_relaxed);
    out->net_callback_calls
        = g_disk_callback_calls.load(std::memory_order_relaxed);
    out->net_callback_total_us
        = g_disk_callback_total_us.load(std::memory_order_relaxed);
    out->net_callback_max_us
        = g_disk_callback_max_us.load(std::memory_order_relaxed);
    out->read_buffers_allocated
        = g_read_buffers_allocated.load(std::memory_order_relaxed);
    out->read_buffers_released
        = g_read_buffers_released.load(std::memory_order_relaxed);
    out->read_buffers_live
        = g_read_buffers_live.load(std::memory_order_relaxed);
    out->read_buffers_dropped
        = g_read_buffers_dropped.load(std::memory_order_relaxed);
    out->piece_publish_failures
        = g_piece_publish_failures.load(std::memory_order_relaxed);
    out->write_completion_errors
        = g_write_completion_errors.load(std::memory_order_relaxed);
    out->write_completion_errors_delivered
        = g_write_completion_errors_delivered.load(std::memory_order_relaxed);
}

void lt_disk_io_counters_reset(void) {
    g_disk_piece_fsyncs.store(0, std::memory_order_relaxed);
    g_disk_piece_markers.store(0, std::memory_order_relaxed);
    g_disk_write_blocks.store(0, std::memory_order_relaxed);
    g_disk_net_thread_io_calls.store(0, std::memory_order_relaxed);
    g_disk_callback_calls.store(0, std::memory_order_relaxed);
    g_disk_callback_total_us.store(0, std::memory_order_relaxed);
    g_disk_callback_max_us.store(0, std::memory_order_relaxed);
    g_read_buffers_allocated.store(0, std::memory_order_relaxed);
    g_read_buffers_released.store(0, std::memory_order_relaxed);
    g_read_buffers_live.store(0, std::memory_order_relaxed);
    g_read_buffers_dropped.store(0, std::memory_order_relaxed);
    g_piece_publish_failures.store(0, std::memory_order_relaxed);
    g_write_completion_errors.store(0, std::memory_order_relaxed);
    g_write_completion_errors_delivered.store(0, std::memory_order_relaxed);
}

// ============================================================================
// C API: read-completion mailbox test hooks
//
// A session's custom disk object is owned by libtorrent, so the integration
// test cannot reach its mailbox directly. These helpers look it up by the
// cache directory it was created for. They exist so the shutdown discard path
// (queued successful read completions destroyed by `close`) can be exercised
// deterministically instead of raced against a live teardown.
// ============================================================================

int lt_disk_io_test_pause_completion_mailbox(const char* piece_cache_dir, int paused) {
    if (!piece_cache_dir || !*piece_cache_dir) return 0;
    std::lock_guard<std::mutex> lock(g_disk_io_registry_mutex);
    auto it = g_disk_io_registry.find(piece_cache_dir);
    if (it == g_disk_io_registry.end()) return 0;
    it->second->test_pause_completion_mailbox(paused != 0);
    return 1;
}

int lt_disk_io_test_close_completion_mailbox(const char* piece_cache_dir) {
    if (!piece_cache_dir || !*piece_cache_dir) return 0;
    std::lock_guard<std::mutex> lock(g_disk_io_registry_mutex);
    auto it = g_disk_io_registry.find(piece_cache_dir);
    if (it == g_disk_io_registry.end()) return 0;
    it->second->test_close_completion_mailbox();
    return 1;
}

int lt_disk_io_test_completion_mailbox_stats(const char* piece_cache_dir,
    uint64_t* pending, uint64_t* discarded, uint64_t* delivered) {
    if (!piece_cache_dir || !*piece_cache_dir
        || !pending || !discarded || !delivered) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(g_disk_io_registry_mutex);
    auto it = g_disk_io_registry.find(piece_cache_dir);
    if (it == g_disk_io_registry.end()) return 0;
    it->second->test_completion_mailbox_stats(pending, discarded, delivered);
    return 1;
}

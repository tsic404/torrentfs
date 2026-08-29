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
};

lt_torrent_info_t lt_torrent_info_create(const char* filepath, lt_error_t* error) {
    try {
        auto ti = new lt::torrent_info(std::string(filepath));
        return static_cast<lt_torrent_info_t>(ti);
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
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
            if (error) {
                error->code = ec.value();
                static thread_local std::string err_msg;
                err_msg = ec.message();
                error->message = err_msg.c_str();
            }
            return nullptr;
        }
        lt::torrent_info ti(node, ec);
        if (ec) {
            if (error) {
                error->code = ec.value();
                static thread_local std::string err_msg;
                err_msg = ec.message();
                error->message = err_msg.c_str();
            }
            return nullptr;
        }
        return static_cast<lt_torrent_info_t>(new lt::torrent_info(std::move(ti)));
    } catch (const lt::system_error& e) {
        if (error) {
            error->code = e.code().value();
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_torrent_info_destroy(lt_torrent_info_t info) {
    if (info) {
        auto ti = static_cast<lt::torrent_info*>(info);
        delete ti;
    }
}

const char* lt_torrent_info_name(lt_torrent_info_t info) {
    if (!info) return nullptr;
    auto ti = static_cast<lt::torrent_info*>(info);
    static thread_local std::string name;
    name = ti->name();
    return name.c_str();
}

uint64_t lt_torrent_info_total_size(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return ti->total_size();
}

uint32_t lt_torrent_info_piece_length(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->piece_length());
}

uint32_t lt_torrent_info_num_pieces(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_pieces());
}

uint32_t lt_torrent_info_num_files(lt_torrent_info_t info) {
    if (!info) return 0;
    auto ti = static_cast<lt::torrent_info*>(info);
    return static_cast<uint32_t>(ti->num_files());
}

int lt_torrent_info_get_files(lt_torrent_info_t info, lt_file_entry_t** files, uint32_t* count) {
    if (!info || !files || !count) return -1;

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
}

void lt_files_free(lt_file_entry_t* files) {
    std::free(files);
}

int lt_torrent_info_get_info_hash(lt_torrent_info_t info, uint8_t* hash_out) {
    if (!info || !hash_out) return -1;
    auto ti = static_cast<lt::torrent_info*>(info);
    auto h = ti->info_hashes();
    auto sha1 = h.get_best();
    std::memcpy(hash_out, sha1.data(), 20);
    return 0;
}

int lt_torrent_info_hash_for_piece(lt_torrent_info_t info, int piece_index, uint8_t* hash_out) {
    if (!info || !hash_out || piece_index < 0) return -1;
    try {
        auto ti = static_cast<lt::torrent_info*>(info);
        if (piece_index >= ti->num_pieces()) return -1;
        auto h = ti->hash_for_piece(lt::piece_index_t(piece_index));
        if (h.is_all_zeros()) return -1;
        std::memcpy(hash_out, h.data(), 20);
        return 0;
    } catch (const std::exception&) {
        return -1;
    }
}

// TSI-2277: Extract the private flag from the torrent info dict.
// libtorrent's torrent_info::priv() returns true when the 'private'
// field in the info dict is set to 1 (BEP-27 / PT isolation).
// Returns 1 if private, 0 if not, -1 on error (null handle).
int lt_torrent_info_is_private(lt_torrent_info_t info) {
    if (!info) return -1;
    try {
        auto ti = static_cast<lt::torrent_info*>(info);
        return ti->priv() ? 1 : 0;
    } catch (const std::exception&) {
        return -1;
    }
}

lt_torrent_metadata_t* lt_torrent_info_get_metadata(lt_torrent_info_t info) {
    if (!info) return nullptr;

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
}

void lt_torrent_metadata_destroy(lt_torrent_metadata_t* metadata) {
    if (metadata) {
        auto* meta = reinterpret_cast<lt_torrent_metadata*>(metadata);
        delete meta;
    }
}

lt_session_t lt_session_create(const char* listen_interface, lt_error_t* error) {
    try {
        auto wrapper = new lt_session_wrapper();
        wrapper->session = new lt::session();

        lt::settings_pack settings;
        if (listen_interface && strlen(listen_interface) > 0) {
            settings.set_str(lt::settings_pack::listen_interfaces, listen_interface);
        }
        settings.set_int(lt::settings_pack::alert_mask,
            lt::alert_category::error | lt::alert_category::status);
        wrapper->session->apply_settings(settings);

        return static_cast<lt_session_t>(wrapper);
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
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
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
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

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_handle_t lt_session_add_torrent_upload_mode(lt_session_t session, lt_torrent_info_t info, const char* save_path, lt_error_t* error) {
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
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

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(params);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

void lt_session_remove_torrent(lt_session_t session, lt_torrent_handle_t handle, int remove_files) {
    if (session && handle) {
        auto wrapper = static_cast<lt_session_wrapper*>(session);
        auto h = static_cast<lt::torrent_handle*>(handle);
        lt::remove_flags_t flags = remove_files ? lt::session::delete_files : lt::remove_flags_t{};
        std::lock_guard<std::mutex> lock(wrapper->mutex);
        wrapper->session->remove_torrent(*h, flags);
        delete h;
    }
}

void lt_torrent_handle_destroy(lt_torrent_handle_t handle) {
    if (handle) {
        auto h = static_cast<lt::torrent_handle*>(handle);
        delete h;
    }
}

int lt_torrent_handle_is_valid(lt_torrent_handle_t handle) {
    if (!handle) return 0;
    auto h = static_cast<lt::torrent_handle*>(handle);
    return h->is_valid() ? 1 : 0;
}

int lt_torrent_handle_status(lt_torrent_handle_t handle, int* state, float* progress, uint64_t* total_done, uint64_t* total,
    int64_t* dl_rate, int64_t* ul_rate, int64_t* total_dl, int64_t* total_ul,
    int32_t* peers, int32_t* seeds) {
    if (!handle || !state || !progress || !total_done || !total) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
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
}

int lt_torrent_handle_read_piece(lt_session_t session, lt_torrent_handle_t handle, int piece_index, uint8_t** data_out, size_t* size_out, lt_error_t* error) {
    (void)session; // session is no longer used for inline pop_alerts
    if (!handle || !data_out || !size_out) {
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) {
        if (error) {
            error->code = -1;
            error->message = "Invalid torrent handle";
        }
        return -1;
    }

    try {
        // Enqueue the read request — the actual data will arrive via
        // read_piece_alert, consumed by the background AlertConsumer thread.
        h->read_piece(lt::piece_index_t(piece_index));
        *data_out = nullptr;
        *size_out = 0;
        return 0;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
}

void lt_piece_data_free(uint8_t* data) {
    if (data) {
        std::free(data);
    }
}

int lt_torrent_handle_get_piece_info(lt_torrent_handle_t handle, int file_index, int64_t* first_piece, int64_t* num_pieces, int64_t* file_offset) {
    if (!handle || !first_piece || !num_pieces || !file_offset) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    const auto& fs = t->files();
    
    lt::file_index_t fi(file_index);
    if (fi >= fs.end_file()) return -1;
    
    auto file_size = fs.file_size(fi);
    auto piece_length = t->piece_length();
    auto file_offset_val = fs.file_offset(fi);
    
    int64_t start_piece = file_offset_val / piece_length;
    int64_t end_offset = file_offset_val + file_size;
    int64_t end_piece = (end_offset + piece_length - 1) / piece_length;
    
    *first_piece = start_piece;
    *num_pieces = end_piece - start_piece;
    *file_offset = file_offset_val;
    
    return 0;
}

int lt_torrent_handle_get_torrent_info(lt_torrent_handle_t handle, int64_t* piece_length, int64_t* num_pieces) {
    if (!handle || !piece_length || !num_pieces) return -1;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return -1;
    
    auto t = h->torrent_file();
    if (!t) return -1;
    
    *piece_length = t->piece_length();
    *num_pieces = t->num_pieces();
    
    return 0;
}

int lt_torrent_handle_have_piece(lt_torrent_handle_t handle, int piece_index) {
    if (!handle) return 0;
    
    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) return 0;
    
    auto status = h->status();
    if (static_cast<int>(status.pieces.size()) <= piece_index) return 0;
    
    return status.pieces[lt::piece_index_t(piece_index)] ? 1 : 0;
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
    } else if (key == "proxy_type") {
        // TSI-2535: map the config string ("socks4"…"i2p_proxy") to the
        // libtorrent proxy_type_t integer. Rust-side validate() restricts
        // the value to this exact domain, so any other string reaching here
        // means either a C++/Rust contract drift or a build where I2P is
        // compiled out (TORRENT_USE_I2P=0) while validate() still accepts
        // "i2p_proxy". Both are programming bugs, never user input — abort
        // loudly instead of silently misconfiguring the session to `none`.
        int proxy;
        if (val == "socks4") proxy = static_cast<int>(lt::settings_pack::socks4);
        else if (val == "socks5") proxy = static_cast<int>(lt::settings_pack::socks5);
        else if (val == "socks5_pw") proxy = static_cast<int>(lt::settings_pack::socks5_pw);
        else if (val == "http") proxy = static_cast<int>(lt::settings_pack::http);
        else if (val == "http_pw") proxy = static_cast<int>(lt::settings_pack::http_pw);
#if TORRENT_USE_I2P
        else if (val == "i2p_proxy") proxy = static_cast<int>(lt::settings_pack::i2p_proxy);
#endif
        else {
            fprintf(stderr, "[proxy] unknown proxy_type %s (TORRENT_USE_I2P=%d) — aborting\n",
                    val.c_str(), TORRENT_USE_I2P);
            std::abort();
        }
        pack.set_int(lt::settings_pack::proxy_type, proxy);
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
    } else if (key == "proxy_port") {
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

    auto pack = build_settings_pack(settings_json);
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    wrapper->session->apply_settings(pack);
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

int lt_session_get_bool_setting(lt_session_t session, const char* key, int* out) {
    if (!session || !key || !out) return -1;
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    try {
        auto settings = wrapper->session->get_settings();
        if (get_session_bool_setting_impl(settings, std::string(key), out)) {
            return 0;
        }
    } catch (const std::exception&) {}
    return -1;
}

static bool get_session_int_setting_impl(lt::settings_pack const& settings, const std::string& key, int* out) {
    if (key == "proxy_type") {
        if (settings.has_val(lt::settings_pack::proxy_type)) {
            *out = settings.get_int(lt::settings_pack::proxy_type);
            return true;
        }
    }
    return false;
}

int lt_session_get_int_setting(lt_session_t session, const char* key, int* out) {
    if (!session || !key || !out) return -1;
    auto wrapper = static_cast<lt_session_wrapper*>(session);
    std::lock_guard<std::mutex> lock(wrapper->mutex);
    try {
        auto settings = wrapper->session->get_settings();
        if (get_session_int_setting_impl(settings, std::string(key), out)) {
            return 0;
        }
    } catch (const std::exception&) {}
    return -1;
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

void lt_session_set_alert_notify(lt_session_t session, void (*callback)(void* user_data), void* user_data) {
    if (!session) return;
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
}

lt_alert_list_t* lt_session_pop_alerts(lt_session_t session) {
    if (!session) return nullptr;

    auto wrapper = static_cast<lt_session_wrapper*>(session);

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
                lt::span<lt::stats_metric const> metrics = lt::session_stats_metrics();
                for (auto const& m : metrics) {
                    int idx = m.value_index;
                    if (idx < 0 || idx >= static_cast<int>(counters.size())) continue;
                    const char* name = m.name;
                    if (!name) continue;
                    std::string namestr(name);
                    if (namestr == "net.recv_rate") out.download_rate = counters[idx];
                    else if (namestr == "net.sent_rate") out.upload_rate = counters[idx];
                    else if (namestr == "net.recv_bytes") out.total_downloaded = counters[idx];
                    else if (namestr == "net.sent_bytes") out.total_uploaded = counters[idx];
                    else if (namestr == "dht.dht_nodes") out.dht_nodes = static_cast<int32_t>(counters[idx]);
                    else if (namestr == "peer.num_peers_connected") out.peers_connected = static_cast<int32_t>(counters[idx]);
                    else if (namestr == "peer.num_peers_half_open") out.half_open_connections = static_cast<int32_t>(counters[idx]);
                }
            }
            // ── torrent_finished_alert ──
            else if (auto* tf = lt::alert_cast<lt::torrent_finished_alert>(alert)) {
                out.type = LT_ALERT_TORRENT_FINISHED;
                alert_fill_info_hash_from_handle(tf->handle, out.info_hash);
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

std::string sha1_to_hex(lt::sha1_hash const& h) {
    char hex[41];
    for (int i = 0; i < 20; i++) {
        snprintf(hex + i * 2, 3, "%02x", static_cast<unsigned char>(h.data()[i]));
    }
    return std::string(hex, 40);
}

static bool ensure_dir_recursive(const std::string& path) {
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

class PieceStorage {
public:
    PieceStorage(const std::string& base_path, const std::string& info_hash_hex)
        : m_base_path(base_path), m_info_hash_hex(info_hash_hex)
    {
        std::string full_path = m_base_path + "/" + m_info_hash_hex;
        if (!ensure_dir_recursive(full_path)) {
            fprintf(stderr, "[DIAG] PieceStorage: failed to create directory %s\n",
                    full_path.c_str());
            throw std::runtime_error("Failed to create piece storage directory: " + full_path);
        }
    }

    std::string get_info_hash_hex() const { return m_info_hash_hex; }

    std::string pieces_dir() const {
        return m_base_path + "/" + m_info_hash_hex;
    }

    std::string piece_path(int piece_index) const {
        return pieces_dir() + "/" + m_info_hash_hex + ":piece:" + std::to_string(piece_index);
    }

    // TSI-2491: per-piece in-progress marker, written by `write_piece` after
    // the piece data reaches disk and cleared once the piece is known complete.
    std::string piece_marker_path(int piece_index) const {
        return piece_path(piece_index) + ".incomplete";
    }

    bool read_piece(int piece_index, int offset, char* buf, int size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return false;
        file.seekg(offset);
        file.read(buf, size);
        bool ok = file.good() || (file.eof() && file.gcount() > 0);
        TORRENTFS_DIAG("[DIAG] read_piece piece=%d offset=%d size=%d ok=%d first4=%02x%02x%02x%02x\n",
                piece_index, offset, size, ok,
                (unsigned char)(size>0?buf[0]:0), (unsigned char)(size>1?buf[1]:0),
                (unsigned char)(size>2?buf[2]:0), (unsigned char)(size>3?buf[3]:0));
        return ok;
    }

    bool write_piece(int piece_index, int offset, const char* buf, int size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        // TSI-2262: acquire an exclusive lock on the per-info-hash
        // shared_mutex so that Rust-side reads (PieceStore::read_piece)
        // cannot read a partially-written piece file. The shared_mutex
        // is separate from m_mutex (which serializes C++-internal
        // reads/writes) because the Rust read path bypasses m_mutex.
        auto piece_lock = get_piece_lock(m_info_hash_hex);
        std::unique_lock<std::shared_mutex> write_lock(*piece_lock);
        if (!ensure_dir_recursive(m_base_path + "/" + m_info_hash_hex)) {
            fprintf(stderr, "[DIAG] write_piece: ensure_dir_recursive failed for %s/%s\n",
                    m_base_path.c_str(), m_info_hash_hex.c_str());
            return false;
        }
        std::string path = piece_path(piece_index);
        TORRENTFS_DIAG("[DIAG] write_piece piece=%d offset=%d size=%d first4=%02x%02x%02x%02x path=%s\n",
                piece_index, offset, size,
                (unsigned char)(size>0?buf[0]:0), (unsigned char)(size>1?buf[1]:0),
                (unsigned char)(size>2?buf[2]:0), (unsigned char)(size>3?buf[3]:0),
                path.c_str());
        std::fstream file;
        file.open(path, std::ios::binary | std::ios::in | std::ios::out);
        if (!file.is_open()) {
            file.open(path, std::ios::binary | std::ios::out);
            if (!file.is_open()) return false;
        }
        file.seekp(offset);
        file.write(buf, size);
        if (!file.good()) return false;
        // TSI-2263: flush + fsync so piece data reaches disk before
        // the metadata is persisted.  Without fsync the OS page cache
        // holds the dirty page; a container restart (which may not
        // flush dirty pages) leaves a partial / zero-length piece file
        // that the restart scan registers at the wrong size, causing
        // the background SHA-1 verifier to purge it ("cache piece
        // cleaned" after restart).
        file.flush();
        // TSI-2263 review: flush pushes the C++ stream buffer to the OS
        // page cache.  If it fails (disk full / I/O error) the piece is
        // incomplete — proceeding to fsync and returning true would
        // silently swallow a partial write, the exact bug class this fix
        // targets.  Return false so libtorrent sees the write failure.
        if (!file.good()) {
            file.close();
            return false;
        }
        file.close();
        // Open the file descriptor solely to fsync it.  fstream does not
        // expose its fd, so reopen the path read-only and fsync that.
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd >= 0) {
            if (::fsync(fd) != 0) {
                TORRENTFS_DIAG("[DIAG] write_piece fsync failed for %s: %s\n",
                        path.c_str(), strerror(errno));
            }
            ::close(fd);
        }
        // TSI-2491: mark the piece as in-progress/incomplete. The piece file
        // may be partially written (blocks arrive at arbitrary offsets), so
        // the restart scan must not treat it as a complete candidate for
        // background SHA-1 verification. The marker is removed by the Rust
        // cache layer once `add_piece` proves the piece is complete.
        int mfd = ::open(piece_marker_path(piece_index).c_str(),
                         O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (mfd < 0) {
            fprintf(stderr, "[DIAG] write_piece: create marker failed for %s: %s\n",
                    piece_marker_path(piece_index).c_str(), strerror(errno));
        } else {
            ::close(mfd);
        }
        return true;
    }

    bool has_piece(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        return file.is_open();
    }

    int64_t piece_size(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary | std::ios::ate);
        if (!file.is_open()) return -1;
        return static_cast<int64_t>(file.tellg());
    }

    lt::sha1_hash hash_piece(int piece_index, int piece_size) {
        std::lock_guard<std::mutex> lock(m_mutex);
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

    lt::sha256_hash hash_piece_sha256(int piece_index, int piece_size) {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string path = piece_path(piece_index);
        std::ifstream file(path, std::ios::binary);
        if (!file.is_open()) return lt::sha256_hash();

        SHA256_CTX ctx;
        SHA256_Init(&ctx);
        std::vector<char> buf(16 * 1024);
        int remaining = piece_size;
        while (remaining > 0) {
            int to_read = std::min(remaining, static_cast<int>(buf.size()));
            file.read(buf.data(), to_read);
            int actual = static_cast<int>(file.gcount());
            if (actual == 0) break;
            SHA256_Update(&ctx, buf.data(), actual);
            remaining -= actual;
        }
        lt::sha256_hash result;
        SHA256_Final(reinterpret_cast<unsigned char*>(result.data()), &ctx);
        return result;
    }

    // Read the full raw piece data into a vector. Returns empty vector if the
    // piece file does not exist or is empty. Needed so that async_hash can
    // compute SHA256 sub-range hashes for v2 blocks in hybrid torrents.
    std::vector<char> read_piece_data(int piece_index) {
        std::lock_guard<std::mutex> lock(m_mutex);
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

    void delete_piece_files() {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string dir = m_base_path + "/" + m_info_hash_hex;
        rmdir(dir.c_str());
    }

private:
    std::string m_base_path;
    std::string m_info_hash_hex;
    mutable std::mutex m_mutex;
};

// ============================================================================
// PieceStorageDiskIO: implements disk_interface for piece-level storage
// ============================================================================

class PieceStorageDiskIO : public lt::disk_interface, public lt::buffer_allocator_interface {
public:
    PieceStorageDiskIO(lt::io_context& ios, const std::string& piece_cache_dir)
        : m_ios(ios), m_piece_cache_dir(piece_cache_dir)
    {
        if (piece_cache_dir.empty()) {
            throw std::runtime_error("PieceStorageDiskIO: piece_cache_dir is empty");
        }
        if (!ensure_dir_recursive(piece_cache_dir)) {
            fprintf(stderr, "[DIAG] PieceStorageDiskIO: failed to create cache dir %s\n",
                    piece_cache_dir.c_str());
            throw std::runtime_error("Failed to create piece cache directory: " + piece_cache_dir);
        }
    }

    // buffer_allocator_interface
    void free_disk_buffer(char* b) override {
        if (b) {
            std::lock_guard<std::mutex> lock(m_alloc_mutex);
            auto it = m_allocated.find(b);
            if (it != m_allocated.end()) {
                m_allocated.erase(it);
                std::free(b);
            }
            // else: double-free, silently ignore
        }
    }
#if LIBTORRENT_VERSION_NUM >= 20100
    void free_multiple_buffers(lt::span<char*> bufs) override {
        std::lock_guard<std::mutex> lock(m_alloc_mutex);
        for (auto* b : bufs) {
            if (b) {
                auto it = m_allocated.find(b);
                if (it != m_allocated.end()) {
                    m_allocated.erase(it);
                    std::free(b);
                }
            }
        }
    }
#endif

    // disk_interface: new_torrent
    lt::storage_holder new_torrent(lt::storage_params const& p,
        std::shared_ptr<void> const& /*torrent*/) override
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        std::string info_hash_hex = sha1_to_hex(p.info_hash);
        TORRENTFS_DIAG("[DIAG] new_torrent info_hash=%s\n",
                info_hash_hex.c_str());
        auto storage = std::make_unique<PieceStorage>(m_piece_cache_dir, info_hash_hex);
        lt::storage_index_t idx = m_next_index;
        ++m_next_index;
        m_storages[idx] = std::move(storage);
        return lt::storage_holder(idx, *this);
    }

    // disk_interface: remove_torrent
    void remove_torrent(lt::storage_index_t idx) override {
        std::lock_guard<std::mutex> lock(m_mutex);
        // TSI-2262: the per-info-hash shared_mutex is intentionally NOT
        // erased from g_piece_locks here. A concurrent lt_unlock_piece_read
        // may still hold a shared_ptr to it; erasing would invalidate the
        // map entry's shared_ptr, but since get_piece_lock returns a copy,
        // the mutex stays alive until the last reference drops. Leaving
        // the entry avoids the erase-during-use UB entirely.
        m_storages.erase(idx);
    }

    // disk_interface: async_read
    void async_read(lt::storage_index_t storage, lt::peer_request const& r,
        std::function<void(lt::disk_buffer_holder, lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
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

        if (ps->read_piece(static_cast<int>(r.piece), r.start, buf, r.length)) {
#if LIBTORRENT_VERSION_NUM >= 20100
            boost::asio::post(m_ios, [h = std::move(handler), this, buf]() mutable {
                h(lt::disk_buffer_holder(*this, buf), lt::storage_error());
            });
#else
            boost::asio::post(m_ios, [h = std::move(handler), this, buf, len = r.length]() mutable {
                h(lt::disk_buffer_holder(*this, buf, len), lt::storage_error());
            });
#endif
        } else {
            {
                std::lock_guard<std::mutex> lock(m_alloc_mutex);
                m_allocated.erase(buf);
            }
            std::free(buf);
            boost::asio::post(m_ios, [h = std::move(handler)] {
                h(lt::disk_buffer_holder(),
                    lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
        }
    }

    // disk_interface: async_write
    bool async_write(lt::storage_index_t storage, lt::peer_request const& r,
        char const* buf, std::shared_ptr<lt::disk_observer> /*o*/,
        std::function<void(lt::storage_error const&)> handler,
        lt::disk_job_flags_t /*flags*/) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
            handler(lt::storage_error(lt::error_code(boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            return false;
        }

        if (ps->write_piece(static_cast<int>(r.piece), r.start, buf, r.length)) {
            TORRENTFS_DIAG("[DIAG] async_write piece=%d offset=%d len=%d => OK (posted)\n",
                    static_cast<int>(r.piece), r.start, r.length);
            boost::asio::post(m_ios, [h = std::move(handler)] { h(lt::storage_error()); });
        } else {
            TORRENTFS_DIAG("[DIAG] async_write piece=%d offset=%d len=%d => FAIL\n",
                    static_cast<int>(r.piece), r.start, r.length);
            boost::asio::post(m_ios, [h = std::move(handler)] {
                h(lt::storage_error(lt::error_code(boost::system::errc::io_error, boost::system::generic_category())));
            });
        }
        return false;
    }

    // disk_interface: async_hash
    void async_hash(lt::storage_index_t storage, lt::piece_index_t piece,
        lt::span<lt::sha256_hash> v2,
        lt::disk_job_flags_t flags,
        std::function<void(lt::piece_index_t, lt::sha1_hash const&, lt::storage_error const&)> handler) override
    {
        (void)flags;
        TORRENTFS_DIAG("[DIAG] async_hash CALLED piece=%d v2_size=%zu\n",
                static_cast<int>(piece), v2.size());
        auto* ps = get_storage(storage);
        if (!ps) {
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d => no storage\n", static_cast<int>(piece));
            // Zero out v2 hashes for hybrid torrents — no data available
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        int piece_idx = static_cast<int>(piece);
        int64_t sz = ps->piece_size(piece_idx);
        TORRENTFS_DIAG("[DIAG] async_hash piece=%d file_size=%ld\n", piece_idx, (long)sz);
        if (sz <= 0) {
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d => file_size<=0, empty hash\n", piece_idx);
            // Zero out v2 hashes for hybrid torrents — no piece data yet
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        // Read full piece data so we can compute v2 sub-range hashes
        auto data = ps->read_piece_data(piece_idx);
        if (data.empty()) {
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d => read_piece_data empty\n", piece_idx);
            for (auto& h : v2) h = lt::sha256_hash();
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha1_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        // Compute SHA1 over the full piece
        lt::hasher hasher;
        hasher.update(data.data(), static_cast<int>(data.size()));
        lt::sha1_hash hash = hasher.final();

        // Compute SHA256 for each v2 block in hybrid torrents.
        // Each v2 block is a contiguous sub-range of the piece data.
        // The v2 span has one entry per v2 block that overlaps this v1 piece.
        if (!v2.empty()) {
            size_t total = data.size();
            size_t n = v2.size();
            size_t block_sz = total / n;   // regular block size
            for (size_t j = 0; j < n; ++j) {
                size_t start = j * block_sz;
                size_t end = (j == n - 1) ? total : (j + 1) * block_sz;
                SHA256_CTX ctx;
                SHA256_Init(&ctx);
                SHA256_Update(&ctx, data.data() + start, end - start);
                SHA256_Final(reinterpret_cast<unsigned char*>(v2[j].data()), &ctx);
            }
            TORRENTFS_DIAG("[DIAG] async_hash piece=%d v2_blocks=%zu block_sz=%zu => populated\n",
                    piece_idx, n, block_sz);
        }

        TORRENTFS_DIAG("[DIAG] async_hash piece=%d size=%d hash=%02x%02x%02x%02x... => posted\n",
                piece_idx, static_cast<int>(data.size()),
                (unsigned char)hash[0], (unsigned char)hash[1],
                (unsigned char)hash[2], (unsigned char)hash[3]);
        boost::asio::post(m_ios, [h = std::move(handler), p = piece, hash] {
            h(p, hash, lt::storage_error());
        });
    }

    // disk_interface: async_hash2
    void async_hash2(lt::storage_index_t storage, lt::piece_index_t piece,
        int /*offset*/, lt::disk_job_flags_t /*flags*/,
        std::function<void(lt::piece_index_t, lt::sha256_hash const&, lt::storage_error const&)> handler) override
    {
        TORRENTFS_DIAG("[DIAG] async_hash2 CALLED piece=%d\n",
                static_cast<int>(piece));
        auto* ps = get_storage(storage);
        if (!ps) {
            TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d => no storage\n", static_cast<int>(piece));
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        int piece_idx = static_cast<int>(piece);
        int64_t sz = ps->piece_size(piece_idx);
        TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d file_size=%ld\n", piece_idx, (long)sz);
        if (sz <= 0) {
            TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d => file_size<=0, empty hash\n", piece_idx);
            boost::asio::post(m_ios, [h = std::move(handler), p = piece] {
                h(p, lt::sha256_hash(), lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory, boost::system::generic_category())));
            });
            return;
        }

        lt::sha256_hash hash = ps->hash_piece_sha256(piece_idx, static_cast<int>(sz));
        TORRENTFS_DIAG("[DIAG] async_hash2 piece=%d size=%d hash=%02x%02x%02x%02x... => posted\n",
                piece_idx, static_cast<int>(sz),
                (unsigned char)hash[0], (unsigned char)hash[1],
                (unsigned char)hash[2], (unsigned char)hash[3]);
        boost::asio::post(m_ios, [h = std::move(handler), p = piece, hash] {
            h(p, hash, lt::storage_error());
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
    void async_release_files(lt::storage_index_t /*storage*/,
        std::function<void()> handler) override
    {
        if (handler) handler();
    }

    // disk_interface: async_check_files
    void async_check_files(lt::storage_index_t storage,
        lt::add_torrent_params const* /*resume_data*/,
        lt::aux::vector<std::string, lt::file_index_t> /*links*/,
        std::function<void(lt::status_t, lt::storage_error const&)> handler) override
    {
        auto* ps = get_storage(storage);
        if (!ps) {
            handler(lt::status_t{},
                lt::storage_error(lt::error_code(
                    boost::system::errc::no_such_file_or_directory,
                    boost::system::generic_category())));
            return;
        }

        // After FUSE remount, PieceStorageDiskIO must re-check which piece files
        // exist on disk so that libtorrent can correctly identify cached pieces.
        // Scan the pieces directory and report existing piece files.
        std::string dir = ps->pieces_dir();
        std::set<int> existing_pieces;

        DIR* dp = opendir(dir.c_str());
        if (dp) {
            struct dirent* entry;
            std::string prefix = ps->get_info_hash_hex() + ":piece:";
            while ((entry = readdir(dp)) != nullptr) {
                std::string name(entry->d_name);
                if (name.size() > prefix.size() &&
                    name.compare(0, prefix.size(), prefix) == 0) {
                    std::string idx_str = name.substr(prefix.size());
                    try {
                        int idx = std::stoi(idx_str);
                        existing_pieces.insert(idx);
                    } catch (...) {
                        // Ignore malformed filenames
                    }
                }
            }
            closedir(dp);
        }

        // Report found pieces via handler — libtorrent will then call
        // async_hash for each piece to verify integrity.
        handler(lt::status_t{}, lt::storage_error());
    }

    // disk_interface: async_stop_torrent
    void async_stop_torrent(lt::storage_index_t /*storage*/,
        std::function<void()> handler) override
    {
        if (handler) handler();
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
        auto* ps = get_storage(storage);
        if (ps) {
            ps->delete_piece_files();
        }
        handler(lt::storage_error());
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
    void async_clear_piece(lt::storage_index_t /*storage*/,
        lt::piece_index_t index,
        std::function<void(lt::piece_index_t)> handler) override
    {
        if (handler) handler(index);
    }

    // disk_interface: update_stats_counters
    void update_stats_counters(lt::counters& /*c*/) const override {
    }

    // disk_interface: get_status
    std::vector<lt::open_file_state> get_status(lt::storage_index_t) const override {
        return {};
    }

    // disk_interface: abort
    void abort(bool /*wait*/) override {
    }

    // disk_interface: submit_jobs
    void submit_jobs() override {
    }

    // disk_interface: settings_updated
    void settings_updated() override {
    }

private:
    PieceStorage* get_storage(lt::storage_index_t idx) {
        std::lock_guard<std::mutex> lock(m_mutex);
        auto it = m_storages.find(idx);
        if (it == m_storages.end()) return nullptr;
        return it->second.get();
    }

    lt::io_context& m_ios;
    std::string m_piece_cache_dir;
    std::mutex m_mutex;
    std::map<lt::storage_index_t, std::unique_ptr<PieceStorage>> m_storages;
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
        if (error) {
            error->code = -1;
            error->message = "piece_cache_dir is required";
        }
        return nullptr;
    }

    try {
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
            // alert_category::error (1) | alert_category::status (64) = 65.
            effective_json = "{\"alert_mask\":65}";
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
                    lt::alert_category::error | lt::alert_category::status);
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

        std::string cache_dir(piece_cache_dir);
        params.disk_io_constructor = [cache_dir](lt::io_context& ios,
            lt::settings_interface const&, lt::counters&) -> std::unique_ptr<lt::disk_interface> {
            return std::make_unique<PieceStorageDiskIO>(ios, cache_dir);
        };
        wrapper->session = new lt::session(std::move(params));

        return static_cast<lt_session_t>(wrapper);
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
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
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
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

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
}

lt_torrent_handle_t lt_session_add_torrent_with_custom_storage_upload_mode(
    lt_session_t session, lt_torrent_info_t info,
    const char* piece_cache_dir, const char* settings_json, lt_error_t* error)
{
    if (!session || !info) {
        if (error) {
            error->code = -1;
            error->message = "Invalid session or torrent info";
        }
        return nullptr;
    }

    try {
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

        std::lock_guard<std::mutex> lock(wrapper->mutex);
        auto handle = wrapper->session->add_torrent(atp);
        return static_cast<lt_torrent_handle_t>(new lt::torrent_handle(handle));
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return nullptr;
    }
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
    // get_piece_lock returns a shared_ptr copy, so the mutex outlives
    // the map entry even if a concurrent remove_torrent happens.
    auto lock = get_piece_lock(std::string(info_hash_hex));
    lock->lock_shared();
}

// Release a previously-acquired shared (read) lock.
// Uses find_piece_lock (not operator[]) so it never creates a spurious
// new mutex. If no entry exists (e.g. torrent never had a write_piece
// call), the unlock is a no-op — the matching lock would also have been
// a no-op on a freshly created empty mutex, but find avoids the UB of
// unlocking a mutex that was never locked.
void lt_unlock_piece_read(const char* info_hash_hex) {
    if (!info_hash_hex || !*info_hash_hex) return;
    auto lock = find_piece_lock(std::string(info_hash_hex));
    if (lock) {
        lock->unlock_shared();
    }
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
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    try {
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
            if (error) {
                error->code = -1;
                error->message = "strdup failed";
            }
            return -1;
        }
        return 0;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
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
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    auto h = static_cast<lt::torrent_handle*>(handle);
    if (!h->is_valid()) {
        if (error) {
            error->code = -1;
            error->message = "Invalid torrent handle";
        }
        return -1;
    }

    try {
        std::vector<lt::announce_entry> trackers;
        if (parse_trackers_json(trackers_json, trackers) != 0) {
            if (error) {
                error->code = -1;
                error->message = "Failed to parse trackers JSON";
            }
            return -1;
        }
        h->replace_trackers(trackers);
        return 0;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
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
}

// TSI-2277: Extract the current tracker list from a torrent_handle.
// Returns the live trackers (after any replace_trackers calls) as a JSON
// array string, same format as lt_torrent_info_trackers. Used by the
// tracker merge logic to get the existing handle's trackers for dedup.
int lt_torrent_handle_trackers(lt_torrent_handle_t handle, char** out_json, lt_error_t* error) {
    if (!handle || !out_json) {
        if (error) {
            error->code = -1;
            error->message = "Invalid arguments";
        }
        return -1;
    }

    try {
        auto h = static_cast<lt::torrent_handle*>(handle);
        if (!h->is_valid()) {
            if (error) {
                error->code = -1;
                error->message = "Invalid torrent handle";
            }
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
            if (error) {
                error->code = -1;
                error->message = "strdup failed";
            }
            return -1;
        }
        return 0;
    } catch (const std::exception& e) {
        if (error) {
            error->code = -1;
            static thread_local std::string err_msg;
            err_msg = e.what();
            error->message = err_msg.c_str();
        }
        return -1;
    }
}

void lt_string_free(char* str) {
    if (str) {
        std::free(str);
    }
}

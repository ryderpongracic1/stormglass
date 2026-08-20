#include "checkpoint/writer.h"
#include "checkpoint/crc32c.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace stormglass {

namespace {

constexpr uint32_t kMagic = 0x4B434753;  // "SGCK" little-endian
constexpr uint32_t kVersion = 2;

// Header layout (32 bytes):
//   magic:        4 bytes
//   version:      4 bytes
//   offset:       8 bytes
//   watermark_ms: 8 bytes
//   num_entries:  8 bytes
constexpr size_t kHeaderSize = 32;

// Entry layout:
//   key_len:      4 bytes
//   key_data:     key_len bytes
//   window_start: 8 bytes
//   window_end:   8 bytes
//   pane_sum:     8 bytes
//   pane_count:   8 bytes

// Trailer layout (12 bytes):
//   entry_count:  8 bytes
//   crc32c:       4 bytes
constexpr size_t kTrailerSize = 12;

bool WriteAll(int fd, const void* data, size_t len) {
    auto* ptr = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < len) {
        auto n = ::write(fd, ptr + written, len - written);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        written += static_cast<size_t>(n);
    }
    return true;
}

void AppendLE32(std::vector<uint8_t>& buf, uint32_t v) {
    buf.push_back(static_cast<uint8_t>(v));
    buf.push_back(static_cast<uint8_t>(v >> 8));
    buf.push_back(static_cast<uint8_t>(v >> 16));
    buf.push_back(static_cast<uint8_t>(v >> 24));
}

void AppendLE64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) {
        buf.push_back(static_cast<uint8_t>(v >> (i * 8)));
    }
}

void AppendSLE64(std::vector<uint8_t>& buf, int64_t v) {
    AppendLE64(buf, static_cast<uint64_t>(v));
}

std::string CheckpointFilename(uint64_t offset) {
    char buf[64];
    std::snprintf(buf, sizeof(buf), "checkpoint-%020lu.ckpt", offset);
    return buf;
}

std::string TmpFilename(uint64_t offset) {
    return CheckpointFilename(offset) + ".tmp";
}

} // namespace

CheckpointWriter::CheckpointWriter(const std::string& checkpoint_dir)
    : dir_(checkpoint_dir) {}

bool CheckpointWriter::WriteCheckpoint(uint64_t offset, Timestamp watermark,
                                        const KeyedWindowState& state) {
    // Build the binary payload in memory
    std::vector<uint8_t> payload;
    payload.reserve(4096);

    // Header
    AppendLE32(payload, kMagic);
    AppendLE32(payload, kVersion);
    AppendLE64(payload, offset);
    AppendSLE64(payload, watermark.time_since_epoch().count());

    const uint64_t num_entries = state.TotalPanes();
    AppendLE64(payload, num_entries);

    // Body: iterate panes (order unspecified; the reader reconstructs a map)
    state.ForEachPane([&](const std::string& key, const Window& window, const Pane& pane) {
        uint32_t key_len = static_cast<uint32_t>(key.size());
        AppendLE32(payload, key_len);
        payload.insert(payload.end(), key.begin(), key.end());
        AppendSLE64(payload, window.start.time_since_epoch().count());
        AppendSLE64(payload, window.end.time_since_epoch().count());
        AppendSLE64(payload, pane.sum);
        AppendLE64(payload, pane.count);
    });

    // Fired windows section (version 2+)
    const auto& fired = state.FiredWindows();
    AppendLE64(payload, fired.size());
    for (const auto& w : fired) {
        AppendSLE64(payload, w.start.time_since_epoch().count());
        AppendSLE64(payload, w.end.time_since_epoch().count());
    }

    // Compute CRC over header + body + fired windows
    uint32_t crc = Crc32c(payload.data(), payload.size());

    // Trailer
    AppendLE64(payload, num_entries);  // redundant entry count
    AppendLE32(payload, crc);

    // Write atomically
    std::string tmp_path = dir_ + "/" + TmpFilename(offset);
    std::string final_path = dir_ + "/" + CheckpointFilename(offset);

    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) return false;

    if (!WriteAll(fd, payload.data(), payload.size())) {
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return false;
    }

    if (::fsync(fd) != 0) {
        ::close(fd);
        ::unlink(tmp_path.c_str());
        return false;
    }
    ::close(fd);

    // Atomic rename
    if (::rename(tmp_path.c_str(), final_path.c_str()) != 0) {
        ::unlink(tmp_path.c_str());
        return false;
    }

    // fsync the directory to persist the rename
    int dir_fd = ::open(dir_.c_str(), O_RDONLY | O_DIRECTORY);
    if (dir_fd >= 0) {
        ::fsync(dir_fd);
        ::close(dir_fd);
    }

    CleanOldCheckpoints(offset);
    return true;
}

void CheckpointWriter::CleanOldCheckpoints(uint64_t current_offset) {
    // List all .ckpt files, keep only the last 2
    DIR* dir = ::opendir(dir_.c_str());
    if (!dir) return;

    std::vector<std::string> ckpt_files;
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 5 && name.substr(name.size() - 5) == ".ckpt") {
            ckpt_files.push_back(name);
        }
    }
    ::closedir(dir);

    // Sort descending (newest first — zero-padded offset makes lex order work)
    std::sort(ckpt_files.begin(), ckpt_files.end(), std::greater<>());

    // Delete all but the newest 2
    for (size_t i = 2; i < ckpt_files.size(); ++i) {
        std::string path = dir_ + "/" + ckpt_files[i];
        ::unlink(path.c_str());
    }
}

} // namespace stormglass

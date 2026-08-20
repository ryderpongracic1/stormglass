#include "checkpoint/reader.h"
#include "checkpoint/crc32c.h"

#include <algorithm>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace stormglass {

namespace {

constexpr uint32_t kMagic = 0x4B434753;  // "SGCK" little-endian
constexpr uint32_t kMaxVersion = 2;
constexpr size_t kHeaderSize = 32;
constexpr size_t kTrailerSize = 12;

uint32_t ReadLE32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) |
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t ReadLE64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) {
        v |= static_cast<uint64_t>(p[i]) << (i * 8);
    }
    return v;
}

int64_t ReadSLE64(const uint8_t* p) {
    return static_cast<int64_t>(ReadLE64(p));
}

std::vector<uint8_t> ReadFile(const std::string& path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return {};

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        return {};
    }

    std::vector<uint8_t> data(static_cast<size_t>(st.st_size));
    size_t total_read = 0;
    while (total_read < data.size()) {
        auto n = ::read(fd, data.data() + total_read, data.size() - total_read);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            ::close(fd);
            return {};
        }
        total_read += static_cast<size_t>(n);
    }
    ::close(fd);
    return data;
}

} // namespace

CheckpointReader::CheckpointReader(const std::string& checkpoint_dir)
    : dir_(checkpoint_dir) {}

std::optional<CheckpointData> CheckpointReader::LoadLatest() {
    CleanTmpFiles();

    // List all .ckpt files
    DIR* dir = ::opendir(dir_.c_str());
    if (!dir) return std::nullopt;

    std::vector<std::string> ckpt_files;
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 5 && name.substr(name.size() - 5) == ".ckpt") {
            ckpt_files.push_back(name);
        }
    }
    ::closedir(dir);

    if (ckpt_files.empty()) return std::nullopt;

    // Sort descending (newest first — zero-padded offset makes lex order work)
    std::sort(ckpt_files.begin(), ckpt_files.end(), std::greater<>());

    // Try loading each until one passes CRC validation
    for (const auto& filename : ckpt_files) {
        auto result = TryLoad(dir_ + "/" + filename);
        if (result.has_value()) {
            return result;
        }
    }

    return std::nullopt;
}

std::vector<uint64_t> CheckpointReader::ValidOffsets() {
    // Deliberately does NOT clean .tmp files: a ".ckpt.tmp" name fails the
    // ".ckpt" suffix test below anyway, and leaving it untouched keeps this
    // call side-effect-free so it can poll a directory another process writes.
    std::vector<uint64_t> offsets;
    DIR* dir = ::opendir(dir_.c_str());
    if (!dir) return offsets;

    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 5 && name.substr(name.size() - 5) == ".ckpt") {
            auto data = TryLoad(dir_ + "/" + name);
            if (data.has_value()) offsets.push_back(data->offset);
        }
    }
    ::closedir(dir);
    return offsets;
}

std::optional<CheckpointData> CheckpointReader::LoadOffset(uint64_t offset) {
    DIR* dir = ::opendir(dir_.c_str());
    if (!dir) return std::nullopt;

    std::vector<std::string> names;
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 5 && name.substr(name.size() - 5) == ".ckpt") {
            names.push_back(std::move(name));
        }
    }
    ::closedir(dir);

    for (const auto& name : names) {
        auto data = TryLoad(dir_ + "/" + name);
        if (data.has_value() && data->offset == offset) {
            return data;
        }
    }
    return std::nullopt;
}

std::optional<CheckpointData> CheckpointReader::TryLoad(const std::string& path) {
    auto data = ReadFile(path);
    if (data.size() < kHeaderSize + kTrailerSize) {
        return std::nullopt;
    }

    // Validate trailer CRC
    size_t body_end = data.size() - kTrailerSize;
    uint32_t stored_crc = ReadLE32(data.data() + data.size() - 4);
    uint32_t computed_crc = Crc32c(data.data(), body_end);

    if (stored_crc != computed_crc) {
        return std::nullopt;
    }

    // Parse header
    const uint8_t* p = data.data();
    uint32_t magic = ReadLE32(p);
    if (magic != kMagic) return std::nullopt;

    uint32_t version = ReadLE32(p + 4);
    if (version == 0 || version > kMaxVersion) return std::nullopt;

    uint64_t offset = ReadLE64(p + 8);
    int64_t watermark_ms = ReadSLE64(p + 16);
    uint64_t num_entries = ReadLE64(p + 24);

    // Validate trailer entry count matches header
    uint64_t trailer_count = ReadLE64(data.data() + body_end);
    if (trailer_count != num_entries) {
        return std::nullopt;
    }

    // Parse body entries
    CheckpointData result;
    result.offset = offset;
    result.watermark = Timestamp{Duration{watermark_ms}};
    result.panes.reserve(num_entries);

    size_t pos = kHeaderSize;
    for (uint64_t i = 0; i < num_entries; ++i) {
        if (pos + 4 > body_end) return std::nullopt;
        uint32_t key_len = ReadLE32(data.data() + pos);
        pos += 4;

        if (pos + key_len + 32 > body_end) return std::nullopt;
        std::string key(reinterpret_cast<const char*>(data.data() + pos), key_len);
        pos += key_len;

        int64_t win_start = ReadSLE64(data.data() + pos); pos += 8;
        int64_t win_end = ReadSLE64(data.data() + pos); pos += 8;
        int64_t pane_sum = ReadSLE64(data.data() + pos); pos += 8;
        uint64_t pane_count = ReadLE64(data.data() + pos); pos += 8;

        result.panes.push_back(CheckpointData::PaneEntry{
            .key = std::move(key),
            .window = Window{Timestamp{Duration{win_start}}, Timestamp{Duration{win_end}}},
            .sum = pane_sum,
            .count = pane_count,
        });
    }

    // Version 2+: read fired windows section
    if (version >= 2) {
        if (pos + 8 > body_end) return std::nullopt;
        uint64_t num_fired = ReadLE64(data.data() + pos);
        pos += 8;
        result.fired_windows.reserve(num_fired);
        for (uint64_t i = 0; i < num_fired; ++i) {
            if (pos + 16 > body_end) return std::nullopt;
            int64_t start = ReadSLE64(data.data() + pos); pos += 8;
            int64_t end = ReadSLE64(data.data() + pos); pos += 8;
            result.fired_windows.push_back(Window{
                Timestamp{Duration{start}}, Timestamp{Duration{end}}});
        }
    }

    if (pos != body_end) return std::nullopt;  // Trailing garbage

    return result;
}

void CheckpointReader::CleanTmpFiles() {
    DIR* dir = ::opendir(dir_.c_str());
    if (!dir) return;

    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 4 && name.substr(name.size() - 4) == ".tmp") {
            std::string path = dir_ + "/" + name;
            ::unlink(path.c_str());
        }
    }
    ::closedir(dir);
}

} // namespace stormglass

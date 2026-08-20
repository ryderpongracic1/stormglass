#include "sink/durable_file_sink.h"

#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

namespace stormglass {

namespace {

bool WriteAll(int fd, const void* data, size_t len) {
    const auto* ptr = static_cast<const uint8_t*>(data);
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
    for (int i = 0; i < 4; ++i) buf.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

void AppendLE64(std::vector<uint8_t>& buf, uint64_t v) {
    for (int i = 0; i < 8; ++i) buf.push_back(static_cast<uint8_t>(v >> (i * 8)));
}

uint32_t ReadLE32(const uint8_t* p) {
    return static_cast<uint32_t>(p[0]) |
           (static_cast<uint32_t>(p[1]) << 8) |
           (static_cast<uint32_t>(p[2]) << 16) |
           (static_cast<uint32_t>(p[3]) << 24);
}

uint64_t ReadLE64(const uint8_t* p) {
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v |= static_cast<uint64_t>(p[i]) << (i * 8);
    return v;
}

} // namespace

DurableFileSink::DurableFileSink(const std::string& path) {
    fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
}

DurableFileSink::~DurableFileSink() {
    if (fd_ >= 0) ::close(fd_);
}

void DurableFileSink::Emit(const WindowResult& result) {
    if (fd_ < 0) return;

    std::vector<uint8_t> buf;
    buf.reserve(4 + result.key.size() + 32);
    AppendLE32(buf, static_cast<uint32_t>(result.key.size()));
    buf.insert(buf.end(), result.key.begin(), result.key.end());
    AppendLE64(buf, static_cast<uint64_t>(result.window.start.time_since_epoch().count()));
    AppendLE64(buf, static_cast<uint64_t>(result.window.end.time_since_epoch().count()));
    AppendLE64(buf, static_cast<uint64_t>(result.result.value));
    AppendLE64(buf, result.result.count);

    // A single write() places the bytes in the kernel page cache, which already
    // survives a process SIGKILL; fsync additionally hardens against machine
    // crash so the durability guarantee holds under either failure model.
    if (WriteAll(fd_, buf.data(), buf.size())) {
        ::fsync(fd_);
    }
}

void DurableFileSink::Flush() {
    if (fd_ >= 0) ::fsync(fd_);
}

std::vector<WindowResult> DurableFileSink::ReadAll(const std::string& path) {
    std::vector<WindowResult> results;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return results;

    struct stat st{};
    if (::fstat(fd, &st) != 0) {
        ::close(fd);
        return results;
    }

    std::vector<uint8_t> data(static_cast<size_t>(st.st_size));
    size_t total = 0;
    while (total < data.size()) {
        auto n = ::read(fd, data.data() + total, data.size() - total);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
            break;
        }
        total += static_cast<size_t>(n);
    }
    ::close(fd);
    data.resize(total);

    size_t pos = 0;
    while (pos + 4 <= data.size()) {
        uint32_t key_len = ReadLE32(data.data() + pos);
        // Full record = 4 (key_len) + key_len + 8+8+8+8 (start,end,sum,count).
        size_t record_size = 4 + static_cast<size_t>(key_len) + 32;
        if (pos + record_size > data.size()) {
            break;  // Torn trailing record — writer was killed mid-append.
        }

        size_t p = pos + 4;
        std::string key(reinterpret_cast<const char*>(data.data() + p), key_len);
        p += key_len;
        int64_t start = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
        int64_t end = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
        int64_t sum = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
        uint64_t count = ReadLE64(data.data() + p); p += 8;

        results.push_back(WindowResult{
            .key = std::move(key),
            .window = Window{Timestamp{Duration{start}}, Timestamp{Duration{end}}},
            .result = AggregateResult{sum, count},
        });

        pos += record_size;
    }

    return results;
}

} // namespace stormglass

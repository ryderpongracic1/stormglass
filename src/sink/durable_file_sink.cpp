#include "sink/durable_file_sink.h"

#include <cerrno>
#include <cstdint>
#include <fcntl.h>
#include <map>
#include <tuple>
#include <sys/stat.h>
#include <unistd.h>
#include <system_error>

namespace stormglass {

namespace {

bool WriteAll(int fd, const void* data, size_t len) {
    const auto* ptr = static_cast<const uint8_t*>(data);
    size_t written = 0;
    while (written < len) {
        auto n = ::write(fd, ptr + written, len - written);
        if (n <= 0) {
            if (n < 0 && errno == EINTR) continue;
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

// Parse complete records from `data`, stopping at a torn trailing record.
// Returns the byte length of the valid prefix.
size_t ParseRecords(const std::vector<uint8_t>& data, std::vector<WindowResult>* results) {
    size_t pos = 0;
    while (pos + 4 <= data.size()) {
        uint32_t key_len = ReadLE32(data.data() + pos);
        // Full record = 4 (key_len) + key_len + 8+8+8+8 (start,end,sum,count).
        size_t record_size = 4 + static_cast<size_t>(key_len) + 32;
        if (pos + record_size > data.size()) {
            break;  // Torn trailing record — writer was killed mid-append.
        }
        if (results) {
            size_t p = pos + 4;
            std::string key(reinterpret_cast<const char*>(data.data() + p), key_len);
            p += key_len;
            int64_t start = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
            int64_t end = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
            int64_t sum = static_cast<int64_t>(ReadLE64(data.data() + p)); p += 8;
            uint64_t count = ReadLE64(data.data() + p);

            results->push_back(WindowResult{
                .key = std::move(key),
                .window = Window{Timestamp{Duration{start}}, Timestamp{Duration{end}}},
                .result = AggregateResult{sum, count},
            });
        }
        pos += record_size;
    }
    return pos;
}

// Read the whole file behind `fd` from offset 0. Throws on read errors so a
// short read is never mistaken for a torn tail and truncated away.
std::vector<uint8_t> ReadFd(int fd) {
    struct stat st{};
    if (::fstat(fd, &st) != 0)
        throw std::system_error(errno, std::generic_category(), "stat sink");
    std::vector<uint8_t> data(static_cast<size_t>(st.st_size));
    size_t total = 0;
    while (total < data.size()) {
        auto n = ::pread(fd, data.data() + total, data.size() - total,
                         static_cast<off_t>(total));
        if (n < 0 && errno == EINTR) continue;
        if (n < 0) throw std::system_error(errno, std::generic_category(), "read sink");
        if (n == 0) break;
        total += static_cast<size_t>(n);
    }
    data.resize(total);
    return data;
}

} // namespace

DurableFileSink::DurableFileSink(const std::string& path, OpenMode mode) {
    if (mode == OpenMode::kTruncate) {
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open sink");
        return;
    }

    fd_ = ::open(path.c_str(), O_RDWR | O_CREAT | O_APPEND, 0644);
    if (fd_ < 0) throw std::system_error(errno, std::generic_category(), "open sink");
    try {
        const auto data = ReadFd(fd_);
        const size_t valid = ParseRecords(data, nullptr);
        if (valid != data.size()) {
            if (::ftruncate(fd_, static_cast<off_t>(valid)) != 0)
                throw std::system_error(errno, std::generic_category(), "truncate torn sink tail");
            Flush();
        }
    } catch (...) {
        ::close(fd_);
        fd_ = -1;
        throw;
    }
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
    if (!WriteAll(fd_, buf.data(), buf.size()))
        throw std::system_error(errno, std::generic_category(), "write sink");
    Flush();
}

void DurableFileSink::Flush() {
    if (fd_ >= 0 && ::fsync(fd_) != 0)
        throw std::system_error(errno, std::generic_category(), "fsync sink");
}

std::vector<WindowResult> DurableFileSink::ReadAll(const std::string& path) {
    std::vector<WindowResult> results;

    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0) return results;

    std::vector<uint8_t> data;
    try {
        data = ReadFd(fd);
    } catch (const std::system_error&) {
        ::close(fd);
        return results;
    }
    ::close(fd);

    ParseRecords(data, &results);
    return results;
}

std::vector<WindowResult> DurableFileSink::ReadLatest(const std::string& path) {
    using Slot = std::tuple<int64_t, std::string, int64_t>;  // start, key, end
    std::map<Slot, WindowResult> latest;
    for (auto& r : ReadAll(path)) {
        Slot slot{r.window.start.time_since_epoch().count(), r.key,
                  r.window.end.time_since_epoch().count()};
        latest.insert_or_assign(std::move(slot), std::move(r));
    }
    std::vector<WindowResult> out;
    out.reserve(latest.size());
    for (auto& [slot, r] : latest) out.push_back(std::move(r));
    return out;
}

} // namespace stormglass

#include "snapshot/store.h"
#include "snapshot/codec.h"
#include "checkpoint/crc32c.h"
#include <cerrno>
#include <charconv>
#include <algorithm>
#include <filesystem>
#include <fstream>
#include <fcntl.h>
#include <unistd.h>
#include <system_error>

namespace stormglass::snapshot {
namespace {
constexpr std::size_t kLimit = 64 * 1024 * 1024;
[[noreturn]] void Error(const char *what) {
    throw std::system_error(errno, std::generic_category(), what);
}
uint32_t Crc(const Bytes &b) { return Crc32c(b.data(), b.size()); }
void Seal(Encoder &e) {
    e.U32(Crc(e.bytes));
    if (e.bytes.size() > kLimit)
        throw std::runtime_error("snapshot file exceeds limit");
}
Decoder Open(const Bytes &bytes) {
    if (bytes.size() < 4 || bytes.size() > kLimit)
        throw std::runtime_error("invalid snapshot file size");
    Decoder tail{std::span<const uint8_t>{bytes}.last(4)};
    if (Crc32c(bytes.data(), bytes.size() - 4) != tail.U32())
        throw std::runtime_error("snapshot file CRC mismatch");
    return Decoder(std::span<const uint8_t>{bytes}.first(bytes.size() - 4));
}
std::filesystem::path Epoch(const std::string &root, uint64_t epoch) {
    if (!epoch)
        throw std::runtime_error("zero snapshot epoch");
    return std::filesystem::path(root) / ("epoch-" + std::to_string(epoch));
}
std::filesystem::path Local(const std::filesystem::path &dir, NodeId node) {
    return dir / ("node-" + std::to_string(node) + ".snapshot");
}
Bytes Read(const std::filesystem::path &path) {
    std::ifstream in(path, std::ios::binary | std::ios::ate);
    if (!in)
        throw std::runtime_error("snapshot file missing: " + path.string());
    auto size = in.tellg();
    if (size < 0 || static_cast<uint64_t>(size) > kLimit)
        throw std::runtime_error("snapshot file too large");
    Bytes b(static_cast<std::size_t>(size));
    in.seekg(0);
    in.read(reinterpret_cast<char *>(b.data()), static_cast<std::streamsize>(b.size()));
    if (!in)
        throw std::runtime_error("snapshot read failed");
    return b;
}
void SyncDirectory(const std::filesystem::path &path) {
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
        Error("open snapshot directory");
    int rc = ::fsync(fd);
    int saved = errno;
    ::close(fd);
    if (rc < 0) {
        errno = saved;
        Error("fsync snapshot directory");
    }
}
void EnsureDirectory(const std::filesystem::path &path) {
    if (path.empty() || std::filesystem::exists(path))
        return;
    EnsureDirectory(path.parent_path());
    std::filesystem::create_directory(path);
    SyncDirectory(path);
    SyncDirectory(path.parent_path().empty() ? std::filesystem::path(".") : path.parent_path());
}
void AtomicWrite(const std::filesystem::path &path, const Bytes &bytes) {
    EnsureDirectory(path.parent_path());
    if (std::filesystem::exists(path)) {
        if (Read(path) != bytes)
            throw std::runtime_error("attempt to overwrite immutable snapshot");
        return;
    }
    auto name = path.string() + ".tmp-XXXXXX";
    std::vector<char> temporary(name.begin(), name.end());
    temporary.push_back(0);
    int fd = ::mkstemp(temporary.data());
    if (fd < 0)
        Error("snapshot mkstemp");
    try {
        std::size_t offset = 0;
        while (offset < bytes.size()) {
            auto n = ::write(fd, bytes.data() + offset, bytes.size() - offset);
            if (n < 0) {
                if (errno == EINTR)
                    continue;
                Error("snapshot write");
            }
            if (n == 0)
                throw std::runtime_error("zero snapshot write");
            offset += n;
        }
        if (::fsync(fd) < 0)
            Error("snapshot fsync");
        if (::close(fd) < 0) {
            fd = -1;
            Error("snapshot close");
        }
        fd = -1;
        // link publishes without replacing another immutable writer's file.
        if (::link(temporary.data(), path.c_str()) < 0)
            Error("snapshot publish");
        if (::unlink(temporary.data()) < 0)
            Error("snapshot temporary unlink");
        SyncDirectory(path.parent_path());
    } catch (...) {
        if (fd >= 0)
            ::close(fd);
        ::unlink(temporary.data());
        throw;
    }
}
void Map(Encoder &e, const std::map<NodeId, uint64_t> &map) {
    e.U32(static_cast<uint32_t>(map.size()));
    for (auto [id, n] : map) {
        e.U32(id);
        e.U64(n);
    }
}
std::map<NodeId, uint64_t> Map(Decoder &d) {
    std::map<NodeId, uint64_t> m;
    auto count = d.Count(12);
    for (uint32_t i = 0; i < count; ++i) {
        auto id = d.U32();
        auto n = d.U64();
        if (!m.emplace(id, n).second)
            throw std::runtime_error("duplicate serialized peer");
    }
    return m;
}
Bytes Manifest(uint64_t epoch, const Topology &topology, const std::string &config,
               const std::map<NodeId, uint32_t> &crcs) {
    if (config.empty() || topology.empty() || topology.size() > 1024)
        throw std::runtime_error("invalid global snapshot config/topology");
    Encoder e;
    e.U32(0x53474d31);
    e.U64(epoch);
    e.String(config);
    e.U32(static_cast<uint32_t>(topology.size()));
    for (const auto &[id, peers] : topology) {
        e.U32(id);
        e.U32(static_cast<uint32_t>(peers.size()));
        for (auto peer : peers)
            e.U32(peer);
        e.U32(crcs.at(id));
    }
    Seal(e);
    return std::move(e.bytes);
}
std::vector<LocalSnapshot> LoadLocals(const std::filesystem::path &dir, uint64_t epoch,
                                      const Topology &topology, std::map<NodeId, uint32_t> &crcs) {
    if (topology.empty() || topology.size() > 1024)
        throw std::runtime_error("invalid snapshot topology size");
    std::vector<LocalSnapshot> snapshots;
    std::size_t total = 0;
    for (auto &[id, peers] : topology) {
        (void)peers;
        auto b = Read(Local(dir, id));
        total += b.size();
        if (total > 512 * 1024 * 1024)
            throw std::runtime_error("global snapshot exceeds load limit");
        // Hash the protected payload, not payload+CRC (which has a fixed CRC residue).
        auto s = Store::DecodeLocal(b);
        crcs.emplace(id, Crc32c(b.data(), b.size() - 4));
        if (s.node != id || s.epoch != epoch)
            throw std::runtime_error("snapshot identity mismatch");
        snapshots.push_back(std::move(s));
    }
    ValidateGlobal(snapshots, topology);
    return snapshots;
}
} // namespace
Bytes Store::EncodeLocal(const LocalSnapshot &s) {
    Encoder e;
    e.U32(0x53474c31);
    e.U32(s.node);
    e.U64(s.epoch);
    e.Blob(s.state);
    Map(e, s.received);
    Map(e, s.sent);
    Map(e, s.marker_sequences);
    e.U32(static_cast<uint32_t>(s.channels.size()));
    for (const auto &r : s.channels) {
        e.U32(r.peer);
        e.U64(r.sequence);
        e.Blob(r.payload);
    }
    Seal(e);
    return std::move(e.bytes);
}
LocalSnapshot Store::DecodeLocal(const Bytes &bytes) {
    auto d = Open(bytes);
    if (d.U32() != 0x53474c31)
        throw std::runtime_error("snapshot format mismatch");
    LocalSnapshot s;
    s.node = d.U32();
    s.epoch = d.U64();
    if (!s.epoch)
        throw std::runtime_error("zero local epoch");
    s.state = d.Blob();
    s.received = Map(d);
    s.sent = Map(d);
    s.marker_sequences = Map(d);
    auto n = d.Count(16);
    for (uint32_t i = 0; i < n; ++i) {
        ChannelRecord r;
        r.peer = d.U32();
        r.sequence = d.U64();
        r.payload = d.Blob();
        s.channels.push_back(std::move(r));
    }
    d.End();
    return s;
}
void Store::SaveLocal(const LocalSnapshot &s) const {
    AtomicWrite(Local(Epoch(root_, s.epoch), s.node), EncodeLocal(s));
}
void Store::Commit(uint64_t epoch, const Topology &topology,
                   const std::string &configuration) const {
    auto dir = Epoch(root_, epoch);
    std::map<NodeId, uint32_t> crcs;
    LoadLocals(dir, epoch, topology, crcs);
    AtomicWrite(dir / "COMMITTED", Manifest(epoch, topology, configuration, crcs));
}
std::vector<LocalSnapshot> Store::LoadCommitted(uint64_t epoch, const Topology &topology,
                                                const std::string &configuration) const {
    auto dir = Epoch(root_, epoch);
    auto manifest = Read(dir / "COMMITTED");
    auto verified = Open(manifest);
    (void)verified;
    std::map<NodeId, uint32_t> crcs;
    auto snapshots = LoadLocals(dir, epoch, topology, crcs);
    if (manifest != Manifest(epoch, topology, configuration, crcs))
        throw std::runtime_error("committed manifest/configuration mismatch");
    return snapshots;
}
std::vector<LocalSnapshot> Store::LoadLatestCommitted(const Topology &topology,
                                                      const std::string &configuration) const {
    std::vector<uint64_t> epochs;
    if (std::filesystem::exists(root_))
        for (const auto &entry : std::filesystem::directory_iterator(root_)) {
            if (!entry.is_directory())
                continue;
            auto name = entry.path().filename().string();
            if (!name.starts_with("epoch-"))
                continue;
            uint64_t epoch = 0;
            auto first = name.data() + 6;
            auto last = name.data() + name.size();
            auto result = std::from_chars(first, last, epoch);
            if (result.ec == std::errc{} && result.ptr == last && epoch > 0)
                epochs.push_back(epoch);
        }
    std::sort(epochs.rbegin(), epochs.rend());
    for (auto epoch : epochs) {
        try {
            return LoadCommitted(epoch, topology, configuration);
        } catch (const std::runtime_error &) {
        }
    }
    throw std::runtime_error("no valid committed snapshot for requested topology/configuration");
}
} // namespace stormglass::snapshot

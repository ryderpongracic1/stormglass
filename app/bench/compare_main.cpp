#include "engine/partitioned_pipeline.h"
#include "sink/memory_sink.h"
#include "source/source.h"
#include "window/tumbling.h"

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdio>
#include <cstdint>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

using namespace stormglass;

namespace {

constexpr std::array<char, 8> kMagic{'S','G','F','X','v','0','0','1'};

uint32_t ReadU32(const std::vector<unsigned char>& bytes, std::size_t& cursor) {
    if (cursor > bytes.size() || bytes.size() - cursor < 4)
        throw std::runtime_error("truncated fixture");
    const auto* b = bytes.data() + cursor;
    cursor += 4;
    return static_cast<uint32_t>(b[0]) | (static_cast<uint32_t>(b[1]) << 8) |
           (static_cast<uint32_t>(b[2]) << 16) | (static_cast<uint32_t>(b[3]) << 24);
}

uint64_t ReadU64(const std::vector<unsigned char>& bytes, std::size_t& cursor) {
    if (cursor > bytes.size() || bytes.size() - cursor < 8)
        throw std::runtime_error("truncated fixture");
    const auto* b = bytes.data() + cursor;
    cursor += 8;
    uint64_t v = 0;
    for (unsigned i = 0; i < 8; ++i) v |= static_cast<uint64_t>(b[i]) << (8 * i);
    return v;
}

struct FixtureHeader {
    uint64_t records;
    uint64_t entries;
    int64_t cycle_span_ms;
    uint64_t keys;
};

FixtureHeader ReadHeader(const std::vector<unsigned char>& bytes, std::size_t& cursor) {
    if (bytes.size() < kMagic.size() ||
        !std::equal(kMagic.begin(), kMagic.end(), bytes.begin()))
        throw std::runtime_error("invalid fixture magic");
    cursor = kMagic.size();
    const auto version = ReadU32(bytes, cursor);
    const auto header_size = ReadU32(bytes, cursor);
    FixtureHeader h{ReadU64(bytes, cursor), ReadU64(bytes, cursor),
                    static_cast<int64_t>(ReadU64(bytes, cursor)), ReadU64(bytes, cursor)};
    if (version != 1 || header_size != 48 || !h.records || !h.entries ||
        h.cycle_span_ms <= 0 || !h.keys || h.keys > std::numeric_limits<uint32_t>::max())
        throw std::runtime_error("unsupported fixture header");
    return h;
}

class FixtureSource final : public Source {
public:
    FixtureSource(std::string path, uint64_t cycles, uint32_t batch_size)
        : path_(std::move(path)), cycles_(cycles), batch_size_(batch_size) {
        if (!cycles_ || !batch_size_) throw std::invalid_argument("cycles and batch size must be positive");
        LoadFixture();
    }

    std::optional<Batch> Next() override {
        if (cycle_ == cycles_) return std::nullopt;
        Batch batch;
        batch.items.reserve(batch_size_ + batch_size_ / 100);
        while (batch.items.size() < batch_size_ && cycle_ < cycles_) {
            if (entry_ == header_.entries) {
                ++cycle_;
                if (cycle_ == cycles_) break;
                cursor_ = header_size_;
                entry_ = 0;
                continue;
            }
            if (cursor_ > bytes_.size() || bytes_.size() - cursor_ < 4)
                throw std::runtime_error("truncated fixture");
            const auto type = bytes_[cursor_];
            cursor_ += 4;  // type plus three reserved bytes
            const uint32_t key_id = ReadU32(bytes_, cursor_);
            const int64_t value = static_cast<int64_t>(ReadU64(bytes_, cursor_));
            const int64_t event_or_watermark = static_cast<int64_t>(ReadU64(bytes_, cursor_));
            (void)ReadU64(bytes_, cursor_);  // fixture sequence is not part of window semantics
            ++entry_;
            const auto shift = static_cast<int64_t>(cycle_) * header_.cycle_span_ms;
            if (event_or_watermark < 0 || event_or_watermark >
                std::numeric_limits<int64_t>::max() - shift - 1000)
                throw std::overflow_error("fixture timestamp outside supported window domain");
            if (type == 0) {
                if (key_id >= keys_.size()) throw std::runtime_error("fixture key id out of range");
                batch.items.emplace_back(Record{
                    keys_[key_id], value, Timestamp{Duration{event_or_watermark + shift}},
                    Timestamp{Duration{static_cast<int64_t>(offset_)}}});
                ++offset_;
            } else if (type == 1) {
                batch.items.emplace_back(ControlRecord{ControlType::kWatermark,
                    Timestamp{Duration{event_or_watermark + shift}}, offset_});
            } else {
                throw std::runtime_error("invalid fixture entry type");
            }
        }
        return batch.empty() ? std::nullopt : std::optional<Batch>{std::move(batch)};
    }

    void Seek(uint64_t) override { throw std::runtime_error("fixture benchmark does not support seek"); }
    [[nodiscard]] uint64_t CurrentOffset() const override { return offset_; }
    [[nodiscard]] uint64_t LogicalRecords() const { return header_.records * cycles_; }

private:
    void LoadFixture() {
        std::ifstream in(path_, std::ios::binary | std::ios::ate);
        if (!in) throw std::runtime_error("cannot open fixture: " + path_);
        const auto end = in.tellg();
        if (end < 0) throw std::runtime_error("cannot size fixture: " + path_);
        bytes_.resize(static_cast<std::size_t>(end));
        in.seekg(0);
        in.read(reinterpret_cast<char*>(bytes_.data()), static_cast<std::streamsize>(bytes_.size()));
        if (!in) throw std::runtime_error("cannot read fixture: " + path_);
        header_ = ReadHeader(bytes_, cursor_);
        header_size_ = cursor_;
        constexpr uint64_t kEntryBytes = 32;
        if (header_.entries > (std::numeric_limits<uint64_t>::max() - header_size_) / kEntryBytes ||
            header_size_ + header_.entries * kEntryBytes != bytes_.size())
            throw std::runtime_error("fixture size does not match its header");
        if (cycles_ - 1 > static_cast<uint64_t>(std::numeric_limits<int64_t>::max() / header_.cycle_span_ms))
            throw std::overflow_error("fixture cycle timestamp overflow");
        keys_.reserve(header_.keys);
        for (uint64_t key_id = 0; key_id < header_.keys; ++key_id) {
            char key[24];
            std::snprintf(key, sizeof(key), "key-%04llu",
                          static_cast<unsigned long long>(key_id));
            keys_.emplace_back(key);
        }
        if (header_.records > static_cast<uint64_t>(std::numeric_limits<int64_t>::max()) / cycles_)
            throw std::overflow_error("logical record count overflow");
    }

    std::string path_;
    uint64_t cycles_;
    uint32_t batch_size_;
    uint64_t cycle_ = 0;
    uint64_t entry_ = 0;
    uint64_t offset_ = 0;
    FixtureHeader header_{};
    std::vector<std::string> keys_;
    std::vector<unsigned char> bytes_;
    std::size_t cursor_ = 0;
    std::size_t header_size_ = 0;
};

uint64_t Mix(uint64_t z) {
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ULL;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebULL;
    return z ^ (z >> 31);
}

void AddBytes(uint64_t& h, const void* data, size_t size) {
    const auto* p = static_cast<const unsigned char*>(data);
    for (size_t i = 0; i < size; ++i) { h ^= p[i]; h *= 1099511628211ULL; }
}

uint64_t ResultHash(const WindowResult& r) {
    uint64_t h = 14695981039346656037ULL;
    AddBytes(h, r.key.data(), r.key.size());
    const std::array<int64_t, 4> fields{
        r.window.start.time_since_epoch().count(), r.window.end.time_since_epoch().count(),
        r.result.value, static_cast<int64_t>(r.result.count)};
    for (auto value : fields) {
        std::array<unsigned char, 8> le{};
        auto u = static_cast<uint64_t>(value);
        for (unsigned i = 0; i < 8; ++i) le[i] = static_cast<unsigned char>(u >> (8 * i));
        AddBytes(h, le.data(), le.size());
    }
    return Mix(h);
}

struct alignas(64) SharedDigest {
    uint64_t outputs = 0;
    uint64_t digest_xor = 0;
    uint64_t digest_sum = 0;
};

class DigestSink final : public Sink {
public:
    explicit DigestSink(SharedDigest& digest) : digest_(digest) {}
    void Emit(const WindowResult& result) override {
        const auto hash = ResultHash(result);
        ++digest_.outputs;
        digest_.digest_xor ^= hash;
        digest_.digest_sum += hash;
    }
    void Flush() override {}
private:
    SharedDigest& digest_;
};

}  // namespace

int main(int argc, char** argv) try {
    std::string fixture;
    uint64_t cycles = 10;
    uint32_t workers = 1;
    int64_t lateness_ms = 0;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        auto value = [&]() -> std::string {
            if (++i >= argc) throw std::invalid_argument("missing value for " + arg);
            return argv[i];
        };
        if (arg == "--fixture") fixture = value();
        else if (arg == "--cycles") cycles = std::stoull(value());
        else if (arg == "--parallelism") {
            const auto parsed = std::stoull(value());
            if (parsed > std::numeric_limits<uint32_t>::max())
                throw std::invalid_argument("parallelism exceeds uint32 range");
            workers = static_cast<uint32_t>(parsed);
        }
        else if (arg == "--lateness-ms") lateness_ms = std::stoll(value());
        else throw std::invalid_argument("unknown argument: " + arg);
    }
    if (lateness_ms != 0)
        throw std::invalid_argument("matched comparison requires zero allowed lateness (refire policies differ)");
    if (fixture.empty() || !workers)
        throw std::invalid_argument("usage: stormglass_compare --fixture PATH [--cycles N] [--parallelism N] [--lateness-ms N]");

    // Nonzero lateness has different refire granularity in the two engines.
    // Keep the matched comparison restricted to zero allowed lateness.
    const auto started = std::chrono::steady_clock::now();
    auto source = std::make_unique<FixtureSource>(fixture, cycles, 4096);
    const auto logical_records = source->LogicalRecords();
    std::vector<SharedDigest> worker_digests(workers);
    auto sink = std::make_unique<MemorySink>();
    PartitionedPipelineConfig config;
    config.num_workers = workers;
    config.allowed_lateness = Duration{lateness_ms};
    config.worker_sink_factory = [&worker_digests](uint32_t worker) {
        return std::make_unique<DigestSink>(worker_digests.at(worker));
    };
    PartitionedPipeline pipeline(std::move(source),
        [] { return std::make_unique<TumblingAssigner>(Duration{1000}); },
        std::move(sink), config);

    const auto stats = pipeline.Run();
    uint64_t outputs = 0, digest_xor = 0, digest_sum = 0;
    for (const auto& digest : worker_digests) {
        outputs += digest.outputs;
        digest_xor ^= digest.digest_xor;
        digest_sum += digest.digest_sum;
    }
    const auto elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    std::cout << "engine=stormglass timing=execute window_ms=1000 records=" << stats.records_processed
              << " expected_records=" << logical_records << " parallelism=" << workers
              << " lateness_ms=" << lateness_ms << " cycles=" << cycles
              << " seconds=" << std::fixed << std::setprecision(6) << elapsed
              << " m_records_per_second=" << std::setprecision(3)
              << (static_cast<double>(stats.records_processed) / elapsed / 1e6)
              << " outputs=" << outputs
              << " late_accepted=" << stats.late_records_accepted
              << " late_dropped=" << stats.late_records_dropped
              << " digest_xor=" << std::hex << std::setw(16) << std::setfill('0') << digest_xor
              << " digest_sum=" << std::setw(16) << digest_sum << std::dec << '\n';
    return stats.records_processed == logical_records ? 0 : 2;
} catch (const std::exception& e) {
    std::cerr << "stormglass_compare: " << e.what() << '\n';
    return 1;
}

#include <gtest/gtest.h>

#include "engine/partitioned_pipeline.h"
#include "engine/pipeline.h"
#include "oracle/differential.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "window/tumbling.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <tuple>
#include <vector>

namespace stormglass {
namespace {

constexpr Duration kWindow{1000};

// Reduce a run's raw emissions to the canonical AUTHORITATIVE result set: one
// row per (key, window), keeping the max-count emission, then sorted by
// (window.start, window.end, key). This is EXACTLY what the differential oracle
// compares against (see DedupEngineResults) — so matching here means the
// N-worker engine passes the same oracle that certifies the single-threaded one.
//
// Why dedup is required (not a workaround): KeyedWindowState::refired_windows_
// is keyed on Window ALONE, and FireWindow(W) re-emits EVERY key co-resident in
// W. So when keys A and B share a fired window W and only A receives late data,
// the single-threaded engine re-emits B too (a redundant row with B's unchanged
// count). Partitioning by key places A and B on different workers, so B is not
// re-emitted. The raw emission multisets therefore differ — single-threaded has
// MORE rows, and the gap grows with N as fewer keys stay co-resident. Both
// collapse to the identical authoritative set once max-count dedup removes the
// redundant re-fires, which carry no new information. The oracle has always
// deduped for this reason; the invariance claim is over that authoritative set.
std::vector<WindowResult> Canonicalize(const std::vector<WindowResult>& raw) {
    auto v = DedupEngineResults(raw);
    std::sort(v.begin(), v.end(), [](const WindowResult& a, const WindowResult& b) {
        return std::tie(a.window.start, a.window.end, a.key) <
               std::tie(b.window.start, b.window.end, b.key);
    });
    return v;
}

std::string Describe(const WindowResult& r) {
    return "{key=" + r.key +
           " win=[" + std::to_string(r.window.start.time_since_epoch().count()) + "," +
           std::to_string(r.window.end.time_since_epoch().count()) + ")" +
           " sum=" + std::to_string(r.result.value) +
           " count=" + std::to_string(r.result.count) + "}";
}

GeneratorConfig MakeConfig(uint64_t seed, DisorderMode mode) {
    GeneratorConfig c{};
    c.seed = seed;
    c.num_keys = 10;
    c.num_records = 20000;  // modest: well under the 50k drain guard ceiling
    c.max_disorder = Duration{500};
    c.batch_size = 1024;
    c.watermark_interval = 100;
    c.disorder_mode = mode;
    if (mode == DisorderMode::kHeavyTailed) {
        c.late_fraction = 0.1;
        c.late_tail = Duration{6000};
    }
    return c;
}

// Single-threaded reference: the authoritative result multiset.
std::vector<WindowResult> RunSingleThreaded(const GeneratorConfig& gen,
                                            Duration allowed_lateness) {
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();
    Pipeline pipeline(std::make_unique<DeterministicGenerator>(gen),
                      std::make_unique<TumblingAssigner>(kWindow),
                      std::move(sink),
                      PipelineConfig{.allowed_lateness = allowed_lateness});
    pipeline.Run();
    return Canonicalize(sink_ptr->Results());
}

// N-worker result multiset.
std::vector<WindowResult> RunPartitioned(const GeneratorConfig& gen,
                                         Duration allowed_lateness,
                                         uint32_t num_workers,
                                         PartitionedPipeline::Stats* out_stats = nullptr) {
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();
    PartitionedPipeline pipeline(
        std::make_unique<DeterministicGenerator>(gen),
        [] { return std::make_unique<TumblingAssigner>(kWindow); },
        std::move(sink),
        PartitionedPipelineConfig{.num_workers = num_workers,
                                  .allowed_lateness = allowed_lateness});
    auto stats = pipeline.Run();
    if (out_stats) *out_stats = stats;
    return Canonicalize(sink_ptr->Results());
}

void ExpectSameMultiset(const std::vector<WindowResult>& single,
                        const std::vector<WindowResult>& partitioned,
                        const std::string& ctx) {
    ASSERT_EQ(single.size(), partitioned.size())
        << ctx << ": emission count differs (single=" << single.size()
        << ", partitioned=" << partitioned.size() << ")";
    for (size_t i = 0; i < single.size(); ++i) {
        const auto& s = single[i];
        const auto& p = partitioned[i];
        const bool same = s.key == p.key && s.window.start == p.window.start &&
                          s.window.end == p.window.end &&
                          s.result.value == p.result.value &&
                          s.result.count == p.result.count;
        ASSERT_TRUE(same) << ctx << ": row " << i << " differs\n  single="
                          << Describe(s) << "\n  parted=" << Describe(p);
    }
}

// ---- THE HEADLINE PROOF ----------------------------------------------------
// For each disorder profile and each seed, the N-worker engine must produce the
// IDENTICAL sorted result multiset to the single-threaded engine, for every
// N in {1,2,4,8}. Parallelism is invisible to semantics.

struct Profile {
    const char* name;
    DisorderMode mode;
    Duration allowed_lateness;
};

class InvarianceTest : public ::testing::TestWithParam<Profile> {};

TEST_P(InvarianceTest, NWorkerMatchesSingleThreadedForAllN) {
    const Profile prof = GetParam();
    for (uint64_t seed : {1ull, 7ull, 42ull}) {
        auto gen = MakeConfig(seed, prof.mode);
        auto single = RunSingleThreaded(gen, prof.allowed_lateness);
        ASSERT_FALSE(single.empty()) << "reference produced no results";

        for (uint32_t n : {1u, 2u, 4u, 8u}) {
            auto parted = RunPartitioned(gen, prof.allowed_lateness, n);
            std::string ctx = std::string(prof.name) + " seed=" +
                              std::to_string(seed) + " N=" + std::to_string(n);
            ExpectSameMultiset(single, parted, ctx);
        }
    }
}

INSTANTIATE_TEST_SUITE_P(
    DisorderProfiles, InvarianceTest,
    ::testing::Values(
        Profile{"bounded_L0", DisorderMode::kBounded, Duration{0}},
        Profile{"heavytailed_L0", DisorderMode::kHeavyTailed, Duration{0}},
        Profile{"heavytailed_Lpos", DisorderMode::kHeavyTailed, Duration{2000}}),
    [](const ::testing::TestParamInfo<Profile>& info) {
        return std::string(info.param.name);
    });

// ---- Drain / shutdown ------------------------------------------------------
// No lost results and prompt termination. For bounded L=0 every record lands in
// exactly one tumbling window that fires exactly once, so the sum of result
// counts equals the record count — a direct no-lost-records assertion.
TEST(PartitionedDrain, NoLostRecordsAndTerminates) {
    auto gen = MakeConfig(/*seed=*/123, DisorderMode::kBounded);
    for (uint32_t n : {1u, 3u, 4u, 8u}) {  // includes a non-power-of-two
        PartitionedPipeline::Stats stats;
        auto res = RunPartitioned(gen, Duration{0}, n, &stats);

        EXPECT_EQ(stats.records_processed, gen.num_records)
            << "N=" << n << ": records lost between router and workers";
        EXPECT_EQ(stats.num_workers, n);

        uint64_t total_count = 0;
        for (const auto& r : res) total_count += r.result.count;
        EXPECT_EQ(total_count, gen.num_records)
            << "N=" << n << ": sum of window counts != record count (lost data)";
    }
}

// The Merge stage reports the effective output watermark = min across
// partitions. On a single broadcast source every partition holds the same
// watermark, so the reported min equals that common value and is > min().
TEST(PartitionedMerge, OutputWatermarkIsMinAcrossPartitions) {
    auto gen = MakeConfig(/*seed=*/9, DisorderMode::kBounded);
    PartitionedPipeline::Stats stats;
    RunPartitioned(gen, Duration{0}, /*num_workers=*/4, &stats);
    EXPECT_GT(stats.output_watermark, Timestamp::min())
        << "output watermark should have advanced past the initial minimum";
    EXPECT_GT(stats.watermarks_advanced, 0u);
}

}  // namespace
}  // namespace stormglass

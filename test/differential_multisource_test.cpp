#include <gtest/gtest.h>

#include "oracle/differential.h"

#include <cstdint>
#include <string>
#include <tuple>

// The v3 multi-source proof, driven through the SHARED differential harness.
//
// source_merge_test.cpp proves the min-combine machinery directly (running min,
// lagging source holds the min back, Seek determinism, K=1 passthrough). This
// file closes the loop end-to-end: the engine, driven over a SourceMerge of K
// generators with DIVERGENT event-time rates, matches the UNCHANGED oracle fed
// the SAME merged stream — for K in {1,2,3}, across disorder/lateness profiles,
// on both the single-threaded Pipeline and the PartitionedPipeline. K=1 is shown
// to reduce EXACTLY to the single-source path.

namespace stormglass {
namespace {

struct Profile {
    const char* name;
    DisorderMode mode;
    Duration allowed_lateness;
};

DifferentialConfig MakeConfig(const Profile& prof, uint32_t k, uint32_t workers) {
    DifferentialConfig c{};
    c.num_seeds = 3;
    c.records_per_seed = 4000;     // per source; merged stream ~ k * this
    c.num_keys = 10;
    c.window_size = Duration{1000};
    c.max_disorder = Duration{500};
    c.allowed_lateness = prof.allowed_lateness;
    c.disorder_mode = prof.mode;
    if (prof.mode == DisorderMode::kHeavyTailed) {
        c.late_fraction = 0.1;
        c.late_tail = Duration{6000};
    }
    c.seed_start = 1;
    c.sources = k;
    c.workers = workers;
    return c;
}

// K in {1,2,3} x {bounded L=0, heavy-tailed L=0, heavy-tailed L>0}, single-
// threaded engine over the merged stream.
class MultiSourceDifferential
    : public ::testing::TestWithParam<std::tuple<Profile, uint32_t>> {};

TEST_P(MultiSourceDifferential, EngineMatchesOracleOverMergedStream) {
    const auto& [prof, k] = GetParam();
    auto result = RunMultiSourceDifferential(MakeConfig(prof, k, /*workers=*/1));

    EXPECT_EQ(result.seeds_tested, 3u);
    EXPECT_EQ(result.seeds_passed, 3u)
        << prof.name << " K=" << k << " first failure: " << result.failure_detail;
    EXPECT_EQ(result.seeds_failed, 0u);
    EXPECT_TRUE(result.failure_detail.empty()) << result.failure_detail;
    // Drop-count contract holds in aggregate too.
    EXPECT_EQ(result.engine_late_dropped, result.oracle_predicted_drops)
        << prof.name << " K=" << k << ": engine/oracle drop counts diverged";
}

INSTANTIATE_TEST_SUITE_P(
    Profiles, MultiSourceDifferential,
    ::testing::Combine(
        ::testing::Values(
            Profile{"bounded_L0", DisorderMode::kBounded, Duration{0}},
            Profile{"heavytailed_L0", DisorderMode::kHeavyTailed, Duration{0}},
            Profile{"heavytailed_Lpos", DisorderMode::kHeavyTailed, Duration{2000}}),
        ::testing::Values(1u, 2u, 3u)),
    [](const ::testing::TestParamInfo<std::tuple<Profile, uint32_t>>& info) {
        return std::string(std::get<0>(info.param).name) + "_K" +
               std::to_string(std::get<1>(info.param));
    });

// SourceMerge is "just a Source", so it drives the PartitionedPipeline unchanged.
// Multi-source over N=4 workers must still match the oracle fed the merged stream.
TEST(MultiSourcePartitioned, DrivesPartitionedPipelineAcrossProfiles) {
    const Profile profiles[] = {
        {"bounded_L0", DisorderMode::kBounded, Duration{0}},
        {"heavytailed_L0", DisorderMode::kHeavyTailed, Duration{0}},
        {"heavytailed_Lpos", DisorderMode::kHeavyTailed, Duration{2000}},
    };
    for (const auto& prof : profiles) {
        for (uint32_t k : {2u, 3u}) {
            auto result = RunMultiSourceDifferential(MakeConfig(prof, k, /*workers=*/4));
            EXPECT_EQ(result.seeds_passed, 3u)
                << prof.name << " K=" << k << " workers=4 first failure: "
                << result.failure_detail;
            EXPECT_EQ(result.seeds_failed, 0u) << result.failure_detail;
            EXPECT_EQ(result.engine_late_dropped, result.oracle_predicted_drops);
        }
    }
}

// K=1 reduces EXACTLY to the single-source path: for the same config, the
// multi-source harness (sources=1) and the ordinary single-source harness must
// produce identical pass counts and identical late-data evidence, because the
// K=1 merged stream IS the single generator's stream (proven byte-wise in
// source_merge_test.cpp's KOneIsPassthroughOfBareGenerator).
TEST(MultiSourceKOne, ReducesToSingleSourcePath) {
    const Profile profiles[] = {
        {"bounded_L0", DisorderMode::kBounded, Duration{0}},
        {"heavytailed_L0", DisorderMode::kHeavyTailed, Duration{0}},
        {"heavytailed_Lpos", DisorderMode::kHeavyTailed, Duration{2000}},
    };
    for (const auto& prof : profiles) {
        DifferentialConfig multi = MakeConfig(prof, /*k=*/1, /*workers=*/1);
        DifferentialConfig single = multi;
        single.sources = 1;  // (already) — the ordinary path ignores `sources`.

        auto rm = RunMultiSourceDifferential(multi);
        auto rs = RunDifferential(single);

        EXPECT_EQ(rm.seeds_passed, rs.seeds_passed) << prof.name;
        EXPECT_EQ(rm.seeds_failed, 0u) << prof.name << ": " << rm.failure_detail;
        EXPECT_EQ(rs.seeds_failed, 0u) << prof.name << ": " << rs.failure_detail;
        // Identical stream => identical late-data evidence, exactly.
        EXPECT_EQ(rm.engine_late_dropped, rs.engine_late_dropped) << prof.name;
        EXPECT_EQ(rm.oracle_predicted_drops, rs.oracle_predicted_drops) << prof.name;
        EXPECT_EQ(rm.engine_windows_refired, rs.engine_windows_refired) << prof.name;
        EXPECT_EQ(rm.engine_late_accepted, rs.engine_late_accepted) << prof.name;
    }
}

}  // namespace
}  // namespace stormglass

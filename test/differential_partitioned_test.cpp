#include <gtest/gtest.h>

#include "oracle/differential.h"

#include <cstdint>
#include <string>

// The Phase-2 proof, driven through the SHARED differential harness.
//
// partitioned_pipeline_test.cpp proves PartitionedPipeline == single-threaded
// Pipeline directly (engine vs engine). This file closes the loop by proving
// PartitionedPipeline == the UNCHANGED oracle, for every N in {1,2,4,8}, over
// the authoritative (deduped) result set — the same oracle that certifies the
// single-threaded engine. RunCrossN, per seed, computes the oracle and the
// single-threaded reference once, then runs the partitioned engine at each N and
// asserts engine(N) == oracle AND engine(N) == single-threaded (result set and
// drop count). The unit test keeps the seed count small (~5) so ctest stays
// fast; the full 100-seed sweep is reproducible from the CLI (--cross-n).

namespace stormglass {
namespace {

// What each profile is expected to exercise, so the test is provably not
// vacuous: bounded never drops; heavy-tailed L=0 drops beyond-window records;
// heavy-tailed L>0 re-fires windows for within-lateness late data.
enum class Evidence { kNoDrops, kDrops, kRefires };

struct Profile {
    const char* name;
    DisorderMode mode;
    Duration allowed_lateness;
    Evidence evidence;
};

DifferentialConfig MakeConfig(const Profile& prof) {
    DifferentialConfig c{};
    c.num_seeds = 5;               // small: full 100-seed run is via --cross-n
    c.records_per_seed = 10000;    // in-memory sinks, so no fsync cost
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
    return c;
}

class PartitionedDifferential : public ::testing::TestWithParam<Profile> {};

// engine(N) == oracle AND engine(N) == single-threaded for N in {1,2,4,8},
// across a handful of seeds, for each disorder/lateness profile.
TEST_P(PartitionedDifferential, CrossNMatchesOracleAndSingleThreaded) {
    const Profile prof = GetParam();
    auto result = RunCrossN(MakeConfig(prof));

    EXPECT_EQ(result.seeds_tested, 5u);
    EXPECT_EQ(result.seeds_passed, 5u)
        << prof.name << " first failure: " << result.failure_detail;
    EXPECT_EQ(result.seeds_failed, 0u);
    EXPECT_TRUE(result.failure_detail.empty()) << result.failure_detail;

    // Cross-N runs 1,2,4,8; drop count is asserted identical across all of them.
    EXPECT_EQ(result.engine_late_dropped, result.oracle_predicted_drops)
        << prof.name << ": engine and oracle drop counts diverged";

    // Prove the profile actually exercised the path it claims to.
    switch (prof.evidence) {
        case Evidence::kNoDrops:
            EXPECT_EQ(result.engine_late_dropped, 0u)
                << "bounded workload should never drop";
            break;
        case Evidence::kDrops:
            EXPECT_GT(result.oracle_predicted_drops, 0u)
                << "heavy-tailed L=0 should drop late records";
            break;
        case Evidence::kRefires:
            EXPECT_GT(result.engine_windows_refired, 0u)
                << "heavy-tailed L>0 should re-fire windows for late-but-in-lateness data";
            break;
    }
}

INSTANTIATE_TEST_SUITE_P(
    Profiles, PartitionedDifferential,
    ::testing::Values(
        Profile{"bounded_L0", DisorderMode::kBounded, Duration{0}, Evidence::kNoDrops},
        Profile{"heavytailed_L0", DisorderMode::kHeavyTailed, Duration{0}, Evidence::kDrops},
        Profile{"heavytailed_Lpos", DisorderMode::kHeavyTailed, Duration{2000},
                Evidence::kRefires}),
    [](const ::testing::TestParamInfo<Profile>& info) {
        return std::string(info.param.name);
    });

// ---- Idle-partition coverage ----------------------------------------------
// With num_keys (3) < num_workers (8), at least five workers receive NO data
// records. The run must still (a) produce results identical to the oracle and
// (b) TERMINATE — an idle worker must not stall global progress.
//
// Mechanism: the Router BROADCASTS every control record (watermarks) to ALL
// workers and, when the source drains, pushes an in-band EndOfStream sentinel to
// EVERY worker before closing its queue. So an idle worker still advances its
// watermark from the broadcast stream and still receives its termination
// sentinel; it FinalFlushes (emitting nothing) and joins. Nothing about global
// progress waits on a worker having received data. If that were false, this
// test would hang rather than fail — completion is itself the liveness proof.
TEST(PartitionedIdleWorkers, IdleWorkerDoesNotStallAndResultsAreCorrect) {
    DifferentialConfig c{};
    c.num_seeds = 3;
    c.records_per_seed = 10000;
    c.num_keys = 3;                       // < workers => idle workers guaranteed
    c.window_size = Duration{1000};
    c.max_disorder = Duration{500};
    c.allowed_lateness = Duration{0};
    c.disorder_mode = DisorderMode::kBounded;
    c.seed_start = 1;
    c.workers = 8;                        // 8 workers, only 3 keys can be routed

    auto result = RunDifferential(c);

    EXPECT_EQ(result.seeds_tested, 3u);
    EXPECT_EQ(result.seeds_passed, 3u)
        << "idle-worker run diverged from oracle: " << result.failure_detail;
    EXPECT_EQ(result.seeds_failed, 0u);
    EXPECT_TRUE(result.failure_detail.empty()) << result.failure_detail;
}

}  // namespace
}  // namespace stormglass

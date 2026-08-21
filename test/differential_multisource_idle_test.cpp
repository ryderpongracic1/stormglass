#include <gtest/gtest.h>

#include "oracle/differential.h"

#include <cstdint>

// v3 Phase 2 — idle + differential.
//
// source_merge_idle_test.cpp proves the idle machinery directly (exclusion
// advances the MIN, resume never regresses, below-watermark records emitted).
// This file closes the loop end-to-end: the engine, driven over a SourceMerge
// whose SLOWEST source goes idle and then resumes, matches the UNCHANGED oracle
// fed the SAME merged stream — INCLUDING the drop/re-fire of the resumed-late
// records, which the oracle predicts exactly from the merged trajectory.
//
// The workload uses BOUNDED disorder, under which nothing is EVER late on its
// own. So with idleness enabled, the ONLY source of lateness is the resumed idle
// source — a nonzero drop/re-fire count is therefore direct proof the
// resumed-late path is genuinely exercised, and engine==oracle proves it is
// handled correctly.

namespace stormglass {
namespace {

DifferentialConfig MakeIdleDiffConfig(uint32_t k) {
    DifferentialConfig c{};
    c.num_seeds = 4;
    c.records_per_seed = 3000;      // per source
    c.num_keys = 10;
    c.window_size = Duration{1000};
    c.max_disorder = Duration{500};
    c.allowed_lateness = Duration{1000};       // L > 0
    c.disorder_mode = DisorderMode::kBounded;  // bounded => ONLY idle-resume is late
    c.seed_start = 1;
    c.sources = k;
    c.workers = 1;
    // Idleness: the slowest source (index k-1) goes quiet at record 600 for 1500
    // turns — long enough to trip the idle_timeout=30 exclusion and let the fast
    // sources drive the merged watermark far ahead, so the resumed records land
    // well below it and are late.
    c.idle_timeout = 30;
    c.idle_span_start = 600;
    c.idle_span_length = 1500;
    return c;
}

TEST(MultiSourceIdleDifferential, EngineMatchesOracleAndResumedLatePathExercised) {
    for (uint32_t k : {2u, 3u}) {
        auto r = RunMultiSourceDifferential(MakeIdleDiffConfig(k));

        EXPECT_EQ(r.seeds_tested, 4u) << "K=" << k;
        EXPECT_EQ(r.seeds_passed, 4u)
            << "K=" << k << " first failure: " << r.failure_detail;
        EXPECT_EQ(r.seeds_failed, 0u) << r.failure_detail;
        EXPECT_TRUE(r.failure_detail.empty()) << r.failure_detail;

        // Drop-count contract: engine and oracle agree exactly on how many
        // resumed-late records were dropped.
        EXPECT_EQ(r.engine_late_dropped, r.oracle_predicted_drops)
            << "K=" << k << ": engine/oracle drop counts diverged";

        // Non-vacuity: bounded disorder is never late by itself, so any drop or
        // re-fire here is a resumed-late record. Prove the path is exercised.
        EXPECT_GT(r.engine_late_dropped + r.engine_windows_refired, 0u)
            << "K=" << k << ": resumed-late path was not exercised";
    }
}

// Regression guard: idle DISABLED reproduces the Phase-1 multi-source path. With
// idle_timeout=0 and no gap, bounded disorder is drop-free and the engine still
// matches the oracle — proving the idle capability is purely additive.
TEST(MultiSourceIdleDifferential, IdleDisabledReproducesPhase1NoLateness) {
    for (uint32_t k : {2u, 3u}) {
        DifferentialConfig c = MakeIdleDiffConfig(k);
        c.idle_timeout = 0;        // no exclusion
        c.idle_span_length = 0;    // no gap

        auto r = RunMultiSourceDifferential(c);

        EXPECT_EQ(r.seeds_passed, 4u) << "K=" << k << ": " << r.failure_detail;
        EXPECT_EQ(r.seeds_failed, 0u) << r.failure_detail;
        EXPECT_EQ(r.engine_late_dropped, 0u)
            << "K=" << k << ": bounded + no-idle must be drop-free (Phase-1)";
        EXPECT_EQ(r.oracle_predicted_drops, 0u) << "K=" << k;
        EXPECT_EQ(r.engine_windows_refired, 0u) << "K=" << k;
        EXPECT_EQ(r.engine_late_accepted, 0u) << "K=" << k;
    }
}

}  // namespace
}  // namespace stormglass

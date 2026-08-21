#include <gtest/gtest.h>

#include "nemesis/nemesis.h"

#include <cstdint>

// v3 Phase 3 — mid-ALIGNMENT crash recovery.
//
// Extends the real fork()+SIGKILL harness with a scenario that kills the process
// WHILE K-way barrier alignment is in progress: the fast channel has delivered
// barrier N (it is blocked) but the slow channel has not, so the merged barrier
// N was never emitted and no checkpoint for epoch N exists. The guarantee: the
// restart restores from the last COMPLETE (fully-aligned) checkpoint — a multiple
// of the merged stride S strictly below the mid-alignment epoch — and at-least-
// once holds (union of pre-crash durable output and post-restore output covers
// the oracle over the merged stream, 0 missing).
//
// L == 0 / bounded, so the DurableFileSink per-emit fsync does not balloon; the
// workload is small (3000 records/source).

using namespace stormglass;

TEST(AlignmentKillNemesis, MidAlignmentCrashRestoresFromLastCompleteCheckpoint) {
    AlignmentKillConfig config{};
    config.seed = 42;

    auto result = RunAlignmentKillNemesis(config);

    // At-least-once: nothing the oracle expects is missing after recovery.
    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u) << result.failure_detail;

    // The crash was genuine AND mid-alignment.
    EXPECT_TRUE(result.killed_by_sigkill);
    EXPECT_FALSE(result.final_flush_completed);
    EXPECT_TRUE(result.alignment_partial_at_kill)
        << "kill must have landed while some (not all) channels had delivered the "
           "open epoch's barrier";
    ASSERT_GT(result.num_channels, 1u);
    EXPECT_GE(result.channels_delivered, 1u);
    EXPECT_LT(result.channels_delivered, result.num_channels)
        << "partial alignment => strictly fewer than K channels delivered";
    EXPECT_GE(result.epoch_in_progress, 2u);

    // Restore fell back to a COMPLETE, fully-aligned cut: a positive multiple of
    // the merged stride S, strictly below the mid-alignment epoch's (never-written)
    // barrier cut.
    ASSERT_GT(result.merged_stride, 0u);
    EXPECT_GT(result.restored_offset, 0u);
    EXPECT_EQ(result.restored_offset % result.merged_stride, 0u)
        << "restore offset must be a fully-aligned cut (multiple of stride "
        << result.merged_stride << ")";
    EXPECT_LT(result.restored_offset, result.epoch_in_progress * result.merged_stride)
        << "restore must fall back below the mid-alignment epoch cut";

    // Post-restore drained and covered the oracle. (pre_crash_emits may be 0: a
    // mid-alignment crash at an early epoch can precede the first durable window
    // emit — at-least-once is carried by the post-restore replay from the last
    // complete checkpoint, which is exactly the property under test.)
    EXPECT_GT(result.post_restore_emits, 0u);
    EXPECT_GT(result.oracle_results, 0u);
    EXPECT_EQ(result.union_results, result.oracle_results)
        << "union of pre-crash + post-restore output must cover the oracle exactly";
}

// A few seeds: every mid-alignment crash recovers with zero missing results.
// Duplicates are permitted (records between the last aligned checkpoint and the
// crash are replayed) — counted, not required to be zero.
TEST(AlignmentKillNemesis, MultiSeedZeroMissing) {
    uint64_t total_missing = 0, kills = 0, partials = 0;
    for (uint64_t seed = 0; seed < 3; ++seed) {
        AlignmentKillConfig config{};
        config.seed = 100 + seed;

        auto result = RunAlignmentKillNemesis(config);
        EXPECT_TRUE(result.passed) << "seed " << seed << ": " << result.failure_detail;
        EXPECT_EQ(result.missing_results, 0u) << "seed " << seed;
        total_missing += result.missing_results;
        if (result.killed_by_sigkill) ++kills;
        if (result.alignment_partial_at_kill) ++partials;
    }
    EXPECT_EQ(total_missing, 0u);
    EXPECT_EQ(kills, 3u) << "all runs must be genuine SIGKILL crashes";
    EXPECT_EQ(partials, 3u) << "all runs must be captured mid-alignment";
}

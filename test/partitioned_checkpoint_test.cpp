#include <gtest/gtest.h>

#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/writer.h"
#include "engine/partitioned_pipeline.h"
#include "nemesis/nemesis.h"
#include "oracle/oracle.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "window/state.h"
#include "window/tumbling.h"

#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

using namespace stormglass;

namespace {

class PartitionedCheckpointTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/stormglass_pckpt_XXXXXX";
        char* result = ::mkdtemp(tmpl);
        ASSERT_NE(result, nullptr);
        dir_ = result;
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

GeneratorConfig MakeGen(uint64_t seed, uint64_t num_records,
                        uint64_t checkpoint_interval) {
    GeneratorConfig c{};
    c.seed = seed;
    c.num_keys = 20;
    c.num_records = num_records;
    c.max_disorder = Duration{500};
    c.batch_size = 1024;
    c.watermark_interval = 100;
    c.checkpoint_interval = checkpoint_interval;
    return c;
}

// Oracle result set over the FULL dataset, keyed by (key, window.start).
std::map<std::pair<std::string, int64_t>, std::pair<int64_t, uint64_t>>
OracleMap(const GeneratorConfig& gen) {
    Oracle oracle(OracleConfig{.window_size = Duration{1000}});
    DeterministicGenerator g(gen);
    while (auto batch = g.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) oracle.AddRecord(*r);
        }
    }
    std::map<std::pair<std::string, int64_t>, std::pair<int64_t, uint64_t>> m;
    for (auto& r : oracle.ComputeResults()) {
        m[{r.key, r.window.start.time_since_epoch().count()}] =
            {r.result.value, r.result.count};
    }
    return m;
}

// ---------------------------------------------------------------------------
// 1. Distributed checkpoint round-trip (no crash), N in {2,4}.
//    A partitioned run with checkpointing stops partway (writing per-partition
//    checkpoint files at barrier offsets); a fresh partitioned pipeline restores
//    from the highest complete global checkpoint and drains to completion. The
//    union of both runs' output covers the oracle with zero missing — and the
//    restore is proven to have actually happened (records_replayed > 0).
// ---------------------------------------------------------------------------
TEST_F(PartitionedCheckpointTest, DistributedRoundTripCoversOracle) {
    const uint64_t total = 20000;
    const uint64_t interval = 1000;
    const uint64_t stop_at = 12000;

    for (uint32_t n : {2u, 4u}) {
        std::string ckpt = dir_ + "/n" + std::to_string(n);
        std::filesystem::create_directories(ckpt);
        auto gen = MakeGen(/*seed=*/7, total, interval);

        std::map<std::pair<std::string, int64_t>, std::pair<int64_t, uint64_t>> combined;

        // Run 1: stop partway. Checkpoints are written on barriers before the stop.
        {
            auto sink = std::make_unique<MemorySink>();
            auto* sp = sink.get();
            PartitionedPipelineConfig pc{};
            pc.num_workers = n;
            pc.checkpoint_dir = ckpt;
            PartitionedPipeline pipeline(
                std::make_unique<StoppingSource>(
                    std::make_unique<DeterministicGenerator>(gen), stop_at),
                [] { return std::make_unique<TumblingAssigner>(Duration{1000}); },
                std::move(sink), pc);
            auto stats = pipeline.Run();
            EXPECT_GT(stats.checkpoints_written, 0u) << "N=" << n;
            for (auto& r : sp->Results()) {
                combined[{r.key, r.window.start.time_since_epoch().count()}] =
                    {r.result.value, r.result.count};
            }
        }

        // Run 2: fresh pipeline restores from the highest complete global
        // checkpoint and runs to completion.
        {
            auto sink = std::make_unique<MemorySink>();
            auto* sp = sink.get();
            PartitionedPipelineConfig pc{};
            pc.num_workers = n;
            pc.checkpoint_dir = ckpt;
            PartitionedPipeline pipeline(
                std::make_unique<DeterministicGenerator>(gen),
                [] { return std::make_unique<TumblingAssigner>(Duration{1000}); },
                std::move(sink), pc);
            auto stats = pipeline.Run();
            EXPECT_GT(stats.records_replayed, 0u)
                << "N=" << n << ": restore did not resume from a checkpoint";
            EXPECT_LT(stats.records_replayed, stop_at)
                << "N=" << n << ": restored offset should be below the stop point";
            for (auto& r : sp->Results()) {
                combined[{r.key, r.window.start.time_since_epoch().count()}] =
                    {r.result.value, r.result.count};
            }
        }

        // At-least-once: union covers every oracle result.
        auto oracle = OracleMap(gen);
        uint64_t missing = 0;
        for (auto& [k, v] : oracle) {
            auto it = combined.find(k);
            if (it == combined.end() || it->second != v) missing++;
        }
        EXPECT_EQ(missing, 0u)
            << "N=" << n << ": distributed restore lost " << missing << "/"
            << oracle.size() << " results";
    }
}

// ---------------------------------------------------------------------------
// 2. Torn-global-checkpoint rejection (no fork). Construct all N partition files
//    at O_lo and only N-1 at a higher O_hi; the coordinator must pick O_lo, and
//    a fresh partitioned pipeline pointed at that dir must restore from O_lo
//    (never the torn O_hi).
// ---------------------------------------------------------------------------
TEST_F(PartitionedCheckpointTest, TornGlobalCheckpointRejected) {
    const uint32_t n = 4;
    const uint64_t o_lo = 5000;
    const uint64_t o_hi = 10000;

    // A minimal non-empty state so each checkpoint has a pane to persist.
    KeyedWindowState state;
    state.Add("key-0001", Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}}, 42);
    Timestamp wm{Duration{500}};

    // Complete set at O_lo: every partition.
    for (uint32_t k = 0; k < n; ++k) {
        std::string pdir = PartitionCheckpointDir(dir_, k);
        std::filesystem::create_directories(pdir);
        ASSERT_TRUE(CheckpointWriter(pdir).WriteCheckpoint(o_lo, wm, state));
    }
    // Torn set at O_hi: all partitions EXCEPT the last.
    for (uint32_t k = 0; k < n - 1; ++k) {
        std::string pdir = PartitionCheckpointDir(dir_, k);
        ASSERT_TRUE(CheckpointWriter(pdir).WriteCheckpoint(o_hi, wm, state));
    }

    // Coordinator: highest COMPLETE is O_lo; highest PARTIAL is O_hi (torn).
    auto complete = HighestCompleteCheckpoint(dir_, n);
    ASSERT_TRUE(complete.has_value());
    EXPECT_EQ(*complete, o_lo);

    auto partial = HighestPartialCheckpoint(dir_, n);
    ASSERT_TRUE(partial.has_value());
    EXPECT_EQ(*partial, o_hi);
    EXPECT_GT(*partial, *complete) << "torn state must be detectable";

    // Positive control: completing O_hi in the last partition makes it selectable.
    {
        std::string pdir = PartitionCheckpointDir(dir_, n - 1);
        ASSERT_TRUE(CheckpointWriter(pdir).WriteCheckpoint(o_hi, wm, state));
        auto now_complete = HighestCompleteCheckpoint(dir_, n);
        ASSERT_TRUE(now_complete.has_value());
        EXPECT_EQ(*now_complete, o_hi);
    }
}

TEST_F(PartitionedCheckpointTest, RestoreSelectsCompleteOverTorn) {
    const uint32_t n = 4;
    const uint64_t total = 20000;
    const uint64_t interval = 1000;

    auto gen = MakeGen(/*seed=*/11, total, interval);

    // Produce a real, self-consistent set of per-partition checkpoints by
    // running a partitioned pipeline that stops partway.
    {
        auto sink = std::make_unique<MemorySink>();
        PartitionedPipelineConfig pc{};
        pc.num_workers = n;
        pc.checkpoint_dir = dir_;
        PartitionedPipeline pipeline(
            std::make_unique<StoppingSource>(
                std::make_unique<DeterministicGenerator>(gen), 12000u),
            [] { return std::make_unique<TumblingAssigner>(Duration{1000}); },
            std::move(sink), pc);
        pipeline.Run();
    }

    auto complete = HighestCompleteCheckpoint(dir_, n);
    ASSERT_TRUE(complete.has_value());

    // Manufacture a TORN higher offset in ONE partition only, as a genuine
    // CRC-valid checkpoint so the reader accepts it.
    KeyedWindowState state;
    state.Add("key-0002", Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}}, 7);
    const uint64_t torn = *complete + interval;  // one barrier past the complete one
    ASSERT_TRUE(CheckpointWriter(PartitionCheckpointDir(dir_, 0))
                    .WriteCheckpoint(torn, Timestamp{Duration{100}}, state));

    // Coordinator still reports the complete offset (torn is ignored).
    auto still_complete = HighestCompleteCheckpoint(dir_, n);
    ASSERT_TRUE(still_complete.has_value());
    EXPECT_EQ(*still_complete, *complete);
    auto partial = HighestPartialCheckpoint(dir_, n);
    ASSERT_TRUE(partial.has_value());
    EXPECT_EQ(*partial, torn);

    // A fresh pipeline restores from the complete offset, never the torn one.
    {
        auto sink = std::make_unique<MemorySink>();
        PartitionedPipelineConfig pc{};
        pc.num_workers = n;
        pc.checkpoint_dir = dir_;
        PartitionedPipeline pipeline(
            std::make_unique<DeterministicGenerator>(gen),
            [] { return std::make_unique<TumblingAssigner>(Duration{1000}); },
            std::move(sink), pc);
        auto stats = pipeline.Run();
        EXPECT_EQ(stats.records_replayed, *complete)
            << "restore must use the complete offset, not the torn " << torn;
        EXPECT_LT(stats.records_replayed, torn);
    }
}

// ---------------------------------------------------------------------------
// 3. Partitioned real-kill mid-alignment: fork+SIGKILL at a torn global
//    checkpoint, N in {2,4}. Assert 0 missing AND restore fell back to a
//    complete checkpoint below the torn one.
// ---------------------------------------------------------------------------
TEST(PartitionedRealKillNemesis, TornGlobalCheckpointZeroMissing) {
    for (uint32_t n : {2u, 4u}) {
        PartitionedRealKillConfig config{};
        config.seed = 42 + n;
        config.num_records = 6000;   // small: per-emit fsync
        config.num_keys = 20;
        config.num_workers = n;
        config.checkpoint_interval = 500;
        config.target_checkpoint = 3;  // arm at offset 1500 so window [0,1000)
                                        // has fired+flushed before the kill

        auto result = RunPartitionedRealKillNemesis(config);

        EXPECT_TRUE(result.passed)
            << "N=" << n << " (attempts=" << result.attempts
            << "): " << result.failure_detail;
        EXPECT_EQ(result.missing_results, 0u) << "N=" << n;

        // (a) The crash was genuine.
        EXPECT_TRUE(result.killed_by_sigkill) << "N=" << n;
        EXPECT_FALSE(result.final_flush_completed) << "N=" << n;
        EXPECT_GT(result.pre_crash_emits, 0u)
            << "N=" << n << ": durable pre-crash output should survive SIGKILL";

        // (b) A torn global checkpoint existed and restore fell back to a
        //     COMPLETE one strictly below it.
        EXPECT_TRUE(result.torn_checkpoint_observed) << "N=" << n;
        EXPECT_LT(result.restored_offset, result.torn_offset)
            << "N=" << n << ": restore used the torn offset instead of the complete one";
        EXPECT_GT(result.oracle_results, 0u) << "N=" << n;
    }
}

}  // namespace

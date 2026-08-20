#include <gtest/gtest.h>

#include "nemesis/nemesis.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "oracle/oracle.h"
#include "nemesis/nemesis.h"

#include <algorithm>
#include <map>
#include <filesystem>

using namespace stormglass;

// --- Nemesis Phase Tests ---

TEST(NemesisTest, BetweenCheckpointsZeroMissing) {
    NemesisConfig config{};
    config.seed = 42;
    config.num_records = 10000;
    config.checkpoint_interval = 1000;
    config.kill_phase = NemesisPhase::kBetweenCheckpoints;
    config.kill_position = 0.5;

    auto result = RunNemesis(config);
    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u);
    EXPECT_GT(result.records_before_kill, 0u);
    EXPECT_GT(result.records_after_restore, 0u);
}

TEST(NemesisTest, MidCheckpointZeroMissing) {
    NemesisConfig config{};
    config.seed = 77;
    config.num_records = 10000;
    config.checkpoint_interval = 1000;
    config.kill_phase = NemesisPhase::kMidCheckpoint;
    config.kill_position = 0.5;

    auto result = RunNemesis(config);
    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u);
}

TEST(NemesisTest, MidEmissionZeroMissing) {
    NemesisConfig config{};
    config.seed = 123;
    config.num_records = 10000;
    config.checkpoint_interval = 1000;
    config.kill_phase = NemesisPhase::kMidEmission;
    config.kill_position = 0.6;

    auto result = RunNemesis(config);
    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u);
}

// --- Differential: restored pipeline = fresh pipeline ---

TEST(NemesisTest, CheckpointDoesNotAlterSemantics) {
    char tmpl[] = "/tmp/stormglass_test_XXXXXX";
    char* dir_result = ::mkdtemp(tmpl);
    ASSERT_NE(dir_result, nullptr);
    std::string ckpt_dir = dir_result;

    // Run pipeline WITH checkpointing (full run, no crash)
    std::vector<WindowResult> with_results;
    {
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();

        auto gen = std::make_unique<DeterministicGenerator>(GeneratorConfig{
            .seed = 42, .num_keys = 10, .num_records = 5000,
            .max_disorder = Duration{500}, .batch_size = 1024, .watermark_interval = 100,
            .checkpoint_interval = 500});
        auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
        PipelineConfig config{};
        config.checkpoint_dir = ckpt_dir;
        config.checkpoint_interval = 500;
        Pipeline pipeline(std::move(gen), std::move(assigner), std::move(sink), config);
        pipeline.Run();
        with_results = sink_ptr->Results();
    }

    // Run pipeline WITHOUT checkpointing
    std::vector<WindowResult> without_results;
    {
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();

        auto gen = std::make_unique<DeterministicGenerator>(GeneratorConfig{
            .seed = 42, .num_keys = 10, .num_records = 5000,
            .max_disorder = Duration{500}, .batch_size = 1024, .watermark_interval = 100});
        auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
        PipelineConfig config{};
        Pipeline pipeline(std::move(gen), std::move(assigner), std::move(sink), config);
        pipeline.Run();
        without_results = sink_ptr->Results();
    }

    // Results must be identical
    ASSERT_EQ(with_results.size(), without_results.size());

    // Sort both by key+window for deterministic comparison
    auto sort_fn = [](const WindowResult& a, const WindowResult& b) {
        if (a.key != b.key) return a.key < b.key;
        return a.window.start < b.window.start;
    };
    std::sort(with_results.begin(), with_results.end(), sort_fn);
    std::sort(without_results.begin(), without_results.end(), sort_fn);

    for (size_t i = 0; i < with_results.size(); ++i) {
        EXPECT_EQ(with_results[i].key, without_results[i].key);
        EXPECT_EQ(with_results[i].window.start, without_results[i].window.start);
        EXPECT_EQ(with_results[i].window.end, without_results[i].window.end);
        EXPECT_EQ(with_results[i].result.value, without_results[i].result.value);
        EXPECT_EQ(with_results[i].result.count, without_results[i].result.count);
    }

    std::filesystem::remove_all(ckpt_dir);
}

// Multiple seeds stress test
TEST(NemesisTest, MultiSeedStress) {
    for (uint64_t seed = 0; seed < 5; ++seed) {
        NemesisConfig config{};
        config.seed = seed;
        config.num_records = 5000;
        config.checkpoint_interval = 500;
        config.kill_phase = NemesisPhase::kBetweenCheckpoints;
        config.kill_position = 0.3 + 0.1 * static_cast<double>(seed);

        auto result = RunNemesis(config);
        EXPECT_TRUE(result.passed)
            << "Seed " << seed << " failed: " << result.failure_detail;
        EXPECT_EQ(result.missing_results, 0u) << "Seed " << seed;
    }
}

TEST(Nemesis, DoubleCrashRestore) {
    // Verifies that after restore, checkpoint offset is absolute.
    // A second crash + third restore produces correct combined results.
    
    char tmpl[] = "/tmp/stormglass_nemesis_XXXXXX";
    std::string tmp_dir = ::mkdtemp(tmpl);
    
    const uint64_t total_records = 30000;
    const uint64_t checkpoint_interval = 5000;
    const uint64_t crash1_at = 12000;
    const uint64_t crash2_at = 8000;
    
    GeneratorConfig gen_config;
    gen_config.seed = 99;
    gen_config.num_records = total_records;
    gen_config.num_keys = 10;
    gen_config.max_disorder = Duration{500};
    gen_config.batch_size = 256;
    gen_config.watermark_interval = 50;
    gen_config.checkpoint_interval = checkpoint_interval;
    
    PipelineConfig pipe_config;
    pipe_config.checkpoint_dir = tmp_dir;
    pipe_config.checkpoint_interval = checkpoint_interval;
    
    // Collect results across all runs
    std::map<std::pair<std::string, int64_t>, std::pair<int64_t, uint64_t>> combined;
    
    // --- First run: crash at 12000 records ---
    {
        auto source = std::make_unique<StoppingSource>(
            std::make_unique<DeterministicGenerator>(gen_config), crash1_at);
        auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();
        Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink), pipe_config);
        auto stats = pipeline.Run();
        EXPECT_EQ(stats.records_processed, crash1_at);
        EXPECT_GE(stats.checkpoints_written, 2u);
        for (auto& r : sink_ptr->Results()) {
            auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
            combined[key] = {r.result.value, r.result.count};
        }
    }
    
    // --- Second run: restores, crashes after 8000 more ---
    {
        auto source = std::make_unique<StoppingSource>(
            std::make_unique<DeterministicGenerator>(gen_config), crash2_at);
        auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();
        Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink), pipe_config);
        auto stats = pipeline.Run();
        EXPECT_EQ(stats.records_replayed, 10000u);
        EXPECT_EQ(stats.records_processed, crash2_at);
        EXPECT_GE(stats.checkpoints_written, 1u);
        for (auto& r : sink_ptr->Results()) {
            auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
            combined[key] = {r.result.value, r.result.count};  // overwrite with latest
        }
    }
    
    // --- Third run: restores from run2's checkpoint, runs to completion ---
    {
        auto source = std::make_unique<DeterministicGenerator>(gen_config);
        auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();
        Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink), pipe_config);
        auto stats = pipeline.Run();
        // Key assertion: restores from 15000 (absolute), not 5000 (relative)
        EXPECT_EQ(stats.records_replayed, 15000u);
        EXPECT_EQ(stats.records_processed, total_records - 15000u);
        for (auto& r : sink_ptr->Results()) {
            auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
            combined[key] = {r.result.value, r.result.count};
        }
    }
    
    // Oracle: compute expected results for full dataset
    DeterministicGenerator oracle_gen(gen_config);
    Oracle oracle(OracleConfig{.window_size = Duration{1000}});
    while (auto batch = oracle_gen.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            }
        }
    }
    auto oracle_results = oracle.ComputeResults();
    
    // Verify: every oracle result appears in combined output (at-least-once)
    uint64_t missing = 0;
    for (auto& r : oracle_results) {
        auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
        if (combined.find(key) == combined.end()) {
            missing++;
        }
    }
    EXPECT_EQ(missing, 0u) << "Double-crash-restore lost " << missing << " results";
    
    std::filesystem::remove_all(tmp_dir);
}


// --- Real crash nemesis: fork() + SIGKILL ---

TEST(RealKillNemesis, BetweenCheckpointsZeroMissing) {
    RealKillConfig config{};
    config.seed = 42;
    config.num_records = 20000;
    config.num_keys = 20;
    config.checkpoint_interval = 1000;
    config.kill_point = RealKillPoint::kBetweenCheckpoints;
    config.target_checkpoint = 2;

    auto result = RunRealKillNemesis(config);

    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u);
    // Evidence the crash was real: SIGKILL landed before any final flush.
    EXPECT_TRUE(result.killed_by_sigkill);
    EXPECT_FALSE(result.final_flush_completed);
    // Pre-crash durable sink output survived the kill and post-restore drained.
    EXPECT_GT(result.pre_crash_emits, 0u)
        << "durable pre-crash output should survive SIGKILL";
    EXPECT_GT(result.post_restore_emits, 0u);
    EXPECT_GT(result.oracle_results, 0u);
}

TEST(RealKillNemesis, MidCheckpointRealPartialWriteRecovery) {
    RealKillConfig config{};
    config.seed = 77;
    config.num_records = 40000;
    config.num_keys = 100;
    config.checkpoint_interval = 500;
    config.kill_point = RealKillPoint::kMidCheckpoint;

    auto result = RunRealKillNemesis(config);

    EXPECT_TRUE(result.passed) << result.failure_detail;
    EXPECT_EQ(result.missing_results, 0u);
    EXPECT_TRUE(result.killed_by_sigkill);
    EXPECT_FALSE(result.final_flush_completed);
    // The kill genuinely interrupted a checkpoint write, leaving a stale .tmp
    // that the restore path had to recover from — not a fabricated garbage file.
    EXPECT_TRUE(result.stale_tmp_after_kill)
        << "expected a real interrupted checkpoint .tmp after " << result.attempts
        << " attempts";
}

TEST(RealKillNemesis, MultiRunZeroMissingCountsDuplicates) {
    uint64_t total_missing = 0;
    uint64_t total_duplicates = 0;
    uint64_t kills = 0;

    for (uint64_t seed = 0; seed < 3; ++seed) {
        RealKillConfig config{};
        config.seed = 100 + seed;
        config.num_records = 20000;
        config.num_keys = 20;
        config.checkpoint_interval = 1000;
        config.kill_point = RealKillPoint::kBetweenCheckpoints;
        config.target_checkpoint = 2;

        auto result = RunRealKillNemesis(config);
        EXPECT_TRUE(result.passed) << "seed " << seed << ": " << result.failure_detail;
        EXPECT_EQ(result.missing_results, 0u) << "seed " << seed;
        total_missing += result.missing_results;
        total_duplicates += result.duplicates;
        if (result.killed_by_sigkill) ++kills;
    }

    EXPECT_EQ(total_missing, 0u);
    EXPECT_EQ(kills, 3u) << "all runs must be genuine SIGKILL crashes";
    // Duplicates are permitted (records between last checkpoint and crash are
    // replayed). We only require them to be counted, not zero.
    RecordProperty("total_duplicates", static_cast<int>(total_duplicates));
}

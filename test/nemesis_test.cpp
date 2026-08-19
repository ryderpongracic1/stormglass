#include <gtest/gtest.h>

#include "nemesis/nemesis.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"

#include <algorithm>
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
            .max_disorder = Duration{500}, .batch_size = 1024, .watermark_interval = 100});
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

#include "oracle/differential.h"
#include "oracle/oracle.h"
#include "engine/pipeline.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "window/tumbling.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

namespace stormglass {
namespace {

// Helper: sort results deterministically
void SortResults(std::vector<WindowResult>& results) {
    std::sort(results.begin(), results.end(),
              [](const WindowResult& a, const WindowResult& b) {
                  if (a.window.start != b.window.start)
                      return a.window.start < b.window.start;
                  return a.key < b.key;
              });
}

TEST(DifferentialTest, RunDifferentialPasses) {
    // Verify that the engine and oracle agree for 10 seeds × 5000 records
    DifferentialConfig config{
        .num_seeds = 10,
        .records_per_seed = 5000,
        .num_keys = 10,
        .window_size = Duration{1000},
        .max_disorder = Duration{500},
        .verbose = false,
    };

    auto result = RunDifferential(config);

    EXPECT_EQ(result.seeds_tested, 10u);
    EXPECT_EQ(result.seeds_passed, 10u);
    EXPECT_EQ(result.seeds_failed, 0u);
    EXPECT_TRUE(result.failure_detail.empty());
}

TEST(DifferentialTest, CompareResultsCatchesSizeMismatch) {
    // Engine has one fewer result than oracle → mismatch
    std::vector<WindowResult> engine = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 100, .count = 5},
        },
    };

    std::vector<WindowResult> oracle = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 100, .count = 5},
        },
        WindowResult{
            .key = "key-0002",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 200, .count = 10},
        },
    };

    auto mismatch = CompareResults(engine, oracle);
    EXPECT_FALSE(mismatch.empty());
    EXPECT_NE(mismatch.find("count mismatch"), std::string::npos);
}

TEST(DifferentialTest, CompareResultsCatchesSumMismatch) {
    // Same structure but different sum → mismatch detected
    std::vector<WindowResult> engine = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 99, .count = 5},
        },
    };

    std::vector<WindowResult> oracle = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 100, .count = 5},
        },
    };

    auto mismatch = CompareResults(engine, oracle);
    EXPECT_FALSE(mismatch.empty());
    EXPECT_NE(mismatch.find("sum mismatch"), std::string::npos);
}

TEST(DifferentialTest, CompareResultsCatchesCountFieldMismatch) {
    // Same sum but different count → mismatch detected
    std::vector<WindowResult> engine = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 100, .count = 4},
        },
    };

    std::vector<WindowResult> oracle = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 100, .count = 5},
        },
    };

    auto mismatch = CompareResults(engine, oracle);
    EXPECT_FALSE(mismatch.empty());
    EXPECT_NE(mismatch.find("count mismatch"), std::string::npos);
}

TEST(DifferentialTest, CompareResultsMatchesOnEqual) {
    // Identical results → no mismatch
    std::vector<WindowResult> results = {
        WindowResult{
            .key = "key-0001",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 500, .count = 10},
        },
        WindowResult{
            .key = "key-0002",
            .window = Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
            .result = AggregateResult{.value = 300, .count = 7},
        },
    };

    auto mismatch = CompareResults(results, results);
    EXPECT_TRUE(mismatch.empty());
}

TEST(DifferentialTest, CompareResultsDetectsRemovedResult) {
    // Simulate a broken pipeline by removing one result from engine output
    // and verify CompareResults catches the mismatch
    const uint64_t seed = 42;
    const uint64_t num_records = 2000;
    const Duration window_size{1000};
    const Duration max_disorder{200};

    GeneratorConfig gen_config{
        .seed = seed,
        .num_keys = 5,
        .num_records = num_records,
        .max_disorder = max_disorder,
        .batch_size = 512,
        .watermark_interval = 50,
    };

    // Run pipeline and get correct results
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();
    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(gen_config),
        std::make_unique<TumblingAssigner>(window_size),
        std::move(sink));
    pipeline.Run();

    auto engine_results = sink_ptr->Results();
    SortResults(engine_results);

    // Get oracle results
    Oracle oracle(OracleConfig{.window_size = window_size});
    DeterministicGenerator replay_gen(gen_config);
    while (auto batch = replay_gen.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                oracle.AddRecord(std::get<Record>(item));
            }
        }
    }
    auto oracle_results = oracle.ComputeResults();

    // First verify they match
    ASSERT_TRUE(CompareResults(engine_results, oracle_results).empty());

    // Now remove one result from engine output (simulate broken pipeline)
    ASSERT_GT(engine_results.size(), 2u);
    engine_results.erase(engine_results.begin() + engine_results.size() / 2);

    // CompareResults should catch the mismatch
    auto mismatch = CompareResults(engine_results, oracle_results);
    EXPECT_FALSE(mismatch.empty());
}

}  // namespace
}  // namespace stormglass

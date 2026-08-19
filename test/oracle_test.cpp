#include "oracle/oracle.h"
#include "oracle/differential.h"
#include "engine/pipeline.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "window/tumbling.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <memory>

namespace stormglass {
namespace {

// Helper: sort results for deterministic comparison
void SortResults(std::vector<WindowResult>& results) {
    std::sort(results.begin(), results.end(),
              [](const WindowResult& a, const WindowResult& b) {
                  if (a.window.start != b.window.start)
                      return a.window.start < b.window.start;
                  return a.key < b.key;
              });
}

TEST(OracleTest, HandCraftedSmallInput) {
    // 3 records, 2 keys, 1s windows
    Oracle oracle(OracleConfig{.window_size = Duration{1000}});

    // key-A at t=500ms → window [0, 1000)
    oracle.AddRecord(Record{
        .key = "key-A",
        .value = 10,
        .event_time = Timestamp{Duration{500}},
        .processing_time = Timestamp{Duration{500}},
    });

    // key-B at t=1500ms → window [1000, 2000)
    oracle.AddRecord(Record{
        .key = "key-B",
        .value = 20,
        .event_time = Timestamp{Duration{1500}},
        .processing_time = Timestamp{Duration{1500}},
    });

    // key-A at t=200ms → window [0, 1000)
    oracle.AddRecord(Record{
        .key = "key-A",
        .value = 30,
        .event_time = Timestamp{Duration{200}},
        .processing_time = Timestamp{Duration{200}},
    });

    auto results = oracle.ComputeResults();

    // Expect 2 results: [0,1000) key-A sum=40 count=2, [1000,2000) key-B sum=20 count=1
    ASSERT_EQ(results.size(), 2u);

    // Sorted by window.start then key
    EXPECT_EQ(results[0].key, "key-A");
    EXPECT_EQ(results[0].window.start, Timestamp{Duration{0}});
    EXPECT_EQ(results[0].window.end, Timestamp{Duration{1000}});
    EXPECT_EQ(results[0].result.value, 40);
    EXPECT_EQ(results[0].result.count, 2u);

    EXPECT_EQ(results[1].key, "key-B");
    EXPECT_EQ(results[1].window.start, Timestamp{Duration{1000}});
    EXPECT_EQ(results[1].window.end, Timestamp{Duration{2000}});
    EXPECT_EQ(results[1].result.value, 20);
    EXPECT_EQ(results[1].result.count, 1u);
}

TEST(OracleTest, OracleMatchesPipeline_Seed42) {
    // Sanity check: oracle produces same results as the pipeline for a known seed
    const uint64_t seed = 42;
    const uint64_t num_records = 1000;
    const uint32_t num_keys = 5;
    const Duration window_size{1000};
    const Duration max_disorder{200};

    GeneratorConfig gen_config{
        .seed = seed,
        .num_keys = num_keys,
        .num_records = num_records,
        .max_disorder = max_disorder,
        .batch_size = 256,
        .watermark_interval = 50,
    };

    // Run pipeline
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();
    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(gen_config),
        std::make_unique<TumblingAssigner>(window_size),
        std::move(sink));
    pipeline.Run();

    auto engine_results = sink_ptr->Results();
    SortResults(engine_results);

    // Run oracle
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

    // They must match
    auto mismatch = CompareResults(engine_results, oracle_results);
    EXPECT_TRUE(mismatch.empty()) << "Mismatch: " << mismatch;
}

TEST(OracleTest, SlidingWindowsAssignMultiple) {
    // A record at t=1500ms with window_size=2000, slide=1000
    // Should be in windows: [0,2000) and [1000,3000)
    Oracle oracle(OracleConfig{
        .window_size = Duration{2000},
        .slide = Duration{1000},
    });

    oracle.AddRecord(Record{
        .key = "key-X",
        .value = 100,
        .event_time = Timestamp{Duration{1500}},
        .processing_time = Timestamp{Duration{1500}},
    });

    auto results = oracle.ComputeResults();

    // Record should appear in 2 windows
    ASSERT_EQ(results.size(), 2u);

    // Both windows contain key-X with value=100, count=1
    for (const auto& r : results) {
        EXPECT_EQ(r.key, "key-X");
        EXPECT_EQ(r.result.value, 100);
        EXPECT_EQ(r.result.count, 1u);
    }

    // Check window bounds
    EXPECT_EQ(results[0].window.start, Timestamp{Duration{0}});
    EXPECT_EQ(results[0].window.end, Timestamp{Duration{2000}});
    EXPECT_EQ(results[1].window.start, Timestamp{Duration{1000}});
    EXPECT_EQ(results[1].window.end, Timestamp{Duration{3000}});
}

TEST(OracleTest, DifferentialSmoke_10Seeds) {
    // Quick smoke test: 10 seeds × 5000 records should all pass
    DifferentialConfig config{
        .num_seeds = 10,
        .records_per_seed = 5000,
        .num_keys = 5,
        .window_size = Duration{1000},
        .max_disorder = Duration{300},
        .verbose = false,
    };

    auto result = RunDifferential(config);

    EXPECT_EQ(result.seeds_tested, 10u);
    EXPECT_EQ(result.seeds_passed, 10u);
    EXPECT_EQ(result.seeds_failed, 0u);
    EXPECT_TRUE(result.failed_seeds.empty());
}

}  // namespace
}  // namespace stormglass

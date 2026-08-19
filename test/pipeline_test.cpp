#include <gtest/gtest.h>
#include "source/generator.h"
#include "window/tumbling.h"
#include "sink/memory_sink.h"
#include "engine/pipeline.h"

#include <algorithm>
#include <unordered_map>

namespace stormglass {
namespace {

TEST(Pipeline, EndToEnd_NoRecordsLost) {
    GeneratorConfig config{
        .seed = 42,
        .num_keys = 5,
        .num_records = 10000,
        .max_disorder = Duration{500},
        .batch_size = 512,
        .watermark_interval = 50,
    };

    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto stats = pipeline.Run();

    // All records processed
    EXPECT_EQ(stats.records_processed, 10000u);

    // Sum of all result counts == num_records (no records lost)
    uint64_t total_count = 0;
    for (const auto& r : sink_ptr->Results()) {
        total_count += r.result.count;
    }
    EXPECT_EQ(total_count, 10000u);
}

TEST(Pipeline, EndToEnd_ValueConservation) {
    GeneratorConfig config{
        .seed = 77,
        .num_keys = 5,
        .num_records = 5000,
        .max_disorder = Duration{200},
        .batch_size = 256,
        .watermark_interval = 25,
    };

    // First, generate all values and track per-key sums
    DeterministicGenerator gen(config);
    std::unordered_map<std::string, int64_t> expected_sums;
    while (auto batch = gen.Next()) {
        for (auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                auto& r = std::get<Record>(item);
                expected_sums[r.key] += r.value;
            }
        }
    }

    // Now run through the pipeline
    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    pipeline.Run();

    // Sum pipeline results per key
    std::unordered_map<std::string, int64_t> actual_sums;
    for (const auto& r : sink_ptr->Results()) {
        actual_sums[r.key] += r.result.value;
    }

    // Verify value conservation per key
    ASSERT_EQ(actual_sums.size(), expected_sums.size());
    for (const auto& [key, expected] : expected_sums) {
        ASSERT_TRUE(actual_sums.count(key)) << "Missing key: " << key;
        EXPECT_EQ(actual_sums[key], expected) << "Value mismatch for key: " << key;
    }
}

TEST(Pipeline, EndToEnd_AllResultsPositive) {
    GeneratorConfig config{
        .seed = 55,
        .num_keys = 3,
        .num_records = 3000,
        .max_disorder = Duration{300},
        .batch_size = 256,
        .watermark_interval = 30,
    };

    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto stats = pipeline.Run();

    // All fired window results have count > 0
    for (const auto& r : sink_ptr->Results()) {
        EXPECT_GT(r.result.count, 0u)
            << "Window result for key=" << r.key << " has zero count";
        EXPECT_GT(r.result.value, 0)
            << "Window result for key=" << r.key << " has non-positive sum";
    }

    // Should have fired some windows
    EXPECT_GT(stats.windows_fired, 0u);
    EXPECT_GT(stats.watermarks_advanced, 0u);
}

TEST(Pipeline, EndToEnd_WatermarksAdvance) {
    GeneratorConfig config{
        .seed = 42,
        .num_keys = 5,
        .num_records = 10000,
        .max_disorder = Duration{500},
        .batch_size = 1024,
        .watermark_interval = 100,
    };

    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto stats = pipeline.Run();

    // With 10K records and wm_interval=100, expect ~100 watermark advancements
    // (some might not advance if watermark hasn't moved forward)
    EXPECT_GT(stats.watermarks_advanced, 50u);
}

TEST(Pipeline, EndToEnd_WindowsFired) {
    GeneratorConfig config{
        .seed = 42,
        .num_keys = 10,
        .num_records = 100000,
        .max_disorder = Duration{5000},
        .batch_size = 1024,
        .watermark_interval = 100,
    };

    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto stats = pipeline.Run();

    // 100K records at 1ms apart = 100s timespan, 1s windows = ~100 windows expected
    // (some fired by watermark, some by final flush)
    EXPECT_GT(stats.windows_fired, 80u);
    EXPECT_LT(stats.windows_fired, 120u);
}

} // namespace
} // namespace stormglass

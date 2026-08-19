#include "source/generator.h"
#include "window/tumbling.h"
#include "window/sliding.h"
#include "sink/stdout_sink.h"
#include "engine/pipeline.h"

#include <chrono>
#include <cstdio>
#include <memory>

using namespace stormglass;

struct CollectingSink : public Sink {
    std::vector<WindowResult> results;
    void Emit(const WindowResult& r) override { results.push_back(r); }
    void Flush() override {}
};

static void RunTumblingDemo() {
    std::printf("=== Demo 1: Tumbling Windows (Phase 1a baseline) ===\n\n");

    GeneratorConfig config{
        .seed = 42,
        .num_keys = 10,
        .num_records = 100000,
        .max_disorder = Duration{5000},
        .batch_size = 1024,
        .watermark_interval = 100,
    };

    std::printf("Config: %u keys, %lu records, %ldms disorder, 1s tumbling windows\n",
                config.num_keys,
                static_cast<unsigned long>(config.num_records),
                static_cast<long>(config.max_disorder.count()));

    auto sink = std::make_unique<CollectingSink>();
    auto* sink_ptr = sink.get();

    auto start = std::chrono::steady_clock::now();
    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(config),
        std::make_unique<TumblingAssigner>(Duration{1000}),
        std::move(sink));
    auto stats = pipeline.Run();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    std::printf("  Records processed: %lu\n", static_cast<unsigned long>(stats.records_processed));
    std::printf("  Windows fired:     %lu\n", static_cast<unsigned long>(stats.windows_fired));
    std::printf("  Watermarks adv:    %lu\n", static_cast<unsigned long>(stats.watermarks_advanced));
    std::printf("  Total results:     %zu\n", sink_ptr->results.size());
    std::printf("  Wall-clock:        %ldms\n\n", static_cast<long>(elapsed));
}

static void RunSlidingDemo() {
    std::printf("=== Demo 2: Sliding Windows (5s window, 2s slide) ===\n\n");

    GeneratorConfig config{
        .seed = 77,
        .num_keys = 5,
        .num_records = 10000,
        .max_disorder = Duration{2000},
        .batch_size = 512,
        .watermark_interval = 50,
    };

    std::printf("Config: %u keys, %lu records, %ldms disorder, 5s window / 2s slide\n",
                config.num_keys,
                static_cast<unsigned long>(config.num_records),
                static_cast<long>(config.max_disorder.count()));
    std::printf("  -> Each record contributes to ceil(5000/2000) = 3 windows\n");

    auto sink = std::make_unique<CollectingSink>();
    auto* sink_ptr = sink.get();

    auto start = std::chrono::steady_clock::now();
    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(config),
        std::make_unique<SlidingAssigner>(Duration{5000}, Duration{2000}),
        std::move(sink));
    auto stats = pipeline.Run();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    // Sum of all result counts should be ~3x num_records (each record in 3 windows)
    uint64_t total_count = 0;
    for (const auto& r : sink_ptr->results) {
        total_count += r.result.count;
    }

    std::printf("  Records processed: %lu\n", static_cast<unsigned long>(stats.records_processed));
    std::printf("  Windows fired:     %lu\n", static_cast<unsigned long>(stats.windows_fired));
    std::printf("  Total results:     %zu\n", sink_ptr->results.size());
    std::printf("  Sum of counts:     %lu (expect ~%lu = 3x records)\n",
                static_cast<unsigned long>(total_count),
                static_cast<unsigned long>(config.num_records * 3));
    std::printf("  Wall-clock:        %ldms\n\n", static_cast<long>(elapsed));
}

static void RunLateDataDemo() {
    std::printf("=== Demo 3: Late-Data Policy (1s tumbling, 2s lateness) ===\n\n");

    // Use high disorder so some records arrive late (event_time jitter up to -3s)
    GeneratorConfig config{
        .seed = 55,
        .num_keys = 5,
        .num_records = 10000,
        .max_disorder = Duration{3000},
        .batch_size = 256,
        .watermark_interval = 25,
    };

    PipelineConfig pipeline_config{
        .allowed_lateness = Duration{2000},
    };

    std::printf("Config: %u keys, %lu records, %ldms disorder, 1s tumbling, 2s lateness\n",
                config.num_keys,
                static_cast<unsigned long>(config.num_records),
                static_cast<long>(config.max_disorder.count()));
    std::printf("  -> Watermark = max_seen - 3s, window = 1s, lateness = 2s\n");
    std::printf("  -> Records can be up to 3s out-of-order, windows tolerate 2s late\n");

    auto sink = std::make_unique<CollectingSink>();
    auto* sink_ptr = sink.get();

    auto start = std::chrono::steady_clock::now();
    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(config),
        std::make_unique<TumblingAssigner>(Duration{1000}),
        std::move(sink),
        pipeline_config);
    auto stats = pipeline.Run();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - start).count();

    std::printf("  Records processed:      %lu\n", static_cast<unsigned long>(stats.records_processed));
    std::printf("  Windows fired:          %lu\n", static_cast<unsigned long>(stats.windows_fired));
    std::printf("  Windows re-fired:       %lu\n", static_cast<unsigned long>(stats.windows_refired));
    std::printf("  Late records accepted:  %lu\n", static_cast<unsigned long>(stats.late_records_accepted));
    std::printf("  Late records dropped:   %lu\n", static_cast<unsigned long>(stats.late_records_dropped));
    std::printf("  Watermarks adv:         %lu\n", static_cast<unsigned long>(stats.watermarks_advanced));
    std::printf("  Total results:          %zu\n", sink_ptr->results.size());
    std::printf("  Wall-clock:             %ldms\n\n", static_cast<long>(elapsed));
}

int main() {
    std::printf("stormglass v0.2.0 — Phase 2 demo\n");
    std::printf("================================\n\n");

    RunTumblingDemo();
    RunSlidingDemo();
    RunLateDataDemo();

    return 0;
}

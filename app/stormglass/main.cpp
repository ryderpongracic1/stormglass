#include "source/generator.h"
#include "window/tumbling.h"
#include "sink/stdout_sink.h"
#include "engine/pipeline.h"

#include <chrono>
#include <cstdio>
#include <memory>

int main() {
    using namespace stormglass;

    std::printf("stormglass v0.1.0 — Phase 1a demo\n");
    std::printf("==================================\n\n");

    GeneratorConfig config{
        .seed = 42,
        .num_keys = 10,
        .num_records = 100000,
        .max_disorder = Duration{5000},
        .batch_size = 1024,
        .watermark_interval = 100,
    };

    std::printf("Config: %u keys, %lu records, %ldms disorder, %ums batch, %ums wm interval\n",
                config.num_keys,
                static_cast<unsigned long>(config.num_records),
                static_cast<long>(config.max_disorder.count()),
                config.batch_size,
                config.watermark_interval);
    std::printf("Window: 1s tumbling\n\n");

    // Use a MemorySink to collect results, then print last 5
    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});

    // Use a custom sink that collects + prints last N
    struct CollectingSink : public Sink {
        std::vector<WindowResult> results;
        void Emit(const WindowResult& r) override { results.push_back(r); }
        void Flush() override {}
    };

    auto sink = std::make_unique<CollectingSink>();
    auto* sink_ptr = sink.get();

    auto start = std::chrono::steady_clock::now();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto stats = pipeline.Run();

    auto end = std::chrono::steady_clock::now();
    auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

    // Print last 5 window results
    std::printf("--- Last 5 window results ---\n");
    auto& results = sink_ptr->results;
    size_t start_idx = results.size() > 5 ? results.size() - 5 : 0;
    for (size_t i = start_idx; i < results.size(); ++i) {
        auto& r = results[i];
        std::printf("  window=[%ld, %ld) key=%s sum=%ld count=%lu\n",
                    static_cast<long>(r.window.start.time_since_epoch().count()),
                    static_cast<long>(r.window.end.time_since_epoch().count()),
                    r.key.c_str(),
                    static_cast<long>(r.result.value),
                    static_cast<unsigned long>(r.result.count));
    }

    std::printf("\n--- Stats ---\n");
    std::printf("  Records processed: %lu\n", static_cast<unsigned long>(stats.records_processed));
    std::printf("  Windows fired:     %lu\n", static_cast<unsigned long>(stats.windows_fired));
    std::printf("  Watermarks adv:    %lu\n", static_cast<unsigned long>(stats.watermarks_advanced));
    std::printf("  Total results:     %zu\n", results.size());
    std::printf("  Wall-clock:        %ldms\n", static_cast<long>(elapsed_ms));

    return 0;
}

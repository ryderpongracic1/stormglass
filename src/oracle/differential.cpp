#include "oracle/differential.h"
#include "oracle/oracle.h"
#include "engine/pipeline.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "window/tumbling.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <variant>

namespace stormglass {

std::string CompareResults(const std::vector<WindowResult>& engine,
                           const std::vector<WindowResult>& oracle) {
    if (engine.size() != oracle.size()) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
                      "result count mismatch: engine=%zu, oracle=%zu",
                      engine.size(), oracle.size());
        return buf;
    }

    for (size_t i = 0; i < engine.size(); ++i) {
        const auto& e = engine[i];
        const auto& o = oracle[i];

        if (e.key != o.key) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "result[%zu] key mismatch: engine='%s', oracle='%s'",
                          i, e.key.c_str(), o.key.c_str());
            return buf;
        }

        if (e.window.start != o.window.start || e.window.end != o.window.end) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "result[%zu] window mismatch for key='%s': "
                          "engine=[%lld,%lld), oracle=[%lld,%lld)",
                          i, e.key.c_str(),
                          static_cast<long long>(e.window.start.time_since_epoch().count()),
                          static_cast<long long>(e.window.end.time_since_epoch().count()),
                          static_cast<long long>(o.window.start.time_since_epoch().count()),
                          static_cast<long long>(o.window.end.time_since_epoch().count()));
            return buf;
        }

        if (e.result.value != o.result.value) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "result[%zu] sum mismatch for key='%s' window=[%lld,%lld): "
                          "engine=%lld, oracle=%lld",
                          i, e.key.c_str(),
                          static_cast<long long>(e.window.start.time_since_epoch().count()),
                          static_cast<long long>(e.window.end.time_since_epoch().count()),
                          static_cast<long long>(e.result.value),
                          static_cast<long long>(o.result.value));
            return buf;
        }

        if (e.result.count != o.result.count) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "result[%zu] count mismatch for key='%s' window=[%lld,%lld): "
                          "engine=%llu, oracle=%llu",
                          i, e.key.c_str(),
                          static_cast<long long>(e.window.start.time_since_epoch().count()),
                          static_cast<long long>(e.window.end.time_since_epoch().count()),
                          static_cast<unsigned long long>(e.result.count),
                          static_cast<unsigned long long>(o.result.count));
            return buf;
        }
    }

    return "";  // Match
}

/// Sort results deterministically by (window.start, key)
static void SortResults(std::vector<WindowResult>& results) {
    std::sort(results.begin(), results.end(),
              [](const WindowResult& a, const WindowResult& b) {
                  if (a.window.start != b.window.start)
                      return a.window.start < b.window.start;
                  return a.key < b.key;
              });
}

DifferentialResult RunDifferential(const DifferentialConfig& config) {
    DifferentialResult result{};

    for (uint64_t seed = 1; seed <= config.num_seeds; ++seed) {
        result.seeds_tested++;

        // --- Engine path ---
        GeneratorConfig gen_config{
            .seed = seed,
            .num_keys = config.num_keys,
            .num_records = config.records_per_seed,
            .max_disorder = config.max_disorder,
            .batch_size = 1024,
            .watermark_interval = 100,
        };

        auto engine_sink = std::make_unique<MemorySink>();
        auto* sink_ptr = engine_sink.get();

        auto pipeline = Pipeline(
            std::make_unique<DeterministicGenerator>(gen_config),
            std::make_unique<TumblingAssigner>(config.window_size),
            std::move(engine_sink));

        auto stats = pipeline.Run();

        auto engine_results = sink_ptr->Results();
        SortResults(engine_results);

        // --- Oracle path ---
        Oracle oracle(OracleConfig{
            .window_size = config.window_size,
            .slide = Duration{0},
            .allowed_lateness = config.allowed_lateness,
        });

        // Replay the same generator, feeding records to oracle
        DeterministicGenerator replay_gen(gen_config);
        while (auto batch = replay_gen.Next()) {
            for (const auto& item : batch->items) {
                if (std::holds_alternative<Record>(item)) {
                    oracle.AddRecord(std::get<Record>(item));
                } else if (std::holds_alternative<ControlRecord>(item)) {
                    const auto& ctrl = std::get<ControlRecord>(item);
                    if (ctrl.type == ControlType::kWatermark) {
                        oracle.AdvanceWatermark(ctrl.watermark);
                    }
                }
            }
        }

        auto oracle_results = oracle.ComputeResults();

        // --- Compare ---
        auto mismatch = CompareResults(engine_results, oracle_results);

        if (mismatch.empty()) {
            result.seeds_passed++;
            if (config.verbose) {
                std::printf("  [seed %llu/%llu] PASS (%zu results)\n",
                            static_cast<unsigned long long>(seed),
                            static_cast<unsigned long long>(config.num_seeds),
                            engine_results.size());
            }
        } else {
            result.seeds_failed++;
            result.failed_seeds.push_back(seed);
            if (result.failure_detail.empty()) {
                char buf[512];
                std::snprintf(buf, sizeof(buf), "seed %llu: %s",
                              static_cast<unsigned long long>(seed),
                              mismatch.c_str());
                result.failure_detail = buf;
            }
            if (config.verbose) {
                std::printf("  [seed %llu/%llu] FAIL: %s\n",
                            static_cast<unsigned long long>(seed),
                            static_cast<unsigned long long>(config.num_seeds),
                            mismatch.c_str());
            }
        }
    }

    return result;
}

}  // namespace stormglass

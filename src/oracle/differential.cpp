#include "oracle/differential.h"
#include "oracle/oracle.h"
#include "engine/pipeline.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "window/sliding.h"
#include "window/tumbling.h"

#include <algorithm>
#include <cstdio>
#include <memory>
#include <unordered_map>
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

std::vector<WindowResult> DedupEngineResults(const std::vector<WindowResult>& results) {
    std::unordered_map<KeyWindow, WindowResult, KeyWindowHash> best;
    best.reserve(results.size());
    for (const auto& r : results) {
        KeyWindow kw{r.key, r.window};
        auto it = best.find(kw);
        if (it == best.end() || r.result.count > it->second.result.count) {
            best[kw] = r;
        }
    }

    std::vector<WindowResult> out;
    out.reserve(best.size());
    for (auto& [kw, r] : best) {
        out.push_back(r);
    }
    return out;
}

DifferentialResult RunDifferential(const DifferentialConfig& config) {
    DifferentialResult result{};

    const bool sliding = config.assigner == AssignerType::kSliding;
    const uint64_t last_seed = config.seed_start + config.num_seeds - 1;

    for (uint64_t seed = config.seed_start; seed <= last_seed; ++seed) {
        result.seeds_tested++;

        GeneratorConfig gen_config{
            .seed = seed,
            .num_keys = config.num_keys,
            .num_records = config.records_per_seed,
            .max_disorder = config.max_disorder,
            .batch_size = 1024,
            .watermark_interval = 100,
            .disorder_mode = config.disorder_mode,
            .late_fraction = config.late_fraction,
            .late_tail = config.late_tail,
        };

        auto make_assigner = [&]() -> std::unique_ptr<WindowAssigner> {
            if (sliding) {
                return std::make_unique<SlidingAssigner>(config.window_size, config.slide);
            }
            return std::make_unique<TumblingAssigner>(config.window_size);
        };

        // --- Engine path ---
        auto engine_sink = std::make_unique<MemorySink>();
        auto* sink_ptr = engine_sink.get();

        auto pipeline = Pipeline(
            std::make_unique<DeterministicGenerator>(gen_config),
            make_assigner(),
            std::move(engine_sink),
            PipelineConfig{.allowed_lateness = config.allowed_lateness});

        auto stats = pipeline.Run();

        auto engine_results = DedupEngineResults(sink_ptr->Results());
        SortResults(engine_results);

        // --- Oracle path ---
        Oracle oracle(OracleConfig{
            .window_size = config.window_size,
            .slide = sliding ? config.slide : Duration{0},
            .allowed_lateness = config.allowed_lateness,
        });

        // Replay the same generator, feeding records and watermarks to oracle
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

        // Accumulate late-data evidence
        result.engine_late_dropped += stats.late_records_dropped;
        result.engine_late_accepted += stats.late_records_accepted;
        result.engine_windows_refired += stats.windows_refired;
        result.oracle_predicted_drops += oracle.PredictedDropCount();

        // --- Compare window results ---
        auto mismatch = CompareResults(engine_results, oracle_results);

        // --- Drop-count contract ---
        // The engine drops beyond-deadline records at every lateness setting
        // (L == 0 means deadline == window.end), so engine and oracle must
        // agree on the count in every cell.
        if (mismatch.empty() &&
            stats.late_records_dropped != oracle.PredictedDropCount()) {
            char buf[256];
            std::snprintf(buf, sizeof(buf),
                          "drop-count mismatch: engine=%llu, oracle=%llu",
                          static_cast<unsigned long long>(stats.late_records_dropped),
                          static_cast<unsigned long long>(oracle.PredictedDropCount()));
            mismatch = buf;
        }

        if (mismatch.empty()) {
            result.seeds_passed++;
            if (config.verbose) {
                std::printf("  [seed %llu] PASS (%zu results)\n",
                            static_cast<unsigned long long>(seed),
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
                std::printf("  [seed %llu] FAIL: %s\n",
                            static_cast<unsigned long long>(seed),
                            mismatch.c_str());
            }
        }
    }

    return result;
}

}  // namespace stormglass

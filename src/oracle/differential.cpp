#include "oracle/differential.h"
#include "oracle/oracle.h"
#include "engine/pipeline.h"
#include "engine/partitioned_pipeline.h"
#include "sink/memory_sink.h"
#include "source/generator.h"
#include "source/source_merge.h"
#include "window/sliding.h"
#include "window/tumbling.h"

#include <algorithm>
#include <cstdio>
#include <functional>
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

// ===========================================================================
// Shared per-seed building blocks. Both the single-threaded and partitioned
// engine paths, and the cross-N proof, are composed from these — so the oracle
// feeding, workload generation, dedup, and drop-count contract are LITERALLY
// the same code regardless of which engine (or how many workers) produced the
// results. The oracle never learns which engine ran.
// ===========================================================================

namespace {

/// The deterministic workload for one seed. Identical for every engine and for
/// the oracle replay — that is what makes the comparison meaningful.
GeneratorConfig MakeGenConfig(const DifferentialConfig& config, uint64_t seed) {
    return GeneratorConfig{
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
}

/// One assigner instance per call. The single-threaded Pipeline consumes one;
/// PartitionedPipeline calls it once per worker.
std::function<std::unique_ptr<WindowAssigner>()> MakeAssignerFactory(
    const DifferentialConfig& config) {
    const bool sliding = config.assigner == AssignerType::kSliding;
    const Duration window_size = config.window_size;
    const Duration slide = config.slide;
    return [sliding, window_size, slide]() -> std::unique_ptr<WindowAssigner> {
        if (sliding) {
            return std::make_unique<SlidingAssigner>(window_size, slide);
        }
        return std::make_unique<TumblingAssigner>(window_size);
    };
}

/// The engine's contribution to the comparison: the authoritative (deduped,
/// sorted) result set plus the drop-contract inputs. Produced identically no
/// matter which engine ran.
struct EngineRun {
    std::vector<WindowResult> results;  // deduped + sorted
    uint64_t late_dropped = 0;
    uint64_t late_accepted = 0;
    uint64_t windows_refired = 0;
};

EngineRun RunSingleThreadedEngine(const DifferentialConfig& config,
                                  const GeneratorConfig& gen) {
    auto factory = MakeAssignerFactory(config);
    auto engine_sink = std::make_unique<MemorySink>();
    auto* sink_ptr = engine_sink.get();

    auto pipeline = Pipeline(
        std::make_unique<DeterministicGenerator>(gen),
        factory(),
        std::move(engine_sink),
        PipelineConfig{.allowed_lateness = config.allowed_lateness});

    auto stats = pipeline.Run();

    EngineRun out;
    out.results = DedupEngineResults(sink_ptr->Results());
    SortResults(out.results);
    out.late_dropped = stats.late_records_dropped;
    out.late_accepted = stats.late_records_accepted;
    out.windows_refired = stats.windows_refired;
    return out;
}

EngineRun RunPartitionedEngine(const DifferentialConfig& config,
                               const GeneratorConfig& gen,
                               uint32_t workers) {
    auto engine_sink = std::make_unique<MemorySink>();
    auto* sink_ptr = engine_sink.get();

    PartitionedPipeline pipeline(
        std::make_unique<DeterministicGenerator>(gen),
        MakeAssignerFactory(config),
        std::move(engine_sink),
        PartitionedPipelineConfig{.num_workers = workers,
                                  .allowed_lateness = config.allowed_lateness});

    auto stats = pipeline.Run();

    EngineRun out;
    out.results = DedupEngineResults(sink_ptr->Results());
    SortResults(out.results);
    out.late_dropped = stats.late_records_dropped;
    out.late_accepted = stats.late_records_accepted;
    out.windows_refired = stats.windows_refired;
    return out;
}

/// Dispatch to the requested engine. workers <= 1 => single-threaded Pipeline
/// (the original path); workers > 1 => PartitionedPipeline.
EngineRun RunEngine(const DifferentialConfig& config, const GeneratorConfig& gen,
                    uint32_t workers) {
    if (workers <= 1) {
        return RunSingleThreadedEngine(config, gen);
    }
    return RunPartitionedEngine(config, gen, workers);
}

/// The UNCHANGED oracle, fed the same generator's records + broadcast
/// watermarks. Returns its result set and predicted drop count.
struct OracleRun {
    std::vector<WindowResult> results;
    uint64_t drops = 0;
};

OracleRun RunOracleFor(const DifferentialConfig& config, const GeneratorConfig& gen) {
    const bool sliding = config.assigner == AssignerType::kSliding;
    Oracle oracle(OracleConfig{
        .window_size = config.window_size,
        .slide = sliding ? config.slide : Duration{0},
        .allowed_lateness = config.allowed_lateness,
    });

    DeterministicGenerator replay_gen(gen);
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

    OracleRun out;
    out.results = oracle.ComputeResults();
    out.drops = oracle.PredictedDropCount();
    return out;
}

/// The drop-count contract, shared by both harnesses. The engine drops
/// beyond-deadline records at every lateness setting (L == 0 means
/// deadline == window.end), so engine and oracle must agree on the count.
/// Returns a message on violation, empty on agreement. Only meaningful to check
/// when the result sets already matched.
std::string CheckDropContract(uint64_t engine_dropped, uint64_t oracle_dropped) {
    if (engine_dropped == oracle_dropped) return "";
    char buf[256];
    std::snprintf(buf, sizeof(buf),
                  "drop-count mismatch: engine=%llu, oracle=%llu",
                  static_cast<unsigned long long>(engine_dropped),
                  static_cast<unsigned long long>(oracle_dropped));
    return buf;
}

}  // namespace

DifferentialResult RunDifferential(const DifferentialConfig& config) {
    DifferentialResult result{};

    const uint64_t last_seed = config.seed_start + config.num_seeds - 1;

    for (uint64_t seed = config.seed_start; seed <= last_seed; ++seed) {
        result.seeds_tested++;

        const auto gen = MakeGenConfig(config, seed);

        // Engine (single-threaded or partitioned) and the UNCHANGED oracle,
        // over the same workload.
        EngineRun engine = RunEngine(config, gen, config.workers);
        OracleRun oracle = RunOracleFor(config, gen);

        // Accumulate late-data evidence
        result.engine_late_dropped += engine.late_dropped;
        result.engine_late_accepted += engine.late_accepted;
        result.engine_windows_refired += engine.windows_refired;
        result.oracle_predicted_drops += oracle.drops;

        // --- Compare window results, then the drop-count contract ---
        auto mismatch = CompareResults(engine.results, oracle.results);
        if (mismatch.empty()) {
            mismatch = CheckDropContract(engine.late_dropped, oracle.drops);
        }

        if (mismatch.empty()) {
            result.seeds_passed++;
            if (config.verbose) {
                std::printf("  [seed %llu] PASS (%zu results)\n",
                            static_cast<unsigned long long>(seed),
                            engine.results.size());
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

CrossNResult RunCrossN(const DifferentialConfig& config) {
    CrossNResult result{};

    const uint64_t last_seed = config.seed_start + config.num_seeds - 1;

    for (uint64_t seed = config.seed_start; seed <= last_seed; ++seed) {
        result.seeds_tested++;

        const auto gen = MakeGenConfig(config, seed);

        // The oracle and the single-threaded reference are computed once per
        // seed; every N is measured against BOTH.
        OracleRun oracle = RunOracleFor(config, gen);
        EngineRun ref = RunSingleThreadedEngine(config, gen);

        // Reference evidence (all N are asserted equal to it).
        result.engine_late_dropped += ref.late_dropped;
        result.engine_late_accepted += ref.late_accepted;
        result.engine_windows_refired += ref.windows_refired;
        result.oracle_predicted_drops += oracle.drops;

        std::string mismatch;

        // Guard the reference itself: single-threaded must match the oracle.
        mismatch = CompareResults(ref.results, oracle.results);
        if (mismatch.empty()) {
            mismatch = CheckDropContract(ref.late_dropped, oracle.drops);
        }
        if (!mismatch.empty()) {
            mismatch = "single-threaded vs oracle: " + mismatch;
        }

        // Each partitioned N vs the oracle AND vs the single-threaded reference.
        for (uint32_t n : kCrossNWorkers) {
            if (!mismatch.empty()) break;

            EngineRun eng = RunPartitionedEngine(config, gen, n);

            std::string m = CompareResults(eng.results, oracle.results);
            if (m.empty()) {
                m = CheckDropContract(eng.late_dropped, oracle.drops);
            }
            if (!m.empty()) {
                char buf[600];
                std::snprintf(buf, sizeof(buf), "N=%u vs oracle: %s", n, m.c_str());
                mismatch = buf;
                break;
            }

            // Direct cross-N identity: engine(N) == single-threaded reference.
            m = CompareResults(eng.results, ref.results);
            if (!m.empty()) {
                char buf[600];
                std::snprintf(buf, sizeof(buf),
                              "N=%u vs single-threaded: %s", n, m.c_str());
                mismatch = buf;
                break;
            }
        }

        if (mismatch.empty()) {
            result.seeds_passed++;
            if (config.verbose) {
                std::printf("  [seed %llu] PASS all N (%zu results)\n",
                            static_cast<unsigned long long>(seed),
                            ref.results.size());
            }
        } else {
            result.seeds_failed++;
            result.failed_seeds.push_back(seed);
            if (result.failure_detail.empty()) {
                char buf[768];
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

// ===========================================================================
// Multi-source (v3) harness. Builds a SourceMerge of K DeterministicGenerators
// with DIVERGENT event-time rates, runs the engine over it, and feeds the
// UNCHANGED oracle the SAME merged stream. Reuses MakeGenConfig / MakeAssigner-
// Factory / EngineRun / OracleRun / DedupEngineResults / CheckDropContract — the
// oracle-feeding and comparison code is LITERALLY the single-source code; only
// the Source differs (SourceMerge instead of a bare DeterministicGenerator).
// ===========================================================================

namespace {

/// Build the K-source merge config for one seed. Source 0 advances event-time
/// FASTEST (largest step); source K-1 slowest — so the running MIN is pinned by
/// the slow tail and the combine is non-trivial. Per-source seeds are spread so
/// the channels are independent. K == 1 yields exactly MakeGenConfig(config,
/// seed) with step 1, i.e. the single-source workload (regression guard).
SourceMergeConfig MakeMergeConfig(const DifferentialConfig& config,
                                  uint64_t seed, uint32_t k) {
    SourceMergeConfig mc;
    mc.checkpoint_interval = 0;   // differential does not checkpoint
    mc.merged_batch_size = 1024;
    mc.idle_timeout = config.idle_timeout;   // 0 = idleness disabled (Phase-1)
    for (uint32_t i = 0; i < k; ++i) {
        GeneratorConfig g = MakeGenConfig(config, seed + static_cast<uint64_t>(i) * 7919u);
        g.event_time_step = static_cast<int64_t>(k - i);  // source 0 fastest
        mc.sources.push_back(g);
    }
    // v3 Phase 2: give the SLOWEST source (index k-1, which pins the MIN) a single
    // idle span so it is excluded after idle_timeout empty pulls and the merged
    // watermark advances via the faster sources; on resume its below-watermark
    // records are late. Only meaningful with >= 2 sources (a lone source has no
    // faster peer to advance the watermark while it is excluded).
    if (config.idle_timeout > 0 && config.idle_span_length > 0 && k >= 2) {
        mc.sources.back().idle_spans.push_back(
            IdleSpan{config.idle_span_start, config.idle_span_length});
    }
    return mc;
}

EngineRun RunSingleThreadedEngineMerged(const DifferentialConfig& config,
                                        const SourceMergeConfig& mc) {
    auto factory = MakeAssignerFactory(config);
    auto engine_sink = std::make_unique<MemorySink>();
    auto* sink_ptr = engine_sink.get();

    auto pipeline = Pipeline(
        std::make_unique<SourceMerge>(mc),
        factory(),
        std::move(engine_sink),
        PipelineConfig{.allowed_lateness = config.allowed_lateness});

    auto stats = pipeline.Run();

    EngineRun out;
    out.results = DedupEngineResults(sink_ptr->Results());
    SortResults(out.results);
    out.late_dropped = stats.late_records_dropped;
    out.late_accepted = stats.late_records_accepted;
    out.windows_refired = stats.windows_refired;
    return out;
}

EngineRun RunPartitionedEngineMerged(const DifferentialConfig& config,
                                     const SourceMergeConfig& mc, uint32_t workers) {
    auto engine_sink = std::make_unique<MemorySink>();
    auto* sink_ptr = engine_sink.get();

    PartitionedPipeline pipeline(
        std::make_unique<SourceMerge>(mc),
        MakeAssignerFactory(config),
        std::move(engine_sink),
        PartitionedPipelineConfig{.num_workers = workers,
                                  .allowed_lateness = config.allowed_lateness});

    auto stats = pipeline.Run();

    EngineRun out;
    out.results = DedupEngineResults(sink_ptr->Results());
    SortResults(out.results);
    out.late_dropped = stats.late_records_dropped;
    out.late_accepted = stats.late_records_accepted;
    out.windows_refired = stats.windows_refired;
    return out;
}

/// The UNCHANGED oracle, fed the SAME merged stream a second SourceMerge
/// produces: records in merged order via AddRecord, the min-combined watermarks
/// via AdvanceWatermark. The oracle applies its existing lateness logic — it
/// never re-derives min-combine.
OracleRun RunOracleForMerged(const DifferentialConfig& config,
                             const SourceMergeConfig& mc) {
    const bool sliding = config.assigner == AssignerType::kSliding;
    Oracle oracle(OracleConfig{
        .window_size = config.window_size,
        .slide = sliding ? config.slide : Duration{0},
        .allowed_lateness = config.allowed_lateness,
    });

    SourceMerge replay(mc);
    while (auto batch = replay.Next()) {
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

    OracleRun out;
    out.results = oracle.ComputeResults();
    out.drops = oracle.PredictedDropCount();
    return out;
}

}  // namespace

DifferentialResult RunMultiSourceDifferential(const DifferentialConfig& config) {
    DifferentialResult result{};

    const uint32_t k = std::max<uint32_t>(1, config.sources);
    const uint64_t last_seed = config.seed_start + config.num_seeds - 1;

    for (uint64_t seed = config.seed_start; seed <= last_seed; ++seed) {
        result.seeds_tested++;

        const auto mc = MakeMergeConfig(config, seed, k);

        // Engine over the SourceMerge (single-threaded or partitioned), and the
        // UNCHANGED oracle over the SAME merged stream.
        EngineRun engine = (config.workers <= 1)
                               ? RunSingleThreadedEngineMerged(config, mc)
                               : RunPartitionedEngineMerged(config, mc, config.workers);
        OracleRun oracle = RunOracleForMerged(config, mc);

        result.engine_late_dropped += engine.late_dropped;
        result.engine_late_accepted += engine.late_accepted;
        result.engine_windows_refired += engine.windows_refired;
        result.oracle_predicted_drops += oracle.drops;

        auto mismatch = CompareResults(engine.results, oracle.results);
        if (mismatch.empty()) {
            mismatch = CheckDropContract(engine.late_dropped, oracle.drops);
        }

        if (mismatch.empty()) {
            result.seeds_passed++;
            if (config.verbose) {
                std::printf("  [seed %llu] PASS (K=%u, %zu results)\n",
                            static_cast<unsigned long long>(seed), k,
                            engine.results.size());
            }
        } else {
            result.seeds_failed++;
            result.failed_seeds.push_back(seed);
            if (result.failure_detail.empty()) {
                char buf[512];
                std::snprintf(buf, sizeof(buf), "seed %llu (K=%u): %s",
                              static_cast<unsigned long long>(seed), k,
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

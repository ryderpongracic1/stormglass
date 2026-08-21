#include "oracle/differential.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace {

using stormglass::AssignerType;
using stormglass::DifferentialConfig;
using stormglass::DifferentialResult;
using stormglass::DisorderMode;
using stormglass::Duration;

void PrintUsage() {
    std::printf(
        "Usage: stormglass_oracle [options]\n"
        "  --seeds N            Number of seeds to test (default: 100)\n"
        "  --seed-start N       First seed (repro seeds are contiguous, default: 1)\n"
        "  --records N          Records per seed (default: 10000)\n"
        "  --keys N             Number of keys (default: 10)\n"
        "  --window-ms N        Window size in ms (default: 1000)\n"
        "  --window-type T      tumbling | sliding (default: tumbling)\n"
        "  --slide-ms N         Slide in ms, sliding only (default: 500)\n"
        "  --disorder-ms N      Max disorder in ms (default: 500)\n"
        "  --lateness-ms N      Allowed lateness in ms (default: 0)\n"
        "  --disorder-profile P bounded | heavy-tailed (default: bounded)\n"
        "  --late-fraction F    Fraction of heavy-tail late records (default: 0.1)\n"
        "  --late-tail-ms N     Max extra lateness beyond disorder (default: 6000)\n"
        "  --matrix             Run the full {tumbling,sliding} x {L=0,L>0}\n"
        "                       x {bounded,heavy-tailed} matrix and exit\n"
        "  --workers N          Engine workers: 1 = single-threaded Pipeline\n"
        "                       (default), >1 = PartitionedPipeline at N workers\n"
        "  --sources K          Multi-source (v3): merge K DeterministicGenerators\n"
        "                       with divergent event-time rates behind the engine\n"
        "                       (respects --workers), assert engine==oracle over the\n"
        "                       merged stream, exit. K=1 == the single-source path.\n"
        "  --cross-n            Run PartitionedPipeline at N in {1,2,4,8}; assert\n"
        "                       each == oracle AND == single-threaded per seed, exit\n"
        "  --verbose            Print per-seed results\n"
        "  --help               Show this message\n");
}

const char* AssignerName(AssignerType a) {
    return a == AssignerType::kSliding ? "sliding" : "tumbling";
}

const char* DisorderName(DisorderMode d) {
    return d == DisorderMode::kHeavyTailed ? "heavy-tailed" : "bounded";
}

/// Run one configuration and print a one-line summary with late-data evidence.
/// Returns true on pass.
bool RunAndReport(const DifferentialConfig& config, const char* label) {
    auto r = stormglass::RunDifferential(config);
    bool ok = r.seeds_failed == 0;

    std::printf(
        "%-46s %s  %llu/%llu seeds | engine_drop=%llu oracle_drop=%llu "
        "refired=%llu late_accepted=%llu\n",
        label, ok ? "PASS" : "FAIL",
        static_cast<unsigned long long>(r.seeds_passed),
        static_cast<unsigned long long>(r.seeds_tested),
        static_cast<unsigned long long>(r.engine_late_dropped),
        static_cast<unsigned long long>(r.oracle_predicted_drops),
        static_cast<unsigned long long>(r.engine_windows_refired),
        static_cast<unsigned long long>(r.engine_late_accepted));

    if (!ok) {
        std::printf("    FAILED seeds:");
        for (auto s : r.failed_seeds) {
            std::printf(" %llu", static_cast<unsigned long long>(s));
        }
        std::printf("\n    First failure: %s\n", r.failure_detail.c_str());
    }
    return ok;
}

/// Run the multi-source (v3) proof for one configuration and print a one-line
/// summary. Each seed builds a SourceMerge of K generators with divergent
/// event-time rates behind the selected engine, and asserts engine == oracle
/// over the merged stream. Returns true on pass.
bool RunMultiSourceAndReport(const DifferentialConfig& config, const char* label) {
    auto r = stormglass::RunMultiSourceDifferential(config);
    bool ok = r.seeds_failed == 0;

    std::printf(
        "%-46s %s  %llu/%llu seeds | engine_drop=%llu oracle_drop=%llu "
        "refired=%llu late_accepted=%llu\n",
        label, ok ? "PASS" : "FAIL",
        static_cast<unsigned long long>(r.seeds_passed),
        static_cast<unsigned long long>(r.seeds_tested),
        static_cast<unsigned long long>(r.engine_late_dropped),
        static_cast<unsigned long long>(r.oracle_predicted_drops),
        static_cast<unsigned long long>(r.engine_windows_refired),
        static_cast<unsigned long long>(r.engine_late_accepted));

    if (!ok) {
        std::printf("    FAILED seeds:");
        for (auto s : r.failed_seeds) {
            std::printf(" %llu", static_cast<unsigned long long>(s));
        }
        std::printf("\n    First failure: %s\n", r.failure_detail.c_str());
    }
    return ok;
}

/// Run the cross-N proof for one configuration and print a one-line summary.
/// A seed passes only when PartitionedPipeline at EVERY N in {1,2,4,8} matches
/// both the oracle and the single-threaded reference (result set + drop count).
/// Returns true on pass.
bool RunCrossNAndReport(const DifferentialConfig& config, const char* label) {
    auto r = stormglass::RunCrossN(config);
    bool ok = r.seeds_failed == 0;

    std::printf(
        "%-46s %s  %llu/%llu seeds | N={1,2,4,8}: engine(N)==oracle && "
        "engine(N)==single-threaded | engine_drop=%llu oracle_drop=%llu "
        "refired=%llu late_accepted=%llu\n",
        label, ok ? "PASS" : "FAIL",
        static_cast<unsigned long long>(r.seeds_passed),
        static_cast<unsigned long long>(r.seeds_tested),
        static_cast<unsigned long long>(r.engine_late_dropped),
        static_cast<unsigned long long>(r.oracle_predicted_drops),
        static_cast<unsigned long long>(r.engine_windows_refired),
        static_cast<unsigned long long>(r.engine_late_accepted));

    if (!ok) {
        std::printf("    FAILED seeds:");
        for (auto s : r.failed_seeds) {
            std::printf(" %llu", static_cast<unsigned long long>(s));
        }
        std::printf("\n    First failure: %s\n", r.failure_detail.c_str());
    }
    return ok;
}

int RunMatrix(DifferentialConfig base) {
    std::printf(
        "Differential matrix: %llu seeds x %llu records "
        "(window=%lldms slide=%lldms disorder=%lldms)\n\n",
        static_cast<unsigned long long>(base.num_seeds),
        static_cast<unsigned long long>(base.records_per_seed),
        static_cast<long long>(base.window_size.count()),
        static_cast<long long>(base.slide.count()),
        static_cast<long long>(base.max_disorder.count()));

    bool all_ok = true;
    for (auto assigner : {AssignerType::kTumbling, AssignerType::kSliding}) {
        for (auto lateness : {Duration{0}, base.allowed_lateness}) {
            for (auto disorder : {DisorderMode::kBounded, DisorderMode::kHeavyTailed}) {
                DifferentialConfig c = base;
                c.assigner = assigner;
                c.allowed_lateness = lateness;
                c.disorder_mode = disorder;
                if (disorder == DisorderMode::kBounded) {
                    c.late_fraction = 0.0;
                }

                char label[64];
                std::snprintf(label, sizeof(label), "%-8s L=%-5lld %s",
                              AssignerName(assigner),
                              static_cast<long long>(lateness.count()),
                              DisorderName(disorder));
                all_ok &= RunAndReport(c, label);
            }
        }
    }

    std::printf("\nMatrix result: %s\n", all_ok ? "ALL PASS" : "FAILURES PRESENT");
    return all_ok ? 0 : 1;
}

}  // namespace

int main(int argc, char* argv[]) {
    DifferentialConfig config{};
    config.allowed_lateness = Duration{2000};  // default L>0 for the matrix / L runs
    config.late_fraction = 0.1;
    config.late_tail = Duration{6000};
    bool matrix = false;
    bool cross_n = false;
    bool multi_source = false;

    auto need_arg = [&](int i) -> bool {
        if (i + 1 >= argc) {
            std::fprintf(stderr, "Missing value for %s\n", argv[i]);
            return false;
        }
        return true;
    };

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seeds") == 0 && need_arg(i)) {
            config.num_seeds = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--seed-start") == 0 && need_arg(i)) {
            config.seed_start = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--records") == 0 && need_arg(i)) {
            config.records_per_seed = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--keys") == 0 && need_arg(i)) {
            config.num_keys = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--window-ms") == 0 && need_arg(i)) {
            config.window_size = Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--slide-ms") == 0 && need_arg(i)) {
            config.slide = Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--disorder-ms") == 0 && need_arg(i)) {
            config.max_disorder = Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--lateness-ms") == 0 && need_arg(i)) {
            config.allowed_lateness = Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--window-type") == 0 && need_arg(i)) {
            const char* v = argv[++i];
            if (std::strcmp(v, "sliding") == 0) {
                config.assigner = AssignerType::kSliding;
            } else if (std::strcmp(v, "tumbling") == 0) {
                config.assigner = AssignerType::kTumbling;
            } else {
                std::fprintf(stderr, "Unknown window-type: %s\n", v);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--disorder-profile") == 0 && need_arg(i)) {
            const char* v = argv[++i];
            if (std::strcmp(v, "heavy-tailed") == 0) {
                config.disorder_mode = DisorderMode::kHeavyTailed;
            } else if (std::strcmp(v, "bounded") == 0) {
                config.disorder_mode = DisorderMode::kBounded;
            } else {
                std::fprintf(stderr, "Unknown disorder-profile: %s\n", v);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--late-fraction") == 0 && need_arg(i)) {
            config.late_fraction = std::strtod(argv[++i], nullptr);
        } else if (std::strcmp(argv[i], "--late-tail-ms") == 0 && need_arg(i)) {
            config.late_tail = Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--matrix") == 0) {
            matrix = true;
        } else if (std::strcmp(argv[i], "--workers") == 0 && need_arg(i)) {
            config.workers = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--cross-n") == 0) {
            cross_n = true;
        } else if (std::strcmp(argv[i], "--sources") == 0 && need_arg(i)) {
            config.sources = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
            multi_source = true;
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            config.verbose = true;
        } else if (std::strcmp(argv[i], "--help") == 0) {
            PrintUsage();
            return 0;
        } else {
            std::fprintf(stderr, "Unknown or malformed option: %s\n", argv[i]);
            PrintUsage();
            return 1;
        }
    }

    if (matrix) {
        return RunMatrix(config);
    }

    if (multi_source) {
        const uint32_t k = config.sources < 1 ? 1 : config.sources;
        std::printf(
            "Multi-source (v3) differential: %llu seeds from %llu, %llu records "
            "per source each, K=%u sources (%s, window=%lldms, L=%lldms, %s, "
            "workers=%u); engine==oracle over the merged stream\n",
            static_cast<unsigned long long>(config.num_seeds),
            static_cast<unsigned long long>(config.seed_start),
            static_cast<unsigned long long>(config.records_per_seed),
            k,
            AssignerName(config.assigner),
            static_cast<long long>(config.window_size.count()),
            static_cast<long long>(config.allowed_lateness.count()),
            DisorderName(config.disorder_mode),
            config.workers);
        bool ok = RunMultiSourceAndReport(config, "multi-source");
        return ok ? 0 : 1;
    }

    if (cross_n) {
        std::printf(
            "Cross-N proof: %llu seeds from %llu, %llu records each "
            "(%s, window=%lldms, L=%lldms, %s); N in {1,2,4,8}\n",
            static_cast<unsigned long long>(config.num_seeds),
            static_cast<unsigned long long>(config.seed_start),
            static_cast<unsigned long long>(config.records_per_seed),
            AssignerName(config.assigner),
            static_cast<long long>(config.window_size.count()),
            static_cast<long long>(config.allowed_lateness.count()),
            DisorderName(config.disorder_mode));
        bool ok = RunCrossNAndReport(config, "cross-n");
        return ok ? 0 : 1;
    }

    std::printf(
        "Differential test: %llu seeds from %llu, %llu records each "
        "(%s, window=%lldms, L=%lldms, %s, workers=%u)\n",
        static_cast<unsigned long long>(config.num_seeds),
        static_cast<unsigned long long>(config.seed_start),
        static_cast<unsigned long long>(config.records_per_seed),
        AssignerName(config.assigner),
        static_cast<long long>(config.window_size.count()),
        static_cast<long long>(config.allowed_lateness.count()),
        DisorderName(config.disorder_mode),
        config.workers);

    bool ok = RunAndReport(config, "run");
    return ok ? 0 : 1;
}

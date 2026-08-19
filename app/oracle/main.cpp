#include "oracle/differential.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

static void PrintUsage() {
    std::printf(
        "Usage: stormglass_oracle [options]\n"
        "  --seeds N       Number of seeds to test (default: 100)\n"
        "  --records N     Records per seed (default: 10000)\n"
        "  --keys N        Number of keys (default: 10)\n"
        "  --window-ms N   Window size in ms (default: 1000)\n"
        "  --disorder-ms N Max disorder in ms (default: 500)\n"
        "  --verbose       Print per-seed results\n"
        "  --help          Show this message\n");
}

int main(int argc, char* argv[]) {
    stormglass::DifferentialConfig config{};

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            config.num_seeds = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            config.records_per_seed = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--keys") == 0 && i + 1 < argc) {
            config.num_keys = static_cast<uint32_t>(std::strtoul(argv[++i], nullptr, 10));
        } else if (std::strcmp(argv[i], "--window-ms") == 0 && i + 1 < argc) {
            config.window_size = stormglass::Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--disorder-ms") == 0 && i + 1 < argc) {
            config.max_disorder = stormglass::Duration{std::strtoll(argv[++i], nullptr, 10)};
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            config.verbose = true;
        } else if (std::strcmp(argv[i], "--help") == 0) {
            PrintUsage();
            return 0;
        } else {
            std::fprintf(stderr, "Unknown option: %s\n", argv[i]);
            PrintUsage();
            return 1;
        }
    }

    std::printf("Differential test: %llu seeds, %llu records each\n",
                static_cast<unsigned long long>(config.num_seeds),
                static_cast<unsigned long long>(config.records_per_seed));

    auto result = stormglass::RunDifferential(config);

    std::printf("\nResult: %llu/%llu seeds passed, %llu failed\n",
                static_cast<unsigned long long>(result.seeds_passed),
                static_cast<unsigned long long>(result.seeds_tested),
                static_cast<unsigned long long>(result.seeds_failed));

    if (!result.failed_seeds.empty()) {
        std::printf("  FAILED seeds:");
        for (auto s : result.failed_seeds) {
            std::printf(" %llu", static_cast<unsigned long long>(s));
        }
        std::printf("\n  First failure: %s\n", result.failure_detail.c_str());
        return 1;
    }

    return 0;
}

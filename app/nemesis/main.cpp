#include "nemesis/nemesis.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace stormglass;

void PrintUsage() {
    std::fprintf(stderr,
        "Usage: stormglass_nemesis [options]\n"
        "  --seeds N              Number of nemesis runs (default: 20)\n"
        "  --records N            Records per run (default: 50000)\n"
        "  --checkpoint-interval N  (default: 5000)\n"
        "  --phase <mid-checkpoint|mid-emission|between>  (default: between)\n"
        "  --verbose              Print per-run details\n"
        "  --help                 Show this help\n");
}

int main(int argc, char* argv[]) {
    uint64_t num_seeds = 20;
    uint64_t num_records = 50000;
    uint64_t checkpoint_interval = 5000;
    NemesisPhase phase = NemesisPhase::kBetweenCheckpoints;
    bool verbose = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--seeds") == 0 && i + 1 < argc) {
            num_seeds = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--records") == 0 && i + 1 < argc) {
            num_records = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--checkpoint-interval") == 0 && i + 1 < argc) {
            checkpoint_interval = std::strtoull(argv[++i], nullptr, 10);
        } else if (std::strcmp(argv[i], "--phase") == 0 && i + 1 < argc) {
            ++i;
            if (std::strcmp(argv[i], "mid-checkpoint") == 0) {
                phase = NemesisPhase::kMidCheckpoint;
            } else if (std::strcmp(argv[i], "mid-emission") == 0) {
                phase = NemesisPhase::kMidEmission;
            } else if (std::strcmp(argv[i], "between") == 0) {
                phase = NemesisPhase::kBetweenCheckpoints;
            } else {
                std::fprintf(stderr, "Unknown phase: %s\n", argv[i]);
                return 1;
            }
        } else if (std::strcmp(argv[i], "--verbose") == 0) {
            verbose = true;
        } else if (std::strcmp(argv[i], "--help") == 0) {
            PrintUsage();
            return 0;
        } else {
            std::fprintf(stderr, "Unknown option: %s\n", argv[i]);
            PrintUsage();
            return 1;
        }
    }

    const char* phase_name = "between checkpoints";
    if (phase == NemesisPhase::kMidCheckpoint) phase_name = "mid-checkpoint";
    else if (phase == NemesisPhase::kMidEmission) phase_name = "mid-emission";

    std::printf("Nemesis test: %lu runs, kill %s\n", num_seeds, phase_name);

    uint64_t passed = 0;
    uint64_t total_duplicates = 0;
    uint64_t total_missing = 0;

    for (uint64_t i = 0; i < num_seeds; ++i) {
        NemesisConfig config{};
        config.seed = 42 + i;  // Different seed each run
        config.num_records = num_records;
        config.checkpoint_interval = checkpoint_interval;
        config.kill_phase = phase;
        config.kill_position = 0.3 + 0.4 * (static_cast<double>(i) / static_cast<double>(num_seeds));

        auto result = RunNemesis(config);

        if (verbose) {
            std::printf("  [run %lu/%lu] %s (killed at %lu, restored, %lu duplicates, %lu missing)",
                        i + 1, num_seeds,
                        result.passed ? "PASS" : "FAIL",
                        result.records_before_kill,
                        result.duplicates_at_sink,
                        result.missing_results);
            if (!result.passed && !result.failure_detail.empty()) {
                std::printf(" — %s", result.failure_detail.c_str());
            }
            std::printf("\n");
        }

        if (result.passed) ++passed;
        total_duplicates += result.duplicates_at_sink;
        total_missing += result.missing_results;
    }

    std::printf("\nResult: %lu/%lu passed, %lu missing results across all runs\n",
                passed, num_seeds, total_missing);
    std::printf("Total duplicates: %lu (at-least-once working as designed)\n",
                total_duplicates);

    return (passed == num_seeds) ? 0 : 1;
}

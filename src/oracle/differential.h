#pragma once

#include "sink/sink.h"
#include "stream/record.h"

#include <cstdint>
#include <string>
#include <vector>

namespace stormglass {

struct DifferentialConfig {
    uint64_t num_seeds = 100;
    uint64_t records_per_seed = 10000;
    uint32_t num_keys = 10;
    Duration window_size{1000};    // 1s
    Duration max_disorder{500};    // 500ms
    Duration allowed_lateness{0};
    bool verbose = false;
};

struct DifferentialResult {
    uint64_t seeds_tested = 0;
    uint64_t seeds_passed = 0;
    uint64_t seeds_failed = 0;
    std::vector<uint64_t> failed_seeds;  // repro seeds
    std::string failure_detail;          // first mismatch description
};

/// Compare two sorted result vectors.
/// Returns empty string on match, mismatch description on failure.
std::string CompareResults(const std::vector<WindowResult>& engine,
                           const std::vector<WindowResult>& oracle);

/// Run N seeded tests comparing engine output vs oracle.
/// For each seed, generates deterministic records, runs both the pipeline
/// and the oracle, then compares results.
///
/// Failed seeds are REPRO seeds — run with --seeds 1 --seed-start N to reproduce.
DifferentialResult RunDifferential(const DifferentialConfig& config);

}  // namespace stormglass

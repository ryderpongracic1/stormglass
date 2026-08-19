#pragma once

#include "stream/record.h"

#include <cstdint>
#include <string>

namespace stormglass {

enum class NemesisPhase {
    kMidCheckpoint,     // Kill during checkpoint write (simulated: leave .tmp file)
    kMidEmission,       // Kill during window emission
    kBetweenCheckpoints // Kill between checkpoints (normal operation)
};

struct NemesisConfig {
    uint64_t seed = 42;
    uint64_t num_records = 50000;
    uint32_t num_keys = 10;
    Duration window_size{1000};
    Duration max_disorder{500};
    uint64_t checkpoint_interval = 5000;
    NemesisPhase kill_phase = NemesisPhase::kBetweenCheckpoints;
    double kill_position = 0.5;  // Kill at ~50% progress
};

struct NemesisResult {
    bool passed = false;
    uint64_t records_before_kill = 0;
    uint64_t records_after_restore = 0;
    uint64_t duplicates_at_sink = 0;
    uint64_t missing_results = 0;  // Should be 0 for at-least-once
    std::string failure_detail;
};

// Run a single nemesis crash-recovery test.
// 1. Runs pipeline with checkpointing, stops at kill_position
// 2. Optionally corrupts the in-flight checkpoint (.tmp simulation)
// 3. Restarts pipeline from checkpoint, runs to completion
// 4. Compares combined output against oracle (at-least-once: zero missing)
NemesisResult RunNemesis(const NemesisConfig& config);

} // namespace stormglass

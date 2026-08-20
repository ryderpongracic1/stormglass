#pragma once

#include "sink/sink.h"
#include "source/generator.h"
#include "stream/record.h"

#include <cstdint>
#include <string>
#include <vector>

namespace stormglass {

/// Which window assigner both the engine and oracle use for a run.
enum class AssignerType : uint8_t { kTumbling, kSliding };

struct DifferentialConfig {
    uint64_t num_seeds = 100;
    uint64_t records_per_seed = 10000;
    uint32_t num_keys = 10;
    Duration window_size{1000};    // 1s
    Duration max_disorder{500};    // 500ms
    Duration allowed_lateness{0};
    bool verbose = false;

    // Window shape. slide is only used when assigner == kSliding.
    AssignerType assigner = AssignerType::kTumbling;
    Duration slide{500};           // 500ms (sliding only)

    // Disorder profile (see DisorderMode). Defaults reproduce the original
    // bounded, never-late workload.
    DisorderMode disorder_mode = DisorderMode::kBounded;
    double late_fraction = 0.0;
    Duration late_tail{0};

    // First seed to run (repro seeds are contiguous from here).
    uint64_t seed_start = 1;
};

struct DifferentialResult {
    uint64_t seeds_tested = 0;
    uint64_t seeds_passed = 0;
    uint64_t seeds_failed = 0;
    std::vector<uint64_t> failed_seeds;  // repro seeds
    std::string failure_detail;          // first mismatch description

    // Late-data evidence, summed across all seeds. Proves the workload actually
    // exercises the late-drop / re-fire paths rather than "testing nothing".
    uint64_t engine_late_dropped = 0;    // engine stats.late_records_dropped
    uint64_t oracle_predicted_drops = 0; // oracle PredictedDropCount()
    uint64_t engine_windows_refired = 0; // within-lateness records re-included
    uint64_t engine_late_accepted = 0;   // engine stats.late_records_accepted
};

/// Compare two sorted result vectors.
/// Returns empty string on match, mismatch description on failure.
std::string CompareResults(const std::vector<WindowResult>& engine,
                           const std::vector<WindowResult>& oracle);

/// Collapse multiple engine emissions for the same (key, window) into a single
/// authoritative row by keeping the most-complete (max count) emission.
/// The engine emits a window more than once under two conditions: re-fires when
/// late-but-within-lateness data arrives (each re-fire emits the full aggregate),
/// and, with allowed_lateness == 0, a stray partial pane for late records that
/// missed their already-fired window. Keeping the max-count row selects the
/// authoritative aggregate in both cases so it can be compared to the oracle.
std::vector<WindowResult> DedupEngineResults(const std::vector<WindowResult>& results);

/// Run N seeded tests comparing engine output vs oracle.
/// For each seed, generates deterministic records, runs both the pipeline
/// and the oracle, then compares results.
///
/// Failed seeds are REPRO seeds — run with --seeds 1 --seed-start N to reproduce.
DifferentialResult RunDifferential(const DifferentialConfig& config);

}  // namespace stormglass

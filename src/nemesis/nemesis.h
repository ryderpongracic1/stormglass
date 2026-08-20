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

// Run a single in-process nemesis recovery test.
// 1. Runs pipeline with checkpointing, stops at kill_position
// 2. Optionally corrupts the in-flight checkpoint (.tmp simulation)
// 3. Restarts pipeline from checkpoint, runs to completion
// 4. Compares combined output against oracle (at-least-once: zero missing)
//
// NOTE: this path stops the source at the kill point, which lets the pipeline
// drain normally (final flush runs). It exercises restore-from-checkpoint but
// NOT a genuine crash. Use RunRealKillNemesis for the headline crash guarantee.
NemesisResult RunNemesis(const NemesisConfig& config);

// ---------------------------------------------------------------------------
// Real crash nemesis: fork() + SIGKILL
// ---------------------------------------------------------------------------

enum class RealKillPoint {
    kBetweenCheckpoints,  // Kill after N durable checkpoints, mid-stream
    kMidCheckpoint,       // Kill inside the WriteCheckpoint .tmp/fsync/rename window
};

struct RealKillConfig {
    uint64_t seed = 42;
    uint64_t num_records = 40000;
    uint32_t num_keys = 50;
    Duration window_size{1000};
    Duration max_disorder{500};
    uint64_t checkpoint_interval = 1000;
    RealKillPoint kill_point = RealKillPoint::kBetweenCheckpoints;

    // kBetweenCheckpoints: SIGKILL once this many completed checkpoints exist.
    uint32_t target_checkpoint = 2;

    // Genuine crashes are captured by retrying the fork until the kill really
    // preempts a final flush (and, for kMidCheckpoint, really lands during a
    // checkpoint write). Cap the retries so a pathological host cannot hang.
    uint32_t max_attempts = 400;
};

struct RealKillResult {
    bool passed = false;

    // Evidence that the crash was real.
    uint32_t attempts = 0;              // fork attempts until a genuine crash
    bool killed_by_sigkill = false;     // child terminated by SIGKILL
    bool final_flush_completed = false; // child's post-flush sentinel present (should be false)
    bool stale_tmp_after_kill = false;  // interrupted checkpoint .tmp left behind
    uint64_t restored_offset = 0;       // offset of the checkpoint restored post-crash

    // Verification.
    uint64_t pre_crash_emits = 0;       // durable sink records written before the kill
    uint64_t post_restore_emits = 0;    // records emitted by the restored pipeline
    uint64_t union_results = 0;         // distinct results across both
    uint64_t oracle_results = 0;
    uint64_t missing_results = 0;       // MUST be 0 for at-least-once
    uint64_t duplicates = 0;            // permitted, counted

    std::string failure_detail;
};

// Run a single genuine crash-recovery test:
// 1. fork() a child that runs the real Pipeline (checkpointing on) into a
//    durable sink, then SIGKILL it before it can final-flush.
// 2. Restart a fresh pipeline that restores from the last valid checkpoint and
//    drains to completion into a second durable sink.
// 3. Verify union(pre-crash durable output, post-restore output) is a
//    superset-or-equal of the oracle over the full dataset (zero missing).
RealKillResult RunRealKillNemesis(const RealKillConfig& config);

} // namespace stormglass

#pragma once

#include "stream/record.h"
#include "source/generator.h"  // DisorderMode

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

    // Cross-axis coverage: crash recovery WITH late data in flight. Default is
    // bounded / L=0 (the original behavior); set these to run heavy-tailed
    // disorder against a non-zero allowed lateness.
    DisorderMode disorder_mode = DisorderMode::kBounded;
    double late_fraction = 0.0;
    Duration late_tail{0};
    Duration allowed_lateness{0};

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

// ---------------------------------------------------------------------------
// Partitioned real crash nemesis: fork() + SIGKILL at a TORN global checkpoint
// ---------------------------------------------------------------------------
//
// Extends the real-kill guarantee to the N-worker PartitionedPipeline. The
// child runs the partitioned engine with per-worker DurableFileSinks and
// distributed checkpointing; the parent SIGKILLs it at a moment when the global
// checkpoint is INCONSISTENT — at least one partition has written its file for
// barrier O_hi but not all have (a torn global checkpoint on disk), while a
// lower COMPLETE global checkpoint O_lo still exists. Restart, restore, and
// verify (a) at-least-once holds (union of all workers' pre-crash durable output
// and post-restore output covers the oracle, 0 missing) and (b) restore fell
// back to O_lo, never the torn O_hi. A capture-retry loop (mirroring the
// single-threaded real-kill) forks until a genuine torn-state crash is observed.

struct PartitionedRealKillConfig {
    uint64_t seed = 42;
    // SMALL by default: DurableFileSink fsyncs every emit, so a large workload
    // makes the fork parent wait a long time to reach the target checkpoint.
    // The invariants under test are scale-free.
    uint64_t num_records = 6000;
    uint32_t num_keys = 20;
    uint32_t num_workers = 4;
    Duration window_size{1000};
    Duration max_disorder{500};
    uint64_t checkpoint_interval = 500;
    Duration allowed_lateness{0};

    // Arm the kill once a COMPLETE global checkpoint at >= target_checkpoint *
    // checkpoint_interval exists (a solid fallback), then SIGKILL the instant a
    // torn (partial, higher) global checkpoint appears on disk.
    uint32_t target_checkpoint = 2;
    uint32_t max_attempts = 400;
};

struct PartitionedRealKillResult {
    bool passed = false;
    uint32_t attempts = 0;
    uint32_t num_workers = 0;

    // Evidence the crash was real and torn.
    bool killed_by_sigkill = false;      // child terminated by SIGKILL
    bool final_flush_completed = false;  // child sentinel present (should be false)
    bool torn_checkpoint_observed = false;  // partial global ckpt on disk at kill
    uint64_t torn_offset = 0;            // the higher, INCOMPLETE global offset
    uint64_t restored_offset = 0;        // COMPLETE offset restore fell back to

    // Verification.
    uint64_t pre_crash_emits = 0;        // durable records across all workers pre-kill
    uint64_t post_restore_emits = 0;     // durable records across all workers post-restore
    uint64_t union_results = 0;
    uint64_t oracle_results = 0;
    uint64_t missing_results = 0;        // MUST be 0 for at-least-once
    uint64_t duplicates = 0;

    std::string failure_detail;
};

PartitionedRealKillResult RunPartitionedRealKillNemesis(
    const PartitionedRealKillConfig& config);

// ---------------------------------------------------------------------------
// Mid-ALIGNMENT crash nemesis: fork() + SIGKILL while K-way barrier alignment
// is in progress (v3 Phase 3)
// ---------------------------------------------------------------------------
//
// The child runs the real single-threaded Pipeline over a SourceMerge of K
// sources with DIVERGENT per-source barrier cadences (interval_fast << interval_
// slow), so the fast channel reaches its barrier N early and is BLOCKED for most
// of each epoch while the slow channel catches up — a wide, easy-to-hit mid-
// alignment window. A probe wrapper writes the SourceMerge's live alignment state
// to a marker file after every pull. The parent arms once `target_checkpoint`
// COMPLETE merged checkpoints exist (a solid fallback), then SIGSTOPs the child
// the instant the marker shows a PARTIAL alignment (some channels delivered
// barrier N, others not), re-verifies while frozen, and SIGKILLs — so the crash
// is provably mid-alignment. Restart, restore, and verify: (a) restore fell back
// to the last COMPLETE (fully-aligned) checkpoint — a multiple of the merged
// stride S = sum of per-source intervals, strictly below the mid-alignment epoch
// — never a partial cut; and (b) at-least-once holds (union of pre-crash durable
// output and post-restore output covers the oracle over the merged stream, 0
// missing). A capture-retry loop mirrors the other real-kill harnesses.
//
// L == 0 / bounded here, so DurableFileSink's per-emit fsync does NOT balloon
// (no lateness re-fire), and the workload stays small.

struct AlignmentKillConfig {
    uint64_t seed = 42;
    uint64_t num_records = 3000;       // PER SOURCE (merged ~ K * this; kept small)
    uint32_t num_keys = 20;
    Duration window_size{1000};
    Duration max_disorder{200};
    // Divergent per-source barrier intervals so alignment blocks the fast channel
    // for most of each epoch. S (merged stride per fully-aligned epoch) == fast +
    // slow.
    uint64_t interval_fast = 50;
    uint64_t interval_slow = 450;
    uint32_t target_checkpoint = 2;    // arm after this many COMPLETE merged checkpoints
    uint32_t max_attempts = 400;
};

struct AlignmentKillResult {
    bool passed = false;
    uint32_t attempts = 0;

    // Evidence the crash was real AND mid-alignment.
    bool killed_by_sigkill = false;        // child terminated by SIGKILL
    bool final_flush_completed = false;    // child sentinel present (should be false)
    bool alignment_partial_at_kill = false;// some (but not all) channels aligned at kill
    uint64_t epoch_in_progress = 0;        // the open (mid-alignment) epoch at kill
    uint32_t channels_delivered = 0;       // channels that had delivered the open barrier
    uint32_t num_channels = 0;
    uint64_t merged_stride = 0;            // S = sum of per-source barrier intervals

    // Verification.
    uint64_t restored_offset = 0;          // COMPLETE (aligned) checkpoint restore used
    uint64_t pre_crash_emits = 0;
    uint64_t post_restore_emits = 0;
    uint64_t union_results = 0;
    uint64_t oracle_results = 0;
    uint64_t missing_results = 0;          // MUST be 0 for at-least-once
    uint64_t duplicates = 0;

    std::string failure_detail;
};

AlignmentKillResult RunAlignmentKillNemesis(const AlignmentKillConfig& config);

} // namespace stormglass

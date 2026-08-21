#pragma once

#include "source/generator.h"
#include "source/source.h"
#include "stream/batch.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <vector>

namespace stormglass {

/// The min-combine machinery, factored out as a PURE, obviously-correct helper
/// so it can be unit-tested directly against known per-source watermark
/// trajectories (see source_merge_test.cpp) — independent of any generator.
///
/// Contract: with K source channels, the effective (merged) watermark is the
/// MIN across the ACTIVE channels. Each channel is monotonic (its watermark
/// never moves backward), and the emitted merged watermark is itself monotonic:
/// it advances only when the running MIN advances. A channel that lags — reports
/// a smaller watermark than the others, or has not reported at all (still
/// Timestamp::min()) — holds the merged watermark back to its value.
///
/// v3 Phase 2 adds an ACTIVE/idle set. All channels start ACTIVE (so K=1 and the
/// Phase-1 multi-source path are byte-for-byte unchanged). MarkIdle(i) drops a
/// quiet channel from the MIN so event-time can progress past it; MarkActive(i)
/// (resume) re-includes it at its RETAINED watermark. Because the merged
/// watermark only ever ADVANCES, a resumed low watermark can never regress it —
/// it simply pins the MIN again, and the resumed channel's below-watermark
/// records become genuinely late downstream.
class MinWatermarkCombiner {
public:
    explicit MinWatermarkCombiner(std::size_t k)
        : per_source_(k, Timestamp::min()), active_(k, true) {}

    /// Observe source `i`'s latest watermark. Marks `i` ACTIVE (a channel that
    /// reports a watermark is, by definition, live), applies per-source
    /// monotonicity (ignores a backwards report), recomputes the running MIN
    /// across ACTIVE channels, and returns the new merged watermark IFF it
    /// advanced; otherwise std::nullopt.
    std::optional<Timestamp> Observe(std::size_t i, Timestamp wm) {
        active_[i] = true;
        if (wm > per_source_[i]) {
            per_source_[i] = wm;
        }
        return Recompute();
    }

    /// Exclude source `i` from the MIN (it went idle). With the lagging idle
    /// channel gone, the MIN over the remaining ACTIVE channels may advance —
    /// returns the new merged watermark IFF it did, else std::nullopt.
    std::optional<Timestamp> MarkIdle(std::size_t i) {
        active_[i] = false;
        return Recompute();
    }

    /// Re-include source `i` (resume) at its RETAINED (possibly stale) watermark.
    /// Never regresses the merged watermark: Recompute only emits on advance, so
    /// a resumed low watermark just pins the MIN again without moving emitted_.
    std::optional<Timestamp> MarkActive(std::size_t i) {
        active_[i] = true;
        return Recompute();
    }

    /// The last emitted merged watermark (running min over active channels).
    [[nodiscard]] Timestamp Current() const { return emitted_; }

    [[nodiscard]] std::size_t size() const { return per_source_.size(); }
    [[nodiscard]] bool IsActive(std::size_t i) const { return active_[i]; }

private:
    /// Recompute the MIN over ACTIVE channels and advance-clamp. Returns the new
    /// merged watermark only when it strictly advanced. When NO channel is active
    /// (every source idle) the merged watermark holds steady — it never regresses.
    std::optional<Timestamp> Recompute() {
        bool any = false;
        Timestamp min_wm = Timestamp::max();
        for (std::size_t s = 0; s < per_source_.size(); ++s) {
            if (!active_[s]) continue;
            any = true;
            if (per_source_[s] < min_wm) min_wm = per_source_[s];
        }
        if (!any) return std::nullopt;  // all idle: hold emitted_ steady
        if (min_wm > emitted_) {
            emitted_ = min_wm;
            return emitted_;
        }
        return std::nullopt;
    }

    std::vector<Timestamp> per_source_;
    std::vector<bool> active_;
    Timestamp emitted_{Timestamp::min()};
};

/// Configuration for a SourceMerge: K underlying DeterministicGenerators plus
/// the merged-stream barrier interval. Give the per-source GeneratorConfigs
/// DIVERGENT event_time_step / seed so their watermarks advance at different
/// rates. K == 1 is a literal passthrough that reproduces the single generator's
/// downstream behavior.
struct SourceMergeConfig {
    std::vector<GeneratorConfig> sources;

    // Records between merged-stream checkpoint barriers (0 = no barriers).
    // SourceMerge is the SINGLE barrier origin in Phase 1: it stamps each barrier
    // with the merged offset, exactly like a single generator does today. Any
    // checkpoint_interval on the wrapped source configs is IGNORED (forced to 0)
    // — the merged stream owns barriers.
    uint64_t checkpoint_interval = 0;

    // Data records assembled per merged Next() batch. Matches the generator's
    // batch_size convention so CurrentOffset() lands on clean batch boundaries.
    uint32_t merged_batch_size = 1024;

    // v3 Phase 2 idleness policy. A source is marked IDLE (excluded from the MIN)
    // after this many CONSECUTIVE empty pulls — round-robin turns on which it
    // yielded no data record while not exhausted (i.e. it is inside a configured
    // IdleSpan). This is a DETERMINISTIC LOGICAL measure, never wall-clock, so
    // the merged trajectory replays exactly and the oracle can predict it.
    // 0 (default) DISABLES idleness entirely: no source is ever excluded and the
    // merged stream is Phase-1 min-combine, bit-for-bit. Idle spans without a
    // timeout simply stall the MIN (the quiet source keeps pinning it).
    uint32_t idle_timeout = 0;
};

/// A Source that deterministically merges K DeterministicGenerators into one
/// stream. It IS a Source (not a thread), so it drops into the existing Pipeline
/// and PartitionedPipeline Router UNCHANGED — Phase 1 adds zero new concurrency.
///
/// Behavior:
///   * Deterministic interleaving: pulls one item at a time from the wrapped
///     sources in a fixed round-robin over the non-exhausted ones.
///   * Per-source watermark tracking: each wrapped generator emits its OWN
///     watermark control records; SourceMerge intercepts them (does NOT forward
///     them), updates the MinWatermarkCombiner, and emits a single merged
///     kWatermark carrying the running MIN — only when that min advances.
///   * Merged offset + Seek: CurrentOffset() counts merged DATA records; Seek(O)
///     re-seeds ALL wrapped sources and replays to O, so a restore replays the
///     identical merged sequence (O(O), the same documented replay cost the
///     single generator's Seek pays).
///   * Barriers: SourceMerge is the single barrier origin, stamping each with the
///     merged offset. REAL K-way per-source barrier ALIGNMENT is Phase 3.
class SourceMerge : public Source {
public:
    explicit SourceMerge(SourceMergeConfig config);

    std::optional<Batch> Next() override;
    void Seek(uint64_t offset) override;
    [[nodiscard]] uint64_t CurrentOffset() const override;

    /// The current merged watermark (running min across channels). Exposed for
    /// tests that assert the lagging-source-holds-the-min behavior end-to-end.
    [[nodiscard]] Timestamp CurrentWatermark() const { return combiner_.Current(); }

    /// Whether wrapped source `i` is currently marked IDLE (excluded from the
    /// MIN). Exposed for the Phase-2 idle unit tests.
    [[nodiscard]] bool IsSourceIdle(std::size_t i) const { return states_[i].idle; }

private:
    enum class StepResult { kProduced, kExhausted };

    struct SourceState {
        GeneratorConfig config;                       // checkpoint_interval forced to 0
        std::unique_ptr<DeterministicGenerator> gen;
        Batch buf;                                    // current buffered batch
        std::size_t cursor = 0;                       // index into buf.items
        bool exhausted = false;

        // --- v3 Phase 2 idle-span modeling + detection state ---
        std::vector<IdleSpan> idle_spans;   // copied from config.idle_spans (sorted)
        std::size_t next_span = 0;          // index of the next span to fire
        uint64_t gap_remaining = 0;         // idle ticks left in the current gap (0 = live)
        uint64_t data_pulled = 0;           // this source's own data-record index
        uint64_t consecutive_empty = 0;     // consecutive empty pulls (resets on any output)
        bool idle = false;                  // excluded from the MIN (idle_timeout tripped)
    };

    void ResetState();
    bool PullNextItem(std::size_t i, BatchItem& out);
    StepResult ProduceOneMergedStep(Batch& out, std::size_t& data_in_batch);
    [[nodiscard]] bool AllExhausted() const;

    SourceMergeConfig config_;
    std::vector<SourceState> states_;
    MinWatermarkCombiner combiner_;

    std::size_t rr_ = 0;                 // round-robin cursor over sources
    uint64_t merged_offset_ = 0;         // count of merged DATA records emitted
    uint64_t records_since_checkpoint_ = 0;
};

} // namespace stormglass

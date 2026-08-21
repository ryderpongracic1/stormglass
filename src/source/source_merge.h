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
/// MIN across channels. Each channel is monotonic (a channel's watermark never
/// moves backward), and the emitted merged watermark is itself monotonic: it
/// advances only when the running MIN advances. A channel that lags — reports a
/// smaller watermark than the others, or has not reported at all (still
/// Timestamp::min()) — holds the merged watermark back to its value.
class MinWatermarkCombiner {
public:
    explicit MinWatermarkCombiner(std::size_t k)
        : per_source_(k, Timestamp::min()) {}

    /// Observe source `i`'s latest watermark. Applies per-source monotonicity
    /// (ignores a backwards report), recomputes the running MIN across all
    /// channels, and returns the new merged watermark IFF it advanced; otherwise
    /// std::nullopt (the merged watermark is unchanged and nothing is emitted).
    std::optional<Timestamp> Observe(std::size_t i, Timestamp wm) {
        if (wm > per_source_[i]) {
            per_source_[i] = wm;
        }
        Timestamp min_wm = per_source_[0];
        for (std::size_t s = 1; s < per_source_.size(); ++s) {
            if (per_source_[s] < min_wm) min_wm = per_source_[s];
        }
        if (min_wm > emitted_) {
            emitted_ = min_wm;
            return emitted_;
        }
        return std::nullopt;
    }

    /// The last emitted merged watermark (running min). Timestamp::min() until
    /// EVERY channel has reported at least once past min().
    [[nodiscard]] Timestamp Current() const { return emitted_; }

    [[nodiscard]] std::size_t size() const { return per_source_.size(); }

private:
    std::vector<Timestamp> per_source_;
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

private:
    enum class StepResult { kProduced, kExhausted };

    struct SourceState {
        GeneratorConfig config;                       // checkpoint_interval forced to 0
        std::unique_ptr<DeterministicGenerator> gen;
        Batch buf;                                    // current buffered batch
        std::size_t cursor = 0;                       // index into buf.items
        bool exhausted = false;
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

#include "source/source_merge.h"

#include <algorithm>
#include <utility>
#include <variant>

namespace stormglass {

SourceMerge::SourceMerge(SourceMergeConfig config)
    : config_(std::move(config)),
      combiner_(std::max<std::size_t>(1, config_.sources.size())) {
    ResetState();
}

void SourceMerge::ResetState() {
    states_.clear();
    states_.reserve(config_.sources.size());
    for (const auto& src : config_.sources) {
        SourceState st;
        st.config = src;
        // SourceMerge owns barriers on the merged stream — the wrapped sources
        // must NOT emit their own (they would carry per-source offsets that mean
        // nothing on the merged stream).
        st.config.checkpoint_interval = 0;
        st.gen = std::make_unique<DeterministicGenerator>(st.config);
        // Idle spans are modeled by SourceMerge (it pauses the wrapped generator
        // for the span); the generator itself ignores them. Copy them out so the
        // per-source detection state is self-contained.
        st.idle_spans = src.idle_spans;
        states_.push_back(std::move(st));
    }
    combiner_ = MinWatermarkCombiner(std::max<std::size_t>(1, config_.sources.size()));
    rr_ = 0;
    merged_offset_ = 0;
    records_since_checkpoint_ = 0;
}

bool SourceMerge::PullNextItem(std::size_t i, BatchItem& out) {
    SourceState& st = states_[i];
    // Refill until we have an item or the wrapped source is exhausted. Batches
    // from DeterministicGenerator are non-empty until exhaustion, so this loops
    // at most once in practice; the guard keeps it robust.
    while (st.cursor >= st.buf.items.size()) {
        auto batch = st.gen->Next();
        if (!batch.has_value()) {
            st.exhausted = true;
            return false;
        }
        st.buf = std::move(*batch);
        st.cursor = 0;
    }
    out = st.buf.items[st.cursor++];
    return true;
}

SourceMerge::StepResult SourceMerge::ProduceOneMergedStep(Batch& out,
                                                          std::size_t& data_in_batch) {
    const std::size_t k = states_.size();
    for (std::size_t attempt = 0; attempt < k; ++attempt) {
        const std::size_t i = (rr_ + attempt) % k;
        SourceState& st = states_[i];
        if (st.exhausted) continue;

        // --- v3 Phase 2 idle-span modeling ---
        // A gap begins when the source has pulled exactly `start_offset` data
        // records; it lasts `length` empty pulls. During a gap SourceMerge does
        // NOT pull from the wrapped generator, so the generator produces nothing
        // and its watermark freezes at its pre-gap value. The paused generator
        // resumes exactly where it left off, so the delayed records keep their
        // ORIGINAL (now-below-watermark) event-times.
        if (st.gap_remaining == 0 && st.next_span < st.idle_spans.size() &&
            st.idle_spans[st.next_span].start_offset == st.data_pulled) {
            st.gap_remaining = st.idle_spans[st.next_span].length;
            ++st.next_span;
        }
        if (st.gap_remaining > 0) {
            // Empty pull (idle tick). Consume the round-robin turn, decrement the
            // gap, and count toward the idle timeout. Purely LOGICAL/deterministic
            // — no wall-clock — so the merged trajectory replays exactly.
            --st.gap_remaining;
            ++st.consecutive_empty;
            rr_ = (i + 1) % k;

            if (config_.idle_timeout > 0 && !st.idle &&
                st.consecutive_empty >= config_.idle_timeout) {
                // Idleness tripped: drop this lagging source from the running MIN
                // so event-time can progress. With it excluded, the MIN over the
                // ACTIVE sources may advance — emit that merged watermark instead
                // of letting the quiet source stall firing forever.
                st.idle = true;
                if (auto merged = combiner_.MarkIdle(i)) {
                    out.items.emplace_back(ControlRecord{
                        .type = ControlType::kWatermark,
                        .watermark = *merged,
                        .checkpoint_offset = merged_offset_,
                    });
                }
            }
            return StepResult::kProduced;  // serviced a turn; stream still live
        }

        BatchItem item;
        if (!PullNextItem(i, item)) {
            continue;  // this source just exhausted; try the next one
        }
        // Advance the round-robin cursor PAST the source we pulled from, so the
        // interleaving is a strict, reproducible rotation over live sources.
        rr_ = (i + 1) % k;

        // Any output ends an idle run. If the source was excluded, RESUME it:
        // re-activate and rejoin the MIN at its RETAINED (stale) watermark. The
        // combiner never regresses the merged watermark (it only emits on
        // advance), so a resumed low watermark simply pins the MIN again. Records
        // this source now emits may sit BELOW the advanced merged watermark —
        // i.e. genuinely LATE — and SourceMerge emits them as ordinary data so
        // the downstream allowed_lateness policy classifies them (dropped beyond
        // deadline, accepted + re-fired within). SourceMerge does NOT special-case
        // them and keeps the merged watermark monotonic.
        if (st.idle) {
            st.idle = false;
            combiner_.MarkActive(i);
        }
        st.consecutive_empty = 0;

        std::visit([&](auto&& v) {
            using T = std::decay_t<decltype(v)>;
            if constexpr (std::is_same_v<T, Record>) {
                out.items.emplace_back(v);
                ++merged_offset_;
                ++data_in_batch;
                ++st.data_pulled;

                // Merged checkpoint barrier, stamped with the merged offset.
                // Because merged items are produced in order, everything at or
                // below this offset has been emitted when a downstream pipeline
                // dequeues the barrier and nothing after it has — the same
                // degenerate Chandy-Lamport property the single source relies on.
                //
                // PHASE 3 HOOK: real multi-source recovery needs K-way barrier
                // ALIGNMENT — inject a per-source barrier into each channel, hold
                // each channel at its barrier until all K have arrived, then
                // snapshot the aligned cut. That replaces this single-origin
                // stamp right here. Phase 2 keeps the single origin intact and
                // idleness does NOT touch barriers.
                if (config_.checkpoint_interval > 0) {
                    ++records_since_checkpoint_;
                    if (records_since_checkpoint_ >= config_.checkpoint_interval) {
                        records_since_checkpoint_ = 0;
                        out.items.emplace_back(ControlRecord{
                            .type = ControlType::kCheckpointBarrier,
                            .watermark = Timestamp::min(),
                            .checkpoint_offset = merged_offset_,
                        });
                    }
                }
            } else {  // ControlRecord from a wrapped source
                const ControlRecord& c = v;
                if (c.type == ControlType::kWatermark) {
                    // Intercept, do NOT forward. Fold into the running MIN and
                    // emit a merged watermark only when the min advances.
                    if (auto merged = combiner_.Observe(i, c.watermark)) {
                        out.items.emplace_back(ControlRecord{
                            .type = ControlType::kWatermark,
                            .watermark = *merged,
                            .checkpoint_offset = merged_offset_,
                        });
                    }
                }
                // Wrapped-source barriers are disabled (checkpoint_interval = 0),
                // so kCheckpointBarrier is never produced here.
            }
        }, item);

        return StepResult::kProduced;
    }
    return StepResult::kExhausted;
}

bool SourceMerge::AllExhausted() const {
    return std::all_of(states_.begin(), states_.end(),
                       [](const SourceState& s) { return s.exhausted; });
}

std::optional<Batch> SourceMerge::Next() {
    if (states_.empty() || AllExhausted()) {
        return std::nullopt;
    }

    Batch out;
    std::size_t data_in_batch = 0;
    while (data_in_batch < config_.merged_batch_size) {
        if (ProduceOneMergedStep(out, data_in_batch) == StepResult::kExhausted) {
            break;  // no live source produced — stream is draining
        }
    }

    if (out.items.empty()) {
        return std::nullopt;
    }
    return out;
}

void SourceMerge::Seek(uint64_t offset) {
    // Re-seed ALL wrapped sources and replay the merged production to `offset`
    // merged DATA records, mirroring the single generator's O(offset) Seek. The
    // combiner, round-robin cursor, and barrier counter are members updated by
    // ProduceOneMergedStep, so replaying reproduces the exact internal state a
    // non-seeked run holds at the same offset — and thus the identical merged
    // sequence afterward.
    ResetState();

    Batch scratch;
    std::size_t dib = 0;
    while (merged_offset_ < offset) {
        if (ProduceOneMergedStep(scratch, dib) == StepResult::kExhausted) {
            break;  // offset beyond stream length; best-effort, matches generator
        }
        scratch.items.clear();  // discard replayed output; state is what matters
        dib = 0;
    }
}

uint64_t SourceMerge::CurrentOffset() const {
    return merged_offset_;
}

} // namespace stormglass

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
        // v3 Phase 3: wrapped sources emit their OWN barriers now (Phase 1/2 forced
        // this to 0). EffectiveInterval picks the source's own checkpoint_interval,
        // else the merged-config default; SourceMerge aligns the resulting per-source
        // barriers K-way. 0 (both) == this source emits no barriers.
        st.config.checkpoint_interval = EffectiveInterval(src);
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
    epoch_closed_ = 0;
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

        // v3 Phase 3 alignment HOLD: a channel that has delivered its barrier for
        // the OPEN epoch is BLOCKED. Its records stay buffered (unpulled in st.buf,
        // st.cursor not advanced past the barrier) and it is skipped in the round-
        // robin until every active channel aligns and the epoch closes. This is the
        // Chandy-Lamport "block the early channel" step.
        if (IsChannelBlocked(i)) continue;

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
                // Excluding the quiet channel from the MIN ALSO excludes it from the
                // barrier ALIGNMENT set. If every remaining active channel already
                // delivered its barrier for the open epoch, the epoch can now close
                // WITHOUT this channel — this is precisely what stops a quiet channel
                // from deadlocking alignment (see MaybeCloseEpoch).
                MaybeCloseEpoch(out);
            }
            return StepResult::kProduced;  // serviced a turn; stream still live
        }

        BatchItem item;
        if (!PullNextItem(i, item)) {
            // This source just exhausted. Remove it from the alignment set: the
            // remaining active channels may now be able to close the open epoch
            // (a dead channel can never deliver another barrier).
            MaybeCloseEpoch(out);
            continue;  // try the next source
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
                // v3 Phase 3: the merged barrier is NO LONGER stamped here by a
                // merged record count. It is produced by MaybeCloseEpoch when the
                // per-source barriers ALIGN (see the kCheckpointBarrier branch),
                // so the merged barrier's offset is the aligned cut, not a count.
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
                } else if (c.type == ControlType::kCheckpointBarrier) {
                    // A per-source barrier arrived on channel i. Record it (do NOT
                    // forward it — the merged stream carries ONE aligned barrier per
                    // epoch) and try to close the open epoch. If this is an EARLY
                    // arrival, IsChannelBlocked(i) is now true and channel i's
                    // subsequent records stay buffered until every active channel
                    // has delivered its barrier for this epoch.
                    ++st.barriers_seen;
                    MaybeCloseEpoch(out);
                }
            }
        }, item);

        return StepResult::kProduced;
    }
    return StepResult::kExhausted;
}

void SourceMerge::MaybeCloseEpoch(Batch& out) {
    const uint64_t next_epoch = epoch_closed_ + 1;
    bool any_active = false;
    for (const SourceState& st : states_) {
        // Idle and exhausted channels are EXCLUDED from the alignment set — the
        // same exclusion the watermark MIN applies. This is what guarantees a quiet
        // (or ended) channel cannot hold up, and thus cannot deadlock, alignment.
        if (st.exhausted || st.idle) continue;
        any_active = true;
        if (st.barriers_seen < next_epoch) return;  // this channel hasn't aligned yet
    }
    if (!any_active) return;  // no active channel to align (all idle/exhausted)

    // Every active channel has delivered its barrier for `next_epoch`: emit ONE
    // merged barrier stamped with the aligned cut's merged offset, then advance the
    // closed-epoch counter — which unblocks every channel that was holding at this
    // barrier (IsChannelBlocked becomes false for them).
    epoch_closed_ = next_epoch;
    out.items.emplace_back(ControlRecord{
        .type = ControlType::kCheckpointBarrier,
        .watermark = Timestamp::min(),
        .checkpoint_offset = merged_offset_,
    });
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

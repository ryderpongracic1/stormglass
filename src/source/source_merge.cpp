#include "source/source_merge.h"

#include <algorithm>
#include <utility>
#include <stdexcept>
#include <variant>

namespace stormglass {

SourceMerge::SourceMerge(SourceMergeConfig config)
    : config_(std::move(config)),
      combiner_(std::max<std::size_t>(1, config_.sources.size())) {
    if (config_.merged_batch_size == 0) throw std::invalid_argument("merged batch size must be positive");
    if (config_.live_inputs)
        throw std::invalid_argument("generator-config SourceMerge inputs are replayable; use SourceMergeInput for live inputs");
    if (config_.idle_timeout_wall.count() != 0)
        throw std::invalid_argument("idle_timeout_wall requires live_inputs");
    for (const auto& src : config_.sources) {
        // v3 Phase 3: wrapped sources emit their OWN barriers now (Phase 1/2 forced
        // this to 0). EffectiveInterval picks the source's own checkpoint_interval,
        // else the merged-config default; SourceMerge aligns the resulting per-source
        // barriers K-way. 0 (both) == this source emits no barriers.
        GeneratorConfig gen = src;
        gen.checkpoint_interval = EffectiveInterval(src);
        // Idle spans are modeled by SourceMerge (it pauses the wrapped generator
        // for the span); the generator itself ignores them.
        AddInput(std::make_unique<DeterministicGenerator>(gen), src.idle_spans);
    }
    ResetProgress();
}

SourceMerge::SourceMerge(std::vector<SourceMergeInput> inputs, SourceMergeConfig options)
    : config_(std::move(options)),
      combiner_(std::max<std::size_t>(1, inputs.size())) {
    if (config_.merged_batch_size == 0) throw std::invalid_argument("merged batch size must be positive");
    if (!config_.sources.empty())
        throw std::invalid_argument("SourceMerge inputs and generator configs cannot be mixed");
    if (config_.checkpoint_interval != 0)
        throw std::invalid_argument("SourceMerge inputs emit their own barriers; checkpoint_interval must be 0");
    if (config_.idle_timeout_wall.count() < 0)
        throw std::invalid_argument("idle_timeout_wall must be nonnegative");
    if (config_.idle_timeout_wall.count() > 0 && !config_.live_inputs)
        throw std::invalid_argument("idle_timeout_wall requires live_inputs");
    for (auto& input : inputs) {
        if (!input.source) throw std::invalid_argument("SourceMerge input source is null");
        AddInput(std::move(input.source), std::move(input.idle_spans));
    }
    ResetProgress();
}

void SourceMerge::AddInput(std::unique_ptr<Source> source, std::vector<IdleSpan> idle_spans) {
    for (std::size_t i = 1; i < idle_spans.size(); ++i) {
        if (idle_spans[i].start_offset < idle_spans[i - 1].start_offset)
            throw std::invalid_argument("idle spans must be sorted by start_offset");
    }
    SourceState st;
    st.source = std::move(source);
    st.idle_spans = std::move(idle_spans);
    states_.push_back(std::move(st));
}

void SourceMerge::ResetProgress() {
    for (SourceState& st : states_) {
        st.buf = Batch{};
        st.cursor = 0;
        st.exhausted = false;
        st.next_span = 0;
        st.gap_remaining = 0;
        st.data_pulled = 0;
        st.consecutive_empty = 0;
        st.idle = false;
        st.barriers_seen = 0;
        st.last_item = std::chrono::steady_clock::now();
    }
    combiner_ = MinWatermarkCombiner(std::max<std::size_t>(1, states_.size()));
    rr_ = 0;
    merged_offset_ = 0;
    epoch_closed_ = 0;
}

std::string SourceMerge::Descriptor() const {
    std::string out = "merge(idle_timeout=" + std::to_string(config_.idle_timeout);
    for (const SourceState& st : states_) {
        const std::string channel = st.source->Descriptor();
        if (channel.empty()) return "";
        // Channel ends change the interleaving after them, so they are part of
        // the merged stream's identity even though a lone source omits them.
        const auto length = st.source->Length();
        out += ";" + channel + "#length=" +
               (!length ? std::string("unknown")
                        : *length == kUnbounded ? std::string("unbounded")
                                                : std::to_string(*length));
        for (const auto& span : st.idle_spans) {
            out += "@idle(" + std::to_string(span.start_offset) + "," +
                   std::to_string(span.length) + ")";
        }
    }
    return out + ")";
}

SourceMerge::PullResult SourceMerge::PullNextItem(std::size_t i, BatchItem& out) {
    SourceState& st = states_[i];
    if (st.cursor >= st.buf.items.size()) {
        auto batch = st.source->Next();
        if (!batch.has_value()) {
            st.exhausted = true;
            return PullResult::kExhausted;
        }
        if (batch->items.empty()) {
            // "No data yet". Only a live merge may see this: in a replayable
            // merge it would make the interleaving depend on timing.
            if (!config_.live_inputs) {
                throw std::logic_error(
                    "SourceMerge input returned an empty batch; set live_inputs for live sources");
            }
            return PullResult::kNoData;
        }
        st.buf = std::move(*batch);
        st.cursor = 0;
    }
    out = st.buf.items[st.cursor++];
    // Clock reads cost more than the pull itself; take one only when wall-clock
    // idleness needs it.
    if (config_.idle_timeout_wall.count() > 0) st.last_item = std::chrono::steady_clock::now();
    return PullResult::kItem;
}

void SourceMerge::MarkChannelIdle(std::size_t i, Batch& out) {
    states_[i].idle = true;
    if (auto merged = combiner_.MarkIdle(i)) {
        out.items.emplace_back(ControlRecord{
            .type = ControlType::kWatermark,
            .watermark = *merged,
            .checkpoint_offset = merged_offset_,
        });
    }
    // Excluding the quiet channel from the MIN ALSO excludes it from the barrier
    // ALIGNMENT set. If every remaining active channel already delivered its
    // barrier for the open epoch, the epoch can now close WITHOUT this channel —
    // this is precisely what stops a quiet channel from deadlocking alignment
    // (see MaybeCloseEpoch).
    MaybeCloseEpoch(out);
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
        // Aligned-checkpoint "block the early channel" step.
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
                MarkChannelIdle(i, out);
            }
            return StepResult::kProduced;  // serviced a turn; stream still live
        }

        BatchItem item;
        const PullResult pulled = PullNextItem(i, item);
        if (pulled == PullResult::kNoData) {
            // A live channel with nothing to deliver yields its turn. Wall-clock
            // idleness is the only idleness that applies to it: a count of empty
            // polls would measure the caller's poll rate, not the channel.
            rr_ = (i + 1) % k;
            if (config_.idle_timeout_wall.count() > 0 && !st.idle &&
                std::chrono::steady_clock::now() - st.last_item >= config_.idle_timeout_wall) {
                MarkChannelIdle(i, out);
            }
            return StepResult::kNoData;
        }
        if (pulled == PullResult::kExhausted) {
            // This source just exhausted. Remove it from the alignment set: the
            // remaining active channels may now be able to close the open epoch
            // (a dead channel can never deliver another barrier).
            if (auto merged = combiner_.MarkIdle(i)) {
                out.items.emplace_back(ControlRecord{
                    .type = ControlType::kWatermark,
                    .watermark = *merged,
                    .checkpoint_offset = merged_offset_,
                });
            }
            MaybeCloseEpoch(out);
            return StepResult::kProduced;  // retry: closing the epoch may unblock an earlier channel
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
    std::size_t quiet_steps = 0;
    while (data_in_batch < config_.merged_batch_size) {
        const StepResult step = ProduceOneMergedStep(out, data_in_batch);
        if (step == StepResult::kExhausted) {
            break;  // no live source produced — stream is draining
        }
        if (step == StepResult::kNoData) {
            // Every channel had its turn without data: hand back what we have
            // (possibly nothing) instead of polling quiet live inputs in a loop.
            if (++quiet_steps >= states_.size()) break;
        } else {
            quiet_steps = 0;
        }
    }

    if (out.items.empty()) {
        if (AllExhausted() || !config_.live_inputs) return std::nullopt;
        return Batch{};  // live and quiet: "no data yet"
    }
    return out;
}

std::optional<uint64_t> SourceMerge::Length() const {
    uint64_t total = 0;
    for (const SourceState& st : states_) {
        const auto length = st.source->Length();
        if (!length) return std::nullopt;
        if (*length == kUnbounded || total > kUnbounded - *length) return kUnbounded;
        total += *length;
    }
    return total;
}

bool SourceMerge::Replayable() const {
    if (config_.live_inputs) return false;
    return std::all_of(states_.begin(), states_.end(), [](const SourceState& s) {
        return s.source->Replayable() && s.source->Length().has_value();
    });
}

void SourceMerge::Cancel() {
    for (SourceState& st : states_) st.source->Cancel();
}

void SourceMerge::Seek(uint64_t offset) {
    // Rewind ALL wrapped sources and replay the merged production to `offset`
    // merged DATA records, mirroring the single generator's O(offset) Seek. The
    // combiner, round-robin cursor, and barrier counter are members updated by
    // ProduceOneMergedStep, so replaying reproduces the exact internal state a
    // non-seeked run holds at the same offset — and thus the identical merged
    // sequence afterward. The merged offset does not determine per-channel
    // offsets, so every channel must replay from its own start.
    if (config_.live_inputs && offset > 0) {
        throw std::logic_error("SourceMerge with live_inputs cannot seek: its merge order is not replayable");
    }
    for (SourceState& st : states_) st.source->Seek(0);
    ResetProgress();

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

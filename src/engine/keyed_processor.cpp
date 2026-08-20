#include "engine/keyed_processor.h"

namespace stormglass {

KeyedProcessor::KeyedProcessor(std::unique_ptr<WindowAssigner> assigner,
                               Sink& sink,
                               Duration allowed_lateness)
    : assigner_(std::move(assigner)),
      sink_(sink),
      allowed_lateness_(allowed_lateness),
      use_lateness_(allowed_lateness.count() > 0) {
    if (use_lateness_) {
        state_.SetAllowedLateness(allowed_lateness_);
    }
}

// Mirrors the Record branch of Pipeline::Run's std::visit.
void KeyedProcessor::ProcessRecord(const Record& r) {
    auto windows = assigner_->AssignWindows(r.event_time);
    bool any_dropped = false;
    bool any_late_accepted = false;
    for (auto& w : windows) {
        if (use_lateness_) {
            bool was_fired = state_.IsFired(w);
            bool accepted = state_.AddWithLateness(
                r.key, w, r.value, watermark_.Current());
            if (!accepted) {
                any_dropped = true;
            } else if (was_fired) {
                any_late_accepted = true;
            }
        } else {
            // L == 0: deadline is window.end + 0. Without this, a record
            // arriving after its window fired (and its panes were erased) would
            // create a fresh pane that re-fires a spurious partial result at
            // final flush.
            bool accepted = state_.AddWithLateness(
                r.key, w, r.value, watermark_.Current());
            if (!accepted) any_dropped = true;
        }
    }
    if (any_dropped) stats_.late_records_dropped++;
    if (any_late_accepted) stats_.late_records_accepted++;
    stats_.records_processed++;
}

// Mirrors the ControlRecord branch of Pipeline::Run's std::visit.
void KeyedProcessor::ProcessControl(const ControlRecord& c) {
    if (c.type == ControlType::kWatermark) {
        if (watermark_.Advance(c.watermark)) {
            // Fire expired windows (first-time fire)
            for (auto& w : state_.ExpiredWindows(watermark_.Current())) {
                for (auto& result : state_.FireWindow(w)) {
                    sink_.Emit(result);
                }
                stats_.windows_fired++;
            }

            // Re-fire windows that received late data
            if (use_lateness_) {
                for (auto& w : state_.RefiredWindows()) {
                    for (auto& result : state_.FireWindow(w)) {
                        sink_.Emit(result);
                    }
                    stats_.windows_refired++;
                }
                state_.ClearRefired();

                // GC windows past allowed lateness
                auto gc_windows = state_.GarbageCollectableWindows(watermark_.Current());
                state_.GarbageCollect(gc_windows);
            }

            stats_.watermarks_advanced++;
        }
    } else if (c.type == ControlType::kCheckpointBarrier) {
        // Phase 1: no snapshotting. The barrier is broadcast to every worker
        // (see PartitionedPipeline::RouterLoop) but workers ignore it here.
        // Phase 3 (distributed checkpoint) hooks in exactly at this point.
    }
}

// Mirrors the final-flush block of Pipeline::Run.
void KeyedProcessor::FinalFlush() {
    if (use_lateness_) {
        for (auto& w : state_.RefiredWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_.Emit(result);
            }
            stats_.windows_refired++;
        }
        state_.ClearRefired();
        for (auto& w : state_.AllWindows()) {
            if (!state_.IsFired(w)) {
                for (auto& result : state_.FireWindow(w)) {
                    sink_.Emit(result);
                }
                stats_.windows_fired++;
            }
        }
        auto remaining = state_.AllWindows();
        state_.GarbageCollect(remaining);
    } else {
        for (auto& w : state_.AllWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_.Emit(result);
            }
            stats_.windows_fired++;
        }
    }
    sink_.Flush();
}

} // namespace stormglass

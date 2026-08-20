#include "engine/keyed_processor.h"

#include "checkpoint/writer.h"

#include <utility>

namespace stormglass {

KeyedProcessor::KeyedProcessor(std::unique_ptr<WindowAssigner> assigner,
                               Sink& sink,
                               Duration allowed_lateness,
                               std::string checkpoint_dir)
    : assigner_(std::move(assigner)),
      sink_(sink),
      allowed_lateness_(allowed_lateness),
      use_lateness_(allowed_lateness.count() > 0),
      checkpoint_dir_(std::move(checkpoint_dir)) {
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
        // Single-input per-worker snapshot. Records reach this worker in order,
        // so everything at or below the barrier's absolute offset has been
        // applied and nothing after it has — no marker buffering needed (each
        // worker has exactly one input queue). Snapshot this worker's state into
        // its partition directory with the UNCHANGED CheckpointWriter. Because
        // the Router broadcasts one absolute offset per barrier, all N workers
        // snapshot at the SAME offset O; the coordinator (see
        // distributed_checkpoint.h) treats the N files for O as one global
        // checkpoint, complete only when all N exist.
        if (!checkpoint_dir_.empty()) {
            CheckpointWriter writer(checkpoint_dir_);
            if (writer.WriteCheckpoint(c.checkpoint_offset, watermark_.Current(), state_)) {
                stats_.checkpoints_written++;
            }
        }
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

// Mirrors Pipeline::TryRestore: seed panes, the persisted fired-window set
// (v2 checkpoints), and the watermark before the worker starts consuming.
void KeyedProcessor::Restore(const CheckpointData& data) {
    for (const auto& entry : data.panes) {
        state_.RestorePane(entry.key, entry.window, entry.sum, entry.count);
    }
    if (!data.fired_windows.empty()) {
        state_.RestoreFiredWindows(data.fired_windows);
    }
    watermark_.Advance(data.watermark);
}

} // namespace stormglass

#include "engine/pipeline.h"

#include <variant>

namespace stormglass {

// Overloaded helper for std::visit
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

Pipeline::Pipeline(std::unique_ptr<Source> source,
                   std::unique_ptr<WindowAssigner> assigner,
                   std::unique_ptr<Sink> sink,
                   PipelineConfig config)
    : source_(std::move(source)),
      assigner_(std::move(assigner)),
      sink_(std::move(sink)),
      config_(config) {
    if (config_.allowed_lateness.count() > 0) {
        state_.SetAllowedLateness(config_.allowed_lateness);
    }
}

bool Pipeline::checkpointing_enabled() const {
    return !config_.checkpoint_dir.empty() && config_.checkpoint_interval > 0;
}

void Pipeline::TryRestore() {
    CheckpointReader reader(config_.checkpoint_dir);
    auto data = reader.LoadLatest();
    if (!data.has_value()) return;

    // Restore pane state
    for (const auto& entry : data->panes) {
        state_.RestorePane(entry.key, entry.window, entry.sum, entry.count);
    }

    // Restore watermark
    watermark_.Advance(data->watermark);

    // Seek source past the checkpointed offset
    source_->Seek(data->offset);
    restored_offset_ = data->offset;
}

void Pipeline::WriteCheckpoint(uint64_t offset, Stats& stats) {
    CheckpointWriter writer(config_.checkpoint_dir);
    if (writer.WriteCheckpoint(offset, watermark_.Current(), state_)) {
        stats.checkpoints_written++;
    }
}

Pipeline::Stats Pipeline::Run() {
    Stats stats{};
    bool use_lateness = config_.allowed_lateness.count() > 0;

    // Attempt restore before processing
    if (checkpointing_enabled()) {
        TryRestore();
        if (restored_offset_ > 0) {
            stats.records_replayed = restored_offset_;
        }
    }

    while (auto batch = source_->Next()) {
        for (auto& item : batch->items) {
            std::visit(overloaded{
                [&](const Record& r) {
                    auto windows = assigner_->AssignWindows(r.event_time);
                    bool any_dropped = false;
                    bool any_late_accepted = false;
                    for (auto& w : windows) {
                        if (use_lateness) {
                            bool was_fired = state_.IsFired(w);
                            bool accepted = state_.AddWithLateness(
                                r.key, w, r.value, watermark_.Current());
                            if (!accepted) {
                                any_dropped = true;
                            } else if (was_fired) {
                                any_late_accepted = true;
                            }
                        } else {
                            state_.Add(r.key, w, r.value);
                        }
                    }
                    if (any_dropped) stats.late_records_dropped++;
                    if (any_late_accepted) stats.late_records_accepted++;
                    stats.records_processed++;

                    // Checkpoint trigger based on record count
                    if (checkpointing_enabled()) {
                        records_since_checkpoint_++;
                        if (records_since_checkpoint_ >= config_.checkpoint_interval) {
                            records_since_checkpoint_ = 0;
                            // Use records_processed as checkpoint offset — this is the
                            // exact count of records whose effects are in the state.
                            WriteCheckpoint(restored_offset_ + stats.records_processed, stats);
                        }
                    }
                },
                [&](const ControlRecord& c) {
                    if (c.type == ControlType::kWatermark) {
                        if (watermark_.Advance(c.watermark)) {
                            // Fire expired windows (first-time fire)
                            for (auto& w : state_.ExpiredWindows(watermark_.Current())) {
                                for (auto& result : state_.FireWindow(w)) {
                                    sink_->Emit(result);
                                }
                                stats.windows_fired++;
                            }

                            // Re-fire windows that received late data
                            if (use_lateness) {
                                for (auto& w : state_.RefiredWindows()) {
                                    for (auto& result : state_.FireWindow(w)) {
                                        sink_->Emit(result);
                                    }
                                    stats.windows_refired++;
                                }
                                state_.ClearRefired();

                                // GC windows past allowed lateness
                                auto gc_windows = state_.GarbageCollectableWindows(watermark_.Current());
                                state_.GarbageCollect(gc_windows);
                            }

                            stats.watermarks_advanced++;
                        }
                    }
                }
            }, item);
        }
    }

    // Final flush: fire all remaining windows
    if (use_lateness) {
        for (auto& w : state_.RefiredWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_->Emit(result);
            }
            stats.windows_refired++;
        }
        state_.ClearRefired();
        for (auto& w : state_.AllWindows()) {
            if (!state_.IsFired(w)) {
                for (auto& result : state_.FireWindow(w)) {
                    sink_->Emit(result);
                }
                stats.windows_fired++;
            }
        }
        auto remaining = state_.AllWindows();
        state_.GarbageCollect(remaining);
    } else {
        for (auto& w : state_.AllWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_->Emit(result);
            }
            stats.windows_fired++;
        }
    }
    sink_->Flush();

    return stats;
}

} // namespace stormglass

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

Pipeline::Stats Pipeline::Run() {
    Stats stats{};
    bool use_lateness = config_.allowed_lateness.count() > 0;

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
        // Re-emit any pending re-fires
        for (auto& w : state_.RefiredWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_->Emit(result);
            }
            stats.windows_refired++;
        }
        state_.ClearRefired();
        // Fire windows that were never fired
        for (auto& w : state_.AllWindows()) {
            if (!state_.IsFired(w)) {
                for (auto& result : state_.FireWindow(w)) {
                    sink_->Emit(result);
                }
                stats.windows_fired++;
            }
        }
        // Clean up all remaining state
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

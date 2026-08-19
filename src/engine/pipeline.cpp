#include "engine/pipeline.h"

#include <variant>

namespace stormglass {

// Overloaded helper for std::visit
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

Pipeline::Pipeline(std::unique_ptr<Source> source,
                   std::unique_ptr<WindowAssigner> assigner,
                   std::unique_ptr<Sink> sink)
    : source_(std::move(source)),
      assigner_(std::move(assigner)),
      sink_(std::move(sink)) {}

Pipeline::Stats Pipeline::Run() {
    Stats stats{};

    while (auto batch = source_->Next()) {
        for (auto& item : batch->items) {
            std::visit(overloaded{
                [&](const Record& r) {
                    auto windows = assigner_->AssignWindows(r.event_time);
                    for (auto& w : windows) {
                        state_.Add(r.key, w, r.value);
                    }
                    stats.records_processed++;
                },
                [&](const ControlRecord& c) {
                    if (c.type == ControlType::kWatermark) {
                        if (watermark_.Advance(c.watermark)) {
                            for (auto& w : state_.ExpiredWindows(watermark_.Current())) {
                                for (auto& result : state_.FireWindow(w)) {
                                    sink_->Emit(result);
                                }
                                stats.windows_fired++;
                            }
                            stats.watermarks_advanced++;
                        }
                    }
                }
            }, item);
        }
    }

    // Final flush: fire all remaining windows
    for (auto& w : state_.AllWindows()) {
        for (auto& result : state_.FireWindow(w)) {
            sink_->Emit(result);
        }
        stats.windows_fired++;
    }
    sink_->Flush();

    return stats;
}

} // namespace stormglass

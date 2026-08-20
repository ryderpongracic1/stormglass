#include "oracle/oracle.h"

#include <algorithm>

namespace stormglass {

Oracle::Oracle(OracleConfig config) : config_(config) {
    // If slide is 0, treat as tumbling (slide == window_size)
    if (config_.slide == Duration{0}) {
        config_.slide = config_.window_size;
    }
}

std::vector<Window> Oracle::AssignWindows(Timestamp event_time) const {
    auto ms = event_time.time_since_epoch().count();
    auto window_ms = config_.window_size.count();
    auto slide_ms = config_.slide.count();

    std::vector<Window> windows;

    if (slide_ms == window_ms) {
        // Tumbling: one window per record (floor-division alignment)
        auto start_ms = (ms / window_ms) * window_ms;
        windows.push_back(Window{
            Timestamp{Duration{start_ms}},
            Timestamp{Duration{start_ms + window_ms}}});
    } else {
        // Sliding: record may belong to multiple windows
        // Find the earliest window whose end > event_time
        // Window starts are spaced slide_ms apart at aligned offsets
        auto last_start = (ms / slide_ms) * slide_ms;
        for (auto start = last_start;
             start > last_start - window_ms && start >= 0;
             start -= slide_ms) {
            auto end = start + window_ms;
            if (ms >= start && ms < end) {
                windows.push_back(Window{
                    Timestamp{Duration{start}},
                    Timestamp{Duration{end}}});
            }
        }
    }

    return windows;
}

void Oracle::AddRecord(const Record& record) {
    // Predict per-window drops using the engine's rule: a window is closed once
    // watermark >= window.end + allowed_lateness. This is evaluated per window
    // (a sliding-window record can be dropped from an already-closed window while
    // still landing in a younger, open one) and mirrors the engine's per-record
    // "any window dropped" counter.
    auto windows = AssignWindows(record.event_time);
    bool any_dropped = false;

    for (const auto& w : windows) {
        bool dropped = current_watermark_ > Timestamp::min() &&
                       current_watermark_ >= w.end + config_.allowed_lateness;
        if (dropped) {
            any_dropped = true;
            continue;  // record excluded from this closed window
        }
        auto& pane = data_[record.key][w];
        pane.sum += record.value;
        pane.count++;
    }

    if (any_dropped) {
        ++predicted_drops_;
    }
}

void Oracle::AdvanceWatermark(Timestamp wm) {
    if (wm > current_watermark_) {
        current_watermark_ = wm;
    }
}

std::vector<WindowResult> Oracle::ComputeResults() const {
    std::vector<WindowResult> results;

    for (const auto& [key, windows] : data_) {
        for (const auto& [window, pane] : windows) {
            results.push_back(WindowResult{
                .key = key,
                .window = window,
                .result = AggregateResult{.value = pane.sum, .count = pane.count},
            });
        }
    }

    // Sort deterministically: by window.start, then by key
    std::sort(results.begin(), results.end(),
              [](const WindowResult& a, const WindowResult& b) {
                  if (a.window.start != b.window.start)
                      return a.window.start < b.window.start;
                  return a.key < b.key;
              });

    return results;
}

}  // namespace stormglass

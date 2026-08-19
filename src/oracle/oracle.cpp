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
    // Check for late-data drop prediction
    if (config_.allowed_lateness > Duration{0}) {
        auto windows = AssignWindows(record.event_time);
        bool all_dropped = true;
        for (const auto& w : windows) {
            // A record is dropped if its window has already been closed:
            // window.end + allowed_lateness <= current_watermark
            if (w.end + config_.allowed_lateness > current_watermark_) {
                all_dropped = false;
                break;
            }
        }
        if (all_dropped && current_watermark_ > Timestamp::min()) {
            ++predicted_drops_;
            return;  // Don't accumulate dropped records
        }
    }

    // Accumulate into panes
    auto windows = AssignWindows(record.event_time);
    for (const auto& w : windows) {
        auto& pane = data_[record.key][w];
        pane.sum += record.value;
        pane.count++;
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

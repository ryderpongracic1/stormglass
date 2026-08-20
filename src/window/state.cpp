#include "window/state.h"

namespace stormglass {

void KeyedWindowState::SetAllowedLateness(Duration lateness) {
    allowed_lateness_ = lateness;
}

void KeyedWindowState::Add(const std::string& key, const Window& window, int64_t value) {
    windows_[window][key].Add(value);
}

bool KeyedWindowState::AddWithLateness(const std::string& key, const Window& window,
                                        int64_t value, Timestamp current_watermark) {
    // If watermark has passed the absolute deadline, always drop
    auto deadline = window.end + allowed_lateness_;
    if (current_watermark >= deadline) {
        return false;
    }

    windows_[window][key].Add(value);

    // Window already fired but still within lateness — mark for re-fire.
    if (fired_windows_.count(window)) {
        refired_windows_.insert(window);
    }
    return true;
}

std::vector<Window> KeyedWindowState::ExpiredWindows(Timestamp watermark) const {
    // windows_ is ordered by window-end, so every expired window is a prefix.
    // Walk that prefix and stop at the first window whose end is beyond the
    // watermark — cost is proportional to expiring windows, not to live panes.
    std::vector<Window> expired;
    for (const auto& [window, panes] : windows_) {
        if (window.end > watermark) break;
        if (!fired_windows_.count(window)) {
            expired.push_back(window);
        }
    }
    return expired;
}

std::vector<WindowResult> KeyedWindowState::FireWindow(const Window& window) {
    std::vector<WindowResult> results;

    auto it = windows_.find(window);
    if (it == windows_.end()) {
        return results;
    }

    results.reserve(it->second.size());
    for (const auto& [key, pane] : it->second) {
        results.push_back(WindowResult{
            .key = key,
            .window = window,
            .result = AggregateResult{.value = pane.sum, .count = pane.count},
        });
    }

    if (allowed_lateness_.count() == 0) {
        // No lateness: erase the window's panes immediately (original behavior).
        windows_.erase(it);
    } else {
        // Keep panes alive for late data; mark the window fired.
        fired_windows_.insert(window);
    }

    return results;
}

std::vector<Window> KeyedWindowState::GarbageCollectableWindows(Timestamp watermark) const {
    std::vector<Window> gc;
    for (const auto& w : fired_windows_) {
        auto deadline = w.end + allowed_lateness_;
        if (watermark >= deadline) {
            gc.push_back(w);
        }
    }
    return gc;
}

void KeyedWindowState::GarbageCollect(const std::vector<Window>& windows) {
    for (const auto& w : windows) {
        windows_.erase(w);
        fired_windows_.erase(w);
        refired_windows_.erase(w);
    }
}

std::vector<Window> KeyedWindowState::RefiredWindows() const {
    return {refired_windows_.begin(), refired_windows_.end()};
}

void KeyedWindowState::ClearRefired() {
    refired_windows_.clear();
}

bool KeyedWindowState::IsFired(const Window& window) const {
    return fired_windows_.count(window) > 0;
}

std::vector<Window> KeyedWindowState::AllWindows() const {
    std::vector<Window> windows;
    windows.reserve(windows_.size());
    for (const auto& [window, panes] : windows_) {
        windows.push_back(window);
    }
    return windows;
}

size_t KeyedWindowState::TotalPanes() const {
    size_t total = 0;
    for (const auto& [window, panes] : windows_) {
        total += panes.size();
    }
    return total;
}

void KeyedWindowState::RestorePane(const std::string& key, const Window& window,
                                    int64_t sum, uint64_t count) {
    auto& pane = windows_[window][key];
    pane.sum = sum;
    pane.count = count;
}

} // namespace stormglass

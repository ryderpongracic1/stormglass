#include "window/state.h"

#include <algorithm>
#include <unordered_set>

namespace stormglass {

void KeyedWindowState::SetAllowedLateness(Duration lateness) {
    allowed_lateness_ = lateness;
}

void KeyedWindowState::Add(const std::string& key, const Window& window, int64_t value) {
    panes_[KeyWindow{key, window}].Add(value);
}

bool KeyedWindowState::AddWithLateness(const std::string& key, const Window& window,
                                        int64_t value, Timestamp current_watermark) {
    // If watermark has passed the absolute deadline, always drop
    auto deadline = window.end + allowed_lateness_;
    if (current_watermark >= deadline) {
        return false;
    }

    // If the window hasn't been fired yet, accept normally
    if (!fired_windows_.count(window)) {
        panes_[KeyWindow{key, window}].Add(value);
        return true;
    }

    // Window has been fired but within lateness — accept late record, mark for re-fire
    panes_[KeyWindow{key, window}].Add(value);
    refired_windows_.insert(window);
    return true;
}

std::vector<Window> KeyedWindowState::ExpiredWindows(Timestamp watermark) const {
    std::unordered_set<Window, WindowHash> expired;
    for (const auto& [kw, pane] : panes_) {
        if (kw.window.end <= watermark && !fired_windows_.count(kw.window)) {
            expired.insert(kw.window);
        }
    }
    return {expired.begin(), expired.end()};
}

std::vector<WindowResult> KeyedWindowState::FireWindow(const Window& window) {
    std::vector<WindowResult> results;

    for (const auto& [kw, pane] : panes_) {
        if (kw.window == window) {
            results.push_back(WindowResult{
                .key = kw.key,
                .window = window,
                .result = AggregateResult{.value = pane.sum, .count = pane.count},
            });
        }
    }

    // If no allowed lateness, erase panes immediately (original behavior)
    if (allowed_lateness_.count() == 0) {
        std::vector<KeyWindow> to_erase;
        for (const auto& [kw, pane] : panes_) {
            if (kw.window == window) {
                to_erase.push_back(kw);
            }
        }
        for (const auto& kw : to_erase) {
            panes_.erase(kw);
        }
    } else {
        // Mark as fired but keep panes alive for late data
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
        // Remove all panes for this window
        std::vector<KeyWindow> to_erase;
        for (const auto& [kw, pane] : panes_) {
            if (kw.window == w) {
                to_erase.push_back(kw);
            }
        }
        for (const auto& kw : to_erase) {
            panes_.erase(kw);
        }
        // Remove from tracking sets
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
    std::unordered_set<Window, WindowHash> windows;
    for (const auto& [kw, pane] : panes_) {
        windows.insert(kw.window);
    }
    return {windows.begin(), windows.end()};
}

void KeyedWindowState::RestorePane(const std::string& key, const Window& window,
                                    int64_t sum, uint64_t count) {
    auto& pane = panes_[KeyWindow{key, window}];
    pane.sum = sum;
    pane.count = count;
}

} // namespace stormglass

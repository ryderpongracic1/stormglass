#include "window/state.h"

#include <algorithm>
#include <unordered_set>

namespace stormglass {

void KeyedWindowState::Add(const std::string& key, const Window& window, int64_t value) {
    panes_[KeyWindow{key, window}].Add(value);
}

std::vector<Window> KeyedWindowState::ExpiredWindows(Timestamp watermark) const {
    std::unordered_set<Window, WindowHash> expired;
    for (const auto& [kw, pane] : panes_) {
        if (kw.window.end <= watermark) {
            expired.insert(kw.window);
        }
    }
    return {expired.begin(), expired.end()};
}

std::vector<WindowResult> KeyedWindowState::FireWindow(const Window& window) {
    std::vector<WindowResult> results;
    // Collect all keys for this window, then erase
    std::vector<KeyWindow> to_erase;
    for (const auto& [kw, pane] : panes_) {
        if (kw.window == window) {
            results.push_back(WindowResult{
                .key = kw.key,
                .window = window,
                .result = AggregateResult{.value = pane.sum, .count = pane.count},
            });
            to_erase.push_back(kw);
        }
    }
    for (const auto& kw : to_erase) {
        panes_.erase(kw);
    }
    return results;
}

std::vector<Window> KeyedWindowState::AllWindows() const {
    std::unordered_set<Window, WindowHash> windows;
    for (const auto& [kw, pane] : panes_) {
        windows.insert(kw.window);
    }
    return {windows.begin(), windows.end()};
}

} // namespace stormglass

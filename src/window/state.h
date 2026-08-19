#pragma once

#include "window/window.h"
#include "sink/sink.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace stormglass {

struct KeyWindow {
    std::string key;
    Window window;
    bool operator==(const KeyWindow&) const = default;
};

struct KeyWindowHash {
    size_t operator()(const KeyWindow& kw) const {
        auto h1 = std::hash<std::string>{}(kw.key);
        auto h2 = WindowHash{}(kw.window);
        return h1 ^ (h2 * 0x9e3779b97f4a7c15ULL + 0x9e3779b9 + (h1 << 6) + (h1 >> 2));
    }
};

struct Pane {
    int64_t sum = 0;
    uint64_t count = 0;
    void Add(int64_t v) { sum += v; ++count; }
};

class KeyedWindowState {
public:
    void Add(const std::string& key, const Window& window, int64_t value);
    [[nodiscard]] std::vector<Window> ExpiredWindows(Timestamp watermark) const;
    std::vector<WindowResult> FireWindow(const Window& window);
    [[nodiscard]] std::vector<Window> AllWindows() const;

private:
    std::unordered_map<KeyWindow, Pane, KeyWindowHash> panes_;
};

} // namespace stormglass

#pragma once

#include "window/window.h"
#include "sink/sink.h"

#include <string>
#include <unordered_map>
#include <unordered_set>
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
    // Existing interface (unchanged behavior when allowed_lateness == 0)
    void Add(const std::string& key, const Window& window, int64_t value);
    [[nodiscard]] std::vector<Window> ExpiredWindows(Timestamp watermark) const;
    std::vector<WindowResult> FireWindow(const Window& window);
    [[nodiscard]] std::vector<Window> AllWindows() const;

    // Late-data policy
    void SetAllowedLateness(Duration lateness);

    // Returns true if the record was accepted (window still within lateness).
    // Returns false if dropped (too late).
    bool AddWithLateness(const std::string& key, const Window& window,
                         int64_t value, Timestamp current_watermark);

    // Windows that should be finally GC'd (watermark > end + allowed_lateness)
    [[nodiscard]] std::vector<Window> GarbageCollectableWindows(Timestamp watermark) const;

    // Actually remove panes and tracking for GC'd windows
    void GarbageCollect(const std::vector<Window>& windows);

    // Windows that received late data after initial fire and need re-emission
    [[nodiscard]] std::vector<Window> RefiredWindows() const;
    void ClearRefired();

    // Check if a window has already been fired
    [[nodiscard]] bool IsFired(const Window& window) const;

    // For checkpoint serialization: read-only access to all panes
    [[nodiscard]] const std::unordered_map<KeyWindow, Pane, KeyWindowHash>& Panes() const {
        return panes_;
    }

    // For checkpoint serialization: read-only access to fired windows
    [[nodiscard]] const std::unordered_set<Window, WindowHash>& FiredWindows() const {
        return fired_windows_;
    }

    // For checkpoint restoration: insert a pane directly
    void RestorePane(const std::string& key, const Window& window, int64_t sum, uint64_t count);

    // For checkpoint restoration: restore fired windows from checkpoint
    void RestoreFiredWindows(const std::vector<Window>& windows) {
        for (const auto& w : windows) {
            fired_windows_.insert(w);
        }
    }

private:
    std::unordered_map<KeyWindow, Pane, KeyWindowHash> panes_;
    Duration allowed_lateness_{0};
    std::unordered_set<Window, WindowHash> fired_windows_;
    std::unordered_set<Window, WindowHash> refired_windows_;
};

} // namespace stormglass

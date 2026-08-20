#pragma once

#include "window/window.h"
#include "sink/sink.h"

#include <map>
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

// Orders windows by end (then start) so that "all windows expired by watermark"
// is a prefix of the ordered structure — a bounded walk instead of a full scan.
struct WindowByEnd {
    bool operator()(const Window& a, const Window& b) const {
        if (a.end != b.end) return a.end < b.end;
        return a.start < b.start;
    }
};

// Keyed window state stored as Window -> { key -> Pane }, keyed on window-end.
//
// This is the Flink-style layout: watermark-driven work touches only the
// windows that actually expire (a prefix of the ordered map) and firing a
// window touches only that window's inner key map. The cost of a watermark
// advance is therefore proportional to the windows expiring, not to the number
// of live panes (keys) in the system.
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

    // For checkpoint serialization: total number of (key, window) panes.
    [[nodiscard]] size_t TotalPanes() const;

    // For checkpoint serialization: visit every pane as fn(key, window, pane).
    // Iteration order is unspecified; the checkpoint format does not depend on it.
    template <typename Fn>
    void ForEachPane(Fn&& fn) const {
        for (const auto& [window, panes] : windows_) {
            for (const auto& [key, pane] : panes) {
                fn(key, window, pane);
            }
        }
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
    using PaneMap = std::unordered_map<std::string, Pane>;
    std::map<Window, PaneMap, WindowByEnd> windows_;
    Duration allowed_lateness_{0};
    std::unordered_set<Window, WindowHash> fired_windows_;
    std::unordered_set<Window, WindowHash> refired_windows_;
};

} // namespace stormglass

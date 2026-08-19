#pragma once

#include "stream/record.h"
#include "window/window.h"
#include "sink/sink.h"

#include <string>
#include <unordered_map>
#include <vector>

namespace stormglass {

struct OracleConfig {
    Duration window_size;
    Duration slide{0};           // 0 = tumbling (slide == window_size)
    Duration allowed_lateness{0};
};

/// The oracle computes expected results WITHOUT watermarks.
/// It sees ALL records and groups them by (key, window) naively.
/// When allowed_lateness > 0, it predicts which records would be dropped
/// given the watermark model (wm = max_event_time - max_disorder).
///
/// Its correctness should be obvious by inspection: no incremental state
/// management, no watermark-driven firing — just grouping and summing.
class Oracle {
public:
    explicit Oracle(OracleConfig config);

    /// Feed a single record (in arrival order) to the oracle
    void AddRecord(const Record& record);

    /// Feed a watermark advancement (for late-data prediction)
    void AdvanceWatermark(Timestamp wm);

    /// Compute expected window results.
    /// Returns results sorted by (window.start, key) for deterministic comparison.
    [[nodiscard]] std::vector<WindowResult> ComputeResults() const;

    /// How many records would be dropped by the late-data policy?
    [[nodiscard]] uint64_t PredictedDropCount() const { return predicted_drops_; }

private:
    /// Assign windows for a given event time (tumbling or sliding)
    [[nodiscard]] std::vector<Window> AssignWindows(Timestamp event_time) const;

    OracleConfig config_;

    struct PaneData {
        int64_t sum = 0;
        uint64_t count = 0;
    };

    // key -> window -> pane accumulator
    std::unordered_map<std::string,
                       std::unordered_map<Window, PaneData, WindowHash>> data_;

    uint64_t predicted_drops_ = 0;
    Timestamp current_watermark_{Timestamp::min()};
};

}  // namespace stormglass

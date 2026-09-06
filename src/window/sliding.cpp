#include "window/sliding.h"

#include <algorithm>
#include <limits>
#include <stdexcept>

namespace stormglass {

SlidingAssigner::SlidingAssigner(Duration window_size, Duration slide_interval)
    : window_size_(window_size), slide_(slide_interval) {
    if (window_size.count() <= 0 || slide_interval.count() <= 0)
        throw std::invalid_argument("window size and slide must be positive");
}

std::vector<Window> SlidingAssigner::AssignWindows(Timestamp event_time) const {
    auto t = event_time.time_since_epoch().count();
    auto win_ms = window_size_.count();
    auto slide_ms = slide_.count();

    if (t < 0 || t > std::numeric_limits<int64_t>::max() - std::max(win_ms, slide_ms))
        throw std::out_of_range("event time outside supported nonnegative window domain");
    // Earliest window start containing t: floor((t - win_ms) / slide_ms + 1) * slide_ms
    // But clamp to >= 0
    int64_t earliest_start;
    int64_t numerator = t - win_ms + slide_ms;  // (t - win_ms)/slide_ms + 1 = (t - win_ms + slide_ms) / slide_ms
    if (numerator <= 0) {
        earliest_start = 0;
    } else {
        // floor division for positive values
        earliest_start = (numerator / slide_ms) * slide_ms;
    }

    std::vector<Window> windows;
    for (int64_t start = earliest_start; start <= t; start += slide_ms) {
        if (start + win_ms > t) {
            // t is within [start, start+win_ms)
            windows.push_back(Window{
                Timestamp{Duration{start}},
                Timestamp{Duration{start + win_ms}}
            });
        }
    }

    return windows;
}

} // namespace stormglass

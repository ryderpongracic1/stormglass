#include "window/tumbling.h"
#include <limits>
namespace stormglass {
std::vector<Window> TumblingAssigner::AssignWindows(Timestamp event_time) const {
    auto ms = event_time.time_since_epoch().count();
    auto window_ms = size_.count();
    if (ms < 0 || ms > std::numeric_limits<int64_t>::max() - window_ms)
        throw std::out_of_range("event time outside supported nonnegative window domain");
    auto start_ms = (ms / window_ms) * window_ms;
    return {{Timestamp{Duration{start_ms}}, Timestamp{Duration{start_ms + window_ms}}}};
}
} // namespace stormglass

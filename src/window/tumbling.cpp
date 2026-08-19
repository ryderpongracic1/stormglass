#include "window/tumbling.h"
namespace stormglass {
std::vector<Window> TumblingAssigner::AssignWindows(Timestamp event_time) const {
    auto ms = event_time.time_since_epoch().count();
    auto window_ms = size_.count();
    auto start_ms = (ms / window_ms) * window_ms;
    return {{Timestamp{Duration{start_ms}}, Timestamp{Duration{start_ms + window_ms}}}};
}
} // namespace stormglass

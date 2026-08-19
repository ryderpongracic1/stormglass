#pragma once

#include "window/window.h"

namespace stormglass {

class SlidingAssigner : public WindowAssigner {
public:
    SlidingAssigner(Duration window_size, Duration slide_interval);
    std::vector<Window> AssignWindows(Timestamp event_time) const override;

private:
    Duration window_size_;
    Duration slide_;
};

} // namespace stormglass

#pragma once
#include "window/window.h"
namespace stormglass {
class TumblingAssigner : public WindowAssigner {
public:
    explicit TumblingAssigner(Duration size) : size_(size) {}
    std::vector<Window> AssignWindows(Timestamp event_time) const override;
private:
    Duration size_;
};
} // namespace stormglass

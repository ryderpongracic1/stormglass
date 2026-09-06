#pragma once
#include "window/window.h"
#include <stdexcept>
namespace stormglass {
class TumblingAssigner : public WindowAssigner {
public:
    explicit TumblingAssigner(Duration size) : size_(size) {
        if (size.count() <= 0) throw std::invalid_argument("window size must be positive");
    }
    std::vector<Window> AssignWindows(Timestamp event_time) const override;
private:
    Duration size_;
};
} // namespace stormglass

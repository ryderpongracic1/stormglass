#pragma once
#include "stream/record.h"

namespace stormglass {

class WatermarkTracker {
public:
    bool Advance(Timestamp wm);
    [[nodiscard]] Timestamp Current() const { return current_; }

private:
    Timestamp current_{Timestamp::min()};
};

} // namespace stormglass

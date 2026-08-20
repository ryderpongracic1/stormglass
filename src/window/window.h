#pragma once
#include "stream/record.h"
#include <vector>

namespace stormglass {

struct Window {
    Timestamp start;
    Timestamp end;
    bool operator==(const Window&) const = default;
};

struct WindowHash {
    size_t operator()(const Window& w) const;
};

class WindowAssigner {
public:
    virtual ~WindowAssigner() = default;
    virtual std::vector<Window> AssignWindows(Timestamp event_time) const = 0;
};

} // namespace stormglass

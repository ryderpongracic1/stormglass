#pragma once
#include "stream/record.h"
#include <string>
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

    // Stable description of the window geometry, recorded in the checkpoint job
    // manifest so restore rejects a changed assigner. Empty means unvalidated.
    [[nodiscard]] virtual std::string Descriptor() const { return ""; }
};

} // namespace stormglass

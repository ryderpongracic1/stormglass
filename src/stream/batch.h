#pragma once
#include "stream/record.h"
#include <variant>

namespace stormglass {

using BatchItem = std::variant<Record, ControlRecord>;

struct Batch {
    std::vector<BatchItem> items;

    [[nodiscard]] bool empty() const { return items.empty(); }
    [[nodiscard]] size_t size() const { return items.size(); }
};

} // namespace stormglass

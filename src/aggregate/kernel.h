#pragma once
#include <cstdint>

namespace stormglass {

/// Result of a windowed aggregation: the accumulated value and how many
/// records contributed to it. Panes accumulate directly (sum + count);
/// this is the emission payload carried by WindowResult.
struct AggregateResult {
    int64_t value;
    uint64_t count;
};

} // namespace stormglass

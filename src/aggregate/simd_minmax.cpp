#include "aggregate/simd_minmax.h"

namespace stormglass {

// --- SimdMinInt64Kernel ---

void SimdMinInt64Kernel::Add(int64_t value) {
    if (value < min_) min_ = value;
    ++count_;
}

void SimdMinInt64Kernel::AddBatch(std::span<const int64_t> values) {
    if (values.empty()) return;

    // Tight scalar loop — the compiler will auto-vectorize this under -O3
    // with the appropriate -march flags. Using explicit min avoids branches.
    int64_t local_min = min_;
    for (const auto v : values) {
        local_min = v < local_min ? v : local_min;
    }
    min_ = local_min;
    count_ += values.size();
}

AggregateResult SimdMinInt64Kernel::Result() const {
    return {min_, count_};
}

void SimdMinInt64Kernel::Reset() {
    min_ = std::numeric_limits<int64_t>::max();
    count_ = 0;
}

// --- SimdMaxInt64Kernel ---

void SimdMaxInt64Kernel::Add(int64_t value) {
    if (value > max_) max_ = value;
    ++count_;
}

void SimdMaxInt64Kernel::AddBatch(std::span<const int64_t> values) {
    if (values.empty()) return;

    // Tight scalar loop — the compiler will auto-vectorize this under -O3
    int64_t local_max = max_;
    for (const auto v : values) {
        local_max = v > local_max ? v : local_max;
    }
    max_ = local_max;
    count_ += values.size();
}

AggregateResult SimdMaxInt64Kernel::Result() const {
    return {max_, count_};
}

void SimdMaxInt64Kernel::Reset() {
    max_ = std::numeric_limits<int64_t>::min();
    count_ = 0;
}

} // namespace stormglass

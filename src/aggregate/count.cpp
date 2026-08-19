#include "aggregate/count.h"

namespace stormglass {

void CountKernel::Add(int64_t /*value*/) { ++count_; }

void CountKernel::AddBatch(std::span<const int64_t> values) {
    count_ += values.size();
}

AggregateResult CountKernel::Result() const {
    return {static_cast<int64_t>(count_), count_};
}

void CountKernel::Reset() { count_ = 0; }

} // namespace stormglass

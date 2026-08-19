#include "aggregate/sum.h"
namespace stormglass {
void SumInt64Kernel::Add(int64_t value) { sum_ += value; ++count_; }
void SumInt64Kernel::AddBatch(std::span<const int64_t> values) { for (auto v : values) sum_ += v; count_ += values.size(); }
AggregateResult SumInt64Kernel::Result() const { return {sum_, count_}; }
void SumInt64Kernel::Reset() { sum_ = 0; count_ = 0; }
} // namespace stormglass

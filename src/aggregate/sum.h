#pragma once
#include "aggregate/kernel.h"
namespace stormglass {
class SumInt64Kernel : public AggregateKernel {
public:
    void Add(int64_t value) override;
    void AddBatch(std::span<const int64_t> values) override;
    [[nodiscard]] AggregateResult Result() const override;
    void Reset() override;
private:
    int64_t sum_ = 0; uint64_t count_ = 0;
};
} // namespace stormglass

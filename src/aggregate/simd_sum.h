#pragma once

#include "aggregate/kernel.h"

namespace stormglass {

/// SIMD-accelerated sum kernel.
/// Uses AVX2 (4 int64s/cycle) when available, SSE4.2 (2 int64s/cycle) otherwise,
/// with scalar fallback for remainder elements.
class SimdSumInt64Kernel : public AggregateKernel {
public:
    void Add(int64_t value) override;
    void AddBatch(std::span<const int64_t> values) override;
    [[nodiscard]] AggregateResult Result() const override;
    void Reset() override;

private:
    int64_t sum_ = 0;
    uint64_t count_ = 0;
};

} // namespace stormglass

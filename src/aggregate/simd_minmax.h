#pragma once

#include "aggregate/kernel.h"

#include <limits>

namespace stormglass {

/// Min kernel with tight scalar loop for compiler auto-vectorization under -O3.
class SimdMinInt64Kernel : public AggregateKernel {
public:
    void Add(int64_t value) override;
    void AddBatch(std::span<const int64_t> values) override;
    [[nodiscard]] AggregateResult Result() const override;
    void Reset() override;

private:
    int64_t min_ = std::numeric_limits<int64_t>::max();
    uint64_t count_ = 0;
};

/// Max kernel with tight scalar loop for compiler auto-vectorization under -O3.
class SimdMaxInt64Kernel : public AggregateKernel {
public:
    void Add(int64_t value) override;
    void AddBatch(std::span<const int64_t> values) override;
    [[nodiscard]] AggregateResult Result() const override;
    void Reset() override;

private:
    int64_t max_ = std::numeric_limits<int64_t>::min();
    uint64_t count_ = 0;
};

} // namespace stormglass

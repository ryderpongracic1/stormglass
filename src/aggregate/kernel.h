#pragma once
#include <cstdint>
#include <span>
namespace stormglass {
struct AggregateResult { int64_t value; uint64_t count; };
class AggregateKernel {
public:
    virtual ~AggregateKernel() = default;
    virtual void Add(int64_t value) = 0;
    virtual void AddBatch(std::span<const int64_t> values) = 0;
    [[nodiscard]] virtual AggregateResult Result() const = 0;
    virtual void Reset() = 0;
};
} // namespace stormglass

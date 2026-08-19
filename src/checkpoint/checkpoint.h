#pragma once
#include <cstdint>
#include <optional>
#include <vector>
namespace stormglass {
class Checkpointer {
public:
    virtual ~Checkpointer() = default;
    virtual bool Snapshot(const std::vector<uint8_t>& state, uint64_t offset) = 0;
    virtual std::optional<std::pair<std::vector<uint8_t>, uint64_t>> Restore() = 0;
};
} // namespace stormglass

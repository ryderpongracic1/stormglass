#pragma once
#include "stream/batch.h"
#include <optional>

namespace stormglass {

class Source {
public:
    virtual ~Source() = default;
    virtual std::optional<Batch> Next() = 0;
    virtual void Seek(uint64_t offset) = 0;
    [[nodiscard]] virtual uint64_t CurrentOffset() const = 0;
};

} // namespace stormglass

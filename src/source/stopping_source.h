#pragma once

#include "source/source.h"

#include <cstdint>
#include <memory>

namespace stormglass {

// Wraps a source and stops after a maximum number of records.
// Used by the nemesis harness to simulate crash at a specific point.
class StoppingSource : public Source {
public:
    StoppingSource(std::unique_ptr<Source> inner, uint64_t max_records);

    std::optional<Batch> Next() override;
    void Seek(uint64_t offset) override;
    [[nodiscard]] uint64_t CurrentOffset() const override;

private:
    std::unique_ptr<Source> inner_;
    uint64_t max_records_;
    uint64_t emitted_ = 0;
};

} // namespace stormglass

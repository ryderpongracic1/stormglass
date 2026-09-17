#pragma once
#include "stream/batch.h"
#include <optional>
#include <string>

namespace stormglass {

class Source {
public:
    virtual ~Source() = default;
    virtual std::optional<Batch> Next() = 0;
    virtual void Seek(uint64_t offset) = 0;
    [[nodiscard]] virtual uint64_t CurrentOffset() const = 0;

    // Stable identity of the replayed record/control trajectory, recorded in a
    // checkpoint directory's job manifest so restore can reject a different
    // source. Include everything that changes what Seek(O) followed by Next()
    // yields (identity, ordering, watermark and barrier cadence); exclude what
    // only extends or regroups it (stream length, batch size). Empty means the
    // source is not validated on restore.
    [[nodiscard]] virtual std::string Descriptor() const { return ""; }
};

} // namespace stormglass

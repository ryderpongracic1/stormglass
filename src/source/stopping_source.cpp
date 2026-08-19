#include "source/stopping_source.h"

namespace stormglass {

StoppingSource::StoppingSource(std::unique_ptr<Source> inner, uint64_t max_records)
    : inner_(std::move(inner)), max_records_(max_records) {}

std::optional<Batch> StoppingSource::Next() {
    if (emitted_ >= max_records_) {
        return std::nullopt;
    }

    auto batch = inner_->Next();
    if (!batch.has_value()) return std::nullopt;

    // Count only records (not control records) toward the limit
    Batch result;
    for (auto& item : batch->items) {
        if (emitted_ >= max_records_) break;

        if (std::holds_alternative<Record>(item)) {
            result.items.push_back(std::move(item));
            ++emitted_;
        } else {
            // Always pass through control records (watermarks, barriers)
            result.items.push_back(std::move(item));
        }
    }

    if (result.empty()) return std::nullopt;
    return result;
}

void StoppingSource::Seek(uint64_t offset) {
    inner_->Seek(offset);
    emitted_ = 0;  // Reset emitted counter — offset tracking is in inner source
}

uint64_t StoppingSource::CurrentOffset() const {
    return inner_->CurrentOffset();
}

} // namespace stormglass

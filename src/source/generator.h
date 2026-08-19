#pragma once

#include "source/source.h"

#include <cstdint>
#include <random>

namespace stormglass {

struct GeneratorConfig {
    uint64_t seed = 42;
    uint32_t num_keys = 10;
    uint64_t num_records = 100000;
    Duration max_disorder{5000};
    uint32_t batch_size = 1024;
    uint32_t watermark_interval = 100;
};

class DeterministicGenerator : public Source {
public:
    explicit DeterministicGenerator(GeneratorConfig config);

    std::optional<Batch> Next() override;
    void Seek(uint64_t offset) override;
    [[nodiscard]] uint64_t CurrentOffset() const override;

private:
    Record GenerateRecord();

    GeneratorConfig config_;
    std::mt19937_64 rng_;
    uint64_t offset_ = 0;
    uint64_t records_since_watermark_ = 0;
    Timestamp max_event_time_seen_{Timestamp::min()};
};

} // namespace stormglass

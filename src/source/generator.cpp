#include "source/generator.h"

#include <algorithm>
#include <cstdio>

namespace stormglass {

DeterministicGenerator::DeterministicGenerator(GeneratorConfig config)
    : config_(config), rng_(config.seed) {}

Record DeterministicGenerator::GenerateRecord() {
    // Key: round-robin over num_keys, zero-padded 4 digits
    char key_buf[16];
    std::snprintf(key_buf, sizeof(key_buf), "key-%04u",
                  static_cast<unsigned>(offset_ % config_.num_keys));

    // Event time: monotonically increasing base (1ms apart) + disorder.
    auto base_ms = static_cast<int64_t>(offset_);  // 1ms per record
    int64_t event_ms;
    if (config_.disorder_mode == DisorderMode::kHeavyTailed &&
        config_.late_fraction > 0.0) {
        // Draw order is fixed at 3 rng draws per record in this mode
        // (coin, then tail-or-jitter, then value) so Seek() replay stays stable.
        std::bernoulli_distribution late_coin(config_.late_fraction);
        bool is_late = late_coin(rng_);
        if (is_late) {
            // Push strictly beyond the disorder bound so the record lands below
            // the emitted watermark (wm = max_seen - max_disorder) and is late.
            auto tail_max = std::max<int64_t>(1, config_.late_tail.count());
            std::uniform_int_distribution<int64_t> tail_dist(1, tail_max);
            event_ms = base_ms - config_.max_disorder.count() - tail_dist(rng_);
        } else {
            std::uniform_int_distribution<int64_t> jitter_dist(
                -config_.max_disorder.count(), 0);
            event_ms = base_ms + jitter_dist(rng_);
        }
    } else {
        std::uniform_int_distribution<int64_t> jitter_dist(
            -config_.max_disorder.count(), 0);
        event_ms = base_ms + jitter_dist(rng_);
    }
    if (event_ms < 0) event_ms = 0;

    // Value: uniform int64 in [1, 1000]
    std::uniform_int_distribution<int64_t> value_dist(1, 1000);
    auto value = value_dist(rng_);

    auto event_time = Timestamp{Duration{event_ms}};
    auto processing_time = Timestamp{Duration{base_ms}};

    // Track max event time seen
    if (event_time > max_event_time_seen_) {
        max_event_time_seen_ = event_time;
    }

    return Record{
        .key = std::string(key_buf),
        .value = value,
        .event_time = event_time,
        .processing_time = processing_time,
    };
}

std::optional<Batch> DeterministicGenerator::Next() {
    if (offset_ >= config_.num_records) {
        return std::nullopt;
    }

    Batch batch;
    auto records_this_batch = std::min(
        static_cast<uint64_t>(config_.batch_size),
        config_.num_records - offset_);

    batch.items.reserve(records_this_batch + records_this_batch / config_.watermark_interval + 1);

    for (uint64_t i = 0; i < records_this_batch; ++i) {
        batch.items.emplace_back(GenerateRecord());
        ++offset_;
        ++records_since_watermark_;

        // Inject watermark control record at interval
        if (records_since_watermark_ >= config_.watermark_interval) {
            records_since_watermark_ = 0;
            auto wm_time = max_event_time_seen_ - config_.max_disorder;
            if (wm_time < Timestamp{Duration{0}}) {
                wm_time = Timestamp{Duration{0}};
            }
            batch.items.emplace_back(ControlRecord{
                .type = ControlType::kWatermark,
                .watermark = wm_time,
                .checkpoint_offset = offset_,
            });
        }
    }

    return batch;
}

void DeterministicGenerator::Seek(uint64_t offset) {
    // Re-seed and replay to target offset (O(offset) — documented limitation)
    rng_.seed(config_.seed);
    offset_ = 0;
    records_since_watermark_ = 0;
    max_event_time_seen_ = Timestamp::min();

    // Replay generation to reach target offset
    while (offset_ < offset) {
        GenerateRecord();
        ++offset_;
        ++records_since_watermark_;
        if (records_since_watermark_ >= config_.watermark_interval) {
            records_since_watermark_ = 0;
        }
    }
}

uint64_t DeterministicGenerator::CurrentOffset() const {
    return offset_;
}

} // namespace stormglass

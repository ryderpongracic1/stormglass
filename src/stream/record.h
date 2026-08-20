#pragma once
#include <cstdint>
#include <chrono>
#include <string>
#include <vector>

namespace stormglass {

using Timestamp = std::chrono::time_point<std::chrono::system_clock, std::chrono::milliseconds>;
using Duration = std::chrono::milliseconds;

struct Record {
    std::string key;
    int64_t value;
    Timestamp event_time;
    Timestamp processing_time;
};

enum class ControlType { kWatermark, kCheckpointBarrier };

struct ControlRecord {
    ControlType type;
    Timestamp watermark;
    uint64_t checkpoint_offset;
};

} // namespace stormglass

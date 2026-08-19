#pragma once

#include "source/source.h"
#include "sink/sink.h"
#include "window/window.h"
#include "window/state.h"
#include "stream/watermark.h"

#include <cstdint>
#include <memory>

namespace stormglass {

struct PipelineConfig {
    Duration allowed_lateness{0};
    // future: checkpoint_interval, etc.
};

class Pipeline {
public:
    Pipeline(std::unique_ptr<Source> source,
             std::unique_ptr<WindowAssigner> assigner,
             std::unique_ptr<Sink> sink,
             PipelineConfig config = {});

    struct Stats {
        uint64_t records_processed = 0;
        uint64_t windows_fired = 0;
        uint64_t windows_refired = 0;
        uint64_t late_records_accepted = 0;
        uint64_t late_records_dropped = 0;
        uint64_t watermarks_advanced = 0;
    };

    Stats Run();

private:
    std::unique_ptr<Source> source_;
    std::unique_ptr<WindowAssigner> assigner_;
    std::unique_ptr<Sink> sink_;
    PipelineConfig config_;
    WatermarkTracker watermark_;
    KeyedWindowState state_;
};

} // namespace stormglass

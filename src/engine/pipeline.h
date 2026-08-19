#pragma once

#include "source/source.h"
#include "sink/sink.h"
#include "window/window.h"
#include "window/state.h"
#include "stream/watermark.h"

#include <cstdint>
#include <memory>

namespace stormglass {

class Pipeline {
public:
    Pipeline(std::unique_ptr<Source> source,
             std::unique_ptr<WindowAssigner> assigner,
             std::unique_ptr<Sink> sink);

    struct Stats {
        uint64_t records_processed = 0;
        uint64_t windows_fired = 0;
        uint64_t watermarks_advanced = 0;
    };

    Stats Run();

private:
    std::unique_ptr<Source> source_;
    std::unique_ptr<WindowAssigner> assigner_;
    std::unique_ptr<Sink> sink_;
    WatermarkTracker watermark_;
    KeyedWindowState state_;
};

} // namespace stormglass

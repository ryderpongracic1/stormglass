#pragma once

#include "source/source.h"
#include "sink/sink.h"
#include "window/window.h"
#include "window/state.h"
#include "stream/watermark.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"

#include <cstdint>
#include <memory>
#include <string>

namespace stormglass {

struct PipelineConfig {
    Duration allowed_lateness{0};

    // Checkpoint config
    std::string checkpoint_dir;         // empty = no checkpointing
    uint64_t checkpoint_interval = 0;   // records between checkpoints (0 = disabled)
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
        uint64_t checkpoints_written = 0;
        uint64_t records_replayed = 0;
    };

    Stats Run();

private:
    std::unique_ptr<Source> source_;
    std::unique_ptr<WindowAssigner> assigner_;
    std::unique_ptr<Sink> sink_;
    PipelineConfig config_;
    WatermarkTracker watermark_;
    KeyedWindowState state_;

    // Checkpoint support
    bool checkpointing_enabled() const;
    void TryRestore();
    void WriteCheckpoint(uint64_t offset, Stats& stats);

    uint64_t restored_offset_ = 0;
};

} // namespace stormglass

#pragma once

#include "engine/job_control.h"
#include "source/source.h"
#include "sink/sink.h"
#include "window/window.h"
#include "window/state.h"
#include "stream/watermark.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"

#include <cstdint>
#include <memory>
#include <optional>
#include <string>

namespace stormglass {

struct PipelineConfig {
    Duration allowed_lateness{0};

    // Checkpoint config
    // Empty = no checkpointing. Restore throws JobManifestMismatch if the
    // directory was written with a different lateness, assigner or source.
    std::string checkpoint_dir;
    // Non-zero enables checkpointing (together with checkpoint_dir). It does not
    // set the cadence: a checkpoint is written at every barrier the source emits,
    // so configure the interval on the source (e.g. GeneratorConfig).
    uint64_t checkpoint_interval = 0;
};

class Pipeline {
public:
    Pipeline(std::unique_ptr<Source> source,
             std::unique_ptr<WindowAssigner> assigner,
             std::unique_ptr<Sink> sink,
             PipelineConfig config = {});
    ~Pipeline();

    struct Stats {
        uint64_t records_processed = 0;
        uint64_t windows_fired = 0;
        uint64_t windows_refired = 0;
        uint64_t late_records_accepted = 0;
        uint64_t late_records_dropped = 0;
        uint64_t watermarks_advanced = 0;
        uint64_t checkpoints_written = 0;
        uint64_t records_replayed = 0;
        // Set when Run() returned because of RequestStop(StopMode::kSuspend).
        bool suspended = false;
        // Offset of the checkpoint taken on suspend (checkpointing enabled only).
        std::optional<uint64_t> suspend_checkpoint_offset;
    };

    // Runs until the source ends or a stop is requested. Throws
    // std::invalid_argument if checkpointing is enabled on a source whose
    // Replayable() is false.
    Stats Run();

    // Thread-safe. Ask a running (or not yet started) Run() to stop after the
    // batch it is processing, waking a blocked source via Source::Cancel(). The
    // first request's mode wins. See StopMode.
    void RequestStop(StopMode mode = StopMode::kSuspend);

    // Thread-safe snapshot of the job's counters, updated after every batch.
    [[nodiscard]] JobProgress Progress() const;

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

    struct Control;
    void PublishProgress(const Stats& stats);
    std::unique_ptr<Control> control_;
};

} // namespace stormglass

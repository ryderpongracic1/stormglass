#pragma once

#include "sink/sink.h"
#include "stream/record.h"
#include "stream/watermark.h"
#include "window/state.h"
#include "window/window.h"

#include <cstdint>
#include <memory>

namespace stormglass {

// The per-worker windowing core.
//
// This is a faithful, self-contained extraction of the record/watermark/flush
// orchestration in Pipeline::Run — the SAME sequence of KeyedWindowState and
// WatermarkTracker calls, in the same order, with the same L==0 vs L>0 branches.
// A PartitionedPipeline worker owns one KeyedProcessor and feeds it its subset
// of keys plus the globally-broadcast watermarks; because the firing decisions
// depend only on (this key's accumulated panes, the global watermark) they are
// identical to what the single-threaded engine would decide for those keys.
// That is what makes the union of per-worker output equal to the single-threaded
// output as a set — and lets the SAME differential oracle verify both.
//
// It is deliberately NOT shared with Pipeline (which is kept completely intact);
// the invariance differential test is the guard that the two stay in lockstep.
class KeyedProcessor {
public:
    struct Stats {
        uint64_t records_processed = 0;
        uint64_t windows_fired = 0;
        uint64_t windows_refired = 0;
        uint64_t late_records_accepted = 0;
        uint64_t late_records_dropped = 0;
        uint64_t watermarks_advanced = 0;
    };

    KeyedProcessor(std::unique_ptr<WindowAssigner> assigner,
                   Sink& sink,
                   Duration allowed_lateness);

    // Apply one data record (window assignment + accumulation).
    void ProcessRecord(const Record& record);

    // Apply one control record. Watermarks drive firing; checkpoint barriers
    // are a no-op in this phase (broadcast plumbing exists for Phase 3).
    void ProcessControl(const ControlRecord& control);

    // Fire every remaining window (mirrors Pipeline::Run's final-flush block).
    void FinalFlush();

    [[nodiscard]] const Stats& stats() const { return stats_; }
    [[nodiscard]] Timestamp watermark() const { return watermark_.Current(); }

private:
    std::unique_ptr<WindowAssigner> assigner_;
    Sink& sink_;
    Duration allowed_lateness_;
    bool use_lateness_;
    WatermarkTracker watermark_;
    KeyedWindowState state_;
    Stats stats_{};
};

} // namespace stormglass

#include "engine/pipeline.h"

#include "checkpoint/job_manifest.h"

#include <atomic>
#include <variant>
#include <stdexcept>

namespace stormglass {

struct Pipeline::Control {
    StopSignal stop;
    std::atomic<bool> running{false};
    std::atomic<uint64_t> records_processed{0};
    std::atomic<uint64_t> windows_fired{0};
    std::atomic<uint64_t> windows_refired{0};
    std::atomic<uint64_t> late_records_accepted{0};
    std::atomic<uint64_t> late_records_dropped{0};
    std::atomic<uint64_t> checkpoints_written{0};
    std::atomic<bool> has_checkpoint{false};
    std::atomic<uint64_t> last_checkpoint{0};
    std::atomic<int64_t> watermark_ms{Timestamp::min().time_since_epoch().count()};
};

// Overloaded helper for std::visit
template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
template<class... Ts> overloaded(Ts...) -> overloaded<Ts...>;

Pipeline::Pipeline(std::unique_ptr<Source> source,
                   std::unique_ptr<WindowAssigner> assigner,
                   std::unique_ptr<Sink> sink,
                   PipelineConfig config)
    : source_(std::move(source)),
      assigner_(std::move(assigner)),
      sink_(std::move(sink)),
      config_(config),
      control_(std::make_unique<Control>()) {
    if (config_.allowed_lateness.count() < 0) throw std::invalid_argument("lateness must be nonnegative");
    if (config_.allowed_lateness.count() > 0) {
        state_.SetAllowedLateness(config_.allowed_lateness);
    }
}

Pipeline::~Pipeline() = default;

void Pipeline::RequestStop(StopMode mode) {
    control_->stop.Request(mode);
    source_->Cancel();
}

JobProgress Pipeline::Progress() const {
    JobProgress p;
    p.running = control_->running.load();
    p.records_processed = control_->records_processed.load();
    p.records_read = p.records_processed;
    p.windows_fired = control_->windows_fired.load();
    p.windows_refired = control_->windows_refired.load();
    p.late_records_accepted = control_->late_records_accepted.load();
    p.late_records_dropped = control_->late_records_dropped.load();
    p.checkpoints_written = control_->checkpoints_written.load();
    if (control_->has_checkpoint.load()) p.last_complete_checkpoint = control_->last_checkpoint.load();
    p.output_watermark = Timestamp{Duration{control_->watermark_ms.load()}};
    return p;
}

void Pipeline::PublishProgress(const Stats& stats) {
    control_->records_processed.store(stats.records_processed, std::memory_order_relaxed);
    control_->windows_fired.store(stats.windows_fired, std::memory_order_relaxed);
    control_->windows_refired.store(stats.windows_refired, std::memory_order_relaxed);
    control_->late_records_accepted.store(stats.late_records_accepted, std::memory_order_relaxed);
    control_->late_records_dropped.store(stats.late_records_dropped, std::memory_order_relaxed);
    control_->checkpoints_written.store(stats.checkpoints_written, std::memory_order_relaxed);
    control_->watermark_ms.store(watermark_.Current().time_since_epoch().count(),
                                 std::memory_order_relaxed);
}

bool Pipeline::checkpointing_enabled() const {
    return !config_.checkpoint_dir.empty() && config_.checkpoint_interval > 0;
}

void Pipeline::TryRestore() {
    ValidateOrCreateJobManifest(config_.checkpoint_dir, JobManifest{
        {"engine", "pipeline"},
        {"allowed_lateness_ms", std::to_string(config_.allowed_lateness.count())},
        {"assigner", assigner_->Descriptor()},
        {"source", source_->Descriptor()},
    });
    CheckpointReader reader(config_.checkpoint_dir);
    auto data = reader.LoadLatest();
    if (!data.has_value()) return;

    // Restore pane state
    for (const auto& entry : data->panes) {
        state_.RestorePane(entry.key, entry.window, entry.sum, entry.count);
    }

    // Restore fired windows (v2+ checkpoints)
    if (!data->fired_windows.empty()) {
        state_.RestoreFiredWindows(data->fired_windows);
    }

    // Restore watermark
    state_.RestoreRefiredWindows(data->refired_windows);
    watermark_.Advance(data->watermark);

    // Seek source past the checkpointed offset
    source_->Seek(data->offset);
    restored_offset_ = data->offset;
    control_->last_checkpoint.store(data->offset);
    control_->has_checkpoint.store(true);
}

void Pipeline::WriteCheckpoint(uint64_t offset, Stats& stats) {
    sink_->Flush();
    CheckpointWriter writer(config_.checkpoint_dir);
    if (writer.WriteCheckpoint(offset, watermark_.Current(), state_)) {
        stats.checkpoints_written++;
        control_->last_checkpoint.store(offset);
        control_->has_checkpoint.store(true);
    } else {
        throw std::runtime_error("checkpoint write failed");
    }
}

Pipeline::Stats Pipeline::Run() {
    Stats stats{};
    bool use_lateness = config_.allowed_lateness.count() > 0;

    if (checkpointing_enabled() && !source_->Replayable()) {
        throw std::invalid_argument("checkpointing requires a replayable source");
    }

    // Attempt restore before processing
    if (checkpointing_enabled()) {
        TryRestore();
        if (restored_offset_ > 0) {
            stats.records_replayed = restored_offset_;
        }
    }

    struct RunningFlag {
        std::atomic<bool>& running;
        explicit RunningFlag(std::atomic<bool>& r) : running(r) { running.store(true); }
        ~RunningFlag() { running.store(false); }
    } running_flag{control_->running};

    IdleBackoff backoff;
    bool suspend = false;
    while (true) {
        if (control_->stop.requested()) {
            suspend = control_->stop.mode() == StopMode::kSuspend;
            break;
        }
        auto batch = source_->Next();
        if (!batch) {
            // A cancelled source may end early; honor the stop's mode.
            if (control_->stop.requested()) suspend = control_->stop.mode() == StopMode::kSuspend;
            break;
        }
        if (batch->empty()) {
            backoff.Wait();  // live source with no data yet
            continue;
        }
        backoff.Reset();
        for (auto& item : batch->items) {
            std::visit(overloaded{
                [&](const Record& r) {
                    auto windows = assigner_->AssignWindows(r.event_time);
                    bool any_dropped = false;
                    bool any_late_accepted = false;
                    for (auto& w : windows) {
                        if (use_lateness) {
                            bool was_fired = state_.IsFired(w);
                            bool accepted = state_.AddWithLateness(
                                r.key, w, r.value, watermark_.Current());
                            if (!accepted) {
                                any_dropped = true;
                            } else if (was_fired) {
                                any_late_accepted = true;
                            }
                        } else {
                            // L == 0: deadline is window.end + 0. Without this, a
                            // record arriving after its window fired (and its panes
                            // were erased) would create a fresh pane that re-fires a
                            // spurious partial result at final flush.
                            bool accepted = state_.AddWithLateness(
                                r.key, w, r.value, watermark_.Current());
                            if (!accepted) any_dropped = true;
                        }
                    }
                    if (any_dropped) stats.late_records_dropped++;
                    if (any_late_accepted) stats.late_records_accepted++;
                    stats.records_processed++;
                },
                [&](const ControlRecord& c) {
                    if (c.type == ControlType::kWatermark) {
                        if (watermark_.Advance(c.watermark)) {
                            // Fire expired windows (first-time fire)
                            for (auto& w : state_.ExpiredWindows(watermark_.Current())) {
                                for (auto& result : state_.FireWindow(w)) {
                                    sink_->Emit(result);
                                }
                                stats.windows_fired++;
                            }

                            // Re-fire windows that received late data
                            if (use_lateness) {
                                for (auto& w : state_.RefiredWindows()) {
                                    for (auto& result : state_.FireWindow(w)) {
                                        sink_->Emit(result);
                                    }
                                    stats.windows_refired++;
                                }
                                state_.ClearRefired();

                                // GC windows past allowed lateness
                                auto gc_windows = state_.GarbageCollectableWindows(watermark_.Current());
                                state_.GarbageCollect(gc_windows);
                            }

                            stats.watermarks_advanced++;
                        }
                    } else if (c.type == ControlType::kCheckpointBarrier) {
                        // Records are processed in order, so everything at or
                        // below the barrier's absolute offset has been applied
                        // and nothing after it has. Snapshot (state + that
                        // absolute offset + current watermark) synchronously —
                        // the degenerate single-path Chandy-Lamport barrier.
                        if (checkpointing_enabled()) {
                            WriteCheckpoint(c.checkpoint_offset, stats);
                        }
                    }
                }
            }, item);
        }
        PublishProgress(stats);
    }

    if (suspend) {
        // Leave open windows unfired. Checkpoint at the batch boundary just
        // processed so a restart resumes here and fires them with full values.
        stats.suspended = true;
        if (checkpointing_enabled()) {
            const uint64_t offset = source_->CurrentOffset();
            WriteCheckpoint(offset, stats);
            stats.suspend_checkpoint_offset = offset;
        }
        sink_->Flush();
        PublishProgress(stats);
        return stats;
    }

    // Final flush: fire all remaining windows
    if (use_lateness) {
        for (auto& w : state_.RefiredWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_->Emit(result);
            }
            stats.windows_refired++;
        }
        state_.ClearRefired();
        for (auto& w : state_.AllWindows()) {
            if (!state_.IsFired(w)) {
                for (auto& result : state_.FireWindow(w)) {
                    sink_->Emit(result);
                }
                stats.windows_fired++;
            }
        }
        auto remaining = state_.AllWindows();
        state_.GarbageCollect(remaining);
    } else {
        for (auto& w : state_.AllWindows()) {
            for (auto& result : state_.FireWindow(w)) {
                sink_->Emit(result);
            }
            stats.windows_fired++;
        }
    }
    sink_->Flush();
    PublishProgress(stats);

    return stats;
}

} // namespace stormglass

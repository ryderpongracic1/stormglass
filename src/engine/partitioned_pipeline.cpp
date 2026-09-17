#include "engine/partitioned_pipeline.h"

#include "engine/bounded_queue.h"
#include "engine/keyed_processor.h"
#include "engine/partition_hash.h"
#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/job_manifest.h"
#include "checkpoint/reader.h"

#include <algorithm>
#include <atomic>
#include <optional>
#include <exception>
#include <mutex>
#include <stdexcept>
#include <chrono>
#include <filesystem>
#include <string>
#include <thread>
#include <variant>
#include <vector>

namespace stormglass {
namespace {

// Sentinel pushed once per worker queue after the source is exhausted: tells the
// worker to final-flush and exit. Carrying it in the queue variant (rather than
// a side flag) keeps the drain strictly in-band and ordered after every data
// and control message that preceded it.
struct EndOfStream {};

// Sentinel for RequestStop(kSuspend): flush output and exit WITHOUT firing open
// windows. Ordered after the suspend checkpoint barrier, when there is one.
struct Suspend {};

using WorkerMessage = std::variant<Record, ControlRecord, EndOfStream, Suspend>;

// A queue slot carries a BATCH of messages — everything the Router routes to
// one worker from a single source batch — rather than a single message. This
// amortizes the per-slot mutex acquire + notify_one (the per-record hand-off
// was the dominant cost on compute-light workloads) over a whole batch, while
// preserving per-worker message order exactly: messages are appended in source
// order within a batch and batches are enqueued strictly FIFO.
using WorkerBatch = std::vector<WorkerMessage>;

// Worker-local front for the caller's shared sink. Results are buffered only
// until the worker finishes its current input batch (Drain) or reaches a
// barrier / final flush (Flush), then handed to the caller's sink under one
// mutex, so the caller's sink sees serialized calls and never holds more than a
// batch of undelivered output.
//
// Flush ordering is the recovery invariant: KeyedProcessor flushes its sink
// before writing a checkpoint, so every result whose window the checkpoint
// records as fired has already reached (and been flushed by) the caller's sink.
// A failure after that checkpoint can therefore lose no output; replay from it
// can only repeat results emitted later (at-least-once).
class ForwardingSink : public Sink {
public:
    ForwardingSink(Sink& downstream, std::mutex& mu) : downstream_(downstream), mu_(mu) {}

    void Emit(const WindowResult& result) override { pending_.push_back(result); }

    void Flush() override {
        std::lock_guard lock(mu_);
        DeliverLocked();
        downstream_.Flush();
    }

    void Drain() {
        if (pending_.empty()) return;
        std::lock_guard lock(mu_);
        DeliverLocked();
    }

private:
    void DeliverLocked() {
        for (const auto& result : pending_) downstream_.Emit(result);
        pending_.clear();
    }

    Sink& downstream_;
    std::mutex& mu_;
    std::vector<WindowResult> pending_;
};

// One shared-nothing worker: owns a bounded input queue, a local sink, and a
// KeyedProcessor over its subset of keys.
struct Worker {
    explicit Worker(std::size_t queue_capacity) : queue(queue_capacity) {}

    BoundedQueue<WorkerBatch> queue;
    // Worker sink: either a ForwardingSink into the caller's sink (forward
    // non-null) or a caller-supplied per-worker sink (forward null,
    // self-persisting). Held by unique_ptr so both cases share one path.
    std::unique_ptr<Sink> sink;
    ForwardingSink* forward = nullptr;
    std::unique_ptr<KeyedProcessor> processor;
    std::thread thread;
    std::string ckpt_dir;  // per-partition checkpoint dir, or empty
};

} // namespace

struct PartitionedPipeline::Control {
    // Written only by the owning worker thread; read by Progress().
    struct WorkerCounters {
        std::atomic<uint64_t> records_processed{0};
        std::atomic<uint64_t> windows_fired{0};
        std::atomic<uint64_t> windows_refired{0};
        std::atomic<uint64_t> late_records_accepted{0};
        std::atomic<uint64_t> late_records_dropped{0};
        std::atomic<uint64_t> checkpoints_written{0};
        std::atomic<int64_t> watermark_ms{Timestamp::min().time_since_epoch().count()};

        void Reset() {
            records_processed.store(0);
            windows_fired.store(0);
            windows_refired.store(0);
            late_records_accepted.store(0);
            late_records_dropped.store(0);
            checkpoints_written.store(0);
            watermark_ms.store(Timestamp::min().time_since_epoch().count());
        }
    };

    explicit Control(uint32_t workers)
        : num_workers(workers), counters(new WorkerCounters[workers]) {}

    void Publish(uint32_t worker, const KeyedProcessor& processor) {
        const auto& s = processor.stats();
        auto& c = counters[worker];
        constexpr auto relaxed = std::memory_order_relaxed;
        c.records_processed.store(s.records_processed, relaxed);
        c.windows_fired.store(s.windows_fired, relaxed);
        c.windows_refired.store(s.windows_refired, relaxed);
        c.late_records_accepted.store(s.late_records_accepted, relaxed);
        c.late_records_dropped.store(s.late_records_dropped, relaxed);
        c.checkpoints_written.store(s.checkpoints_written, relaxed);
        c.watermark_ms.store(processor.watermark().time_since_epoch().count(), relaxed);
    }

    const uint32_t num_workers;
    std::unique_ptr<WorkerCounters[]> counters;
    StopSignal stop;
    std::atomic<bool> running{false};
    std::atomic<uint64_t> records_read{0};
    std::atomic<bool> has_complete_checkpoint{false};
    std::atomic<uint64_t> last_complete_checkpoint{0};
};

PartitionedPipeline::PartitionedPipeline(
    std::unique_ptr<Source> source,
    std::function<std::unique_ptr<WindowAssigner>()> assigner_factory,
    std::unique_ptr<Sink> sink,
    PartitionedPipelineConfig config)
    : source_(std::move(source)),
      assigner_factory_(std::move(assigner_factory)),
      sink_(std::move(sink)),
      config_(config),
      control_(std::make_unique<Control>(std::max<uint32_t>(1, config_.num_workers))) {}

PartitionedPipeline::~PartitionedPipeline() = default;

void PartitionedPipeline::RequestStop(StopMode mode) {
    control_->stop.Request(mode);
    source_->Cancel();
}

JobProgress PartitionedPipeline::Progress() const {
    JobProgress p;
    p.running = control_->running.load();
    p.records_read = control_->records_read.load();
    Timestamp min_wm = Timestamp::max();
    for (uint32_t i = 0; i < control_->num_workers; ++i) {
        const auto& c = control_->counters[i];
        p.records_processed += c.records_processed.load();
        p.windows_fired += c.windows_fired.load();
        p.windows_refired += c.windows_refired.load();
        p.late_records_accepted += c.late_records_accepted.load();
        p.late_records_dropped += c.late_records_dropped.load();
        p.checkpoints_written += c.checkpoints_written.load();
        min_wm = std::min(min_wm, Timestamp{Duration{c.watermark_ms.load()}});
    }
    p.output_watermark = min_wm;
    if (control_->has_complete_checkpoint.load()) {
        p.last_complete_checkpoint = control_->last_complete_checkpoint.load();
    }
    return p;
}

PartitionedPipeline::Stats PartitionedPipeline::Run() {
    const uint32_t n = control_->num_workers;
    const bool checkpointing = !config_.checkpoint_dir.empty();

    if (checkpointing && !source_->Replayable()) {
        throw std::invalid_argument("checkpointing requires a replayable source");
    }

    // Reject a checkpoint directory from a different job shape before touching
    // any state: partition files are only meaningful to the same worker count,
    // window geometry, lateness, and replayed source.
    if (checkpointing) {
        ValidateOrCreateJobManifest(config_.checkpoint_dir, JobManifest{
            {"engine", "partitioned"},
            {"num_workers", std::to_string(n)},
            {"allowed_lateness_ms", std::to_string(config_.allowed_lateness.count())},
            {"assigner", assigner_factory_()->Descriptor()},
            {"source", source_->Descriptor()},
        });
    }

    // Per-partition checkpoint directories must exist before any worker writes.
    // create_directories is idempotent, so both the initial run and a restart
    // (restore) run land on the same layout.
    if (checkpointing) {
        for (uint32_t i = 0; i < n; ++i) {
            std::filesystem::create_directories(
                PartitionCheckpointDir(config_.checkpoint_dir, i));
        }
    }

    // --- Build workers (each owns its state, assigner, and local sink) ---
    std::mutex sink_mutex;
    std::vector<std::unique_ptr<Worker>> workers;
    workers.reserve(n);
    for (uint32_t i = 0; i < n; ++i) {
        auto w = std::make_unique<Worker>(config_.queue_capacity);
        if (checkpointing) {
            w->ckpt_dir = PartitionCheckpointDir(config_.checkpoint_dir, i);
        }
        if (config_.worker_sink_factory) {
            w->sink = config_.worker_sink_factory(i);
        } else {
            auto forward = std::make_unique<ForwardingSink>(*sink_, sink_mutex);
            w->forward = forward.get();
            w->sink = std::move(forward);
        }
        w->processor = std::make_unique<KeyedProcessor>(
            assigner_factory_(), *w->sink, config_.allowed_lateness, w->ckpt_dir);
        workers.push_back(std::move(w));
    }

    // --- Restore from the highest COMPLETE global checkpoint, if any ---
    // Each partition loads its own file for that exact offset (NOT its local
    // latest, which may be a torn higher offset). Then the shared source is
    // sought past the offset, so deterministic FNV-1a routing replays the
    // remaining records to the same workers.
    Stats stats{};
    std::optional<uint64_t> restored;
    if (checkpointing) {
        const auto scan_t0 = std::chrono::steady_clock::now();
        if (auto complete = HighestCompleteCheckpoint(config_.checkpoint_dir, n)) {
            for (uint32_t i = 0; i < n; ++i) {
                CheckpointReader reader(workers[i]->ckpt_dir);
                if (auto data = reader.LoadOffset(*complete)) {
                    workers[i]->processor->Restore(*data);
                } else {
                    throw std::runtime_error("complete checkpoint disappeared during restore");
                }
            }
            const auto seek_t0 = std::chrono::steady_clock::now();
            source_->Seek(*complete);
            const auto seek_t1 = std::chrono::steady_clock::now();
            stats.records_replayed = *complete;
            stats.restore_state_micros = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    seek_t0 - scan_t0).count());
            stats.restore_seek_micros = static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(
                    seek_t1 - seek_t0).count());
            restored = *complete;
        }
    }

    // Progress is reset per run and reports running until Run() returns.
    struct RunningFlag {
        Control& control;
        explicit RunningFlag(Control& c) : control(c) {
            control.running.store(true);
            control.records_read.store(0);
            control.has_complete_checkpoint.store(false);
            for (uint32_t i = 0; i < control.num_workers; ++i) control.counters[i].Reset();
        }
        ~RunningFlag() { control.running.store(false); }
    } running_flag{*control_};
    // Completion tracking and coordinated retention across partitions.
    std::optional<PartitionedCheckpointTracker> tracker;
    if (checkpointing) {
        tracker.emplace(config_.checkpoint_dir, n, config_.checkpoint_retention, restored);
        if (restored) {
            control_->last_complete_checkpoint.store(*restored);
            control_->has_complete_checkpoint.store(true);
        }
        for (uint32_t i = 0; i < n; ++i) {
            workers[i]->processor->SetCheckpointListener([this, &tracker, i](uint64_t offset) {
                tracker->OnPartitionCheckpoint(i, offset);
                if (auto complete = tracker->LastComplete()) {
                    control_->last_complete_checkpoint.store(*complete);
                    control_->has_complete_checkpoint.store(true);
                }
            });
        }
    }

    std::atomic<bool> cancelled{false};
    std::mutex error_mutex;
    std::exception_ptr error;
    auto fail = [&] {
        {
            std::lock_guard lock(error_mutex);
            if (!error) error = std::current_exception();
        }
        cancelled.store(true);
        for (auto& w : workers) w->queue.Close();
    };
    // Joining is also required when creating a thread itself fails.
    struct JoinWorkers {
        std::vector<std::unique_ptr<Worker>>& workers;
        std::atomic<bool>& cancelled;
        ~JoinWorkers() {
            cancelled.store(true);
            for (auto& w : workers) w->queue.Close();
            for (auto& w : workers) if (w->thread.joinable()) w->thread.join();
        }
    } join_workers{workers, cancelled};

    // --- Worker loop: drain queue until EndOfStream (final-flush) or Suspend ---
    for (uint32_t i = 0; i < n; ++i) {
        Worker* w = workers[i].get();
        w->thread = std::thread([&, w, i] {
          try {
            bool done = false;
            bool suspend = false;
            while (!done && !cancelled.load()) {
                auto batch = w->queue.Pop();
                if (!batch.has_value()) break;  // queue closed and drained
                // Process messages in the EXACT order the Router appended them
                // (source order for this worker) — batching changes granularity,
                // never ordering.
                for (auto& msg : *batch) {
                    bool stop = false;
                    std::visit([&](auto&& m) {
                        using T = std::decay_t<decltype(m)>;
                        if constexpr (std::is_same_v<T, Record>) {
                            w->processor->ProcessRecord(m);
                        } else if constexpr (std::is_same_v<T, ControlRecord>) {
                            w->processor->ProcessControl(m);
                        } else if constexpr (std::is_same_v<T, Suspend>) {
                            suspend = true;
                            stop = true;
                        } else {  // EndOfStream
                            stop = true;
                        }
                    }, msg);
                    if (stop) { done = true; break; }
                }
                if (w->forward) w->forward->Drain();
                control_->Publish(i, *w->processor);
            }
            if (!cancelled.load()) {
                // Suspend leaves open windows in (checkpointed) state; only output
                // already produced is flushed.
                if (suspend) {
                    w->sink->Flush();
                } else {
                    w->processor->FinalFlush();
                }
                control_->Publish(i, *w->processor);
            }
          } catch (...) { fail(); }
        });
    }

    // --- Router: hash-route data, broadcast control, then send sentinels ---
    // Runs on its own thread to match the Source -> Router -> Workers topology;
    // the calling thread becomes the Merge stage after join.
    bool suspended = false;
    std::optional<uint64_t> suspend_offset;
    std::thread router([&] {
      try {
        // One pending vector per worker; ownership transfers to the queue.
        std::vector<WorkerBatch> pending(n);
        IdleBackoff backoff;
        while (!cancelled.load()) {
            if (control_->stop.requested()) {
                suspended = control_->stop.mode() == StopMode::kSuspend;
                break;
            }
            auto batch = source_->Next();
            if (!batch) {
                // A cancelled source may end early; honor the stop's mode.
                if (control_->stop.requested()) {
                    suspended = control_->stop.mode() == StopMode::kSuspend;
                }
                break;
            }
            if (batch->empty()) {
                backoff.Wait();  // live source with no data yet
                continue;
            }
            backoff.Reset();
            uint64_t data_records = 0;
            for (auto& p : pending) p.clear();
            // Accumulate this source batch into per-worker vectors in source
            // order. DATA goes to exactly one worker; CONTROL is appended to
            // EVERY worker. Because we append in item order, each worker's
            // vector is the exact record/control interleaving the old
            // one-Push-per-message path produced for that worker.
            for (auto& item : batch->items) {
                std::visit([&](auto&& v) {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, Record>) {
                        // DATA: exactly one worker, chosen by portable key hash.
                        uint32_t p = PartitionForKey(v.key, n);
                        pending[p].push_back(WorkerMessage{std::move(v)});
                        ++data_records;
                    } else if constexpr (std::is_same_v<T, ControlRecord>) {
                        // CONTROL: broadcast to ALL workers so every worker
                        // fires against the identical GLOBAL watermark.
                        for (uint32_t p = 0; p < n; ++p) {
                            pending[p].push_back(WorkerMessage{v});
                        }
                    }
                }, item);
            }
            // Flush once per source batch: one Push per worker, not per record.
            // Skip empty vectors so we never enqueue a no-op batch.
            for (uint32_t p = 0; p < n; ++p) {
                if (pending[p].empty()) continue;
                bool pushed = workers[p]->queue.Push(std::move(pending[p]));
                if (!pushed) return;  // cancellation closed all queues
            }
            control_->records_read.fetch_add(data_records, std::memory_order_relaxed);
        }
        // Source exhausted or stopped: in-band sentinel (its own final batch) to
        // each worker AFTER the last data flush, then close. A suspend first
        // broadcasts a checkpoint barrier at the offset of everything routed, so
        // every partition snapshots the same cut and a restart resumes there.
        if (suspended && checkpointing) {
            suspend_offset = source_->CurrentOffset();
            const ControlRecord barrier{
                .type = ControlType::kCheckpointBarrier,
                .watermark = Timestamp::min(),
                .checkpoint_offset = *suspend_offset,
            };
            for (uint32_t p = 0; p < n; ++p) {
                if (!workers[p]->queue.Push(WorkerBatch{WorkerMessage{barrier}})) return;
            }
        }
        for (uint32_t p = 0; p < n; ++p) {
            WorkerMessage sentinel = suspended ? WorkerMessage{Suspend{}}
                                               : WorkerMessage{EndOfStream{}};
            bool pushed = workers[p]->queue.Push(WorkerBatch{std::move(sentinel)});
            if (!pushed) return;
            workers[p]->queue.Close();
        }
      } catch (...) { fail(); }
    });

    router.join();
    for (uint32_t i = 0; i < n; ++i) {
        workers[i]->thread.join();
    }

    if (error) std::rethrow_exception(error);

    // --- Merge: aggregate stats (output was already forwarded while running) ---
    stats.num_workers = n;
    stats.suspended = suspended;
    stats.suspend_checkpoint_offset = suspend_offset;
    Timestamp min_wm = Timestamp::max();
    for (uint32_t i = 0; i < n; ++i) {
        const auto& ws = workers[i]->processor->stats();
        // Disjoint-key quantities sum to the single-threaded totals.
        stats.records_processed += ws.records_processed;
        stats.windows_fired += ws.windows_fired;
        stats.windows_refired += ws.windows_refired;
        stats.late_records_accepted += ws.late_records_accepted;
        stats.late_records_dropped += ws.late_records_dropped;
        stats.checkpoints_written += ws.checkpoints_written;
        // watermarks_advanced is a GLOBAL count (identical across workers on a
        // single broadcast source); report the max rather than a meaningless sum.
        stats.watermarks_advanced = std::max(stats.watermarks_advanced, ws.watermarks_advanced);
        // Effective output watermark = min across partitions.
        min_wm = std::min(min_wm, workers[i]->processor->watermark());
    }
    stats.output_watermark = min_wm;
    sink_->Flush();

    return stats;
}

} // namespace stormglass

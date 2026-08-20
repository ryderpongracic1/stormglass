#include "engine/partitioned_pipeline.h"

#include "engine/bounded_queue.h"
#include "engine/keyed_processor.h"
#include "engine/partition_hash.h"
#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/reader.h"
#include "sink/memory_sink.h"

#include <algorithm>
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

using WorkerMessage = std::variant<Record, ControlRecord, EndOfStream>;

// One shared-nothing worker: owns a bounded input queue, a local sink, and a
// KeyedProcessor over its subset of keys.
struct Worker {
    explicit Worker(std::size_t queue_capacity) : queue(queue_capacity) {}

    BoundedQueue<WorkerMessage> queue;
    // Worker sink: either an internal MemorySink (mem_sink non-null, merged into
    // the caller's sink at join) or a caller-supplied durable sink (mem_sink
    // null, self-persisting). Held by unique_ptr so both cases share one path.
    std::unique_ptr<Sink> sink;
    MemorySink* mem_sink = nullptr;
    std::unique_ptr<KeyedProcessor> processor;
    std::thread thread;
    std::string ckpt_dir;  // per-partition checkpoint dir, or empty
};

} // namespace

PartitionedPipeline::PartitionedPipeline(
    std::unique_ptr<Source> source,
    std::function<std::unique_ptr<WindowAssigner>()> assigner_factory,
    std::unique_ptr<Sink> sink,
    PartitionedPipelineConfig config)
    : source_(std::move(source)),
      assigner_factory_(std::move(assigner_factory)),
      sink_(std::move(sink)),
      config_(config) {}

PartitionedPipeline::Stats PartitionedPipeline::Run() {
    const uint32_t n = std::max<uint32_t>(1, config_.num_workers);
    const bool checkpointing = !config_.checkpoint_dir.empty();

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
            auto mem = std::make_unique<MemorySink>();
            w->mem_sink = mem.get();
            w->sink = std::move(mem);
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
    if (checkpointing) {
        if (auto complete = HighestCompleteCheckpoint(config_.checkpoint_dir, n)) {
            for (uint32_t i = 0; i < n; ++i) {
                CheckpointReader reader(workers[i]->ckpt_dir);
                if (auto data = reader.LoadOffset(*complete)) {
                    workers[i]->processor->Restore(*data);
                }
            }
            source_->Seek(*complete);
            stats.records_replayed = *complete;
        }
    }

    // --- Worker loop: drain queue until EndOfStream, then final-flush ---
    for (uint32_t i = 0; i < n; ++i) {
        Worker* w = workers[i].get();
        w->thread = std::thread([w] {
            for (;;) {
                auto msg = w->queue.Pop();
                if (!msg.has_value()) break;  // queue closed and drained
                bool done = false;
                std::visit([&](auto&& m) {
                    using T = std::decay_t<decltype(m)>;
                    if constexpr (std::is_same_v<T, Record>) {
                        w->processor->ProcessRecord(m);
                    } else if constexpr (std::is_same_v<T, ControlRecord>) {
                        w->processor->ProcessControl(m);
                    } else {  // EndOfStream
                        done = true;
                    }
                }, *msg);
                if (done) break;
            }
            w->processor->FinalFlush();
        });
    }

    // --- Router: hash-route data, broadcast control, then send sentinels ---
    // Runs on its own thread to match the Source -> Router -> Workers topology;
    // the calling thread becomes the Merge stage after join.
    std::thread router([&] {
        while (auto batch = source_->Next()) {
            for (auto& item : batch->items) {
                std::visit([&](auto&& v) {
                    using T = std::decay_t<decltype(v)>;
                    if constexpr (std::is_same_v<T, Record>) {
                        // DATA: exactly one worker, chosen by portable key hash.
                        uint32_t p = PartitionForKey(v.key, n);
                        workers[p]->queue.Push(WorkerMessage{v});
                    } else if constexpr (std::is_same_v<T, ControlRecord>) {
                        // CONTROL: broadcast to ALL workers so every worker
                        // fires against the identical GLOBAL watermark.
                        for (uint32_t p = 0; p < n; ++p) {
                            workers[p]->queue.Push(WorkerMessage{v});
                        }
                    }
                }, item);
            }
        }
        // Source exhausted: in-band end sentinel to each worker, then close.
        for (uint32_t p = 0; p < n; ++p) {
            workers[p]->queue.Push(WorkerMessage{EndOfStream{}});
            workers[p]->queue.Close();
        }
    });

    router.join();
    for (uint32_t i = 0; i < n; ++i) {
        workers[i]->thread.join();
    }

    // --- Merge: union worker outputs into the caller's sink + aggregate stats ---
    stats.num_workers = n;
    Timestamp min_wm = Timestamp::max();
    for (uint32_t i = 0; i < n; ++i) {
        // Internal MemorySink workers merge into the caller's sink. Workers with
        // a caller-supplied durable sink have already persisted independently, so
        // there is nothing to merge (mem_sink is null).
        if (workers[i]->mem_sink) {
            for (const auto& result : workers[i]->mem_sink->Results()) {
                sink_->Emit(result);
            }
        }
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

#pragma once

#include "engine/keyed_processor.h"
#include "sink/sink.h"
#include "source/source.h"
#include "stream/record.h"
#include "window/window.h"

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

namespace stormglass {

struct PartitionedPipelineConfig {
    uint32_t num_workers = 4;         // N worker threads
    Duration allowed_lateness{0};
    std::size_t queue_capacity = 8192; // per-worker bounded queue depth

    // Distributed-checkpoint root. Empty (default) = no checkpointing, in which
    // case every field below is inert and the engine behaves exactly as in
    // Phase 0-2. When set, each worker k snapshots on every broadcast barrier
    // into <checkpoint_dir>/p<k> (see distributed_checkpoint.h), and Run() first
    // restores from the highest COMPLETE global checkpoint and seeks the source
    // past it.
    std::string checkpoint_dir;

    // Optional per-worker sink factory. When set, worker k emits directly to
    // worker_sink_factory(k) instead of an internal MemorySink merged into the
    // constructor's sink at join. Used by the partitioned real-kill nemesis so
    // each worker's pre-crash output is independently durable. Null (default)
    // keeps the original merge-into-caller-sink path byte-for-byte.
    std::function<std::unique_ptr<Sink>(uint32_t)> worker_sink_factory = nullptr;
};

// Multi-worker keyed-parallel windowing engine.
//
// Topology:   Source -> Router(1 thread) -> N Workers -> Merge (this thread)
//
//   * The Router pulls source batches, hash-routes each DATA record to exactly
//     one worker by FNV-1a(key) % N, and BROADCASTS every CONTROL record
//     (watermark now; checkpoint barrier later) to ALL workers.
//   * Each Worker is shared-nothing: it owns its KeyedProcessor (its own
//     KeyedWindowState, WatermarkTracker, assigner) and its local sink. It
//     fires/drops its disjoint keys against the broadcast GLOBAL watermark, so
//     its decisions match the single-threaded engine's for those keys.
//   * The Merge stage (the thread calling Run) joins all workers, unions their
//     outputs into the caller's sink, and reports the effective output
//     watermark = min across per-partition watermarks.
//
// The only shared memory is the per-worker BoundedQueue and, after join, the
// worker result buffers. Output ORDER across workers is nondeterministic;
// compare results as a SET (sort by (window.start, key)).
class PartitionedPipeline {
public:
    // assigner_factory is called once per worker so each worker owns its own
    // assigner instance (the single-threaded Pipeline takes one unique_ptr; N
    // shared-nothing workers need N). Assigners are stateless, but giving each
    // worker its own keeps the shared-nothing invariant literal.
    PartitionedPipeline(std::unique_ptr<Source> source,
                        std::function<std::unique_ptr<WindowAssigner>()> assigner_factory,
                        std::unique_ptr<Sink> sink,
                        PartitionedPipelineConfig config);

    struct Stats {
        // Partitioned quantities — summed across workers (disjoint keys), so
        // these equal the single-threaded totals.
        uint64_t records_processed = 0;
        uint64_t windows_fired = 0;
        uint64_t windows_refired = 0;
        uint64_t late_records_accepted = 0;
        uint64_t late_records_dropped = 0;
        // Global quantity — every worker sees the same broadcast watermark
        // sequence, so this is a single global count (not a sum).
        uint64_t watermarks_advanced = 0;
        // Effective output watermark = min across partitions. With one broadcast
        // source all partitions are equal, so the min is trivially that value;
        // the code takes a real min so multi-source needs no rewrite.
        Timestamp output_watermark{Timestamp::min()};
        uint32_t num_workers = 0;

        // Checkpoint quantities (0 when checkpoint_dir is empty).
        uint64_t checkpoints_written = 0;  // summed across workers (per-partition files)
        uint64_t records_replayed = 0;     // offset of the complete checkpoint restored from
        // Restore-cost breakdown (additive bench hook; semantics-neutral). Both
        // measure the RESTORE step only — before the worker drain begins — and
        // are 0 when checkpoint_dir is empty or no complete checkpoint exists.
        //   * restore_state_micros: coordinator scan (HighestCompleteCheckpoint)
        //     + per-partition LoadOffset + KeyedProcessor::Restore. This is the
        //     "load the snapshot" cost — the number a replayable-log deployment
        //     would pay.
        //   * restore_seek_micros: source_->Seek(offset). For the in-memory
        //     DeterministicGenerator this is an O(offset) replay (a documented
        //     toy-source limitation); a real log source seeks in ~O(1).
        uint64_t restore_state_micros = 0;
        uint64_t restore_seek_micros = 0;
    };

    Stats Run();

private:
    std::unique_ptr<Source> source_;
    std::function<std::unique_ptr<WindowAssigner>()> assigner_factory_;
    std::unique_ptr<Sink> sink_;
    PartitionedPipelineConfig config_;
};

} // namespace stormglass

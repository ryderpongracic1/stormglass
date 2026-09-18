#pragma once

#include <cstdint>
#include <deque>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace stormglass {

// Coordinator for the partitioned engine's distributed checkpoint.
//
// This is a coordinator-COLLECTED distributed snapshot, NOT per-operator
// multi-input alignment: every worker in PartitionedPipeline is single-input
// (one queue), so its per-worker snapshot is trivially aligned — everything at
// or below the barrier's absolute offset O has been applied and nothing after
// it has. There is no need for the multi-channel marker buffering of full
// Chandy-Lamport. The only global concern is all-or-nothing completeness across
// the N independent per-partition snapshots, which is exactly what this file
// reasons about.
//
// Layout: each worker k writes into a per-partition subdirectory
//   <root>/p<k>/checkpoint-<O>.ckpt
// using the UNCHANGED single-threaded CheckpointWriter (so the on-disk format,
// CRC trailer, and tmp+rename+dir-fsync discipline are reused verbatim). Because
// the source stamps ONE absolute offset per barrier and the Router broadcasts
// it, all N partitions snapshot at the SAME offset O for a given barrier.

// The per-partition checkpoint directory for worker `partition` under `root`.
std::string PartitionCheckpointDir(const std::string& root, uint32_t partition);

// The highest offset O for which ALL `num_partitions` partition directories hold
// a CRC-valid checkpoint at O — i.e., the highest COMPLETE global checkpoint.
// A partial set (some partitions wrote O, others crashed first) is a TORN global
// checkpoint and is never returned here; restore falls back to this value.
// nullopt if no offset is complete across every partition.
std::optional<uint64_t> HighestCompleteCheckpoint(const std::string& root,
                                                  uint32_t num_partitions);

// Every offset complete across all `num_partitions` partitions, ascending.
std::vector<uint64_t> CompleteCheckpoints(const std::string& root, uint32_t num_partitions);

// The highest offset present (CRC-valid) in ANY single partition, regardless of
// whether the other partitions have it. When this exceeds
// HighestCompleteCheckpoint (or the latter is nullopt while this is set), a torn
// global checkpoint exists on disk — the exact condition the partitioned
// real-kill nemesis captures and the restore path must discard.
std::optional<uint64_t> HighestPartialCheckpoint(const std::string& root,
                                                  uint32_t num_partitions);

// Live completion tracking and coordinated retention for one running job.
//
// Partitions report each checkpoint after it is durably written. Every
// partition receives the same broadcast barriers in the same order, so once
// every partition has reported an offset >= O, all of them have written O: the
// minimum of the per-partition latest offsets is the newest complete global
// checkpoint, with no directory scan or CRC read.
//
// Retention is decided only here, never per partition: with `retain` > 0, the
// newest `retain` complete checkpoints are kept and every partition file with
// a lower offset is deleted. Files above the oldest retained complete offset,
// including a torn newer partial set, are never touched, so the last complete
// common cut always survives. Deleting is best effort; a leftover file only
// costs space. Thread-safe.
class PartitionedCheckpointTracker {
public:
    // `existing_complete` lists the complete checkpoints already on disk,
    // ascending (CompleteCheckpoints); the newest `retain` of them count toward
    // retention, so a restart does not forget history it should keep.
    // retain == 0 keeps all.
    PartitionedCheckpointTracker(std::string root, uint32_t num_partitions, uint32_t retain,
                                 std::vector<uint64_t> existing_complete);

    void OnPartitionCheckpoint(uint32_t partition, uint64_t offset);

    [[nodiscard]] std::optional<uint64_t> LastComplete() const;

private:
    void PruneBelowLocked(uint64_t cutoff);

    const std::string root_;
    const uint32_t num_partitions_;
    const uint32_t retain_;
    mutable std::mutex mu_;
    std::vector<std::optional<uint64_t>> latest_;
    std::deque<uint64_t> complete_;  // ascending, newest at back
};

} // namespace stormglass

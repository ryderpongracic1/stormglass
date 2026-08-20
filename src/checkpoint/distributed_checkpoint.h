#pragma once

#include <cstdint>
#include <optional>
#include <string>

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

// The highest offset present (CRC-valid) in ANY single partition, regardless of
// whether the other partitions have it. When this exceeds
// HighestCompleteCheckpoint (or the latter is nullopt while this is set), a torn
// global checkpoint exists on disk — the exact condition the partitioned
// real-kill nemesis captures and the restore path must discard.
std::optional<uint64_t> HighestPartialCheckpoint(const std::string& root,
                                                  uint32_t num_partitions);

} // namespace stormglass

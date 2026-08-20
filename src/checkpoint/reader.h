#pragma once

#include "window/window.h"
#include "stream/record.h"

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace stormglass {

struct CheckpointData {
    uint64_t offset;
    Timestamp watermark;

    struct PaneEntry {
        std::string key;
        Window window;
        int64_t sum;
        uint64_t count;
    };
    std::vector<PaneEntry> panes;
    std::vector<Window> fired_windows;  // v2+: persisted fired-window set
};

class CheckpointReader {
public:
    explicit CheckpointReader(const std::string& checkpoint_dir);

    // Find and load the latest valid checkpoint.
    // Returns nullopt if no valid checkpoint exists.
    std::optional<CheckpointData> LoadLatest();

    // --- Distributed-checkpoint support (additive; existing LoadLatest and the
    //     on-disk format are unchanged). These let a coordinator reason about a
    //     specific per-partition checkpoint directory without assuming the
    //     writer's filename encoding: the offset is read from the CRC-validated
    //     payload, never parsed from the name.

    // Every offset for which this directory holds a CRC-valid checkpoint file.
    // Unlike LoadLatest, this does NOT delete stale .tmp files, so it is safe to
    // call repeatedly while another process is still writing checkpoints (used
    // by the partitioned nemesis parent while polling for a torn global state).
    std::vector<uint64_t> ValidOffsets();

    // Load the CRC-valid checkpoint whose stored offset equals `offset`, or
    // nullopt if none. A partition may hold a higher (torn) offset than the one
    // requested; this targets the exact global-complete offset the coordinator
    // chose, rather than the partition-local latest LoadLatest would return.
    std::optional<CheckpointData> LoadOffset(uint64_t offset);

private:
    std::string dir_;

    // Validate CRC, return nullopt if corrupt.
    std::optional<CheckpointData> TryLoad(const std::string& path);

    // Remove stale .tmp files from interrupted writes.
    void CleanTmpFiles();
};

} // namespace stormglass

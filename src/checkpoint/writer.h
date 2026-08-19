#pragma once

#include "window/state.h"
#include "stream/record.h"

#include <cstdint>
#include <string>

namespace stormglass {

class CheckpointWriter {
public:
    explicit CheckpointWriter(const std::string& checkpoint_dir);

    // Write a checkpoint atomically: write to .tmp, fsync, rename to final name.
    // Returns true on success.
    bool WriteCheckpoint(uint64_t offset, Timestamp watermark,
                         const KeyedWindowState& state);

private:
    std::string dir_;

    // Retain last 2 checkpoints, delete older ones.
    void CleanOldCheckpoints(uint64_t current_offset);
};

} // namespace stormglass

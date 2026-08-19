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

private:
    std::string dir_;

    // Validate CRC, return nullopt if corrupt.
    std::optional<CheckpointData> TryLoad(const std::string& path);

    // Remove stale .tmp files from interrupted writes.
    void CleanTmpFiles();
};

} // namespace stormglass

#include "checkpoint/distributed_checkpoint.h"

#include "checkpoint/reader.h"

#include <cstdio>
#include <set>
#include <vector>

namespace stormglass {

std::string PartitionCheckpointDir(const std::string& root, uint32_t partition) {
    char buf[32];
    std::snprintf(buf, sizeof(buf), "/p%u", partition);
    return root + buf;
}

std::optional<uint64_t> HighestCompleteCheckpoint(const std::string& root,
                                                  uint32_t num_partitions) {
    if (num_partitions == 0) return std::nullopt;

    // Seed the intersection with partition 0's valid offsets, then keep only
    // offsets that also appear in every other partition. The surviving set is
    // exactly the offsets for which all N partitions hold a CRC-valid file.
    auto v0 = CheckpointReader(PartitionCheckpointDir(root, 0)).ValidOffsets();
    std::set<uint64_t> common(v0.begin(), v0.end());

    for (uint32_t k = 1; k < num_partitions && !common.empty(); ++k) {
        auto vk = CheckpointReader(PartitionCheckpointDir(root, k)).ValidOffsets();
        std::set<uint64_t> present(vk.begin(), vk.end());
        std::set<uint64_t> next;
        for (uint64_t o : common) {
            if (present.count(o)) next.insert(o);
        }
        common.swap(next);
    }

    if (common.empty()) return std::nullopt;
    return *common.rbegin();  // std::set is ordered — the max is the last element
}

std::optional<uint64_t> HighestPartialCheckpoint(const std::string& root,
                                                  uint32_t num_partitions) {
    std::optional<uint64_t> best;
    for (uint32_t k = 0; k < num_partitions; ++k) {
        for (uint64_t o : CheckpointReader(PartitionCheckpointDir(root, k)).ValidOffsets()) {
            if (!best || o > *best) best = o;
        }
    }
    return best;
}

} // namespace stormglass

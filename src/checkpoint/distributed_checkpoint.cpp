#include "checkpoint/distributed_checkpoint.h"

#include "checkpoint/reader.h"

#include <algorithm>
#include <charconv>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <set>
#include <string_view>
#include <system_error>
#include <utility>
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

PartitionedCheckpointTracker::PartitionedCheckpointTracker(std::string root,
                                                           uint32_t num_partitions,
                                                           uint32_t retain,
                                                           std::optional<uint64_t> restored)
    : root_(std::move(root)),
      num_partitions_(num_partitions),
      retain_(retain),
      latest_(num_partitions) {
    if (restored) complete_.push_back(*restored);
}

void PartitionedCheckpointTracker::OnPartitionCheckpoint(uint32_t partition, uint64_t offset) {
    std::lock_guard lock(mu_);
    latest_.at(partition) = offset;
    uint64_t candidate = UINT64_MAX;
    for (const auto& latest : latest_) {
        if (!latest) return;  // some partition has not checkpointed yet this run
        candidate = std::min(candidate, *latest);
    }
    if (!complete_.empty() && candidate <= complete_.back()) return;
    complete_.push_back(candidate);
    if (retain_ == 0) {
        // Keep only the newest entry in memory; nothing is deleted.
        while (complete_.size() > 1) complete_.pop_front();
        return;
    }
    while (complete_.size() > retain_) complete_.pop_front();
    PruneBelowLocked(complete_.front());
}

std::optional<uint64_t> PartitionedCheckpointTracker::LastComplete() const {
    std::lock_guard lock(mu_);
    if (complete_.empty()) return std::nullopt;
    return complete_.back();
}

void PartitionedCheckpointTracker::PruneBelowLocked(uint64_t cutoff) {
    // Writer file names are "checkpoint-<20-digit offset>.ckpt". Anything else
    // (temporary files, the job manifest, foreign files) is left alone.
    static constexpr std::string_view kPrefix = "checkpoint-";
    static constexpr std::string_view kSuffix = ".ckpt";
    for (uint32_t k = 0; k < num_partitions_; ++k) {
        std::error_code ec;
        std::filesystem::directory_iterator it(PartitionCheckpointDir(root_, k), ec);
        if (ec) continue;
        for (const auto& entry : it) {
            const std::string name = entry.path().filename().string();
            if (name.size() != kPrefix.size() + 20 + kSuffix.size() ||
                name.compare(0, kPrefix.size(), kPrefix) != 0 ||
                name.compare(name.size() - kSuffix.size(), kSuffix.size(), kSuffix) != 0) {
                continue;
            }
            const char* first = name.data() + kPrefix.size();
            const char* last = first + 20;
            uint64_t offset = 0;
            auto [ptr, err] = std::from_chars(first, last, offset);
            if (err != std::errc{} || ptr != last) continue;
            if (offset < cutoff) {
                std::error_code remove_ec;
                std::filesystem::remove(entry.path(), remove_ec);
            }
        }
    }
}

} // namespace stormglass

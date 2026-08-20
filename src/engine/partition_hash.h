#pragma once

#include <cstdint>
#include <string_view>

namespace stormglass {

// FNV-1a 64-bit hash of a byte string.
//
// We deliberately do NOT use std::hash<std::string>: its result is
// implementation-defined and may differ across standard-library versions,
// platforms, and even process runs (some libstdc++ builds salt string hashing).
// Partition assignment must be a FIXED, portable function of the key so that a
// given key always lands on the same worker — this is what lets the N-worker
// engine produce the exact same per-key result set as the single-threaded
// engine, on any machine, run after run. FNV-1a is a small, fully specified
// constant-driven hash with good byte-level dispersion for short string keys.
[[nodiscard]] inline uint64_t Fnv1a64(std::string_view data) noexcept {
    constexpr uint64_t kOffsetBasis = 14695981039346656037ULL;
    constexpr uint64_t kPrime = 1099511628211ULL;
    uint64_t hash = kOffsetBasis;
    for (unsigned char byte : data) {
        hash ^= static_cast<uint64_t>(byte);
        hash *= kPrime;
    }
    return hash;
}

// Map a key to one of `num_partitions` workers. Total function for any
// num_partitions >= 1. Because it is a pure function of the key bytes, all of a
// key's records route to exactly one worker, keeping each key's window state on
// a single shared-nothing worker.
[[nodiscard]] inline uint32_t PartitionForKey(std::string_view key,
                                              uint32_t num_partitions) noexcept {
    return static_cast<uint32_t>(Fnv1a64(key) % num_partitions);
}

} // namespace stormglass

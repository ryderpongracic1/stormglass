#include <gtest/gtest.h>

#include "engine/partition_hash.h"

#include <cstdio>
#include <string>
#include <vector>

namespace stormglass {
namespace {

// Same key always maps to the same partition, for any N.
TEST(PartitionHash, DeterministicPerKey) {
    for (uint32_t n : {1u, 2u, 4u, 8u, 13u}) {
        for (int trial = 0; trial < 100; ++trial) {
            std::string key = "key-" + std::to_string(trial % 37);
            uint32_t a = PartitionForKey(key, n);
            uint32_t b = PartitionForKey(key, n);
            EXPECT_EQ(a, b);
            EXPECT_LT(a, n);
        }
    }
}

// FNV-1a is a fixed constant: pin known-answer values so a future refactor that
// silently changes the hash (or falls back to std::hash) is caught. Values
// computed from the FNV-1a 64-bit reference constants.
TEST(PartitionHash, KnownAnswerValues) {
    EXPECT_EQ(Fnv1a64(""), 14695981039346656037ULL);
    EXPECT_EQ(Fnv1a64("a"), 12638187200555641996ULL);
    EXPECT_EQ(Fnv1a64("foobar"), 9625390261332436968ULL);
}

// N == 1 sends every key to partition 0.
TEST(PartitionHash, SingleWorkerAllZero) {
    for (int i = 0; i < 50; ++i) {
        EXPECT_EQ(PartitionForKey("key-" + std::to_string(i), 1), 0u);
    }
}

// Reasonable dispersion: with many distinct keys, every partition gets a
// nontrivial share (no partition empty, none wildly dominant). This is a
// distribution sanity check, not a uniformity proof.
TEST(PartitionHash, ReasonableDistribution) {
    const uint32_t n = 8;
    const int num_keys = 8000;
    std::vector<int> counts(n, 0);
    for (int i = 0; i < num_keys; ++i) {
        char buf[16];
        std::snprintf(buf, sizeof(buf), "key-%04d", i);
        counts[PartitionForKey(buf, n)]++;
    }
    const double expected = static_cast<double>(num_keys) / n;  // 1000
    for (uint32_t p = 0; p < n; ++p) {
        EXPECT_GT(counts[p], 0) << "partition " << p << " empty";
        // Each bucket within 40% of the mean — generous, just catches gross skew.
        EXPECT_GT(counts[p], expected * 0.6) << "partition " << p << " underweight";
        EXPECT_LT(counts[p], expected * 1.4) << "partition " << p << " overweight";
    }
}

}  // namespace
}  // namespace stormglass

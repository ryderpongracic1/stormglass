#include <gtest/gtest.h>

#include "aggregate/sum.h"
#include "aggregate/simd_sum.h"
#include "aggregate/simd_minmax.h"
#include "aggregate/simd_detect.h"

#include <algorithm>
#include <cstdint>
#include <numeric>
#include <random>
#include <vector>

using namespace stormglass;

// --- SIMD Detection ---

TEST(SimdDetect, SSE42Available) {
    // On any x86_64 cloud instance, SSE4.2 should be present
    EXPECT_TRUE(HasSSE42());
}

TEST(SimdDetect, AVX2Consistent) {
    // Calling twice should give same result (cached)
    bool first = HasAVX2();
    bool second = HasAVX2();
    EXPECT_EQ(first, second);
}

// --- SIMD Sum: Equivalence with Scalar ---

TEST(SimdSum, EquivalentToScalarForRandomInput) {
    std::mt19937_64 rng(12345);
    std::uniform_int_distribution<int64_t> dist(-1'000'000, 1'000'000);

    std::vector<int64_t> data(10'000);
    for (auto& v : data) v = dist(rng);

    SumInt64Kernel scalar;
    SimdSumInt64Kernel simd;

    scalar.AddBatch(data);
    simd.AddBatch(data);

    EXPECT_EQ(scalar.Result().value, simd.Result().value);
    EXPECT_EQ(scalar.Result().count, simd.Result().count);
}

TEST(SimdSum, EquivalentForMultipleBatches) {
    std::mt19937_64 rng(99999);
    std::uniform_int_distribution<int64_t> dist(-500'000, 500'000);

    SumInt64Kernel scalar;
    SimdSumInt64Kernel simd;

    // Feed in multiple batches of varying sizes
    for (int batch = 0; batch < 50; ++batch) {
        size_t size = 100 + (rng() % 1000);
        std::vector<int64_t> data(size);
        for (auto& v : data) v = dist(rng);

        scalar.AddBatch(data);
        simd.AddBatch(data);
    }

    EXPECT_EQ(scalar.Result().value, simd.Result().value);
    EXPECT_EQ(scalar.Result().count, simd.Result().count);
}

// --- SIMD Sum: Edge Cases ---

TEST(SimdSum, EmptySpan) {
    SimdSumInt64Kernel kernel;
    std::span<const int64_t> empty;
    kernel.AddBatch(empty);
    EXPECT_EQ(kernel.Result().value, 0);
    EXPECT_EQ(kernel.Result().count, 0u);
}

TEST(SimdSum, SingleElement) {
    SimdSumInt64Kernel kernel;
    std::vector<int64_t> data = {42};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 42);
    EXPECT_EQ(kernel.Result().count, 1u);
}

TEST(SimdSum, NonAlignedSizes) {
    // Sizes that don't evenly divide into SIMD lanes (4 for AVX2, 2 for SSE)
    for (size_t sz : {1, 3, 5, 7, 9, 15, 33}) {
        SimdSumInt64Kernel simd;
        SumInt64Kernel scalar;

        std::vector<int64_t> data(sz);
        std::iota(data.begin(), data.end(), 1); // 1, 2, 3, ...

        simd.AddBatch(data);
        scalar.AddBatch(data);

        EXPECT_EQ(simd.Result().value, scalar.Result().value)
            << "Failed for size " << sz;
        EXPECT_EQ(simd.Result().count, scalar.Result().count)
            << "Failed for size " << sz;
    }
}

TEST(SimdSum, NegativeValues) {
    SimdSumInt64Kernel kernel;
    std::vector<int64_t> data = {-100, -200, -300, -400, -500, -600, -700, -800};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, -3600);
    EXPECT_EQ(kernel.Result().count, 8u);
}

TEST(SimdSum, MixedPositiveNegative) {
    SimdSumInt64Kernel kernel;
    std::vector<int64_t> data = {100, -100, 200, -200, 300, -300};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 0);
    EXPECT_EQ(kernel.Result().count, 6u);
}

TEST(SimdSum, ResetWorks) {
    SimdSumInt64Kernel kernel;
    std::vector<int64_t> data = {1, 2, 3, 4, 5};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 15);

    kernel.Reset();
    EXPECT_EQ(kernel.Result().value, 0);
    EXPECT_EQ(kernel.Result().count, 0u);

    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 15);
}

TEST(SimdSum, AddSingleThenBatch) {
    SimdSumInt64Kernel kernel;
    kernel.Add(10);
    std::vector<int64_t> data = {20, 30, 40};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 100);
    EXPECT_EQ(kernel.Result().count, 4u);
}

// --- SIMD Min ---

TEST(SimdMin, FindsMinimum) {
    SimdMinInt64Kernel kernel;
    std::vector<int64_t> data = {5, 3, 8, 1, 9, 2, 7, 4, 6};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 1);
    EXPECT_EQ(kernel.Result().count, 9u);
}

TEST(SimdMin, NegativeValues) {
    SimdMinInt64Kernel kernel;
    std::vector<int64_t> data = {-5, -3, -8, -1, -9, -2};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, -9);
}

TEST(SimdMin, SingleElement) {
    SimdMinInt64Kernel kernel;
    std::vector<int64_t> data = {42};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 42);
}

TEST(SimdMin, EmptySpan) {
    SimdMinInt64Kernel kernel;
    std::span<const int64_t> empty;
    kernel.AddBatch(empty);
    EXPECT_EQ(kernel.Result().value, std::numeric_limits<int64_t>::max());
    EXPECT_EQ(kernel.Result().count, 0u);
}

TEST(SimdMin, MultipleBatches) {
    SimdMinInt64Kernel kernel;
    std::vector<int64_t> batch1 = {10, 20, 30};
    std::vector<int64_t> batch2 = {5, 25, 35};
    kernel.AddBatch(batch1);
    kernel.AddBatch(batch2);
    EXPECT_EQ(kernel.Result().value, 5);
    EXPECT_EQ(kernel.Result().count, 6u);
}

TEST(SimdMin, ResetWorks) {
    SimdMinInt64Kernel kernel;
    std::vector<int64_t> data = {5, 10};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 5);

    kernel.Reset();
    EXPECT_EQ(kernel.Result().value, std::numeric_limits<int64_t>::max());
}

// --- SIMD Max ---

TEST(SimdMax, FindsMaximum) {
    SimdMaxInt64Kernel kernel;
    std::vector<int64_t> data = {5, 3, 8, 1, 9, 2, 7, 4, 6};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 9);
    EXPECT_EQ(kernel.Result().count, 9u);
}

TEST(SimdMax, NegativeValues) {
    SimdMaxInt64Kernel kernel;
    std::vector<int64_t> data = {-5, -3, -8, -1, -9, -2};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, -1);
}

TEST(SimdMax, SingleElement) {
    SimdMaxInt64Kernel kernel;
    std::vector<int64_t> data = {42};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 42);
}

TEST(SimdMax, EmptySpan) {
    SimdMaxInt64Kernel kernel;
    std::span<const int64_t> empty;
    kernel.AddBatch(empty);
    EXPECT_EQ(kernel.Result().value, std::numeric_limits<int64_t>::min());
    EXPECT_EQ(kernel.Result().count, 0u);
}

TEST(SimdMax, MultipleBatches) {
    SimdMaxInt64Kernel kernel;
    std::vector<int64_t> batch1 = {10, 20, 30};
    std::vector<int64_t> batch2 = {5, 25, 35};
    kernel.AddBatch(batch1);
    kernel.AddBatch(batch2);
    EXPECT_EQ(kernel.Result().value, 35);
    EXPECT_EQ(kernel.Result().count, 6u);
}

TEST(SimdMax, ResetWorks) {
    SimdMaxInt64Kernel kernel;
    std::vector<int64_t> data = {5, 10};
    kernel.AddBatch(data);
    EXPECT_EQ(kernel.Result().value, 10);

    kernel.Reset();
    EXPECT_EQ(kernel.Result().value, std::numeric_limits<int64_t>::min());
}

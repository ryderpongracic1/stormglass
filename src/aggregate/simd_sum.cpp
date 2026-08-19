#include "aggregate/simd_sum.h"
#include "aggregate/simd_detect.h"

#include <immintrin.h>

namespace stormglass {

namespace {

// AVX2 path: accumulate 4 int64s per iteration
__attribute__((target("avx2")))
int64_t SumBatchAVX2(const int64_t* data, size_t count) {
    __m256i acc = _mm256_setzero_si256();
    size_t i = 0;
    const size_t vec_end = count - (count % 4);

    for (; i < vec_end; i += 4) {
        __m256i v = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(data + i));
        acc = _mm256_add_epi64(acc, v);
    }

    // Horizontal sum of 4 lanes
    // Extract high 128 bits, add to low 128 bits
    __m128i lo = _mm256_castsi256_si128(acc);
    __m128i hi = _mm256_extracti128_si256(acc, 1);
    __m128i sum128 = _mm_add_epi64(lo, hi);
    // Sum the two 64-bit lanes
    int64_t result = _mm_extract_epi64(sum128, 0) + _mm_extract_epi64(sum128, 1);

    // Scalar remainder
    for (; i < count; ++i) {
        result += data[i];
    }
    return result;
}

// SSE4.2 path: accumulate 2 int64s per iteration
int64_t SumBatchSSE(const int64_t* data, size_t count) {
    __m128i acc = _mm_setzero_si128();
    size_t i = 0;
    const size_t vec_end = count - (count % 2);

    for (; i < vec_end; i += 2) {
        __m128i v = _mm_loadu_si128(reinterpret_cast<const __m128i*>(data + i));
        acc = _mm_add_epi64(acc, v);
    }

    // Horizontal sum of 2 lanes
    int64_t result = _mm_extract_epi64(acc, 0) + _mm_extract_epi64(acc, 1);

    // Scalar remainder
    for (; i < count; ++i) {
        result += data[i];
    }
    return result;
}

// Scalar fallback
int64_t SumBatchScalar(const int64_t* data, size_t count) {
    int64_t result = 0;
    for (size_t i = 0; i < count; ++i) {
        result += data[i];
    }
    return result;
}

} // namespace

void SimdSumInt64Kernel::Add(int64_t value) {
    sum_ += value;
    ++count_;
}

void SimdSumInt64Kernel::AddBatch(std::span<const int64_t> values) {
    if (values.empty()) return;

    if (HasAVX2()) {
        sum_ += SumBatchAVX2(values.data(), values.size());
    } else if (HasSSE42()) {
        sum_ += SumBatchSSE(values.data(), values.size());
    } else {
        sum_ += SumBatchScalar(values.data(), values.size());
    }
    count_ += values.size();
}

AggregateResult SimdSumInt64Kernel::Result() const {
    return {sum_, count_};
}

void SimdSumInt64Kernel::Reset() {
    sum_ = 0;
    count_ = 0;
}

} // namespace stormglass

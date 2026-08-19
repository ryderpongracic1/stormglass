#include "aggregate/simd_detect.h"

namespace stormglass {

bool HasSSE42() {
    static const bool result = __builtin_cpu_supports("sse4.2");
    return result;
}

bool HasAVX2() {
    static const bool result = __builtin_cpu_supports("avx2");
    return result;
}

} // namespace stormglass

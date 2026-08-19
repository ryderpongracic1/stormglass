#pragma once

namespace stormglass {

/// Returns true if the CPU supports SSE 4.2. Result is cached after first call.
bool HasSSE42();

/// Returns true if the CPU supports AVX2. Result is cached after first call.
bool HasAVX2();

} // namespace stormglass

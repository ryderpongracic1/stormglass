#pragma once

#include <cstddef>
#include <cstdint>

namespace stormglass {

// Software CRC32C (Castagnoli) using a precomputed lookup table.
// No hardware dependency — works on any platform.
uint32_t Crc32c(const void* data, size_t length, uint32_t initial = 0);

} // namespace stormglass

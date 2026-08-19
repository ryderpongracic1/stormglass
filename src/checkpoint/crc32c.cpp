#include "checkpoint/crc32c.h"

#include <array>

namespace stormglass {
namespace {

// CRC32C polynomial (Castagnoli): 0x1EDC6F41, bit-reversed: 0x82F63B78
constexpr uint32_t kCrc32cPoly = 0x82F63B78;

constexpr std::array<uint32_t, 256> BuildTable() {
    std::array<uint32_t, 256> table{};
    for (uint32_t i = 0; i < 256; ++i) {
        uint32_t crc = i;
        for (int j = 0; j < 8; ++j) {
            if (crc & 1) {
                crc = (crc >> 1) ^ kCrc32cPoly;
            } else {
                crc >>= 1;
            }
        }
        table[i] = crc;
    }
    return table;
}

constexpr auto kTable = BuildTable();

} // namespace

uint32_t Crc32c(const void* data, size_t length, uint32_t initial) {
    uint32_t crc = ~initial;
    auto* bytes = static_cast<const uint8_t*>(data);
    for (size_t i = 0; i < length; ++i) {
        crc = kTable[(crc ^ bytes[i]) & 0xFF] ^ (crc >> 8);
    }
    return ~crc;
}

} // namespace stormglass

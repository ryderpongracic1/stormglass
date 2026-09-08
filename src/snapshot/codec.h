#pragma once
#include "snapshot/chandy_lamport.h"
#include <bit>
#include <limits>
#include <span>
#include <stdexcept>
#include <string>

namespace stormglass::snapshot {
struct Encoder {
    Bytes bytes;
    void U8(uint8_t v) { bytes.push_back(v); }
    void U32(uint32_t v) {
        for (int i = 0; i < 4; ++i)
            U8(static_cast<uint8_t>(v >> (8 * i)));
    }
    void U64(uint64_t v) {
        for (int i = 0; i < 8; ++i)
            U8(static_cast<uint8_t>(v >> (8 * i)));
    }
    void I64(int64_t v) { U64(std::bit_cast<uint64_t>(v)); }
    void Blob(std::span<const uint8_t> b) {
        if (b.size() > std::numeric_limits<uint32_t>::max())
            throw std::runtime_error("encoded blob too large");
        U32(static_cast<uint32_t>(b.size()));
        bytes.insert(bytes.end(), b.begin(), b.end());
    }
    void String(const std::string &s) {
        Blob(std::span(reinterpret_cast<const uint8_t *>(s.data()), s.size()));
    }
};
class Decoder {
  public:
    explicit Decoder(std::span<const uint8_t> bytes) : bytes_(bytes) {}
    uint8_t U8() {
        Need(1);
        return bytes_[position_++];
    }
    uint32_t U32() {
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i)
            v |= uint32_t(U8()) << (8 * i);
        return v;
    }
    uint64_t U64() {
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i)
            v |= uint64_t(U8()) << (8 * i);
        return v;
    }
    int64_t I64() { return std::bit_cast<int64_t>(U64()); }
    Bytes Blob() {
        auto n = U32();
        Need(n);
        Bytes b(bytes_.begin() + position_, bytes_.begin() + position_ + n);
        position_ += n;
        return b;
    }
    std::string String() {
        auto b = Blob();
        return std::string(b.begin(), b.end());
    }
    uint32_t Count(std::size_t minimum_bytes) {
        auto n = U32();
        if (n > Remaining() / minimum_bytes)
            throw std::runtime_error("impossible serialized count");
        return n;
    }
    std::size_t Remaining() const { return bytes_.size() - position_; }
    void End() const {
        if (Remaining())
            throw std::runtime_error("trailing serialized data");
    }

  private:
    void Need(std::size_t n) const {
        if (n > Remaining())
            throw std::runtime_error("truncated serialized data");
    }
    std::span<const uint8_t> bytes_;
    std::size_t position_ = 0;
};
} // namespace stormglass::snapshot

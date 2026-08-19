#include "source/generator.h"
namespace stormglass {
DeterministicGenerator::DeterministicGenerator(GeneratorConfig config) : config_(config) {}
std::optional<Batch> DeterministicGenerator::Next() { return std::nullopt; }
void DeterministicGenerator::Seek(uint64_t offset) { offset_ = offset; }
uint64_t DeterministicGenerator::CurrentOffset() const { return offset_; }
} // namespace stormglass

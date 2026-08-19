#include "sink/memory_sink.h"
namespace stormglass {
void MemorySink::Emit(const WindowResult& result) { results_.push_back(result); }
void MemorySink::Flush() {}
} // namespace stormglass

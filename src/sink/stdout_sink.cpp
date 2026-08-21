#include "sink/stdout_sink.h"
#include <cinttypes>

#include <cstdio>

namespace stormglass {

void StdoutSink::Emit(const WindowResult& result) {
    std::printf("window=[%" PRId64 ", %" PRId64 ") key=%s sum=%" PRId64 " count=%" PRIu64 "\n",
                static_cast<int64_t>(result.window.start.time_since_epoch().count()),
                static_cast<int64_t>(result.window.end.time_since_epoch().count()),
                result.key.c_str(),
                static_cast<int64_t>(result.result.value),
                static_cast<uint64_t>(result.result.count));
}

void StdoutSink::Flush() {
    std::fflush(stdout);
}

} // namespace stormglass

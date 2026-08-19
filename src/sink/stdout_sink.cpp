#include "sink/stdout_sink.h"

#include <cstdio>

namespace stormglass {

void StdoutSink::Emit(const WindowResult& result) {
    std::printf("window=[%ld, %ld) key=%s sum=%ld count=%lu\n",
                static_cast<long>(result.window.start.time_since_epoch().count()),
                static_cast<long>(result.window.end.time_since_epoch().count()),
                result.key.c_str(),
                static_cast<long>(result.result.value),
                static_cast<unsigned long>(result.result.count));
}

void StdoutSink::Flush() {
    std::fflush(stdout);
}

} // namespace stormglass

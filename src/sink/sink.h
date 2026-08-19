#pragma once
#include "stream/record.h"
#include "window/window.h"
#include "aggregate/kernel.h"
namespace stormglass {
struct WindowResult { std::string key; Window window; AggregateResult result; };
class Sink {
public:
    virtual ~Sink() = default;
    virtual void Emit(const WindowResult& result) = 0;
    virtual void Flush() = 0;
};
} // namespace stormglass

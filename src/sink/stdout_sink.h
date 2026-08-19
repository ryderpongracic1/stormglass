#pragma once

#include "sink/sink.h"

namespace stormglass {

class StdoutSink : public Sink {
public:
    void Emit(const WindowResult& result) override;
    void Flush() override;
};

} // namespace stormglass

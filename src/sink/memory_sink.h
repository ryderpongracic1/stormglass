#pragma once
#include "sink/sink.h"
#include <vector>
namespace stormglass {
class MemorySink : public Sink {
public:
    void Emit(const WindowResult& result) override;
    void Flush() override;
    [[nodiscard]] const std::vector<WindowResult>& Results() const { return results_; }
    void Clear() { results_.clear(); }
private:
    std::vector<WindowResult> results_;
};
} // namespace stormglass

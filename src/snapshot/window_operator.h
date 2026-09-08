#pragma once
#include "snapshot/chandy_lamport.h"
#include "engine/keyed_processor.h"

namespace stormglass::snapshot {
// Event-time operator for concurrent TCP inputs. Captures min-combiner state as
// well as panes/fired/pending-refire state. Every configured input participates;
// quiet inputs pin watermarks and snapshots until they send progress/markers.
class WindowOperator {
  public:
    WindowOperator(std::vector<NodeId> inputs, Sink &sink, int64_t window_ms = 1000,
                   int64_t slide_ms = 0, int64_t lateness_ms = 0);
    void Apply(NodeId input, const Bytes &payload);
    [[nodiscard]] Bytes Capture() const;
    void Restore(const Bytes &state); // fresh operator only; requires same config
    void FinalFlush() { processor_->FinalFlush(); }
    [[nodiscard]] uint64_t records() const { return records_; }
    [[nodiscard]] Timestamp watermark() const { return processor_->watermark(); }
    static Bytes Data(const Record &record);
    static Bytes Watermark(Timestamp watermark);

  private:
    std::map<NodeId, Timestamp> watermarks_;
    Sink &sink_;
    int64_t window_ms_, slide_ms_, lateness_ms_;
    std::unique_ptr<KeyedProcessor> processor_;
    uint64_t records_ = 0;
    bool touched_ = false;
};
} // namespace stormglass::snapshot

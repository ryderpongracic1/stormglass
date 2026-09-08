#include "snapshot/window_operator.h"
#include "snapshot/codec.h"
#include "window/tumbling.h"
#include "window/sliding.h"
#include <algorithm>
#include <set>
#include <stdexcept>

namespace stormglass::snapshot {
namespace {
Timestamp Time(int64_t n) { return Timestamp{Duration{n}}; }
void WriteWindow(Encoder &e, const Window &w) {
    e.I64(w.start.time_since_epoch().count());
    e.I64(w.end.time_since_epoch().count());
}
Window ReadWindow(Decoder &d) {
    auto start = d.I64();
    auto end = d.I64();
    if (start < 0 || end <= start)
        throw std::runtime_error("invalid saved window");
    return {Time(start), Time(end)};
}
} // namespace
WindowOperator::WindowOperator(std::vector<NodeId> inputs, Sink &sink, int64_t window_ms,
                               int64_t slide_ms, int64_t lateness_ms)
    : sink_(sink), window_ms_(window_ms), slide_ms_(slide_ms), lateness_ms_(lateness_ms) {
    if (inputs.empty() || slide_ms < 0)
        throw std::invalid_argument("invalid window input/slide configuration");
    for (auto id : inputs)
        if (!watermarks_.emplace(id, Timestamp::min()).second)
            throw std::invalid_argument("duplicate window input");
    std::unique_ptr<WindowAssigner> assigner;
    if (slide_ms)
        assigner = std::make_unique<SlidingAssigner>(Duration{window_ms}, Duration{slide_ms});
    else
        assigner = std::make_unique<TumblingAssigner>(Duration{window_ms});
    processor_ = std::make_unique<KeyedProcessor>(std::move(assigner), sink, Duration{lateness_ms});
}
Bytes WindowOperator::Data(const Record &r) {
    Encoder e;
    e.U8(1);
    e.String(r.key);
    e.I64(r.value);
    e.I64(r.event_time.time_since_epoch().count());
    e.I64(r.processing_time.time_since_epoch().count());
    return std::move(e.bytes);
}
Bytes WindowOperator::Watermark(Timestamp wm) {
    Encoder e;
    e.U8(2);
    e.I64(wm.time_since_epoch().count());
    return std::move(e.bytes);
}
void WindowOperator::Apply(NodeId input, const Bytes &payload) {
    if (!watermarks_.contains(input))
        throw std::runtime_error("unknown window input");
    touched_ = true;
    Decoder d(payload);
    auto kind = d.U8();
    if (kind == 1) {
        Record r;
        r.key = d.String();
        r.value = d.I64();
        r.event_time = Time(d.I64());
        r.processing_time = Time(d.I64());
        d.End();
        if (records_ == std::numeric_limits<uint64_t>::max())
            throw std::overflow_error("window record count overflow");
        processor_->ProcessRecord(r);
        ++records_;
    } else if (kind == 2) {
        auto wm = Time(d.I64());
        d.End();
        if (wm < watermarks_.at(input))
            throw std::runtime_error("network source watermark regressed");
        watermarks_.at(input) = wm;
        auto minimum = Timestamp::max();
        for (auto [peer, value] : watermarks_) {
            (void)peer;
            minimum = std::min(minimum, value);
        }
        processor_->ProcessControl({ControlType::kWatermark, minimum, records_});
    } else
        throw std::runtime_error("unknown window input payload");
}
Bytes WindowOperator::Capture() const {
    // The sink contract is still at-least-once: flush preceding output, but no
    // transaction is implied by taking an operator/channel snapshot.
    sink_.Flush();
    auto state = processor_->Capture(records_);
    Encoder e;
    e.U32(0x53475731);
    e.I64(window_ms_);
    e.I64(slide_ms_);
    e.I64(lateness_ms_);
    e.U64(records_);
    e.I64(state.watermark.time_since_epoch().count());
    e.U32(static_cast<uint32_t>(watermarks_.size()));
    for (auto [peer, wm] : watermarks_) {
        e.U32(peer);
        e.I64(wm.time_since_epoch().count());
    }
    e.U32(static_cast<uint32_t>(state.panes.size()));
    for (const auto &p : state.panes) {
        e.String(p.key);
        WriteWindow(e, p.window);
        e.I64(p.sum);
        e.U64(p.count);
    }
    auto windows = [&](const auto &list) {
        e.U32(static_cast<uint32_t>(list.size()));
        for (const auto &w : list)
            WriteWindow(e, w);
    };
    windows(state.fired_windows);
    windows(state.refired_windows);
    return std::move(e.bytes);
}
void WindowOperator::Restore(const Bytes &bytes) {
    if (touched_)
        throw std::runtime_error("window restore requires fresh operator");
    Decoder d(bytes);
    if (d.U32() != 0x53475731 || d.I64() != window_ms_ || d.I64() != slide_ms_ ||
        d.I64() != lateness_ms_)
        throw std::runtime_error("window snapshot configuration mismatch");
    CheckpointData state;
    state.offset = d.U64();
    state.watermark = Time(d.I64());
    std::map<NodeId, Timestamp> restored;
    auto count = d.Count(12);
    for (uint32_t i = 0; i < count; ++i) {
        auto id = d.U32();
        auto wm = Time(d.I64());
        if (!watermarks_.contains(id) || !restored.emplace(id, wm).second)
            throw std::runtime_error("window snapshot inputs mismatch");
    }
    if (restored.size() != watermarks_.size())
        throw std::runtime_error("window snapshot input missing");
    auto minimum = Timestamp::max();
    for (auto [id, wm] : restored) {
        (void)id;
        minimum = std::min(minimum, wm);
    }
    if (minimum != state.watermark)
        throw std::runtime_error("inconsistent saved watermark combiner");
    auto panes = d.Count(36);
    std::set<std::pair<std::string, std::pair<int64_t, int64_t>>> unique;
    for (uint32_t i = 0; i < panes; ++i) {
        CheckpointData::PaneEntry p;
        p.key = d.String();
        p.window = ReadWindow(d);
        p.sum = d.I64();
        p.count = d.U64();
        if (!p.count ||
            !unique
                 .emplace(p.key, std::make_pair(p.window.start.time_since_epoch().count(),
                                                p.window.end.time_since_epoch().count()))
                 .second)
            throw std::runtime_error("duplicate/empty saved pane");
        state.panes.push_back(std::move(p));
    }
    auto windows = [&](auto &list) {
        auto n = d.Count(16);
        for (uint32_t i = 0; i < n; ++i)
            list.push_back(ReadWindow(d));
    };
    windows(state.fired_windows);
    windows(state.refired_windows);
    d.End();
    for (const auto &w : state.refired_windows)
        if (std::find(state.fired_windows.begin(), state.fired_windows.end(), w) ==
            state.fired_windows.end())
            throw std::runtime_error("pending re-fire without fired window");
    processor_->Restore(state);
    watermarks_ = std::move(restored);
    records_ = state.offset;
    touched_ = true;
}
} // namespace stormglass::snapshot

// Finding-1 isolation benchmark.
//
// The pathology: KeyedWindowState touches window-state on every watermark
// advance. If that work is proportional to the number of *live* panes rather
// than to the windows actually expiring, throughput moves with watermark
// frequency and collapses as the key space grows — even though no additional
// useful work is being done.
//
// This benchmark isolates that. LivePaneSource keeps a fixed number of panes
// alive in a single window whose end sits far in the future, so no window ever
// fires during the run. It then drives a fixed number of records into those
// panes while injecting a monotonically-increasing watermark (held below the
// window end) every `watermark_interval` records. Any time that moves with the
// watermark interval or with the live-pane count is pure per-watermark overhead.
//
// Two sweeps are reported:
//   1. Fixed records + fixed live panes, varying watermark interval.
//   2. Fixed records + fixed watermark interval, varying live-pane count.

#include "engine/pipeline.h"
#include "sink/memory_sink.h"
#include "source/source.h"
#include "window/tumbling.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <memory>

using namespace stormglass;
using Clock = std::chrono::high_resolution_clock;

namespace {

// A window end far beyond any watermark this benchmark emits, so the single
// window never expires and every pane stays live for the whole run.
constexpr int64_t kFarFutureMs = 1LL << 50;

class LivePaneSource : public Source {
public:
    LivePaneSource(uint64_t live_panes, uint64_t total_records,
                   uint32_t watermark_interval)
        : live_panes_(live_panes),
          total_records_(total_records),
          watermark_interval_(watermark_interval) {}

    std::optional<Batch> Next() override {
        if (emitted_ >= total_records_) return std::nullopt;

        Batch batch;
        uint64_t this_batch = std::min<uint64_t>(kBatch, total_records_ - emitted_);
        batch.items.reserve(this_batch + this_batch / watermark_interval_ + 1);

        for (uint64_t i = 0; i < this_batch; ++i) {
            // Round-robin over live_panes_ keys: the first pass creates the
            // panes, every later pass adds to an existing pane (no new panes).
            // Sized for "key-" + up to 20 digits (max uint64) + NUL so
            // -Wformat-truncation cannot fire, though live_panes_ keeps the
            // actual value far shorter.
            char key[32];
            std::snprintf(key, sizeof(key), "key-%08llu",
                          static_cast<unsigned long long>(emitted_ % live_panes_));
            // Event time inside [0, window) keeps every record in one window.
            batch.items.emplace_back(Record{
                .key = key,
                .value = 1,
                .event_time = Timestamp{Duration{static_cast<int64_t>(emitted_ % 1000)}},
                .processing_time = Timestamp{Duration{static_cast<int64_t>(emitted_)}},
            });
            ++emitted_;

            if (++since_wm_ >= watermark_interval_) {
                since_wm_ = 0;
                // Strictly-increasing watermark held far below the window end,
                // so ExpiredWindows finds nothing to fire but must still be asked.
                wm_ms_ += 1;
                batch.items.emplace_back(ControlRecord{
                    .type = ControlType::kWatermark,
                    .watermark = Timestamp{Duration{wm_ms_}},
                    .checkpoint_offset = emitted_,
                });
            }
        }
        return batch;
    }

    void Seek(uint64_t) override {}
    [[nodiscard]] uint64_t CurrentOffset() const override { return emitted_; }

private:
    static constexpr uint64_t kBatch = 4096;
    uint64_t live_panes_;
    uint64_t total_records_;
    uint32_t watermark_interval_;
    uint64_t emitted_ = 0;
    uint64_t since_wm_ = 0;
    int64_t wm_ms_ = 0;
};

double RunOnce(uint64_t live_panes, uint64_t total_records,
               uint32_t watermark_interval) {
    // A single window covering [0, kFarFutureMs): one window, live_panes keys.
    auto source = std::make_unique<LivePaneSource>(live_panes, total_records,
                                                   watermark_interval);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{kFarFutureMs});
    auto sink = std::make_unique<MemorySink>();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));

    auto start = Clock::now();
    auto stats = pipeline.Run();
    auto end = Clock::now();

    double sec = std::chrono::duration<double>(end - start).count();
    return static_cast<double>(stats.records_processed) / sec / 1e6;  // M rec/s
}

double Median3(uint64_t live_panes, uint64_t total_records, uint32_t wm) {
    double a = RunOnce(live_panes, total_records, wm);
    double b = RunOnce(live_panes, total_records, wm);
    double c = RunOnce(live_panes, total_records, wm);
    // median of 3
    double hi = std::max({a, b, c});
    double lo = std::min({a, b, c});
    return a + b + c - hi - lo;
}

} // namespace

int main() {
    constexpr uint64_t kRecords = 2'000'000;

    std::printf("=== Finding 1 isolation: per-watermark window-state cost ===\n");
    std::printf("Fixed workload: %llu records into a single non-expiring window.\n",
                static_cast<unsigned long long>(kRecords));
    std::printf("Throughput is median of 3 runs (M rec/s).\n\n");

    std::printf("Sweep A — vary watermark interval, live panes fixed at 20000\n");
    std::printf("  %-18s %-14s\n", "watermark_interval", "throughput");
    for (uint32_t wm : {50u, 100u, 500u, 2000u}) {
        double mrs = Median3(20000, kRecords, wm);
        std::printf("  %-18u %.2f M rec/s\n", wm, mrs);
    }

    std::printf("\nSweep B — vary live-pane count, watermark interval fixed at 100\n");
    std::printf("  %-18s %-14s\n", "live_panes", "throughput");
    for (uint64_t panes : {2000u, 5000u, 10000u, 20000u}) {
        double mrs = Median3(panes, kRecords, 100);
        std::printf("  %-18llu %.2f M rec/s\n",
                    static_cast<unsigned long long>(panes), mrs);
    }
    std::printf("\n");
    return 0;
}

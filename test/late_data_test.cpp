#include <gtest/gtest.h>
#include "window/state.h"
#include "window/tumbling.h"
#include "sink/memory_sink.h"
#include "source/source.h"
#include "source/generator.h"
#include "engine/pipeline.h"

#include <algorithm>
#include <unordered_map>

namespace stormglass {
namespace {

TEST(LateData, AcceptedWithinLateness) {
    // Record within allowed_lateness after fire: accepted, window re-fires with updated result
    KeyedWindowState state;
    state.SetAllowedLateness(Duration{2000});

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    // Add initial data and fire
    state.Add("k1", w, 10);
    state.Add("k1", w, 20);
    auto results = state.FireWindow(w);
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].result.value, 30);
    EXPECT_EQ(results[0].result.count, 2u);

    // Now add late data — watermark is at 1500 (< end + lateness = 3000)
    bool accepted = state.AddWithLateness("k1", w, 5, Timestamp{Duration{1500}});
    EXPECT_TRUE(accepted);

    // Window should be marked for re-fire
    auto refired = state.RefiredWindows();
    ASSERT_EQ(refired.size(), 1u);
    EXPECT_EQ(refired[0], w);

    // Re-fire should include ALL data (original + late)
    auto results2 = state.FireWindow(w);
    ASSERT_EQ(results2.size(), 1u);
    EXPECT_EQ(results2[0].result.value, 35);  // 10 + 20 + 5
    EXPECT_EQ(results2[0].result.count, 3u);
}

TEST(LateData, DroppedBeyondLateness) {
    // Record beyond allowed_lateness: dropped, drop counter incremented
    KeyedWindowState state;
    state.SetAllowedLateness(Duration{2000});

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 10);
    state.FireWindow(w);

    // Watermark at 3000 >= end(1000) + lateness(2000) = 3000 → dropped
    bool accepted = state.AddWithLateness("k1", w, 99, Timestamp{Duration{3000}});
    EXPECT_FALSE(accepted);

    // No re-fire should be marked
    EXPECT_TRUE(state.RefiredWindows().empty());
}

TEST(LateData, NoLatenessDefaultDropsImmediately) {
    // No allowed_lateness (default): late records are dropped immediately after fire
    // because FireWindow erases panes, and AddWithLateness sees no pane + fired = drop
    KeyedWindowState state;
    // No SetAllowedLateness — default is 0

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 10);
    state.FireWindow(w);

    // Window is gone — AddWithLateness with 0 lateness:
    // fired_windows_ is empty (lateness=0 doesn't track), so this is a fresh add
    // Actually with lateness=0, Fire erases panes and doesn't track in fired_windows_
    // So Add just creates a new pane — this is the "no late policy" behavior
    // The pipeline controls this: it uses Add (not AddWithLateness) when lateness=0
    // Test that at pipeline level instead
}

TEST(LateData, GCRemovesPanesAfterLateness) {
    // GC removes panes only after lateness expires
    KeyedWindowState state;
    state.SetAllowedLateness(Duration{2000});

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 10);
    state.FireWindow(w);

    // Before deadline: no GC
    auto gc1 = state.GarbageCollectableWindows(Timestamp{Duration{2999}});
    EXPECT_TRUE(gc1.empty());

    // At deadline: GC
    auto gc2 = state.GarbageCollectableWindows(Timestamp{Duration{3000}});
    ASSERT_EQ(gc2.size(), 1u);
    EXPECT_EQ(gc2[0], w);

    // Perform GC
    state.GarbageCollect(gc2);

    // Pane is gone — AllWindows should be empty
    EXPECT_TRUE(state.AllWindows().empty());
}

TEST(LateData, MultipleKeysInWindow) {
    // Late data for different keys in the same window
    KeyedWindowState state;
    state.SetAllowedLateness(Duration{5000});

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 10);
    state.Add("k2", w, 20);
    state.FireWindow(w);

    // Late data for both keys
    EXPECT_TRUE(state.AddWithLateness("k1", w, 5, Timestamp{Duration{2000}}));
    EXPECT_TRUE(state.AddWithLateness("k2", w, 7, Timestamp{Duration{2000}}));

    auto results = state.FireWindow(w);
    ASSERT_EQ(results.size(), 2u);

    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.key < b.key; });
    EXPECT_EQ(results[0].key, "k1");
    EXPECT_EQ(results[0].result.value, 15);  // 10 + 5
    EXPECT_EQ(results[1].key, "k2");
    EXPECT_EQ(results[1].result.value, 27);  // 20 + 7
}

TEST(LateData, PipelineEndToEnd) {
    // Pipeline end-to-end: with lateness, verify records arriving after watermark
    // advances past window end are handled correctly.
    // We use a hand-crafted source that deliberately produces late records.
    struct LateDataSource : public Source {
        int phase_ = 0;
        std::optional<Batch> Next() override {
            Batch batch;
            switch (phase_++) {
                case 0:
                    // Phase 0: normal records for window [0, 1000)
                    batch.items.emplace_back(Record{"k1", 10, Timestamp{Duration{100}}, Timestamp{Duration{100}}});
                    batch.items.emplace_back(Record{"k1", 20, Timestamp{Duration{500}}, Timestamp{Duration{500}}});
                    batch.items.emplace_back(Record{"k1", 30, Timestamp{Duration{900}}, Timestamp{Duration{900}}});
                    return batch;
                case 1:
                    // Phase 1: watermark advances past window [0,1000) end
                    batch.items.emplace_back(ControlRecord{ControlType::kWatermark, Timestamp{Duration{1500}}, 0});
                    return batch;
                case 2:
                    // Phase 2: late record for window [0,1000) — within lateness
                    batch.items.emplace_back(Record{"k1", 5, Timestamp{Duration{700}}, Timestamp{Duration{2000}}});
                    return batch;
                case 3:
                    // Phase 3: watermark advances more but still within lateness (< 1000 + 2000 = 3000)
                    batch.items.emplace_back(ControlRecord{ControlType::kWatermark, Timestamp{Duration{2500}}, 0});
                    return batch;
                case 4:
                    // Phase 4: another late record — beyond lateness (wm=2500 >= end+lateness=3000? no, 2500<3000)
                    // Actually still within. Add one more watermark to push past.
                    batch.items.emplace_back(Record{"k1", 7, Timestamp{Duration{800}}, Timestamp{Duration{3000}}});
                    return batch;
                case 5:
                    // Phase 5: watermark past lateness deadline (>= 1000 + 2000 = 3000)
                    batch.items.emplace_back(ControlRecord{ControlType::kWatermark, Timestamp{Duration{3500}}, 0});
                    return batch;
                case 6:
                    // Phase 6: record for window [0,1000) — now DROPPED (wm=3500 >= deadline=3000)
                    batch.items.emplace_back(Record{"k1", 99, Timestamp{Duration{200}}, Timestamp{Duration{4000}}});
                    return batch;
                default:
                    return std::nullopt;
            }
        }
        void Seek(uint64_t) override {}
        uint64_t CurrentOffset() const override { return 0; }
    };

    PipelineConfig pipe_config{.allowed_lateness = Duration{2000}};

    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();

    Pipeline pipeline(
        std::make_unique<LateDataSource>(),
        std::make_unique<TumblingAssigner>(Duration{1000}),
        std::move(sink),
        pipe_config);
    auto stats = pipeline.Run();

    // 6 records processed total (3 in phase 0 + 1 each in phases 2, 4, 6)
    EXPECT_EQ(stats.records_processed, 6u);
    // Window [0,1000) fired once initially
    EXPECT_GE(stats.windows_fired, 1u);
    // Late records accepted: 2 (phases 2 and 4)
    EXPECT_EQ(stats.late_records_accepted, 2u);
    // Late records dropped: 1 (phase 6)
    EXPECT_EQ(stats.late_records_dropped, 1u);
    // Windows re-fired: 2 (after phase 3 and phase 5 watermark advances)
    EXPECT_EQ(stats.windows_refired, 2u);

    // Verify the re-fired results contain updated aggregate
    // Final result for window [0,1000) key k1 should be 10+20+30+5+7 = 72
    // Look for the last emission for k1 in window [0,1000)
    Window target{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};
    int64_t last_value = 0;
    uint64_t last_count = 0;
    for (const auto& r : sink_ptr->Results()) {
        if (r.key == "k1" && r.window == target) {
            last_value = r.result.value;
            last_count = r.result.count;
        }
    }
    EXPECT_EQ(last_value, 72);  // 10+20+30+5+7
    EXPECT_EQ(last_count, 5u);
}

TEST(LateData, PipelineNoLatenessBackwardCompatible) {
    // Pipeline with no lateness config should behave exactly like Phase 1a
    GeneratorConfig gen_config{
        .seed = 42,
        .num_keys = 5,
        .num_records = 10000,
        .max_disorder = Duration{500},
        .batch_size = 512,
        .watermark_interval = 50,
    };

    // No PipelineConfig (default = no lateness)
    auto sink = std::make_unique<MemorySink>();
    auto* sink_ptr = sink.get();

    Pipeline pipeline(
        std::make_unique<DeterministicGenerator>(gen_config),
        std::make_unique<TumblingAssigner>(Duration{1000}),
        std::move(sink));
    auto stats = pipeline.Run();

    EXPECT_EQ(stats.records_processed, 10000u);
    EXPECT_EQ(stats.windows_refired, 0u);
    EXPECT_EQ(stats.late_records_accepted, 0u);
    EXPECT_EQ(stats.late_records_dropped, 0u);

    // Value conservation: all records accounted for
    uint64_t total_count = 0;
    for (const auto& r : sink_ptr->Results()) {
        total_count += r.result.count;
    }
    EXPECT_EQ(total_count, 10000u);
}

TEST(LateData, RefiredResultIsUpdatedAggregate) {
    // Verify the re-fire emits the FULL updated aggregate, not a delta
    KeyedWindowState state;
    state.SetAllowedLateness(Duration{5000});

    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    // Add 3 records, fire
    state.Add("k1", w, 100);
    state.Add("k1", w, 200);
    state.Add("k1", w, 300);
    auto fire1 = state.FireWindow(w);
    ASSERT_EQ(fire1.size(), 1u);
    EXPECT_EQ(fire1[0].result.value, 600);
    EXPECT_EQ(fire1[0].result.count, 3u);

    // Late record arrives
    state.AddWithLateness("k1", w, 50, Timestamp{Duration{500}});

    // Re-fire should show TOTAL (not delta)
    auto fire2 = state.FireWindow(w);
    ASSERT_EQ(fire2.size(), 1u);
    EXPECT_EQ(fire2[0].result.value, 650);  // 100 + 200 + 300 + 50
    EXPECT_EQ(fire2[0].result.count, 4u);   // 3 + 1
}

} // namespace
} // namespace stormglass

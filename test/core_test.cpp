#include <gtest/gtest.h>
#include "stream/record.h"
#include "stream/batch.h"
#include "stream/watermark.h"
#include "aggregate/sum.h"
#include "aggregate/count.h"
#include "sink/memory_sink.h"

namespace stormglass {
namespace {

TEST(Watermark, AdvancesForward) {
    WatermarkTracker wm;
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{100}}));
    EXPECT_EQ(wm.Current(), Timestamp{Duration{100}});
}

TEST(Watermark, RejectsBackward) {
    WatermarkTracker wm;
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{100}}));
    EXPECT_FALSE(wm.Advance(Timestamp{Duration{50}}));
    EXPECT_EQ(wm.Current(), Timestamp{Duration{100}});
}

TEST(Watermark, RejectsEqual) {
    WatermarkTracker wm;
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{100}}));
    EXPECT_FALSE(wm.Advance(Timestamp{Duration{100}}));
}

TEST(Watermark, MonotonicSequence) {
    WatermarkTracker wm;
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{100}}));
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{200}}));
    EXPECT_TRUE(wm.Advance(Timestamp{Duration{300}}));
    EXPECT_EQ(wm.Current(), Timestamp{Duration{300}});
}

TEST(SumKernel, BasicAccumulation) {
    SumInt64Kernel sum;
    sum.Add(10);
    sum.Add(20);
    sum.Add(30);
    auto r = sum.Result();
    EXPECT_EQ(r.value, 60);
    EXPECT_EQ(r.count, 3u);
}

TEST(SumKernel, Reset) {
    SumInt64Kernel sum;
    sum.Add(100);
    sum.Reset();
    EXPECT_EQ(sum.Result().value, 0);
    EXPECT_EQ(sum.Result().count, 0u);
}

TEST(SumKernel, BatchAdd) {
    SumInt64Kernel sum;
    std::vector<int64_t> values = {1, 2, 3, 4, 5};
    sum.AddBatch(values);
    auto r = sum.Result();
    EXPECT_EQ(r.value, 15);
    EXPECT_EQ(r.count, 5u);
}

TEST(CountKernel, BasicCount) {
    CountKernel count;
    count.Add(100);
    count.Add(200);
    count.Add(300);
    auto r = count.Result();
    EXPECT_EQ(r.value, 3);  // count kernel returns count as value
    EXPECT_EQ(r.count, 3u);
}

TEST(CountKernel, BatchCount) {
    CountKernel count;
    std::vector<int64_t> values = {1, 2, 3, 4, 5, 6, 7};
    count.AddBatch(values);
    auto r = count.Result();
    EXPECT_EQ(r.value, 7);
    EXPECT_EQ(r.count, 7u);
}

TEST(CountKernel, Reset) {
    CountKernel count;
    count.Add(1);
    count.Add(2);
    count.Reset();
    EXPECT_EQ(count.Result().value, 0);
    EXPECT_EQ(count.Result().count, 0u);
}

TEST(MemorySink, CollectsResults) {
    MemorySink s;
    WindowResult wr{
        .key = "k1",
        .window = {Timestamp{Duration{0}}, Timestamp{Duration{10000}}},
        .result = {100, 5},
    };
    s.Emit(wr);
    ASSERT_EQ(s.Results().size(), 1u);
    EXPECT_EQ(s.Results()[0].result.value, 100);
    EXPECT_EQ(s.Results()[0].result.count, 5u);
}

TEST(MemorySink, Clear) {
    MemorySink s;
    WindowResult wr{
        .key = "k1",
        .window = {Timestamp{Duration{0}}, Timestamp{Duration{1000}}},
        .result = {42, 1},
    };
    s.Emit(wr);
    s.Clear();
    EXPECT_TRUE(s.Results().empty());
}

TEST(Record, Construction) {
    Record r{
        .key = "test-key",
        .value = 42,
        .event_time = Timestamp{Duration{1000}},
        .processing_time = Timestamp{Duration{1001}},
    };
    EXPECT_EQ(r.key, "test-key");
    EXPECT_EQ(r.value, 42);
}

TEST(Batch, VariantHoldsRecordAndControl) {
    Batch b;
    b.items.emplace_back(Record{.key = "k", .value = 1,
                                .event_time = Timestamp{Duration{0}},
                                .processing_time = Timestamp{Duration{0}}});
    b.items.emplace_back(ControlRecord{.type = ControlType::kWatermark,
                                       .watermark = Timestamp{Duration{100}},
                                       .checkpoint_offset = 0});
    EXPECT_EQ(b.size(), 2u);
    EXPECT_FALSE(b.empty());
    EXPECT_TRUE(std::holds_alternative<Record>(b.items[0]));
    EXPECT_TRUE(std::holds_alternative<ControlRecord>(b.items[1]));
}

} // namespace
} // namespace stormglass

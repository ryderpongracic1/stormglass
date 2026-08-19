#include <gtest/gtest.h>
#include "window/tumbling.h"

namespace stormglass {
namespace {

TEST(TumblingAssigner, BasicBoundary) {
    TumblingAssigner assigner(Duration{1000});

    // 999ms → [0, 1000)
    auto w = assigner.AssignWindows(Timestamp{Duration{999}});
    ASSERT_EQ(w.size(), 1u);
    EXPECT_EQ(w[0].start, Timestamp{Duration{0}});
    EXPECT_EQ(w[0].end, Timestamp{Duration{1000}});
}

TEST(TumblingAssigner, ExactBoundary) {
    TumblingAssigner assigner(Duration{1000});

    // 1000ms → [1000, 2000)
    auto w = assigner.AssignWindows(Timestamp{Duration{1000}});
    ASSERT_EQ(w.size(), 1u);
    EXPECT_EQ(w[0].start, Timestamp{Duration{1000}});
    EXPECT_EQ(w[0].end, Timestamp{Duration{2000}});
}

TEST(TumblingAssigner, ZeroTimestamp) {
    TumblingAssigner assigner(Duration{1000});

    // 0ms → [0, 1000)
    auto w = assigner.AssignWindows(Timestamp{Duration{0}});
    ASSERT_EQ(w.size(), 1u);
    EXPECT_EQ(w[0].start, Timestamp{Duration{0}});
    EXPECT_EQ(w[0].end, Timestamp{Duration{1000}});
}

TEST(TumblingAssigner, Consistency) {
    TumblingAssigner assigner(Duration{5000});

    // Same event_time always maps to the same window
    auto w1 = assigner.AssignWindows(Timestamp{Duration{7500}});
    auto w2 = assigner.AssignWindows(Timestamp{Duration{7500}});
    ASSERT_EQ(w1.size(), 1u);
    ASSERT_EQ(w2.size(), 1u);
    EXPECT_EQ(w1[0], w2[0]);
    EXPECT_EQ(w1[0].start, Timestamp{Duration{5000}});
    EXPECT_EQ(w1[0].end, Timestamp{Duration{10000}});
}

TEST(TumblingAssigner, SingleWindowPerRecord) {
    TumblingAssigner assigner(Duration{1000});

    // Tumbling windows always assign exactly one window
    for (int64_t ms = 0; ms < 10000; ms += 137) {
        auto w = assigner.AssignWindows(Timestamp{Duration{ms}});
        ASSERT_EQ(w.size(), 1u) << "at ms=" << ms;
    }
}

TEST(TumblingAssigner, LargeTimestamp) {
    TumblingAssigner assigner(Duration{10000});

    auto w = assigner.AssignWindows(Timestamp{Duration{95000}});
    ASSERT_EQ(w.size(), 1u);
    EXPECT_EQ(w[0].start, Timestamp{Duration{90000}});
    EXPECT_EQ(w[0].end, Timestamp{Duration{100000}});
}

} // namespace
} // namespace stormglass

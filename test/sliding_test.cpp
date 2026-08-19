#include <gtest/gtest.h>
#include "window/sliding.h"

#include <algorithm>
#include <unordered_set>

namespace stormglass {
namespace {

TEST(SlidingAssigner, BasicTwoWindows) {
    // 10s window / 5s slide: record at 7000ms belongs to [5000, 15000) and [0, 10000)
    SlidingAssigner assigner(Duration{10000}, Duration{5000});

    auto windows = assigner.AssignWindows(Timestamp{Duration{7000}});
    ASSERT_EQ(windows.size(), 2u);

    std::unordered_set<Window, WindowHash> win_set(windows.begin(), windows.end());
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{0}}, Timestamp{Duration{10000}}}));
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{5000}}, Timestamp{Duration{15000}}}));
}

TEST(SlidingAssigner, FiveWindowsPerRecord) {
    // 10s window / 2s slide: in steady state (T >> window_size), a record
    // belongs to 5 windows (ceil(10000/2000) = 5)
    // At T=15000: starts in (5000, 15000] aligned to 2000 → 6000, 8000, 10000, 12000, 14000 = 5
    SlidingAssigner assigner(Duration{10000}, Duration{2000});

    auto windows = assigner.AssignWindows(Timestamp{Duration{15000}});
    ASSERT_EQ(windows.size(), 5u);

    // Check all windows contain event_time=15000
    for (const auto& w : windows) {
        EXPECT_LE(w.start.time_since_epoch().count(), 15000) << "start=" << w.start.time_since_epoch().count();
        EXPECT_GT(w.end.time_since_epoch().count(), 15000) << "end=" << w.end.time_since_epoch().count();
    }
}

TEST(SlidingAssigner, SlideEqualsWindowIsTumbling) {
    // 1s window / 1s slide = equivalent to tumbling (exactly 1 window per record)
    SlidingAssigner assigner(Duration{1000}, Duration{1000});

    for (int64_t ms = 0; ms < 10000; ms += 137) {
        auto windows = assigner.AssignWindows(Timestamp{Duration{ms}});
        ASSERT_EQ(windows.size(), 1u) << "at ms=" << ms;

        // Verify it matches tumbling behavior: window start is floor(ms/1000)*1000
        auto expected_start = (ms / 1000) * 1000;
        EXPECT_EQ(windows[0].start, Timestamp{Duration{expected_start}}) << "at ms=" << ms;
        EXPECT_EQ(windows[0].end, Timestamp{Duration{expected_start + 1000}}) << "at ms=" << ms;
    }
}

TEST(SlidingAssigner, WindowBoundariesAreSlideAligned) {
    // All window starts must be multiples of slide
    SlidingAssigner assigner(Duration{6000}, Duration{3000});

    for (int64_t ms = 0; ms < 20000; ms += 500) {
        auto windows = assigner.AssignWindows(Timestamp{Duration{ms}});
        for (const auto& w : windows) {
            EXPECT_EQ(w.start.time_since_epoch().count() % 3000, 0)
                << "Window start not aligned: " << w.start.time_since_epoch().count()
                << " at event_time=" << ms;
        }
    }
}

TEST(SlidingAssigner, ZeroTimestamp) {
    SlidingAssigner assigner(Duration{10000}, Duration{5000});

    auto windows = assigner.AssignWindows(Timestamp{Duration{0}});
    ASSERT_EQ(windows.size(), 1u);
    EXPECT_EQ(windows[0].start, Timestamp{Duration{0}});
    EXPECT_EQ(windows[0].end, Timestamp{Duration{10000}});
}

TEST(SlidingAssigner, ExactOnSlideBoundary) {
    // Event at exactly a slide boundary
    SlidingAssigner assigner(Duration{10000}, Duration{5000});

    auto windows = assigner.AssignWindows(Timestamp{Duration{5000}});
    ASSERT_EQ(windows.size(), 2u);

    std::unordered_set<Window, WindowHash> win_set(windows.begin(), windows.end());
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{0}}, Timestamp{Duration{10000}}}));
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{5000}}, Timestamp{Duration{15000}}}));
}

TEST(SlidingAssigner, LargeTimestamp) {
    SlidingAssigner assigner(Duration{10000}, Duration{5000});

    auto windows = assigner.AssignWindows(Timestamp{Duration{95000}});
    ASSERT_EQ(windows.size(), 2u);

    std::unordered_set<Window, WindowHash> win_set(windows.begin(), windows.end());
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{90000}}, Timestamp{Duration{100000}}}));
    EXPECT_TRUE(win_set.count(Window{Timestamp{Duration{95000}}, Timestamp{Duration{105000}}}));
}

TEST(SlidingAssigner, ConsistentWindowCount) {
    // For 10s window / 5s slide in steady state: exactly 2 windows per record
    // (window_size / slide = 10000/5000 = 2, evenly divisible)
    SlidingAssigner assigner(Duration{10000}, Duration{5000});

    for (int64_t ms = 10000; ms < 50000; ms += 1000) {
        auto windows = assigner.AssignWindows(Timestamp{Duration{ms}});
        EXPECT_EQ(windows.size(), 2u) << "at ms=" << ms;
    }
}

TEST(SlidingAssigner, AllWindowsContainEventTime) {
    SlidingAssigner assigner(Duration{8000}, Duration{3000});

    for (int64_t ms = 0; ms < 30000; ms += 777) {
        auto windows = assigner.AssignWindows(Timestamp{Duration{ms}});
        for (const auto& w : windows) {
            auto start_ms = w.start.time_since_epoch().count();
            auto end_ms = w.end.time_since_epoch().count();
            EXPECT_LE(start_ms, ms) << "at event_time=" << ms;
            EXPECT_GT(end_ms, ms) << "at event_time=" << ms;
        }
    }
}

} // namespace
} // namespace stormglass

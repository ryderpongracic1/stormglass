#include <gtest/gtest.h>
#include "window/state.h"

#include <algorithm>
#include <unordered_set>

namespace stormglass {
namespace {

TEST(KeyedWindowState, AddAndFire) {
    KeyedWindowState state;
    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 10);
    state.Add("k1", w, 20);
    state.Add("k2", w, 30);

    auto results = state.FireWindow(w);
    ASSERT_EQ(results.size(), 2u);

    // Find k1 and k2 results
    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.key < b.key; });

    EXPECT_EQ(results[0].key, "k1");
    EXPECT_EQ(results[0].result.value, 30);  // 10 + 20
    EXPECT_EQ(results[0].result.count, 2u);

    EXPECT_EQ(results[1].key, "k2");
    EXPECT_EQ(results[1].result.value, 30);
    EXPECT_EQ(results[1].result.count, 1u);
}

TEST(KeyedWindowState, ExpiredWindows) {
    KeyedWindowState state;
    Window w1{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};
    Window w2{Timestamp{Duration{1000}}, Timestamp{Duration{2000}}};
    Window w3{Timestamp{Duration{2000}}, Timestamp{Duration{3000}}};

    state.Add("k1", w1, 10);
    state.Add("k1", w2, 20);
    state.Add("k1", w3, 30);

    // Watermark at 2000 → w1 (end=1000) and w2 (end=2000) are expired
    auto expired = state.ExpiredWindows(Timestamp{Duration{2000}});
    EXPECT_EQ(expired.size(), 2u);

    std::unordered_set<Window, WindowHash> expired_set(expired.begin(), expired.end());
    EXPECT_TRUE(expired_set.count(w1));
    EXPECT_TRUE(expired_set.count(w2));
    EXPECT_FALSE(expired_set.count(w3));
}

TEST(KeyedWindowState, FireWindowRemovesPane) {
    KeyedWindowState state;
    Window w{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};

    state.Add("k1", w, 42);
    auto results1 = state.FireWindow(w);
    ASSERT_EQ(results1.size(), 1u);
    EXPECT_EQ(results1[0].result.value, 42);

    // Second call returns empty — pane was removed
    auto results2 = state.FireWindow(w);
    EXPECT_TRUE(results2.empty());
}

TEST(KeyedWindowState, AllWindows) {
    KeyedWindowState state;
    Window w1{Timestamp{Duration{0}}, Timestamp{Duration{1000}}};
    Window w2{Timestamp{Duration{1000}}, Timestamp{Duration{2000}}};
    Window w3{Timestamp{Duration{2000}}, Timestamp{Duration{3000}}};

    state.Add("k1", w1, 10);
    state.Add("k2", w2, 20);
    state.Add("k1", w3, 30);
    state.Add("k2", w3, 40);

    auto all = state.AllWindows();
    EXPECT_EQ(all.size(), 3u);

    std::unordered_set<Window, WindowHash> window_set(all.begin(), all.end());
    EXPECT_TRUE(window_set.count(w1));
    EXPECT_TRUE(window_set.count(w2));
    EXPECT_TRUE(window_set.count(w3));
}

TEST(KeyedWindowState, MultipleKeysInWindow) {
    KeyedWindowState state;
    Window w{Timestamp{Duration{5000}}, Timestamp{Duration{6000}}};

    state.Add("a", w, 1);
    state.Add("b", w, 2);
    state.Add("c", w, 3);
    state.Add("a", w, 4);

    auto results = state.FireWindow(w);
    ASSERT_EQ(results.size(), 3u);

    std::sort(results.begin(), results.end(),
              [](const auto& a, const auto& b) { return a.key < b.key; });
    EXPECT_EQ(results[0].key, "a");
    EXPECT_EQ(results[0].result.value, 5);  // 1 + 4
    EXPECT_EQ(results[0].result.count, 2u);
    EXPECT_EQ(results[1].key, "b");
    EXPECT_EQ(results[1].result.value, 2);
    EXPECT_EQ(results[2].key, "c");
    EXPECT_EQ(results[2].result.value, 3);
}

} // namespace
} // namespace stormglass

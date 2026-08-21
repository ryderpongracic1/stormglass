#include <gtest/gtest.h>

#include "source/generator.h"
#include "source/source_merge.h"
#include "stream/record.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

namespace stormglass {
namespace {

// Convenience: build a Timestamp at `ms` since epoch.
Timestamp Ts(int64_t ms) { return Timestamp{Duration{ms}}; }
int64_t Ms(Timestamp t) { return t.time_since_epoch().count(); }

// ===========================================================================
// MinWatermarkCombiner — the pure min-combine machinery, tested directly with
// KNOWN per-source watermark trajectories. This is the crown-jewel invariant:
// the emitted merged watermark is exactly the running MIN across channels, it
// only advances, and a lagging channel holds it back.
// ===========================================================================

TEST(MinWatermarkCombiner, EmitsRunningMinAndOnlyOnAdvance) {
    MinWatermarkCombiner c(2);
    std::vector<int64_t> emitted;
    auto observe = [&](std::size_t i, int64_t ms) {
        if (auto m = c.Observe(i, Ts(ms))) emitted.push_back(Ms(*m));
    };

    // Source 1 has never reported => min is pinned at Timestamp::min(); source 0
    // advancing alone emits nothing.
    observe(0, 10);
    EXPECT_TRUE(emitted.empty());

    observe(1, 5);   // both reported: min = 5 -> emit 5
    observe(0, 20);  // min still 5 (source 1) -> no emit
    observe(1, 15);  // min = 15 -> emit 15
    observe(1, 15);  // unchanged -> no emit
    observe(0, 5);   // backwards on source 0 (already 20) -> ignored, no emit

    ASSERT_EQ(emitted.size(), 2u);
    EXPECT_EQ(emitted[0], 5);
    EXPECT_EQ(emitted[1], 15);
    EXPECT_EQ(Ms(c.Current()), 15);
}

TEST(MinWatermarkCombiner, LaggingSourceHoldsTheMinBack) {
    MinWatermarkCombiner c(2);
    std::vector<int64_t> emitted;
    auto observe = [&](std::size_t i, int64_t ms) {
        if (auto m = c.Observe(i, Ts(ms))) emitted.push_back(Ms(*m));
    };

    // Source 0 races far ahead; source 1 lags and then stops at 5.
    observe(0, 10);
    observe(0, 20);
    observe(0, 30);
    observe(0, 40);
    EXPECT_TRUE(emitted.empty()) << "source 1 has not reported; min must stay at min()";

    observe(1, 5);     // now min = 5
    observe(0, 100);   // source 0 leaps to 100; min still pinned at source 1's 5
    observe(0, 1000);

    ASSERT_EQ(emitted.size(), 1u);
    EXPECT_EQ(emitted[0], 5);
    EXPECT_EQ(Ms(c.Current()), 5)
        << "the merged watermark must not exceed the lagging source's 5";

    // The lagging source finally catches up; the min jumps to min(1000, 800).
    observe(1, 800);
    ASSERT_EQ(emitted.size(), 2u);
    EXPECT_EQ(emitted[1], 800);
}

// ===========================================================================
// SourceMerge — Seek round-trip, K=1 passthrough, and the end-to-end lag proof
// that the combiner is actually wired into the merged stream.
// ===========================================================================

// Drain a source fully into flat (data-record) and (emitted-watermark) vectors.
struct DrainResult {
    std::vector<Record> records;
    std::vector<int64_t> watermarks;  // emitted watermark values, in order
};
template <class Src>
DrainResult Drain(Src& s) {
    DrainResult out;
    while (auto batch = s.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                out.records.push_back(std::get<Record>(item));
            } else {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kWatermark) out.watermarks.push_back(Ms(c.watermark));
            }
        }
    }
    return out;
}

TEST(SourceMerge, SeekReproducesIdenticalMergedSequence) {
    SourceMergeConfig mc;
    mc.merged_batch_size = 256;
    for (uint32_t i = 0; i < 3; ++i) {
        GeneratorConfig g{.seed = 100 + i, .num_keys = 6, .num_records = 4000,
                          .event_time_step = static_cast<int64_t>(3 - i),
                          .max_disorder = Duration{200}, .batch_size = 512,
                          .watermark_interval = 50};
        mc.sources.push_back(g);
    }

    // Advance genA by three merged batches; record the merged offset.
    SourceMerge genA(mc);
    genA.Next();
    genA.Next();
    genA.Next();
    const uint64_t offset = genA.CurrentOffset();
    ASSERT_GT(offset, 0u);

    // Seek a fresh merge to that offset; the tails must be byte-identical.
    SourceMerge genB(mc);
    genB.Seek(offset);
    EXPECT_EQ(genB.CurrentOffset(), offset);

    for (int b = 0; b < 3; ++b) {
        auto a = genA.Next();
        auto bb = genB.Next();
        ASSERT_EQ(a.has_value(), bb.has_value()) << "batch " << b;
        if (!a.has_value()) break;
        ASSERT_EQ(a->items.size(), bb->items.size()) << "batch " << b;
        for (size_t i = 0; i < a->items.size(); ++i) {
            ASSERT_EQ(a->items[i].index(), bb->items[i].index());
            if (std::holds_alternative<Record>(a->items[i])) {
                const auto& ra = std::get<Record>(a->items[i]);
                const auto& rb = std::get<Record>(bb->items[i]);
                EXPECT_EQ(ra.key, rb.key);
                EXPECT_EQ(ra.value, rb.value);
                EXPECT_EQ(ra.event_time, rb.event_time);
            } else {
                const auto& ca = std::get<ControlRecord>(a->items[i]);
                const auto& cb = std::get<ControlRecord>(bb->items[i]);
                EXPECT_EQ(ca.type, cb.type);
                EXPECT_EQ(ca.watermark, cb.watermark);
                EXPECT_EQ(ca.checkpoint_offset, cb.checkpoint_offset);
            }
        }
    }
}

TEST(SourceMerge, KOneIsPassthroughOfBareGenerator) {
    GeneratorConfig g{.seed = 7, .num_keys = 10, .num_records = 5000,
                      .max_disorder = Duration{500}, .batch_size = 512,
                      .watermark_interval = 100};

    SourceMergeConfig mc;
    mc.merged_batch_size = 512;
    mc.sources.push_back(g);

    SourceMerge merge(mc);
    DeterministicGenerator bare(g);

    auto m = Drain(merge);
    auto b = Drain(bare);

    // Data records must match exactly (order, key, value, event time).
    ASSERT_EQ(m.records.size(), b.records.size());
    for (size_t i = 0; i < m.records.size(); ++i) {
        EXPECT_EQ(m.records[i].key, b.records[i].key) << "record " << i;
        EXPECT_EQ(m.records[i].value, b.records[i].value) << "record " << i;
        EXPECT_EQ(m.records[i].event_time, b.records[i].event_time) << "record " << i;
    }

    // SourceMerge suppresses non-advancing watermarks, so its emitted watermark
    // sequence equals the STRICTLY-INCREASING subsequence of the generator's.
    std::vector<int64_t> bare_advancing;
    int64_t last = Ms(Timestamp::min());
    for (int64_t w : b.watermarks) {
        if (w > last) { bare_advancing.push_back(w); last = w; }
    }
    ASSERT_EQ(m.watermarks.size(), bare_advancing.size());
    for (size_t i = 0; i < m.watermarks.size(); ++i) {
        EXPECT_EQ(m.watermarks[i], bare_advancing[i]) << "watermark " << i;
    }
}

// Run a single generator alone and return its final (max) emitted watermark.
int64_t MaxEmittedWatermark(const GeneratorConfig& g) {
    DeterministicGenerator gen(g);
    auto d = Drain(gen);
    int64_t mx = Ms(Timestamp::min());
    for (int64_t w : d.watermarks) mx = std::max(mx, w);
    return mx;
}

TEST(SourceMerge, SlowGeneratorHoldsBackTheMergedWatermarkEndToEnd) {
    // Source 0 is FAST (event_time_step 5), source 1 is SLOW (step 1). Same
    // record count, so source 1's watermark trajectory tops out far below
    // source 0's — and the merged (min) watermark must be gated by source 1.
    GeneratorConfig fast{.seed = 1, .num_keys = 8, .num_records = 3000,
                         .event_time_step = 5, .max_disorder = Duration{200},
                         .batch_size = 512, .watermark_interval = 50};
    GeneratorConfig slow{.seed = 2, .num_keys = 8, .num_records = 3000,
                         .event_time_step = 1, .max_disorder = Duration{200},
                         .batch_size = 512, .watermark_interval = 50};

    const int64_t fast_max = MaxEmittedWatermark(fast);
    const int64_t slow_max = MaxEmittedWatermark(slow);
    // Non-vacuity: the fast source alone WOULD drive the watermark much higher.
    ASSERT_GT(fast_max, slow_max);

    SourceMergeConfig mc;
    mc.merged_batch_size = 512;
    mc.sources = {fast, slow};

    SourceMerge merge(mc);
    auto d = Drain(merge);

    ASSERT_FALSE(d.watermarks.empty());
    // Emitted merged watermarks are monotonically non-decreasing (running min).
    for (size_t i = 1; i < d.watermarks.size(); ++i) {
        EXPECT_GE(d.watermarks[i], d.watermarks[i - 1]) << "watermark " << i;
    }
    int64_t merged_max = d.watermarks.back();
    // The slow source holds the min back: the merged watermark never exceeds the
    // slow source's max, and stays strictly below what the fast source alone
    // would have produced.
    EXPECT_LE(merged_max, slow_max);
    EXPECT_LT(merged_max, fast_max);
    EXPECT_EQ(Ms(merge.CurrentWatermark()), merged_max);
}

}  // namespace
}  // namespace stormglass

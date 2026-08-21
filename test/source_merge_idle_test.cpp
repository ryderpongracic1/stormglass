#include <gtest/gtest.h>

#include "source/generator.h"
#include "source/source_merge.h"
#include "stream/record.h"

#include <algorithm>
#include <cstdint>
#include <optional>
#include <variant>
#include <vector>

// v3 Phase 2 — idleness.
//
// The HARD problem: with min-combine, a source that goes quiet pins the merged
// watermark at its stale value and stalls all downstream firing forever. A real
// engine detects idle sources and excludes them from the MIN so event-time
// progresses. The trap (Flink had bugs here for years): when an idle source
// RESUMES, the merged watermark has advanced without it, so its new records are
// now LATE relative to the advanced watermark.
//
// This file proves both directly:
//   * MinWatermarkCombiner idle/resume — the pure machinery, with KNOWN inputs.
//   * SourceMerge idle-exclusion end-to-end — a stalled slow source is excluded
//     and the merged watermark advances (driven by the active source), monotone.
//   * SourceMerge resume end-to-end — the resumed source's below-watermark
//     records ARE emitted (so downstream can classify them late) and the merged
//     watermark NEVER regresses.

namespace stormglass {
namespace {

Timestamp Ts(int64_t ms) { return Timestamp{Duration{ms}}; }
int64_t Ms(Timestamp t) { return t.time_since_epoch().count(); }

// ===========================================================================
// MinWatermarkCombiner — idle exclusion + resume, tested with KNOWN trajectories.
// ===========================================================================

TEST(MinWatermarkCombinerIdle, ExcludingIdleSourceAdvancesTheMin) {
    MinWatermarkCombiner c(2);
    std::vector<int64_t> emitted;
    auto observe = [&](std::size_t i, int64_t ms) {
        if (auto m = c.Observe(i, Ts(ms))) emitted.push_back(Ms(*m));
    };
    auto mark_idle = [&](std::size_t i) {
        if (auto m = c.MarkIdle(i)) emitted.push_back(Ms(*m));
    };
    auto mark_active = [&](std::size_t i) {
        if (auto m = c.MarkActive(i)) emitted.push_back(Ms(*m));
    };

    observe(0, 10);
    observe(1, 5);    // min = 5 -> emit 5
    observe(0, 20);   // min still 5 (source 1) -> no emit
    EXPECT_EQ(Ms(c.Current()), 5);

    // Source 1 goes idle: excluded from the MIN, which now advances to source 0.
    mark_idle(1);     // min over {0} = 20 -> emit 20
    EXPECT_EQ(Ms(c.Current()), 20);

    // Source 1 resumes at its STALE watermark (5). It rejoins the MIN but the
    // merged watermark must NOT regress (monotonic-clamped): min(20, 5) = 5 < 20.
    mark_active(1);   // no emit
    EXPECT_EQ(Ms(c.Current()), 20) << "resume must not regress the merged watermark";

    observe(1, 50);   // min(20, 50) = 20 -> no emit (already there)
    observe(0, 60);   // min(60, 50) = 50 -> emit 50

    ASSERT_EQ(emitted.size(), 3u);
    EXPECT_EQ(emitted[0], 5);
    EXPECT_EQ(emitted[1], 20);
    EXPECT_EQ(emitted[2], 50);
}

TEST(MinWatermarkCombinerIdle, AllIdleHoldsSteadyAndNeverRegresses) {
    MinWatermarkCombiner c(2);
    ASSERT_TRUE(c.Observe(0, Ts(10)).has_value() == false);  // source 1 unreported
    auto e = c.Observe(1, Ts(20));                           // min = 10 -> emit 10
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ(Ms(*e), 10);

    auto e2 = c.MarkIdle(0);   // exclude the min-holder -> min over {1} = 20
    ASSERT_TRUE(e2.has_value());
    EXPECT_EQ(Ms(*e2), 20);

    // Now exclude EVERYONE: with no active channel the merged watermark holds
    // steady at its last value — it must never regress or reset.
    auto e3 = c.MarkIdle(1);
    EXPECT_FALSE(e3.has_value());
    EXPECT_EQ(Ms(c.Current()), 20);
    EXPECT_FALSE(c.IsActive(0));
    EXPECT_FALSE(c.IsActive(1));
}

// ===========================================================================
// SourceMerge — idle-exclusion end-to-end. A slow source stalls the MIN; the
// idle policy excludes it so the fast source drives the watermark far past it.
// ===========================================================================

struct DrainResult {
    std::vector<Record> records;      // data records, in merged order
    std::vector<int64_t> watermarks;  // emitted merged watermark values, in order
    std::vector<BatchItem> stream;    // the full merged item stream, in order
};

DrainResult Drain(SourceMerge& s) {
    DrainResult out;
    while (auto batch = s.Next()) {
        for (const auto& item : batch->items) {
            out.stream.push_back(item);
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

// The most a standalone generator's emitted watermark ever reaches — the ceiling
// on what it could pin the MIN to while it is active.
int64_t MaxEmittedWatermark(const GeneratorConfig& g) {
    DeterministicGenerator gen(g);
    int64_t mx = Ms(Timestamp::min());
    while (auto batch = gen.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<ControlRecord>(item)) {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kWatermark) mx = std::max(mx, Ms(c.watermark));
            }
        }
    }
    return mx;
}

// Build the two-source config: A fast+active throughout, B slow with a single
// idle span. `idle_timeout` 0 disables exclusion (B just stalls the MIN).
SourceMergeConfig MakeIdleConfig(uint32_t idle_timeout) {
    GeneratorConfig a{.seed = 1, .num_keys = 8, .num_records = 3000,
                      .event_time_step = 4, .max_disorder = Duration{200},
                      .batch_size = 512, .watermark_interval = 50};
    GeneratorConfig b{.seed = 2, .num_keys = 8, .num_records = 3000,
                      .event_time_step = 1, .max_disorder = Duration{200},
                      .batch_size = 512, .watermark_interval = 50};
    // B goes quiet at its 400th record for 1500 turns, then resumes for the rest.
    b.idle_spans.push_back(IdleSpan{/*start_offset=*/400, /*length=*/1500});

    SourceMergeConfig mc;
    mc.merged_batch_size = 512;
    mc.idle_timeout = idle_timeout;
    mc.sources = {a, b};
    return mc;
}

TEST(SourceMergeIdle, ExcludedIdleSourceLetsWatermarkAdvancePastStalledSource) {
    // The most the slow source B could EVER pin the MIN to (its whole trajectory).
    GeneratorConfig b_full{.seed = 2, .num_keys = 8, .num_records = 3000,
                           .event_time_step = 1, .max_disorder = Duration{200},
                           .batch_size = 512, .watermark_interval = 50};
    const int64_t b_ceiling = MaxEmittedWatermark(b_full);

    auto run = [](uint32_t idle_timeout) {
        SourceMerge m(MakeIdleConfig(idle_timeout));
        return Drain(m);
    };

    // WITHOUT exclusion: B is never excluded, so the merged watermark = MIN(A, B)
    // is pinned by the slow source and can never exceed B's ceiling.
    auto without = run(/*idle_timeout=*/0);
    ASSERT_FALSE(without.watermarks.empty());
    for (size_t i = 1; i < without.watermarks.size(); ++i) {
        EXPECT_GE(without.watermarks[i], without.watermarks[i - 1]) << "monotonic (no-exclusion)";
    }
    const int64_t without_max =
        *std::max_element(without.watermarks.begin(), without.watermarks.end());
    EXPECT_LE(without_max, b_ceiling)
        << "without exclusion the slow source must pin the merged watermark";

    // WITH exclusion (idle_timeout=30): after 30 empty pulls B is dropped from the
    // MIN and the FAST source A drives the merged watermark far past B's ceiling.
    auto with = run(/*idle_timeout=*/30);
    ASSERT_FALSE(with.watermarks.empty());
    for (size_t i = 1; i < with.watermarks.size(); ++i) {
        EXPECT_GE(with.watermarks[i], with.watermarks[i - 1]) << "monotonic (with-exclusion)";
    }
    const int64_t with_max =
        *std::max_element(with.watermarks.begin(), with.watermarks.end());

    EXPECT_GT(with_max, b_ceiling)
        << "idle exclusion must let the merged watermark advance past everything "
           "the stalled slow source could pin (with_max=" << with_max
        << ", b_ceiling=" << b_ceiling << ")";
    EXPECT_GT(with_max, without_max)
        << "exclusion advances the watermark strictly further than no-exclusion";
}

// ===========================================================================
// SourceMerge — resume. The idle source rejoins with event-times below the
// advanced merged watermark; those records are emitted (late downstream) and the
// merged watermark never regresses.
// ===========================================================================

TEST(SourceMergeIdle, ResumedSourceEmitsBelowWatermarkRecordsAndWatermarkNeverRegresses) {
    SourceMerge m(MakeIdleConfig(/*idle_timeout=*/30));
    auto d = Drain(m);

    ASSERT_FALSE(d.watermarks.empty());
    // (a) The merged watermark NEVER regresses across the whole run — including
    // the resume, where the slow source rejoins the MIN at a stale value.
    for (size_t i = 1; i < d.watermarks.size(); ++i) {
        EXPECT_GE(d.watermarks[i], d.watermarks[i - 1])
            << "merged watermark regressed at index " << i;
    }

    // (b) Walk the merged stream in order, tracking the running merged watermark,
    // and count data records that appear BELOW it. Source A (fast, bounded) never
    // emits below its own watermark, and after exclusion the merged watermark is
    // driven by A — so every below-watermark data record is a resumed record from
    // the slow source B. A nonzero count proves the resumed-late records are
    // genuinely emitted (so the downstream lateness policy can classify them).
    int64_t running_wm = Ms(Timestamp::min());
    uint64_t below_wm_records = 0;
    for (const auto& item : d.stream) {
        if (std::holds_alternative<ControlRecord>(item)) {
            const auto& c = std::get<ControlRecord>(item);
            if (c.type == ControlType::kWatermark) {
                EXPECT_GE(Ms(c.watermark), running_wm) << "stream watermark regressed";
                running_wm = Ms(c.watermark);
            }
        } else {
            const auto& r = std::get<Record>(item);
            if (running_wm > Ms(Timestamp::min()) && Ms(r.event_time) < running_wm) {
                ++below_wm_records;
            }
        }
    }

    EXPECT_GT(below_wm_records, 0u)
        << "resume must emit below-watermark (late) records for downstream to classify";
    // The source did get excluded and then resumed (idle flag cleared by end).
    EXPECT_FALSE(m.IsSourceIdle(1)) << "slow source should have resumed by end of stream";
}

}  // namespace
}  // namespace stormglass

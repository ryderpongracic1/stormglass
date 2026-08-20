#include <gtest/gtest.h>

#include "oracle/differential.h"
#include "source/generator.h"

#include <variant>

namespace stormglass {
namespace {

// Count records whose event_time is strictly below the watermark in effect when
// they arrive (the last watermark emitted before the record). These are the
// genuinely-late records that exercise the drop / re-fire paths. Watermark
// control records are emitted in-band after their interval's records, mirroring
// what the engine sees.
uint64_t CountGenuinelyLate(const GeneratorConfig& config) {
    DeterministicGenerator gen(config);
    Timestamp watermark = Timestamp::min();
    uint64_t late = 0;
    while (auto batch = gen.Next()) {
        for (const auto& item : batch->items) {
            if (std::holds_alternative<Record>(item)) {
                const auto& r = std::get<Record>(item);
                if (watermark > Timestamp::min() && r.event_time < watermark) {
                    ++late;
                }
            } else {
                const auto& c = std::get<ControlRecord>(item);
                if (c.type == ControlType::kWatermark && c.watermark > watermark) {
                    watermark = c.watermark;
                }
            }
        }
    }
    return late;
}

GeneratorConfig HeavyTailedConfig(uint64_t seed) {
    return GeneratorConfig{
        .seed = seed,
        .num_keys = 10,
        .num_records = 20000,
        .max_disorder = Duration{500},
        .batch_size = 1024,
        .watermark_interval = 100,
        .disorder_mode = DisorderMode::kHeavyTailed,
        .late_fraction = 0.1,
        .late_tail = Duration{6000},
    };
}

// GUARD: the bounded workload must produce zero genuinely-late records. This is
// the exact "testing nothing" condition the original harness silently relied on.
TEST(HeavyTailedGuard, BoundedModeProducesNoLateRecords) {
    GeneratorConfig config{
        .seed = 7,
        .num_keys = 10,
        .num_records = 20000,
        .max_disorder = Duration{500},
        .batch_size = 1024,
        .watermark_interval = 100,
    };
    EXPECT_EQ(CountGenuinelyLate(config), 0u)
        << "bounded disorder should never emit a record below the watermark";
}

// GUARD: heavy-tailed mode MUST produce genuinely-late records. If the generator
// ever regresses to a never-late workload, this fails.
TEST(HeavyTailedGuard, HeavyTailedModeProducesLateRecords) {
    auto late = CountGenuinelyLate(HeavyTailedConfig(7));
    EXPECT_GT(late, 0u)
        << "heavy-tailed disorder must emit records below the watermark";
}

// GUARD (critical): with allowed lateness > 0 on a heavy-tailed stream the
// engine must actually DROP beyond-deadline records AND re-include within-lateness
// ones, and the oracle must agree exactly. A regression to a never-late workload
// drives engine_late_dropped and engine_windows_refired to 0 and fails here.
TEST(HeavyTailedGuard, TumblingLatenessDropsAndReincludes) {
    DifferentialConfig config{
        .num_seeds = 5,
        .records_per_seed = 20000,
        .num_keys = 10,
        .window_size = Duration{1000},
        .max_disorder = Duration{500},
        .allowed_lateness = Duration{2000},
        .verbose = false,
        .assigner = AssignerType::kTumbling,
        .slide = Duration{500},
        .disorder_mode = DisorderMode::kHeavyTailed,
        .late_fraction = 0.1,
        .late_tail = Duration{6000},
        .seed_start = 1,
    };

    auto r = RunDifferential(config);

    // Engine and oracle agree on every window and on the drop count.
    EXPECT_EQ(r.seeds_failed, 0u) << "first failure: " << r.failure_detail;
    EXPECT_EQ(r.seeds_passed, r.seeds_tested);

    // Lateness is genuinely exercised.
    EXPECT_GT(r.engine_late_dropped, 0u)
        << "no late-drops: the workload is not exercising the drop path";
    EXPECT_GT(r.engine_windows_refired, 0u)
        << "no re-fires: within-lateness records are not being re-included";

    // Drop-count contract holds.
    EXPECT_EQ(r.engine_late_dropped, r.oracle_predicted_drops);
}

TEST(HeavyTailedGuard, SlidingLatenessDropsAndReincludes) {
    DifferentialConfig config{
        .num_seeds = 5,
        .records_per_seed = 20000,
        .num_keys = 10,
        .window_size = Duration{2000},
        .max_disorder = Duration{500},
        .allowed_lateness = Duration{2000},
        .verbose = false,
        .assigner = AssignerType::kSliding,
        .slide = Duration{1000},
        .disorder_mode = DisorderMode::kHeavyTailed,
        .late_fraction = 0.1,
        .late_tail = Duration{6000},
        .seed_start = 1,
    };

    auto r = RunDifferential(config);

    EXPECT_EQ(r.seeds_failed, 0u) << "first failure: " << r.failure_detail;
    EXPECT_EQ(r.seeds_passed, r.seeds_tested);
    EXPECT_GT(r.engine_late_dropped, 0u);
    EXPECT_GT(r.engine_windows_refired, 0u);
    EXPECT_EQ(r.engine_late_dropped, r.oracle_predicted_drops);
}

// With lateness == 0 on a heavy-tailed stream the engine cannot recover late
// data, so the oracle (which excludes beyond-deadline records) must still match
// the engine's deduped authoritative output — and the oracle must observe that
// records were indeed late.
TEST(HeavyTailedGuard, LatenessZeroHeavyTailedReconciles) {
    DifferentialConfig config{
        .num_seeds = 5,
        .records_per_seed = 20000,
        .num_keys = 10,
        .window_size = Duration{1000},
        .max_disorder = Duration{500},
        .allowed_lateness = Duration{0},
        .verbose = false,
        .assigner = AssignerType::kTumbling,
        .slide = Duration{500},
        .disorder_mode = DisorderMode::kHeavyTailed,
        .late_fraction = 0.1,
        .late_tail = Duration{6000},
        .seed_start = 1,
    };

    auto r = RunDifferential(config);

    EXPECT_EQ(r.seeds_failed, 0u) << "first failure: " << r.failure_detail;
    EXPECT_GT(r.oracle_predicted_drops, 0u)
        << "oracle should observe late records excluded at lateness=0";
}

// The repro contract: --seed-start selects a contiguous seed range.
TEST(HeavyTailedGuard, SeedStartSelectsContiguousRange) {
    DifferentialConfig config{
        .num_seeds = 3,
        .records_per_seed = 4000,
        .num_keys = 5,
        .window_size = Duration{1000},
        .max_disorder = Duration{500},
        .allowed_lateness = Duration{2000},
        .disorder_mode = DisorderMode::kHeavyTailed,
        .late_fraction = 0.1,
        .late_tail = Duration{6000},
        .seed_start = 42,
    };

    auto r = RunDifferential(config);
    EXPECT_EQ(r.seeds_tested, 3u);
    EXPECT_EQ(r.seeds_failed, 0u) << "first failure: " << r.failure_detail;
}

}  // namespace
}  // namespace stormglass

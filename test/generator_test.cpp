#include <gtest/gtest.h>
#include "source/generator.h"

namespace stormglass {
namespace {

TEST(Generator, Determinism) {
    GeneratorConfig config{.seed = 123, .num_keys = 5, .num_records = 3000,
                           .max_disorder = Duration{100}, .batch_size = 1024,
                           .watermark_interval = 50};

    DeterministicGenerator gen1(config);
    DeterministicGenerator gen2(config);

    // Compare first 3 batches byte-for-byte
    for (int batch_idx = 0; batch_idx < 3; ++batch_idx) {
        auto b1 = gen1.Next();
        auto b2 = gen2.Next();
        ASSERT_TRUE(b1.has_value());
        ASSERT_TRUE(b2.has_value());
        ASSERT_EQ(b1->items.size(), b2->items.size())
            << "Batch " << batch_idx << " size mismatch";

        for (size_t i = 0; i < b1->items.size(); ++i) {
            ASSERT_EQ(b1->items[i].index(), b2->items[i].index())
                << "Item " << i << " type mismatch in batch " << batch_idx;
            if (std::holds_alternative<Record>(b1->items[i])) {
                auto& r1 = std::get<Record>(b1->items[i]);
                auto& r2 = std::get<Record>(b2->items[i]);
                EXPECT_EQ(r1.key, r2.key);
                EXPECT_EQ(r1.value, r2.value);
                EXPECT_EQ(r1.event_time, r2.event_time);
            } else {
                auto& c1 = std::get<ControlRecord>(b1->items[i]);
                auto& c2 = std::get<ControlRecord>(b2->items[i]);
                EXPECT_EQ(c1.type, c2.type);
                EXPECT_EQ(c1.watermark, c2.watermark);
            }
        }
    }
}

TEST(Generator, OffsetTracking) {
    GeneratorConfig config{.seed = 42, .num_keys = 3, .num_records = 500,
                           .batch_size = 100, .watermark_interval = 50};
    DeterministicGenerator gen(config);

    EXPECT_EQ(gen.CurrentOffset(), 0u);
    gen.Next();  // 100 records
    EXPECT_EQ(gen.CurrentOffset(), 100u);
    gen.Next();  // 200
    EXPECT_EQ(gen.CurrentOffset(), 200u);
}

TEST(Generator, ExhaustsRecords) {
    GeneratorConfig config{.seed = 42, .num_keys = 2, .num_records = 250,
                           .batch_size = 100, .watermark_interval = 50};
    DeterministicGenerator gen(config);

    ASSERT_TRUE(gen.Next().has_value());   // 100
    ASSERT_TRUE(gen.Next().has_value());   // 200
    ASSERT_TRUE(gen.Next().has_value());   // 250 (partial batch)
    ASSERT_FALSE(gen.Next().has_value());  // exhausted

    EXPECT_EQ(gen.CurrentOffset(), 250u);
}

TEST(Generator, SeekAndResume) {
    GeneratorConfig config{.seed = 99, .num_keys = 4, .num_records = 1000,
                           .batch_size = 100, .watermark_interval = 50};

    // Generate 300 records normally
    DeterministicGenerator gen1(config);
    gen1.Next(); gen1.Next(); gen1.Next();  // offset = 300

    // Seek a fresh generator to 300, then compare next batch
    DeterministicGenerator gen2(config);
    gen2.Seek(300);
    EXPECT_EQ(gen2.CurrentOffset(), 300u);

    auto b1 = gen1.Next();
    auto b2 = gen2.Next();
    ASSERT_TRUE(b1.has_value());
    ASSERT_TRUE(b2.has_value());
    ASSERT_EQ(b1->items.size(), b2->items.size());

    for (size_t i = 0; i < b1->items.size(); ++i) {
        ASSERT_EQ(b1->items[i].index(), b2->items[i].index());
        if (std::holds_alternative<Record>(b1->items[i])) {
            auto& r1 = std::get<Record>(b1->items[i]);
            auto& r2 = std::get<Record>(b2->items[i]);
            EXPECT_EQ(r1.key, r2.key);
            EXPECT_EQ(r1.value, r2.value);
            EXPECT_EQ(r1.event_time, r2.event_time);
        }
    }
}

TEST(Generator, WatermarkInjection) {
    GeneratorConfig config{.seed = 42, .num_keys = 5, .num_records = 500,
                           .batch_size = 500, .watermark_interval = 100};
    DeterministicGenerator gen(config);

    auto batch = gen.Next();
    ASSERT_TRUE(batch.has_value());

    // Count watermarks — expect 5 watermarks for 500 records at interval 100
    int watermark_count = 0;
    for (auto& item : batch->items) {
        if (std::holds_alternative<ControlRecord>(item)) {
            auto& c = std::get<ControlRecord>(item);
            if (c.type == ControlType::kWatermark) {
                ++watermark_count;
            }
        }
    }
    EXPECT_EQ(watermark_count, 5);
}

TEST(Generator, KeyFormat) {
    GeneratorConfig config{.seed = 42, .num_keys = 10, .num_records = 10,
                           .batch_size = 10, .watermark_interval = 100};
    DeterministicGenerator gen(config);

    auto batch = gen.Next();
    ASSERT_TRUE(batch.has_value());

    // Check first 10 records have keys key-0000 through key-0009
    int record_idx = 0;
    for (auto& item : batch->items) {
        if (std::holds_alternative<Record>(item)) {
            auto& r = std::get<Record>(item);
            char expected[16];
            std::snprintf(expected, sizeof(expected), "key-%04d", record_idx % 10);
            EXPECT_EQ(r.key, std::string(expected))
                << "Record " << record_idx;
            ++record_idx;
        }
    }
}

TEST(Generator, ValueRange) {
    GeneratorConfig config{.seed = 42, .num_keys = 5, .num_records = 10000,
                           .batch_size = 10000, .watermark_interval = 1000};
    DeterministicGenerator gen(config);

    auto batch = gen.Next();
    ASSERT_TRUE(batch.has_value());

    for (auto& item : batch->items) {
        if (std::holds_alternative<Record>(item)) {
            auto& r = std::get<Record>(item);
            EXPECT_GE(r.value, 1);
            EXPECT_LE(r.value, 1000);
        }
    }
}

} // namespace
} // namespace stormglass

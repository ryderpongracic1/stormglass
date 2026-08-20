#include <gtest/gtest.h>

#include "engine/pipeline.h"
#include "source/generator.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "checkpoint/reader.h"

#include <filesystem>
#include <variant>

using namespace stormglass;

namespace {

class BarrierTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/stormglass_barrier_XXXXXX";
        char* result = ::mkdtemp(tmpl);
        ASSERT_NE(result, nullptr);
        dir_ = result;
    }
    void TearDown() override { std::filesystem::remove_all(dir_); }
    std::string dir_;
};

GeneratorConfig BaseConfig() {
    GeneratorConfig c;
    c.seed = 42;
    c.num_keys = 10;
    c.num_records = 20000;
    c.max_disorder = Duration{500};
    c.batch_size = 1024;
    c.watermark_interval = 100;
    return c;
}

} // namespace

// The source must actually EMIT kCheckpointBarrier control records, stamped
// with the absolute source offset, at the configured interval. If barrier
// emission regresses, this fails.
TEST(Barrier, SourceEmitsBarrierWithAbsoluteOffset) {
    auto config = BaseConfig();
    config.num_records = 12000;
    config.checkpoint_interval = 5000;

    DeterministicGenerator gen(config);
    std::vector<uint64_t> barrier_offsets;
    while (auto batch = gen.Next()) {
        for (const auto& item : batch->items) {
            if (auto* c = std::get_if<ControlRecord>(&item)) {
                if (c->type == ControlType::kCheckpointBarrier) {
                    barrier_offsets.push_back(c->checkpoint_offset);
                }
            }
        }
    }

    // Barriers at the absolute offsets 5000 and 10000 (12000 records, interval 5000).
    ASSERT_EQ(barrier_offsets.size(), 2u)
        << "source must emit one barrier per checkpoint interval";
    EXPECT_EQ(barrier_offsets[0], 5000u);
    EXPECT_EQ(barrier_offsets[1], 10000u);
}

// A generator with no checkpoint interval emits no barriers at all.
TEST(Barrier, NoIntervalNoBarrier) {
    auto config = BaseConfig();  // checkpoint_interval defaults to 0
    DeterministicGenerator gen(config);
    uint64_t barriers = 0;
    while (auto batch = gen.Next()) {
        for (const auto& item : batch->items) {
            if (auto* c = std::get_if<ControlRecord>(&item)) {
                if (c->type == ControlType::kCheckpointBarrier) ++barriers;
            }
        }
    }
    EXPECT_EQ(barriers, 0u);
}

// The pipeline must HANDLE the barrier by writing a checkpoint. With barriers
// enabled, checkpoints are produced and readable at barrier offsets.
TEST_F(BarrierTest, PipelineCheckpointsOnBarrier) {
    auto config = BaseConfig();
    config.checkpoint_interval = 5000;  // barriers at 5000, 10000, 15000, 20000

    PipelineConfig pconfig;
    pconfig.checkpoint_dir = dir_;
    pconfig.checkpoint_interval = 5000;

    Pipeline pipeline(std::make_unique<DeterministicGenerator>(config),
                      std::make_unique<TumblingAssigner>(Duration{1000}),
                      std::make_unique<MemorySink>(), pconfig);
    auto stats = pipeline.Run();

    // Four barriers => four checkpoint writes.
    EXPECT_EQ(stats.checkpoints_written, 4u)
        << "pipeline must snapshot once per barrier";

    // The latest durable checkpoint carries a barrier offset (a multiple of the
    // interval), proving the pipeline snapshotted at the barrier, not elsewhere.
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->offset % 5000u, 0u);
    EXPECT_GT(data->offset, 0u);
}

// THE GUARD FOR FINDING 3a: checkpoints are barrier-driven, not record-count
// driven. A source that emits no barriers must produce ZERO checkpoints even
// though the pipeline has checkpointing enabled. If anyone reintroduces the
// old inline records_since_checkpoint_ trigger, this fails.
TEST_F(BarrierTest, NoBarriersMeansNoCheckpoints) {
    auto config = BaseConfig();  // checkpoint_interval = 0 => no barriers

    PipelineConfig pconfig;
    pconfig.checkpoint_dir = dir_;
    pconfig.checkpoint_interval = 5000;  // pipeline is "enabled", but no barriers arrive

    Pipeline pipeline(std::make_unique<DeterministicGenerator>(config),
                      std::make_unique<TumblingAssigner>(Duration{1000}),
                      std::make_unique<MemorySink>(), pconfig);
    auto stats = pipeline.Run();

    EXPECT_EQ(stats.checkpoints_written, 0u)
        << "no barriers emitted, so no checkpoint may be written";

    CheckpointReader reader(dir_);
    EXPECT_FALSE(reader.LoadLatest().has_value());
}

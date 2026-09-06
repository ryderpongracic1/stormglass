#include <gtest/gtest.h>
#include "engine/keyed_processor.h"
#include "engine/partitioned_pipeline.h"
#include "engine/bounded_queue.h"
#include "checkpoint/writer.h"
#include "checkpoint/distributed_checkpoint.h"
#include "sink/memory_sink.h"
#include "sink/durable_file_sink.h"
#include "source/generator.h"
#include "window/tumbling.h"
#include "window/sliding.h"
#include <filesystem>
#include <stdexcept>
#include <unistd.h>
using namespace stormglass;
namespace {
struct HardeningTest : testing::Test {
    std::string dir;
    void SetUp() override { char name[]="/tmp/sg-hardening-XXXXXX"; dir=::mkdtemp(name); }
    void TearDown() override { std::filesystem::remove_all(dir); }
};
TEST_F(HardeningTest, PendingLateUpdateSurvivesCheckpointAndFinalFlush) {
    MemorySink before;
    KeyedProcessor first(std::make_unique<TumblingAssigner>(Duration{100}), before, Duration{100}, dir);
    first.ProcessRecord(Record{.key="k", .value=10, .event_time=Timestamp{Duration{5}}});
    first.ProcessControl(ControlRecord{.type=ControlType::kWatermark, .watermark=Timestamp{Duration{100}}});
    first.ProcessRecord(Record{.key="k", .value=7, .event_time=Timestamp{Duration{5}}});
    first.ProcessControl(ControlRecord{.type=ControlType::kCheckpointBarrier, .checkpoint_offset=2});
    auto snapshot=CheckpointReader(dir).LoadLatest(); ASSERT_TRUE(snapshot);
    MemorySink after;
    KeyedProcessor restored(std::make_unique<TumblingAssigner>(Duration{100}), after, Duration{100});
    restored.Restore(*snapshot); restored.FinalFlush();
    ASSERT_EQ(after.Results().size(),1u);
    EXPECT_EQ(after.Results()[0].result.value,17);
    EXPECT_EQ(after.Results()[0].result.count,2u);
}
TEST_F(HardeningTest, FastPartitionCannotPruneCommonCheckpoint) {
    auto p0=PartitionCheckpointDir(dir,0), p1=PartitionCheckpointDir(dir,1);
    std::filesystem::create_directories(p0); std::filesystem::create_directories(p1);
    KeyedWindowState state;
    ASSERT_TRUE(CheckpointWriter(p1,true).WriteCheckpoint(1,Timestamp{},state));
    for (uint64_t i=1;i<=5;++i) ASSERT_TRUE(CheckpointWriter(p0,true).WriteCheckpoint(i,Timestamp{},state));
    EXPECT_EQ(HighestCompleteCheckpoint(dir,2),1u);
}
struct FailingSource : Source {
    std::optional<Batch> Next() override { throw std::runtime_error("source failed"); }
    void Seek(uint64_t) override {}
    uint64_t CurrentOffset() const override { return 0; }
};
struct FailingSink : Sink {
    void Emit(const WindowResult&) override { throw std::runtime_error("sink failed"); }
    void Flush() override {}
};
TEST(Hardening, RouterExceptionReachesCaller) {
    PartitionedPipeline p(std::make_unique<FailingSource>(), [] { return std::make_unique<TumblingAssigner>(Duration{10}); }, std::make_unique<MemorySink>(), {.num_workers=4,.queue_capacity=1});
    EXPECT_THROW(p.Run(),std::runtime_error);
}
TEST(Hardening, WorkerExceptionCancelsBlockedRouter) {
    GeneratorConfig gen; gen.num_records=100000; gen.watermark_interval=1;
    PartitionedPipelineConfig config; config.num_workers=4; config.queue_capacity=1;
    config.worker_sink_factory=[](uint32_t) { return std::make_unique<FailingSink>(); };
    PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), [] { return std::make_unique<TumblingAssigner>(Duration{10}); }, std::make_unique<MemorySink>(), config);
    EXPECT_THROW(p.Run(),std::runtime_error);
}
TEST(Hardening, InvalidInputsFailBeforeProcessing) {
    EXPECT_THROW(BoundedQueue<int>(0),std::invalid_argument);
    EXPECT_THROW(TumblingAssigner(Duration{0}),std::invalid_argument);
    EXPECT_THROW(SlidingAssigner(Duration{1},Duration{0}),std::invalid_argument);
    EXPECT_THROW(DurableFileSink("/nonexistent-stormglass-dir/output"),std::system_error);
    TumblingAssigner t(Duration{10});
    EXPECT_THROW(t.AssignWindows(Timestamp{Duration{-1}}),std::out_of_range);
}
}
TEST(Hardening, AggregateOverflowIsReportedWithoutMutation) {
    Pane p{std::numeric_limits<int64_t>::max(),1};
    EXPECT_THROW(p.Add(1),std::overflow_error);
    EXPECT_EQ(p.sum,std::numeric_limits<int64_t>::max());
    EXPECT_EQ(p.count,1u);
}
#include "source/source_merge.h"
TEST(Hardening, ExhaustedChannelDoesNotPinWatermark) {
    GeneratorConfig short_source; short_source.num_records=1; short_source.max_disorder=Duration{0}; short_source.watermark_interval=1;
    auto long_source=short_source; long_source.num_records=10;
    SourceMerge merged({.sources={short_source,long_source},.merged_batch_size=1});
    uint64_t records=0;
    while (auto batch=merged.Next()) for (auto& item:batch->items) if (std::holds_alternative<Record>(item)) ++records;
    EXPECT_EQ(records,11u);
    EXPECT_EQ(merged.CurrentWatermark(),Timestamp{Duration{9}});
}
TEST(Hardening, ExhaustionUnblocksEarlierChannelWithoutFalseEndOfStream) {
    GeneratorConfig a; a.num_records=4; a.checkpoint_interval=1; a.batch_size=1;
    auto b=a; b.num_records=0;
    SourceMerge merged({.sources={a,b},.merged_batch_size=1});
    uint64_t records=0;
    while (auto batch=merged.Next()) for(auto& item:batch->items) if(std::holds_alternative<Record>(item)) ++records;
    EXPECT_EQ(records,4u);
}
#include "checkpoint/crc32c.h"
#include <fstream>
namespace {
std::vector<uint8_t> ReadBytes(const std::string& path) {
    std::ifstream in(path,std::ios::binary);
    return {std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};
}
void StoreLE(std::vector<uint8_t>& bytes, size_t pos, uint64_t value, size_t width) {
    for(size_t i=0;i<width;++i) bytes[pos+i]=static_cast<uint8_t>(value>>(8*i));
}
void SaveWithCrc(const std::string& path, std::vector<uint8_t> bytes) {
    StoreLE(bytes,bytes.size()-4,Crc32c(bytes.data(),bytes.size()-12),4);
    std::ofstream out(path,std::ios::binary|std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()),bytes.size());
}
TEST_F(HardeningTest, CrcValidImpossibleCountIsRejectedBeforeAllocation) {
    KeyedWindowState state;
    ASSERT_TRUE(CheckpointWriter(dir).WriteCheckpoint(1,Timestamp{},state));
    auto path=dir+"/checkpoint-00000000000000000001.ckpt";
    auto bytes=ReadBytes(path);
    StoreLE(bytes,24,std::numeric_limits<uint64_t>::max(),8);
    StoreLE(bytes,bytes.size()-12,std::numeric_limits<uint64_t>::max(),8);
    SaveWithCrc(path,bytes);
    EXPECT_FALSE(CheckpointReader(dir).LoadLatest());
}
TEST_F(HardeningTest, VersionTwoCheckpointRemainsReadable) {
    KeyedWindowState state;
    state.Add("k",Window{Timestamp{},Timestamp{Duration{10}}},3);
    ASSERT_TRUE(CheckpointWriter(dir).WriteCheckpoint(1,Timestamp{},state));
    auto path=dir+"/checkpoint-00000000000000000001.ckpt";
    auto bytes=ReadBytes(path);
    StoreLE(bytes,4,2,4);
    bytes.erase(bytes.end()-20,bytes.end()-12); // v3 empty pending-window count
    SaveWithCrc(path,bytes);
    auto snapshot=CheckpointReader(dir).LoadLatest(); ASSERT_TRUE(snapshot);
    ASSERT_EQ(snapshot->panes.size(),1u);
    EXPECT_EQ(snapshot->panes[0].sum,3);
}
}

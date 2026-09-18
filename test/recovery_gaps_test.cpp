#include <gtest/gtest.h>

#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/job_manifest.h"
#include "engine/partitioned_pipeline.h"
#include "engine/pipeline.h"
#include "sink/durable_file_sink.h"
#include "source/generator.h"
#include "source/source_merge.h"
#include "source/stopping_source.h"
#include "window/sliding.h"
#include "window/tumbling.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <fcntl.h>
#include <filesystem>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unistd.h>
#include <variant>
#include <vector>

using namespace stormglass;

namespace {

struct RecoveryGapsTest : ::testing::Test {
    std::string dir;
    void SetUp() override {
        char name[] = "/tmp/sg-recovery-gaps-XXXXXX";
        char* made = ::mkdtemp(name);
        ASSERT_NE(made, nullptr);
        dir = made;
    }
    void TearDown() override { std::filesystem::remove_all(dir); }
};

Timestamp Ms(int64_t ms) { return Timestamp{Duration{ms}}; }

// A replayable scripted source: one item per batch, Seek(O) resumes at the
// (O+1)-th data record exactly like DeterministicGenerator. With a non-empty
// fail_after_checkpoint_in, pulling past the script waits until that directory
// holds a complete checkpoint for `workers` partitions and then throws, so the
// failure deterministically lands after a completed checkpoint.
class ScriptedSource : public Source {
public:
    explicit ScriptedSource(std::vector<BatchItem> items,
                            std::string fail_after_checkpoint_in = "", uint32_t workers = 0)
        : items_(std::move(items)),
          fail_dir_(std::move(fail_after_checkpoint_in)),
          workers_(workers) {}

    std::optional<Batch> Next() override {
        if (cursor_ >= items_.size()) {
            if (fail_dir_.empty()) return std::nullopt;
            for (int i = 0; i < 5000 && !HighestCompleteCheckpoint(fail_dir_, workers_); ++i) {
                ::usleep(1000);
            }
            throw std::runtime_error("injected source failure");
        }
        Batch b;
        b.items.push_back(items_[cursor_]);
        if (std::holds_alternative<Record>(items_[cursor_])) ++offset_;
        ++cursor_;
        return b;
    }
    void Seek(uint64_t offset) override {
        cursor_ = 0;
        offset_ = 0;
        uint64_t records = 0;
        for (; cursor_ < items_.size(); ++cursor_) {
            if (std::holds_alternative<Record>(items_[cursor_])) {
                if (records == offset) break;
                ++records;
            }
        }
        offset_ = offset;
    }
    uint64_t CurrentOffset() const override { return offset_; }
    std::string Descriptor() const override { return "scripted"; }

private:
    std::vector<BatchItem> items_;
    std::string fail_dir_;
    uint32_t workers_;
    std::size_t cursor_ = 0;
    uint64_t offset_ = 0;
};

// Captures emitted results into storage the test still owns after the pipeline
// takes ownership of the sink.
class CaptureSink : public Sink {
public:
    explicit CaptureSink(std::vector<WindowResult>& out) : out_(out) {}
    void Emit(const WindowResult& r) override { out_.push_back(r); }
    void Flush() override {}

private:
    std::vector<WindowResult>& out_;
};

std::vector<BatchItem> CheckpointedScript() {
    return {
        Record{.key = "k", .value = 10, .event_time = Ms(5)},
        ControlRecord{.type = ControlType::kWatermark, .watermark = Ms(100)},
        ControlRecord{.type = ControlType::kCheckpointBarrier,
                      .watermark = Timestamp::min(), .checkpoint_offset = 1},
        Record{.key = "k", .value = 3, .event_time = Ms(150)},
    };
}

using ResultKey = std::tuple<int64_t, std::string, int64_t, uint64_t>;
std::vector<ResultKey> Sorted(const std::vector<WindowResult>& results) {
    std::vector<ResultKey> out;
    for (const auto& r : results) {
        out.emplace_back(r.window.start.time_since_epoch().count(), r.key,
                         r.result.value, r.result.count);
    }
    std::sort(out.begin(), out.end());
    out.erase(std::unique(out.begin(), out.end()), out.end());
    return out;
}

std::function<std::unique_ptr<WindowAssigner>()> Tumbling(int64_t ms) {
    return [ms] { return std::make_unique<TumblingAssigner>(Duration{ms}); };
}

WindowResult MakeResult(const std::string& key, int64_t start, int64_t end,
                        int64_t sum, uint64_t count) {
    return WindowResult{
        .key = key,
        .window = Window{Ms(start), Ms(end)},
        .result = AggregateResult{sum, count},
    };
}

} // namespace

// ---------------------------------------------------------------------------
// Gap 1: default partitioned output must reach the caller's sink before the
// checkpoint that marks its window fired, or a failure after that checkpoint
// loses the result permanently.
// ---------------------------------------------------------------------------
TEST_F(RecoveryGapsTest, PartitionedDefaultSinkPreservesOutputAcrossFailure) {
    std::vector<WindowResult> uninterrupted;
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript()),
                              Tumbling(100), std::make_unique<CaptureSink>(uninterrupted),
                              {.num_workers = 2});
        p.Run();
    }
    ASSERT_EQ(Sorted(uninterrupted).size(), 2u);

    std::vector<WindowResult> recovered;
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript(), dir, 2),
                              Tumbling(100), std::make_unique<CaptureSink>(recovered),
                              {.num_workers = 2, .checkpoint_dir = dir});
        EXPECT_THROW(p.Run(), std::runtime_error);
    }
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript()),
                              Tumbling(100), std::make_unique<CaptureSink>(recovered),
                              {.num_workers = 2, .checkpoint_dir = dir});
        auto stats = p.Run();
        EXPECT_EQ(stats.records_replayed, 1u);
    }
    EXPECT_EQ(Sorted(recovered), Sorted(uninterrupted));
}

// Output is forwarded while the job runs rather than buffered until the end.
TEST_F(RecoveryGapsTest, PartitionedDefaultSinkStreamsBeforeRunReturns) {
    struct CountingSink : Sink {
        std::atomic<std::size_t>& count;
        explicit CountingSink(std::atomic<std::size_t>& c) : count(c) {}
        void Emit(const WindowResult&) override { count.fetch_add(1); }
        void Flush() override {}
    };
    struct ProbeSource : Source {
        std::atomic<std::size_t>& count;
        std::size_t observed = 0;
        int calls = 0;
        explicit ProbeSource(std::atomic<std::size_t>& c) : count(c) {}
        std::optional<Batch> Next() override {
            if (calls++ == 0) {
                Batch b;
                b.items.emplace_back(Record{.key = "k", .value = 1, .event_time = Ms(5)});
                b.items.emplace_back(ControlRecord{.type = ControlType::kWatermark,
                                                   .watermark = Ms(100)});
                return b;
            }
            // Wait (bounded) for the worker to forward the fired window while
            // the stream is still open.
            for (int i = 0; i < 5000 && count.load() == 0; ++i) ::usleep(1000);
            observed = count.load();
            return std::nullopt;
        }
        void Seek(uint64_t) override {}
        uint64_t CurrentOffset() const override { return 0; }
    };
    std::atomic<std::size_t> count{0};
    auto source = std::make_unique<ProbeSource>(count);
    auto* probe = source.get();
    PartitionedPipeline p(std::move(source), Tumbling(100),
                          std::make_unique<CountingSink>(count), {.num_workers = 1});
    p.Run();
    EXPECT_EQ(probe->observed, 1u);
}

// ---------------------------------------------------------------------------
// Gap 2: DurableFileSink reopened on the same path must keep prior output and
// repair a torn tail before appending.
// ---------------------------------------------------------------------------
TEST_F(RecoveryGapsTest, DurableSinkAppendsAcrossReopen) {
    const std::string path = dir + "/out.bin";
    { DurableFileSink sink(path); sink.Emit(MakeResult("a", 0, 100, 1, 1)); }
    { DurableFileSink sink(path); sink.Emit(MakeResult("b", 0, 100, 2, 1)); }
    EXPECT_EQ(DurableFileSink::ReadAll(path).size(), 2u);

    { DurableFileSink sink(path, DurableFileSink::OpenMode::kTruncate); }
    EXPECT_TRUE(DurableFileSink::ReadAll(path).empty());
}

TEST_F(RecoveryGapsTest, DurableSinkRepairsTornTailBeforeAppend) {
    const std::string path = dir + "/out.bin";
    { DurableFileSink sink(path); sink.Emit(MakeResult("a", 0, 100, 1, 1)); }
    {
        int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
        ASSERT_GE(fd, 0);
        const uint8_t torn[] = {5, 0, 0, 0, 'x', 'y'};  // header + partial key
        ASSERT_EQ(::write(fd, torn, sizeof(torn)), static_cast<ssize_t>(sizeof(torn)));
        ::close(fd);
    }
    { DurableFileSink sink(path); sink.Emit(MakeResult("b", 0, 100, 2, 1)); }
    auto all = DurableFileSink::ReadAll(path);
    ASSERT_EQ(all.size(), 2u);
    EXPECT_EQ(all[0].key, "a");
    EXPECT_EQ(all[1].key, "b");
}

TEST_F(RecoveryGapsTest, DurableSinkReadLatestCollapsesReplayAndRefires) {
    const std::string path = dir + "/out.bin";
    {
        DurableFileSink sink(path);
        sink.Emit(MakeResult("a", 0, 100, 1, 1));
        sink.Emit(MakeResult("a", 0, 100, 1, 1));    // replay duplicate
        sink.Emit(MakeResult("a", 0, 100, 5, 2));    // late re-fire revision
        sink.Emit(MakeResult("b", 100, 200, 7, 1));
    }
    auto latest = DurableFileSink::ReadLatest(path);
    ASSERT_EQ(latest.size(), 2u);
    EXPECT_EQ(latest[0].key, "a");
    EXPECT_EQ(latest[0].result.value, 5);
    EXPECT_EQ(latest[1].key, "b");
}

// End to end: a restarted partitioned job writing to the SAME durable file.
TEST_F(RecoveryGapsTest, RestartOnSameDurableFileRecoversEveryResult) {
    const std::string ckpt = dir + "/ckpt";
    const std::string out = dir + "/out.bin";
    std::vector<WindowResult> uninterrupted;
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript()),
                              Tumbling(100), std::make_unique<CaptureSink>(uninterrupted),
                              {.num_workers = 2});
        p.Run();
    }
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript(), ckpt, 2),
                              Tumbling(100), std::make_unique<DurableFileSink>(out),
                              {.num_workers = 2, .checkpoint_dir = ckpt});
        EXPECT_THROW(p.Run(), std::runtime_error);
    }
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript()),
                              Tumbling(100), std::make_unique<DurableFileSink>(out),
                              {.num_workers = 2, .checkpoint_dir = ckpt});
        p.Run();
    }
    EXPECT_EQ(Sorted(DurableFileSink::ReadLatest(out)), Sorted(uninterrupted));
}

// ---------------------------------------------------------------------------
// Gap 3: native restore must reject a checkpoint directory written by an
// incompatible job configuration instead of silently emitting old state.
// ---------------------------------------------------------------------------
TEST_F(RecoveryGapsTest, PartitionedRestoreRejectsChangedWindowSize) {
    std::vector<WindowResult> first;
    {
        PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript(), dir, 2),
                              Tumbling(1000), std::make_unique<CaptureSink>(first),
                              {.num_workers = 2, .checkpoint_dir = dir});
        EXPECT_THROW(p.Run(), std::runtime_error);
    }
    std::vector<WindowResult> out;
    PartitionedPipeline p(std::make_unique<ScriptedSource>(CheckpointedScript()),
                          Tumbling(2000), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2, .checkpoint_dir = dir});
    EXPECT_THROW(p.Run(), JobManifestMismatch);
    EXPECT_TRUE(out.empty());
}

TEST_F(RecoveryGapsTest, PartitionedRestoreRejectsChangedWorkerCountLatenessAndSource) {
    GeneratorConfig gen;
    gen.num_records = 5000;
    gen.checkpoint_interval = 1000;
    std::vector<WindowResult> sink;
    auto run = [&](uint32_t workers, int64_t lateness, uint64_t seed) {
        GeneratorConfig g = gen;
        g.seed = seed;
        PartitionedPipelineConfig c;
        c.num_workers = workers;
        c.allowed_lateness = Duration{lateness};
        c.checkpoint_dir = dir;
        PartitionedPipeline p(std::make_unique<DeterministicGenerator>(g), Tumbling(1000),
                              std::make_unique<CaptureSink>(sink), c);
        p.Run();
    };
    run(2, 0, 42);
    EXPECT_THROW(run(4, 0, 42), JobManifestMismatch);
    EXPECT_THROW(run(2, 500, 42), JobManifestMismatch);
    EXPECT_THROW(run(2, 0, 43), JobManifestMismatch);
    EXPECT_NO_THROW(run(2, 0, 42));
}

TEST_F(RecoveryGapsTest, SingleThreadedRestoreRejectsChangedAssigner) {
    GeneratorConfig gen;
    gen.num_records = 5000;
    gen.checkpoint_interval = 1000;
    std::vector<WindowResult> sink;
    auto run = [&](std::unique_ptr<WindowAssigner> assigner) {
        PipelineConfig c;
        c.checkpoint_dir = dir;
        c.checkpoint_interval = 1000;
        Pipeline p(std::make_unique<DeterministicGenerator>(gen), std::move(assigner),
                   std::make_unique<CaptureSink>(sink), c);
        p.Run();
    };
    run(std::make_unique<SlidingAssigner>(Duration{1000}, Duration{500}));
    EXPECT_THROW(run(std::make_unique<SlidingAssigner>(Duration{1000}, Duration{250})),
                 JobManifestMismatch);
    EXPECT_THROW(run(std::make_unique<TumblingAssigner>(Duration{1000})), JobManifestMismatch);
    EXPECT_NO_THROW(run(std::make_unique<SlidingAssigner>(Duration{1000}, Duration{500})));
}

// ---------------------------------------------------------------------------
// Gap 4: SourceMerge must combine arbitrary Source implementations, not only
// generator configurations it constructs itself.
// ---------------------------------------------------------------------------
namespace {

GeneratorConfig MergeGen(uint64_t seed, int64_t step, uint64_t interval) {
    GeneratorConfig c;
    c.seed = seed;
    c.num_keys = 7;
    c.num_records = 3000;
    c.event_time_step = step;
    c.max_disorder = Duration{200};
    c.checkpoint_interval = interval;
    return c;
}

std::vector<BatchItem> Drain(Source& s) {
    std::vector<BatchItem> out;
    while (auto b = s.Next()) {
        for (auto& item : b->items) out.push_back(std::move(item));
    }
    return out;
}

bool SameItems(const std::vector<BatchItem>& a, const std::vector<BatchItem>& b) {
    if (a.size() != b.size()) return false;
    for (std::size_t i = 0; i < a.size(); ++i) {
        if (a[i].index() != b[i].index()) return false;
        if (auto* ra = std::get_if<Record>(&a[i])) {
            const auto& rb = std::get<Record>(b[i]);
            if (ra->key != rb.key || ra->value != rb.value || ra->event_time != rb.event_time)
                return false;
        } else {
            const auto& ca = std::get<ControlRecord>(a[i]);
            const auto& cb = std::get<ControlRecord>(b[i]);
            if (ca.type != cb.type || ca.watermark != cb.watermark ||
                ca.checkpoint_offset != cb.checkpoint_offset)
                return false;
        }
    }
    return true;
}

std::vector<SourceMergeInput> Inputs(const std::vector<GeneratorConfig>& gens) {
    std::vector<SourceMergeInput> inputs;
    for (const auto& g : gens) {
        inputs.push_back({.source = std::make_unique<DeterministicGenerator>(g),
                          .idle_spans = g.idle_spans});
    }
    return inputs;
}

} // namespace

TEST(SourceMergeInputsTest, ArbitrarySourcesMatchGeneratorConfigMerge) {
    auto a = MergeGen(1, 1, 400);
    auto b = MergeGen(2, 3, 700);
    b.idle_spans = {{.start_offset = 500, .length = 300}};
    SourceMergeConfig config{.sources = {a, b}, .idle_timeout = 50};

    SourceMerge by_config(config);
    SourceMerge by_inputs(Inputs({a, b}), SourceMergeConfig{.idle_timeout = 50});
    EXPECT_TRUE(SameItems(Drain(by_config), Drain(by_inputs)));
    EXPECT_EQ(by_config.CurrentOffset(), by_inputs.CurrentOffset());
}

TEST(SourceMergeInputsTest, ArbitrarySourcesSeekReplaysIdentically) {
    auto a = MergeGen(5, 1, 300);
    auto b = MergeGen(6, 2, 500);
    b.idle_spans = {{.start_offset = 200, .length = 150}};
    SourceMergeConfig config{.sources = {a, b}, .idle_timeout = 40};

    for (uint64_t offset : {0u, 1u, 777u, 2048u, 5999u}) {
        SourceMerge by_config(config);
        by_config.Seek(offset);
        SourceMerge by_inputs(Inputs({a, b}), SourceMergeConfig{.idle_timeout = 40});
        (void)by_inputs.Next();  // Seek must rewind channels that already advanced
        by_inputs.Seek(offset);
        EXPECT_EQ(by_inputs.CurrentOffset(), by_config.CurrentOffset()) << offset;
        EXPECT_TRUE(SameItems(Drain(by_inputs), Drain(by_config))) << offset;
    }
}

TEST(SourceMergeInputsTest, RejectsMixedOrInvalidConstruction) {
    EXPECT_THROW(SourceMerge(Inputs({MergeGen(1, 1, 0)}),
                             SourceMergeConfig{.sources = {MergeGen(2, 1, 0)}}),
                 std::invalid_argument);
    EXPECT_THROW(SourceMerge(Inputs({MergeGen(1, 1, 0)}),
                             SourceMergeConfig{.checkpoint_interval = 100}),
                 std::invalid_argument);
    std::vector<SourceMergeInput> null_input(1);
    EXPECT_THROW(SourceMerge(std::move(null_input), {}), std::invalid_argument);
}

TEST_F(RecoveryGapsTest, MergedArbitrarySourcesRecoverThroughPartitionedPipeline) {
    auto a = MergeGen(11, 1, 500);
    auto b = MergeGen(12, 2, 500);
    std::vector<WindowResult> uninterrupted;
    {
        PartitionedPipeline p(std::make_unique<SourceMerge>(Inputs({a, b}), SourceMergeConfig{}),
                              Tumbling(250), std::make_unique<CaptureSink>(uninterrupted),
                              {.num_workers = 3});
        p.Run();
    }
    // Same merged job restored mid-stream from a checkpoint written by a run
    // cut short by StoppingSource. The inputs are unchanged (changing an input's
    // length would change the merge order and is rejected), so the manifest
    // accepts it.
    std::vector<WindowResult> recovered;
    {
        PartitionedPipeline p(std::make_unique<StoppingSource>(
                                  std::make_unique<SourceMerge>(Inputs({a, b}), SourceMergeConfig{}),
                                  2400),
                              Tumbling(250), std::make_unique<CaptureSink>(recovered),
                              {.num_workers = 3, .checkpoint_dir = dir});
        p.Run();
    }
    recovered.clear();
    {
        PartitionedPipeline p(std::make_unique<SourceMerge>(Inputs({a, b}), SourceMergeConfig{}),
                              Tumbling(250), std::make_unique<CaptureSink>(recovered),
                              {.num_workers = 3, .checkpoint_dir = dir});
        auto stats = p.Run();
        EXPECT_GT(stats.records_replayed, 0u);
    }
    // Every final-flush result of the restored run agrees with the uninterrupted
    // run for windows the restored run re-emitted.
    auto full = Sorted(uninterrupted);
    for (const auto& r : Sorted(recovered)) {
        EXPECT_TRUE(std::binary_search(full.begin(), full.end(), r));
    }
}

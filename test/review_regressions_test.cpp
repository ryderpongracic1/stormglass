#include <gtest/gtest.h>

#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/job_manifest.h"
#include "checkpoint/writer.h"
#include "engine/partitioned_pipeline.h"
#include "source/generator.h"
#include "source/source_merge.h"
#include "source/stopping_source.h"
#include "window/tumbling.h"

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <future>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unistd.h>
#include <variant>
#include <vector>

using namespace stormglass;
using namespace std::chrono_literals;

namespace {

Timestamp Ms(int64_t ms) { return Timestamp{Duration{ms}}; }

struct ReviewRegressionTest : ::testing::Test {
    std::string dir;
    void SetUp() override {
        char name[] = "/tmp/sg-review-XXXXXX";
        char* made = ::mkdtemp(name);
        ASSERT_NE(made, nullptr);
        dir = made;
    }
    void TearDown() override { std::filesystem::remove_all(dir); }
};

struct SharedResults {
    std::mutex mu;
    std::vector<WindowResult> results;
};
class CaptureSink : public Sink {
public:
    explicit CaptureSink(SharedResults& out) : out_(out) {}
    void Emit(const WindowResult& r) override {
        std::lock_guard lock(out_.mu);
        out_.results.push_back(r);
    }
    void Flush() override {}

private:
    SharedResults& out_;
};

using Row = std::tuple<int64_t, std::string, int64_t, uint64_t>;
std::set<Row> Rows(const std::vector<WindowResult>& results) {
    std::set<Row> rows;
    for (const auto& r : results) {
        rows.emplace(r.window.start.time_since_epoch().count(), r.key, r.result.value,
                     r.result.count);
    }
    return rows;
}

auto Tumbling(int64_t ms) {
    return [ms] { return std::make_unique<TumblingAssigner>(Duration{ms}); };
}

GeneratorConfig MergeInput(uint64_t seed, uint64_t records) {
    GeneratorConfig c;
    c.seed = seed;
    c.num_keys = 8;
    c.num_records = records;
    c.max_disorder = Duration{100};
    c.checkpoint_interval = 250;
    return c;
}

std::unique_ptr<SourceMerge> Merge(uint64_t a_records, uint64_t b_records) {
    std::vector<SourceMergeInput> inputs;
    inputs.push_back({.source = std::make_unique<DeterministicGenerator>(MergeInput(1, a_records))});
    inputs.push_back({.source = std::make_unique<DeterministicGenerator>(MergeInput(2, b_records))});
    return std::make_unique<SourceMerge>(std::move(inputs), SourceMergeConfig{});
}

std::vector<uint64_t> CheckpointFiles(const std::string& partition_dir) {
    std::vector<uint64_t> offsets;
    for (const auto& entry : std::filesystem::directory_iterator(partition_dir)) {
        const auto name = entry.path().filename().string();
        if (name.size() == 36 && name.rfind("checkpoint-", 0) == 0 && name.substr(31) == ".ckpt") {
            offsets.push_back(std::stoull(name.substr(11, 20)));
        }
    }
    std::sort(offsets.begin(), offsets.end());
    return offsets;
}

// Replayable scripted source that blocks once its script is consumed, until
// Cancel(). Stands in for a live input that is waiting for data.
class BlockingSource : public Source {
public:
    std::optional<Batch> Next() override {
        std::unique_lock lock(mu_);
        if (!sent_) {
            sent_ = true;
            Batch b;
            b.items.emplace_back(Record{.key = "k", .value = 1, .event_time = Ms(5)});
            b.items.emplace_back(ControlRecord{.type = ControlType::kWatermark, .watermark = Ms(100)});
            return b;
        }
        cv_.wait(lock, [&] { return cancelled_; });
        return std::nullopt;
    }
    void Seek(uint64_t) override {}
    uint64_t CurrentOffset() const override { return 1; }
    void Cancel() override {
        std::lock_guard lock(mu_);
        cancelled_ = true;
        cv_.notify_all();
    }

private:
    std::mutex mu_;
    std::condition_variable cv_;
    bool sent_ = false;
    bool cancelled_ = false;
};

struct ThrowingSink : Sink {
    void Emit(const WindowResult&) override { throw std::runtime_error("sink failed"); }
    void Flush() override {}
};

} // namespace

// ---------------------------------------------------------------------------
// [P1] Extending one merged input changes the merged order after that input's
// former end. Restore must reject it rather than replay a different prefix.
// ---------------------------------------------------------------------------
TEST_F(ReviewRegressionTest, MergeRestoreRejectsExtendedInput) {
    // Input A (1000 records) ends partway; the checkpoint lands after that end.
    SharedResults first;
    {
        PartitionedPipeline p(std::make_unique<StoppingSource>(Merge(1000, 3000), 2600),
                              Tumbling(500), std::make_unique<CaptureSink>(first),
                              {.num_workers = 2, .checkpoint_dir = dir});
        p.Run();
    }
    auto complete = HighestCompleteCheckpoint(dir, 2);
    ASSERT_TRUE(complete.has_value());
    ASSERT_GT(*complete, 2000u) << "checkpoint must follow input A's end to exercise the bug";

    SharedResults second;
    PartitionedPipeline p(Merge(3000, 3000), Tumbling(500), std::make_unique<CaptureSink>(second),
                          {.num_workers = 2, .checkpoint_dir = dir});
    EXPECT_THROW(p.Run(), JobManifestMismatch);
    EXPECT_TRUE(second.results.empty());
}

// The same merged job restored with unchanged inputs matches an uninterrupted
// run exactly (the positive control for the rejection above).
TEST_F(ReviewRegressionTest, MergeRestoreWithSameInputsMatchesUninterrupted) {
    SharedResults uninterrupted;
    {
        PartitionedPipeline p(Merge(1000, 3000), Tumbling(500),
                              std::make_unique<CaptureSink>(uninterrupted), {.num_workers = 2});
        p.Run();
    }
    SharedResults first;
    {
        PartitionedPipeline p(std::make_unique<StoppingSource>(Merge(1000, 3000), 2600),
                              Tumbling(500), std::make_unique<CaptureSink>(first),
                              {.num_workers = 2, .checkpoint_dir = dir});
        p.Run();
    }
    SharedResults second;
    {
        PartitionedPipeline p(Merge(1000, 3000), Tumbling(500),
                              std::make_unique<CaptureSink>(second),
                              {.num_workers = 2, .checkpoint_dir = dir});
        EXPECT_GT(p.Run().records_replayed, 2000u);
    }
    // Final-flush results of the truncated first run are partial by design;
    // compare the restored run, which re-emits every window it restored.
    const auto expected = Rows(uninterrupted.results);
    for (const auto& row : Rows(second.results)) EXPECT_TRUE(expected.count(row));
}

// A merge channel that does not declare its extent could end at a different
// point on restore without detection, so such a merge refuses checkpointing.
TEST_F(ReviewRegressionTest, MergeWithUndeclaredChannelExtentIsNotCheckpointable) {
    struct Undeclared : Source {
        DeterministicGenerator inner{MergeInput(3, 100)};
        std::optional<Batch> Next() override { return inner.Next(); }
        void Seek(uint64_t o) override { inner.Seek(o); }
        uint64_t CurrentOffset() const override { return inner.CurrentOffset(); }
        std::string Descriptor() const override { return "undeclared"; }
    };
    std::vector<SourceMergeInput> inputs;
    inputs.push_back({.source = std::make_unique<DeterministicGenerator>(MergeInput(1, 100))});
    inputs.push_back({.source = std::make_unique<Undeclared>()});
    auto merge = std::make_unique<SourceMerge>(std::move(inputs), SourceMergeConfig{});
    EXPECT_FALSE(merge->Replayable());
    SharedResults out;
    PartitionedPipeline p(std::move(merge), Tumbling(500), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2, .checkpoint_dir = dir});
    EXPECT_THROW(p.Run(), std::invalid_argument);
}

// ---------------------------------------------------------------------------
// [P1] A worker failure while the router is blocked in Source::Next() must
// still end Run() with the worker's exception.
// ---------------------------------------------------------------------------
TEST(ReviewRegression, WorkerFailureCancelsBlockedSource) {
    PartitionedPipeline p(std::make_unique<BlockingSource>(), Tumbling(100),
                          std::make_unique<ThrowingSink>(), {.num_workers = 2});
    auto run = std::async(std::launch::async, [&] { return p.Run(); });
    const bool finished = run.wait_for(10s) == std::future_status::ready;
    if (!finished) p.RequestStop();  // release the hang so the test can report it
    EXPECT_TRUE(finished) << "Run() hung after a worker failure";
    EXPECT_THROW(run.get(), std::runtime_error);
}

// ---------------------------------------------------------------------------
// [P2] After a restart, retention must count the complete checkpoints already
// on disk, not just the one restored from.
// ---------------------------------------------------------------------------
TEST_F(ReviewRegressionTest, TrackerRetainsExistingHistoryAfterRestart) {
    const uint32_t n = 2;
    KeyedWindowState state;
    for (uint32_t k = 0; k < n; ++k) std::filesystem::create_directories(PartitionCheckpointDir(dir, k));
    auto write = [&](uint32_t k, uint64_t offset) {
        ASSERT_TRUE(CheckpointWriter(PartitionCheckpointDir(dir, k), true)
                        .WriteCheckpoint(offset, Timestamp{}, state));
    };
    for (uint64_t o : {100, 200, 300}) {
        for (uint32_t k = 0; k < n; ++k) write(k, o);
    }
    PartitionedCheckpointTracker tracker(dir, n, /*retain=*/3, CompleteCheckpoints(dir, n));
    EXPECT_EQ(tracker.LastComplete(), 300u);
    for (uint32_t k = 0; k < n; ++k) {
        write(k, 400);
        tracker.OnPartitionCheckpoint(k, 400);
    }
    for (uint32_t k = 0; k < n; ++k) {
        EXPECT_EQ(CheckpointFiles(PartitionCheckpointDir(dir, k)),
                  (std::vector<uint64_t>{200, 300, 400}));
    }
}

TEST_F(ReviewRegressionTest, RestartedJobKeepsRetainedHistory) {
    GeneratorConfig gen;
    gen.num_records = 20000;
    gen.checkpoint_interval = 1000;
    SharedResults out;
    auto run = [&](uint64_t stop_at) {
        PartitionedPipeline p(
            std::make_unique<StoppingSource>(std::make_unique<DeterministicGenerator>(gen), stop_at),
            Tumbling(1000), std::make_unique<CaptureSink>(out),
            {.num_workers = 2, .checkpoint_dir = dir, .checkpoint_retention = 3});
        p.Run();
    };
    // StoppingSource drops the barrier that shares its last record, so stop
    // past each barrier we need.
    run(10500);  // checkpoints 1000..10000, keeps 8000, 9000, 10000
    run(1500);   // restores at 10000, writes 11000
    for (uint32_t k = 0; k < 2; ++k) {
        EXPECT_EQ(CheckpointFiles(PartitionCheckpointDir(dir, k)),
                  (std::vector<uint64_t>{9000, 10000, 11000}));
    }
}

// ---------------------------------------------------------------------------
// [P2] StoppingSource must pass "no data yet" through, not end the stream.
// ---------------------------------------------------------------------------
TEST(ReviewRegression, StoppingSourcePassesEmptyBatchThrough) {
    struct QuietThenData : Source {
        int calls = 0;
        std::optional<Batch> Next() override {
            ++calls;
            if (calls == 1) return Batch{};
            if (calls > 2) return std::nullopt;
            Batch b;
            b.items.emplace_back(Record{.key = "k", .value = 7, .event_time = Ms(5)});
            return b;
        }
        void Seek(uint64_t) override {}
        uint64_t CurrentOffset() const override { return 0; }
    };
    StoppingSource source(std::make_unique<QuietThenData>(), 10);
    auto quiet = source.Next();
    ASSERT_TRUE(quiet.has_value()) << "an empty batch was turned into end of stream";
    EXPECT_TRUE(quiet->empty());
    auto data = source.Next();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->size(), 1u);
    EXPECT_FALSE(source.Next().has_value());
}

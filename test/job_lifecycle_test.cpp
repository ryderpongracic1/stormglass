#include <gtest/gtest.h>

#include "checkpoint/distributed_checkpoint.h"
#include "checkpoint/writer.h"
#include "engine/partition_hash.h"
#include "engine/partitioned_pipeline.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "source/source_merge.h"
#include "window/tumbling.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <filesystem>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <variant>
#include <vector>

using namespace stormglass;
using namespace std::chrono_literals;

namespace {

Timestamp Ms(int64_t ms) { return Timestamp{Duration{ms}}; }

struct LifecycleTest : ::testing::Test {
    std::string dir;
    void SetUp() override {
        char name[] = "/tmp/sg-lifecycle-XXXXXX";
        char* made = ::mkdtemp(name);
        ASSERT_NE(made, nullptr);
        dir = made;
    }
    void TearDown() override { std::filesystem::remove_all(dir); }
};

// Thread-safe capture: sinks are called from worker threads.
struct SharedResults {
    std::mutex mu;
    std::vector<WindowResult> results;
    std::vector<WindowResult> Snapshot() {
        std::lock_guard lock(mu);
        return results;
    }
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

// A replayable live source over a fixed script. Items at index >= `pause_at`
// are withheld: Next() blocks until Release() or Cancel(). After the script it
// ends (nullopt). Seek(O) resumes at the (O+1)-th record, like the generator.
class GatedSource : public Source {
public:
    GatedSource(std::vector<BatchItem> items, std::size_t pause_at)
        : items_(std::move(items)), pause_at_(pause_at) {}

    std::optional<Batch> Next() override {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return cancelled_ || released_ || cursor_ < pause_at_; });
        if (cancelled_) return std::nullopt;
        if (cursor_ >= items_.size()) return std::nullopt;
        Batch b;
        const std::size_t end = released_ ? items_.size() : std::min(items_.size(), pause_at_);
        for (; cursor_ < end; ++cursor_) {
            if (std::holds_alternative<Record>(items_[cursor_])) ++offset_;
            b.items.push_back(items_[cursor_]);
        }
        ++batches_;
        cv_.notify_all();
        return b;
    }
    void Seek(uint64_t offset) override {
        std::lock_guard lock(mu_);
        cursor_ = 0;
        uint64_t records = 0;
        for (; cursor_ < items_.size(); ++cursor_) {
            if (std::holds_alternative<Record>(items_[cursor_])) {
                if (records == offset) break;
                ++records;
            }
        }
        offset_ = offset;
    }
    uint64_t CurrentOffset() const override {
        std::lock_guard lock(mu_);
        return offset_;
    }
    std::string Descriptor() const override { return "gated"; }
    void Cancel() override {
        std::lock_guard lock(mu_);
        cancelled_ = true;
        cv_.notify_all();
    }
    void Release() {
        std::lock_guard lock(mu_);
        released_ = true;
        cv_.notify_all();
    }
    // Block until the pre-pause prefix has been handed out.
    void WaitForPrefix() {
        std::unique_lock lock(mu_);
        cv_.wait(lock, [&] { return cursor_ >= std::min(pause_at_, items_.size()); });
    }

private:
    mutable std::mutex mu_;
    std::condition_variable cv_;
    std::vector<BatchItem> items_;
    std::size_t pause_at_;
    std::size_t cursor_ = 0;
    uint64_t offset_ = 0;
    uint64_t batches_ = 0;
    bool cancelled_ = false;
    bool released_ = false;
};

// Window [0,100) closes at watermark 100; [100,200) is still open at the pause.
std::vector<BatchItem> LifecycleScript() {
    return {
        Record{.key = "a", .value = 1, .event_time = Ms(10)},
        Record{.key = "b", .value = 2, .event_time = Ms(20)},
        ControlRecord{.type = ControlType::kWatermark, .watermark = Ms(100)},
        Record{.key = "a", .value = 4, .event_time = Ms(120)},
        Record{.key = "b", .value = 8, .event_time = Ms(130)},
        // --- pause here (index 5) ---
        Record{.key = "a", .value = 16, .event_time = Ms(150)},
        Record{.key = "b", .value = 32, .event_time = Ms(160)},
        ControlRecord{.type = ControlType::kWatermark, .watermark = Ms(300)},
    };
}
constexpr std::size_t kPause = 5;

auto Tumbling100() {
    return [] { return std::make_unique<TumblingAssigner>(Duration{100}); };
}

std::set<Row> UninterruptedPartitioned() {
    SharedResults out;
    auto source = std::make_unique<GatedSource>(LifecycleScript(), SIZE_MAX);
    PartitionedPipeline p(std::move(source), Tumbling100(), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2});
    p.Run();
    return Rows(out.results);
}

} // namespace

// ---------------------------------------------------------------------------
// Suspend and restart
// ---------------------------------------------------------------------------

TEST_F(LifecycleTest, PartitionedSuspendCancelsBlockedSourceAndRestartsWithoutPartialWindows) {
    const auto expected = UninterruptedPartitioned();
    ASSERT_EQ(expected.size(), 4u);

    SharedResults first;
    {
        auto source = std::make_unique<GatedSource>(LifecycleScript(), kPause);
        auto* gate = source.get();
        PartitionedPipeline p(std::move(source), Tumbling100(),
                              std::make_unique<CaptureSink>(first),
                              {.num_workers = 2, .checkpoint_dir = dir});
        std::thread stopper([&] {
            gate->WaitForPrefix();
            // Wait until both records of the prefix are applied, then stop while
            // the source is blocked with no further input.
            for (int i = 0; i < 5000 && p.Progress().records_processed < 4; ++i) ::usleep(1000);
            p.RequestStop(StopMode::kSuspend);
        });
        auto stats = p.Run();
        stopper.join();
        EXPECT_TRUE(stats.suspended);
        ASSERT_TRUE(stats.suspend_checkpoint_offset.has_value());
        EXPECT_EQ(*stats.suspend_checkpoint_offset, 4u);
        EXPECT_EQ(HighestCompleteCheckpoint(dir, 2), 4u);
    }
    // Only the closed window was emitted; the open [100,200) window was not.
    for (const auto& row : Rows(first.results)) {
        EXPECT_EQ(std::get<0>(row), 0) << "suspend emitted an open window";
        EXPECT_TRUE(expected.count(row));
    }
    EXPECT_EQ(Rows(first.results).size(), 2u);

    SharedResults second;
    {
        PartitionedPipeline p(std::make_unique<GatedSource>(LifecycleScript(), SIZE_MAX),
                              Tumbling100(), std::make_unique<CaptureSink>(second),
                              {.num_workers = 2, .checkpoint_dir = dir});
        auto stats = p.Run();
        EXPECT_FALSE(stats.suspended);
        EXPECT_EQ(stats.records_replayed, 4u);
    }
    auto all = Rows(first.results);
    for (const auto& row : Rows(second.results)) all.insert(row);
    EXPECT_EQ(all, expected);
}

TEST_F(LifecycleTest, PartitionedFinalStopFiresOpenWindows) {
    SharedResults out;
    auto source = std::make_unique<GatedSource>(LifecycleScript(), kPause);
    auto* gate = source.get();
    PartitionedPipeline p(std::move(source), Tumbling100(), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2});
    std::thread stopper([&] {
        gate->WaitForPrefix();
        for (int i = 0; i < 5000 && p.Progress().records_processed < 4; ++i) ::usleep(1000);
        p.RequestStop(StopMode::kFinal);
    });
    auto stats = p.Run();
    stopper.join();
    EXPECT_FALSE(stats.suspended);
    EXPECT_FALSE(stats.suspend_checkpoint_offset.has_value());
    const auto rows = Rows(out.results);
    EXPECT_EQ(rows.size(), 4u);  // two closed + two fired early by the final stop
    EXPECT_TRUE(rows.count({100, "a", 4, 1}));
}

TEST_F(LifecycleTest, StopRequestedBeforeRunSuspendsImmediately) {
    SharedResults out;
    PartitionedPipeline p(std::make_unique<GatedSource>(LifecycleScript(), SIZE_MAX), Tumbling100(),
                          std::make_unique<CaptureSink>(out), {.num_workers = 2});
    p.RequestStop();
    p.RequestStop(StopMode::kFinal);  // first request wins
    auto stats = p.Run();
    EXPECT_TRUE(stats.suspended);
    EXPECT_EQ(stats.records_processed, 0u);
    EXPECT_TRUE(out.results.empty());
}

TEST_F(LifecycleTest, SingleThreadedSuspendAndRestartMatchesUninterrupted) {
    SharedResults uninterrupted;
    {
        Pipeline p(std::make_unique<GatedSource>(LifecycleScript(), SIZE_MAX),
                   std::make_unique<TumblingAssigner>(Duration{100}),
                   std::make_unique<CaptureSink>(uninterrupted));
        p.Run();
    }
    PipelineConfig config{.checkpoint_dir = dir, .checkpoint_interval = 1};
    SharedResults first;
    {
        auto source = std::make_unique<GatedSource>(LifecycleScript(), kPause);
        auto* gate = source.get();
        Pipeline p(std::move(source), std::make_unique<TumblingAssigner>(Duration{100}),
                   std::make_unique<CaptureSink>(first), config);
        std::thread stopper([&] {
            gate->WaitForPrefix();
            for (int i = 0; i < 5000 && p.Progress().records_processed < 4; ++i) ::usleep(1000);
            p.RequestStop();
        });
        auto stats = p.Run();
        stopper.join();
        EXPECT_TRUE(stats.suspended);
        EXPECT_EQ(stats.suspend_checkpoint_offset, 4u);
        EXPECT_EQ(p.Progress().last_complete_checkpoint, 4u);
        EXPECT_FALSE(p.Progress().running);
    }
    EXPECT_EQ(Rows(first.results).size(), 2u);
    SharedResults second;
    {
        Pipeline p(std::make_unique<GatedSource>(LifecycleScript(), SIZE_MAX),
                   std::make_unique<TumblingAssigner>(Duration{100}),
                   std::make_unique<CaptureSink>(second), config);
        EXPECT_EQ(p.Run().records_replayed, 4u);
    }
    auto all = Rows(first.results);
    for (const auto& row : Rows(second.results)) all.insert(row);
    EXPECT_EQ(all, Rows(uninterrupted.results));
}

// Stop a busy multi-worker job at an arbitrary point and resume: the union
// equals an uninterrupted run and the stopped run emitted no premature value.
TEST_F(LifecycleTest, SuspendMidStreamOnBusyJobResumesExactly) {
    GeneratorConfig gen;
    gen.seed = 9;
    gen.num_keys = 50;
    // Sized to stay fast under ThreadSanitizer; small batches and a one-slot
    // queue keep the router close to the workers so the stop lands mid-stream.
    gen.num_records = 300000;
    gen.batch_size = 256;
    gen.max_disorder = Duration{300};
    gen.checkpoint_interval = 10000;
    auto factory = [] { return std::make_unique<TumblingAssigner>(Duration{1000}); };

    SharedResults uninterrupted;
    {
        PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), factory,
                              std::make_unique<CaptureSink>(uninterrupted), {.num_workers = 4});
        p.Run();
    }
    const auto expected = Rows(uninterrupted.results);

    SharedResults first;
    uint64_t stopped_at = 0;
    {
        PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), factory,
                              std::make_unique<CaptureSink>(first),
                              {.num_workers = 4, .queue_capacity = 1, .checkpoint_dir = dir});
        std::thread stopper([&] {
            for (int i = 0; i < 10000 && p.Progress().records_read < 30000; ++i) ::usleep(100);
            p.RequestStop(StopMode::kSuspend);
        });
        auto stats = p.Run();
        stopper.join();
        ASSERT_TRUE(stats.suspended);
        ASSERT_TRUE(stats.suspend_checkpoint_offset.has_value());
        stopped_at = *stats.suspend_checkpoint_offset;
        EXPECT_LT(stopped_at, gen.num_records) << "stop landed after the stream ended";
    }
    for (const auto& row : Rows(first.results)) {
        EXPECT_TRUE(expected.count(row)) << "suspended run emitted a value the full run never did";
    }
    SharedResults second;
    {
        PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), factory,
                              std::make_unique<CaptureSink>(second),
                              {.num_workers = 4, .checkpoint_dir = dir});
        EXPECT_EQ(p.Run().records_replayed, stopped_at);
    }
    auto all = Rows(first.results);
    for (const auto& row : Rows(second.results)) all.insert(row);
    EXPECT_EQ(all, expected);
}

// ---------------------------------------------------------------------------
// Progress
// ---------------------------------------------------------------------------

TEST_F(LifecycleTest, ProgressIsVisibleWhileRunning) {
    SharedResults out;
    auto source = std::make_unique<GatedSource>(LifecycleScript(), kPause);
    auto* gate = source.get();
    PartitionedPipeline p(std::move(source), Tumbling100(), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2});
    EXPECT_FALSE(p.Progress().running);
    JobProgress mid;
    std::thread observer([&] {
        gate->WaitForPrefix();
        for (int i = 0; i < 5000 && p.Progress().records_processed < 4; ++i) ::usleep(1000);
        mid = p.Progress();
        gate->Release();
    });
    auto stats = p.Run();
    observer.join();
    EXPECT_TRUE(mid.running);
    EXPECT_EQ(mid.records_read, 4u);
    EXPECT_EQ(mid.records_processed, 4u);
    // The [0,100) window fires once on each worker that owns one of its keys.
    const uint64_t owners = PartitionForKey("a", 2) == PartitionForKey("b", 2) ? 1 : 2;
    EXPECT_EQ(mid.windows_fired, owners);
    EXPECT_EQ(mid.output_watermark, Ms(100));
    const auto done = p.Progress();
    EXPECT_FALSE(done.running);
    EXPECT_EQ(done.records_processed, stats.records_processed);
    EXPECT_EQ(done.windows_fired, stats.windows_fired);
}

// ---------------------------------------------------------------------------
// Live inputs
// ---------------------------------------------------------------------------

namespace {

struct AlwaysQuiet : Source {
    std::atomic<uint64_t> polls{0};
    std::optional<Batch> Next() override { ++polls; return Batch{}; }
    void Seek(uint64_t) override {}
    uint64_t CurrentOffset() const override { return 0; }
    std::optional<uint64_t> Length() const override { return kUnbounded; }
};

GeneratorConfig LiveGen() {
    GeneratorConfig g;
    g.seed = 3;
    g.num_keys = 4;
    g.num_records = 2000;
    g.max_disorder = Duration{0};
    g.watermark_interval = 100;
    return g;
}

std::vector<SourceMergeInput> WithQuietChannel(AlwaysQuiet** quiet) {
    std::vector<SourceMergeInput> inputs;
    inputs.push_back({.source = std::make_unique<DeterministicGenerator>(LiveGen())});
    auto q = std::make_unique<AlwaysQuiet>();
    *quiet = q.get();
    inputs.push_back({.source = std::move(q)});
    return inputs;
}

} // namespace

TEST(LiveInputsTest, QuietChannelDoesNotBlockMergeAndWallIdleReleasesWatermark) {
    // A live channel delivering one record (and its watermark) per millisecond.
    struct Trickle : Source {
        uint64_t sent = 0;
        std::chrono::steady_clock::time_point next = std::chrono::steady_clock::now();
        std::optional<Batch> Next() override {
            if (sent == 200) return std::nullopt;
            if (std::chrono::steady_clock::now() < next) return Batch{};
            next += 1ms;
            ++sent;
            Batch b;
            b.items.emplace_back(Record{.key = "k", .value = 1, .event_time = Ms(sent * 10)});
            b.items.emplace_back(ControlRecord{.type = ControlType::kWatermark,
                                               .watermark = Ms(sent * 10)});
            return b;
        }
        void Seek(uint64_t) override {}
        uint64_t CurrentOffset() const override { return sent; }
    };
    std::vector<SourceMergeInput> inputs;
    auto trickle = std::make_unique<Trickle>();
    auto* live = trickle.get();
    inputs.push_back({.source = std::move(trickle)});
    inputs.push_back({.source = std::make_unique<AlwaysQuiet>()});
    SourceMerge merge(std::move(inputs),
                      SourceMergeConfig{.live_inputs = true, .idle_timeout_wall = 20ms});

    const auto start = std::chrono::steady_clock::now();
    uint64_t records = 0;
    bool saw_empty = false;
    bool advanced_while_live = false;
    while (std::chrono::steady_clock::now() - start < 5s) {
        auto batch = merge.Next();
        if (!batch) break;
        if (batch->empty()) {
            saw_empty = true;
            std::this_thread::sleep_for(100us);
        }
        for (auto& item : batch->items) records += std::holds_alternative<Record>(item);
        if (live->sent < 200 && merge.CurrentWatermark() > Timestamp::min()) {
            advanced_while_live = true;
        }
    }
    EXPECT_EQ(records, 200u);
    EXPECT_TRUE(saw_empty) << "a quiet live merge should hand back empty batches";
    EXPECT_TRUE(merge.IsSourceIdle(1));
    EXPECT_TRUE(advanced_while_live)
        << "wall-clock idleness should exclude the quiet channel from the minimum";
}

TEST(LiveInputsTest, WithoutWallIdleQuietChannelHoldsWatermark) {
    AlwaysQuiet* quiet = nullptr;
    SourceMerge merge(WithQuietChannel(&quiet), SourceMergeConfig{.live_inputs = true});
    for (int i = 0; i < 20; ++i) {
        auto batch = merge.Next();
        ASSERT_TRUE(batch.has_value());
    }
    EXPECT_FALSE(merge.IsSourceIdle(1));
    EXPECT_EQ(merge.CurrentWatermark(), Timestamp::min());
}

TEST(LiveInputsTest, LiveMergeIsNotReplayableOrCheckpointable) {
    AlwaysQuiet* quiet = nullptr;
    auto merge = std::make_unique<SourceMerge>(WithQuietChannel(&quiet),
                                               SourceMergeConfig{.live_inputs = true});
    EXPECT_FALSE(merge->Replayable());
    EXPECT_NO_THROW(merge->Seek(0));
    EXPECT_THROW(merge->Seek(10), std::logic_error);

    char name[] = "/tmp/sg-live-XXXXXX";
    std::string dir = ::mkdtemp(name);
    SharedResults out;
    PartitionedPipeline p(std::move(merge), Tumbling100(), std::make_unique<CaptureSink>(out),
                          {.num_workers = 2, .checkpoint_dir = dir});
    EXPECT_THROW(p.Run(), std::invalid_argument);
    std::filesystem::remove_all(dir);
}

TEST(LiveInputsTest, ReplayableMergeRejectsEmptyBatchAndMisconfiguration) {
    AlwaysQuiet* quiet = nullptr;
    SourceMerge merge(WithQuietChannel(&quiet), SourceMergeConfig{});
    EXPECT_TRUE(merge.Replayable());
    EXPECT_THROW(
        {
            for (int i = 0; i < 4; ++i) (void)merge.Next();
        },
        std::logic_error);
    EXPECT_THROW(SourceMerge(WithQuietChannel(&quiet), SourceMergeConfig{.idle_timeout_wall = 5ms}),
                 std::invalid_argument);
    EXPECT_THROW(SourceMerge(SourceMergeConfig{.sources = {LiveGen()}, .live_inputs = true}),
                 std::invalid_argument);
}

// An empty batch straight into a pipeline means "no data yet": it backs off
// instead of spinning, and results match the same data without gaps.
TEST(LiveInputsTest, PipelinesBackOffOnEmptyBatches) {
    struct Gappy : Source {
        std::vector<BatchItem> items = LifecycleScript();
        std::size_t cursor = 0;
        uint64_t polls = 0;
        std::chrono::steady_clock::time_point quiet_until{};
        std::optional<Batch> Next() override {
            ++polls;
            const auto now = std::chrono::steady_clock::now();
            if (cursor == 3 && quiet_until == std::chrono::steady_clock::time_point{}) {
                quiet_until = now + 100ms;
            }
            if (now < quiet_until) return Batch{};
            if (cursor >= items.size()) return std::nullopt;
            Batch b;
            b.items.push_back(items[cursor++]);
            return b;
        }
        void Seek(uint64_t) override {}
        uint64_t CurrentOffset() const override { return 0; }
    };
    const auto expected = UninterruptedPartitioned();
    for (bool partitioned : {true, false}) {
        SharedResults out;
        auto source = std::make_unique<Gappy>();
        auto* gappy = source.get();  // owned by the pipeline; read before it is destroyed
        uint64_t polls = 0;
        if (partitioned) {
            PartitionedPipeline p(std::move(source), Tumbling100(),
                                  std::make_unique<CaptureSink>(out), {.num_workers = 2});
            p.Run();
            polls = gappy->polls;
        } else {
            Pipeline p(std::move(source), std::make_unique<TumblingAssigner>(Duration{100}),
                       std::make_unique<CaptureSink>(out));
            p.Run();
            polls = gappy->polls;
        }
        EXPECT_EQ(Rows(out.results), expected) << partitioned;
        // A 100 ms gap at a 1 ms backoff ceiling is on the order of 100 polls.
        EXPECT_LT(polls, 5000u) << partitioned;
    }
}

// ---------------------------------------------------------------------------
// Coordinated retention
// ---------------------------------------------------------------------------

namespace {

std::vector<uint64_t> CheckpointFiles(const std::string& partition_dir) {
    std::vector<uint64_t> offsets;
    for (const auto& entry : std::filesystem::directory_iterator(partition_dir)) {
        const auto name = entry.path().filename().string();
        if (name.size() == 36 && name.rfind("checkpoint-", 0) == 0 &&
            name.substr(31) == ".ckpt") {
            offsets.push_back(std::stoull(name.substr(11, 20)));
        }
    }
    std::sort(offsets.begin(), offsets.end());
    return offsets;
}

} // namespace

TEST_F(LifecycleTest, RetentionKeepsNewestCompleteCheckpointsAndRestoreStillWorks) {
    GeneratorConfig gen;
    gen.num_records = 20000;
    gen.checkpoint_interval = 1000;
    auto factory = [] { return std::make_unique<TumblingAssigner>(Duration{1000}); };
    SharedResults out;
    {
        PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), factory,
                              std::make_unique<CaptureSink>(out),
                              {.num_workers = 3, .checkpoint_dir = dir, .checkpoint_retention = 2});
        p.Run();
        EXPECT_EQ(p.Progress().last_complete_checkpoint, 20000u);
    }
    for (uint32_t k = 0; k < 3; ++k) {
        EXPECT_EQ(CheckpointFiles(PartitionCheckpointDir(dir, k)),
                  (std::vector<uint64_t>{19000, 20000}))
            << "partition " << k;
    }
    PartitionedPipeline p(std::make_unique<DeterministicGenerator>(gen), factory,
                          std::make_unique<CaptureSink>(out),
                          {.num_workers = 3, .checkpoint_dir = dir, .checkpoint_retention = 2});
    EXPECT_EQ(p.Run().records_replayed, 20000u);
}

TEST_F(LifecycleTest, TrackerNeverPrunesIncompleteOrRetainedCheckpoints) {
    const uint32_t n = 2;
    KeyedWindowState state;
    for (uint32_t k = 0; k < n; ++k) {
        std::filesystem::create_directories(PartitionCheckpointDir(dir, k));
    }
    auto write = [&](uint32_t k, uint64_t offset) {
        ASSERT_TRUE(CheckpointWriter(PartitionCheckpointDir(dir, k), true)
                        .WriteCheckpoint(offset, Timestamp{}, state));
    };
    // A previous run left 100 and 200 complete, plus a torn 300 on partition 0.
    for (uint32_t k = 0; k < n; ++k) { write(k, 100); write(k, 200); }
    write(0, 300);

    PartitionedCheckpointTracker tracker(dir, n, /*retain=*/1, CompleteCheckpoints(dir, n));
    EXPECT_EQ(tracker.LastComplete(), 200u);

    write(0, 300);
    tracker.OnPartitionCheckpoint(0, 300);  // partition 1 has not reached 300
    EXPECT_EQ(tracker.LastComplete(), 200u);
    EXPECT_EQ(CheckpointFiles(PartitionCheckpointDir(dir, 0)), (std::vector<uint64_t>{100, 200, 300}));

    write(1, 300);
    tracker.OnPartitionCheckpoint(1, 300);
    EXPECT_EQ(tracker.LastComplete(), 300u);
    for (uint32_t k = 0; k < n; ++k) {
        EXPECT_EQ(CheckpointFiles(PartitionCheckpointDir(dir, k)), (std::vector<uint64_t>{300}));
    }
    EXPECT_EQ(HighestCompleteCheckpoint(dir, n), 300u);
}

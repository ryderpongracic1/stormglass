#include <gtest/gtest.h>

#include "checkpoint/crc32c.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"
#include "window/state.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "oracle/oracle.h"
#include <map>
#include <tuple>

#include <cstdlib>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <unistd.h>

using namespace stormglass;

namespace {

class CheckpointTest : public ::testing::Test {
protected:
    void SetUp() override {
        char tmpl[] = "/tmp/stormglass_test_XXXXXX";
        char* result = ::mkdtemp(tmpl);
        ASSERT_NE(result, nullptr);
        dir_ = result;
    }

    void TearDown() override {
        std::filesystem::remove_all(dir_);
    }

    std::string dir_;
};

} // namespace

// --- CRC32C Tests ---

TEST(Crc32cTest, EmptyInput) {
    uint32_t crc = Crc32c(nullptr, 0);
    EXPECT_EQ(crc, 0);
}

TEST(Crc32cTest, KnownValue) {
    // CRC32C of "123456789" is 0xE3069283
    const char* input = "123456789";
    uint32_t crc = Crc32c(input, 9);
    EXPECT_EQ(crc, 0xE3069283);
}

TEST(Crc32cTest, Incremental) {
    const char* input = "hello world";
    uint32_t full = Crc32c(input, 11);

    // Incremental should yield same result
    uint32_t partial = Crc32c(input, 5);
    uint32_t combined = Crc32c(input + 5, 6, partial);
    EXPECT_EQ(full, combined);
}

// --- Writer/Reader Round-Trip ---

TEST_F(CheckpointTest, WriteReadRoundTrip) {
    KeyedWindowState state;
    state.Add("key-0001", Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}}, 42);
    state.Add("key-0001", Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}}, 58);
    state.Add("key-0002", Window{Timestamp{Duration{1000}}, Timestamp{Duration{2000}}}, 100);

    CheckpointWriter writer(dir_);
    Timestamp wm{Duration{500}};
    ASSERT_TRUE(writer.WriteCheckpoint(1000, wm, state));

    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());

    EXPECT_EQ(data->offset, 1000u);
    EXPECT_EQ(data->watermark, wm);
    EXPECT_EQ(data->panes.size(), 2u);

    // Verify pane data (order may vary)
    bool found_key1 = false, found_key2 = false;
    for (const auto& pane : data->panes) {
        if (pane.key == "key-0001") {
            EXPECT_EQ(pane.sum, 100);  // 42 + 58
            EXPECT_EQ(pane.count, 2u);
            EXPECT_EQ(pane.window.start, Timestamp{Duration{0}});
            EXPECT_EQ(pane.window.end, Timestamp{Duration{1000}});
            found_key1 = true;
        } else if (pane.key == "key-0002") {
            EXPECT_EQ(pane.sum, 100);
            EXPECT_EQ(pane.count, 1u);
            found_key2 = true;
        }
    }
    EXPECT_TRUE(found_key1);
    EXPECT_TRUE(found_key2);
}

TEST_F(CheckpointTest, CrcValidationCorruptByte) {
    KeyedWindowState state;
    state.Add("key-0001", Window{Timestamp{Duration{0}}, Timestamp{Duration{1000}}}, 42);

    CheckpointWriter writer(dir_);
    ASSERT_TRUE(writer.WriteCheckpoint(100, Timestamp{Duration{50}}, state));

    // Corrupt one byte in the checkpoint file
    std::string path = dir_ + "/checkpoint-00000000000000000100.ckpt";
    int fd = ::open(path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    // Seek to middle of file and flip a byte
    ::lseek(fd, 20, SEEK_SET);
    uint8_t byte = 0xFF;
    [[maybe_unused]] auto wr = ::write(fd, &byte, 1);
    ::close(fd);

    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    EXPECT_FALSE(data.has_value());  // CRC validation should fail
}

TEST_F(CheckpointTest, RetainLast2Checkpoints) {
    KeyedWindowState state;
    state.Add("k", Window{Timestamp{Duration{0}}, Timestamp{Duration{100}}}, 1);

    CheckpointWriter writer(dir_);
    ASSERT_TRUE(writer.WriteCheckpoint(100, Timestamp{Duration{10}}, state));
    ASSERT_TRUE(writer.WriteCheckpoint(200, Timestamp{Duration{20}}, state));
    ASSERT_TRUE(writer.WriteCheckpoint(300, Timestamp{Duration{30}}, state));

    // Count .ckpt files — should be exactly 2
    DIR* dir = ::opendir(dir_.c_str());
    ASSERT_NE(dir, nullptr);
    int count = 0;
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
        std::string name = entry->d_name;
        if (name.size() > 5 && name.substr(name.size() - 5) == ".ckpt") {
            count++;
        }
    }
    ::closedir(dir);
    EXPECT_EQ(count, 2);

    // The latest checkpoint should be offset 300
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->offset, 300u);
}

TEST_F(CheckpointTest, TmpFileCleanup) {
    // Create a stale .tmp file
    std::string tmp_path = dir_ + "/checkpoint-00000000000000000050.ckpt.tmp";
    int fd = ::open(tmp_path.c_str(), O_WRONLY | O_CREAT, 0644);
    [[maybe_unused]] auto wr = ::write(fd, "garbage", 7);
    ::close(fd);

    // Write a valid checkpoint
    KeyedWindowState state;
    state.Add("k", Window{Timestamp{Duration{0}}, Timestamp{Duration{100}}}, 1);
    CheckpointWriter writer(dir_);
    ASSERT_TRUE(writer.WriteCheckpoint(100, Timestamp{Duration{10}}, state));

    // LoadLatest should clean up .tmp files
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->offset, 100u);

    // Verify .tmp is gone
    EXPECT_NE(::access(tmp_path.c_str(), F_OK), 0);
}

TEST_F(CheckpointTest, LargeRoundTrip100Panes) {
    KeyedWindowState state;
    for (int i = 0; i < 100; ++i) {
        char key[16];
        std::snprintf(key, sizeof(key), "key-%04d", i);
        Window w{Timestamp{Duration{i * 1000}}, Timestamp{Duration{(i + 1) * 1000}}};
        state.Add(key, w, static_cast<int64_t>(i * 10));
        state.Add(key, w, static_cast<int64_t>(i * 5));
    }

    CheckpointWriter writer(dir_);
    Timestamp wm{Duration{50000}};
    ASSERT_TRUE(writer.WriteCheckpoint(99999, wm, state));

    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->offset, 99999u);
    EXPECT_EQ(data->watermark, wm);
    EXPECT_EQ(data->panes.size(), 100u);

    // Verify sums and counts
    for (const auto& pane : data->panes) {
        EXPECT_EQ(pane.count, 2u);
        // Extract key index from key name
        int idx = std::atoi(pane.key.c_str() + 4);
        EXPECT_EQ(pane.sum, idx * 10 + idx * 5);
    }
}

TEST_F(CheckpointTest, FallbackToOlderCheckpoint) {
    KeyedWindowState state;
    state.Add("k", Window{Timestamp{Duration{0}}, Timestamp{Duration{100}}}, 1);

    CheckpointWriter writer(dir_);
    ASSERT_TRUE(writer.WriteCheckpoint(100, Timestamp{Duration{10}}, state));
    ASSERT_TRUE(writer.WriteCheckpoint(200, Timestamp{Duration{20}}, state));

    // Corrupt the newer checkpoint
    std::string newer_path = dir_ + "/checkpoint-00000000000000000200.ckpt";
    int fd = ::open(newer_path.c_str(), O_RDWR);
    ASSERT_GE(fd, 0);
    ::lseek(fd, 10, SEEK_SET);
    uint8_t byte = 0xFF;
    [[maybe_unused]] auto wr = ::write(fd, &byte, 1);
    ::close(fd);

    // Reader should fall back to offset 100
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());
    EXPECT_EQ(data->offset, 100u);
}

TEST_F(CheckpointTest, NoCheckpointReturnsNullopt) {
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    EXPECT_FALSE(data.has_value());
}

TEST_F(CheckpointTest, RestoreIntoState) {
    // Write a checkpoint
    KeyedWindowState state;
    state.Add("k1", Window{Timestamp{Duration{0}}, Timestamp{Duration{100}}}, 42);
    state.Add("k1", Window{Timestamp{Duration{0}}, Timestamp{Duration{100}}}, 8);
    state.Add("k2", Window{Timestamp{Duration{100}}, Timestamp{Duration{200}}}, 99);

    CheckpointWriter writer(dir_);
    ASSERT_TRUE(writer.WriteCheckpoint(500, Timestamp{Duration{50}}, state));

    // Read it back
    CheckpointReader reader(dir_);
    auto data = reader.LoadLatest();
    ASSERT_TRUE(data.has_value());

    // Restore into a fresh state
    KeyedWindowState restored;
    for (const auto& entry : data->panes) {
        restored.RestorePane(entry.key, entry.window, entry.sum, entry.count);
    }

    // Verify restored state matches original
    EXPECT_EQ(restored.TotalPanes(), state.TotalPanes());

    auto pane_map = [](const KeyedWindowState& s) {
        std::map<std::tuple<std::string, int64_t, int64_t>,
                 std::pair<int64_t, uint64_t>> m;
        s.ForEachPane([&](const std::string& key, const Window& w, const Pane& pane) {
            m[{key, w.start.time_since_epoch().count(),
               w.end.time_since_epoch().count()}] = {pane.sum, pane.count};
        });
        return m;
    };
    EXPECT_EQ(pane_map(restored), pane_map(state));
}

TEST_F(CheckpointTest, RestoreWithLatenessRefiresBehavior) {
    // Checkpoint v2 persists fired_windows_, so after restore with
    // allowed_lateness, previously-fired windows are correctly tracked.
    // Combined results across crash+restore must contain all oracle results
    // (at-least-once semantics).
    
    GeneratorConfig gen_config;
    gen_config.seed = 77;
    gen_config.num_records = 20000;
    gen_config.num_keys = 5;
    gen_config.max_disorder = Duration{500};
    gen_config.batch_size = 256;
    gen_config.watermark_interval = 50;
    gen_config.checkpoint_interval = 5000;
    
    PipelineConfig pipe_config;
    pipe_config.checkpoint_dir = dir_;
    pipe_config.checkpoint_interval = 5000;
    pipe_config.allowed_lateness = Duration{2000};  // 2s lateness
    
    // Collect results across all runs (at-least-once: combined must cover oracle)
    std::map<std::pair<std::string, int64_t>, std::pair<int64_t, uint64_t>> combined;
    
    // First run: process 10000 records with lateness, then "crash"
    {
        auto source = std::make_unique<StoppingSource>(
            std::make_unique<DeterministicGenerator>(gen_config), 10000u);
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();
        Pipeline pipeline(std::move(source), std::make_unique<TumblingAssigner>(Duration{1000}),
                         std::move(sink), pipe_config);
        auto stats = pipeline.Run();
        EXPECT_GE(stats.checkpoints_written, 1u);
        for (auto& r : sink_ptr->Results()) {
            auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
            combined[key] = {r.result.value, r.result.count};
        }
    }
    
    // Second run: restore and run to completion
    {
        auto source = std::make_unique<DeterministicGenerator>(gen_config);
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();
        Pipeline pipeline(std::move(source), std::make_unique<TumblingAssigner>(Duration{1000}),
                         std::move(sink), pipe_config);
        auto stats = pipeline.Run();
        EXPECT_GT(stats.records_replayed, 0u);
        for (auto& r : sink_ptr->Results()) {
            auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
            combined[key] = {r.result.value, r.result.count};
        }
    }
    
    // Oracle: what should the FULL run produce?
    Oracle oracle(OracleConfig{.window_size = Duration{1000}});
    DeterministicGenerator oracle_gen(gen_config);
    while (auto batch = oracle_gen.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            }
        }
    }
    auto oracle_results = oracle.ComputeResults();
    
    // Key assertion: combined results across crash+restore must contain all
    // oracle results with zero data loss. Fired windows are persisted in v2
    // checkpoints, preventing spurious re-fire and duplicate-instead-of-loss.
    uint64_t missing = 0;
    for (auto& r : oracle_results) {
        auto key = std::make_pair(r.key, r.window.start.time_since_epoch().count());
        if (combined.find(key) == combined.end()) {
            missing++;
        }
    }
    EXPECT_EQ(missing, 0u) << "Checkpoint+lateness: restore lost " << missing
                           << "/" << oracle_results.size() << " results";
}

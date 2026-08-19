#include "nemesis/nemesis.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <map>
#include <set>
#include <unistd.h>

namespace stormglass {

namespace {

std::string CreateTempDir() {
    char tmpl[] = "/tmp/stormglass_nemesis_XXXXXX";
    char* result = ::mkdtemp(tmpl);
    if (!result) return "";
    return std::string(result);
}

void RemoveDir(const std::string& path) {
    std::filesystem::remove_all(path);
}

// Create a fake .tmp file to simulate an interrupted checkpoint write
void CreateStaleTmpFile(const std::string& dir, uint64_t offset) {
    char filename[128];
    std::snprintf(filename, sizeof(filename),
                  "%s/checkpoint-%020lu.ckpt.tmp", dir.c_str(), offset);
    int fd = ::open(filename, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        const char* garbage = "PARTIAL_CHECKPOINT_DATA";
        ::write(fd, garbage, 23);
        ::close(fd);
    }
}

// Canonical result key for deduplication
struct ResultKey {
    std::string key;
    int64_t window_start;
    int64_t window_end;

    bool operator<(const ResultKey& o) const {
        if (key != o.key) return key < o.key;
        if (window_start != o.window_start) return window_start < o.window_start;
        return window_end < o.window_end;
    }

    bool operator==(const ResultKey& o) const {
        return key == o.key && window_start == o.window_start &&
               window_end == o.window_end;
    }
};

struct ResultValue {
    int64_t sum;
    uint64_t count;
    bool operator==(const ResultValue& o) const {
        return sum == o.sum && count == o.count;
    }
};

using ResultMap = std::map<ResultKey, ResultValue>;

ResultMap ToResultMap(const std::vector<WindowResult>& results) {
    ResultMap map;
    for (const auto& r : results) {
        ResultKey rk{
            r.key,
            r.window.start.time_since_epoch().count(),
            r.window.end.time_since_epoch().count()
        };
        auto it = map.find(rk);
        if (it == map.end() || r.result.count >= it->second.count) {
            map[rk] = ResultValue{r.result.value, r.result.count};
        }
    }
    return map;
}

GeneratorConfig MakeGenConfig(const NemesisConfig& config) {
    return GeneratorConfig{
        .seed = config.seed,
        .num_keys = config.num_keys,
        .num_records = config.num_records,
        .max_disorder = config.max_disorder,
        .batch_size = 1024,
        .watermark_interval = 100,
    };
}

} // namespace

NemesisResult RunNemesis(const NemesisConfig& config) {
    NemesisResult result;
    std::string ckpt_dir = CreateTempDir();
    if (ckpt_dir.empty()) {
        result.failure_detail = "Failed to create temp dir";
        return result;
    }

    // Compute kill point
    auto kill_at = static_cast<uint64_t>(
        static_cast<double>(config.num_records) * config.kill_position);

    // Adjust kill point based on phase
    if (config.kill_phase == NemesisPhase::kBetweenCheckpoints) {
        uint64_t ckpt_boundary = (kill_at / config.checkpoint_interval) * config.checkpoint_interval;
        kill_at = ckpt_boundary + config.checkpoint_interval / 2;
    } else if (config.kill_phase == NemesisPhase::kMidCheckpoint) {
        kill_at = (kill_at / config.checkpoint_interval) * config.checkpoint_interval;
        if (kill_at == 0) kill_at = config.checkpoint_interval;
    }

    if (kill_at >= config.num_records) {
        kill_at = config.num_records - config.checkpoint_interval;
    }

    // --- Phase 1: Run pipeline until "crash" point ---
    std::vector<WindowResult> pre_crash_results;
    {
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();

        auto gen = std::make_unique<DeterministicGenerator>(MakeGenConfig(config));
        auto source = std::make_unique<StoppingSource>(std::move(gen), kill_at);
        auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

        PipelineConfig pconfig{};
        pconfig.checkpoint_dir = ckpt_dir;
        pconfig.checkpoint_interval = config.checkpoint_interval;

        Pipeline pipeline(std::move(source), std::move(assigner),
                          std::move(sink), pconfig);
        auto stats = pipeline.Run();
        result.records_before_kill = stats.records_processed;
        pre_crash_results = sink_ptr->Results();  // Copy before pipeline dies
    }

    // Simulate mid-checkpoint crash
    if (config.kill_phase == NemesisPhase::kMidCheckpoint) {
        CreateStaleTmpFile(ckpt_dir, kill_at);
    }

    // --- Phase 2: Run pipeline from checkpoint to completion ---
    std::vector<WindowResult> post_restore_results;
    {
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();

        auto gen = std::make_unique<DeterministicGenerator>(MakeGenConfig(config));
        auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

        PipelineConfig pconfig{};
        pconfig.checkpoint_dir = ckpt_dir;
        pconfig.checkpoint_interval = config.checkpoint_interval;

        Pipeline pipeline(std::move(gen), std::move(assigner),
                          std::move(sink), pconfig);
        auto stats = pipeline.Run();
        result.records_after_restore = stats.records_processed;
        post_restore_results = sink_ptr->Results();  // Copy before pipeline dies
    }

    // --- Phase 3: Run oracle (fresh, no checkpoint, full dataset) ---
    std::vector<WindowResult> oracle_results;
    {
        auto sink = std::make_unique<MemorySink>();
        auto* sink_ptr = sink.get();

        auto gen = std::make_unique<DeterministicGenerator>(MakeGenConfig(config));
        auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

        PipelineConfig pconfig{};
        Pipeline pipeline(std::move(gen), std::move(assigner),
                          std::move(sink), pconfig);
        pipeline.Run();
        oracle_results = sink_ptr->Results();  // Copy before pipeline dies
    }

    // --- Phase 4: Compare ---
    // Build deduped map from combined results
    ResultMap combined;
    for (const auto& r : pre_crash_results) {
        ResultKey rk{r.key, r.window.start.time_since_epoch().count(),
                      r.window.end.time_since_epoch().count()};
        combined[rk] = ResultValue{r.result.value, r.result.count};
    }
    for (const auto& r : post_restore_results) {
        ResultKey rk{r.key, r.window.start.time_since_epoch().count(),
                      r.window.end.time_since_epoch().count()};
        combined[rk] = ResultValue{r.result.value, r.result.count};
    }

    ResultMap oracle_map = ToResultMap(oracle_results);

    // Count duplicates
    result.duplicates_at_sink =
        (pre_crash_results.size() + post_restore_results.size()) - combined.size();

    // Check at-least-once: every oracle result must appear in combined
    result.missing_results = 0;
    for (const auto& [rk, val] : oracle_map) {
        auto it = combined.find(rk);
        if (it == combined.end()) {
            result.missing_results++;
        } else if (!(it->second == val)) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Value mismatch for key=%s window=[%ld,%ld): "
                    "expected sum=%ld count=%lu, got sum=%ld count=%lu",
                    rk.key.c_str(), rk.window_start, rk.window_end,
                    val.sum, val.count, it->second.sum, it->second.count);
                result.failure_detail = buf;
            }
        }
    }

    result.passed = (result.missing_results == 0);

    RemoveDir(ckpt_dir);
    return result;
}

} // namespace stormglass

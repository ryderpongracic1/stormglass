#include "nemesis/nemesis.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "sink/memory_sink.h"
#include "sink/durable_file_sink.h"
#include "window/tumbling.h"
#include "oracle/oracle.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"

#include <algorithm>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <dirent.h>
#include <fcntl.h>
#include <filesystem>
#include <map>
#include <set>
#include <sys/stat.h>
#include <sys/wait.h>
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
        .checkpoint_interval = config.checkpoint_interval,
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

// ---------------------------------------------------------------------------
// Real crash nemesis: fork() a child running the real pipeline, SIGKILL it.
// ---------------------------------------------------------------------------

namespace {

bool EndsWith(const std::string& s, const std::string& suffix) {
    return s.size() >= suffix.size() &&
           s.compare(s.size() - suffix.size(), suffix.size(), suffix) == 0;
}

uint32_t CountCompletedCheckpoints(const std::string& dir) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) return 0;
    uint32_t count = 0;
    struct dirent* entry;
    while ((entry = ::readdir(d)) != nullptr) {
        if (EndsWith(entry->d_name, ".ckpt")) ++count;  // ".ckpt.tmp" excluded
    }
    ::closedir(d);
    return count;
}

bool HasTmpFile(const std::string& dir) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) return false;
    bool found = false;
    struct dirent* entry;
    while ((entry = ::readdir(d)) != nullptr) {
        if (EndsWith(entry->d_name, ".tmp")) { found = true; break; }
    }
    ::closedir(d);
    return found;
}

bool PathExists(const std::string& path) {
    struct stat st{};
    return ::stat(path.c_str(), &st) == 0;
}

void SleepMicros(long micros) {
    if (micros <= 0) return;
    struct timespec ts{0, micros * 1000};
    ::nanosleep(&ts, nullptr);
}

GeneratorConfig MakeRealGenConfig(const RealKillConfig& config) {
    return GeneratorConfig{
        .seed = config.seed,
        .num_keys = config.num_keys,
        .num_records = config.num_records,
        .max_disorder = config.max_disorder,
        .batch_size = 1024,
        .watermark_interval = 100,
        .checkpoint_interval = config.checkpoint_interval,
    };
}

// Runs the real pipeline to completion in the forked child. Reached only if the
// parent fails to kill it in time — in which case it records a sentinel proving
// the final flush completed, so the parent can reject that attempt as non-genuine.
void RunRealKillChild(const RealKillConfig& config, const std::string& ckpt_dir,
                      const std::string& sink_path, const std::string& sentinel_path) {
    auto gen = std::make_unique<DeterministicGenerator>(MakeRealGenConfig(config));
    auto sink = std::make_unique<DurableFileSink>(sink_path);
    auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

    PipelineConfig pconfig{};
    pconfig.checkpoint_dir = ckpt_dir;
    pconfig.checkpoint_interval = config.checkpoint_interval;

    Pipeline pipeline(std::move(gen), std::move(assigner), std::move(sink), pconfig);
    pipeline.Run();  // includes final flush of all in-memory windows

    // Only reached on a clean drain. The sentinel lets the parent detect that
    // the kill lost the race and this attempt is not a genuine crash.
    int fd = ::open(sentinel_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        const char done = 'D';
        [[maybe_unused]] auto n = ::write(fd, &done, 1);
        ::fsync(fd);
        ::close(fd);
    }
}

} // namespace

RealKillResult RunRealKillNemesis(const RealKillConfig& config) {
    RealKillResult result;

    std::string workdir;
    bool captured = false;
    std::string ckpt_dir, sink_pre, sink_post, sentinel;

    for (uint32_t attempt = 1; attempt <= config.max_attempts; ++attempt) {
        result.attempts = attempt;

        workdir = CreateTempDir();
        if (workdir.empty()) {
            result.failure_detail = "Failed to create temp dir";
            return result;
        }
        ckpt_dir = workdir + "/ckpt";
        std::filesystem::create_directory(ckpt_dir);
        sink_pre = workdir + "/pre.bin";
        sink_post = workdir + "/post.bin";
        sentinel = workdir + "/done";

        pid_t pid = ::fork();
        if (pid < 0) {
            RemoveDir(workdir);
            result.failure_detail = "fork() failed";
            return result;
        }

        if (pid == 0) {
            // Child: never returns to the caller (tests/CLI) — _exit skips atexit
            // handlers so inherited stdio buffers are not double-flushed.
            RunRealKillChild(config, ckpt_dir, sink_pre, sentinel);
            ::_exit(0);
        }

        // Parent: kill the child at the targeted point.
        bool killed = false;
        bool child_reaped = false;
        int status = 0;

        if (config.kill_point == RealKillPoint::kBetweenCheckpoints) {
            while (true) {
                pid_t w = ::waitpid(pid, &status, WNOHANG);
                if (w == pid) { child_reaped = true; break; }
                if (CountCompletedCheckpoints(ckpt_dir) >= config.target_checkpoint) {
                    ::kill(pid, SIGKILL);
                    killed = true;
                    break;
                }
                SleepMicros(50);
            }
        } else {  // kMidCheckpoint: kill during a checkpoint write, after at
                  // least target_checkpoint completed checkpoints exist, so the
                  // restore must both discard the stale .tmp AND fall back to the
                  // last valid checkpoint.
            bool armed = false;
            while (true) {
                pid_t w = ::waitpid(pid, &status, WNOHANG);
                if (w == pid) { child_reaped = true; break; }
                if (!armed) {
                    if (CountCompletedCheckpoints(ckpt_dir) >= config.target_checkpoint) {
                        armed = true;
                    } else {
                        SleepMicros(50);
                        continue;
                    }
                }
                // Armed: kill the instant a new checkpoint .tmp appears. The
                // .tmp/fsync/rename window is brief, so poll tightly.
                if (HasTmpFile(ckpt_dir)) {
                    ::kill(pid, SIGKILL);
                    killed = true;
                    break;
                }
            }
        }

        if (!child_reaped) {
            ::waitpid(pid, &status, 0);
        }

        bool by_sigkill = killed && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
        bool sentinel_present = PathExists(sentinel);
        bool tmp_present = HasTmpFile(ckpt_dir);

        // A genuine crash: SIGKILL landed before the child could final-flush.
        bool genuine = by_sigkill && !sentinel_present;
        if (config.kill_point == RealKillPoint::kMidCheckpoint) {
            // Must have interrupted a real checkpoint write.
            genuine = genuine && tmp_present;
        }

        if (!genuine) {
            RemoveDir(workdir);
            continue;  // retry with a fresh fork
        }

        result.killed_by_sigkill = by_sigkill;
        result.final_flush_completed = sentinel_present;
        result.stale_tmp_after_kill = tmp_present;
        captured = true;
        break;
    }

    if (!captured) {
        result.failure_detail =
            "Could not capture a genuine crash within max_attempts";
        if (!workdir.empty()) RemoveDir(workdir);
        return result;
    }

    // --- Restart: fresh pipeline restores from the last valid checkpoint and
    //     drains to completion into a second durable sink. The interrupted .tmp
    //     (if any) is discarded by the reader's stale-.tmp recovery. ---
    {
        auto gen = std::make_unique<DeterministicGenerator>(MakeRealGenConfig(config));
        auto sink = std::make_unique<DurableFileSink>(sink_post);
        auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

        PipelineConfig pconfig{};
        pconfig.checkpoint_dir = ckpt_dir;
        pconfig.checkpoint_interval = config.checkpoint_interval;

        Pipeline pipeline(std::move(gen), std::move(assigner), std::move(sink), pconfig);
        auto stats = pipeline.Run();
        result.restored_offset = stats.records_replayed;
    }

    auto pre = DurableFileSink::ReadAll(sink_pre);
    auto post = DurableFileSink::ReadAll(sink_post);
    result.pre_crash_emits = pre.size();
    result.post_restore_emits = post.size();

    // Union of durable pre-crash output and post-restore output.
    ResultMap combined;
    auto absorb = [&combined](const std::vector<WindowResult>& results) {
        for (const auto& r : results) {
            ResultKey rk{r.key, r.window.start.time_since_epoch().count(),
                         r.window.end.time_since_epoch().count()};
            combined[rk] = ResultValue{r.result.value, r.result.count};
        }
    };
    absorb(pre);
    absorb(post);
    result.union_results = combined.size();
    result.duplicates = (pre.size() + post.size()) - combined.size();

    // Oracle over the full dataset (naive group-by-window, no watermarks).
    Oracle oracle(OracleConfig{.window_size = config.window_size});
    DeterministicGenerator oracle_gen(MakeRealGenConfig(config));
    while (auto batch = oracle_gen.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            }
        }
    }
    auto oracle_results = oracle.ComputeResults();
    result.oracle_results = oracle_results.size();

    result.missing_results = 0;
    for (const auto& r : oracle_results) {
        ResultKey rk{r.key, r.window.start.time_since_epoch().count(),
                     r.window.end.time_since_epoch().count()};
        auto it = combined.find(rk);
        if (it == combined.end()) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Missing result: key=%s window=[%ld,%ld)",
                    r.key.c_str(), rk.window_start, rk.window_end);
                result.failure_detail = buf;
            }
        } else if (it->second.sum != r.result.value ||
                   it->second.count != r.result.count) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Value mismatch key=%s window=[%ld,%ld): "
                    "oracle sum=%ld count=%lu, got sum=%ld count=%lu",
                    r.key.c_str(), rk.window_start, rk.window_end,
                    r.result.value, r.result.count,
                    it->second.sum, it->second.count);
                result.failure_detail = buf;
            }
        }
    }

    result.passed = (result.missing_results == 0);

    RemoveDir(workdir);
    return result;
}

} // namespace stormglass

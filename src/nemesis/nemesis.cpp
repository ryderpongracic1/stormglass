#include "nemesis/nemesis.h"
#include "engine/pipeline.h"
#include "engine/partitioned_pipeline.h"
#include "checkpoint/distributed_checkpoint.h"
#include "source/generator.h"
#include "source/stopping_source.h"
#include "source/source_merge.h"
#include "sink/memory_sink.h"
#include "sink/durable_file_sink.h"
#include "window/tumbling.h"
#include "oracle/oracle.h"
#include "checkpoint/writer.h"
#include "checkpoint/reader.h"

#include <cinttypes>
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
                  "%s/checkpoint-%020" PRIu64 ".ckpt.tmp", dir.c_str(), offset);
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
                    "Value mismatch for key=%s window=[%" PRId64 ",%" PRId64 "): "
                    "expected sum=%" PRId64 " count=%" PRIu64 ", got sum=%" PRId64 " count=%" PRIu64 "",
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
        .disorder_mode = config.disorder_mode,
        .late_fraction = config.late_fraction,
        .late_tail = config.late_tail,
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
    pconfig.allowed_lateness = config.allowed_lateness;

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
        pconfig.allowed_lateness = config.allowed_lateness;

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

    // Oracle over the full dataset. With allowed_lateness > 0 the oracle must
    // be watermark-fed so it predicts the same beyond-deadline drops the engine
    // makes; otherwise it would include late-dropped records and its window
    // values would diverge from the (correct) crash-recovered engine output.
    Oracle oracle(OracleConfig{.window_size = config.window_size,
                               .allowed_lateness = config.allowed_lateness});
    DeterministicGenerator oracle_gen(MakeRealGenConfig(config));
    while (auto batch = oracle_gen.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            } else if (auto* c = std::get_if<ControlRecord>(&item)) {
                if (c->type == ControlType::kWatermark) {
                    oracle.AdvanceWatermark(c->watermark);
                }
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
                    "Missing result: key=%s window=[%" PRId64 ",%" PRId64 ")",
                    r.key.c_str(), rk.window_start, rk.window_end);
                result.failure_detail = buf;
            }
        } else if (it->second.sum != r.result.value ||
                   it->second.count != r.result.count) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Value mismatch key=%s window=[%" PRId64 ",%" PRId64 "): "
                    "oracle sum=%" PRId64 " count=%" PRIu64 ", got sum=%" PRId64 " count=%" PRIu64 "",
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

// ---------------------------------------------------------------------------
// Partitioned real crash nemesis: fork() a child running PartitionedPipeline
// with per-worker durable sinks + distributed checkpointing, SIGKILL it at a
// torn global-checkpoint state, restore, and verify at-least-once + complete
// fallback.
// ---------------------------------------------------------------------------

namespace {

GeneratorConfig MakePartGenConfig(const PartitionedRealKillConfig& config) {
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

std::function<std::unique_ptr<Sink>(uint32_t)> MakeDurableFactory(
    const std::string& prefix) {
    return [prefix](uint32_t k) -> std::unique_ptr<Sink> {
        return std::make_unique<DurableFileSink>(prefix + std::to_string(k) + ".bin");
    };
}

// Runs the partitioned pipeline to completion in the forked child. Reached only
// if the parent fails to kill it in time — it then records a sentinel proving
// the drain (and final flush) completed, so the parent rejects the attempt.
void RunPartitionedRealKillChild(const PartitionedRealKillConfig& config,
                                 const std::string& ckpt_root,
                                 const std::string& sink_prefix,
                                 const std::string& sentinel_path) {
    PartitionedPipelineConfig pconfig{};
    pconfig.num_workers = config.num_workers;
    pconfig.allowed_lateness = config.allowed_lateness;
    pconfig.checkpoint_dir = ckpt_root;
    pconfig.worker_sink_factory = MakeDurableFactory(sink_prefix);

    const Duration window_size = config.window_size;
    PartitionedPipeline pipeline(
        std::make_unique<DeterministicGenerator>(MakePartGenConfig(config)),
        [window_size] { return std::make_unique<TumblingAssigner>(window_size); },
        std::make_unique<MemorySink>(),  // caller sink unused (workers self-persist)
        pconfig);
    pipeline.Run();  // includes final flush of every worker's in-memory windows

    int fd = ::open(sentinel_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        const char done = 'D';
        [[maybe_unused]] auto n = ::write(fd, &done, 1);
        ::fsync(fd);
        ::close(fd);
    }
}

} // namespace

PartitionedRealKillResult RunPartitionedRealKillNemesis(
    const PartitionedRealKillConfig& config) {
    PartitionedRealKillResult result;
    result.num_workers = config.num_workers;
    const uint32_t n = std::max<uint32_t>(1, config.num_workers);
    const uint64_t arm_offset =
        static_cast<uint64_t>(config.target_checkpoint) * config.checkpoint_interval;

    std::string workdir, ckpt_root, sink_pre, sink_post, sentinel;
    bool captured = false;
    uint64_t captured_complete = 0, captured_torn = 0;

    for (uint32_t attempt = 1; attempt <= config.max_attempts; ++attempt) {
        result.attempts = attempt;

        workdir = CreateTempDir();
        if (workdir.empty()) {
            result.failure_detail = "Failed to create temp dir";
            return result;
        }
        ckpt_root = workdir + "/ckpt";
        std::filesystem::create_directory(ckpt_root);
        sink_pre = workdir + "/pre-p";
        sink_post = workdir + "/post-p";
        sentinel = workdir + "/done";

        pid_t pid = ::fork();
        if (pid < 0) {
            RemoveDir(workdir);
            result.failure_detail = "fork() failed";
            return result;
        }
        if (pid == 0) {
            RunPartitionedRealKillChild(config, ckpt_root, sink_pre, sentinel);
            ::_exit(0);
        }

        // Parent: arm once a complete global checkpoint deep enough exists, then
        // SIGKILL the instant a torn (higher, incomplete) global checkpoint is
        // on disk — some partition wrote O_hi but not all have.
        bool killed = false, child_reaped = false, armed = false;
        int status = 0;
        while (true) {
            pid_t w = ::waitpid(pid, &status, WNOHANG);
            if (w == pid) { child_reaped = true; break; }

            auto complete = HighestCompleteCheckpoint(ckpt_root, n);
            if (!armed) {
                if (complete && *complete >= arm_offset) armed = true;
                else { SleepMicros(50); continue; }
            }
            auto partial = HighestPartialCheckpoint(ckpt_root, n);
            if (complete && partial && *partial > *complete) {
                // Freeze the child before killing: on fast fsync (e.g. Apple
                // silicon/APFS) the torn window between the (N-1)th and Nth
                // partition rename is near-instantaneous, and the final rename
                // can land between this detection and the SIGKILL, healing the
                // torn state and wasting the attempt (observed: 3/10 capture
                // failures at N=2 on an M-series Mac). SIGSTOP freezes the
                // child; re-verify tornness while frozen (an already-in-flight
                // rename may still complete), then SIGKILL -- which is
                // delivered even to a stopped process -- so the persisted torn
                // state cannot change. If the race was lost, resume and retry.
                ::kill(pid, SIGSTOP);
                auto c2 = HighestCompleteCheckpoint(ckpt_root, n);
                auto p2 = HighestPartialCheckpoint(ckpt_root, n);
                if (c2 && p2 && *p2 > *c2) {
                    ::kill(pid, SIGKILL);
                    killed = true;
                    break;
                }
                ::kill(pid, SIGCONT);
            }
        }

        if (!child_reaped) {
            ::waitpid(pid, &status, 0);
        }

        // Re-evaluate on the PERSISTED disk (a renamed file survives SIGKILL).
        auto complete = HighestCompleteCheckpoint(ckpt_root, n);
        auto partial = HighestPartialCheckpoint(ckpt_root, n);
        bool by_sigkill = killed && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
        bool sentinel_present = PathExists(sentinel);
        bool torn = complete.has_value() && partial.has_value() && *partial > *complete;

        bool genuine = by_sigkill && !sentinel_present && torn;
        if (!genuine) {
            RemoveDir(workdir);
            continue;
        }

        result.killed_by_sigkill = by_sigkill;
        result.final_flush_completed = sentinel_present;
        result.torn_checkpoint_observed = torn;
        captured_complete = *complete;
        captured_torn = *partial;
        result.torn_offset = captured_torn;
        captured = true;
        break;
    }

    if (!captured) {
        result.failure_detail =
            "Could not capture a genuine torn-state crash within max_attempts";
        if (!workdir.empty()) RemoveDir(workdir);
        return result;
    }

    // --- Restart: fresh partitioned pipeline restores from the highest COMPLETE
    //     global checkpoint (discarding the torn O_hi) and drains to completion
    //     into a second set of per-worker durable sinks. ---
    {
        PartitionedPipelineConfig pconfig{};
        pconfig.num_workers = n;
        pconfig.allowed_lateness = config.allowed_lateness;
        pconfig.checkpoint_dir = ckpt_root;
        pconfig.worker_sink_factory = MakeDurableFactory(sink_post);

        const Duration window_size = config.window_size;
        PartitionedPipeline pipeline(
            std::make_unique<DeterministicGenerator>(MakePartGenConfig(config)),
            [window_size] { return std::make_unique<TumblingAssigner>(window_size); },
            std::make_unique<MemorySink>(),
            pconfig);
        auto stats = pipeline.Run();
        result.restored_offset = stats.records_replayed;
    }

    // Restore MUST have used the complete offset, never the torn one.
    if (result.restored_offset != captured_complete) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "restore used offset %" PRIu64 ", expected complete offset %" PRIu64 " (torn=%" PRIu64 ")",
            result.restored_offset, captured_complete, captured_torn);
        result.failure_detail = buf;
    }
    if (result.restored_offset >= captured_torn) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "restore offset %" PRIu64 " did not fall back below torn offset %" PRIu64 "",
            result.restored_offset, captured_torn);
        if (result.failure_detail.empty()) result.failure_detail = buf;
    }

    // --- Union durable pre-crash + post-restore output across ALL workers ---
    ResultMap combined;
    auto absorb = [&combined](const std::vector<WindowResult>& results) {
        for (const auto& r : results) {
            ResultKey rk{r.key, r.window.start.time_since_epoch().count(),
                         r.window.end.time_since_epoch().count()};
            combined[rk] = ResultValue{r.result.value, r.result.count};
        }
    };
    uint64_t pre_total = 0, post_total = 0;
    for (uint32_t k = 0; k < n; ++k) {
        auto pre = DurableFileSink::ReadAll(sink_pre + std::to_string(k) + ".bin");
        auto post = DurableFileSink::ReadAll(sink_post + std::to_string(k) + ".bin");
        pre_total += pre.size();
        post_total += post.size();
        absorb(pre);
        absorb(post);
    }
    result.pre_crash_emits = pre_total;
    result.post_restore_emits = post_total;
    result.union_results = combined.size();
    result.duplicates = (pre_total + post_total) - combined.size();

    // --- Oracle over the full dataset (fed identically to the single-threaded
    //     real-kill) ---
    Oracle oracle(OracleConfig{.window_size = config.window_size,
                               .allowed_lateness = config.allowed_lateness});
    DeterministicGenerator oracle_gen(MakePartGenConfig(config));
    while (auto batch = oracle_gen.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            } else if (auto* c = std::get_if<ControlRecord>(&item)) {
                if (c->type == ControlType::kWatermark) {
                    oracle.AdvanceWatermark(c->watermark);
                }
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
                    "Missing result: key=%s window=[%" PRId64 ",%" PRId64 ")",
                    r.key.c_str(), rk.window_start, rk.window_end);
                result.failure_detail = buf;
            }
        } else if (it->second.sum != r.result.value ||
                   it->second.count != r.result.count) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Value mismatch key=%s window=[%" PRId64 ",%" PRId64 "): "
                    "oracle sum=%" PRId64 " count=%" PRIu64 ", got sum=%" PRId64 " count=%" PRIu64 "",
                    r.key.c_str(), rk.window_start, rk.window_end,
                    r.result.value, r.result.count,
                    it->second.sum, it->second.count);
                result.failure_detail = buf;
            }
        }
    }

    result.passed = (result.missing_results == 0) && result.failure_detail.empty();

    RemoveDir(workdir);
    return result;
}

// ---------------------------------------------------------------------------
// Mid-ALIGNMENT crash nemesis (v3 Phase 3): fork() a child running the real
// Pipeline over a SourceMerge with divergent per-source barrier cadences, SIGKILL
// it WHILE K-way alignment is in progress, restore, and verify fallback to the
// last COMPLETE aligned checkpoint + at-least-once.
// ---------------------------------------------------------------------------

namespace {

// A Source decorator that forwards to a SourceMerge and, after every pull, writes
// the merge's live ALIGNMENT state to a marker file so the parent can detect a
// mid-alignment moment. Uses the SourceMerge Phase-3 introspection accessors —
// SourceMerge itself is unchanged and single-threaded. The pipeline drives this
// exactly like any other Source.
class AlignmentProbeSource : public Source {
public:
    AlignmentProbeSource(SourceMergeConfig cfg, std::string marker_path, std::size_t k)
        : merge_(std::move(cfg)), marker_(std::move(marker_path)), k_(k) {}

    std::optional<Batch> Next() override {
        auto b = merge_.Next();
        WriteMarker();
        return b;
    }
    void Seek(uint64_t offset) override { merge_.Seek(offset); }
    [[nodiscard]] uint64_t CurrentOffset() const override { return merge_.CurrentOffset(); }

private:
    void WriteMarker() {
        uint32_t delivered = 0;
        const uint64_t closed = merge_.EpochsClosed();
        for (std::size_t i = 0; i < k_; ++i) {
            if (merge_.BarriersSeen(i) > closed) ++delivered;  // blocked at the open epoch
        }
        std::string tmp = marker_ + ".tmp";
        FILE* f = std::fopen(tmp.c_str(), "w");
        if (!f) return;
        if (delivered > 0 && delivered < k_) {
            // PARTIAL: some (not all) channels delivered the open epoch's barrier.
            std::fprintf(f, "PARTIAL %" PRIu64 " %u %u %" PRIu64 "\n",
                         closed + 1, delivered, static_cast<unsigned>(k_),
                         merge_.CurrentOffset());
        } else {
            std::fprintf(f, "NONE\n");
        }
        std::fclose(f);
        std::rename(tmp.c_str(), marker_.c_str());  // atomic swap for the reader
    }

    SourceMerge merge_;
    std::string marker_;
    std::size_t k_;
};

struct MarkerState {
    bool partial = false;
    uint64_t epoch = 0;
    uint32_t delivered = 0;
    uint32_t k = 0;
    uint64_t offset = 0;
};

MarkerState ReadMarker(const std::string& path) {
    MarkerState m;
    FILE* f = std::fopen(path.c_str(), "r");
    if (!f) return m;
    char tag[16] = {0};
    if (std::fscanf(f, "%15s", tag) == 1 && std::strcmp(tag, "PARTIAL") == 0) {
        unsigned long long epoch = 0, offset = 0;
        unsigned d = 0, k = 0;
        if (std::fscanf(f, "%llu %u %u %llu", &epoch, &d, &k, &offset) == 4) {
            m.partial = true;
            m.epoch = epoch;
            m.delivered = d;
            m.k = k;
            m.offset = offset;
        }
    }
    std::fclose(f);
    return m;
}

// Highest COMPLETE single-file checkpoint offset on disk (.ckpt, excluding .tmp),
// parsed from the "checkpoint-<offset>.ckpt" filename.
std::optional<uint64_t> HighestCompleteSingleCheckpoint(const std::string& dir) {
    DIR* d = ::opendir(dir.c_str());
    if (!d) return std::nullopt;
    std::optional<uint64_t> best;
    struct dirent* entry;
    while ((entry = ::readdir(d)) != nullptr) {
        std::string name = entry->d_name;
        if (!EndsWith(name, ".ckpt")) continue;  // ".ckpt.tmp" excluded
        auto dash = name.find('-');
        if (dash == std::string::npos) continue;
        uint64_t off = std::strtoull(name.c_str() + dash + 1, nullptr, 10);
        if (!best || off > *best) best = off;
    }
    ::closedir(d);
    return best;
}

// The divergent-cadence merge config: two sources, fast + slow barrier intervals.
SourceMergeConfig MakeAlignmentMergeConfig(const AlignmentKillConfig& config) {
    SourceMergeConfig mc;
    mc.merged_batch_size = 100;   // < S so a batch stays within ~one epoch
    mc.checkpoint_interval = 0;   // per-source intervals drive the barriers

    GeneratorConfig fast{};
    fast.seed = config.seed;
    fast.num_keys = config.num_keys;
    fast.num_records = config.num_records;
    fast.event_time_step = 2;     // faster event-time
    fast.max_disorder = config.max_disorder;
    fast.batch_size = 512;
    fast.watermark_interval = 100;
    fast.checkpoint_interval = config.interval_fast;

    GeneratorConfig slow{};
    slow.seed = config.seed + 7919u;
    slow.num_keys = config.num_keys;
    slow.num_records = config.num_records;
    slow.event_time_step = 1;     // slower event-time (pins the MIN)
    slow.max_disorder = config.max_disorder;
    slow.batch_size = 512;
    slow.watermark_interval = 100;
    slow.checkpoint_interval = config.interval_slow;

    mc.sources = {fast, slow};
    return mc;
}

void RunAlignmentKillChild(const AlignmentKillConfig& config,
                           const std::string& ckpt_dir, const std::string& sink_path,
                           const std::string& marker_path,
                           const std::string& sentinel_path) {
    auto mc = MakeAlignmentMergeConfig(config);
    auto source = std::make_unique<AlignmentProbeSource>(mc, marker_path,
                                                         mc.sources.size());
    auto sink = std::make_unique<DurableFileSink>(sink_path);
    auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

    PipelineConfig pconfig{};
    pconfig.checkpoint_dir = ckpt_dir;
    // Only used as the "checkpointing enabled" flag; the merged barriers drive
    // WHEN checkpoints are written.
    pconfig.checkpoint_interval = config.interval_fast + config.interval_slow;

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink), pconfig);
    pipeline.Run();  // includes final flush

    int fd = ::open(sentinel_path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd >= 0) {
        const char done = 'D';
        [[maybe_unused]] auto n = ::write(fd, &done, 1);
        ::fsync(fd);
        ::close(fd);
    }
}

}  // namespace

AlignmentKillResult RunAlignmentKillNemesis(const AlignmentKillConfig& config) {
    AlignmentKillResult result;
    const uint64_t S = config.interval_fast + config.interval_slow;
    result.merged_stride = S;
    result.num_channels = 2;

    std::string workdir, ckpt_dir, sink_pre, sink_post, marker, sentinel;
    bool captured = false;
    uint64_t captured_complete = 0;  // highest complete (aligned) checkpoint AT KILL time

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
        marker = workdir + "/align.marker";
        sentinel = workdir + "/done";

        pid_t pid = ::fork();
        if (pid < 0) {
            RemoveDir(workdir);
            result.failure_detail = "fork() failed";
            return result;
        }
        if (pid == 0) {
            RunAlignmentKillChild(config, ckpt_dir, sink_pre, marker, sentinel);
            ::_exit(0);
        }

        // Parent: arm once `target_checkpoint` COMPLETE merged checkpoints exist,
        // then SIGKILL the instant a PARTIAL alignment (some channels delivered
        // the open barrier, others not) is observed for an epoch above the
        // fallback. Freeze-verify-kill so the persisted state cannot change.
        bool killed = false, child_reaped = false, armed = false;
        int status = 0;
        MarkerState kill_marker;
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
            MarkerState ms = ReadMarker(marker);
            // Mid-alignment for an epoch strictly above at least one complete
            // checkpoint (epoch-1 >= 1) is a genuine partial cut we can kill on.
            if (ms.partial && ms.epoch >= 2 && ms.delivered >= 1 && ms.delivered < ms.k) {
                ::kill(pid, SIGSTOP);
                MarkerState m2 = ReadMarker(marker);
                if (m2.partial && m2.delivered >= 1 && m2.delivered < m2.k &&
                    !PathExists(sentinel)) {
                    ::kill(pid, SIGKILL);
                    killed = true;
                    kill_marker = m2;
                    break;
                }
                ::kill(pid, SIGCONT);
            }
        }

        if (!child_reaped) {
            ::waitpid(pid, &status, 0);
        }

        bool by_sigkill = killed && WIFSIGNALED(status) && WTERMSIG(status) == SIGKILL;
        bool sentinel_present = PathExists(sentinel);

        bool genuine = by_sigkill && !sentinel_present && kill_marker.partial;
        if (!genuine) {
            RemoveDir(workdir);
            continue;  // retry with a fresh fork
        }

        result.killed_by_sigkill = by_sigkill;
        result.final_flush_completed = sentinel_present;
        result.alignment_partial_at_kill = kill_marker.partial;
        result.epoch_in_progress = kill_marker.epoch;
        result.channels_delivered = kill_marker.delivered;
        result.num_channels = kill_marker.k;
        // Snapshot the highest COMPLETE (fully-aligned) checkpoint NOW, before the
        // restore run writes fresh checkpoints into the same directory.
        if (auto hc = HighestCompleteSingleCheckpoint(ckpt_dir)) captured_complete = *hc;
        captured = true;
        break;
    }

    if (!captured) {
        result.failure_detail =
            "Could not capture a genuine mid-alignment crash within max_attempts";
        if (!workdir.empty()) RemoveDir(workdir);
        return result;
    }

    // --- Restart: fresh pipeline restores from the last COMPLETE (fully-aligned)
    //     merged checkpoint and drains to completion into a second durable sink.
    {
        auto mc = MakeAlignmentMergeConfig(config);
        auto source = std::make_unique<SourceMerge>(mc);  // no probe needed on restore
        auto sink = std::make_unique<DurableFileSink>(sink_post);
        auto assigner = std::make_unique<TumblingAssigner>(config.window_size);

        PipelineConfig pconfig{};
        pconfig.checkpoint_dir = ckpt_dir;
        pconfig.checkpoint_interval = S;

        Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink), pconfig);
        auto stats = pipeline.Run();
        result.restored_offset = stats.records_replayed;
    }

    // Restore MUST have used a COMPLETE aligned cut: a positive multiple of the
    // merged stride S, matching the highest complete checkpoint captured AT KILL
    // time (the restore run itself writes fresh checkpoints, so we compare against
    // the pre-restore snapshot), and strictly below the mid-alignment epoch's cut
    // (its merged barrier was never emitted, so no checkpoint for it can exist).
    if (result.restored_offset == 0 || result.restored_offset % S != 0) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "restore offset %" PRIu64 " is not a positive multiple of the merged "
            "stride %" PRIu64 " (not a fully-aligned cut)",
            result.restored_offset, S);
        result.failure_detail = buf;
    } else if (result.restored_offset != captured_complete) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "restore offset %" PRIu64 " != highest complete checkpoint at kill %" PRIu64 "",
            result.restored_offset, captured_complete);
        result.failure_detail = buf;
    } else if (result.restored_offset >= result.epoch_in_progress * S) {
        char buf[256];
        std::snprintf(buf, sizeof(buf),
            "restore offset %" PRIu64 " did not fall back below the mid-alignment "
            "epoch cut %" PRIu64 "",
            result.restored_offset, result.epoch_in_progress * S);
        result.failure_detail = buf;
    }

    // --- Union durable pre-crash + post-restore output ---
    auto pre = DurableFileSink::ReadAll(sink_pre);
    auto post = DurableFileSink::ReadAll(sink_post);
    result.pre_crash_emits = pre.size();
    result.post_restore_emits = post.size();

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

    // --- Oracle over the full MERGED stream (a fresh SourceMerge with the same
    //     config), fed records + min-combined watermarks. Barriers do not affect
    //     aggregation, so the oracle stays byte-unmodified. ---
    Oracle oracle(OracleConfig{.window_size = config.window_size,
                               .allowed_lateness = Duration{0}});
    SourceMerge oracle_src(MakeAlignmentMergeConfig(config));
    while (auto batch = oracle_src.Next()) {
        for (auto& item : batch->items) {
            if (auto* r = std::get_if<Record>(&item)) {
                oracle.AddRecord(*r);
            } else if (auto* c = std::get_if<ControlRecord>(&item)) {
                if (c->type == ControlType::kWatermark) {
                    oracle.AdvanceWatermark(c->watermark);
                }
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
                    "Missing result: key=%s window=[%" PRId64 ",%" PRId64 ")",
                    r.key.c_str(), rk.window_start, rk.window_end);
                result.failure_detail = buf;
            }
        } else if (it->second.sum != r.result.value ||
                   it->second.count != r.result.count) {
            result.missing_results++;
            if (result.failure_detail.empty()) {
                char buf[256];
                std::snprintf(buf, sizeof(buf),
                    "Value mismatch key=%s window=[%" PRId64 ",%" PRId64 "): "
                    "oracle sum=%" PRId64 " count=%" PRIu64 ", got sum=%" PRId64 " count=%" PRIu64 "",
                    r.key.c_str(), rk.window_start, rk.window_end,
                    r.result.value, r.result.count,
                    it->second.sum, it->second.count);
                result.failure_detail = buf;
            }
        }
    }

    result.passed = (result.missing_results == 0) && result.failure_detail.empty();

    RemoveDir(workdir);
    return result;
}

} // namespace stormglass

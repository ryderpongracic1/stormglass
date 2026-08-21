#include "engine/pipeline.h"
#include "engine/partitioned_pipeline.h"
#include "source/generator.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "window/state.h"
#include "checkpoint/writer.h"
#include "stream/record.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <iostream>
#include <cstdlib>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>
#include <unistd.h>

using namespace stormglass;
using Clock = std::chrono::high_resolution_clock;

static void BenchmarkPipeline(uint64_t num_records) {
    GeneratorConfig config;
    config.num_records = num_records;
    config.num_keys = 100;
    config.batch_size = 4096;
    config.watermark_interval = 500;
    config.max_disorder = Duration{5000};

    auto source = std::make_unique<DeterministicGenerator>(config);
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000}); // 1 second tumbling windows
    auto sink = std::make_unique<MemorySink>();

    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));

    auto start = Clock::now();
    auto stats = pipeline.Run();
    auto end = Clock::now();

    double ms = std::chrono::duration<double, std::milli>(end - start).count();
    double mrecs = static_cast<double>(stats.records_processed) / 1'000'000.0;
    double mrecs_per_sec = mrecs / (ms / 1000.0);

    std::cout << "  " << num_records / 1000 << "K records:  "
              << mrecs_per_sec << " M rec/s  ("
              << stats.windows_fired << " windows, "
              << static_cast<int>(ms) << "ms)\n";
}

// Measure the checkpoint pause directly: build a KeyedWindowState with a
// known number of panes, then time WriteCheckpoint (serialize + fsync +
// rename + dir-fsync) in isolation. This is the pause the operator
// experiences at each barrier.
static void BenchmarkCheckpoint() {
    std::cout << "=== Checkpoint Pause (direct measurement) ===\n";

    char tmpl[] = "/tmp/stormglass_ckpt_bench_XXXXXX";
    char* dir_c = ::mkdtemp(tmpl);
    if (!dir_c) {
        std::cout << "  (skipped: mkdtemp failed)\n\n";
        return;
    }
    std::string dir = dir_c;

    for (size_t num_panes : {100, 1000, 10000}) {
        // Build state: num_panes panes across 10 keys
        KeyedWindowState state;
        size_t windows = num_panes / 10;
        for (size_t w = 0; w < windows; ++w) {
            Timestamp start{Duration{static_cast<int64_t>(w) * 1000}};
            Window window{start, start + Duration{1000}};
            for (int k = 0; k < 10; ++k) {
                char key[16];
                std::snprintf(key, sizeof(key), "key-%04d", k);
                state.Add(key, window, 42);
            }
        }

        constexpr int kWrites = 20;
        auto start_t = Clock::now();
        for (int i = 0; i < kWrites; ++i) {
            CheckpointWriter writer(dir);
            writer.WriteCheckpoint(static_cast<uint64_t>(i + 1) * 1000,
                                   Timestamp{Duration{1000000}}, state);
        }
        auto end_t = Clock::now();
        double total_ms = std::chrono::duration<double, std::milli>(end_t - start_t).count();
        std::cout << "  " << num_panes << " panes: "
                  << (total_ms / kWrites) << " ms/checkpoint  ("
                  << kWrites << " writes incl. fsync + rename + dir-fsync)\n";
    }

    std::string cmd = "rm -rf " + dir;
    [[maybe_unused]] int rc = std::system(cmd.c_str());
    std::cout << "\n";
}

// ===========================================================================
// Partitioned engine benchmarks (Phase 4). All numbers below come from the
// PartitionedPipeline / single-threaded Pipeline on the box this runs on; the
// harness prints median + observed min/max over multiple reps. Reproduce with
// `make bench`.
// ===========================================================================

namespace {

// Median + observed min/max over a set of reps.
struct Agg { double median = 0, lo = 0, hi = 0; };
Agg Aggregate(std::vector<double> v) {
    std::sort(v.begin(), v.end());
    Agg a;
    a.lo = v.front();
    a.hi = v.back();
    const size_t n = v.size();
    a.median = (n % 2) ? v[n / 2] : (v[n / 2 - 1] + v[n / 2]) / 2.0;
    return a;
}

// The single shared scaling workload: 1M records, 1000 keys, tumbling 1s.
GeneratorConfig ScalingGenConfig() {
    GeneratorConfig c{};
    c.seed = 42;
    c.num_keys = 1000;
    c.num_records = 1'000'000;
    c.batch_size = 4096;
    c.watermark_interval = 500;
    c.max_disorder = Duration{5000};
    return c;  // checkpoint_interval left 0 (no barriers)
}

std::function<std::unique_ptr<WindowAssigner>()> TumblingFactory() {
    return [] { return std::make_unique<TumblingAssigner>(Duration{1000}); };
}

double Throughput(uint64_t records, double sec) {
    return static_cast<double>(records) / sec / 1e6;  // M rec/s
}

// One single-threaded Pipeline run over the scaling workload.
double RunSingleThreadedOnce() {
    auto source = std::make_unique<DeterministicGenerator>(ScalingGenConfig());
    auto assigner = std::make_unique<TumblingAssigner>(Duration{1000});
    auto sink = std::make_unique<MemorySink>();
    Pipeline pipeline(std::move(source), std::move(assigner), std::move(sink));
    auto t0 = Clock::now();
    auto stats = pipeline.Run();
    auto t1 = Clock::now();
    return Throughput(stats.records_processed,
                      std::chrono::duration<double>(t1 - t0).count());
}

// One PartitionedPipeline run at N workers over the scaling workload (no ckpt).
double RunPartitionedOnce(uint32_t n) {
    auto source = std::make_unique<DeterministicGenerator>(ScalingGenConfig());
    auto sink = std::make_unique<MemorySink>();
    PartitionedPipelineConfig pc{};
    pc.num_workers = n;
    PartitionedPipeline pipeline(std::move(source), TumblingFactory(),
                                 std::move(sink), pc);
    auto t0 = Clock::now();
    auto stats = pipeline.Run();
    auto t1 = Clock::now();
    return Throughput(stats.records_processed,
                      std::chrono::duration<double>(t1 - t0).count());
}

}  // namespace

// Part A.1 — throughput vs N, with the single-threaded engine as reference.
static void BenchmarkScalingCurve() {
    constexpr int kReps = 7;
    std::cout << "=== Partitioned Scaling Curve ===\n";
    std::cout << "  Workload: 1M records, 1000 keys, tumbling 1s, MemorySink, no checkpointing\n";
    std::cout << "  Hardware: 4-vCPU shared Intel Xeon 6975P-C. "
              << kReps << " reps; median (min-max) M rec/s\n";
    std::cout << "  (Router thread + N workers + merge: N=4 and N=8 oversubscribe 4 vCPUs.)\n\n";

    {
        std::vector<double> v;
        for (int i = 0; i < kReps; ++i) v.push_back(RunSingleThreadedOnce());
        Agg a = Aggregate(v);
        std::printf("  single-threaded Pipeline (reference) : %6.2f M rec/s  (%.2f - %.2f)\n",
                    a.median, a.lo, a.hi);
    }
    for (uint32_t n : {1u, 2u, 4u, 8u}) {
        std::vector<double> v;
        for (int i = 0; i < kReps; ++i) v.push_back(RunPartitionedOnce(n));
        Agg a = Aggregate(v);
        std::printf("  PartitionedPipeline  N=%u             : %6.2f M rec/s  (%.2f - %.2f)\n",
                    n, a.median, a.lo, a.hi);
    }
    std::cout << "\n";
}

// Part A.2 — checkpoint overhead (throughput ON vs OFF) + restore time.
static void BenchmarkCheckpointOverhead() {
    constexpr int kReps = 5;
    constexpr uint32_t kN = 4;
    constexpr uint64_t kCkptInterval = 100'000;

    std::cout << "=== Partitioned Checkpoint Overhead (N=" << kN << ") ===\n";
    std::cout << "  Workload: 1M records, 1000 keys, tumbling 1s; checkpoint interval "
              << kCkptInterval << " records\n";
    std::cout << "  Hardware: 4-vCPU shared Intel Xeon 6975P-C. "
              << kReps << " reps; median (min-max) M rec/s\n\n";

    // OFF: reuse the no-checkpoint partitioned run at N.
    std::vector<double> off;
    for (int i = 0; i < kReps; ++i) off.push_back(RunPartitionedOnce(kN));
    Agg aoff = Aggregate(off);

    // ON: checkpoint into a fresh tmp dir each rep.
    std::vector<double> on;
    uint64_t ckpt_files = 0;
    for (int i = 0; i < kReps; ++i) {
        char tmpl[] = "/tmp/stormglass_p4_ckpt_XXXXXX";
        char* d = ::mkdtemp(tmpl);
        if (!d) continue;
        std::string dir = d;
        GeneratorConfig g = ScalingGenConfig();
        g.checkpoint_interval = kCkptInterval;
        auto source = std::make_unique<DeterministicGenerator>(g);
        auto sink = std::make_unique<MemorySink>();
        PartitionedPipelineConfig pc{};
        pc.num_workers = kN;
        pc.checkpoint_dir = dir;
        PartitionedPipeline pipeline(std::move(source), TumblingFactory(),
                                     std::move(sink), pc);
        auto t0 = Clock::now();
        auto stats = pipeline.Run();
        auto t1 = Clock::now();
        on.push_back(Throughput(stats.records_processed,
                                std::chrono::duration<double>(t1 - t0).count()));
        ckpt_files = stats.checkpoints_written;  // per-partition files, summed
        std::filesystem::remove_all(dir);
    }
    Agg aon = Aggregate(on);

    std::printf("  checkpointing OFF : %6.2f M rec/s  (%.2f - %.2f)\n",
                aoff.median, aoff.lo, aoff.hi);
    std::printf("  checkpointing ON  : %6.2f M rec/s  (%.2f - %.2f)\n",
                aon.median, aon.lo, aon.hi);
    double delta_pct = (aoff.median - aon.median) / aoff.median * 100.0;
    std::printf("  overhead          : %+.1f%% throughput vs OFF; "
                "%llu per-partition checkpoint files written across %u partitions "
                "(= %llu global barriers x %u)\n",
                -delta_pct,
                static_cast<unsigned long long>(ckpt_files), kN,
                static_cast<unsigned long long>(ckpt_files / kN), kN);
    std::cout << "\n";

    // Part A.2 (cont.) — restore time. Populate a checkpoint dir with one run,
    // then time a fresh pipeline's restore step (load only, not the drain).
    std::cout << "=== Partitioned Restore Time (N=" << kN << ") ===\n";
    char tmpl[] = "/tmp/stormglass_p4_restore_XXXXXX";
    char* d = ::mkdtemp(tmpl);
    if (!d) {
        std::cout << "  (skipped: mkdtemp failed)\n\n";
        return;
    }
    std::string dir = d;
    {
        GeneratorConfig g = ScalingGenConfig();
        g.checkpoint_interval = kCkptInterval;
        auto source = std::make_unique<DeterministicGenerator>(g);
        auto sink = std::make_unique<MemorySink>();
        PartitionedPipelineConfig pc{};
        pc.num_workers = kN;
        pc.checkpoint_dir = dir;
        PartitionedPipeline pipeline(std::move(source), TumblingFactory(),
                                     std::move(sink), pc);
        pipeline.Run();
    }

    // Restore reps: generator with NO barriers (checkpoint_interval=0) so the
    // timed reps write no new checkpoints; restore still loads the highest
    // complete checkpoint already on disk. stats.restore_state_micros isolates
    // the snapshot load from the generator's O(offset) Seek replay.
    std::vector<double> state_us, seek_us;
    uint64_t restored_from = 0;
    for (int i = 0; i < kReps; ++i) {
        auto source = std::make_unique<DeterministicGenerator>(ScalingGenConfig());
        auto sink = std::make_unique<MemorySink>();
        PartitionedPipelineConfig pc{};
        pc.num_workers = kN;
        pc.checkpoint_dir = dir;
        PartitionedPipeline pipeline(std::move(source), TumblingFactory(),
                                     std::move(sink), pc);
        auto stats = pipeline.Run();
        state_us.push_back(static_cast<double>(stats.restore_state_micros));
        seek_us.push_back(static_cast<double>(stats.restore_seek_micros));
        restored_from = stats.records_replayed;
    }
    std::filesystem::remove_all(dir);
    Agg astate = Aggregate(state_us);
    Agg aseek = Aggregate(seek_us);
    std::printf("  restored from highest COMPLETE global checkpoint @ offset %llu\n",
                static_cast<unsigned long long>(restored_from));
    std::printf("  snapshot load (scan + LoadOffset x %u + Restore) : %8.0f us  (%.0f - %.0f)\n",
                kN, astate.median, astate.lo, astate.hi);
    std::printf("  source Seek (in-memory generator, O(offset) replay): %8.0f us  (%.0f - %.0f)\n",
                aseek.median, aseek.lo, aseek.hi);
    std::printf("  [%d reps; a replayable-log source seeks in ~O(1) — the load line is the "
                "portable restore cost]\n", kReps);
    std::cout << "\n";
}

int main() {
    std::cout << "=== Pipeline Throughput ===\n";
    BenchmarkPipeline(100'000);
    BenchmarkPipeline(500'000);
    BenchmarkPipeline(1'000'000);
    std::cout << "\n";

    BenchmarkCheckpoint();
    BenchmarkScalingCurve();
    BenchmarkCheckpointOverhead();
    return 0;
}

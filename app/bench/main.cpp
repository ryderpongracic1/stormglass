#include "aggregate/sum.h"
#include "aggregate/simd_sum.h"
#include "aggregate/simd_minmax.h"
#include "aggregate/simd_detect.h"
#include "engine/pipeline.h"
#include "source/generator.h"
#include "sink/memory_sink.h"
#include "window/tumbling.h"
#include "window/state.h"
#include "checkpoint/writer.h"
#include "stream/record.h"

#include <chrono>
#include <cstdint>
#include <iostream>
#include <random>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <vector>

using namespace stormglass;
using Clock = std::chrono::high_resolution_clock;

// Generate a vector of random int64 values
static std::vector<int64_t> GenerateData(size_t count, uint64_t seed = 42) {
    std::mt19937_64 rng(seed);
    std::uniform_int_distribution<int64_t> dist(-1'000'000, 1'000'000);
    std::vector<int64_t> data(count);
    for (auto& v : data) v = dist(rng);
    return data;
}

// Measure wall-clock time of a function, return nanoseconds
template <typename Fn>
static double MeasureNs(Fn&& fn, int iterations) {
    auto start = Clock::now();
    for (int i = 0; i < iterations; ++i) {
        fn();
    }
    auto end = Clock::now();
    return std::chrono::duration<double, std::nano>(end - start).count() /
           static_cast<double>(iterations);
}

static void BenchmarkKernels() {
    constexpr size_t kElements = 1'000'000;
    constexpr int kIterations = 100;

    auto data = GenerateData(kElements);
    std::span<const int64_t> span(data);

    std::cout << "=== Kernel Microbenchmark (" << kElements / 1'000'000
              << "M elements, " << kIterations << " iterations) ===\n";

    // CPU features
    std::cout << "  CPU: SSE4.2=" << (HasSSE42() ? "yes" : "no")
              << " AVX2=" << (HasAVX2() ? "yes" : "no") << "\n";

    // Scalar sum
    double scalar_ns = MeasureNs([&]() {
        SumInt64Kernel kernel;
        kernel.AddBatch(span);
    }, kIterations);
    double scalar_per_elem = scalar_ns / static_cast<double>(kElements);

    // SIMD sum
    double simd_ns = MeasureNs([&]() {
        SimdSumInt64Kernel kernel;
        kernel.AddBatch(span);
    }, kIterations);
    double simd_per_elem = simd_ns / static_cast<double>(kElements);

    double speedup = scalar_per_elem / simd_per_elem;

    // Min kernel
    double min_ns = MeasureNs([&]() {
        SimdMinInt64Kernel kernel;
        kernel.AddBatch(span);
    }, kIterations);
    double min_per_elem = min_ns / static_cast<double>(kElements);

    // Max kernel
    double max_ns = MeasureNs([&]() {
        SimdMaxInt64Kernel kernel;
        kernel.AddBatch(span);
    }, kIterations);
    double max_per_elem = max_ns / static_cast<double>(kElements);

    std::cout << "  Scalar sum:   " << scalar_per_elem << " ns/elem\n";
    std::cout << "  SIMD sum:     " << simd_per_elem << " ns/elem  ("
              << speedup << "x speedup)\n";
    std::cout << "  Min kernel:   " << min_per_elem << " ns/elem\n";
    std::cout << "  Max kernel:   " << max_per_elem << " ns/elem\n";
    std::cout << "\n";
    std::cout << "  Note: SIMD sum uses explicit intrinsics (AVX2/SSE4.2).\n";
    std::cout << "  Min/Max use tight scalar loops that auto-vectorize under -O3.\n";
    std::cout << "  The pipeline currently uses scalar per-pane accumulation;\n";
    std::cout << "  the SIMD kernels will be exploited by a future vectorized\n";
    std::cout << "  window operator that batches records by key first.\n\n";
}

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

    // Clean up
    std::string cmd = "rm -rf " + dir;
    [[maybe_unused]] int rc = std::system(cmd.c_str());
    std::cout << "\n";
}

int main() {
    BenchmarkKernels();

    std::cout << "=== Pipeline Throughput ===\n";
    BenchmarkPipeline(100'000);
    BenchmarkPipeline(500'000);
    BenchmarkPipeline(1'000'000);
    std::cout << "\n";

    BenchmarkCheckpoint();

    return 0;
}

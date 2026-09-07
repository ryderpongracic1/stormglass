# Benchmarking stormglass on macOS

Use this runbook to produce benchmark numbers that are attributable to a specific machine, revision, build, and workload. Run it on an otherwise idle Mac while connected to AC power. Disable Low Power Mode and close CPU-heavy applications.

## Record the environment

```sh
git rev-parse HEAD
sw_vers
uname -m
sysctl -n machdep.cpu.brand_string
sysctl -n hw.logicalcpu hw.physicalcpu hw.memsize
xcrun clang++ --version
cmake --version
```

Apple Silicon may return an empty CPU brand string. In that case, record the Mac model from **System Settings → General → About**. Do not reuse the hardware description printed by the benchmark executable; it is historical text rather than runtime detection.

## Build and validate

Use a clean Release build. `-O3 -march=native` is applied to the benchmark executables by CMake.

```sh
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DSTORMGLASS_BENCH=ON
cmake --build build-release --parallel "$(sysctl -n hw.logicalcpu)"
ctest --test-dir build-release --output-on-failure
```

Run the correctness suite before timing. Do not benchmark sanitizer builds.

## Run the benchmarks

Warm the binaries once, then retain three complete runs. The executables already perform their documented inner repetitions and report medians where applicable.

```sh
build-release/app/stormglass_bench > bench-warmup.txt
build-release/app/stormglass_scanbench > scan-warmup.txt

for run in 1 2 3; do
  build-release/app/stormglass_bench > "bench-${run}.txt"
  build-release/app/stormglass_scanbench > "scan-${run}.txt"
done
```

Report the complete workload description with every number: record count, key count, window type and size, source batch size, watermark interval, worker count, sink, checkpoint setting, statistic, repetition count, observed range, architecture, and commit SHA. `PartitionedPipeline N=1` uses a router thread plus a worker thread and must not be labeled single-threaded. The scan benchmark isolates pane-index behavior and must not be presented as full pipeline throughput.

For more repeatable results, reboot, wait for indexing and background updates to settle, keep the machine thermally stable, and run each configuration in an interleaved order. macOS does not provide a portable supported equivalent to Linux CPU affinity, so document that limitation rather than implying pinned cores.

## Correctness and crash evidence

```sh
build-release/app/stormglass_oracle \
  --cross-n --seeds 100 --records 10000 \
  --disorder-profile heavy-tailed --lateness-ms 2000

build-release/app/stormglass_oracle \
  --cross-n --seeds 100 --records 10000 --window-type sliding \
  --disorder-profile heavy-tailed --lateness-ms 2000

build-release/app/stormglass_nemesis --real-kill --seeds 7 --verbose
build-release/app/stormglass_nemesis \
  --real-kill --real-phase mid-checkpoint --seeds 7 --verbose
build-release/app/stormglass_nemesis --partitioned --seeds 7 --verbose
build-release/app/stormglass_nemesis --alignment-kill --seeds 5 --verbose
```

The crash harness verifies the union of separate pre-crash and post-restore output files. Report replay duplicates as well as missing results. These runs establish the harness's at-least-once recovery property; they do not establish a transactional exactly-once sink.

## Comparing with Apache Flink

A useful Flink comparison requires a purpose-built benchmark with matched semantics. Do not compare `stormglass_bench` directly with a published Flink throughput number.

Match at least these variables:

- identical pre-generated records, keys, values, event timestamps, and arrival order;
- the same tumbling or sliding window definition and allowed-lateness policy;
- equivalent watermark cadence and idle-source behavior;
- the same parallelism and CPU allocation;
- equivalent sink behavior, preferably an in-memory black-hole sink for throughput;
- checkpointing either disabled in both systems or enabled with the same interval and comparable durable storage;
- the same warmup, measured duration, repetitions, and reporting statistic.

Flink runs on a JVM, so include JVM version, heap size, garbage collector, TaskManager slots, operator chaining, object-reuse setting, and whether serialization or networking is bypassed. A single-process native microbenchmark and a full Flink runtime answer different questions. Present the comparison as architectural overhead under a controlled workload, not as a general claim that one engine is faster.

The matched comparator is available in [`bench/flink`](../bench/flink/README.md).
It pins Flink 2.3.0 and Java 17, drives both engines from the same binary fixture,
requires equal output digests before their throughput numbers are compared, and
stores its raw results separately from ordinary C++ builds. Published results
and their limitations are summarized in [benchmarks.md](benchmarks.md).

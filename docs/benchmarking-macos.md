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

Apple Silicon may return an empty CPU brand string. In that case, record the Mac model from **System Settings → General → About**. The benchmark executable labels results as coming from the current host; hardware metadata must accompany retained logs.

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
requires actual/expected record counts and equal output digests before their throughput numbers are compared, and
stores its raw results separately from ordinary C++ builds. Published results
and their limitations are summarized in [benchmarks.md](benchmarks.md).

## Comparison timing and evidence gate

The comparison is restricted to **zero allowed lateness**. Stormglass coalesces
pending late re-fires until a watermark; Flink can emit each late update. A
nonzero-lateness result stream would therefore not be the same workload.

The `timing=execute` interval includes native fixture loading, pipeline setup,
execution, and digest reduction. Flink uses wall time around `env.execute()`
through result retrieval, including local job startup, fixture mapping and
execution. Command-line parsing and process launch are excluded; Flink graph
construction is excluded. These are explicit local execution boundaries, not
identical instruction paths or steady-state operator-only timings.

Each Flink trial starts a fresh JVM with `-Xms1g -Xmx4g -XX:+UseG1GC`. The
separate two-cycle warm-up process warms filesystem/OS caches but **does not
warm the measured JVM's JIT**. Compilation and GC during execution remain in
the measurements. Flink retains its default local runtime configuration, key
partitioning, chaining and transport, with object reuse requested; no custom
Flink tuning search was performed. Native uses FNV-1a partitioning and batched
in-process queues. Equal results do not imply equal serialization or scheduling
work, equal CPU consumption, or a universally faster engine.

The runner rebuilds both implementations, records binary hashes, tracked-diff
hash, build configuration, fixture SHA-256, heap/GC settings, and the expected
run matrix. It alternates engine order between repetitions and stops on failed
commands even when output is piped through `tee`. A private fixture per run
prevents concurrent invocations from overwriting one another's input. Hardware
is not exclusively reserved and macOS core affinity is not controlled.

The summarizer rejects missing/duplicate fields, missing jobs, mismatched
workloads, incorrect actual/expected counts, invalid timings, and any output
count/drop/digest disagreement across repetitions or worker counts. Two
64-bit digests are a probabilistic multiset check, not a proof of output
identity. This comparison does not validate crash recovery.

Run the benchmark regression checks separately from timed measurements:

```sh
python3 -m unittest discover -s bench/flink -p 'test_summarize_results.py'
python3 bench/flink/test_comparison_inputs.py \
  --stormglass build-release/app/stormglass_compare \
  --java-classpath "bench/flink/target/classes:$(cat bench/flink/target/classpath.txt)"
```

# Development

## Requirements

- CMake 3.22 or newer
- a C++20 compiler
- Git for CMake's GoogleTest fetch
- POSIX threads and filesystem primitives

Java 17 and Maven are needed only for the Apache Flink comparator.

## Build modes

Release build:

```bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel
ctest --test-dir build-release --output-on-failure
```

ASan/UBSan build:

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTORMGLASS_SANITIZERS=ON
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure --timeout 120
```

ThreadSanitizer must use a separate build:

```bash
cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTORMGLASS_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure --timeout 120
```

CMake fails configuration if sanitizers were requested but their runtime probe
does not work. This prevents a validation build from silently running without
the requested instrumentation.

## Executables

| Executable | Purpose |
|---|---|
| `stormglass_demo` | Demonstrates tumbling, sliding, lateness, and partitioned modes |
| `stormglass_bench` | Native pipeline, checkpoint, fan-in, and scaling benchmarks |
| `stormglass_scanbench` | Isolates watermark cost as live-pane count and cadence change |
| `stormglass_oracle` | Runs single-source, cross-N, and multi-source differential checks |
| `stormglass_nemesis` | Runs checkpoint and process-kill recovery scenarios |
| `stormglass_compare` | Consumes the binary fixture used by the Flink comparison |

Benchmark targets require `-DSTORMGLASS_BENCH=ON`.

## Repository layout

```text
app/                 executable entry points
bench/fixture/       deterministic binary fixture generator
bench/flink/         Java DataStream comparator and macOS scripts
cmake/               dependency setup
src/aggregate/       sum/count kernel
src/checkpoint/      snapshot format, CRC, common-cut selection
src/engine/          pipelines, processors, partitioning, queues
src/nemesis/         process-kill and torn-checkpoint harnesses
src/oracle/          reference model and differential runners
src/sink/            output interfaces and implementations
src/source/          generator, wrappers, multi-source fan-in
src/stream/          record, batch, and watermark types
src/window/          tumbling/sliding assignment and keyed state
test/                unit, differential, recovery, and hardening tests
```

## Useful focused commands

```bash
# Multi-source and alignment tests
ctest --test-dir build-release -R 'Alignment|SourceMerge' --output-on-failure

# Late-data and snapshot tests
ctest --test-dir build-release -R 'Late|Checkpoint' --output-on-failure

# Cross-N differential proof
./build-release/app/stormglass_oracle --cross-n --seeds 100 --records 10000 \
  --disorder-profile heavy-tailed --lateness-ms 2000

# Crash scenarios
./build-release/app/stormglass_nemesis --partitioned --seeds 7 --verbose
./build-release/app/stormglass_nemesis --alignment-kill --seeds 5 --verbose
```

## Error-handling expectations

Sources, sinks, checkpoint I/O, and worker processing report failure through
exceptions. `PartitionedPipeline` captures the first cross-thread exception,
closes every queue, joins every thread, and rethrows on the caller. New
concurrent code should preserve that cancellation path and include a test that
exercises backpressure while failure occurs.

Serialized checkpoint data is untrusted input even after CRC validation. New
fields require bounds checks before allocation and a compatible format-version
decision.

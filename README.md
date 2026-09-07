# stormglass

[![CI](https://github.com/ryderpongracic1/stormglass/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/stormglass/actions/workflows/ci.yml)

Built a C++20 event-time stream processor with keyed shared-nothing workers,
tumbling and sliding windows, multi-source watermark coordination, aligned
checkpoint recovery, and deterministic fault testing.

On a matched 100-million-record local workload, stormglass reached **9.07M
records/s with one worker** and **30.02M records/s with four workers**—4.62×
and 4.55× Apache Flink 2.3.0—while both engines emitted the same 66,843,149
window results and identical dual checksums.

```text
 sources                         keyed execution

 ┌────────┐
 │ source0├──┐                 ┌──────────► worker 0 ──┐
 ├────────┤  │  ┌──────────┐  │                       │
 │ source1├──┼─►│  merge   ├─►│ FNV-1a   worker 1 ──┼─► sink
 ├────────┤  │  │ min(wm)  │  │ router                │
 │   ...  │  │  │ barriers │  │            ...       │
 ├────────┤  │  └──────────┘  └──────────► worker N ──┘
 │ sourceK├──┘
 └────────┘
```

**Stack:** C++20 · CMake · `std::thread` · bounded batched queues · CRC32C ·
GoogleTest · ASan/UBSan · ThreadSanitizer · Apache Flink 2.3 comparator

## Overview

stormglass models the parts of stream processing where correctness is easiest
to lose: event-time progress, late data, fan-in from sources moving at different
rates, consistent state cuts, and recovery after a process dies during a
checkpoint.

Records are hash-partitioned by key across workers. Each worker owns its keys,
window state, watermark tracker, and sink, so aggregation needs no shared
mutable state. The router transfers one batch per worker queue instead of
locking and notifying for every record. Watermarks and barriers travel in-band
with data and are broadcast to every partition in FIFO order.

`SourceMerge` combines independent input streams before the router. It emits the
minimum watermark across active inputs, excludes logically idle inputs, and
blocks an input that reaches a checkpoint barrier until the other active inputs
reach the same epoch. The implementation is deterministic and single-process,
which makes the complete event trajectory replayable against a differential
oracle.

## Semantics

- **Event time:** half-open tumbling and sliding windows with monotonic,
  bounded-out-of-order watermarks.
- **Late data:** records inside the allowed-lateness horizon update retained
  aggregates and re-fire; records beyond the horizon are counted and dropped.
- **Keyed parallelism:** `FNV-1a(key) % N` assigns each key to one worker.
  Parallel and single-worker runs produce the same result set.
- **Multi-source progress:** the global watermark is the running minimum across
  active sources. A resumed source can produce late records but cannot regress
  the emitted watermark.
- **Aligned checkpoints:** an epoch closes after every active source reaches its
  barrier. Each worker snapshots its partition at the resulting common cut.
- **Recovery:** restore selects the newest offset with a CRC-valid snapshot for
  every partition; partial and corrupt snapshot sets are rejected.

The exact event-time rules are documented in
[event-time-semantics.md](docs/event-time-semantics.md). The checkpoint format,
restore algorithm, and source/sink contract are in
[recovery.md](docs/recovery.md).

## Correctness evidence

The current suite contains **153 tests** and runs under ASan/UBSan plus
ThreadSanitizer on Linux and macOS in CI.

- A naive differential oracle checks tumbling and sliding aggregation under
  bounded and heavy-tailed disorder.
- The cross-N matrix runs 100 seeds × 10,000 records over N in `{1,2,4,8}` and
  requires every parallel result set and late-drop count to match the oracle.
- Direct fan-in tests cover minimum watermark combination, idle-source resume,
  exact aligned cuts, blocked early channels, and the K=1 reduction.
- Twenty-six confirmed fork-and-SIGKILL scenarios cover crashes between
  checkpoints, during writes, with torn partition sets, and during barrier
  alignment; the recovery harness observed zero missing results.
- The Flink comparator runs the same binary fixture through both engines and
  rejects differences in record count, output count, late drops, or either
  output digest.

The crash harness establishes tested **at-least-once recovery**. It observed
replay duplicates in scenarios where output preceded the recovered checkpoint;
stormglass does not claim an end-to-end exactly-once sink protocol. Full test
scope and evidence: [verification.md](docs/verification.md).

## Measured results

Matched local comparison on a 10-core Apple M1 Max with 32 GiB RAM, OpenJDK
17.0.16, and Flink 2.3.0. Each cell is the median of three measured
100-million-record jobs after warm-up. Checkpointing was disabled in both
engines.

| Parallelism | stormglass | Flink | stormglass / Flink |
|---:|---:|---:|---:|
| 1 | **9.071M rec/s** | 1.964M rec/s | **4.62×** |
| 2 | **14.029M rec/s** | 3.623M rec/s | **3.87×** |
| 4 | **30.023M rec/s** | 6.593M rec/s | **4.55×** |
| 8 | **28.227M rec/s** | 6.984M rec/s | **4.04×** |

Every measured job emitted 66,843,149 results with zero late drops and matching
checksums. This is a controlled single-host comparison of the keyed tumbling
window path; it is not a distributed networking, durable-sink, or checkpoint
comparison. Workloads, ranges, native microbenchmarks, and limitations are in
[benchmarks.md](docs/benchmarks.md).

## Quickstart

Requirements: CMake 3.22+, a C++20 compiler, and Git. GoogleTest is fetched by
CMake.

```bash
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel
ctest --test-dir build --output-on-failure

# Run the demonstration pipeline
./build/app/stormglass_demo
```

Build the benchmark and verification tools:

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DSTORMGLASS_BENCH=ON
cmake --build build-release --parallel

./build-release/app/stormglass_bench
./build-release/app/stormglass_scanbench
./build-release/app/stormglass_oracle --cross-n --seeds 100 --records 10000 \
  --disorder-profile heavy-tailed --lateness-ms 2000
./build-release/app/stormglass_nemesis --real-kill --seeds 7 --verbose
```

The matched Flink runbook is in [`bench/flink`](bench/flink/README.md).

## Documentation

| Document | Contents |
|---|---|
| [architecture.md](docs/architecture.md) | Source merge, routing, worker ownership, window state, and backpressure |
| [event-time-semantics.md](docs/event-time-semantics.md) | Watermarks, idleness, allowed lateness, re-firing, and window lifecycle |
| [recovery.md](docs/recovery.md) | Barrier alignment, checkpoint format, restore selection, and delivery contract |
| [verification.md](docs/verification.md) | Differential oracles, cross-N invariance, sanitizers, and crash nemeses |
| [benchmarks.md](docs/benchmarks.md) | Current results, methodology, Flink comparison, and interpretation |
| [benchmarking-macos.md](docs/benchmarking-macos.md) | Reproducible Apple Silicon commands and artifact capture |
| [development.md](docs/development.md) | Build modes, repository layout, tests, and command reference |
| [hardening-audit.md](docs/hardening-audit.md) | Defects found during the 2026-09-06 audit and their resolutions |

## Scope and limitations

stormglass is a portfolio and research engine hardened through deterministic
testing and measurement. Its current boundaries are explicit:

- Execution is single-process. `SourceMerge` models K inputs through
  deterministic round-robin pulls rather than concurrent broker or socket
  consumers.
- Included sources are deterministic generators and fixtures. There is no Kafka,
  CDC, or network transport integration.
- Checkpoints protect operator state. There is no transactional sink, input
  event-ID deduplication, or general end-to-end exactly-once guarantee.
- Checkpoint history for partitioned jobs is retained without coordinated
  pruning, so disk use and recovery scans grow over time.
- Restore requires stable worker count, window configuration, source ordering,
  lateness, and barrier cadence. State has no persisted configuration
  fingerprint or rescaling protocol.
- Checkpoint serialization pauses the operator; asynchronous snapshotting is a
  future optimization.

Those constraints keep the project honest without weakening what it does
demonstrate: careful event-time semantics, deterministic concurrent execution,
failure-aware state recovery, and measurement against a production stream
processor.

# stormglass

[![CI](https://github.com/ryderpongracic1/stormglass/actions/workflows/ci.yml/badge.svg)](https://github.com/ryderpongracic1/stormglass/actions/workflows/ci.yml)

Built a C++20 event-time stream processor with keyed shared-nothing workers,
tumbling and sliding windows, multi-source watermark coordination, aligned
checkpoint recovery, TCP Chandy–Lamport snapshots, and deterministic fault testing.

On a matched 100-million-record local workload, stormglass reached **27.50M
records/s with eight workers**, **3.68× Apache Flink 2.3.0**, using the documented
execution wall-time interval. All measured jobs agreed on 66,843,149 window
results and both output digests. This is a single-host, checkpoint-disabled
comparison with an in-memory digest sink; see [method and ranges](docs/benchmarks.md).

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

## TCP distributed snapshot mode

A separate fixed-topology TCP path implements **Chandy–Lamport snapshots**:
participants save local operator/source state, propagate FIFO markers, and
record in-flight messages while processing continues. A global manifest commits
only after all participant files and channel-sequence cuts validate. Recovery
restores local state and replays channel records before fresh input.

Tests use three separate TCP processes, capture nonempty in-flight state, and
recover after a consumer is killed while a marker is missing. The protocol also
passes an independent conservation check across 100 randomized FIFO schedules.
This mode uses the same keyed window core, but is separate from native
`SourceMerge` alignment and from the throughput benchmarks. See
[chandy-lamport.md](docs/chandy-lamport.md) for the protocol, runbook and limits.

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

The current CTest suite contains **177 tests** (175 GoogleTest cases and two
TCP process scenarios) and runs under ASan/UBSan plus
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
  rejects incomplete jobs, incorrect actual/expected record counts, differing workloads, output counts, late drops, or either
  output digest.

The crash harness establishes tested **at-least-once recovery**. It observed
replay duplicates in scenarios where output preceded the recovered checkpoint;
stormglass does not claim an end-to-end exactly-once sink protocol. Full test
scope and evidence: [verification.md](docs/verification.md).

## Measured results

Corrected-harness rerun on 2026-09-07: 10-core Apple M1 Max, 32 GiB RAM,
OpenJDK 17.0.16, Flink 2.3.0. Each cell is the median of three measured
100-million-record jobs. Checkpointing was disabled in both engines; Flink used
a fresh JVM per job, a 1–4 GiB heap, and G1 GC. The separate warm-up process does
not warm the measured JVM's JIT.

| Workers / keyed parallelism | stormglass M rec/s | Flink M rec/s | stormglass / Flink |
|---:|---:|---:|---:|
| 1 | **8.211** | 1.868 | **4.40×** |
| 2 | **9.364** | 3.637 | **2.57×** |
| 4 | **21.097** | 6.409 | **3.29×** |
| 8 | **27.499** | 7.474 | **3.68×** |

Every measured job consumed the expected 100M records and emitted 66,843,149
results with zero late drops and matching dual digests. Digest agreement is
strong probabilistic evidence, not a proof of identical output. Timing includes
native fixture load/setup and Flink local job startup; this is not a comparison
of distributed networking, durable sinks, checkpoints, or operator-only speed.

The earlier 30.023M rec/s / 4.55× Flink and 3.31× scaling figures describe the
previous harness and run. Use the [current approved claims](docs/approved-claims.md)
for resume text. Full ranges, timing changes, historical measurements and
limitations are in [benchmarks.md](docs/benchmarks.md).

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
| [chandy-lamport.md](docs/chandy-lamport.md) | TCP markers, in-flight state, global commit and process recovery |
| [recovery.md](docs/recovery.md) | Barrier alignment, checkpoint format, restore selection, and delivery contract |
| [verification.md](docs/verification.md) | Differential oracles, cross-N invariance, sanitizers, and crash nemeses |
| [benchmarks.md](docs/benchmarks.md) | Current results, methodology, Flink comparison, and interpretation |
| [benchmarking-macos.md](docs/benchmarking-macos.md) | Reproducible Apple Silicon commands and artifact capture |
| [development.md](docs/development.md) | Build modes, repository layout, tests, and command reference |
| [hardening-audit.md](docs/hardening-audit.md) | Defects found during the 2026-09-06 audit and their resolutions |

## Scope and limitations

stormglass is a portfolio and research engine hardened through deterministic
testing and measurement. Its current boundaries are explicit:

- The native router/worker path is single-process. `SourceMerge` models K inputs through
  deterministic round-robin pulls rather than concurrent broker or socket
  consumers.
- The TCP snapshot path supports fixed, configured peer connections and replayable
  application sources; its process tests use generated input. There is no Kafka,
  CDC, dynamic membership or authenticated network deployment integration.
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

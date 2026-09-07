# Benchmarks

Performance claims in stormglass are tied to a named workload, host, build, and
measurement interval. Release builds are used throughout.

## Matched Apache Flink comparison

Commit `6955b13` was measured on a 10-core Apple M1 Max with 32 GiB RAM,
macOS 26.6.2, OpenJDK 17.0.16, and Apache Flink 2.3.0.

Both engines consumed the same one-million-record binary fixture replayed across
100 disjoint event-time cycles. The workload used:

- 100,000,000 logical records and 1,000 keys;
- deterministic bounded out-of-order arrival with a 5-second bound;
- an explicit watermark every 500 records;
- keyed one-second tumbling sum/count windows;
- zero allowed lateness and checkpointing disabled;
- source parallelism one, keyed operator parallelism N;
- in-memory, order-independent XOR and wrapping-sum output digests;
- one warm-up followed by three measured jobs at each N.

The C++ fixture is loaded once and decoded from memory. Flink reads the same
format through a memory-mapped source. The Flink adapter converts stormglass's
exclusive watermark frontier to Flink's inclusive millisecond representation.

| Parallelism | stormglass M rec/s | Flink M rec/s | stormglass / Flink |
|---:|---:|---:|---:|
| 1 | **9.071** (9.025–9.252) | 1.964 (1.945–1.992) | **4.62×** |
| 2 | **14.029** (14.022–14.066) | 3.623 (3.503–3.783) | **3.87×** |
| 4 | **30.023** (29.948–30.286) | 6.593 (6.425–6.985) | **4.55×** |
| 8 | **28.227** (28.196–28.475) | 6.984 (6.789–7.182) | **4.04×** |

Every one of the 24 measured engine jobs consumed 100,000,000 records and
emitted 66,843,149 window results with zero late drops. Every output had:

```text
digest_xor = 320d80c21c0b88e6
digest_sum = 5f1326cbfb8a0660
```

The comparison demonstrates lower local execution overhead for this keyed
window workload. It does not compare distributed network shuffle, durable
sources or sinks, checkpointing, rescaling, or production operations. It is not
a general claim that stormglass outperforms Flink.

The runnable implementation and compact report are in
[`bench/flink`](../bench/flink/README.md).

## Native engine measurements

Additional arm64 measurements from the hardening run isolate different parts of
the engine. They are separate workloads and must not be combined into one
throughput claim.

| Measurement | Result | Scope |
|---|---:|---|
| Single-threaded `Pipeline` | 7.55M rec/s | One 1M-record run, 100 keys, 1s tumbling, memory sink |
| Partitioned N=1 | 9.28M rec/s median | 1M records, 1,000 keys, seven reps |
| Partitioned N=2 | **9.69M rec/s median** | Same workload, seven reps |
| Partitioned N=4 | 9.51M rec/s median | Same workload, seven reps |
| Partitioned N=8 | 9.18M rec/s median | Same workload, seven reps |
| 20K live-pane isolation | **9.44–9.55M rec/s** | Varying watermark cadence, three medians per setting |
| Checkpoint write, 1,000 panes | **0.383 ms mean** | 20 serialize + file-fsync + rename + directory-fsync writes |
| N=4 snapshot state load | **5.703 ms median** | Five directory-scan + load + restore runs |
| Generator seek after restore | 72.784 ms median | O(offset) deterministic replay to record 1M |

N=1 in `PartitionedPipeline` still overlaps source routing and worker processing
on separate threads. The pane benchmark holds windows open deliberately to
isolate whether watermark work scales with all live panes. Snapshot load
excludes source seek, and the generator's O(offset) seek is not representative
of a broker log offset.

## What the measurements show

The matched workload scales strongly through four workers: 30.023 / 9.071 =
3.31× over N=1. N=8 falls about 6% from N=4, so the defensible description is
"scales through four workers and plateaus by eight" rather than linear scaling.

The pane-index isolation remains near 9.5M records/s with 20,000 live panes,
showing that watermark advancement is proportional to windows reaching an end
boundary rather than all retained state.

The source/router/worker benchmark and the matched Flink comparison stress
substantially different output rates and source paths. Their absolute numbers
are intentionally reported separately.

## Reproduce

For the native suite:

```bash
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DSTORMGLASS_BENCH=ON
cmake --build build-release --parallel

./build-release/app/stormglass_bench
./build-release/app/stormglass_scanbench
```

For the Flink comparison on macOS:

```bash
bench/flink/setup_macos.sh
bench/flink/run_comparison.sh
```

The runner records the Git commit, OS, architecture, processor, CPU count,
memory, Java version, Maven version, individual job outputs, and summary under
an ignored timestamped directory in `bench/results/`.

See [benchmarking-macos.md](benchmarking-macos.md) for the full runbook.

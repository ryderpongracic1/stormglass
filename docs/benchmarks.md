# Benchmarks

Performance claims in stormglass are tied to a named workload, host, build, and
measurement interval. Release builds are used throughout.

## Matched Apache Flink comparison

The corrected harness was measured on 2026-09-07 at base commit `40ffef6`
with the benchmark fixes in this change, on a 10-core Apple M1 Max with 32 GiB
RAM, macOS 26.6.2, Apple Clang 21.0.0, OpenJDK 17.0.16, and Apache Flink 2.3.0.
Raw local evidence is retained in `bench/results/run-2KsfsmPC/`, including the
fixture hash, configuration, binary hashes, individual jobs, and summary.
Hardware was not exclusively reserved and no CPU affinity was set.

Both engines consumed the same one-million-record binary fixture replayed across
100 disjoint event-time cycles. The workload used:

- 100,000,000 logical records and 1,000 keys;
- deterministic bounded out-of-order arrival with a 5-second bound;
- an explicit watermark every 500 records;
- keyed one-second tumbling sum/count windows;
- zero allowed lateness and checkpointing disabled;
- source parallelism one, keyed operator parallelism N;
- in-memory, order-independent XOR and wrapping-sum output digests;
- three measured fresh-process jobs at each N, with a separate two-cycle
  cache warm-up (which does not warm the measured JVM's JIT);
- Flink heap 1–4 GiB and G1 GC; execution wall time as specified below.

The C++ fixture is loaded once and decoded from memory; fixture loading is timed. Flink reads the same
format through a memory-mapped source. The Flink adapter converts stormglass's
exclusive watermark frontier to Flink's inclusive millisecond representation.

| Workers / keyed parallelism | stormglass M rec/s | Flink M rec/s | stormglass / Flink |
|---:|---:|---:|---:|
| 1 | **8.211 (8.207–8.426)** | 1.868 (1.859–1.890) | **4.40×** |
| 2 | **9.364 (9.289–10.453)** | 3.637 (3.555–3.646) | **2.57×** |
| 4 | **21.097 (17.878–21.637)** | 6.409 (6.334–6.738) | **3.29×** |
| 8 | **27.499 (26.987–28.312)** | 7.474 (7.235–7.607) | **3.68×** |

Every one of the 24 measured engine jobs consumed 100,000,000 records and
emitted 66,843,149 window results with zero late drops. Counts and dual digests agree across all jobs; this is a probabilistic multiset check, not a proof of identical output. Every measured job reported:

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

### Timing correction and historical numbers

The historical `6955b13` run reported 9.071M / 30.023M rec/s at N=1 / N=4
and 4.62× / 4.55× Flink. It timed only native `PartitionedPipeline::Run()` versus Flink's
reported net job runtime. Those figures are superseded for current resume
wording; they must not be mixed with the new timer or described as a before/after
engine optimization. Engine processing code did not change in this correction.

The new native timer covers fixture loading, pipeline setup, execution and
digest reduction. The Flink timer covers `env.execute()` through result
retrieval, including local job startup and fixture mapping; graph construction
and process launch are outside it. These explicit application execution
intervals include different runtime work, not an operator-only or warm-JIT
comparison. Different adapter checks, JVM settings, scheduling and host load
also prevent attributing the numerical change to a single cause.

One initial N=1 pair was excluded because a short validation process may have
overlapped startup; its logs are retained in `excluded-startup-overlap/`. That
pair was rerun after the full matrix. No other samples were excluded.
The full matrix used the rebuilt Java class directory; the final runner now
executes the equivalent hashed JAR and refreshes dependency classpaths. The
runner's separate smoke test checks that path.

The report now rejects missing fields, incomplete runs, unequal actual/expected
counts, inconsistent workloads and nonfinite or inconsistent rates. Nonzero
allowed lateness is rejected because re-fire granularity differs between the
engines. The complete method is in [benchmarking-macos.md](benchmarking-macos.md).

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
| 20K live-pane isolation | **9.44–9.55M rec/s** | Varying watermark cadence, three repetitions per setting |
| Checkpoint write, 1,000 panes | **0.383 ms mean** | 20 serialize + file-fsync + rename + directory-fsync writes |
| N=4 snapshot state load | **5.703 ms median** | Five directory-scan + load + restore runs |
| Generator seek after restore | 72.784 ms median | O(offset) deterministic replay to record 1M |

N=1 in `PartitionedPipeline` still overlaps source routing and worker processing
on separate threads. The pane benchmark holds windows open deliberately to
isolate whether watermark work scales with all live panes. Snapshot load
excludes source seek, and the generator's O(offset) seek is not representative
of a broker log offset.

## What the measurements show

In this rerun, four workers reached 2.57× the one-worker rate,
and eight reached 3.35×. The N=4 range is wide (17.878–21.637M rec/s),
so these observations are not a fixed scaling guarantee. The historical
"3.31× through four workers, plateau at eight" describes the earlier run,
not this one.

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

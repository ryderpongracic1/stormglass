# Matched Apache Flink comparison

This module compares stormglass with Apache Flink 2.3.0 using the same finite,
language-neutral binary input. The source emits the fixture's event timestamps
and explicit watermarks; both engines run keyed 1-second tumbling sum/count
windows and consume results in an in-memory digest sink.

The digest is an order-independent pair (XOR and wrapping sum) over every
emitted `(key, window-start, window-end, sum, count)` tuple. A valid comparison
requires equal record counts, output counts, and both digest fields.

The fixture stores stormglass's exclusive event-time frontier. The Flink source
subtracts one millisecond when emitting it because Flink watermarks are
inclusive timestamps and its tumbling-window trigger is the window's `end - 1`.
This maps both APIs to the same logical frontier at millisecond resolution.

## macOS setup

The scripts require Homebrew OpenJDK 17 and Maven. Maven resolves the official
Flink artifacts pinned to 2.3.0 and runs its local execution environment. This
avoids requiring a separately managed Flink cluster for the single-host test.

```sh
brew install openjdk@17 maven
bench/flink/setup_macos.sh
```

Build the matching C++ executable:

```sh
cmake -S . -B build-release \
  -DCMAKE_BUILD_TYPE=Release \
  -DSTORMGLASS_BENCH=ON
cmake --build build-release --parallel "$(sysctl -n hw.logicalcpu)"
```

Then run three measured 100-million-record trials at each parallelism after a
warm-up job:

```sh
bench/flink/run_comparison.sh
```

Override the defaults through environment variables:

```sh
CYCLES=20 REPS=1 PARALLELISMS="1 2" bench/flink/run_comparison.sh
```

The heavy-disorder, zero-allowed-lateness variant additionally verifies that
both engines drop the same records:

```sh
PROFILE=heavy LATENESS_MS=0 CYCLES=2 REPS=1 PARALLELISMS="1 2" \
  bench/flink/run_comparison.sh
```

The runner stores raw results and environment metadata in an ignored,
timestamped directory under `bench/results/`. The physical fixture is one
million records and is replayed with a disjoint event-time offset per cycle, so
event time and watermarks remain monotonic while the measured job is long
enough to amortize JVM and job startup.

After the runs, `summarize_results.py` rejects any mismatch in records, emitted
windows, late drops, or either digest. It then prints median throughput, the
observed range, and the stormglass/Flink ratio for each parallelism.

## Reference result: Apple M1 Max

Commit `6955b13` was measured on a 10-core Apple M1 Max with 32 GiB RAM,
macOS 26.6.2, OpenJDK 17.0.16, and Flink 2.3.0. Each cell is the median of three
measured 100-million-record jobs after a warm-up. The workload used 1,000 keys,
bounded 5-second out-of-order arrival, a watermark every 500 records, 1-second
tumbling sum/count windows, zero allowed lateness, and checkpointing disabled.

| Parallelism | stormglass M rec/s | Flink M rec/s | stormglass / Flink |
|---:|---:|---:|---:|
| 1 | 9.071 (9.025–9.252) | 1.964 (1.945–1.992) | 4.62x |
| 2 | 14.029 (14.022–14.066) | 3.623 (3.503–3.783) | 3.87x |
| 4 | 30.023 (29.948–30.286) | 6.593 (6.425–6.985) | 4.55x |
| 8 | 28.227 (28.196–28.475) | 6.984 (6.789–7.182) | 4.04x |

Every measured job consumed 100,000,000 records and emitted 66,843,149 window
results with zero late drops, digest XOR `320d80c21c0b88e6`, and digest sum
`5f1326cbfb8a0660`. The comparison measures single-host execution with an
in-memory digest sink. It does not measure distributed network shuffle,
durable sinks, or checkpoint overhead.

This workload has checkpointing disabled. Compare checkpoint cost separately:
Flink checkpoints are scheduled by elapsed time, while the current stormglass
benchmark injects barriers by record count. The two should be normalized by
completed checkpoint count and state size before reporting a ratio.

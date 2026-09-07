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

This workload has checkpointing disabled. Compare checkpoint cost separately:
Flink checkpoints are scheduled by elapsed time, while the current stormglass
benchmark injects barriers by record count. The two should be normalized by
completed checkpoint count and state size before reporting a ratio.

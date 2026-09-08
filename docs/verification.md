# Verification

stormglass uses deterministic generation, an independent aggregation oracle,
direct protocol tests, fault injection, and sanitizers. The CTest suite contains 177 tests: 175
GoogleTest cases and two separate-process TCP scenarios. Benchmark tooling has separate Python and executable regression checks; these are not included in that count.

## Differential oracle

The oracle groups records by key and window and applies the documented
watermark and allowed-lateness rules without using `KeyedWindowState`. The
engine and oracle consume the same generated record/watermark trajectory and
must agree on:

- every emitted `(key, window, sum, count)` result;
- late records accepted and dropped;
- re-fired windows;
- final output watermark.

The primary cross-N run covers 100 seeds × 10,000 records for N in `{1,2,4,8}`.
Both tumbling and sliding windows run under heavy-tailed disorder with two
seconds of allowed lateness. Every N must match both the oracle and the
single-worker execution.

Representative completed matrices:

| Workload | Seeds and size | Result |
|---|---:|---|
| Tumbling, heavy-tailed, L=2s | 100 × 10,000 | zero mismatches; engine/oracle drops 36,429 |
| Sliding, heavy-tailed, L=2s | 100 × 10,000 | zero mismatches; engine/oracle drops 38,925 |
| K=3 sources, N=4, tumbling, heavy-tailed, L=2s | 100 × 10,000/source | zero mismatches; engine/oracle drops 36,316 |

Absolute pseudo-random drop counts can differ between libc++ and libstdc++
because `std::uniform_int_distribution` is implementation-defined. Equality
between the engine and oracle on the same host is the invariant.

For multi-source differential tests, the oracle consumes the trajectory emitted
by another deterministic `SourceMerge`. This checks downstream aggregation and
lateness against that trajectory; direct tests independently cover merge
scheduling and alignment.

## Direct multi-source tests

The suite checks protocol properties that result-set comparison alone cannot
prove:

- minimum watermark advancement and lagging-source pinning;
- logical idle exclusion, all-idle behavior, and monotonic resume;
- resumed records entering the ordinary late-data path;
- early barrier channels remaining blocked until peers align;
- merged barriers appearing at the exact expected record cut;
- idle channels not deadlocking alignment;
- seek replaying the identical aligned sequence;
- K=1 reducing to the single-source record, watermark, and barrier stream.

## Crash injection

The nemesis forks a child, lands a real `SIGKILL`, restores from durable state,
and compares the union of pre-crash and post-restore output with the oracle.
Twenty-six confirmed scenarios cover:

| Failure point | Confirmed scenarios |
|---|---:|
| Between checkpoints | 7 |
| During checkpoint writes | 7 |
| Torn partition sets | 7 |
| Mid-barrier alignment | 5 |

All 26 produced zero missing results. The harness observed 771 replay duplicates
across the first three groups and zero in the five mid-alignment cases. Those
five killed the process before any durable output was visible, so they establish
recovery from an incomplete alignment rather than a general exactly-once
property.

## Snapshot corruption and hardening

Recovery tests include:

- CRC corruption and truncated files;
- fallback to an older complete checkpoint;
- partial global partition sets;
- restored pending late-window re-fires;
- valid-CRC payloads containing impossible collection counts;
- sink and synchronization failures;
- invalid queue/window configuration and checked aggregate overflow;
- worker failure while the router is blocked by queue backpressure.

The findings that motivated these tests are recorded in
[hardening-audit.md](hardening-audit.md).

## Sanitizers and CI

GitHub Actions runs:

- Debug tests under AddressSanitizer and UndefinedBehaviorSanitizer on Linux;
- the full suite under ThreadSanitizer on Linux x86-64 and macOS arm64;
- Release benchmarks plus oracle and nemesis smoke runs;
- the C++/Flink semantic comparator for bounded and heavy disorder.

At `40ffef6`, [CI run 34089133633](https://github.com/ryderpongracic1/stormglass/actions/runs/34089133633)
completed successfully, including both ThreadSanitizer jobs. This verifies the
existing engine suite on Linux x86-64 and macOS arm64; it is not a claim that
uncommitted benchmark tooling has already passed remote CI.

A sanitizer pass establishes that the exercised executions emitted no sanitizer report.
It does not prove freedom from every possible schedule, leak, or external I/O
failure.

## Reproduce

```bash
cmake -S . -B build-asan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTORMGLASS_SANITIZERS=ON
cmake --build build-asan --parallel
ctest --test-dir build-asan --output-on-failure --timeout 120

cmake -S . -B build-tsan \
  -DCMAKE_BUILD_TYPE=Debug \
  -DSTORMGLASS_TSAN=ON
cmake --build build-tsan --parallel
ctest --test-dir build-tsan --output-on-failure --timeout 120

cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel

./build-release/app/stormglass_oracle --cross-n --seeds 100 --records 10000 \
  --disorder-profile heavy-tailed --lateness-ms 2000
./build-release/app/stormglass_oracle --cross-n --seeds 100 --records 10000 \
  --window-type sliding --disorder-profile heavy-tailed --lateness-ms 2000
./build-release/app/stormglass_oracle --sources 3 --workers 4 \
  --seeds 100 --records 10000 --disorder-profile heavy-tailed \
  --lateness-ms 2000

./build-release/app/stormglass_nemesis --real-kill --seeds 7 --verbose
./build-release/app/stormglass_nemesis --real-kill \
  --real-phase mid-checkpoint --seeds 7 --verbose
./build-release/app/stormglass_nemesis --partitioned --seeds 7 --verbose
./build-release/app/stormglass_nemesis --alignment-kill --seeds 5 --verbose
```

## Benchmark validation

The comparator counts source records in both engines and checks them against
the fixture declaration. The report requires complete job pairs and verifies
actual/expected counts, workload settings, output counts, late drops and both
digests across repetitions and N. It rejects nonzero allowed lateness because
the two engines use different re-fire granularity. Digest agreement is strong
probabilistic evidence, not a mathematical proof of identical result streams.

The benchmark checks include 14 Python report-validator tests and nine
executable input checks (valid input, false fixture counts, nonzero lateness,
invalid parallelism, and native cycle overflow). They run in the Flink CI job.

## TCP Chandy–Lamport snapshots

The new protocol has 24 focused tests: 22 GoogleTest cases and two TCP process
scenarios. They cover marker rules, continued processing and in-flight capture,
an independent token-conservation invariant under 100 randomized FIFO schedules,
window/watermark restoration, global commit validation, corruption, sequence-aware
replay, and recovery after a confirmed mid-marker SIGKILL. See the
[protocol and exact test scope](chandy-lamport.md). These are separate from the
original 26 native crash-harness scenarios and do not establish an exactly-once sink.

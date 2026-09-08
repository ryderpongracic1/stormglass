# Stormglass hardening audit — 2026-09-06

> Historical audit from 2026-09-06. For the corrected comparison methodology, see [benchmarks](benchmarks.md). The findings below describe that earlier workload, not the latest benchmark.

This is the dated record of the hardening review. See the project
[README](../README.md) and the focused documents in this directory for the
current architecture, evidence, and benchmark summary.

Audited upstream `abf9a53a9a47c6f6e8471f95da9518da9de1f1d9`. Validation host: macOS 26.6.2 (25G83), arm64, Apple Clang 21.0.0, C++20, CMake Release for benchmarks and Debug for sanitizers. No CPU affinity or exclusive machine reservation was used. The benchmark's printed Xeon reference and 4-vCPU oversubscription sentence are historical labels, not detected hardware.

## Verdict

The implementation supports a strong portfolio description of keyed event-time processing, aligned checkpoint recovery, and tested batched concurrency. It does **not** support an unconditional production-ready or end-to-end exactly-once claim. Several real correctness gaps were repaired. The approved wording below describes tested capabilities and preserves the boundaries of the evidence.

## Findings and patches

| Finding | Consequence before patch | Resolution |
|---|---|---|
| Pending re-fire set omitted from checkpoint | A fired window accepts a late record, checkpoints before the next watermark, then crashes. Restore loads its updated sum but never emits that update if no further late record arrives. | Checkpoint binary format v3 persists pending re-fires; both pipeline implementations restore them. Regression checks the recovered sum **and** count after final flush. |
| Finished source remains in watermark minimum | A short source can pin progress after it has exhausted. | Exclude exhausted sources from the minimum and retry merge scheduling when exhaustion closes an alignment epoch. Direct tests check progress and complete record counts. |
| Exceptions escape router/worker entry points | Source, sink, allocation, or processor failures invoke `std::terminate`; simply throwing from a worker also leaves a blocked router without a shutdown protocol. | First-error capture, shared cancellation, close/wake all queues, join every thread, and rethrow to the caller. A full-queue worker-failure regression exercises cancellation under backpressure. |
| Independent partition retention | A fast worker can prune an offset still needed as the last common recovery cut. Deterministic replay may recover from scratch, but last-two local retention does not preserve a common snapshot under arbitrary worker skew. | Partition workers retain all checkpoint files. Single-pipeline retention stays at two. This trades bounded disk use and fast scans for preservation of recovery cuts; coordinated retention is still needed. |
| Silent sink/checkpoint errors | Failed writes or flushes can be reported as successful processing; directory-fsync failures were ignored. | Sink open/write/fsync and checkpoint failures propagate; flush the sink before snapshotting; check directory synchronization. Zero-byte writes no longer spin forever. |
| Untrusted serialized counts reserved before bounds checks | A CRC-valid malformed snapshot could cause an enormous allocation or exception during recovery. | Bound pane, fired-window and pending-window counts by available bytes before allocation. Test constructs a valid-CRC impossible count. |
| Invalid configuration/arithmetic | Zero-capacity queues can hang; zero window sizes/intervals divide by zero; integer aggregate overflow is undefined behavior. | Validate relevant inputs; throw on aggregate overflow without changing the pane; saturate lateness deadlines. Window assigners explicitly reject negative or unrepresentable timestamp ranges. |
| Sanitizer option could silently disable sanitizers | A nominal sanitizer build could provide no sanitizer coverage. | Requested ASan/UBSan now fails configuration if its runtime probe fails. Add a separate TSan option and Linux/macOS CI jobs. |
| Router copies records into batches | Extra ownership copies on the routing path. | Move consumed data records into worker batches; preserve control broadcasts and FIFO ordering. Existing batching remains mutex/condition-variable based. |

Tests were added in `test/hardening_test.cpp`; the hardening commit includes all implementation, test, CMake, CI and documentation changes.

## Semantics and concurrency audit

* `Fnv1a64(key) % N` assigns disjoint **key subsets**, not contiguous key ranges. Worker window state and sinks are private; per-worker queues synchronize router-to-worker ownership, and joining synchronizes result collection.
* Data, watermarks, barriers, and end-of-stream sentinels preserve FIFO order within each worker. One queue operation carries all messages routed to a worker from a source batch. The queue is bounded and blocking; it is not lock-free. Queue capacity counts batches, so the memory bound additionally depends on source batch size, key sizes, and retained output/state.
* Watermarks advance monotonically. `SourceMerge` min-combines active channels, excludes logical idle channels, and reactivates a resumed channel at its retained watermark without regressing the emitted watermark. Idleness is deterministic pull-count modeling, not production wall-clock source detection.
* Windows are half-open. A record is dropped for a window when watermark >= end + lateness. Fired windows retain aggregates during allowed lateness; accepted late updates emit full replacement aggregates on the next advancing watermark or final flush. Sliding records may be accepted for some windows and dropped for others. The patched timestamp domain is nonnegative and representable; negative timestamps are explicitly unsupported.
* `SourceMerge` blocks early-barrier channels and closes an epoch once active channels align. This is blocking barrier alignment inspired by distributed snapshot techniques, implemented in one process with deterministic generator fan-in. It is not a networked Chandy–Lamport implementation that snapshots arbitrary in-flight network messages.
* The differential oracle independently checks window aggregation/lateness against the supplied record/watermark trajectory. The multi-source oracle uses another instance of **the same SourceMerge**, so it does not independently prove the merge's scheduling, idleness, or barrier implementation. Direct merge and alignment tests cover those behaviors.
* TSan checks data races for exercised executions, not every possible schedule and not memory leaks. ASan/UBSan add memory-access and undefined-behavior checks; this macOS run does not establish comprehensive leak freedom.

## Recovery contract and remaining architecture limits

1. Restore chooses a common offset for which every requested partition has a CRC-valid file. This is a **recovery selection rule**, not a transactional global checkpoint commit manifest. Reload failure now aborts instead of silently restoring an empty partition.
2. Recovery assumes a single job owns the checkpoint directory and that source identity/order/configuration, worker count, windowing, lateness and barrier cadence remain stable. There is no persisted job configuration fingerprint, source offset vector for a real broker, rescaling protocol, or multi-process writer lock. CRC proves byte integrity, not that two jobs or configurations have compatible state.
3. Default MemorySink output is volatile and merges only after joining. DurableFileSink is a crash-test sink that truncates its path when constructed. The harness deliberately uses distinct pre-crash and post-restore files and preserves their union. Restarting against the same sink filename can erase earlier output. Checkpointing alone does not protect output already emitted to an arbitrary sink.
4. There is no input event ID or input deduplication: repeated delivery of the same Record is another contribution. Output replay duplicates are expected. End-to-end exactly-once requires replayable sources and a transactional or appropriately versioned idempotent sink coordinated with checkpoint progress. A key-only upsert is insufficient if an older replay can overwrite a newer late-window revision. See the [Flink fault-tolerance documentation](https://github.com/apache/flink/blob/master/docs/content/docs/learn-flink/fault_tolerance.md).
5. The tests inject process SIGKILL, not power loss. File and directory fsync improve checkpoint durability, but there is no end-to-end machine-crash experiment or transactional sink proof. Sink output has length framing, not CRC protection. New directory-entry durability and arbitrary storage faults remain outside the tested guarantee.
6. Readers accept checkpoint versions 1–3. Versions 1/2 cannot reconstruct pending re-fire information they never stored. A new v3 checkpoint is needed for the repaired guarantee; do not describe old late-data snapshots as retroactively repaired.
7. Partition checkpoint history now grows without automatic pruning, and recovery scans grow with it. Error cancellation cannot interrupt a custom Source::Next that blocks indefinitely; the Source API has no cancellation hook. Neither unbounded external blocking nor hostile sources are modeled by these tests.

These boundaries make a transactional, broker-integrated, end-to-end exactly-once claim impossible **within the current source/sink architecture**. The approved wording uses measured at-least-once recovery in the crash harness instead of pretending that a local checkpoint patch supplies those missing protocols.

## Verification

* **153/153 Release tests passed.** Ten new tests supplement the upstream 143.
* **ASan/UBSan, arm64:** full 151-test suite passed, then all ten hardening tests passed after two format tests were added. All 153 distinct tests are covered; unchanged tests were not unnecessarily rerun.
* **TSan:** the local arm64 audit run covered all 153 distinct tests without a race report. Subsequent CI runs execute the complete suite under TSan on Linux x86-64 and macOS arm64.
* **26 fresh confirmed SIGKILL scenarios passed:** 7 between checkpoints, 7 during checkpoint writes, 7 with torn partition sets, and 5 mid-alignment. Zero missing results across all scenarios. The harness observed **771 replay duplicates** (91 + 350 + 330 + 0). Retries used to land a particular failure point are recorded in the logs; 26 is the number of successful scenario runs, not a claim that exactly 26 child processes were launched.
* All five mid-alignment scenarios had zero duplicates and **zero pre-crash emits**. This supports recovery from partial alignment, not exactly-once behavior across an already-visible output boundary.
* Historical 44–56% hand-off overhead reduction appears in the upstream README and commit message; this audit did not reproduce that before/after experiment. It is not carried forward as a current result.

Benchmark and differential results follow after final validation. The accompanying evidence bundle contains exact command output.

## Final benchmark and differential results

Release build on the arm64/macOS host above. Times include only what each harness explicitly measures; the full final logs are included alongside the earlier run, so results are not selected from hidden repetitions.

| Measurement | Final result | Scope |
|---|---:|---|
| Single-threaded Pipeline | 7.55M records/s | One 1M-record run, 100 keys, 1s tumbling, MemorySink |
| Same-workload single-threaded scaling reference | 5.85M records/s median | 1M records, **1000** keys; 7 reps, 5.65–5.88M |
| Partitioned N=1 | 9.28M records/s median | Router + one worker; 7 reps, 9.04–9.36M |
| Partitioned N=2 | **9.69M records/s median** | Router + two workers; 7 reps, 9.46–9.74M |
| Partitioned N=4 | 9.51M records/s median | 7 reps, 9.26–9.54M |
| Partitioned N=8 | 9.18M records/s median | 7 reps, 9.06–9.27M |
| 20K live panes, varying watermark interval | **9.44–9.55M records/s** | Non-expiring-window isolation benchmark; median of 3 per setting |
| Checkpoint write, 1000 panes | **0.383ms mean** | 20 writes: serialize, file fsync, rename, directory fsync |
| N=4 snapshot state load | **5.703ms median** | 5 reps, 5.618–6.273ms; directory scans + load + Restore |
| Generator seek after restore | **72.784ms median** | 5 reps, 72.227–73.030ms, O(offset) replay to offset 1M |

N=1 in PartitionedPipeline is **not single-threaded**: source generation/routing and state processing overlap on separate threads. The single-threaded 100-key and 1000-key results are different workloads. The pane isolation benchmark is not the full source-to-sink throughput benchmark. The snapshot-load number excludes generator replay and grows with retained history. Do not combine it into a claim of 5.703ms total recovery. The earlier run measured 7.48M single-threaded/100 keys and 9.61M at N=2, consistent with the final run's approximate scale.

Final differential runs:

* Tumbling, heavy-tailed disorder, L=2s: **100 seeds × 10K records**, N={1,2,4,8}; zero result mismatches against the oracle and single-threaded engine; engine/oracle drop counts both 36,429.
* Sliding, heavy-tailed disorder, L=2s: **100 seeds × 10K records**, N={1,2,4,8}; zero mismatches; both drop counts 38,925.
* K=3 deterministic sources, N=4, tumbling/heavy-tailed/L=2s: **100 seeds × 10K records per source**, zero mismatches; both drop counts 36,316. This checks aggregation on the shared merge trajectory, as qualified above.

## Disposition of the supplied claims

| Original claim | Approved disposition |
|---|---|
| C++20, FNV-1a router, N private key partitions, in-band controls | Verified; use “disjoint key subsets,” not “key ranges.” |
| 8.5–9.0M single-threaded | Historical, not reproduced here; replace with measured 7.55M for that workload, or use the stronger correctly labeled 9.69M two-worker result. |
| ~10M across 20K panes | Supported approximately **in the isolation benchmark**; final 9.44–9.55M across watermark frequencies. |
| Tumbling/sliding, bounded disorder, allowed lateness | Verified within the documented domain; pending re-fire recovery repaired. |
| Chandy–Lamport K-way snapshots | Use “K-way aligned barrier checkpoints over deterministic multi-source fan-in.” No networked distributed snapshot protocol is implemented. |
| Commit only when all N files pass CRC | Use “restore selects a common CRC-valid offset across all partitions.” There is no global commit manifest. |
| 5.6ms pause, 1.73ms restore | Historical hardware/workload-specific figures; replace with current measured components or omit from resume. |
| 44–56% overhead reduction | Upstream reports it, but raw before/after evidence was not reproduced here. Omit from newly approved text. Batching itself is verified. |
| 143 tests | Now 153 passing tests. |
| 100 seeds × 10K, cross-N, zero mismatches | Reproduced for both tumbling and sliding with heavy-tailed disorder and L=2s. |
| 26 fork+SIGKILL runs, zero missing output | Reproduced as 26 successful scenario runs in the durable pre/post-output union harness; replay duplicates occurred. |
| TSan-clean x86-64 and arm64 | The audit directly verified arm64; subsequent Linux x86-64 and macOS arm64 TSan CI jobs pass the complete suite. |
| Correct through duplicate delivery / exactly-once | Not supported as a general guarantee; input duplicates are not deduplicated, output replays duplicate, and sinks do not transact with checkpoints. |

## Reproduction

```sh
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build --parallel 8
ctest --test-dir build --output-on-failure --timeout 60
build/app/stormglass_bench
build/app/stormglass_scanbench
build/app/stormglass_oracle --cross-n --seeds 100 --records 10000 --disorder-profile heavy-tailed --lateness-ms 2000
build/app/stormglass_oracle --cross-n --seeds 100 --records 10000 --window-type sliding --disorder-profile heavy-tailed --lateness-ms 2000
build/app/stormglass_oracle --sources 3 --workers 4 --seeds 100 --records 10000 --disorder-profile heavy-tailed --lateness-ms 2000
build/app/stormglass_nemesis --real-kill --seeds 7 --verbose
build/app/stormglass_nemesis --real-kill --real-phase mid-checkpoint --seeds 7 --verbose
build/app/stormglass_nemesis --partitioned --seeds 7 --verbose
build/app/stormglass_nemesis --alignment-kill --seeds 5 --verbose
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DSTORMGLASS_SANITIZERS=ON
cmake --build build-asan --parallel 8
ctest --test-dir build-asan --output-on-failure --timeout 60
cmake -S . -B build-tsan -DCMAKE_BUILD_TYPE=Debug -DSTORMGLASS_TSAN=ON
cmake --build build-tsan --parallel 8
ctest --test-dir build-tsan --output-on-failure --timeout 60
```

GoogleTest was fetched once and reused locally with `FETCHCONTENT_SOURCE_DIR_GOOGLETEST` during this audit. Run benchmarks without concurrent builds or sanitizer runs. The evidence bundle contains the original full-suite sanitizer logs and the later hardening-subset logs.

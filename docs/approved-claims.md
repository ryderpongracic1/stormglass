# Approved resume and portfolio claims

These claims refer to the corrected benchmark harness and the tested engine.
Performance is workload- and host-specific; the full method and observed ranges
are in [benchmarks.md](benchmarks.md).

## Resume

- Built a C++20 event-time stream processor with FNV-1a key partitioning and batched worker queues; measured **27.50M records/s with eight workers, 3.68× Apache Flink 2.3.0**, on a matched 100M-record, checkpoint-disabled local workload on Apple M1 Max (three-run medians; in-memory digest sinks).
- Implemented tumbling/sliding windows, min-combined watermarks, and allowed-lateness re-fires; added **Chandy–Lamport snapshots over fixed-topology TCP channels**, capturing operator and in-flight state with validated global commits and three-process crash recovery.
- Verified **177 CTest cases**, zero differential mismatches over **100 seeds × 10K records per window type across N={1,2,4,8}**, and **26 fork+SIGKILL recovery scenarios with zero missing results in the retained-output harness**; engine tests passed ThreadSanitizer on Linux x86-64 and macOS arm64. Replay duplicates are permitted.

## Portfolio

Stormglass is a C++20 event-time processor with keyed parallelism, tumbling and
sliding windows, and aligned checkpoint recovery, plus a fixed-topology TCP
Chandy–Lamport snapshot mode. It combines active-source watermarks, handles
allowed late updates with re-fires, and restores a common CRC-valid partition
checkpoint. A matched 100M-record local comparison reached 27.50M records/s with
eight workers, 3.68× Apache Flink 2.3.0, using three-run medians on Apple M1 Max
with checkpointing disabled and in-memory digest sinks. All 24 measured jobs
agreed on 66,843,149 outputs and both digests. The TCP mode captures operator and
in-flight channel state and validates a global commit before recovery. Validation
includes 177 CTest cases, cross-worker differential checks, and 26 process-crash
scenarios with zero missing results in the retained-output recovery harness.
Recovery is at-least-once under that replay/output-retention contract;
transactional exactly-once sinks remain future work.

## Interview qualifications

- N counts workers, not total threads. A router runs alongside the workers.
- The comparison measures execution wall time, including native fixture setup
  and Flink local job startup. It is not a warm-JIT, distributed, durable-sink,
  checkpoint, or operator-only comparison. Matching digests are probabilistic
  evidence, not a mathematical proof of identical output.
- This run scaled 2.57× from one to four workers and 3.35× from one to eight;
  N=4 ranged from 17.878 to 21.637M rec/s. Avoid a universal scaling claim.
- Do not reuse the withdrawn 44–56% batching-overhead reduction, the earlier
  N≥4 regression narrative, or the old 30.023M / 4.55× comparison as current
  corrected-harness results. Batching itself is implemented and verified.
- If checkpoint timing is needed, quote the separate 2026-09-06 measurement:
  **0.383 ms mean write for 1,000 panes (20 writes), 5.703 ms median N=4 snapshot
  state load (five runs)**. Load excludes the generator's 72.784 ms median seek.
  These are separate components and workloads, not a 0.383 ms global pause or
  a 5.703 ms complete restart.
- The TCP path supports the claim “Chandy–Lamport snapshots over fixed-topology
  TCP channels,” validated by 100 randomized protocol schedules and three-process
  TCP recovery including a mid-marker SIGKILL. Native `SourceMerge` is still
  deterministic in-process barrier alignment. Neither mode provides a general
  exactly-once output sink; native throughput figures do not measure TCP mode.
- Existing [CI evidence](https://github.com/ryderpongracic1/stormglass/actions/runs/34089133633)
  establishes the two-architecture sanitizer result at `40ffef6`. The benchmark
  changes add 14 report-validator tests and nine executable input checks; these
  are separate from the CTest count. The older CI link covers the pre-TCP
  153-test suite; new snapshot validation is documented in [chandy-lamport.md](chandy-lamport.md).

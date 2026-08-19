# stormglass — Event-Time Stream Processing Engine

Single-process event-time stream processing engine with verified windowed aggregation semantics. Implements tumbling and sliding windows, bounded out-of-orderness watermarks, and at-least-once delivery via atomic checkpoint/restore. Correctness verified by differential testing against an independent oracle across 100+ seeded runs and 20 nemesis (kill-restart) scenarios with zero data loss.

## What this proves

| | distrikv | stormglass |
|---|---|---|
| **Layer** | Infrastructure — consensus, replication, convergence under partition | Application — semantic correctness of temporal computations under failure |
| **Stack** | Go, gRPC, Raft | C++20, SIMD, event-time |
| **Failure model** | Network partition, leader election | Time disorder, crash-recovery |
| **Verification** | Convergence (anti-entropy, chaos nemesis) | Oracle comparison (deterministic replay) |
| **Lineage** | Dynamo / Raft | Flink / Dataflow |

## Architecture

```
Source → [Watermark Injection] → Keyed Window Operator → Sink
                                        ↓
                               Checkpoint Barrier → Snapshot to disk
```

- Single pipeline loop, no threads in v1
- Records flow as contiguous batches; aggregation kernels operate on `int64_t*` spans (SIMD-friendly)
- Watermark = max_event_time − bounded_disorder, in-band as control records
- Checkpoint = atomic snapshot of (window state + source offset) in one file — structurally impossible state/offset mismatch

## Key design decisions

1. **Snapshot checkpoint, not delta-WAL** — window state is bounded and self-expiring; the source itself is the replay log
2. **Degenerate Chandy-Lamport barrier** — single path = trivial; extends to multi-input in v2
3. **SIMD aggregation kernels** — AVX2/SSE4.2 over contiguous int64 spans, not per-record virtual calls
4. **Bounded out-of-orderness watermark model** — wm = max_event_time − bound
5. **Single-threaded by design in v1** — eliminates data races; concurrency is v2's problem

## Measured results

| Metric | Value |
|---|---|
| Throughput | TBD M records/sec |
| SIMD delta | TBD× (batch SIMD vs scalar fold) |
| Checkpoint pause | TBD ms at N keys |

## Verification

- **Deterministic generator** (seeded, reproducible) → engine → compare against batch-mode oracle
- **Oracle** computes expected results without watermarks (naive group-by-window) — obviously correct
- **Nemesis**: kill -9 at targeted phases, restart, assert zero losses after dedup
- **Pass criteria**: 100 seeds × oracle = zero mismatches; 20 nemesis runs = zero lost results

## Limitations

- Single-process only (v2: partition across workers with per-partition watermarks)
- At-least-once, not exactly-once (idempotent sink upgrades to effectively-once)
- Checkpoint pauses the operator (upgrade path: clone-then-serialize-async)
- No persistent source yet (v2: Postgres CDC via logical decoding)

## Building

```bash
make build    # Debug + sanitizers
make test     # Run all tests
make bench    # Release build + benchmark
make clean    # Remove build artifacts
```

Requires CMake ≥ 3.22 and a C++20 compiler (GCC 11+, Clang 14+).

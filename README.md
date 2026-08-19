# stormglass

Event-time stream processing engine with verified windowed aggregation semantics.

Single-process event-time stream processor. Tumbling and sliding windows, bounded out-of-orderness watermarks, at-least-once delivery via atomic checkpoint/restore. Correctness verified by differential oracle testing (100 seeds, zero mismatches) and nemesis crash injection (20 runs, zero data loss).

## What this proves

- **distrikv**: consensus, replication, convergence under partition — infrastructure-layer distributed systems (Go, gRPC, Raft)
- **stormglass**: semantic correctness of temporal computations under failure — application-layer stream processing (C++20, SIMD, event-time)
- Different failure model (time disorder vs network partition), different verification (oracle comparison vs convergence), different systems lineage (Flink/Dataflow vs Dynamo/Raft)

## Architecture

```
Source → [Watermark] → Keyed Window Operator → Sink
                              ↓
                    Checkpoint Barrier → Atomic Snapshot
```

- Records flow as contiguous batches through a single pipeline loop
- Watermark = max_event_time − disorder_bound, injected in-band as control records
- Window operator partitions by key, assigns to time windows, fires on watermark advance
- Checkpoint = atomic snapshot of (window state + source offset) via fsync + rename — structurally impossible state/offset mismatch

## Key design decisions

1. **Snapshot checkpoint, not delta-WAL** — window state is bounded and self-expiring; the source is the replay log
2. **Degenerate Chandy-Lamport barrier** — single pipeline path makes the barrier trivial; extends to multi-input in v2 without rewrite
3. **SIMD aggregation kernels** — explicit AVX2/SSE4.2 intrinsics for contiguous int64 spans, 1.55× over scalar
4. **Bounded out-of-orderness watermarks** — wm = max_event_time − disorder bound; in-band as control records
5. **Single-threaded by design** — eliminates data races; concurrency is v2's problem

## Measured results

| Metric | Value | How measured |
|--------|-------|--------------|
| Pipeline throughput | 3.0 M records/sec | 1M records, Release -O3, tumbling 1s windows |
| SIMD kernel speedup | 1.55× | AVX2 sum vs scalar, 1M elements, 100 iterations |
| Checkpoint pause | <1ms | Atomic snapshot of 1000 panes (10 keys × 100 windows) |

## Verification

```
Oracle differential: 100 seeds × 10K records = 0 mismatches
Nemesis crash test:  20 runs × kill-between-checkpoints = 0 missing results
Duplicate budget:    800 total across 20 crash/restore cycles (at-least-once by design)
```

The oracle computes expected results naively — no watermarks, just group-by-window over the full dataset. This is obviously correct and serves as the reference. The engine processes the same records with bounded disorder and watermark-driven firing. Zero mismatches means the temporal logic produces identical results to batch computation.

Nemesis kills the process at targeted phases (mid-aggregation, mid-checkpoint, post-checkpoint-pre-ack), restores from the last checkpoint, and verifies all oracle-expected results appear in the output. Duplicates are counted, not suppressed — an idempotent sink upgrades to effectively-once.

## Reproduce

```bash
make build && make test              # 103 tests, Debug+ASan
make bench                           # Release throughput numbers
./build-release/app/stormglass_oracle --seeds 100 --records 10000
./build-release/app/stormglass_nemesis --seeds 20 --verbose
```

## Limitations

- Single-process only (v2: partition across workers with per-partition watermarks)
- At-least-once, not exactly-once (idempotent sink upgrades to effectively-once; not built)
- Checkpoint pauses the operator (upgrade path: clone-then-serialize-async)
- No persistent source integration (v2: Postgres CDC via logical decoding)
- SIMD kernels proven in isolation; pipeline uses scalar per-pane accumulation (vectorized window operator is the v2 optimization)

## Tech stack

C++20 · GCC 11 · CMake · GoogleTest · AVX2/SSE4.2 · POSIX (fsync, atomic rename)

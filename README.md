# stormglass

Event-time stream processing engine with verified windowed aggregation semantics.

Single-process event-time stream processor. Tumbling and sliding windows, bounded out-of-orderness watermarks, at-least-once delivery via atomic checkpoint/restore. Correctness verified by differential oracle testing (100 seeds, zero mismatches; heavy-tailed late-data matrix) and a crash nemesis that fork()s the pipeline and SIGKILLs it mid-stream (0 missing results, including kills during checkpoint writes).

## What this proves

- **distrikv**: consensus, replication, convergence under partition — infrastructure-layer distributed systems (Go, gRPC, Raft)
- **stormglass**: semantic correctness of temporal computations under failure — application-layer stream processing (C++20, event-time)
- Different failure model (time disorder vs network partition), different verification (oracle comparison vs convergence), different systems lineage (Flink/Dataflow vs Dynamo/Raft)

## Architecture

```
Source → [Watermark | Checkpoint Barrier] → Keyed Window Operator → Sink
                                                    ↓
                            barrier ⇒ Atomic Snapshot (state + offset)
```

- Records flow as contiguous batches through a single pipeline loop
- Watermark = max_event_time − disorder_bound, injected in-band as control records
- Checkpoint barrier is injected in-band by the source at the checkpoint interval, stamped with the absolute source offset
- Window operator partitions by key into a per-window `key → pane` map ordered by window-end, so a watermark advance touches only the windows that expire — not every live pane
- Checkpoint = atomic snapshot of (window state + the barrier's absolute offset + watermark) via fsync + rename, taken when the operator dequeues the barrier — structurally impossible state/offset mismatch

## Key design decisions

1. **Snapshot checkpoint, not delta-WAL** — window state is bounded and self-expiring; the source is the replay log
2. **Real in-band Chandy-Lamport barrier** — the source emits a checkpoint-barrier control record stamped with the absolute source offset; the operator snapshots when it dequeues it. In-order processing on a single path means everything ≤ the barrier offset is applied and nothing after it is (the degenerate, single-input case); extends to multi-input alignment in v2 without rewrite
3. **Bounded out-of-orderness watermarks** — wm = max_event_time − disorder bound; in-band as control records
4. **Single-threaded by design** — eliminates data races; concurrency is v2's problem

## Measured results

| Metric | Value | How measured |
|--------|-------|--------------|
| Pipeline throughput | median ~9.0 M rec/s (observed 8.2–9.1) | 1M records, tumbling 1s (1000 windows fired), Release -O3 -march=native, 8 runs on shared 4-vCPU Xeon 6975P-C. Earlier builds measured ~1.8 M rec/s with 1.3–3.1 spread; the window-end-ordered pane index removed the per-watermark full-pane scan, which was both the bottleneck and the variance source |
| Checkpoint pause | ~5.6 ms at 1000 panes (~9.2 ms at 10K) | Direct measurement in `make bench` (Checkpoint Pause section): 20 writes each of serialize + fsync + rename + dir-fsync. Dominated by fsync, not serialization |

## Verification

```
Oracle differential:  100 seeds × 10K records = 0 mismatches
Lateness matrix:      {tumbling, sliding} × {L=0, L=2s} × {bounded, heavy-tailed} = all pass;
                      in heavy-tailed cells the engine and oracle agree exactly on drop counts
                      (7294 tumbling / 7770 sliding per 20 seeds) with thousands of
                      within-lateness re-fires — late data is genuinely exercised, by construction
Nemesis (real crash): 20 fork+SIGKILL runs between checkpoints + 6 killed mid-checkpoint-write
                      = 0 missing results; every mid-write kill leaves a real torn .ckpt.tmp
                      that recovery discards before falling back to the last valid checkpoint
```

The oracle computes expected results naively — no watermarks, just group-by-window over the full dataset. This is obviously correct and serves as the reference. The engine processes the same records with watermark-driven firing. The heavy-tailed disorder mode pushes a configurable fraction of records below the watermark, so the allowed-lateness re-fire and beyond-lateness drop paths are exercised for real — a guard test fails if a heavy-tailed run ever produces zero late records.

The nemesis fork()s a child running the real pipeline and SIGKILLs it before the final flush (verified per run: the child never reaches its post-flush sentinel), so in-memory window state genuinely dies. Pre-crash results survive only because the sink fsyncs each emitted window — that durable output, unioned with the post-restore run, must cover the oracle exactly (at-least-once). Duplicates are counted, not suppressed; at the default checkpoint spacing the union is a clean partition (0 duplicates), and an idempotent sink upgrades to effectively-once. Double-crash restore is separately tested to pin absolute-offset checkpoint semantics.

## Reproduce

```bash
make build && make test              # 99 tests, Debug (+ASan where the runtime is available)
make bench                           # Release throughput numbers
./build-release/app/stormglass_oracle --seeds 100 --records 10000
./build-release/app/stormglass_nemesis --seeds 20 --verbose
```

## Limitations

- Single-process only (v2: partition across workers with per-partition watermarks)
- At-least-once, not exactly-once (idempotent sink upgrades to effectively-once; not built)
- Checkpoint pauses the operator (upgrade path: clone-then-serialize-async)
- No persistent source integration (v2: Postgres CDC via logical decoding)

## Tech stack

C++20 · GCC 11 · CMake · GoogleTest · POSIX (fsync, atomic rename)

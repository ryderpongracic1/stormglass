# stormglass

Event-time stream processing engine with verified windowed aggregation semantics.

Single-process, keyed-parallel event-time stream processor. Tumbling and sliding windows, bounded out-of-orderness watermarks, at-least-once delivery via atomic checkpoint/restore. Work is partitioned across N shared-nothing workers by key, and the headline correctness property is that **the same unmodified differential oracle that verifies the single-threaded engine verifies the N-worker engine** — parallelism is invisible to the semantics. Verified by differential oracle testing (100 seeds × {1,2,4,8} workers × three disorder profiles, zero mismatches) and a crash nemesis that fork()s the pipeline and SIGKILLs it mid-stream — including at a *torn distributed checkpoint* — with 0 missing results.

## What this proves

- **distrikv**: consensus, replication, convergence under partition — infrastructure-layer distributed systems (Go, gRPC, Raft)
- **stormglass**: semantic correctness of temporal computations under failure *and under key-parallelism* — application-layer stream processing (C++20, event-time, multi-threaded)
- Different failure model (time disorder vs network partition), different verification (oracle comparison vs convergence), different systems lineage (Flink/Dataflow vs Dynamo/Raft)

## Architecture

```
                    ┌──────────► Worker 0  (owns KeyedWindowState 0) ─┐
                    │            Worker 1  (owns KeyedWindowState 1)  │
 Source ──► Router ─┼──────────► Worker k  (owns KeyedWindowState k) ─┼─► Merge ──► Sink
   │         │      │            ...                                  │     │
   │         │      └──────────► Worker N-1                           │     └─ output watermark
   │         │                                                        │        = min across
   │         └─ DATA:    hash-route by FNV-1a(key) % N (one worker)   │          partitions
   │         └─ CONTROL: BROADCAST to every worker                    │
   │            (watermark + checkpoint barrier)                      │
   └─ per-barrier: each worker snapshots its own state+offset ────────┘
                   into <ckpt>/p<k>/  (all-or-nothing global checkpoint)
```

- The **Router** (one thread) pulls source batches, hash-routes each DATA record to exactly one worker by `FNV-1a(key) % N`, and **broadcasts** every CONTROL record (watermark, checkpoint barrier) to all workers.
- Each **Worker** is shared-nothing: it owns its `KeyedWindowState`, watermark tracker, and assigner, and processes its disjoint key subset. The per-worker windowing core (`KeyedProcessor`) is a faithful extraction of the single-threaded `Pipeline`'s record/watermark/flush logic — same calls, same order, same `L==0` vs `L>0` branches.
- The **Merge** stage (the thread that called `Run`) joins all workers, unions their output as a set, and reports the effective output watermark as the min across partitions.
- The window operator partitions by key into a per-window `key → pane` map ordered by window-end, so a watermark advance touches only the windows that expire — not every live pane (unchanged from v1).

## Key design decisions

1. **Keyed parallelism across N shared-nothing workers** *(reverses v1's "single-threaded by design")* — records are partitioned by key so each worker owns a disjoint slice of the state and never shares mutable memory with another worker. The only shared memory is each worker's bounded input queue (mutex + condvar) and, after join, its result buffer. This is the design v1 deferred as "concurrency is v2's problem"; the arc that follows is the proof that adding it did not change a single result.

2. **Deterministic key partitioning (FNV-1a)** — routing is `FNV-1a(key) % N`, a portable byte-hash with no dependence on `std::hash` (which is implementation- and salt-defined). The same key lands on the same worker across runs, platforms, and — critically — across a restore: replayed records re-route to the workers that hold their state.

3. **Global-broadcast watermark model** — the single source computes the global watermark exactly as the single-threaded engine does (`wm = max_event_time − disorder_bound`) and the Router broadcasts it to every worker. Each worker therefore fires and drops its disjoint keys against the *identical* watermark the single-threaded engine would have used. **This is what makes parallelism invisible to the semantics**: per-partition watermark *generation* would let partitions diverge (a slow partition would hold back a fast one, or vice-versa) and the result set would no longer equal the single-threaded engine's — the oracle would stop verifying it. Per-partition watermark generation needs genuinely independent sources and is a later arc.

4. **Min-across-partitions at Merge — single-source-degenerate, multi-source-ready** — the Merge stage reports the effective output watermark as `min` over the per-partition watermarks. Under one broadcast source every partition holds the same value, so the min is trivially that value; the code takes a real `min` so the multi-source case needs no rewrite. This is the same single-input-degenerate framing as the checkpoint barrier below.

5. **Distributed checkpoint = coordinator-collected snapshot, all-or-nothing** — on each broadcast barrier every worker snapshots its own state + watermark + the barrier's absolute offset into a per-partition subdir `<ckpt>/p<k>/checkpoint-<O>.ckpt`, reusing the single-threaded `CheckpointWriter` **completely unchanged** (same on-disk format, CRC trailer, tmp+rename+dir-fsync discipline). Because the Router broadcasts one absolute offset per barrier, all N partitions snapshot at the *same* offset `O`. A global checkpoint at `O` is **complete iff all N partition files for `O` exist and pass CRC**; a partial set (crash mid-broadcast) is a **torn** global checkpoint and is never restored from — the coordinator falls back to the highest complete `O`. This is explicitly **NOT** per-operator multi-input Chandy-Lamport: every worker is single-input (one queue), so its snapshot is trivially aligned and needs no marker buffering. The only global concern is all-or-nothing completeness across the N independent snapshots.

6. **Idle partitions never stall** — because watermarks are broadcast (not generated per-partition), a worker that owns no keys in the current data still receives the global watermark and the end-of-stream sentinel. It neither holds back global progress nor hangs shutdown; completion is the liveness proof (tested with `num_keys < workers`, ≥5 idle workers).

7. **Snapshot checkpoint, not delta-WAL** — window state is bounded and self-expiring; the source is the replay log *(unchanged)*.

8. **Bounded out-of-orderness watermarks** — `wm = max_event_time − disorder bound`; in-band as control records *(unchanged)*.

## The headline claim: parallelism is invisible to the semantics

> The same **unmodified** oracle that verifies the single-threaded engine verifies the N-worker engine over the **authoritative (deduped) result set**, for N in {1, 2, 4, 8}, across 100 seeds × {bounded, heavy-tailed L=0, heavy-tailed L>0}.

For every seed the harness asserts, at each N, that `engine(N) == oracle` **and** `engine(N) == single-threaded` (result set + drop count). The oracle never learns which engine ran — workload generation, oracle feeding, dedup, the drop-count contract, and the set compare are literally shared code between the single-threaded and partitioned paths.

Reproduce (one invocation per profile):

```bash
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile bounded --lateness-ms 0
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 0 \
    --late-fraction 0.1 --late-tail-ms 6000
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 2000 \
    --late-fraction 0.1 --late-tail-ms 6000
```

**Not an unqualified "bit-for-bit" claim, and the reason is precise.** Raw *emission* counts differ by N. The single-threaded engine's window-keyed re-fire redundantly re-emits co-resident keys (multiple keys sharing a window are all re-emitted when that window re-fires); partitioning splits those keys across workers, so the redundant rows are distributed differently. Those redundant rows are exactly what `DedupEngineResults` (and therefore the oracle comparison) already collapses — the *same* reduction the single-threaded engine has always relied on to match its own oracle. Equality holds over the authoritative (deduped) set, which is the result set that has semantic meaning.

## Measured results

All partitioned numbers are median + observed min/max over the stated reps, measured this session on a **4-vCPU shared Intel Xeon 6975P-C** (the shared box was under contention — the co-measured single-threaded 1M/100-key throughput came in at ~4.2 M rec/s this session versus ~8.5–9.0 on a quiet box, which sharpens the oversubscription reading below). Reproduce with `make bench`.

### Scaling curve — throughput vs N

Workload: 1M records, 1000 keys, tumbling 1s, `MemorySink`, no checkpointing. 7 reps.

| Engine | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| single-threaded `Pipeline` (reference) | **2.63** | 2.41–2.73 |
| `PartitionedPipeline` N=1 | 1.38 | 1.33–1.41 |
| `PartitionedPipeline` N=2 | 1.38 | 1.32–1.40 |
| `PartitionedPipeline` N=4 | 0.92 | 0.87–0.96 |
| `PartitionedPipeline` N=8 | 0.36 | 0.35–0.37 |

**Honest reading — the curve bends at N=1 and regresses from there, exactly as a 4-vCPU box predicts for this workload.** Three effects stack:

1. **The per-record work is tiny** (one integer add into a hash map), so there is very little compute to parallelize. Meanwhile every record crosses a thread boundary through a mutex+condvar bounded queue (Router → Worker). That hand-off costs more than the windowing work it distributes — which is why partitioned **N=1 (1.38) is already ~half of single-threaded (2.63)** despite doing identical windowing: the whole gap is the Router→queue→Worker→Merge plumbing.
2. **The single Router thread is a serial funnel** — it hashes and enqueues *every* record. N=2 shows no gain over N=1 because the workers are never the bottleneck; the router (and queue contention) is.
3. **Oversubscription.** 1 Router + N Workers + the Merge thread = N+2 runnable threads on 4 vCPUs. N=4 (6 threads) already oversubscribes and N=8 (10 threads) thrashes — condvar wakeups and context switches dominate, collapsing throughput to 0.36.

The takeaway is deliberately unflattering: on a compute-light workload on a small, shared box, keyed partitioning buys **parallelism-invisible semantics and multi-source readiness, not throughput**. It would pay off only when per-record work is heavy enough (expensive aggregation, or I/O-bound sinks) to dominate the queue hand-off, and when the core count is not oversubscribed by the router+workers+merge topology.

### Checkpoint overhead & restore

Same partitioned workload at N=4, checkpoint interval 100k records. 5 reps.

| Config | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| checkpointing OFF | 0.83 | 0.77–0.84 |
| checkpointing ON  | 0.91 | 0.90–0.94 |

10 global barriers over the run → **40 per-partition checkpoint files** written (10 barriers × 4 partitions). The two intervals overlap and the ON median lands *nominally higher* than OFF — which is only possible if the true per-barrier fsync cost of 10 infrequent distributed checkpoints is **below the run-to-run variance of this shared box**. So the honest statement is that checkpoint overhead at this spacing is **not measurable above the contention noise floor** — not that checkpointing is free or (obviously not) faster.

Restore time (fresh pipeline restores from the highest complete global checkpoint; load only, not the drain), N=4, 5 reps, restored from offset 1,000,000:

| Component | Time | min–max |
|-----------|-----:|:--------|
| snapshot load (coordinator scan + LoadOffset ×4 + Restore) | **1.68 ms** | 1.66–1.74 ms |
| source Seek (in-memory generator, O(offset) replay) | 117.7 ms | 117.0–129.4 ms |

The **1.68 ms snapshot load is the portable restore cost** — scan four partition directories, CRC-validate and load four checkpoint files, restore the panes. The 118 ms source Seek is the `DeterministicGenerator` replaying a million records to reposition itself, an O(offset) property of this toy in-memory source; a replayable-log source (Kafka offset seek) is ~O(1). The two are reported separately so the toy-source cost never masks the real one.

### Single-threaded core (unchanged from v1)

| Metric | Value | How measured |
|--------|-------|--------------|
| Pipeline throughput (default config) | ~8.5–9.0 M rec/s (quiet box) | 1M records, 100 keys, tumbling 1s, Release -O3 -march=native, interleaved A/B reps on 4-vCPU Xeon 6975P-C. An earlier published ~1.8 M rec/s figure was measured on the same box under contention and has been retracted as an engine number |
| Pane-index scaling (isolation bench) | flat ~10 M rec/s at 20K live panes, all watermark rates | `stormglass_scanbench`: before the index, throughput swung 16× with watermark frequency and collapsed 11× as pane count grew; after, both sweeps are flat within ~1.1× |
| Checkpoint pause | ~5.6 ms at 1000 panes (~10 ms at 10K) | Direct measurement in `make bench` (Checkpoint Pause section): 20 writes each of serialize + fsync + rename + dir-fsync. Dominated by fsync |

## Verification

```
Oracle differential:  100 seeds × 10K records = 0 mismatches (single-threaded)
Cross-N proof:        100 seeds × N in {1,2,4,8} × {bounded, heavy-tailed L=0, heavy-tailed L>0}
                      = all pass; every N matches BOTH the oracle and the single-threaded
                      reference on result set AND drop count. Non-vacuous:
                        bounded          0 drops
                        heavy-tailed L=0 76010 drops (engine == oracle)
                        heavy-tailed L>0 36378 drops / 13255 re-fires / 39632 late-accepted
Lateness matrix:      {tumbling, sliding} × {L=0, L=2s} × {bounded, heavy-tailed} = all pass;
                      engine and oracle agree exactly on drop counts in every heavy-tailed cell
Nemesis (real crash, single-threaded):
                      20 fork+SIGKILL runs between checkpoints + 6 killed mid-checkpoint-write
                      = 0 missing; every mid-write kill leaves a real torn .ckpt.tmp that
                      recovery discards before falling back to the last valid checkpoint
Nemesis (real crash, PARTITIONED, torn global checkpoint):
                      20 fork+SIGKILL runs at a torn global checkpoint, N in {2,4}
                      = 20/20 passed, 0 missing, 0 duplicates; 20/20 genuine SIGKILL,
                      20 torn global checkpoints captured on disk, restore fell back to a
                      COMPLETE checkpoint strictly below the torn offset every time
```

The oracle computes expected results naively — no watermarks, just group-by-window over the full dataset — and serves as the obviously-correct reference. The heavy-tailed disorder mode pushes a configurable fraction of records below the watermark, so the allowed-lateness re-fire and beyond-lateness drop paths are exercised for real (a guard test fails if a heavy-tailed run ever produces zero late records).

The **partitioned nemesis** arms at a torn global checkpoint: it runs a partitioned pipeline with per-worker durable sinks, fork()s it, and SIGKILLs the child after some — but not all — partitions have written their file for a given barrier offset, leaving a genuine torn global checkpoint on disk (verified: the child never reaches its post-flush sentinel). On restart the coordinator ignores the torn offset, restores every worker from the highest *complete* global checkpoint below it, and the union of (durable pre-crash output ∪ post-restore output) must cover the oracle with 0 missing. Deterministic FNV-1a routing guarantees replayed records reach the workers that hold their restored state.

Crash recovery is also tested *with late data in flight* (single-threaded): restore resets the watermark backward, but replay feeds the identical record sequence, so the watermark trajectory re-derives exactly and every drop/accept decision is recovery-deterministic. The watermark is a pure function of the replayed prefix.

## Reproduce

```bash
make build && make test              # 114 tests, Debug (+ASan where the runtime is available)
make bench                           # Release throughput: single-threaded + checkpoint pause
                                     #   + partitioned scaling curve + checkpoint overhead + restore
./build-release/app/stormglass_scanbench   # pane-index isolation sweeps

# Single-threaded oracle + lateness matrix
./build-release/app/stormglass_oracle --seeds 100 --records 10000
./build-release/app/stormglass_oracle --matrix --seeds 20 --records 10000 \
    --lateness-ms 2000 --late-fraction 0.1 --late-tail-ms 6000

# Cross-N proof: parallelism is invisible to the semantics (one run per profile)
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile bounded --lateness-ms 0
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 0 \
    --late-fraction 0.1 --late-tail-ms 6000
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 2000 \
    --late-fraction 0.1 --late-tail-ms 6000

# Single-threaded crash nemesis
./build-release/app/stormglass_nemesis --real-kill --real-phase between --seeds 20
./build-release/app/stormglass_nemesis --real-kill --real-phase mid-checkpoint \
    --records 30000 --checkpoint-interval 500 --keys 100 --seeds 6

# Partitioned crash nemesis: fork+SIGKILL at a torn global checkpoint
./build-release/app/stormglass_nemesis --partitioned --workers 4
./build-release/app/stormglass_nemesis --partitioned --workers 2
```

## Limitations

- **Single source.** Watermarks are generated once and broadcast, which is what makes the N-worker result set equal the single-threaded one. Genuine multi-source watermark divergence (independent sources, per-partition watermark generation, real min-combine at the merge) is future work — the merge already takes a real `min` so the plumbing is ready, but the semantics are not yet exercised.
- **Oversubscription on small core counts.** On a 4-vCPU box the router + N workers + merge topology oversubscribes at N≥4 and throughput regresses (see the scaling curve). Partitioning here is for correctness and multi-source readiness, not throughput on compute-light workloads.
- **Per-partition checkpoint retention = last 2.** The reused `CheckpointWriter` retains the last 2 checkpoints per partition. A single torn offset above the last complete one is safe (the complete one is still present), but **more than one stacked torn offset above the last complete checkpoint could evict it** — deeper retention is needed before relying on multi-torn recovery. The tested single-torn scenario is safe.
- **At-least-once, not exactly-once** (idempotent sink upgrades to effectively-once; not built).
- **Checkpoint pauses the operator** (upgrade path: clone-then-serialize-async).
- **No persistent source integration** (future: Postgres CDC via logical decoding); the in-memory generator's `Seek` is O(offset).
- **ThreadSanitizer is CI/Mac-only.** DevSpaces lacks libtsan (and libasan), so the sanitized/TSan build runs in CI and on macOS, not on the DevSpaces box these numbers were measured on.

## Tech stack

C++20 · GCC 11 · CMake · GoogleTest · POSIX threads (std::thread, mutex + condvar) · POSIX I/O (fsync, atomic rename)

# stormglass

Event-time stream processing engine with verified windowed aggregation semantics.

Single-process, keyed-parallel event-time stream processor with **multi-source fan-in**. Tumbling and sliding windows, bounded out-of-orderness watermarks, at-least-once delivery via atomic checkpoint/restore. Work is partitioned across N shared-nothing workers by key, and **K independent sources are merged upstream by a `SourceMerge`** that min-combines their watermarks, detects idle sources deterministically, and aligns their checkpoint barriers K-way (Chandy-Lamport). The headline correctness property is that **an unmodified differential oracle, fed the same deterministic interleaved multi-source input, computes exactly the drop/fire decisions the engine makes** — and crash recovery holds at-least-once through SIGKILLs landed *mid-barrier-alignment*.

## What this proves

- **distrikv**: consensus, replication, convergence under partition — infrastructure-layer distributed systems (Go, gRPC, Raft)
- **stormglass**: semantic correctness of temporal computations under failure, under key-parallelism, *and under multi-source watermark merging* — application-layer stream processing (C++20, event-time, multi-threaded)
- Different failure model (time disorder vs network partition), different verification (oracle comparison vs convergence), different systems lineage (Flink/Dataflow vs Dynamo/Raft)

## The arcs

- **v1** — single-threaded event-time core: tumbling/sliding windows, bounded-disorder watermarks, allowed-lateness re-fire/drop, atomic checkpoint/restore, verified against a naive oracle.
- **v2** — keyed parallelism: partition by key across N shared-nothing workers. Proved *parallelism is invisible to the semantics* — the N-worker result set equals the single-threaded one because one source's watermark is broadcast to every worker.
- **v3** — multi-source fan-in: merge K independent sources ahead of the (unchanged) pipeline. Min-combined watermarks, deterministic idle-source detection with resumed-late semantics, and real K-way barrier alignment — each verified against the same unmodified oracle, plus a mid-alignment crash nemesis. **This document is the v3 close.**

## Architecture

```
 ┌─ src 0 ─┐
 │  src 1  │   ┌───────────── SourceMerge ─────────────┐
 │  src k  ├──►│  • deterministic round-robin interleave│
 │   ...   │   │  • min-combine watermark (running MIN)  │      ┌──────────► Worker 0 ─┐
 └─ src K-1┘   │  • idle detection (empty-pull timeout)  │      │            Worker 1  │
               │  • K-way barrier ALIGNMENT (Chandy-     ├──► Router ─┼──► Worker k ────┼─► Merge ─► Sink
               │    Lamport: block early channel until   │      │      │    ...          │     │
               │    all active channels deliver barrier N)│      │      └──► Worker N-1 ──┘     └─ out wm
               └─────────────────────────────────────────┘      │         hash-route DATA        = min
                        ONE Source, ONE pulling thread           │         broadcast CONTROL      across
                        (alignment adds NO new concurrency)      └─ per-barrier: each worker snapshots  parts
                                                                    state+offset into <ckpt>/p<k>/
```

**Upstream (v3): `SourceMerge`** wraps K `DeterministicGenerator`s and presents them as one `Source`. It pulls its wrapped sources in a fixed deterministic round-robin, intercepts each source's own watermarks and emits a single merged watermark equal to the running MIN across active channels, and — when each wrapped source emits its own checkpoint barriers — performs real K-way alignment: an early channel is *blocked* (its records buffered, not pulled past the barrier) until barrier N has arrived on every active channel, at which point one merged barrier stamped with the aligned cut is emitted downstream. It is a `Source`, not a thread, so it drops into the existing `Pipeline` / `PartitionedPipeline` **unchanged — Phase 1–3 add zero new concurrency.**

**Downstream (v2, unchanged):**
- The **Router** (one thread) pulls source batches, hash-routes each DATA record to exactly one worker by `FNV-1a(key) % N`, and **broadcasts** every CONTROL record (watermark, checkpoint barrier) to all workers.
- Each **Worker** is shared-nothing: it owns its `KeyedWindowState`, watermark tracker, and assigner, and processes its disjoint key subset. The per-worker windowing core (`KeyedProcessor`) is a faithful extraction of the single-threaded `Pipeline`'s record/watermark/flush logic.
- The **Merge** stage joins all workers, unions their output as a set, and reports the effective output watermark as the min across partitions.
- The window operator partitions by key into a per-window `key → pane` map ordered by window-end, so a watermark advance touches only the windows that expire (unchanged from v1).

## Key design decisions

### v3 — multi-source fan-in (this arc)

1. **Min-combined watermark, monotonic-clamped on resume.** The merged watermark is the running MIN across the *active* source channels; a lagging source (smaller watermark, or not yet reported) holds the merged watermark back. The combiner emits a merged watermark **only when the MIN strictly advances**, so it never regresses. When an idle source resumes at its retained (stale) watermark, it simply re-pins the MIN — it cannot move the merged watermark backward. Its below-watermark records are therefore emitted as **genuinely late data** and classified by the *existing* allowed-lateness policy (dropped beyond deadline, accepted + re-fired within). SourceMerge does not special-case them.

2. **Idleness is a deterministic empty-pull timeout — never wall-clock.** A source is marked idle (excluded from the MIN) after a configured number of *consecutive empty round-robin pulls*, a purely logical measure. This is the whole reason the oracle can replay it: a wall-clock idle policy would make the merged trajectory non-deterministic and unverifiable. `idle_timeout == 0` disables idleness entirely and reproduces the Phase-1 min-combine trajectory bit-for-bit.

3. **Idle channels are excluded from the alignment set.** A channel excluded from the watermark MIN is simultaneously excluded from barrier alignment. Without this, an active channel would block at its next barrier *forever* waiting for a quiet source — a deadlock. With it, the remaining active channels keep closing aligned epochs alone while the quiet source is idle; on resume it rejoins the set and catches up. This is a liveness guarantee, proven directly (`Alignment.IdleChannelIsExcludedAndDoesNotDeadlockAlignment`).

4. **SourceMerge is one Source pulled by one thread — alignment adds NO new concurrency.** This is stated plainly because it is a **scope boundary, not a flaw**: the K-way Chandy-Lamport alignment is implemented as sequential round-robin bookkeeping over in-memory buffers, not as K concurrent input threads with marker buffering. Genuinely concurrent independent sources (each on its own thread/socket) are a later arc; the alignment *algorithm* is exercised here in its deterministic, single-threaded form.

5. **Merged barrier sits exactly at the aligned cut.** Epoch N closes only when every active channel has delivered its barrier N; the merged barrier is stamped with the merged offset of that cut. With per-source intervals `I_i`, the Nth merged barrier lands at exactly `N · Σ I_i` — any record leaking past a channel's barrier would push it above, a premature close would leave it below. Exact equality is a direct proof of "records preceding barrier N on *every* channel — no leak, none missing" (`Alignment.MergedBarrierSitsExactlyAtAlignedCut`).

6. **K = 1 is a literal passthrough.** A single wrapped source reduces byte-for-byte to the bare generator's stream — same records, same barrier offsets `N, 2N, …`, same strictly-advancing watermark subsequence — so v1/v2 behavior is preserved exactly and serves as a regression guard.

### v2 — keyed parallelism (unchanged)

7. **Keyed parallelism across N shared-nothing workers** — records are partitioned by key so each worker owns a disjoint slice of state and never shares mutable memory. The only shared memory is each worker's bounded input queue (mutex + condvar) and, after join, its result buffer.

8. **Deterministic key partitioning (FNV-1a)** — routing is `FNV-1a(key) % N`, a portable byte-hash independent of `std::hash`. The same key lands on the same worker across runs, platforms, and restores.

9. **Global-broadcast watermark model (single-source path)** — when driven by ONE source, the watermark is computed once and broadcast to every worker, which is what made the v2 N-worker result set equal the single-threaded one. **Under `SourceMerge` the single broadcast watermark is the merged MIN** — the Router still broadcasts one watermark stream, now sourced from the combiner rather than a lone generator.

10. **Distributed checkpoint = coordinator-collected snapshot, all-or-nothing** — on each broadcast barrier every worker snapshots its own state + watermark + the barrier's absolute offset into `<ckpt>/p<k>/checkpoint-<O>.ckpt`, reusing the single-threaded `CheckpointWriter` unchanged. A global checkpoint at `O` is complete iff all N partition files for `O` exist and pass CRC; a partial set (crash mid-broadcast) is a **torn** global checkpoint, never restored from.

11. **Snapshot checkpoint, not delta-WAL; bounded out-of-orderness watermarks** — window state is bounded and self-expiring; the source is the replay log; `wm = max_event_time − disorder bound`, in-band as control records (unchanged from v1).

## The v3 thesis: multi-source semantics match the oracle, and recovery survives mid-alignment kills

v2 proved *parallelism is invisible to the semantics* — the N-worker result set equals the single-threaded one because one source's watermark is broadcast to every worker. **v3 does not extend that invariance; it replaces the premise.** With K independent sources there is no single broadcast watermark for the result to be invariant over — the watermark is now a derived quantity (the min-combine), and idle detection and barrier alignment introduce new decision points. So the claim is restated precisely for the multi-source engine:

> The engine's **min-combined watermark**, **deterministic idle-source policy**, and **K-way-aligned checkpoints** produce **exactly** the drop/fire decisions an **unchanged oracle** computes from the **same deterministic interleaved multi-source input** — for K in {1, 2, 3}, over both the single-threaded `Pipeline` and the N-worker `PartitionedPipeline` — and crash recovery holds **at-least-once through SIGKILLs landed mid-barrier-alignment**.

The oracle does **not** re-derive min-combine, idleness, or alignment. It consumes whatever merged stream `SourceMerge` emits — records in merged order via `AddRecord`, the min-combined watermarks via `AdvanceWatermark` — and applies its existing group-by-window + lateness logic. Equality therefore proves that the *merged trajectory itself* (which watermark advanced when, which source went idle and resumed, where the aligned barriers fell) drives the same semantic outcome the naive reference computes. **This is not the v2 invariance carried forward** — it is a distinct property about a distinct engine, and it is stated and tested as such. (Barriers do not affect aggregation, so alignment gets its own direct coverage rather than being routed through the oracle.)

## Measured results

All numbers are median + observed min/max over the stated reps, measured this session on a **4-vCPU shared Intel Xeon 6975P-C** — a shared box under contention, so absolute throughput drifts run-to-run (the co-measured single-threaded 1M/1000-key figure ranged ~2.0–2.6 M rec/s across three full bench runs). Reproduce with `make bench`. Three full runs agreed on every trend below except where noted; the tables quote run 1.

### v3 — SourceMerge overhead (fixed total record count)

`SourceMerge` wraps K sources behind one thread. Holding the **total merged record count fixed at 1M** (each source produces ~1M/K records), so per-record work is constant and only K varies. Uniform per-source event-time rate isolates the merge machinery (round-robin pull + watermark interception/min-combine) from windowing divergence. 7 reps.

| Engine | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| bare `Pipeline` (single source, reference) | **2.60** | 2.50–2.67 |
| `SourceMerge` K=1 | 2.05 | 1.98–2.14 |
| `SourceMerge` K=2 | 2.35 | 2.21–2.40 |
| `SourceMerge` K=3 | 2.44 | 2.34–2.51 |

**Honest reading — the K=1 cost is not stable enough to quote as a number; the K-trend is.** Across three full runs the bare-`Pipeline` reference itself swung 1.97 / 2.39 / 2.60 M rec/s (contention on the shared box) while `SourceMerge` K=1 held a tight 2.00–2.05 — so the apparent K=1 gap ranged from ~20% down to zero depending on which run you read. The wrapper does real extra work (every item is variant-visited and copied from a wrapped batch into the merged batch, plus round-robin bookkeeping and per-watermark min-combine), but on this box its cost is at or below the contention noise floor. What *is* stable across all three runs: K=2 and K=3 consistently outrun K=1 at the same total work — the likely mechanism, consistent with the combiner emitting only on a strict MIN advance: with K interleaved same-rate channels the merged MIN advances less often *per source-watermark*, so higher K **coalesces** per-source watermark advances into fewer merged watermarks, triggering fewer downstream watermark-driven window scans. The merge is single-threaded and its cost does not grow with fan-in on this workload.

### v3 — K-way alignment cost (barriers ON vs OFF)

Same fixed-total workload, per-source checkpoint barriers ON at a realistic interval (~10 aligned epochs over the run) vs OFF. `checkpoint_dir` is empty, so the pipeline dispatches barriers but writes nothing — this isolates the **alignment machinery** (early-channel blocking + buffering + epoch-close), not fsync. 7 reps.

| Config | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| K=2 barriers OFF | 2.39 | 2.22–2.41 |
| K=2 barriers ON  | 2.35 | 2.17–2.41 |
| K=3 barriers OFF | 2.45 | 2.40–2.51 |
| K=3 barriers ON  | 2.45 | 2.26–2.51 |

**Honest reading — K-way alignment at this cadence is not measurable above the contention noise floor.** The ON/OFF min–max bands overlap fully at both K, and the sign of the median delta flips between the two full bench runs (K=2: −1.8% then +1.4%; K=3: +0.1% then −0.9%) — the same discipline the partitioned checkpoint-overhead table applies. So the honest statement is that ~10 aligned epochs cost **below the run-to-run variance of this shared box**, not that alignment is free. (Alignment blocks a channel only until its peers catch up, over in-memory buffers with no I/O, so a low cost is expected — but it is reported as *unmeasurable here*, not *zero*.)

### v2 — partitioned scaling curve (unchanged workload)

Workload: 1M records, 1000 keys, tumbling 1s, `MemorySink`, no checkpointing. 7 reps.

| Engine | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| single-threaded `Pipeline` (reference) | **2.59** | 2.28–2.65 |
| `PartitionedPipeline` N=1 | 1.39 | 1.28–1.43 |
| `PartitionedPipeline` N=2 | 1.51 | 1.47–1.54 |
| `PartitionedPipeline` N=4 | 0.90 | 0.88–0.94 |
| `PartitionedPipeline` N=8 | 0.47 | 0.38–0.60 |

**The curve regresses on a 4-vCPU box, exactly as predicted.** The per-record work is tiny (one integer add into a hash map), so the mutex+condvar queue hand-off (Router → Worker) costs more than the windowing it distributes — partitioned N=1 (1.39) is already ~half of single-threaded (2.59). The single Router thread is a serial funnel (N=2 barely gains), and 1 Router + N Workers + Merge = N+2 runnable threads oversubscribe 4 vCPUs at N≥4. Keyed partitioning here buys **parallelism-invisible semantics and multi-source readiness, not throughput** on compute-light workloads.

### v2 — checkpoint overhead & restore (unchanged)

Partitioned workload at N=4, checkpoint interval 100k records. 5 reps.

| Config | Throughput (M rec/s) | min–max |
|--------|---------------------:|:--------|
| checkpointing OFF | 1.08 | 0.94–1.15 |
| checkpointing ON  | 1.02 | 1.01–1.04 |

10 global barriers over the run → 40 per-partition checkpoint files (10 × 4). The two bands overlap and the sign of the median delta flips between runs (−5.7% then +11.0%), so checkpoint overhead at this spacing is **not measurable above the contention noise floor** — not that checkpointing is free.

Restore (fresh pipeline restores from the highest complete global checkpoint; load only), N=4, 5 reps, restored from offset 1,000,000:

| Component | Time | min–max |
|-----------|-----:|:--------|
| snapshot load (scan + LoadOffset ×4 + Restore) | **1.73 ms** | 1.67–1.94 ms |
| source Seek (in-memory generator, O(offset) replay) | 123.3 ms | 119.9–126.6 ms |

The **1.73 ms snapshot load is the portable restore cost**; the 123 ms source Seek is the `DeterministicGenerator` replaying a million records — an O(offset) property of this toy source. A replayable-log source (Kafka offset seek) is ~O(1). Reported separately so the toy-source cost never masks the real one.

### Single-threaded core (unchanged from v1)

| Metric | Value | How measured |
|--------|-------|--------------|
| Pipeline throughput (default config) | ~8.5–9.0 M rec/s (quiet box) | 1M records, 100 keys, tumbling 1s, Release -O3 -march=native. An earlier published ~1.8 M rec/s figure was measured under contention and has been retracted as an engine number |
| Pane-index scaling (isolation bench) | flat ~10 M rec/s at 20K live panes | `stormglass_scanbench`: before the index, throughput swung 16× with watermark frequency; after, flat within ~1.1× |
| Checkpoint pause | ~5.6 ms at 1000 panes (~10 ms at 10K) | Direct measurement in `make bench`: 20 writes each of serialize + fsync + rename + dir-fsync. Dominated by fsync |

## Verification

143 tests (Debug + ASan/UBSan where the runtime is available; also passes Release with sanitizers off). The v3 arc adds direct alignment tests, the multi-source differential, the idle differential, the min-combine unit tests, and the mid-alignment crash nemesis.

```
── v1/v2 (carried forward) ──────────────────────────────────────────────────
Oracle differential:  100 seeds × 10K records = 0 mismatches (single-threaded)
Cross-N proof:        100 seeds × N in {1,2,4,8} × 3 disorder profiles = all pass;
                      every N matches BOTH the oracle and single-threaded on set + drops
Lateness matrix:      {tumbling, sliding} × {L=0, L=2s} × {bounded, heavy-tailed} = all pass
Nemesis (real crash): 20 fork+SIGKILL between checkpoints + 6 mid-checkpoint = 0 missing;
                      partitioned torn-global-checkpoint nemesis 20/20, N in {2,4}, 0 missing

── v3 multi-source (this arc) ───────────────────────────────────────────────
Min-combine unit:     MinWatermarkCombinerIdle.* — excluding an idle source advances the
                      MIN; all-idle holds steady and never regresses
Direct alignment:     Alignment.MergedBarrierSitsExactlyAtAlignedCut  — Nth merged barrier
                        at exactly N·Σ(intervals): 20/20 epochs at n·400, 8000 records, no leak
                      Alignment.EarlyChannelIsBlockedUntilAllChannelsDeliverBarrier — early
                        channel held (barrier count frozen) until all K deliver; cut at 5050
                      Alignment.KOneReducesToSingleSourceBarriers — K=1 ≡ bare generator,
                        records/barriers/watermarks identical
                      Alignment.SeekReproducesIdenticalAlignedSequence — restore replays the
                        aligned merged sequence bit-for-bit
                      Alignment.IdleChannelIsExcludedAndDoesNotDeadlockAlignment — epochs keep
                        closing while a channel is idle-excluded; stream drains, no deadlock
Multi-source diff:    engine == oracle over the merged stream, K in {1,2,3} × 3 profiles,
                      single-threaded AND PartitionedPipeline N=4. Real run (50 seeds,
                      4000 rec/source, single-threaded):
                        K=2 bounded L=0        50/50  engine_drop=0     oracle_drop=0
                        K=3 bounded L=0        50/50  engine_drop=0     oracle_drop=0
                        K=2 heavy-tailed L=2s  50/50  engine_drop=1247  oracle_drop=1247
                                                      refired=1864  late_accepted=15120
                        K=3 heavy-tailed L=2s  50/50  engine_drop=1312  oracle_drop=1312
                                                      refired=1871  late_accepted=15859
                      Over PartitionedPipeline N=4 (30 seeds, K=3 heavy-tailed L=2s):
                        30/30  engine_drop=820  oracle_drop=820  refired=3478  late_accepted=9469
                      K=1 reduces EXACTLY to the single-source path (MultiSourceKOne).
Idle differential:    MultiSourceIdleDifferential — the slowest source goes idle then resumes;
                      under BOUNDED disorder (never late on its own) the resumed-below-watermark
                      records are the ONLY lateness, so a nonzero drop/re-fire count is direct
                      proof the resumed-late path is exercised, and engine == oracle proves it
                      is handled correctly (K in {2,3}). Idle DISABLED reproduces Phase-1
                      (drop-free) — the capability is purely additive.
Mid-alignment nemesis: fork+SIGKILL WHILE alignment is in progress (fast channel blocked at its
                      barrier, slow channel has not delivered, so merged barrier N was never
                      emitted and no checkpoint for epoch N exists). Restore falls back to the
                      last fully-aligned checkpoint. Real run (5 runs):
                        5/5 passed, 0 missing, 5/5 SIGKILL, 5 partial-alignment captures
                        each: epoch=3 delivered=1/2 stride=500 restored@1000
                              pre=0 post=120 union=120 oracle=120 missing=0 dup=0
                      Evidence line: killed=SIGKILL flush=no partial=yes — restore offset is a
                      multiple of the merged stride, strictly below the mid-alignment epoch cut.
```

The oracle computes expected results naively (no watermarks, just group-by-window over the full dataset) and serves as the obviously-correct reference. The heavy-tailed disorder mode pushes a configurable fraction of records below the watermark, so the late-drop / re-fire paths are exercised for real (a guard test fails if a heavy-tailed run produces zero late records). The mid-alignment nemesis's `pre=0` is expected and not a gap: a crash at an early epoch can precede the first durable window emit, so at-least-once is carried entirely by the post-restore replay from the last complete aligned checkpoint — exactly the property under test.

## Cross-platform note

Absolute drop/late counts are **libstdc++ (GCC) numbers**. `std::uniform_int_distribution` is implementation-defined, so libc++ (Clang/macOS) generates a *different* record stream and reports *different* absolute counts (e.g. a different `engine_drop` total). **The invariant is not the count — it is `engine_drop == oracle_drop` (and the full result-set equality) on each platform independently**, because the engine and the oracle consume the *same* generated stream on whichever stdlib is running. This holds for the single-source, cross-N, and multi-source differentials alike: the K=2/K=3 heavy-tailed numbers above reproduce on libstdc++; on libc++ the totals shift but engine still equals oracle seed-for-seed.

## Reproduce

```bash
make build && make test              # 143 tests, Debug (+ASan/UBSan where the runtime is available)
make bench                           # Release: single-threaded + checkpoint pause + partitioned
                                     #   scaling/overhead/restore + SourceMerge overhead + alignment cost
./build-release/app/stormglass_scanbench   # pane-index isolation sweeps

# Single-threaded oracle + lateness matrix
./build-release/app/stormglass_oracle --seeds 100 --records 10000
./build-release/app/stormglass_oracle --matrix --seeds 20 --records 10000 \
    --lateness-ms 2000 --late-fraction 0.1 --late-tail-ms 6000

# Cross-N proof (v2): parallelism is invisible to the semantics (one run per profile)
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile bounded --lateness-ms 0
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 0 \
    --late-fraction 0.1 --late-tail-ms 6000
./build-release/app/stormglass_oracle --cross-n --seeds 100 --disorder-profile heavy-tailed --lateness-ms 2000 \
    --late-fraction 0.1 --late-tail-ms 6000

# Multi-source differential (v3): engine == oracle over the merged K-source stream
./build-release/app/stormglass_oracle --sources 2 --seeds 50 --records 4000 --disorder-profile bounded --lateness-ms 0
./build-release/app/stormglass_oracle --sources 3 --seeds 50 --records 4000 \
    --disorder-profile heavy-tailed --lateness-ms 2000 --late-fraction 0.1 --late-tail-ms 6000
./build-release/app/stormglass_oracle --sources 3 --workers 4 --seeds 30 --records 4000 \
    --disorder-profile heavy-tailed --lateness-ms 2000 --late-fraction 0.1 --late-tail-ms 6000   # over N=4

# Idle differential (v3): exposed as tests (idle params are not on the oracle CLI)
cd build-release && ctest -R MultiSourceIdleDifferential --output-on-failure

# Crash nemeses
./build-release/app/stormglass_nemesis --real-kill --real-phase between --seeds 20      # single-threaded
./build-release/app/stormglass_nemesis --partitioned --workers 4                        # torn global checkpoint
./build-release/app/stormglass_nemesis --alignment-kill --seeds 5 --verbose             # v3 mid-alignment
```

## Limitations & what's next

**Delivered in v3 (no longer future work):** genuine multi-source watermark divergence — independent sources with per-source watermark generation, real min-combine at the merge, deterministic idle detection with resumed-late semantics, and K-way barrier alignment — is done and verified above. The v2 note that called this "future work, plumbing ready but semantics not yet exercised" is retired.

Remaining boundaries:

- **Sources are still deterministic in-memory generators.** No Kafka / CDC / socket integration; the merge and its alignment are exercised in their deterministic, replayable form (which is what makes the oracle proof possible), not against a live broker. Integrating a real replayable-log source is the next arc.
- **Single process; alignment adds no concurrency.** `SourceMerge` is one Source pulled by one thread — the K-way Chandy-Lamport alignment is sequential bookkeeping over in-memory buffers, not K concurrent input threads with marker buffering. Genuine cross-machine distribution (sources and workers on separate hosts, network markers, distributed coordinator) is out of scope.
- **At-least-once, not exactly-once** (idempotent sink upgrades to effectively-once; not built).
- **Oversubscription on small core counts** — on a 4-vCPU box the router + N workers + merge topology regresses at N≥4 (see the scaling curve). Partitioning is for correctness and multi-source readiness, not throughput on compute-light workloads.
- **Per-partition checkpoint retention = last 2.** A single torn offset above the last complete one is safe; more than one stacked torn offset could evict it — deeper retention is needed before relying on multi-torn recovery.
- **Checkpoint pauses the operator** (upgrade path: clone-then-serialize-async).
- **ThreadSanitizer is CI/Mac-only** (the DevSpaces build host lacks libtsan/libasan). Verified: TSan-instrumented run of all partitioned/invariance/cross-N tests on Apple clang/arm64 — zero race reports.

## Tech stack

C++20 · GCC 11 · CMake · GoogleTest · POSIX threads (std::thread, mutex + condvar) · POSIX I/O (fsync, atomic rename).

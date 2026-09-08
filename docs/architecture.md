# Architecture

The native path has two single-process execution modes: `Pipeline` for one
processing thread and `PartitionedPipeline` for keyed parallel execution.
A separate TCP path combines `snapshot::Participant`, `TcpTransport` and
`WindowOperator` for distributed snapshots over a fixed peer topology.
Both paths use the same window aggregation core.

## Dataflow

```text
K sources
   │
   ▼
SourceMerge ── records + merged watermarks + aligned barriers
   │
   ▼
Router ─────── FNV-1a(key) % N
   │
   ├── worker 0: queue → KeyedProcessor → private state → private sink
   ├── worker 1: queue → KeyedProcessor → private state → private sink
   └── worker N: queue → KeyedProcessor → private state → private sink
   │
   ▼
join + result union + minimum output watermark
```

The source and router run together on one router thread. Each worker runs on its
own thread. The caller joins the router and workers before reading worker
statistics or merging in-memory results.

## Components

| Component | Responsibility |
|---|---|
| `Source` | Produces batches containing records and in-band control records |
| `SourceMerge` | Deterministically interleaves K sources, combines watermarks, detects logical idleness, and aligns barriers |
| `PartitionedPipeline` | Builds workers, routes records, broadcasts control records, manages shutdown, and merges results |
| `KeyedProcessor` | Applies window assignments, updates aggregates, advances watermarks, fires windows, and writes snapshots |
| `KeyedWindowState` | Owns active panes, fired panes retained for lateness, and pending re-fire state |
| `CheckpointWriter` / `Reader` | Serialize, sync, validate, and restore partition state |
| `Sink` | Receives window results; implementations include memory, stdout, and durable crash-harness sinks |

## Keyed execution

Routing uses the portable byte hash `FNV-1a(key) % N`. This assigns disjoint key
subsets rather than contiguous key ranges. Every record for a key reaches the
same worker for a fixed worker count, including after deterministic replay.

Workers share no window state. Each owns its assigner, watermark tracker,
`KeyedWindowState`, and sink. The only concurrent handoff is the bounded queue
between the router and each worker.

A queue element is a batch of messages routed from one source batch. The router
preserves source order inside every worker batch and enqueues batches FIFO. This
amortizes mutex acquisition and notification across many records while retaining
backpressure. The queue is mutex/condition-variable based; stormglass does not
claim a lock-free handoff.

Data records go to one worker. Watermarks and checkpoint barriers are broadcast
to all workers. End-of-stream is also an in-band message, ordered after the last
data and control batch.

## Multi-source fan-in

`SourceMerge` wraps K sources and exposes the ordinary `Source` interface. It
pulls channels in deterministic round-robin order and maintains per-channel
watermark, idle, exhausted, and barrier state.

The emitted watermark is the monotonically increasing minimum across active
channels. A channel that exceeds the configured number of consecutive empty
pulls becomes logically idle and is excluded from both the watermark minimum
and the current alignment set. When it resumes, its retained watermark may pin
future progress, but the global watermark never moves backward. Records below
the already emitted frontier enter the normal late-data path.

When a channel reaches barrier epoch E, it is blocked from advancing past that
barrier. Once all active channels reach E, `SourceMerge` emits one barrier at
the aligned merged offset and releases the channels. This implements the state
cut needed by the checkpoint layer without adding source threads or network
marker transport.

## TCP snapshot execution

Each network participant owns its application state and polls connected TCP
channels from one event loop. Processes run independently. On the first marker,
it captures local state and propagates markers before further sends, then
records messages on still-unmarked channels while continuing normal processing.
The global store validates the complete local-state/channel-state cut before
publishing a commit manifest. Fresh-process recovery restores local state and
replays the saved channel messages before accepting network input.

`WindowOperator` bridges this protocol to `KeyedProcessor`, including per-input
watermarks and pending late re-fires. The three-process executable exercises
nonempty channel snapshots and SIGKILL recovery. See
[chandy-lamport.md](chandy-lamport.md) for the operating contract and limits.

## Window state

Tumbling windows assign one half-open interval to each record. Sliding windows
may assign several overlapping intervals. State is partitioned first by window
and then by key.

Windows are indexed by end time. Watermark advancement visits windows that can
expire instead of scanning every live pane. Fired aggregates remain available
through the allowed-lateness horizon; garbage collection removes them once that
horizon closes.

The aggregate kernel stores a signed 64-bit sum and unsigned count. Overflow is
checked before state mutation.

## Failure and shutdown

The first exception from a source, worker, sink, allocation, or checkpoint path
is captured and rethrown to the caller. Cancellation closes every worker queue,
wakes blocked producers and consumers, and joins all started threads. This also
covers a worker failure while the router is blocked by backpressure.

A custom `Source::Next()` that blocks forever cannot currently be interrupted;
the source interface has no cancellation hook. The separate TCP path uses
nonblocking sockets and bounded poll calls rather than this pull interface.

## Repository map

```text
src/engine       pipelines, workers, routing, queues
src/source       generator, stopping wrapper, multi-source merge
src/window       assigners and keyed window state
src/checkpoint   CRC32C checkpoint reader/writer and common-cut selection
src/snapshot     Chandy–Lamport state machine, TCP transport, global store, window adapter
src/sink         memory, stdout, and durable crash-harness sinks
src/oracle       independent aggregation oracle and differential runners
src/nemesis      fork/SIGKILL recovery scenarios
app              demo, benchmark, oracle, and nemesis executables
test             175 GoogleTest cases; app also registers two TCP process scenarios
bench/flink      matched Apache Flink DataStream comparator
```

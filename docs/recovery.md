# Checkpointing and recovery

stormglass checkpoints operator state at in-band barriers and restores from the
newest complete common partition cut. The tested guarantee is at-least-once
state recovery under deterministic source replay.

This document describes the native aligned-barrier mode. The separate
[Chandy–Lamport TCP mode](chandy-lamport.md) records nonempty in-flight channel
state and uses a global commit manifest; its source and topology contract is
different.

## Barrier alignment

Each input source emits monotonically numbered barriers. `SourceMerge` blocks a
channel after it delivers epoch E and does not pull records beyond that barrier
until every active channel reaches E. It then emits one merged barrier stamped
with the merged record offset at the aligned cut.

Idle channels are excluded from the alignment set using the same logical policy
that excludes them from the watermark minimum. Exhausted channels are removed
permanently. Direct tests assert that early channels remain blocked and that the
Nth merged barrier appears at the exact expected cut.

This is Chandy-Lamport-style aligned checkpointing over deterministic in-process
fan-in. It is not a networked snapshot protocol for arbitrary in-flight channel
messages.

## Partition snapshots

The router broadcasts a merged barrier to all N workers. Each worker serializes:

- format version and checkpoint offset;
- current watermark;
- active keyed panes;
- fired panes retained for allowed lateness;
- pending late-window re-fires.

The writer encodes a CRC32C-protected binary payload, writes a temporary file,
fsyncs it, atomically renames it to its final checkpoint name, and synchronizes
the directory entry. Write and synchronization failures propagate to the
caller.

Checkpoint format v3 contains pending re-fire state. Readers remain compatible
with versions 1 and 2, but those older files cannot reconstruct information that
their formats never stored.

## Common-cut restore

A partitioned restore scans checkpoint offsets and selects the highest offset
for which every requested partition has a present, readable, CRC-valid file.
Workers load that exact offset, restore local state and watermark, and then the
source seeks to the same record offset before processing resumes.

A newer partial partition set is a torn checkpoint and is ignored. A corrupt or
malformed file is rejected. Serialized collection counts are bounded by the
remaining payload size before allocation.

There is no separate transactional global commit manifest. The guarantee is a
validated common-offset selection rule across partition files.

## Source contract

Recovery assumes the source can replay the same ordered records and control
trajectory after `Seek(offset)`. Source identity, ordering, worker count, window
configuration, lateness, and barrier cadence must remain stable across restart.
The job manifest below enforces this. There is no source-offset vector for a
broker and no rescaling protocol.

Checkpointed jobs refuse a source whose `Replayable()` is false. A live
`SourceMerge` reports false, because its interleaving depends on arrival timing;
a durable live merge would need per-input offsets in the checkpoint.

A replayable merge also depends on where each input ends: once an input is
exhausted, round-robin skips it, so extending or shortening one input changes
the merged order after its former end. Replaying such a changed stream to the
checkpoint offset would apply a different mix of records than the checkpoint
holds. Every channel of a checkpointed merge must therefore declare
`Source::Length()` (a record count, or `Source::kUnbounded`). A merge with an
undeclared channel reports `Replayable()` false, and the declared lengths are
part of the merge descriptor, so the job manifest rejects a restore after any
input's length changes.

The deterministic generator implements seek by replaying from its seed, which
is O(offset). `SourceMerge` accepts arbitrary replayable `Source` channels, but
its seek rewinds each channel to 0 and replays the merge, so it is O(offset)
across all channels even when a channel could seek directly. A broker-backed
source would normally seek by a persisted per-partition log offset, but no such
integration is included.

## Job manifest

The first checkpointed run writes `<checkpoint_dir>/job.manifest` atomically
(temporary file, fsync, rename, directory fsync) before any checkpoint. It
records the engine kind, worker count (partitioned only), allowed lateness,
`WindowAssigner::Descriptor()`, and `Source::Descriptor()`. Every later run
compares these fields before restoring and throws `JobManifestMismatch`, naming
each differing field, if any differs. A directory that holds checkpoints but no
manifest is also rejected, because it cannot be verified.

Built-in descriptors cover tumbling and sliding geometry, generator seed, key
space, event-time step, disorder shape, and watermark and barrier cadence.
`SourceMerge` combines its channel descriptors and lengths with idle spans and
the idle timeout. `StoppingSource` reports its inner source. A single source's
length and batch size are excluded because they do not change its replayed
prefix; inside a merge, channel lengths do, so the merge includes them. Custom assigners and
sources describe themselves as empty strings by default, which only match
another empty string; override `Descriptor()` to have them validated.

The separate TCP `WindowOperator` path keeps its own configuration check inside
each snapshot.

## Sink and delivery contract

Operator checkpoints do not make an arbitrary sink transactional, but they do
order output ahead of state. A worker flushes its sink before it writes a
checkpoint. In the default partitioned configuration that flush hands every
buffered result to the caller's sink and calls the caller's `Flush()`. Any
window a checkpoint records as fired has already reached that sink, so a failure
after the checkpoint loses no output. Results emitted after the checkpoint are
produced again during replay.

- `MemorySink` is volatile.
- `DurableFileSink` appends length-framed records and fsyncs each one. By
  default it reopens an existing file in append mode, first cutting a torn
  trailing record left by a killed writer, so a restarted job can use the same
  path. `OpenMode::kTruncate` discards existing contents; the crash nemeses use
  it with distinct pre-crash and post-restore files.
- `DurableFileSink::ReadLatest` returns the last record for each key and window.
  That collapses replay duplicates and keeps the newest late re-fire, which is
  the value an uninterrupted run ends with. `ReadAll` returns the raw log.
- Inputs have no event IDs or deduplication. Delivering the same input twice
  contributes twice.
- Outputs have no transaction coordinated with checkpoint completion. Replay
  can duplicate a previously visible result, so downstream readers must treat
  output as revisions keyed by key and window.

The crash harness therefore asserts no missing results across the union and
reports duplicates. End-to-end exactly-once delivery would still require a
replayable external source plus a transactional sink tied to checkpoint
progress.

## Suspend

`RequestStop(StopMode::kSuspend)` stops a job for a restart or upgrade without
publishing partial windows. After the batch being routed, the router broadcasts
a checkpoint barrier stamped with the source's current offset, so every
partition snapshots the same cut, including open panes and pending re-fires.
Workers then flush output and exit without a final flush. `Stats` reports
`suspended` and `suspend_checkpoint_offset`. A restart restores that checkpoint,
seeks the source there, and emits each open window once with its full value.
Without checkpointing, suspend discards open window state.

## Retention and ownership

Workers never prune independently, because that could delete the last complete
common cut. `PartitionedPipelineConfig::checkpoint_retention` enables
coordinated pruning. Workers report each durable checkpoint to one tracker.
Every partition receives the same barriers in order, so the minimum of the
partitions' latest offsets is the newest complete checkpoint, with no directory
scan. The tracker keeps the newest `checkpoint_retention` complete checkpoints
and deletes partition files below the oldest one it keeps. On restart it starts
from the complete checkpoints already on disk (found by the same scan restore
uses), so the retained history carries across restarts. Newer partial sets
are never touched. The default of 0 retains all history, so disk use and
recovery scans grow over time. The single-threaded `Pipeline` keeps its latest
two checkpoints.

A checkpoint directory is assumed to belong to one job. The job manifest rejects
a restart under a different configuration, but there is no multi-process writer
lock against two concurrent runs sharing a directory.

# Checkpointing and recovery

stormglass checkpoints operator state at in-band barriers and restores from the
newest complete common partition cut. The tested guarantee is at-least-once
state recovery under deterministic source replay.

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
There is no source-offset vector for a broker, persisted job-configuration
fingerprint, or rescaling protocol.

The deterministic generator implements seek by replaying from its seed, which
is O(offset). A broker-backed source would normally seek by a persisted log
offset, but no such integration is included.

## Sink and delivery contract

Operator checkpoints do not make an arbitrary sink transactional.

- `MemorySink` is volatile.
- `DurableFileSink` is a crash-harness sink with framed records. It truncates its
  configured path on construction, so the nemesis uses distinct pre-crash and
  post-restore files and verifies their union.
- Inputs have no event IDs or deduplication. Delivering the same input twice
  contributes twice.
- Outputs have no transaction coordinated with checkpoint completion. Replay
  can duplicate a previously visible result.

The crash harness therefore asserts no missing results across the union and
reports duplicates. End-to-end exactly-once delivery would require a replayable
external source plus a transactional or revision-aware idempotent sink tied to
checkpoint progress.

## Retention and ownership

Partitioned jobs currently retain all checkpoint history so independent worker
pruning cannot delete the last complete common cut. Disk consumption and scan
time therefore grow with history. Coordinated pruning is future work.

A checkpoint directory is assumed to belong to one job. There is no
multi-process writer lock or defense against mixing snapshots from different
job configurations.

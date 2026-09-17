# Event-time semantics

This document defines the behavior tested by the oracle and recovery suite.
Timestamps use millisecond resolution and the supported event-time domain is
nonnegative and representable by the engine's signed duration type.

## Windows

Windows are half-open intervals `[start, end)`.

- A tumbling assigner maps each event to one interval of fixed width.
- A sliding assigner maps an event to every fixed-width interval, aligned to the
  slide, that contains the event timestamp.
- Invalid sizes, slides, negative timestamps, and unrepresentable interval
  arithmetic are rejected.

Each pane contains a sum and count for one `(window, key)` pair. A sliding event
can be accepted for some assigned windows and dropped for others because each
window has its own lateness deadline.

## Watermarks

A stormglass watermark `W` is an exclusive frontier: the engine assumes future
on-time events have timestamps at or after `W`. A window is ready when
`W >= window.end`.

Watermarks are in-band control records and may only advance. Equal or regressing
watermarks have no effect. Under bounded out-of-orderness, generated watermarks
follow:

```text
watermark = maximum observed event time - disorder bound
```

The matched Flink comparator subtracts one millisecond when translating this
frontier because Flink represents watermarks as inclusive millisecond
timestamps and triggers a time window at `end - 1`.

## Allowed lateness

For allowed lateness `L`, a window's final deadline is `end + L`.

- Before the first firing, records update the active pane.
- Once the watermark reaches `end`, the aggregate fires and is retained if
  `L > 0`.
- A record arriving after the first firing but before the final deadline updates
  the retained aggregate and schedules a full replacement result.
- A record arriving when `watermark >= end + L` is dropped for that window.
- Pending replacement aggregates emit on the next advancing watermark or during
  final flush.
- Once the final deadline is reached, retained state is garbage-collected.

Checkpoint format v3 persists both retained fired panes and the pending re-fire
set. A crash after accepting a late record therefore cannot silently suppress
its replacement result after restore.

## Multi-source watermarks

For active source set `A`, `SourceMerge` computes:

```text
merged watermark = min(source watermark[i] for i in A)
```

A source without an initial watermark holds progress back. The merge emits only
strict advances, so it never regresses even when an idle source resumes with a
stale frontier.

Replayable merges base idleness on consecutive empty logical pulls, not
wall-clock time, which keeps the merged trajectory deterministic and replayable.
That policy models idle exclusion over configured idle spans.

Live merges (`live_inputs`) accept channels that return empty batches while
they have no data and can set `idle_timeout_wall`: a channel that produces
nothing for that long is excluded exactly as above and rejoins when it produces
again. Their merge order depends on arrival timing, so they are not replayable
and cannot be checkpointed.

A channel excluded from the watermark minimum is also excluded from barrier
alignment. Otherwise an active source could wait forever for a quiet peer.
When all channels are idle, the last emitted watermark remains unchanged.

## Final flush

End-of-stream causes each processor to emit active panes and pending re-fires.
This makes finite deterministic workloads comparable as complete result sets.
`RequestStop(StopMode::kFinal)` has the same effect on a running job.
`RequestStop(StopMode::kSuspend)` does not: open windows stay unfired (and are
checkpointed when checkpointing is enabled), so a later restart emits them once
with their complete values.
It does not replace watermark-driven lifecycle management for an unbounded
stream.

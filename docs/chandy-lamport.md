# Chandy–Lamport snapshots over TCP

Stormglass implements the Chandy–Lamport marker protocol for a fixed graph of
processes connected by reliable FIFO TCP channels. This is a separate network
execution path using the same `KeyedProcessor` window core as native workers.
It does not rename the existing deterministic `SourceMerge` barrier alignment.

The algorithm follows the marker-sending and marker-receiving rules in
[Chandy and Lamport, Distributed Snapshots: Determining Global States of a
Distributed System (1985)](https://www.microsoft.com/en-us/research/redirect/?ref=https%3A%2F%2Fresearch.microsoft.com%2Fen-us%2Fum%2Fpeople%2Flamport%2Fpubs%2Fchandy.pdf).

## Consistent cut

Each participant has one event-loop owner. A duplex TCP connection supplies two
FIFO channels, with an independent data-message sequence in each direction.
Data includes both records and application watermark messages.

1. An initiator saves its application state and enqueues a marker on every
   outgoing channel before any further application sends.
2. A participant receiving its first marker saves its application state,
   records that incoming channel as empty, and sends its outgoing markers.
3. While a snapshot is active, messages received on an unmarked channel are
   copied into that channel's snapshot and still delivered to the live
   application. Marked channels keep processing too; they are not blocked.
4. A marker closes its incoming channel. The participant completes only after
   every configured incoming channel has delivered that epoch's marker.

The saved channel log retains FIFO sequence numbers and receive order across
channels. Quiet channels cannot be excluded: silence does not establish that
no pre-cut messages are in flight. A missing marker prevents commit. A broken
connection, protocol error, callback failure or exceeded recording limit aborts
the participant; incomplete epochs cannot authorize recovery.

For every directed channel, the global validator requires:

```text
sender sequence at its local cut
    = receiver sequence at its local cut
      + exactly the contiguous saved in-flight messages
```

This rejects missing messages, orphan receives, gaps and duplicates in the cut.
It also checks all participant identities, epochs and the configured topology.

## Window integration and recovery

`WindowOperator` captures keyed panes, fired windows, pending allowed-lateness
re-fires, the operator watermark, the input watermark vector, logical record
count, and window/lateness configuration. It min-combines actual concurrent
inputs. Source participants in the runnable example capture their next record
cursor. On recovery, every participant restores its local state and replays its
saved incoming channel log before accepting fresh network input. Sources resume
from their own saved cursors, using new TCP connections and restored sequences.

This restores a consistent distributed state. It does not promise the same
future cross-channel arrival order as the original execution. Applications
whose late-event outcomes depend on that order need a deterministic replay
schedule to reproduce the exact historical output stream.

`Store` writes immutable CRC32C-protected files under `epoch-E/node-ID.snapshot`.
A coordinator validates every local file and the global cut before publishing
`COMMITTED`. That manifest binds the epoch, topology, caller-supplied job/config
identity and each local payload's CRC. Writes use a temporary file, file fsync,
atomic hard-link publication without overwriting an existing slot, and directory
fsync. Recovery requires the manifest and revalidates every file. A local file
alone is insufficient; `LoadLatestCommitted` skips torn or corrupt newer epochs.

The configuration identity must cover source dataset/version and any application
settings outside `WindowOperator`. Each node/epoch has one writer and each job
has one commit coordinator. CRC detects accidental corruption; it is not an
authentication mechanism against malicious peers or storage modification.

## Transport and operating contract

- `TcpTransport` polls nonblocking sockets and preserves per-peer frame order.
  Markers and application data use the same framed, CRC-protected channel.
- Frames are limited to 1 MiB; queued outbound bytes default to 4 MiB per peer.
  Channel recording defaults to a 64 MiB budget per participant. Exceeding a
  limit fails the operation rather than dropping data or silently closing a
  snapshot. Application capture callbacks must also bound their own state.
- Snapshot files are limited to 64 MiB each, with a 512 MiB global load limit.
  There is no asynchronous spill of channel logs or automatic history pruning.
- The event loop, state callbacks and transport have a single owner thread.
  Independent processes execute concurrently; the API is not safe for arbitrary
  concurrent calls on the same participant. Callbacks must not reenter it.
- Use a fixed symmetric peer topology; the initiator must be able to reach all
  participants through outgoing channels. Only one epoch may be outstanding;
  simultaneous initiation of the same epoch is supported.
- A socket is mapped to a configured peer by its caller. There is no TLS,
  peer-authentication handshake, service discovery, dynamic membership,
  reconnect/resume protocol, Kafka adapter or broker offset integration.
- A job must stop all participants and establish fresh connections before
  restoring a globally validated snapshot. In-place rollback of live peers is
  unsupported. The demonstration runs separate processes over loopback TCP;
  tests do not establish multi-host operational reliability or power-loss safety.

## What is validated

The snapshot suite includes:

- 100 randomized FIFO delivery schedules on three fully connected logical
  participants, including concurrent initiators; an independent token-balance
  invariant verifies the saved local states plus in-flight messages conserve
  the initial total;
- first-marker behavior, continued processing after the cut, marker-before-data
  ordering, quiet channels, multiple epochs, duplicate/gapped messages,
  conflicting epochs, bounded-log failure and sequence-aware channel replay;
- CRC/truncation rejection, immutable slots, changed payloads with recomputed
  CRC rejected by the manifest, configuration checks and fallback after a torn
  or corrupt newer epoch;
- tumbling/sliding state restoration, min-combined watermark restoration,
  pending late re-fires and replay of in-flight records and watermarks;
- real TCP framing, bidirectional markers, bounded output and disconnect errors;
- a three-process TCP run that captures nonempty channel state, commits,
  establishes fresh connections and restores exact final window sums/counts
  against an independent aggregate;
- a confirmed `SIGKILL` of the consumer while another source's marker is still
  missing, rejection of that partial epoch, and successful three-process
  recovery from the previous commit.

The recovered demo's results match exactly because its bounded input and final
watermarks make final aggregates independent of cross-source interleaving.
This does **not** establish a transactional exactly-once output sink. The sink
contract remains separate: output already made visible after a checkpoint can
be replayed after rollback. The original 26 crash-harness scenarios and their
at-least-once qualification remain distinct evidence.

## Suite and platform status

The 177-test CTest suite (175 GoogleTest cases plus two process scenarios)
passed ASan/UBSan and ThreadSanitizer on macOS arm64, including all 24 focused
snapshot tests. CI at `6e3d5d4` ran the suite on Linux x86-64 under ASan/UBSan
and ThreadSanitizer, and on macOS arm64 under ThreadSanitizer. Both
architectures pass, so this code is no longer validated on Clang alone.

The in-flight channel log is load-bearing rather than bookkeeping. Suppressing
replay delivery in `Participant::Restore` while leaving sequence accounting
intact makes the three-process demo fail and exit nonzero: recovery correctness
depends on replaying the recorded channel messages, not only on restoring
operator state.

## Run it

```sh
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release --parallel 4
ctest --test-dir build-release --output-on-failure

# Three TCP processes, nonempty in-flight state, fresh-process recovery.
build-release/app/stormglass_snapshot_demo

# Also kill the consumer mid-marker and recover the previous committed epoch.
build-release/app/stormglass_snapshot_demo --crash

# Retain local files and manifests in a new directory for inspection.
build-release/app/stormglass_snapshot_demo --directory /tmp/stormglass-tcp-example
```

The example orchestrator connects sockets before forking two sources and one
window processor; the three processes then run concurrently. Parent coordination
is used for durable commit and test completion, not to choose input arrival
order or simulate message transport. The underlying numeric-IPv4 connection
helpers also accept non-loopback endpoints, but such deployments are not covered
by the current tests.

The native Flink-comparison throughput numbers do not measure this TCP mode.

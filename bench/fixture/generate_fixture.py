#!/usr/bin/env python3
"""Generate the language-neutral stormglass/Flink comparison fixture."""

from __future__ import annotations

import argparse
import pathlib
import struct

MAGIC = b"SGFXv001"
HEADER = struct.Struct("<8sIIQQqQ")
ENTRY = struct.Struct("<B3xIqqq")
MASK = (1 << 64) - 1


class SplitMix64:
    def __init__(self, seed: int) -> None:
        self.state = seed & MASK

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & MASK
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & MASK
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & MASK
        return (z ^ (z >> 31)) & MASK

    def bounded(self, bound: int) -> int:
        return self.next() % bound


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=pathlib.Path, required=True)
    parser.add_argument("--records", type=int, default=1_000_000)
    parser.add_argument("--keys", type=int, default=1_000)
    parser.add_argument("--watermark-interval", type=int, default=500)
    parser.add_argument("--max-disorder-ms", type=int, default=5_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--profile", choices=("bounded", "heavy"), default="bounded")
    parser.add_argument("--late-percent", type=int, default=5)
    parser.add_argument("--late-tail-ms", type=int, default=4_000)
    args = parser.parse_args()
    if min(args.records, args.keys, args.watermark_interval) <= 0:
        parser.error("records, keys, and watermark interval must be positive")
    if args.max_disorder_ms < 0 or not 0 <= args.late_percent <= 100:
        parser.error("invalid disorder configuration")

    watermark_count = args.records // args.watermark_interval
    entry_count = args.records + watermark_count
    cycle_span_ms = args.records + args.max_disorder_ms + args.late_tail_ms + 1
    args.output.parent.mkdir(parents=True, exist_ok=True)
    rng = SplitMix64(args.seed)
    max_event = 0
    since_watermark = 0

    with args.output.open("wb") as out:
        out.write(HEADER.pack(MAGIC, 1, HEADER.size, args.records,
                              entry_count, cycle_span_ms, args.keys))
        for sequence in range(args.records):
            base_ms = sequence
            heavy = args.profile == "heavy" and rng.bounded(100) < args.late_percent
            if heavy:
                event_ms = base_ms - args.max_disorder_ms - 1 - rng.bounded(
                    max(1, args.late_tail_ms))
            else:
                event_ms = base_ms - rng.bounded(args.max_disorder_ms + 1)
            event_ms = max(0, event_ms)
            value = 1 + rng.bounded(1_000)
            max_event = max(max_event, event_ms)
            out.write(ENTRY.pack(0, sequence % args.keys, value, event_ms, sequence))
            since_watermark += 1
            if since_watermark == args.watermark_interval:
                since_watermark = 0
                watermark = max(0, max_event - args.max_disorder_ms)
                out.write(ENTRY.pack(1, 0, 0, watermark, sequence + 1))

    print(f"fixture={args.output} records={args.records} entries={entry_count} "
          f"bytes={args.output.stat().st_size} profile={args.profile}")


if __name__ == "__main__":
    main()

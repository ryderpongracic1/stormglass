#!/usr/bin/env python3
"""Validate paired stormglass/Flink results and print the comparison table."""

from __future__ import annotations

import argparse
import pathlib
import re
import statistics

FIELD = re.compile(r"([a-z_]+)=([^ ]+)")


def read_result(path: pathlib.Path) -> dict[str, str]:
    lines = [line for line in path.read_text().splitlines() if line.startswith("engine=")]
    if len(lines) != 1:
        raise ValueError(f"{path}: expected exactly one engine result line")
    return dict(FIELD.findall(lines[0]))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("result_dir", type=pathlib.Path)
    args = parser.parse_args()
    stormglass_files = sorted(args.result_dir.glob("stormglass-n*-run*.txt"))
    if not stormglass_files:
        parser.error("no stormglass result files found")

    rates: dict[int, dict[str, list[float]]] = {}
    for stormglass_path in stormglass_files:
        match = re.fullmatch(r"stormglass-n(\d+)-run(\d+)\.txt", stormglass_path.name)
        assert match
        parallelism, run = map(int, match.groups())
        flink_path = args.result_dir / f"flink-n{parallelism}-run{run}.txt"
        if not flink_path.exists():
            raise ValueError(f"missing paired result {flink_path}")
        stormglass = read_result(stormglass_path)
        flink = read_result(flink_path)
        for field in ("records", "outputs", "digest_xor", "digest_sum", "late_dropped"):
            if stormglass.get(field) != flink.get(field):
                raise ValueError(
                    f"N={parallelism} run={run}: {field} mismatch: "
                    f"stormglass={stormglass.get(field)} flink={flink.get(field)}")
        bucket = rates.setdefault(parallelism, {"stormglass": [], "flink": []})
        bucket["stormglass"].append(float(stormglass["m_records_per_second"]))
        bucket["flink"].append(float(flink["m_records_per_second"]))

    print("N  engine       median M rec/s   range M rec/s       stormglass/flink")
    for parallelism, engines in sorted(rates.items()):
        stormglass_median = statistics.median(engines["stormglass"])
        flink_median = statistics.median(engines["flink"])
        ratio = stormglass_median / flink_median
        for engine in ("stormglass", "flink"):
            values = engines[engine]
            suffix = f"{ratio:6.2f}x" if engine == "stormglass" else ""
            print(f"{parallelism:<2} {engine:<12} {statistics.median(values):>8.3f}          "
                  f"{min(values):>6.3f}-{max(values):<6.3f}  {suffix}")
    print("All paired record counts, output counts, late-drop counts, and digests match.")


if __name__ == "__main__":
    main()

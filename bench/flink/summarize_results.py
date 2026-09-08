#!/usr/bin/env python3
"""Fail closed on incomplete/mismatched jobs before comparing their throughput."""
from __future__ import annotations

import argparse
import json
import math
import pathlib
import re
import statistics

REQUIRED = {'engine', 'records', 'expected_records', 'parallelism', 'cycles',
            'window_ms', 'lateness_ms', 'timing', 'seconds', 'm_records_per_second',
            'outputs', 'late_dropped', 'digest_xor', 'digest_sum'}
SIGNATURE = ('records', 'expected_records', 'cycles', 'window_ms', 'lateness_ms',
             'timing', 'outputs', 'late_dropped', 'digest_xor', 'digest_sum')


def read_result(path: pathlib.Path, engine: str, parallelism: int) -> dict[str, str]:
    lines = [line for line in path.read_text().splitlines() if line.startswith('engine=')]
    if len(lines) != 1:
        raise ValueError(f'{path}: expected exactly one engine result line')
    result: dict[str, str] = {}
    for token in lines[0].split():
        if token.count('=') != 1:
            raise ValueError(f'{path}: malformed field {token!r}')
        key, value = token.split('=', 1)
        if key in result or not value:
            raise ValueError(f'{path}: duplicate or empty field {key!r}')
        result[key] = value
    if missing := REQUIRED - result.keys():
        raise ValueError(f'{path}: missing required fields {sorted(missing)}')
    if result['engine'] != engine or result['parallelism'] != str(parallelism):
        raise ValueError(f'{path}: engine/parallelism does not match filename')
    if engine == 'flink' and result.get('version') != '2.3.0':
        raise ValueError(f'{path}: expected Flink version 2.3.0')
    for field in ('records', 'expected_records', 'cycles', 'window_ms', 'outputs', 'late_dropped'):
        if not re.fullmatch(r'\d+', result[field]):
            raise ValueError(f'{path}: invalid {field}')
    if min(int(result[f]) for f in ('records', 'expected_records', 'cycles', 'window_ms')) <= 0:
        raise ValueError(f'{path}: records and workload sizes must be positive')
    if result['records'] != result['expected_records']:
        raise ValueError(f'{path}: consumed records differ from expected_records')
    if int(result['late_dropped']) > int(result['records']):
        raise ValueError(f'{path}: impossible late-drop count')
    if result['lateness_ms'] != '0' or result['window_ms'] != '1000' or result['timing'] != 'execute':
        raise ValueError(f'{path}: unsupported semantics or timing interval')
    for field in ('digest_xor', 'digest_sum'):
        if not re.fullmatch(r'[0-9a-f]{16}', result[field]):
            raise ValueError(f'{path}: invalid {field}')
    seconds, rate = (float(result[f]) for f in ('seconds', 'm_records_per_second'))
    if not all(math.isfinite(v) and v > 0 for v in (seconds, rate)):
        raise ValueError(f'{path}: nonpositive or nonfinite timing/rate')
    calculated = int(result['records']) / seconds / 1e6
    # Output seconds are rounded to 1us, throughput to 0.001M rec/s.
    tolerance = 0.000501 + calculated * 0.00000051 / seconds
    if abs(rate - calculated) > tolerance:
        raise ValueError(f'{path}: throughput disagrees with records / seconds')
    return result


def summarize(directory: pathlib.Path) -> str:
    files: dict[tuple[str, int, int], pathlib.Path] = {}
    for path in directory.glob('*-n*-run*.txt'):
        match = re.fullmatch(r'(stormglass|flink)-n([1-9]\d*)-run([1-9]\d*)\.txt', path.name)
        if not match:
            raise ValueError(f'unrecognized result filename: {path}')
        engine, n, run = match.groups()
        files[engine, int(n), int(run)] = path
    if not files:
        raise ValueError('no result files found')
    ns = sorted({n for _, n, _ in files})
    reps = max(run for _, _, run in files)
    manifest_path = directory / 'manifest.json'
    manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else None
    if manifest is not None:
        ns, reps = manifest['parallelisms'], manifest['reps']
        if not ns or len(set(ns)) != len(ns) or any(n <= 0 for n in ns) or reps <= 0:
            raise ValueError('invalid manifest run matrix')
    expected = {(engine, n, run) for engine in ('stormglass', 'flink')
                for n in ns for run in range(1, reps + 1)}
    if set(files) != expected:
        raise ValueError(f'incomplete run matrix: missing={sorted(expected - files.keys())}; '
                         f'unexpected={sorted(files.keys() - expected)}')
    rates: dict[int, dict[str, list[float]]] = {}
    reference = None
    for (engine, n, run), path in sorted(files.items()):
        result = read_result(path, engine, n)
        signature = tuple(result[field] for field in SIGNATURE)
        if reference is None:
            reference = signature
        if signature != reference:
            raise ValueError(f'{path}: workload, count, late drops, or digests differ across jobs')
        if manifest is not None:
            for field in ('expected_records', 'cycles', 'window_ms', 'lateness_ms', 'timing'):
                if result[field] != str(manifest[field]):
                    raise ValueError(f'{path}: {field} differs from run manifest')
        # Derive rates from count and duration instead of trusting a reported ratio.
        rates.setdefault(n, {'stormglass': [], 'flink': []})[engine].append(
            int(result['records']) / float(result['seconds']) / 1e6)
    lines = ['N  engine       median M rec/s   range M rec/s       stormglass/flink']
    for n, engines in sorted(rates.items()):
        ratio = statistics.median(engines['stormglass']) / statistics.median(engines['flink'])
        for engine in ('stormglass', 'flink'):
            values = engines[engine]
            suffix = f'{ratio:6.2f}x' if engine == 'stormglass' else ''
            lines.append(f'{n:<2} {engine:<12} {statistics.median(values):>8.3f}          '
                         f'{min(values):>6.3f}-{max(values):<6.3f}  {suffix}')
    lines.append(f'Validated {len(files)} jobs: complete matrix; actual/expected records, '
                 'workload, output counts, late drops, and dual digests match across all jobs.')
    lines.append('Digest agreement is a probabilistic multiset check, not proof of identical output.')
    if manifest is None:
        lines.append('No manifest: matrix inferred from files; an entirely absent final repetition cannot be detected.')
    return '\n'.join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('result_dir', type=pathlib.Path)
    args = parser.parse_args()
    try:
        print(summarize(args.result_dir))
    except (ValueError, KeyError, OSError) as error:
        parser.exit(1, f'comparison rejected: {error}\n')


if __name__ == '__main__':
    main()

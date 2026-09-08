#!/usr/bin/env python3
"""Regression checks for the actual comparison executables (build both first)."""
import argparse
import pathlib
import struct
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--stormglass', type=pathlib.Path, required=True)
    parser.add_argument('--java-classpath', required=True)
    args = parser.parse_args()
    engines = [
        [str(args.stormglass.resolve())],
        ['java', '-cp', args.java_classpath, 'io.stormglass.bench.FlinkComparison'],
    ]
    with tempfile.TemporaryDirectory() as directory:
        fixture = pathlib.Path(directory) / 'test.sgfx'
        # One actual data record, a closing frontier, and a known header count.
        entries = struct.pack('<B3xIqqq', 0, 0, 7, 0, 0) + struct.pack('<B3xIqqq', 1, 0, 0, 1000, 1)
        def write(records=1):
            fixture.write_bytes(struct.pack('<8sIIQQqQ', b'SGFXv001', 1, 48, records, 2, 2000, 1) + entries)
        def run(command, extra, success):
            result = subprocess.run(command + ['--fixture', str(fixture), '--cycles', '1'] + extra,
                                    capture_output=True, text=True, timeout=60)
            if (result.returncode == 0) != success:
                raise AssertionError(f'{command[0]} {extra}: exit={result.returncode}\n{result.stdout}\n{result.stderr}')
            if success:
                assert 'records=1 expected_records=1' in result.stdout
                assert 'outputs=1' in result.stdout
                assert 'timing=execute' in result.stdout
        for engine in engines:
            write()
            run(engine, [], True)
            run(engine, ['--lateness-ms', '1'], False)
            run(engine, ['--parallelism', '4294967297'], False)
            write(records=2)
            run(engine, [], False)
        write()
        run(engines[0], ['--cycles', '9223372036854775807'], False)
    print('PASS: both engines validate actual records and reject unmatched lateness/invalid parallelism; C++ rejects cycle overflow')


if __name__ == '__main__':
    main()

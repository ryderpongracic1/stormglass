"""Regression tests for benchmark evidence validation (stdlib only)."""
import json
from pathlib import Path
import tempfile
import unittest

from summarize_results import summarize


class ComparisonValidationTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for engine in ('stormglass', 'flink'):
            self.write(engine)

    def write(self, engine, n=1, run=1, **overrides):
        fields = dict(engine=engine, records='1000000', expected_records='1000000',
                      parallelism=str(n), cycles='1', window_ms='1000', lateness_ms='0',
                      timing='execute', seconds='1.000000', m_records_per_second='1.000',
                      outputs='100', late_dropped='0', digest_xor='0123456789abcdef',
                      digest_sum='fedcba9876543210')
        if engine == 'flink':
            fields['version'] = '2.3.0'
        fields.update(overrides)
        path = self.root / f'{engine}-n{n}-run{run}.txt'
        path.write_text(' '.join(f'{k}={v}' for k, v in fields.items() if v is not None) + '\n')
        return path

    def test_valid_pair(self):
        self.assertIn('Validated 2 jobs', summarize(self.root))

    def test_missing_in_both_is_rejected(self):
        for engine in ('stormglass', 'flink'):
            self.write(engine, outputs=None)
        with self.assertRaisesRegex(ValueError, 'missing required'):
            summarize(self.root)

    def test_header_count_is_not_actual_count(self):
        self.write('flink', expected_records='2000000')
        with self.assertRaisesRegex(ValueError, 'expected_records'):
            summarize(self.root)

    def test_nonfinite_or_inconsistent_rates(self):
        for rate in ('nan', 'inf', '0', '-1', '42'):
            with self.subTest(rate=rate):
                self.write('flink', m_records_per_second=rate)
                with self.assertRaises(ValueError):
                    summarize(self.root)

    def test_mislabeled_engine_and_parallelism(self):
        for changes in ({'engine': 'stormglass'}, {'parallelism': '8'}):
            self.write('flink')
            path = self.root / 'flink-n1-run1.txt'
            text = path.read_text()
            for field, value in changes.items():
                old = 'flink' if field == 'engine' else '1'
                text = text.replace(f'{field}={old}', f'{field}={value}')
            path.write_text(text)
            with self.assertRaisesRegex(ValueError, 'filename'):
                summarize(self.root)

    def test_flink_version_required(self):
        self.write('flink', version='2.2.0')
        with self.assertRaisesRegex(ValueError, 'Flink version'):
            summarize(self.root)

    def test_duplicate_field(self):
        path = self.root / 'flink-n1-run1.txt'
        path.write_text(path.read_text().rstrip() + ' outputs=100\n')
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            summarize(self.root)

    def test_unpaired_flink_job(self):
        self.write('flink', n=2)
        with self.assertRaisesRegex(ValueError, 'incomplete run matrix'):
            summarize(self.root)

    def test_missing_middle_repetition(self):
        for engine in ('stormglass', 'flink'):
            self.write(engine, run=3)
        with self.assertRaisesRegex(ValueError, 'incomplete run matrix'):
            summarize(self.root)

    def test_manifest_detects_entirely_missing_last_run(self):
        (self.root / 'manifest.json').write_text(json.dumps(dict(parallelisms=[1], reps=2)))
        with self.assertRaisesRegex(ValueError, 'incomplete run matrix'):
            summarize(self.root)

    def test_cross_n_agreement_required(self):
        for engine in ('stormglass', 'flink'):
            self.write(engine, n=2, outputs='101')
        with self.assertRaisesRegex(ValueError, 'differ across jobs'):
            summarize(self.root)

    def test_lateness_semantics_rejected(self):
        for engine in ('stormglass', 'flink'):
            self.write(engine, lateness_ms='1000')
        with self.assertRaisesRegex(ValueError, 'unsupported semantics'):
            summarize(self.root)

    def test_timing_boundary_must_match(self):
        self.write('flink', timing='net_runtime')
        with self.assertRaisesRegex(ValueError, 'unsupported semantics'):
            summarize(self.root)

    def test_manifest_workload_required(self):
        (self.root / 'manifest.json').write_text(json.dumps(dict(
            parallelisms=[1], reps=1, expected_records=2000000, cycles=2,
            window_ms=1000, lateness_ms=0, timing='execute')))
        with self.assertRaisesRegex(ValueError, 'differs from run manifest'):
            summarize(self.root)


if __name__ == '__main__':
    unittest.main()

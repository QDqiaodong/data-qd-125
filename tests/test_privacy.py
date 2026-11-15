import json
import math
import os
import random
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest
from pathlib import Path

from visit_attribution import pipeline, privacy

ROOT = Path(__file__).resolve().parents[1]
GAP, LOOKBACK = 1800, 3600


def touch(eid, user, at, channel):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'touch',
            'channel': channel, 'amount_cents': None}


def batch(batch_id, as_of, events, gap=GAP, lookback=LOOKBACK):
    return {'batch_id': batch_id, 'as_of': as_of,
            'session_gap_seconds': gap, 'lookback_seconds': lookback,
            'events': events}


def request(release_id, start='2026-01-01', end='2026-01-04',
            channels=('email', 'search'), limit=2, epsilon='0.5'):
    return {'release_id': release_id, 'start_date': start, 'end_date': end,
            'channels': list(channels), 'L': limit, 'epsilon': epsilon}


def zero_noise():
    return 0.5  # u == 0 -> Laplace draw is exactly 0.0


class CountingRng:
    def __init__(self, value=0.5):
        self.value = value
        self.calls = 0

    def __call__(self):
        self.calls += 1
        return self.value


class PrivacyTests(unittest.TestCase):
    def setUp(self):
        os.environ.pop('VA_PRIVACY_FAULT_AFTER_COMMIT', None)
        privacy._snapshot_hook = None
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'privacy.db'

    def tearDown(self):
        privacy._snapshot_hook = None
        os.environ.pop('VA_PRIVACY_FAULT_AFTER_COMMIT', None)
        self.td.cleanup()

    def apply(self, name, payload):
        source = self.dir / name
        source.write_text(json.dumps(payload))
        return pipeline.apply_batch(self.db, source)

    def write_request(self, payload, name='req.json'):
        path = self.dir / name
        path.write_text(json.dumps(payload))
        return path

    def release(self, req_path, out_name='out.json', rng=zero_noise, **kw):
        return privacy.release(kw.get('source', self.db), kw.get('ledger', self.ledger),
                               req_path, self.dir / out_name, rng=rng)

    def read_output(self, out_name='out.json'):
        return json.loads((self.dir / out_name).read_text())

    def counts_of(self, report):
        return {(c['date'], c['channel']): c['noisy_count'] for c in report['counts']}

    def spent(self):
        conn = sqlite3.connect(self.ledger)
        try:
            row = conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
            return privacy._canonical(int(row.fetchone()[0]))
        finally:
            conn.close()

    # -- ledger initialization ---------------------------------------------

    def test_init_creates_ledger_and_refuses_reinit(self):
        summary = privacy.init_ledger(self.ledger, '10')
        self.assertEqual(summary['budget'], '10')
        self.assertEqual(self.spent(), '0')
        with self.assertRaises(privacy.PrivacyError):
            privacy.init_ledger(self.ledger, '10')
        # budget unchanged after refused reinit
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_budget_scaled'")
                         .fetchone()[0], str(10 * 10 ** 18))
        conn.close()

    def test_init_rejects_bad_budget_and_leaves_no_file(self):
        for bad in ('0', '0.0', '-1', 'abc', '1e3', 'NaN', 'inf', '',
                    '1.1234567890123456789', '1234567890123', 1.5, None):
            with self.assertRaises(ValueError, msg=repr(bad)):
                privacy.init_ledger(self.ledger, bad)
            self.assertFalse(self.ledger.exists(), msg=repr(bad))
        privacy.init_ledger(self.ledger, '0.000000000000000001')

    def test_release_requires_initialized_ledger(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        req = self.write_request(request('R1'))
        with self.assertRaises(privacy.PrivacyError):
            self.release(req)  # ledger file does not exist
        (self.dir / 'empty.db').write_bytes(b'')
        with self.assertRaises(privacy.PrivacyError):
            self.release(req, ledger=self.dir / 'empty.db')

    # -- request validation --------------------------------------------------

    def test_request_validation(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '10')
        good = request('R1')
        bad_requests = [
            {}, dict(good, extra=1),
            dict(good, release_id=' x'), dict(good, release_id=''),
            dict(good, start_date='2026-1-1'), dict(good, start_date='2026-02-30'),
            dict(good, start_date=20260101), dict(good, start_date='2026-01-04', end_date='2026-01-04'),
            dict(good, start_date='2026-01-04', end_date='2026-01-01'),
            dict(good, start_date='2026-01-01', end_date='2027-01-03'),  # 367 days
            dict(good, channels='email'), dict(good, channels=['email', 'email']),
            dict(good, channels=['other']), dict(good, channels=['']),
            dict(good, channels=['email'] * 65),
            dict(good, L=0), dict(good, L=-1), dict(good, L='2'), dict(good, L=2.0),
            dict(good, L=True), dict(good, L=10 ** 6 + 1),
            dict(good, epsilon=0.5), dict(good, epsilon='0'), dict(good, epsilon='0.00'),
            dict(good, epsilon='-0.5'), dict(good, epsilon='abc'), dict(good, epsilon='1e-3'),
            dict(good, epsilon='NaN'), dict(good, epsilon='Infinity'),
            dict(good, epsilon='0.1234567890123456789'),
        ]
        for i, bad in enumerate(bad_requests):
            req = self.write_request(bad, name='bad%d.json' % i)
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.release(req)
        self.assertEqual(self.spent(), '0')  # no bad request charged the ledger
        # exactly 366 days is accepted; empty public set folds everything into other
        req = self.write_request(request('R1', end='2027-01-02', channels=(), epsilon='0.5'))
        summary = self.release(req)
        self.assertEqual(summary['buckets'], 366)

    # -- counting semantics --------------------------------------------------

    def seed_source(self):
        self.apply('b1.json', batch('B1', '2026-01-03T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),    # interval start, included
            touch('E2', 'U1', '2026-01-01T12:00:00Z', 'email'),    # same bucket, still one user
            touch('E3', 'U1', '2026-01-02T00:00:00Z', 'sms'),      # unknown channel -> other
            touch('E4', 'U2', '2026-01-02T00:00:00Z', 'search'),
            touch('E5', 'U3', '2026-01-04T00:00:00Z', 'email'),    # interval end, excluded
        ]))

    def test_counts_zero_fill_other_and_half_open_bounds(self):
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1', limit=10))
        summary = self.release(req)
        self.assertFalse(summary['replayed'])
        report = self.read_output()
        self.assertEqual(set(report), {'schema_version', 'release_id', 'start_date',
                                       'end_date', 'channels', 'L', 'epsilon',
                                       'noise_scale', 'source_as_of', 'counts'})
        self.assertEqual(set(report['counts'][0]), {'date', 'channel', 'noisy_count'})
        self.assertEqual(report['channels'], ['email', 'search', 'other'])
        self.assertEqual(report['source_as_of'], '2026-01-03T00:00:00Z')
        # full domain: 3 days x 3 channels, zero-filled, deterministic order
        self.assertEqual([(c['date'], c['channel']) for c in report['counts']],
                         [(d, ch) for d in ('2026-01-01', '2026-01-02', '2026-01-03')
                          for ch in ('email', 'search', 'other')])
        counts = self.counts_of(report)
        self.assertEqual(counts[('2026-01-01', 'email')], 1.0)   # U1 once despite two touches
        self.assertEqual(counts[('2026-01-02', 'other')], 1.0)   # sms folded into other
        self.assertEqual(counts[('2026-01-02', 'search')], 1.0)  # U2
        self.assertEqual(counts[('2026-01-02', 'email')], 0.0)   # zero bucket still released
        self.assertEqual(counts[('2026-01-03', 'email')], 0.0)
        self.assertNotIn(('2026-01-04', 'email'), counts)        # half-open end excluded
        self.assertEqual(summary['epsilon_spent'], '0.5')

    def test_inactive_future_events_excluded_until_activated(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('F1', 'U2', '2026-01-01T02:00:00Z', 'email')]))  # future, active=0
        privacy.init_ledger(self.ledger, '10')
        req1 = self.write_request(request('R1', start='2026-01-01', end='2026-01-02'))
        self.release(req1)
        counts = self.counts_of(self.read_output())
        self.assertEqual(counts[('2026-01-01', 'email')], 1.0)  # F1 not active yet
        self.apply('b2.json', batch('B2', '2026-01-01T03:00:00Z', []))  # activates F1
        req2 = self.write_request(request('R2', start='2026-01-01', end='2026-01-02'))
        self.release(req2)
        counts = self.counts_of(self.read_output())
        self.assertEqual(counts[('2026-01-01', 'email')], 2.0)

    def test_cross_day_clipping_keeps_first_l_buckets(self):
        self.apply('b1.json', batch('B1', '2026-01-04T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('E2', 'U1', '2026-01-02T00:00:00Z', 'email'),
            touch('E3', 'U1', '2026-01-03T00:00:00Z', 'email'),
            touch('E4', 'U2', '2026-01-03T00:00:00Z', 'email')]))
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1', channels=('email',), limit=2))
        self.release(req)
        counts = self.counts_of(self.read_output())
        # U1 clipped to the first two date-ordered buckets; day 3 keeps only U2
        self.assertEqual(counts[('2026-01-01', 'email')], 1.0)
        self.assertEqual(counts[('2026-01-02', 'email')], 1.0)
        self.assertEqual(counts[('2026-01-03', 'email')], 1.0)

    def test_same_day_clipping_orders_channels_lexicographically(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'search'),   # earlier touch...
            touch('E2', 'U1', '2026-01-01T00:01:00Z', 'email'),    # ...but email sorts first
            touch('E3', 'U1', '2026-01-01T00:02:00Z', 'sms')]))   # maps to other
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1', start='2026-01-01', end='2026-01-02',
                                         channels=('email', 'search'), limit=2))
        self.release(req)
        counts = self.counts_of(self.read_output())
        self.assertEqual(counts[('2026-01-01', 'email')], 1.0)
        self.assertEqual(counts[('2026-01-01', 'other')], 1.0)
        self.assertEqual(counts[('2026-01-01', 'search')], 0.0)  # clipped: 3rd bucket

    # -- noise ---------------------------------------------------------------

    def test_injected_rng_verifies_scale_and_independent_draws(self):
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        seed = 20261115
        req = self.write_request(request('R1', limit=2, epsilon='0.25'))
        self.release(req, rng=random.Random(seed).random)
        report = self.read_output()
        self.assertEqual(report['noise_scale'], 2 / 0.25)
        # replay the exact draw sequence: one independent draw per bucket,
        # iterated in (date, domain) order with scale L/epsilon
        replay = random.Random(seed)
        scale = 2 / 0.25
        true = {('2026-01-01', 'email'): 1, ('2026-01-02', 'other'): 1,
                ('2026-01-02', 'search'): 1}
        for entry in report['counts']:
            u = replay.random() - 0.5
            noise = 0.0 if u == 0 else scale * math.copysign(math.log1p(-2 * abs(u)), u)
            expected = true.get((entry['date'], entry['channel']), 0) + noise
            self.assertEqual(entry['noisy_count'], expected)
        # noise actually moved counts off their true values
        self.assertTrue(any(e['noisy_count'] != 0.0 for e in report['counts']
                            if (e['date'], e['channel']) not in true))

    def test_production_default_rng_is_system_random(self):
        self.assertIs(privacy.release.__defaults__[0], None)
        # no rng argument -> secrets.SystemRandom().random; just exercise it
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1', limit=1))
        self.release(req, rng=None)
        report = self.read_output()
        values = [c['noisy_count'] for c in report['counts']]
        self.assertTrue(all(math.isfinite(v) for v in values))
        self.assertTrue(any(not float(v).is_integer() for v in values))

    # -- ledger accounting ---------------------------------------------------

    def test_exact_decimal_accumulation(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '0.3')
        for i in range(3):  # 0.1+0.1+0.1 == 0.3 exactly; floats would overshoot
            req = self.write_request(request('R%d' % i, epsilon='0.1'))
            self.release(req, out_name='out%d.json' % i)
        self.assertEqual(self.spent(), '0.3')
        req = self.write_request(request('R3', epsilon='0.000000000000000001'))
        with self.assertRaisesRegex(privacy.PrivacyError, 'insufficient privacy budget'):
            self.release(req)
        self.assertEqual(self.spent(), '0.3')
        self.assertFalse((self.dir / 'out.json').exists())

    def test_insufficient_budget_charges_nothing(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '1')
        req = self.write_request(request('R1', epsilon='1.5'))
        with self.assertRaisesRegex(privacy.PrivacyError, 'insufficient privacy budget'):
            self.release(req)
        self.assertEqual(self.spent(), '0')
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 0)
        conn.close()

    def test_replay_reuses_payload_bytes_without_recharge(self):
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        rng = CountingRng()
        req = self.write_request(request('R1'))
        first = self.release(req, rng=rng)
        calls = rng.calls
        self.assertEqual(calls, 3 * 3)  # 3 days x 3 channels
        payload = (self.dir / 'out.json').read_bytes()
        (self.dir / 'out.json').unlink()
        again = self.release(req, rng=rng)  # same id, same spec
        self.assertTrue(again['replayed'])
        self.assertEqual(rng.calls, calls)  # no resampling on replay
        self.assertEqual((self.dir / 'out.json').read_bytes(), payload)
        self.assertEqual(self.spent(), '0.5')
        # semantically identical request (key order, epsilon '0.50', channel order)
        # also replays
        twin = self.write_request({'epsilon': '0.50', 'L': 2, 'channels': ['search', 'email'],
                                   'end_date': '2026-01-04', 'start_date': '2026-01-01',
                                   'release_id': 'R1'}, name='twin.json')
        third = self.release(twin, rng=rng)
        self.assertTrue(third['replayed'])
        self.assertEqual((self.dir / 'out.json').read_bytes(), payload)
        self.assertEqual(self.spent(), '0.5')
        self.assertEqual(first['epsilon_spent'], again['epsilon_spent'])

    def test_same_id_different_parameters_rejected(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '10')
        self.release(self.write_request(request('R1')))
        for altered in (dict(request('R1'), epsilon='0.6'),
                        dict(request('R1'), L=3),
                        dict(request('R1'), end_date='2026-01-05'),
                        dict(request('R1'), channels=['email'])):
            req = self.write_request(altered, name='alt.json')
            with self.assertRaisesRegex(privacy.PrivacyError, 'different parameters'):
                self.release(req)
        self.assertEqual(self.spent(), '0.5')

    def test_concurrent_releases_never_overspend(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '1')
        requests = [self.write_request(request('RA', epsilon='0.6'), name='ra.json'),
                    self.write_request(request('RB', epsilon='0.6'), name='rb.json')]
        outcomes = {}

        def worker(name, req):
            try:
                outcomes[name] = self.release(req, out_name=name + '.json')
            except Exception as error:  # noqa: BLE001 - collected for assertions
                outcomes[name] = error

        threads = [threading.Thread(target=worker, args=('RA', requests[0])),
                   threading.Thread(target=worker, args=('RB', requests[1]))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        winners = [n for n, r in outcomes.items() if not isinstance(r, Exception)]
        losers = [r for r in outcomes.values() if isinstance(r, Exception)]
        self.assertEqual(len(winners), 1)
        self.assertEqual(len(losers), 1)
        self.assertIsInstance(losers[0], privacy.PrivacyError)
        self.assertIn('insufficient privacy budget', str(losers[0]))
        self.assertEqual(self.spent(), '0.6')  # exactly one charge, never overspent
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 1)
        conn.close()

    def test_concurrent_same_id_charges_once(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1'))
        outcomes = {}

        def worker(name):
            outcomes[name] = self.release(req, out_name=name + '.json')

        threads = [threading.Thread(target=worker, args=('w1',)),
                   threading.Thread(target=worker, args=('w2',))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(self.spent(), '0.5')
        self.assertEqual(sum(1 for r in outcomes.values() if r['replayed']), 1)
        self.assertEqual((self.dir / 'w1.json').read_bytes(),
                         (self.dir / 'w2.json').read_bytes())

    # -- commit / output failure ---------------------------------------------

    def test_post_commit_output_failure_keeps_old_file_and_retry_is_free(self):
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1'))
        out = self.dir / 'out.json'
        out.write_text('old')
        rng = CountingRng()
        os.environ['VA_PRIVACY_FAULT_AFTER_COMMIT'] = '1'
        with self.assertRaises(OSError):
            self.release(req, rng=rng)
        os.environ.pop('VA_PRIVACY_FAULT_AFTER_COMMIT')
        self.assertEqual(out.read_text(), 'old')   # old file preserved
        self.assertEqual(self.spent(), '0.5')      # ledger committed before the failure
        calls = rng.calls
        summary = self.release(req, rng=rng)       # retry after the failed write
        self.assertTrue(summary['replayed'])
        self.assertEqual(rng.calls, calls)         # no resampling
        self.assertEqual(self.spent(), '0.5')      # no second charge
        report = self.read_output()
        self.assertEqual(report['release_id'], 'R1')

    def test_unwritable_output_target_preserves_destination(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1'))
        dest_dir = self.dir / 'dest-dir'
        dest_dir.mkdir()
        with self.assertRaises(OSError):
            privacy.release(self.db, self.ledger, req, dest_dir, rng=zero_noise)
        self.assertTrue(dest_dir.is_dir())         # destination untouched
        self.assertEqual(self.spent(), '0.5')      # committed; retry elsewhere is a replay
        summary = self.release(req)
        self.assertTrue(summary['replayed'])

    # -- source snapshot -----------------------------------------------------

    def test_source_write_interleaving_keeps_single_snapshot(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')]))
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1', start='2026-01-01', end='2026-01-02',
                                         channels=('email',), limit=5))
        began = threading.Event()
        proceed = threading.Event()
        errors = []

        def concurrent_write():
            try:
                writer = sqlite3.connect(str(self.db), isolation_level=None)
                writer.execute('PRAGMA busy_timeout=5000')
                writer.execute('BEGIN IMMEDIATE')
                writer.execute("INSERT INTO events(id,user,at,kind,channel,amount_cents,"
                               "active,first_batch_id) VALUES('E9','U9',"
                               "'2026-01-01T00:05:00Z','touch','email',NULL,1,'BX')")
                began.set()          # uncommitted insert now in flight
                proceed.wait(5)
                writer.execute('COMMIT')
                writer.close()
            except Exception as error:  # noqa: BLE001 - collected for assertions
                errors.append(error)
                began.set()

        thread = threading.Thread(target=concurrent_write)
        thread.start()

        def hook():
            self.assertTrue(began.wait(5))  # writer is mid-transaction during our read

        privacy._snapshot_hook = hook
        try:
            summary = self.release(req)
        finally:
            privacy._snapshot_hook = None
            proceed.set()
            thread.join(10)
        self.assertEqual(errors, [])
        self.assertEqual(summary['source_as_of'], '2026-01-02T00:00:00Z')
        counts = self.counts_of(self.read_output())
        # the interleaved uncommitted insert never leaked into the snapshot
        self.assertEqual(counts[('2026-01-01', 'email')], 1.0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM events WHERE id='E9'")
                         .fetchone()[0], 1)  # the writer did commit afterwards
        conn.close()

    def test_release_does_not_modify_source_database(self):
        self.seed_source()
        privacy.init_ledger(self.ledger, '10')
        before = self.db.read_bytes()
        req = self.write_request(request('R1'))
        self.release(req)
        self.assertEqual(self.db.read_bytes(), before)
        conn = sqlite3.connect(self.db)
        names = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        conn.close()
        self.assertEqual(names, {'meta', 'batches', 'events', 'm_sessions',
                                 'm_session_events', 'm_conversions',
                                 'tombstones', 'erasures'})

    def test_release_requires_populated_source(self):
        privacy.init_ledger(self.ledger, '10')
        req = self.write_request(request('R1'))
        with self.assertRaisesRegex(privacy.PrivacyError, 'source database does not exist'):
            self.release(req)
        (self.dir / 'fresh.db').write_bytes(b'')
        with self.assertRaisesRegex(privacy.PrivacyError, 'no applied batches'):
            self.release(req, source=self.dir / 'fresh.db')
        self.assertEqual(self.spent(), '0')

    # -- CLI -------------------------------------------------------------------

    def test_cli_privacy_init_release_and_error_paths(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')]))
        ledger = self.dir / 'cli-ledger.db'

        def run_cli(*args):
            return subprocess.run([sys.executable, '-m', 'visit_attribution.cli', *args],
                                  capture_output=True, text=True, env=env)

        r = run_cli('privacy-init', '--ledger', str(ledger), '--budget', '2')
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)['budget'], '2')
        r = run_cli('privacy-init', '--ledger', str(ledger), '--budget', '2')
        self.assertEqual(r.returncode, 2)  # already initialized
        r = run_cli('privacy-init', '--ledger', str(self.dir / 'x.db'), '--budget', 'nope')
        self.assertEqual(r.returncode, 2)

        req = self.write_request(request('R1', epsilon='1'))
        out = self.dir / 'cli-out.json'
        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(req), '--output', str(out))
        self.assertEqual(r.returncode, 0, r.stderr)
        summary = json.loads(r.stdout)
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['epsilon_spent'], '1')
        report = json.loads(out.read_text())
        self.assertEqual(report['release_id'], 'R1')
        self.assertEqual(report['source_as_of'], '2026-01-02T00:00:00Z')
        first_bytes = out.read_bytes()

        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(req), '--output', str(out))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertTrue(json.loads(r.stdout)['replayed'])
        self.assertEqual(out.read_bytes(), first_bytes)

        altered = self.write_request(request('R1', epsilon='0.5'), name='altered.json')
        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(altered), '--output', str(out))
        self.assertEqual(r.returncode, 2)
        self.assertIn('different parameters', r.stderr)

        big = self.write_request(request('R2', epsilon='1.5'), name='big-req.json')
        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(big), '--output', str(self.dir / 'big-out.json'))
        self.assertEqual(r.returncode, 2)
        self.assertIn('insufficient privacy budget', r.stderr)

        fits = self.write_request(request('R2', epsilon='1'), name='fits-req.json')
        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(fits), '--output', str(self.dir / 'fits-out.json'))
        self.assertEqual(r.returncode, 0, r.stderr)
        self.assertEqual(json.loads(r.stdout)['epsilon_spent'], '2')

        # output must not clobber the ledger/source/request
        r = run_cli('privacy-release', '--source', str(self.db), '--ledger', str(ledger),
                    '--request', str(fits), '--output', str(ledger))
        self.assertEqual(r.returncode, 2)
        # legacy and incremental commands still dispatch
        r = run_cli('export', '--db', str(self.db), '--output', str(self.dir / 'exp.json'))
        self.assertEqual(r.returncode, 0, r.stderr)
        r = run_cli('--source', str(ROOT / 'fixtures/input.json'),
                    '--output', str(self.dir / 'legacy.json'))
        self.assertEqual(r.returncode, 0, r.stderr)


if __name__ == '__main__':
    unittest.main()

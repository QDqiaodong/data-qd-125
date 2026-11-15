import hashlib
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

from visit_attribution import erase, pipeline, privacy

ROOT = Path(__file__).resolve().parents[1]
GAP, LOOKBACK = 1800, 3600


def touch(eid, user, at, channel='email'):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'touch',
            'channel': channel, 'amount_cents': None}


def purchase(eid, user, at, amount=100):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'purchase',
            'channel': None, 'amount_cents': amount}


def batch(batch_id, as_of, events, gap=GAP, lookback=LOOKBACK):
    return {'batch_id': batch_id, 'as_of': as_of,
            'session_gap_seconds': gap, 'lookback_seconds': lookback,
            'events': events}


def release_request(release_id, start='2026-01-01', end='2026-01-04',
                    channels=('email',), limit=10, epsilon='0.5'):
    return {'release_id': release_id, 'start_date': start, 'end_date': end,
            'channels': list(channels), 'L': limit, 'epsilon': epsilon}


def zero_noise():
    return 0.5


def sha(user):
    return hashlib.sha256(user.encode('utf-8')).hexdigest()


class EraseCoreTests(unittest.TestCase):
    def setUp(self):
        for var in ('VA_ERASE_FAULT_AFTER_COMMIT', 'VA_PRIVACY_FAULT_AFTER_COMMIT'):
            os.environ.pop(var, None)
        privacy._snapshot_hook = None
        privacy._commit_barrier = None
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'privacy.db'

    def tearDown(self):
        privacy._snapshot_hook = None
        privacy._commit_barrier = None
        for var in ('VA_ERASE_FAULT_AFTER_COMMIT', 'VA_PRIVACY_FAULT_AFTER_COMMIT'):
            os.environ.pop(var, None)
        self.td.cleanup()

    def apply(self, name, payload):
        source = self.dir / name
        source.write_text(json.dumps(payload))
        return pipeline.apply_batch(self.db, source)

    def write_erase(self, request_id, users, name='erase.json'):
        path = self.dir / name
        path.write_text(json.dumps({'request_id': request_id, 'users': users}))
        return path

    def erase(self, request_id, users, name='erase.json'):
        return erase.erase(self.db, self.write_erase(request_id, users, name))

    def table_count(self, table, where='', params=()):
        conn = sqlite3.connect(self.db)
        try:
            return conn.execute('SELECT COUNT(*) FROM %s %s' % (table, where),
                                params).fetchone()[0]
        finally:
            conn.close()

    def result(self):
        return pipeline.build_result(pipeline.connect(self.db))

    # -- request validation --------------------------------------------------

    def test_request_validation(self):
        self.apply('b1.json', batch('B1', '2026-01-02T00:00:00Z', []))
        bad_requests = [
            {}, {'users': ['U1']}, {'request_id': 'E1'},
            {'request_id': 'E1', 'users': []},
            {'request_id': 'E1', 'users': 'x'},  # users must be an array
            {'request_id': 'E1', 'users': ['U1'], 'extra': 1},
            {'request_id': ' E1', 'users': ['U1']},
            {'request_id': '', 'users': ['U1']},
            {'request_id': 'E1', 'users': ['U1', 1]},
            {'request_id': 'E1', 'users': ['U' * 129]},
            {'request_id': 'E1', 'users': [' bad ']},
            {'request_id': 'E1', 'users': ['U1'] * 10001},
        ]
        for i, bad in enumerate(bad_requests):
            path = self.dir / ('bad%d.json' % i)
            path.write_text(json.dumps(bad))
            with self.assertRaises((ValueError, TypeError), msg=repr(bad)):
                erase.erase(self.db, path)
        self.assertEqual(self.table_count('tombstones'), 0)
        self.assertEqual(self.table_count('erasures'), 0)

    def test_erase_on_uninitialized_store_creates_schema_and_tombstone(self):
        # Unknown users are banned even when no batch was ever applied.
        fresh = self.dir / 'fresh.db'
        summary = erase.erase(fresh, self.write_erase('E1', ['ghost'], 'g.json'))
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['erased_users'], 0)
        self.assertEqual(summary['unknown_users'], 1)
        self.assertEqual(summary['tombstones_written'], 1)
        conn = sqlite3.connect(fresh)
        self.assertEqual(conn.execute('SELECT user_sha256,request_id FROM tombstones')
                         .fetchall(), [(sha('ghost'), 'E1')])
        conn.close()

    # -- erasure semantics ----------------------------------------------------

    def seed(self):
        self.apply('b1.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('E2', 'U1', '2026-01-01T00:30:00Z', 'search'),
            purchase('P1', 'U1', '2026-01-01T00:35:00Z', 700),
            touch('F1', 'U1', '2026-01-10T00:00:00Z', 'email'),   # future row
            touch('E3', 'U2', '2026-01-02T00:00:00Z', 'email'),
            purchase('P2', 'U2', '2026-01-02T00:10:00Z', 300),
            touch('E4', 'U3', '2026-01-03T00:00:00Z', 'sms'),
        ]))

    def test_erases_events_including_future_sessions_members_conversions(self):
        self.seed()
        before = self.result()
        self.assertEqual(before['future_event_ids'], ['F1'])
        self.assertEqual(self.table_count('events'), 7)
        self.assertEqual(self.table_count('m_sessions'), 3)
        self.assertEqual(self.table_count('m_conversions'), 2)
        summary = self.erase('E1', ['U1'])
        self.assertEqual(summary['erased_users'], 1)
        self.assertEqual(summary['unknown_users'], 0)
        self.assertEqual(summary['tombstones_written'], 1)
        # all U1 rows gone, including the inactive future event
        self.assertEqual(self.table_count('events'), 3)
        self.assertEqual(self.table_count('events', "WHERE user='U1'"), 0)
        self.assertEqual(self.table_count('m_sessions'), 2)
        self.assertEqual(self.table_count('m_session_events', "WHERE user='U1'"), 0)
        self.assertEqual(self.table_count('m_conversions'), 1)
        result = self.result()
        self.assertEqual(result['future_event_ids'], [])
        self.assertEqual(result['conversion_count'], 1)
        self.assertEqual(result['total_revenue_cents'], 300)
        users = {row[0] for row in sqlite3.connect(self.db).execute(
            'SELECT DISTINCT user FROM events')}
        self.assertEqual(users, {'U2', 'U3'})
        # tombstone stores only the hash; the identifier never appears
        conn = sqlite3.connect(self.db)
        rows = conn.execute('SELECT user_sha256,request_id FROM tombstones').fetchall()
        self.assertEqual(rows, [(sha('U1'), 'E1')])
        raw = Path(self.db).read_bytes()
        self.assertNotIn(b'U1', raw)
        self.assertIn(sha('U1').encode(), raw)
        # batch credentials and watermark survive
        self.assertEqual(conn.execute('SELECT batch_id FROM batches').fetchall(), [('B1',)])
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='as_of'")
                         .fetchone()[0], '2026-01-05T00:00:00Z')
        conn.close()

    def test_unknown_user_is_tombstoned_and_banned(self):
        self.seed()
        summary = self.erase('E1', ['nobody'])
        self.assertEqual(summary['erased_users'], 0)
        self.assertEqual(summary['unknown_users'], 1)
        self.assertEqual(self.table_count('tombstones'), 1)
        # an unknown tombstone blocks ingestion just like a known one
        payload = batch('B2', '2026-01-06T00:00:00Z',
                        [touch('E9', 'nobody', '2026-01-06T00:00:00Z')])
        source = self.dir / 'b2.json'
        source.write_text(json.dumps(payload))
        with self.assertRaisesRegex(pipeline.PipelineError, 'erased user'):
            pipeline.apply_batch(self.db, source)
        self.assertEqual(self.table_count('events'), 7)

    def test_mixed_known_and_unknown_users_in_one_request(self):
        self.seed()
        summary = self.erase('E1', ['U2', 'ghost', 'U1'])
        self.assertEqual(summary['erased_users'], 2)
        self.assertEqual(summary['unknown_users'], 1)
        self.assertEqual(self.table_count('tombstones'), 3)
        self.assertEqual(self.table_count('events'), 1)  # only U3 remains
        self.assertEqual(
            [r[0] for r in sqlite3.connect(self.db).execute(
                'SELECT user_sha256 FROM tombstones ORDER BY user_sha256')],
            sorted(sha(u) for u in ('U1', 'U2', 'ghost')))

    # -- idempotency -----------------------------------------------------------

    def test_same_request_replays_without_side_effects(self):
        self.seed()
        req_path = self.write_erase('E1', ['U1', 'U2'])
        first = erase.erase(self.db, req_path)
        self.assertFalse(first['replayed'])
        conn = sqlite3.connect(self.db)
        state = {table: conn.execute('SELECT * FROM %s ORDER BY 1' % table).fetchall()
                 for table in ('tombstones', 'erasures', 'meta', 'batches',
                               'events', 'm_sessions', 'm_conversions')}
        gen = conn.execute("SELECT value FROM meta WHERE key='write_generation'").fetchone()[0]
        conn.close()
        second = erase.erase(self.db, req_path)
        self.assertTrue(second['replayed'])
        self.assertEqual(second['erased_users'], 0)
        self.assertEqual(second['tombstones_written'], 0)
        self.assertEqual(second['generation'], int(gen))
        conn = sqlite3.connect(self.db)
        for table, rows in state.items():
            self.assertEqual(conn.execute('SELECT * FROM %s ORDER BY 1' % table).fetchall(),
                             rows, table)
        conn.close()
        # semantically identical request (reordered users, same id) replays too
        twin = self.write_erase('E1', ['U2', 'U1'], name='twin.json')
        third = erase.erase(self.db, twin)
        self.assertTrue(third['replayed'])

    def test_same_id_different_parameters_refused_entirely(self):
        self.seed()
        self.erase('E1', ['U1'])
        for users in (['U2'], ['U1', 'U2']):
            path = self.write_erase('E1', users, name='alt-%s.json' % users[0])
            with self.assertRaisesRegex(erase.PipelineError, 'different parameters'):
                erase.erase(self.db, path)
        # U2 untouched, no extra tombstones
        self.assertEqual(self.table_count('events', "WHERE user='U2'"), 2)
        self.assertEqual(self.table_count('tombstones'), 1)

    def test_user_already_erased_by_other_request_rejects_whole_batch(self):
        self.seed()
        self.erase('E1', ['U1'])
        with self.assertRaisesRegex(erase.PipelineError, 'already erased'):
            self.erase('E2', ['U1', 'U2'])
        # E2 wrote nothing: U2 data intact, single tombstone/credential remain
        self.assertEqual(self.table_count('tombstones'), 1)
        self.assertEqual(self.table_count('erasures'), 1)
        self.assertEqual(self.table_count('events', "WHERE user='U2'"), 2)

    # -- re-ingestion ban ------------------------------------------------------

    def test_apply_rejects_batches_referencing_tombstoned_users(self):
        self.seed()
        self.erase('E1', ['U1'])
        blocked = batch('B2', '2026-01-06T00:00:00Z', [
            touch('E3', 'U2', '2026-01-06T00:00:00Z'),       # fine user
            touch('E9', 'U1', '2026-01-06T00:01:00Z')])      # erased user
        source = self.dir / 'b2.json'
        source.write_text(json.dumps(blocked))
        with self.assertRaisesRegex(pipeline.PipelineError, 'erased user'):
            pipeline.apply_batch(self.db, source)
        # nothing from the rejected batch landed, and the fine event is not in
        self.assertEqual(self.table_count('events'), 3)
        self.assertEqual(self.table_count('batches'), 1)
        # a batch for the remaining users is still accepted
        ok = batch('B3', '2026-01-07T00:00:00Z',
                   [touch('E10', 'U2', '2026-01-07T00:00:00Z')])
        ok_source = self.dir / 'b3.json'
        ok_source.write_text(json.dumps(ok))
        summary = pipeline.apply_batch(self.db, ok_source)
        self.assertFalse(summary['replayed'])
        self.assertEqual(self.table_count('events'), 4)

    def test_replaying_a_batch_touching_an_erased_user_still_side_effect_free(self):
        self.seed()
        b1 = self.dir / 'b1.json'
        pipeline.apply_batch(self.db, b1)
        self.erase('E1', ['U1'])
        # B1's original bytes still replay as a no-op; the tombstone check runs
        # only for genuinely new batches.
        summary = pipeline.apply_batch(self.db, b1)
        self.assertTrue(summary['replayed'])

    # -- generation fence ------------------------------------------------------

    def test_apply_and_erase_advance_generation_monotonically(self):
        self.seed()
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='write_generation'")
                         .fetchone()[0], '1')  # one apply so far
        conn.close()
        s1 = self.erase('E1', ['U1'])
        self.assertEqual(s1['generation'], 2)
        payload = batch('B2', '2026-01-06T00:00:00Z',
                        [touch('E9', 'U2', '2026-01-06T00:00:00Z')])
        source = self.dir / 'b2.json'
        source.write_text(json.dumps(payload))
        s2 = pipeline.apply_batch(self.db, source)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='write_generation'")
                         .fetchone()[0], '3')
        conn.close()
        # an erase replay does not advance the generation
        replay = erase.erase(self.db, self.write_erase('E1', ['U1'], 'r.json'))
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['generation'], 3)

    def test_stale_snapshot_release_aborts_without_charge_and_retry_succeeds(self):
        self.seed()
        privacy.init_ledger(self.ledger, '10')
        req = self.dir / 'req.json'
        req.write_text(json.dumps(release_request('R1')))

        def barrier(generation):
            # Erasure commits after the snapshot was read but before the
            # ledger write transaction: the fence must reject the snapshot.
            self.erase('E1', ['U1'])

        privacy._commit_barrier = barrier
        with self.assertRaisesRegex(privacy.PrivacyError, 'source changed since snapshot'):
            privacy.release(self.db, self.ledger, req,
                            self.dir / 'out1.json', rng=zero_noise)
        privacy._commit_barrier = None
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], '0')
        conn.close()
        self.assertFalse((self.dir / 'out1.json').exists())
        # retry against the new snapshot: U1 absent, exactly one charge
        summary = privacy.release(self.db, self.ledger, req,
                                  self.dir / 'out2.json', rng=zero_noise)
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['epsilon_spent'], '0.5')
        report = json.loads((self.dir / 'out2.json').read_text())
        counts = {(c['date'], c['channel']): c['noisy_count'] for c in report['counts']}
        self.assertEqual(counts[('2026-01-01', 'email')], 0.0)   # U1 erased
        self.assertEqual(counts[('2026-01-02', 'email')], 1.0)   # U2 remains

    def test_release_before_erase_keeps_old_bytes_and_erase_blocks_fresh_charge(self):
        self.seed()
        privacy.init_ledger(self.ledger, '10')
        req = self.dir / 'req.json'
        req.write_text(json.dumps(release_request('R1')))
        first = privacy.release(self.db, self.ledger, req,
                                self.dir / 'out.json', rng=zero_noise)
        self.assertEqual(first['source_as_of'], '2026-01-05T00:00:00Z')
        original_bytes = (self.dir / 'out.json').read_bytes()
        self.erase('E1', ['U1'])
        # the committed release id still returns the original payload bytes,
        # no re-read, no resample, no second charge
        again = privacy.release(self.db, self.ledger, req,
                                self.dir / 'out2.json', rng=zero_noise)
        self.assertTrue(again['replayed'])
        self.assertEqual((self.dir / 'out2.json').read_bytes(), original_bytes)
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], str(10 ** 18 // 2))
        self.assertEqual(conn.execute('SELECT source_generation FROM releases')
                         .fetchone()[0], 1)
        conn.close()
        # but a new release id cannot publish the erased user from any snapshot
        req2 = self.dir / 'req2.json'
        req2.write_text(json.dumps(release_request('R2')))
        summary = privacy.release(self.db, self.ledger, req2,
                                  self.dir / 'out3.json', rng=zero_noise)
        self.assertFalse(summary['replayed'])
        report = json.loads((self.dir / 'out3.json').read_text())
        counts = {(c['date'], c['channel']): c['noisy_count'] for c in report['counts']}
        self.assertEqual(counts[('2026-01-01', 'email')], 0.0)

    def test_snapshot_read_never_writes_source(self):
        self.seed()
        privacy.init_ledger(self.ledger, '10')
        req = self.dir / 'req.json'
        req.write_text(json.dumps(release_request('R1')))
        before = self.db.read_bytes()
        privacy.release(self.db, self.ledger, req,
                        self.dir / 'out.json', rng=zero_noise)
        self.assertEqual(self.db.read_bytes(), before)

    def test_apply_commit_in_fence_window_also_aborts_release(self):
        self.seed()
        privacy.init_ledger(self.ledger, '10')
        req = self.dir / 'req.json'
        req.write_text(json.dumps(release_request('R1')))

        def barrier(generation):
            # Any committed source writer (here a plain apply, not an erase)
            # advances the generation and invalidates the held snapshot.
            payload = batch('B2', '2026-01-06T00:00:00Z',
                            [touch('E9', 'U3', '2026-01-06T00:00:00Z')])
            source = self.dir / 'b2.json'
            source.write_text(json.dumps(payload))
            pipeline.apply_batch(self.db, source)

        privacy._commit_barrier = barrier
        with self.assertRaisesRegex(privacy.PrivacyError, 'source changed since snapshot'):
            privacy.release(self.db, self.ledger, req,
                            self.dir / 'out.json', rng=zero_noise)
        privacy._commit_barrier = None
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], '0')
        conn.close()

    # -- crash semantics -------------------------------------------------------

    def test_pre_commit_failure_leaves_no_trace_and_retry_completes(self):
        self.seed()
        req = self.write_erase('E1', ['U1'])
        # Kill a separate process mid-transaction, well before the commit
        # sequence starts; the rollback journal must undo everything and
        # leave no half deletion.
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        code = (
            "import time\n"
            "from visit_attribution import erase\n"
            "orig = erase._bump_generation\n"
            "def slow(conn):\n"
            "    time.sleep(3600)\n"
            "    orig(conn)\n"
            "erase._bump_generation = slow\n"
            "erase.erase(%r, %r)\n"
            % (str(self.db), str(req)))
        proc = subprocess.Popen([sys.executable, '-c', code], env=env,
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True)
        deadline = time.time() + 10
        # Wait until the write transaction has clearly opened (journal exists)
        # but the commit cannot have started while _bump_generation sleeps.
        journal = Path(str(self.db) + '-journal')
        while not journal.exists() and time.time() < deadline:
            time.sleep(0.05)
        self.assertTrue(journal.exists())
        time.sleep(0.3)
        os.killpg(proc.pid, 9)
        proc.wait(10)
        self.assertEqual(self.table_count('tombstones'), 0)
        self.assertEqual(self.table_count('erasures'), 0)
        self.assertEqual(self.table_count('events'), 7)   # nothing half-deleted
        self.assertEqual(self.table_count('m_conversions'), 2)
        summary = erase.erase(self.db, req)               # retry works
        self.assertFalse(summary['replayed'])
        self.assertEqual(self.table_count('events'), 3)
        self.assertEqual(self.table_count('tombstones'), 1)

    def test_post_commit_fault_retry_is_replay_without_resurrection(self):
        self.seed()
        req = self.write_erase('E1', ['U1'])
        os.environ['VA_ERASE_FAULT_AFTER_COMMIT'] = '1'
        with self.assertRaises(OSError):
            erase.erase(self.db, req)
        os.environ.pop('VA_ERASE_FAULT_AFTER_COMMIT')
        # the transaction already committed: deletion is durable
        self.assertEqual(self.table_count('events'), 3)
        self.assertEqual(self.table_count('tombstones'), 1)
        summary = erase.erase(self.db, req)
        self.assertTrue(summary['replayed'])
        self.assertEqual(self.table_count('events'), 3)
        self.assertEqual(self.table_count('tombstones'), 1)
        # data cannot be resurrected through ingestion either
        payload = batch('B2', '2026-01-06T00:00:00Z',
                        [touch('E9', 'U1', '2026-01-06T00:00:00Z')])
        source = self.dir / 'b2.json'
        source.write_text(json.dumps(payload))
        with self.assertRaises(pipeline.PipelineError):
            pipeline.apply_batch(self.db, source)

    # -- ledger migration ------------------------------------------------------

    def test_legacy_ledger_migrates_and_keeps_payload_and_budget(self):
        self.seed()
        # build a pre-fence ledger the way privacy v1 did: no generation column
        conn = sqlite3.connect(self.ledger)
        conn.execute('CREATE TABLE meta(key TEXT PRIMARY KEY, value TEXT NOT NULL) WITHOUT ROWID')
        conn.execute('CREATE TABLE releases(release_id TEXT PRIMARY KEY, spec_sha256 TEXT NOT NULL,'
                     ' epsilon_scaled TEXT NOT NULL, payload BLOB NOT NULL) WITHOUT ROWID')
        conn.execute("INSERT INTO meta VALUES('epsilon_budget_scaled','10000000000000000000')")
        conn.execute("INSERT INTO meta VALUES('epsilon_spent_scaled','500000000000000000')")
        conn.execute("INSERT INTO releases VALUES('R0','deadbeef','500000000000000000',X'0102')")
        conn.commit()
        conn.close()
        req = self.dir / 'req.json'
        req.write_text(json.dumps(release_request('R1')))
        summary = privacy.release(self.db, self.ledger, req,
                                  self.dir / 'out.json', rng=zero_noise)
        self.assertEqual(summary['epsilon_spent'], '1')  # 0.5 + 0.5
        conn = sqlite3.connect(self.ledger)
        columns = {r[1] for r in conn.execute('PRAGMA table_info(releases)')}
        self.assertIn('source_generation', columns)
        self.assertEqual(conn.execute(
            "SELECT source_generation,payload FROM releases WHERE release_id='R0'")
            .fetchone(), (0, b'\x01\x02'))
        conn.close()


class EraseProcessInterleaveTests(unittest.TestCase):
    """Real cross-process interleaving of erase, apply and privacy-release."""

    def setUp(self):
        for var in ('VA_ERASE_FAULT_AFTER_COMMIT', 'VA_PRIVACY_FAULT_AFTER_COMMIT',
                    'VA_ERASE_BARRIER', 'VA_PRIVACY_SNAPSHOT_BARRIER'):
            os.environ.pop(var, None)
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'privacy.db'
        self.barriers = self.dir / 'barriers'
        self.barriers.mkdir()
        self.env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'),
                        VA_PRIVACY_RNG_ZERO='1')
        payload = batch('B1', '2026-01-05T00:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z'),
            touch('E2', 'U2', '2026-01-02T00:00:00Z'),
            purchase('P1', 'U2', '2026-01-02T00:05:00Z')])
        (self.dir / 'b1.json').write_text(json.dumps(payload))
        subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'apply',
                        '--db', str(self.db), '--source', str(self.dir / 'b1.json')],
                       env=self.env, check=True, capture_output=True)
        subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'privacy-init',
                        '--ledger', str(self.ledger), '--budget', '10'],
                       env=self.env, check=True, capture_output=True)
        (self.dir / 'req.json').write_text(json.dumps(release_request('R1')))
        (self.dir / 'erase.json').write_text(json.dumps(
            {'request_id': 'ER1', 'users': ['U1']}))
        (self.dir / 'b2.json').write_text(json.dumps(batch(
            'B2', '2026-01-06T00:00:00Z',
            [touch('E9', 'U2', '2026-01-06T00:00:00Z')])))

    def tearDown(self):
        for var in ('VA_ERASE_FAULT_AFTER_COMMIT', 'VA_PRIVACY_FAULT_AFTER_COMMIT',
                    'VA_ERASE_BARRIER', 'VA_PRIVACY_SNAPSHOT_BARRIER'):
            os.environ.pop(var, None)
        self.td.cleanup()

    def wait_for(self, name, timeout=30):
        deadline = time.time() + timeout
        path = self.barriers / name
        while not path.exists():
            if time.time() > deadline:
                raise AssertionError('timed out waiting for barrier ' + name)
            time.sleep(0.02)

    def signal(self, name):
        (self.barriers / name).write_text('1')

    def start(self, *cli_args, extra_env=None):
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        return subprocess.Popen([sys.executable, '-m', 'visit_attribution.cli', *cli_args],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                text=True)

    def drain(self, proc):
        out, err = proc.communicate(timeout=30)
        return proc.returncode, out, err

    def test_erase_wins_then_paused_release_aborts_and_retry_is_clean(self):
        # Release pauses with a completed source snapshot but no ledger charge.
        rel = self.start('privacy-release', '--source', str(self.db),
                         '--ledger', str(self.ledger), '--request', str(self.dir / 'req.json'),
                         '--output', str(self.dir / 'rel.json'),
                         extra_env={'VA_PRIVACY_SNAPSHOT_BARRIER': str(self.barriers)})
        self.wait_for('snapshot-read')
        # eraser commits while the release is paused between snapshot and charge
        er = self.start('erase', '--db', str(self.db),
                        '--request', str(self.dir / 'erase.json'))
        code, _, err = self.drain(er)
        self.assertEqual(code, 0, err)
        self.signal('release-proceed')
        code, _, err = self.drain(rel)
        self.assertEqual(code, 2, err)
        self.assertIn('source changed since snapshot', err)
        # no charge, no output from the aborted release
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 0)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], '0')
        conn.close()
        self.assertFalse((self.dir / 'rel.json').exists())
        # concurrent apply during the same window is serialized, not refused
        ap = self.start('apply', '--db', str(self.db),
                        '--source', str(self.dir / 'b2.json'))
        code, out, err = self.drain(ap)
        self.assertEqual(code, 0, err)
        self.assertFalse(json.loads(out)['replayed'])
        # retry the release: succeeds against the post-erasure snapshot. CLI
        # uses SystemRandom noise, so assert on true-count bounds rather than
        # exact zeros: the erased day has no users and the other day has one.
        rel2 = self.start('privacy-release', '--source', str(self.db),
                          '--ledger', str(self.ledger),
                          '--request', str(self.dir / 'req.json'),
                          '--output', str(self.dir / 'rel2.json'))
        code, out, err = self.drain(rel2)
        self.assertEqual(code, 0, err)
        self.assertFalse(json.loads(out)['replayed'])
        report = json.loads((self.dir / 'rel2.json').read_text())
        counts = {(c['date'], c['channel']): c['noisy_count'] for c in report['counts']}
        self.assertEqual(counts[('2026-01-01', 'email')], 0.0)
        self.assertEqual(counts[('2026-01-02', 'email')], 1.0)
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], '500000000000000000')
        conn.close()

    def test_release_wins_when_its_fence_lock_blocks_the_eraser(self):
        # Eraser waits to start until the release holds its fence read lock.
        rel = self.start('privacy-release', '--source', str(self.db),
                         '--ledger', str(self.ledger), '--request', str(self.dir / 'req.json'),
                         '--output', str(self.dir / 'rel.json'),
                         extra_env={'VA_ERASE_BARRIER': str(self.barriers)})
        self.wait_for('fence-held')
        er = self.start('erase', '--db', str(self.db),
                        '--request', str(self.dir / 'erase.json'))
        # give the eraser time to block on the writer lock
        time.sleep(1.0)
        self.assertIsNone(er.poll())           # blocked, not refused
        code, out, err = self.drain(rel)
        self.assertEqual(code, 0, err)         # release commits first
        original = (self.dir / 'rel.json').read_bytes()
        code2, out2, err2 = self.drain(er)
        self.assertEqual(code2, 0, err2)       # then the eraser goes through
        # the release was charged exactly once and keeps its old-snapshot bytes
        conn = sqlite3.connect(self.ledger)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM releases').fetchone()[0], 1)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='epsilon_spent_scaled'")
                         .fetchone()[0], '500000000000000000')
        conn.close()
        # replaying the committed id after erasure returns identical bytes
        replay = self.start('privacy-release', '--source', str(self.db),
                            '--ledger', str(self.ledger),
                            '--request', str(self.dir / 'req.json'),
                            '--output', str(self.dir / 'rel-copy.json'))
        code, out, err = self.drain(replay)
        self.assertEqual(code, 0, err)
        self.assertTrue(json.loads(out)['replayed'])
        self.assertEqual((self.dir / 'rel-copy.json').read_bytes(), original)
        # and the erased user is genuinely gone from the source
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT COUNT(*) FROM events WHERE user='U1'")
                         .fetchone()[0], 0)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM tombstones').fetchone()[0], 1)
        conn.close()

    def test_cli_erase_end_to_end_and_fault_retry(self):
        er = self.start('erase', '--db', str(self.db),
                        '--request', str(self.dir / 'erase.json'))
        code, out, err = self.drain(er)
        self.assertEqual(code, 0, err)
        summary = json.loads(out)
        self.assertEqual(summary['request_id'], 'ER1')
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['erased_users'], 1)
        self.assertEqual(summary['tombstones_total'], 1)
        # CLI replay is a no-op
        er2 = self.start('erase', '--db', str(self.db),
                         '--request', str(self.dir / 'erase.json'))
        code, out, err = self.drain(er2)
        self.assertEqual(code, 0, err)
        self.assertTrue(json.loads(out)['replayed'])
        # different parameters under the same id are refused via exit code 2
        alt = self.dir / 'erase-alt.json'
        alt.write_text(json.dumps({'request_id': 'ER1', 'users': ['U2']}))
        er3 = self.start('erase', '--db', str(self.db), '--request', str(alt))
        code, _, err = self.drain(er3)
        self.assertEqual(code, 2)
        self.assertIn('different parameters', err)
        # export still works and never mentions the erased user
        ex = self.start('export', '--db', str(self.db),
                        '--output', str(self.dir / 'report.json'))
        code, _, err = self.drain(ex)
        self.assertEqual(code, 0, err)
        report = json.loads((self.dir / 'report.json').read_text())
        self.assertNotIn(b'U1', (self.dir / 'report.json').read_bytes())
        self.assertEqual({s['user'] for s in report['result']['sessions']}, {'U2'})


if __name__ == '__main__':
    unittest.main()

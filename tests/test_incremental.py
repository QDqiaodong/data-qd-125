import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from visit_attribution import pipeline
from visit_attribution.common import parse_raw, read_raw
from visit_attribution.events import iso, parse as parse_events
from visit_attribution.model import analyze

ROOT = Path(__file__).resolve().parents[1]
GAP, LOOKBACK = 1800, 3600


def touch(eid, user, at, channel):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'touch',
            'channel': channel, 'amount_cents': None}


def purchase(eid, user, at, amount):
    return {'id': eid, 'user': user, 'at': at, 'kind': 'purchase',
            'channel': None, 'amount_cents': amount}


def batch(batch_id, as_of, events, gap=GAP, lookback=LOOKBACK):
    return {'batch_id': batch_id, 'as_of': as_of,
            'session_gap_seconds': gap, 'lookback_seconds': lookback,
            'events': events}


def write_batch(td, name, payload):
    path = Path(td) / name
    path.write_text(json.dumps(payload))
    return path


def accumulated_model(batches):
    """What model.analyze would produce on the accumulated snapshot."""
    merged = {'as_of': batches[-1]['as_of'],
              'session_gap_seconds': GAP, 'lookback_seconds': LOOKBACK,
              'events': [e for b in batches for e in b['events']]}
    return analyze(merged)


def export_result(db):
    conn = pipeline.connect(db)
    try:
        return pipeline.build_result(conn)
    finally:
        conn.close()


class IncrementalTests(unittest.TestCase):
    def setUp(self):
        os.environ.pop('VA_FAULT_AFTER_EVENTS', None)
        self.td = tempfile.TemporaryDirectory()
        self.td_path = Path(self.td.name)
        self.db = self.td_path / 'store.db'

    def tearDown(self):
        self.td.cleanup()

    def apply(self, name, payload):
        source = write_batch(self.td_path, name, payload)
        return pipeline.apply_batch(self.db, source)

    def export(self, output_name='report.json'):
        out = self.td_path / output_name
        pipeline.export_db(self.db, out)
        return out

    def test_batch_id_required_and_validated(self):
        payload = batch('B1', '2026-01-01T01:00:00Z', [])
        source = write_batch(self.td_path, 'b.json', {k: v for k, v in payload.items()
                                                      if k != 'batch_id'})
        with self.assertRaises(pipeline.PipelineError):
            pipeline.apply_batch(self.db, source)
        bad = dict(payload, batch_id=' has space ')
        with self.assertRaises(ValueError):
            self.apply('bad.json', bad)

    def test_first_batch_fixes_gap_and_lookback(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', []))
        with self.assertRaises(pipeline.PipelineError):
            self.apply('b2.json', batch('B2', '2026-01-01T02:00:00Z', [], gap=1801))
        with self.assertRaises(pipeline.PipelineError):
            self.apply('b2.json', batch('B2', '2026-01-01T02:00:00Z', [], lookback=3500))
        # same parameters are accepted
        self.apply('b2.json', batch('B2', '2026-01-01T02:00:00Z', []))

    def test_as_of_never_moves_backwards(self):
        self.apply('b1.json', batch('B1', '2026-01-01T02:00:00Z', []))
        with self.assertRaises(pipeline.PipelineError):
            self.apply('b2.json', batch('B2', '2026-01-01T01:59:59Z', []))
        # equal as_of is allowed (empty re-confirmation)
        self.apply('b2.json', batch('B2', '2026-01-01T02:00:00Z', []))

    def test_replay_same_bytes_is_side_effect_free(self):
        payload = batch('B1', '2026-01-01T01:00:00Z',
                        [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')])
        source = write_batch(self.td_path, 'b1.json', payload)
        first = pipeline.apply_batch(self.db, source)
        self.assertFalse(first['replayed'])
        conn = sqlite3.connect(self.db)
        snapshot = conn.execute('SELECT * FROM events').fetchall()
        meta = conn.execute('SELECT key,value FROM meta').fetchall()
        conn.close()
        second = pipeline.apply_batch(self.db, source)
        self.assertTrue(second['replayed'])
        self.assertEqual(second['inserted_events'], 0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT * FROM events').fetchall(), snapshot)
        self.assertEqual(conn.execute('SELECT key,value FROM meta').fetchall(), meta)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1)
        conn.close()

    def test_same_batch_id_different_bytes_refused(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z',
                                    [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')]))
        altered = batch('B1', '2026-01-01T01:00:00Z',
                        [touch('E1', 'U1', '2026-01-01T00:00:01Z', 'email')])
        with self.assertRaises(pipeline.PipelineError):
            self.apply('b1b.json', altered)
        # nothing partially applied: original event stays unchanged
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT at FROM events WHERE id=?', ('E1',)).fetchone()[0],
                         '2026-01-01T00:00:00Z')
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 1)
        conn.close()

    def test_cross_batch_dedup_conflict_and_counter(self):
        dup = touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')
        b1 = batch('B1', '2026-01-01T01:00:00Z', [dup])
        b2 = batch('B2', '2026-01-01T02:00:00Z', [dict(dup), purchase('P1', 'U1',
                   '2026-01-01T00:30:00Z', 100)])
        self.apply('b1.json', b1)
        summary = self.apply('b2.json', b2)
        # cross-batch identical ID does not inflate the duplicate counter
        self.assertEqual(summary['inserted_events'], 1)
        self.assertEqual(export_result(self.db)['duplicate_events'], 0)
        conflict = batch('B3', '2026-01-01T03:00:00Z',
                         [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'search')])
        with self.assertRaises(pipeline.PipelineError):
            self.apply('b3.json', conflict)
        # intra-batch duplicates still count, and only on first success
        b4 = batch('B4', '2026-01-01T04:00:00Z',
                   [touch('E2', 'U1', '2026-01-01T00:05:00Z', 'email'),
                    {'id': 'E2', 'user': 'U1', 'at': '2026-01-01T08:05:00+08:00',
                     'kind': 'touch', 'channel': 'email', 'amount_cents': None}])
        self.apply('b4.json', b4)
        self.assertEqual(export_result(self.db)['duplicate_events'], 1)
        self.apply('b4r.json', b4)  # replay
        self.assertEqual(export_result(self.db)['duplicate_events'], 1)

    def test_future_events_retained_and_activated_on_as_of_advance(self):
        b1 = batch('B1', '2026-01-01T01:00:00Z',
                   [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
                    touch('F1', 'U1', '2026-01-01T02:00:00Z', 'search')])
        self.apply('b1.json', b1)
        result = export_result(self.db)
        self.assertEqual(result['future_event_ids'], ['F1'])
        self.assertEqual(result['session_count'], 1)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT active FROM events WHERE id=?', ('F1',)).fetchone()[0], 0)
        conn.close()
        # empty batch advancing the watermark activates F1 and recomputes U1
        summary = self.apply('b2.json', batch('B2', '2026-01-01T03:00:00Z', []))
        self.assertEqual(summary['activated_events'], 1)
        self.assertEqual(summary['recomputed_users'], 1)
        result = export_result(self.db)
        self.assertEqual(result['future_event_ids'], [])
        self.assertEqual(result['session_count'], 2)
        # an empty batch that does not cross any event does no recompute work
        summary = self.apply('b3.json', batch('B3', '2026-01-01T03:30:00Z', []))
        self.assertEqual(summary['activated_events'], 0)
        self.assertEqual(summary['recomputed_users'], 0)

    def test_late_event_merges_sessions_and_rewrites_history(self):
        # Two events 1801s apart form two sessions; a late bridge touch between
        # them must merge them and move the session id back to the earliest id.
        b1 = batch('B1', '2026-01-02T00:00:00Z',
                   [touch('E2', 'U1', '2026-01-01T00:30:00Z', 'email'),
                    purchase('P1', 'U1', '2026-01-01T00:35:00Z', 100)])
        self.apply('b1.json', b1)
        before = export_result(self.db)
        self.assertEqual(before['session_count'], 1)
        b2 = batch('B2', '2026-01-02T00:00:00Z',
                   [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'search'),
                    purchase('P2', 'U1', '2026-01-01T01:00:01Z', 200)])
        self.apply('b2.json', b2)
        after = export_result(self.db)
        # both purchases now belong to one session whose id is the earliest event id
        self.assertEqual(after['session_count'], 1)
        sessions = self._sessions()
        self.assertEqual(sessions, [('E1', 'E1', '2026-01-01T00:00:00Z',
                                     '2026-01-01T01:00:01Z', ['E1', 'E2', 'P1', 'P2'])])
        conversions = self._conversions()
        self.assertEqual([c[0] for c in conversions], ['P1', 'P2'])
        self.assertTrue(all(c[1] == 'E1' for c in conversions))
        # the historical purchase P1 is re-attributed through the late touch
        p1 = next(c for c in conversions if c[0] == 'P1')
        self.assertEqual(p1[2], 'search')
        # untouched user U2 from a third batch is not recomputed here
        self.apply('b3.json', batch('B3', '2026-01-02T00:00:00Z',
                                    [purchase('Q1', 'U2', '2026-01-01T00:10:00Z', 50)]))
        self.assertEqual(self.apply('b4.json', batch('B4', '2026-01-02T00:00:00Z',
                                                      [touch('E3', 'U1', '2026-01-01T00:15:00Z', 'social')]))['recomputed_users'], 1)

    def test_same_second_touches_ordered_by_id(self):
        events = [touch('EZ', 'U1', '2026-01-01T00:00:00Z', 'email'),
                  touch('EA', 'U1', '2026-01-01T00:00:00Z', 'search'),
                  purchase('P1', 'U1', '2026-01-01T00:00:00Z', 100)]
        # split across batches to prove cross-batch same-second handling
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', events[:1]))
        self.apply('b2.json', batch('B2', '2026-01-01T01:00:00Z', events[1:]))
        result = export_result(self.db)
        conv = result['conversions'][0]
        self.assertEqual(conv['models']['first_non_direct']['channel'], 'search')
        self.assertEqual(conv['models']['last_non_direct']['channel'], 'email')
        self._assert_equivalent([batch('B1', '2026-01-01T01:00:00Z', events[:1]),
                                 batch('B2', '2026-01-01T01:00:00Z', events[1:])])

    def test_empty_batch_advances_as_of(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', []))
        self.assertEqual(export_result(self.db)['as_of'], '2026-01-01T01:00:00Z')
        self.apply('b2.json', batch('B2', '2026-01-02T00:00:00Z', []))
        self.assertEqual(export_result(self.db)['as_of'], '2026-01-02T00:00:00Z')
        out = self.export()
        report = json.loads(out.read_text())
        self.assertEqual(report['result']['as_of'], '2026-01-02T00:00:00Z')
        self.assertEqual(report['result']['sessions'], [])

    def test_multi_batch_equivalent_to_full_model(self):
        b1 = batch('B1', '2026-01-01T01:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('E2', 'U1', '2026-01-01T00:10:00Z', 'direct'),
            purchase('P1', 'U1', '2026-01-01T00:15:00Z', 1000),
            touch('F1', 'U1', '2026-01-01T02:00:00Z', 'search')])
        b2 = batch('B2', '2026-01-01T01:30:00Z', [
            purchase('P2', 'U1', '2026-01-01T00:45:01Z', 500),
            purchase('P3', 'U2', '2026-01-01T00:20:00Z', 100),
            touch('E3', 'U2', '2026-01-01T00:05:00Z', 'social')])
        b3 = batch('B3', '2026-01-02T00:00:00Z', [])  # activates F1
        b4 = batch('B4', '2026-01-02T00:30:00Z', [
            {'id': 'E4', 'user': 'U2', 'at': '2026-01-01T08:06:00+08:00',
             'kind': 'touch', 'channel': 'email', 'amount_cents': None},
            {'id': 'E4', 'user': 'U2', 'at': '2026-01-01T00:06:00Z',
             'kind': 'touch', 'channel': 'email', 'amount_cents': None}])  # in-batch duplicate
        batches = [b1, b2, b3, b4]
        for i, payload in enumerate(batches, 1):
            self.apply(f'b{i}.json', payload)
        self._assert_equivalent(batches)
        result = export_result(self.db)
        self.assertEqual(result['duplicate_events'], 1)
        self.assertEqual(result['future_event_ids'], [])

    def test_only_affected_users_recomputed(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            touch('G1', 'U9', '2026-01-01T00:00:00Z', 'email')]))
        summary = self.apply('b2.json', batch('B2', '2026-01-01T02:00:00Z', [
            touch('E2', 'U1', '2026-01-01T00:30:00Z', 'search')]))
        self.assertEqual(summary['recomputed_users'], 1)
        # future events stay inactive; the later watermark activates only them,
        # and recomputation is limited to their owners
        self.apply('b3a.json', batch('B3A', '2026-01-01T02:00:00Z', [
            touch('F1', 'U9', '2026-01-01T02:30:00Z', 'search'),
            touch('F2', 'U1', '2026-01-01T02:45:00Z', 'search')]))
        self.assertEqual(export_result(self.db)['future_event_ids'], ['F1', 'F2'])
        summary = self.apply('b3.json', batch('B3', '2026-01-01T03:00:00Z', []))
        self.assertEqual(summary['activated_events'], 2)
        self.assertEqual(summary['recomputed_users'], 2)
    def test_interrupt_leaves_no_partial_effect(self):
        payload = batch('B1', '2026-01-01T01:00:00Z',
                        [touch(f'E{i}', 'U1', f'2026-01-01T00:{i:02d}:00Z', 'email')
                         for i in range(5)])
        source = write_batch(self.td_path, 'b1.json', payload)
        os.environ['VA_FAULT_AFTER_EVENTS'] = '2'
        with self.assertRaises(RuntimeError):
            pipeline.apply_batch(self.db, source)
        os.environ.pop('VA_FAULT_AFTER_EVENTS')
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM events').fetchone()[0], 0)
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM batches').fetchone()[0], 0)
        self.assertIsNone(conn.execute("SELECT value FROM meta WHERE key='as_of'").fetchone())
        conn.close()
        # retry succeeds and database is usable
        pipeline.apply_batch(self.db, source)
        self.assertEqual(export_result(self.db)['session_count'], 1)

    def test_export_failure_keeps_old_file_and_is_complete(self):
        self.apply('b1.json', batch('B1', '2026-01-01T01:00:00Z', [
            touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email'),
            purchase('P1', 'U1', '2026-01-01T00:05:00Z', 999)]))
        out = self.export()
        before = out.read_bytes()
        # exporting an uninitialized database fails and must not touch the file
        fresh = self.td_path / 'fresh.db'
        fresh.write_bytes(b'')
        with self.assertRaises(pipeline.PipelineError):
            pipeline.export_db(fresh, out)
        self.assertEqual(out.read_bytes(), before)
        # symlink output is rejected
        link = self.td_path / 'link.json'
        link.symlink_to(out)
        with self.assertRaises(pipeline.PipelineError):
            pipeline.export_db(self.db, link)
        # export preserves field order of the in-memory model
        pairs = []
        json.loads(out.read_text(), object_pairs_hook=lambda p: pairs.append(p))
        top = pairs[-1]  # hooks fire bottom-up; the root object is appended last
        self.assertEqual([k for k, _ in top], ['schema_version', 'result'])
        result_keys = ['as_of', 'duplicate_events', 'future_event_ids', 'sessions',
                       'conversions', 'session_count', 'conversion_count',
                       'total_revenue_cents', 'channel_totals']
        result_pairs = next(p for p in pairs if [k for k, _ in p] == result_keys)
        self.assertEqual([k for k, _ in result_pairs],
                         ['as_of', 'duplicate_events', 'future_event_ids', 'sessions',
                          'conversions', 'session_count', 'conversion_count',
                          'total_revenue_cents', 'channel_totals'])

    def test_cli_apply_export_and_legacy_unchanged(self):
        payload = batch('B1', '2026-01-01T01:00:00Z', json.loads(
            (ROOT / 'fixtures/input.json').read_text())['events'])
        source = write_batch(self.td_path, 'b1.json', payload)
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        db = self.td_path / 'cli.db'
        out = self.td_path / 'cli.json'
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'apply',
                            '--db', str(db), '--source', str(source)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'export',
                            '--db', str(db), '--output', str(out)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        model = analyze({k: v for k, v in payload.items() if k != 'batch_id'})
        self.assertEqual(json.loads(out.read_text())['result'], model)
        # legacy CLI still works byte-identically
        legacy_out = self.td_path / 'legacy.json'
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli',
                            '--source', str(ROOT / 'fixtures/input.json'),
                            '--output', str(legacy_out)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        from visit_attribution.cli import run
        self.assertEqual(json.loads(legacy_out.read_text()),
                         run(ROOT / 'fixtures/input.json'))

    def test_cli_error_paths_and_help_dispatch(self):
        env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        payload = batch('B1', '2026-01-01T01:00:00Z',
                        [touch('E1', 'U1', '2026-01-01T00:00:00Z', 'email')])
        source = write_batch(self.td_path, 'b1.json', payload)
        db = self.td_path / 'e.db'
        good = [sys.executable, '-m', 'visit_attribution.cli', 'apply',
                '--db', str(db), '--source', str(source)]
        r = subprocess.run(good, capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 0, r.stderr)
        # replaying altered bytes under the same id fails with exit code 2
        altered = dict(payload)
        altered['events'] = [touch('E1', 'U1', '2026-01-01T00:00:01Z', 'email')]
        bad_source = write_batch(self.td_path, 'b1-alt.json', altered)
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'apply',
                            '--db', str(db), '--source', str(bad_source)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 2)
        self.assertIn('replay refused', r.stderr)
        # db and output must differ
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'export',
                            '--db', str(db), '--output', str(db)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 2)
        # exporting an uninitialized database fails without creating output
        fresh = self.td_path / 'fresh.db'
        fresh_out = self.td_path / 'never.json'
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'export',
                            '--db', str(fresh), '--output', str(fresh_out)],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 2)
        self.assertFalse(fresh_out.exists())
        # missing subcommand arguments fail through argparse (exit 2)
        r = subprocess.run([sys.executable, '-m', 'visit_attribution.cli', 'apply'],
                           capture_output=True, text=True, env=env)
        self.assertEqual(r.returncode, 2)

    def _sessions(self):
        conn = sqlite3.connect(self.db)
        rows = conn.execute('SELECT session_id,start,end FROM m_sessions ORDER BY user,seq').fetchall()
        events = conn.execute('SELECT event_id FROM m_session_events ORDER BY user,session_seq,event_seq').fetchall()
        conn.close()
        sid, start, end = rows[0]
        return [(sid, sid, start, end, [e[0] for e in events])]

    def _conversions(self):
        conn = sqlite3.connect(self.db)
        rows = conn.execute(
            'SELECT purchase_id,session_id,first_channel,last_channel '
            'FROM m_conversions ORDER BY user,seq').fetchall()
        conn.close()
        return rows

    def _assert_equivalent(self, batches):
        out = self.td_path / 'eq.json'
        pipeline.export_db(self.db, out)
        exported = json.loads(out.read_text())['result']
        expected = accumulated_model(batches)
        self.assertEqual(exported, expected)


if __name__ == '__main__':
    unittest.main()

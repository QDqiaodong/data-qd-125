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

from visit_attribution import commission, pipeline

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


class _CommissionBase(unittest.TestCase):
    RATES = {'email': 1000, 'search': 2500, 'sms': 500, 'direct': 0}

    def setUp(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'commission.db'

    def tearDown(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td.cleanup()

    # -- helpers -------------------------------------------------------------

    def apply(self, name, payload, db=None):
        source = self.dir / name
        source.write_text(json.dumps(payload))
        return pipeline.apply_batch(db or self.db, source)

    def init(self, rates=None, name='rates.json'):
        path = self.dir / name
        path.write_text(json.dumps({'rates': self.RATES if rates is None else rates}))
        return commission.init_ledger(self.ledger, path)

    def sync(self, sync_id, source=None):
        return commission.sync(source or self.db, self.ledger, sync_id)

    def plan(self, plan_id, amounts, name=None):
        path = self.dir / (name or ('plan-%s.json' % plan_id))
        path.write_text(json.dumps({'plan_id': plan_id, 'amounts': amounts}))
        return commission.plan_payout(self.ledger, path)

    def confirm(self, plan_id):
        return commission.confirm_payout(self.ledger, plan_id)

    def cancel(self, plan_id):
        return commission.cancel_payout(self.ledger, plan_id)

    def report(self, name='report.json'):
        out = self.dir / name
        commission.report(self.ledger, out)
        return json.loads(out.read_text())

    def channel_report(self, name='report.json'):
        return {row['channel']: row for row in self.report(name)['channels']}

    def ledger_rows(self, sql, params=()):
        conn = sqlite3.connect(self.ledger)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()

    def seed(self, db=None):
        """A: purchase 1000 @ email (rate 1000bps -> 100); B: 600 @ sms (500bps -> 30)."""
        return self.apply('b1.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('touch-email-1', 'user-alpha', '2026-01-01T00:00:00Z', 'email'),
            purchase('PA', 'user-alpha', '2026-01-01T00:05:00Z', 1000),
            touch('touch-sms-1', 'user-beta', '2026-01-02T00:00:00Z', 'sms'),
            purchase('PB', 'user-beta', '2026-01-02T00:05:00Z', 600),
        ]), db=db)


class CommissionCase(_CommissionBase):
    # -- initialization -------------------------------------------------------

    def test_init_requires_rates_file_and_refuses_reinit(self):
        summary = self.init()
        self.assertEqual(summary['channels_configured'], len(self.RATES))
        self.assertEqual(summary['rates'], dict(sorted(self.RATES.items())))
        with self.assertRaisesRegex(commission.CommissionError, 'already initialized'):
            self.init(name='rates2.json')
        self.assertTrue(self.ledger.exists())
        self.assertEqual(sorted(
            row[0] for row in self.ledger_rows(
                "SELECT name FROM sqlite_master WHERE type='table'")),
            sorted(commission._TABLES))  # no stray sqlite internal tables

    def test_init_rejects_invalid_rates(self):
        bad = [
            {'rates': []},
            {'rates': {'email': 10001}},
            {'rates': {'email': -1}},
            {'rates': {'email': '1000'}},
            {'rates': {'email': 1000.0}},
            {'rates': {'email': True}},
            {'rates': {' direct ': 100}},
            {'rates': {'direct': 100}},
            {'rates': {'unattributed': 500}},
            {'rates': {'x' * 129: 100}},
            {},
        ]
        for i, payload in enumerate(bad):
            path = self.dir / ('bad%d.json' % i)
            path.write_text(json.dumps(payload))
            with self.assertRaises(ValueError, msg=repr(payload)):
                commission.init_ledger(self.dir / ('led%d.db' % i), path)
        # an explicitly empty mapping is valid: every channel rate is zero
        empty = self.dir / 'empty-rates.json'
        empty.write_text(json.dumps({'rates': {}}))
        commission.init_ledger(self.dir / 'empty.db', empty)
        conn = sqlite3.connect(self.dir / 'empty.db')
        self.assertEqual(conn.execute('SELECT COUNT(*) FROM rates').fetchone()[0], 0)
        conn.close()
        # zero is a valid rate everywhere, including reserved channels
        path = self.dir / 'zero.json'
        path.write_text(json.dumps({'rates': {'email': 0, 'direct': 0, 'unattributed': 0}}))
        commission.init_ledger(self.ledger, path)
        self.assertEqual(self.ledger_rows('SELECT channel,bps FROM rates ORDER BY channel'),
                         [('direct', 0), ('email', 0), ('unattributed', 0)])

    def test_rates_are_immutable_after_init(self):
        self.init()
        conn = sqlite3.connect(self.ledger)
        try:
            for sql in ("UPDATE rates SET bps=9999 WHERE channel='email'",
                        "DELETE FROM rates WHERE channel='email'",
                        "INSERT INTO rates(channel,bps) VALUES('new',100)"):
                with self.assertRaises(sqlite3.IntegrityError, msg=sql):
                    conn.execute(sql)
            conn.rollback()
        finally:
            conn.close()
        self.assertEqual(self.ledger_rows('SELECT bps FROM rates WHERE channel=?', ('email',)),
                         [(1000,)])

    def test_unconfigured_channel_rate_is_zero_after_init(self):
        self.init({'email': 1000})
        self.apply('b.json', batch('B1', '2026-01-06T00:00:00Z', [
            touch('t1', 'u1', '2026-01-01T00:00:00Z', 'unconfigured'),
            purchase('P1', 'u1', '2026-01-01T00:01:00Z', 1000),
            touch('t2', 'u2', '2026-01-02T00:00:00Z', 'direct'),
            purchase('P2', 'u2', '2026-01-02T00:01:00Z', 1000)]))
        summary = self.sync('S1')
        self.assertEqual(summary['new_rights'], 2)
        self.assertEqual(summary['grants'], 0)
        self.assertEqual(self.ledger_rows('SELECT purchase_id,channel,commission_cents '
                                          'FROM rights ORDER BY purchase_id'),
                         [('P1', 'unconfigured', 0), ('P2', 'direct', 0)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(0,)])

    def test_commands_require_an_initialized_ledger(self):
        self.seed()
        for action in (lambda: self.sync('S1'), lambda: self.plan('P1', {'email': 1}),
                       lambda: self.confirm('P1'), lambda: self.cancel('P1'),
                       lambda: commission.report(self.ledger, self.dir / 'r.json')):
            with self.assertRaisesRegex(commission.CommissionError, 'not initialized'):
                action()
        self.assertFalse(self.ledger.exists())

    def test_plan_and_report_before_any_sync(self):
        self.init()
        with self.assertRaisesRegex(commission.CommissionError, 'no successful sync'):
            self.plan('P1', {'email': 1})
        report = self.report()
        self.assertIsNone(report['source'])
        self.assertIsNone(report['last_sync'])
        # configured channels are listed at zero balance before any sync
        self.assertEqual([row['channel'] for row in report['channels']],
                         sorted(self.RATES))
        self.assertTrue(all(row['accrued_cents'] == 0 for row in report['channels']))
        self.assertEqual(report['totals']['accrued_cents'], 0)
        self.assertTrue(report['conservation_ok'])

    # -- commission-sync: integer truncation and identity ---------------------

    def test_integer_truncation_and_zero_rate_channels(self):
        self.init({'email': 333, 'search': 2500, 'sms': 1})
        self.apply('b.json', batch('B1', '2026-01-06T00:00:00Z', [
            touch('t1', 'u1', '2026-01-01T00:00:00Z', 'email'),
            purchase('P1', 'u1', '2026-01-01T00:01:00Z', 999),    # 999*333//10000 = 33
            purchase('P2', 'u1', '2026-01-01T00:02:00Z', 3),      # 3*333//10000 = 0
            touch('t2', 'u2', '2026-01-02T00:00:00Z', 'sms'),
            purchase('P3', 'u2', '2026-01-02T00:01:00Z', 19999),  # *1//10000 = 1
            touch('t3', 'u3', '2026-01-03T00:00:00Z', 'unknown-chan'),
            purchase('P4', 'u3', '2026-01-03T00:01:00Z', 1000),   # unconfigured -> 0
            touch('t4', 'u4', '2026-01-04T00:00:00Z', 'direct'),
            purchase('P5', 'u4', '2026-01-04T00:01:00Z', 1000)]))  # direct -> 0
        summary = self.sync('S1')
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['new_rights'], 5)
        self.assertEqual(summary['grants'], 2)
        self.assertEqual(summary['entries'], 2)
        self.assertEqual(self.ledger_rows('SELECT purchase_id,commission_cents FROM rights '
                                          'ORDER BY purchase_id'),
                         [('P1', 33), ('P2', 0), ('P3', 1), ('P4', 0), ('P5', 0)])
        self.assertEqual(self.ledger_rows('SELECT channel,amount_cents FROM entries '
                                          'ORDER BY channel'),
                         [('email', 33), ('sms', 1)])
        report = self.channel_report()
        self.assertEqual(report['email']['accrued_cents'], 33)
        self.assertEqual(report['email']['available_cents'], 33)
        self.assertEqual(report['unknown-chan']['accrued_cents'], 0)
        self.assertEqual(report['direct']['accrued_cents'], 0)
        self.assertNotIn('unattributed', report)

    def test_unattributed_zero_rate(self):
        self.init()
        self.apply('b.json', batch('B1', '2026-01-06T00:00:00Z', [
            purchase('P1', 'u1', '2026-01-01T00:01:00Z', 1000)]))  # no touch
        summary = self.sync('S1')
        self.assertEqual(summary['new_rights'], 1)
        self.assertEqual(summary['grants'], 0)
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute('SELECT last_channel FROM m_conversions').fetchall(),
                         [('unattributed',)])
        conn.close()
        self.assertEqual(self.ledger_rows('SELECT channel FROM rights'), [('unattributed',)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(0,)])

    def test_snapshot_read_ignores_uncommitted_source_writes(self):
        self.init()
        self.seed()
        began = threading.Event()
        proceed = threading.Event()
        errors = []

        def concurrent_write():
            try:
                writer = sqlite3.connect(str(self.db), isolation_level=None)
                writer.execute('PRAGMA busy_timeout=5000')
                writer.execute('BEGIN IMMEDIATE')
                writer.execute(
                    "UPDATE m_conversions SET last_channel='search' WHERE purchase_id='PA'")
                began.set()                     # uncommitted edit in flight
                proceed.wait(10)
                writer.execute('COMMIT')
                writer.close()
            except Exception as error:          # noqa: BLE001 - asserted below
                errors.append(error)
                began.set()

        thread = threading.Thread(target=concurrent_write)
        thread.start()
        commission._snapshot_hook = lambda: self.assertTrue(began.wait(5))
        try:
            summary = self.sync('S1')
        finally:
            commission._snapshot_hook = None
            proceed.set()
            thread.join(10)
        self.assertEqual(errors, [])
        self.assertFalse(summary['changed_rights'])
        # the committed snapshot saw PA on email; the later uncommitted edit
        # never leaked into the ledger
        self.assertEqual(self.ledger_rows(
            'SELECT purchase_id,channel FROM rights ORDER BY purchase_id'),
            [('PA', 'email'), ('PB', 'sms')])
        # the writer did commit afterwards, so a fresh sync is a different snapshot
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute(
            "SELECT last_channel FROM m_conversions WHERE purchase_id='PA'").fetchone()[0],
            'search')
        conn.close()

    def test_sync_binds_source_path_and_rejects_other_source(self):
        self.init()
        self.seed()
        self.sync('S1')
        other = self.dir / 'other-store.db'
        self.apply('b2.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('t9', 'u9', '2026-01-01T00:00:00Z', 'email'),
            purchase('P9', 'u9', '2026-01-01T00:05:00Z', 100)]), db=other)
        with self.assertRaisesRegex(commission.CommissionError, 'bound to source'):
            self.sync('S2', source=other)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM syncs'), [(1,)])

    def test_sync_rejects_watermark_and_generation_regression(self):
        self.init()
        self.seed()                                   # generation 1
        self.sync('S1')
        self._force_state(self.ledger, as_of='2026-01-09T00:00:00Z')
        with self.assertRaisesRegex(commission.CommissionError, 'watermark moved backwards'):
            self.sync('S2')
        self._force_state(self.ledger, as_of='2026-01-05T00:00:00Z', generation=5)
        with self.assertRaisesRegex(commission.CommissionError, 'generation moved backwards'):
            self.sync('S3')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM syncs'), [(1,)])

    def _force_state(self, ledger, as_of=None, generation=None, content_sha256=None):
        conn = sqlite3.connect(ledger)
        try:
            row = conn.execute('SELECT as_of,generation,content_sha256 FROM source_state '
                               'WHERE id=1').fetchone()
            conn.execute('UPDATE source_state SET as_of=?,generation=?,content_sha256=? '
                         'WHERE id=1',
                         (as_of or row[0], row[1] if generation is None else generation,
                          content_sha256 or row[2]))
            conn.commit()
        finally:
            conn.close()

    def test_same_generation_different_content_is_rejected(self):
        self.init()
        self.seed()
        self.sync('S1')
        self._force_state(self.ledger, content_sha256='0' * 64)
        with self.assertRaisesRegex(commission.CommissionError,
                                    'content changed without a new generation'):
            self.sync('S2')

    def test_duplicate_purchase_id_in_source_snapshot_is_rejected(self):
        self.init()
        self.seed()
        # purchase_id is only unique per (user, seq) in the source schema; a
        # duplicate across users must be refused, not silently overwrite a right.
        conn = sqlite3.connect(self.db)
        conn.execute('INSERT INTO m_conversions(user,seq,purchase_id,at,session_id,'
                     'revenue_cents,first_channel,first_touch_id,last_channel,last_touch_id) '
                     "VALUES('other-user',99,'PA','2026-01-03T00:00:00Z','X',500,"
                     "'email',NULL,'email',NULL)")
        conn.execute("UPDATE meta SET value='2' WHERE key='write_generation'")
        conn.commit()
        conn.close()
        with self.assertRaisesRegex(commission.CommissionError, 'duplicate purchase_id'):
            self.sync('S2')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM syncs'), [(0,)])

    def test_same_id_same_snapshot_is_idempotent_and_other_snapshot_refused(self):
        self.init()
        self.seed()
        first = self.sync('S1')
        self.assertEqual(first['entries'], 2)
        second = self.sync('S1')
        self.assertTrue(second['replayed'])
        self.assertEqual(second['entries'], 2)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])
        # same sync id, new committed source snapshot -> refused
        self.apply('b2.json', batch('B2', '2026-01-06T00:00:00Z', [
            touch('t9', 'u1', '2026-01-06T00:00:00Z', 'email'),
            purchase('P9', 'u1', '2026-01-06T00:05:00Z', 500)]))
        with self.assertRaisesRegex(commission.CommissionError, 'different source snapshot'):
            self.sync('S1')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])

    def test_same_watermark_new_generation_is_a_new_snapshot(self):
        self.init()
        self.seed()
        self.sync('S1')
        # generation advances while as_of stays put (content changed with it):
        # equal as_of must never be used to infer an equal snapshot.
        self._rewrite_source(PA={'last_channel': 'search'})
        conn = sqlite3.connect(self.db)
        self.assertEqual(conn.execute("SELECT value FROM meta WHERE key='as_of'")
                         .fetchone()[0], '2026-01-05T00:00:00Z')
        conn.close()
        summary = self.sync('S2')
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['generation'], 2)
        self.assertEqual(summary['as_of'], '2026-01-05T00:00:00Z')
        self.assertEqual(summary['changed_rights'], 1)
        # PA (1000 cents) moves from email (1000 bps -> 100) to search (2500 bps)
        self.assertEqual(self.channel_report()['search']['accrued_cents'], 250)

    def test_repeated_sync_across_new_generation_posts_no_duplicates(self):
        self.init()
        self.seed()
        self.sync('S1')
        # a new apply that changes no conversion advances generation and as_of
        self.apply('b2.json', batch('B2', '2026-01-06T00:00:00Z', [
            touch('t9', 'u9', '2026-01-06T00:00:00Z', 'search')]))
        summary = self.sync('S2')
        self.assertFalse(summary['replayed'])
        self.assertEqual(summary['new_rights'], 0)
        self.assertEqual(summary['changed_rights'], 0)
        self.assertEqual(summary['removed_rights'], 0)
        self.assertEqual(summary['entries'], 0)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])

    # -- rights changes: reversals --------------------------------------------

    def _rewrite_source(self, **updates):
        """Model a committed source re-attribution directly on the source rows."""
        conn = sqlite3.connect(self.db)
        try:
            for purchase_id, fields in updates.items():
                assignments = ','.join('%s=?' % column for column in fields)
                conn.execute('UPDATE m_conversions SET %s WHERE purchase_id=?'
                             % assignments, list(fields.values()) + [purchase_id])
            row = conn.execute("SELECT value FROM meta WHERE key='write_generation'")
            generation = int(row.fetchone()[0]) + 1
            conn.execute("UPDATE meta SET value=? WHERE key='write_generation'",
                         (str(generation),))
            conn.commit()
        finally:
            conn.close()

    def test_channel_reassignment_reverses_old_then_grants_new(self):
        self.init()
        self.seed()                     # PA: email 100, PB: sms 30
        self.sync('S1')
        self._rewrite_source(PA={'last_channel': 'search', 'revenue_cents': 2000})
        summary = self.sync('S2')
        self.assertEqual(summary['changed_rights'], 1)
        self.assertEqual(summary['reversals'], 1)
        self.assertEqual(summary['grants'], 1)
        self.assertEqual(self.ledger_rows(
            'SELECT purchase_id,channel,amount_cents,kind FROM entries ORDER BY seq'), [
                ('PA', 'email', 100, 'grant'), ('PB', 'sms', 30, 'grant'),
                ('PA', 'email', -100, 'reversal'), ('PA', 'search', 500, 'grant')])
        self.assertEqual(self.channel_report()['email']['accrued_cents'], 0)
        self.assertEqual(self.channel_report()['search']['accrued_cents'], 500)
        self.assertEqual(self.channel_report()['sms']['accrued_cents'], 30)

    def test_shrinking_channel_expires_plans_touching_it_only(self):
        self.init()
        self.apply('b.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('te', 'u1', '2026-01-01T00:00:00Z', 'email'),
            purchase('PE', 'u1', '2026-01-01T00:10:00Z', 1000),
            touch('ts', 'u2', '2026-01-02T00:00:00Z', 'sms'),
            purchase('PS', 'u2', '2026-01-02T00:10:00Z', 2000),   # 500bps -> 100
            touch('tq', 'u3', '2026-01-03T00:00:00Z', 'search'),
            purchase('PQ', 'u3', '2026-01-03T00:10:00Z', 1000)]))
        self.sync('S1')
        self.plan('P-EMAIL', {'email': 60})
        self.plan('P-SMS', {'sms': 50})
        self.plan('P-BOTH', {'email': 40, 'sms': 30})
        self.plan('P-SEARCH', {'search': 100})
        # only email shrinks: every active plan containing email dies as a whole
        self._rewrite_source(PE={'revenue_cents': 0})
        summary = self.sync('S2')
        self.assertEqual(summary['expired_plans'], 2)
        statuses = {plan['plan_id']: plan['status'] for plan in self.report()['plans']}
        self.assertEqual(statuses, {'P-EMAIL': 'expired', 'P-SMS': 'active',
                                    'P-BOTH': 'expired', 'P-SEARCH': 'active'})
        report = self.channel_report()
        # P-BOTH's sms half was released too, P-SMS keeps its own reservation
        self.assertEqual(report['sms']['reserved_cents'], 50)
        self.assertEqual(report['sms']['available_cents'], 50)  # 100 - 50; P-BOTH's 30 released
        self.assertEqual(report['email']['reserved_cents'], 0)
        self.assertEqual(report['search']['reserved_cents'], 100)

    def test_amount_change_reverses_old_right(self):
        self.init()
        self.seed()
        self.sync('S1')
        self._rewrite_source(PA={'revenue_cents': 2000})
        summary = self.sync('S2')
        self.assertEqual(summary['reversals'], 1)
        self.assertEqual(self.ledger_rows(
            'SELECT channel,amount_cents,kind FROM entries ORDER BY seq'), [
                ('email', 100, 'grant'), ('sms', 30, 'grant'),
                ('email', -100, 'reversal'), ('email', 200, 'grant')])
        self.assertEqual(self.channel_report()['email']['accrued_cents'], 200)

    def test_purchase_disappearance_reverses_only_old_rights(self):
        self.init()
        self.seed()
        self.sync('S1')
        conn = sqlite3.connect(self.db)
        conn.execute("DELETE FROM m_conversions WHERE purchase_id='PB'")
        conn.execute("UPDATE meta SET value='2' WHERE key='write_generation'")
        conn.commit()
        conn.close()
        summary = self.sync('S2')
        self.assertEqual(summary['removed_rights'], 1)
        self.assertEqual(summary['reversals'], 1)
        self.assertEqual(summary['new_rights'], 0)
        self.assertEqual(self.channel_report()['sms']['accrued_cents'], 0)
        self.assertEqual(self.channel_report()['email']['accrued_cents'], 100)
        self.assertEqual(self.ledger_rows('SELECT purchase_id FROM rights'), [('PA',)])

    def test_full_reversal_chain_matches_rights_total(self):
        self.init()
        self.seed()
        self.sync('S1')
        self._rewrite_source(PA={'last_channel': 'direct'}, PB={'revenue_cents': 0})
        self.sync('S2')
        report = self.report()
        rights = {ch: total for ch, total in self.ledger_rows(
            'SELECT channel,SUM(commission_cents) FROM rights GROUP BY channel')}
        entries = {ch: total for ch, total in self.ledger_rows(
            'SELECT channel,SUM(amount_cents) FROM entries GROUP BY channel')}
        for channel in set(entries) | set(rights):
            self.assertEqual(entries.get(channel, 0), rights.get(channel, 0), channel)
        self.assertEqual(report['totals']['accrued_cents'], 0)
        self.assertTrue(report['conservation_ok'])
        # entries themselves are immutable
        conn = sqlite3.connect(self.ledger)
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute('UPDATE entries SET amount_cents=1')
        conn.rollback()
        conn.close()

    # -- plans ------------------------------------------------------------------

    def test_plan_requires_all_channels_and_creates_nothing_partial(self):
        self.init()
        self.seed()
        self.sync('S1')       # email 100, sms 30
        with self.assertRaisesRegex(commission.CommissionError, 'insufficient available'):
            self.plan('P1', {'email': 100, 'sms': 31})
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plans'), [(0,)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plan_allocations'), [(0,)])
        # amounts are positive integers only
        for bad in ({'email': 0}, {'email': -1}, {'email': 1.5}, {'email': '5'},
                    {'email': True}, {}):
            with self.assertRaises(ValueError, msg=repr(bad)):
                self.plan('PX', bad)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plans'), [(0,)])

    def test_plan_for_unconfigured_channel_has_no_balance(self):
        self.init()
        self.seed()
        self.sync('S1')
        with self.assertRaisesRegex(commission.CommissionError, 'insufficient available'):
            self.plan('P1', {'email': 1, 'not-configured': 1})
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plans'), [(0,)])

    def test_plan_ok_reserves_and_reports(self):
        self.init()
        self.seed()
        self.sync('S1')
        result = self.plan('P1', {'email': 80, 'sms': 30})
        self.assertFalse(result['replayed'])
        self.assertEqual(result['plan']['status'], 'active')
        self.assertEqual(result['plan']['total_cents'], 110)
        report = self.channel_report()
        self.assertEqual(report['email']['reserved_cents'], 80)
        self.assertEqual(report['email']['available_cents'], 20)
        self.assertEqual(report['sms']['available_cents'], 0)

    def test_plan_replay_and_conflicting_parameters(self):
        self.init()
        self.seed()
        self.sync('S1')
        first = self.plan('P1', {'email': 80, 'sms': 30})
        again = self.plan('P1', {'sms': 30, 'email': 80}, name='plan2.json')
        self.assertTrue(again['replayed'])
        self.assertEqual(first['plan']['plan_id'], again['plan']['plan_id'])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plans'), [(1,)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plan_allocations'), [(2,)])
        with self.assertRaisesRegex(commission.CommissionError, 'different allocations'):
            self.plan('P1', {'email': 80, 'sms': 29}, name='plan3.json')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM plan_allocations'), [(2,)])

    def test_plan_replay_after_expiry_returns_expired_status(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80})
        self._rewrite_source(PA={'revenue_cents': 0})
        self.sync('S2')
        replay = self.plan('P1', {'email': 80}, name='replay.json')
        self.assertTrue(replay['replayed'])
        self.assertEqual(replay['plan']['status'], 'expired')

    def test_competing_plans_respect_existing_reservations(self):
        self.init()
        self.seed()
        self.sync('S1')       # email 100
        self.plan('P1', {'email': 60})
        with self.assertRaisesRegex(commission.CommissionError, 'insufficient'):
            self.plan('P2', {'email': 50})  # only 40 free
        self.plan('P2', {'email': 40})
        with self.assertRaisesRegex(commission.CommissionError, 'insufficient'):
            self.plan('P3', {'email': 1})
        self.assertEqual(self.channel_report()['email']['reserved_cents'], 100)
        self.cancel('P1')
        self.plan('P3', {'email': 60})
        self.assertEqual(self.channel_report()['email']['reserved_cents'], 100)

    def test_confirm_is_idempotent_and_permanent(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80, 'sms': 30})
        first = self.confirm('P1')
        self.assertFalse(first['replayed'])
        self.assertEqual(first['payout']['total_cents'], 110)
        again = self.confirm('P1')
        self.assertTrue(again['replayed'])
        self.assertEqual(again['payout']['total_cents'], 110)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM payouts'), [(1,)])
        self.assertEqual(self.ledger_rows(
            'SELECT channel,amount_cents FROM payout_allocations ORDER BY channel'),
            [('email', 80), ('sms', 30)])
        report = self.channel_report()
        self.assertEqual(report['email']['paid_cents'], 80)
        self.assertEqual(report['email']['reserved_cents'], 0)
        self.assertEqual(report['email']['available_cents'], 20)
        with self.assertRaisesRegex(commission.CommissionError, 'cannot be cancelled'):
            self.cancel('P1')
        conn = sqlite3.connect(self.ledger)
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute('DELETE FROM payouts')
        conn.rollback()
        conn.close()

    def test_cancel_releases_and_is_idempotent(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80})
        first = self.cancel('P1')
        self.assertFalse(first['replayed'])
        self.assertEqual(first['plan']['status'], 'cancelled')
        again = self.cancel('P1')
        self.assertTrue(again['replayed'])
        self.assertEqual(self.channel_report()['email']['reserved_cents'], 0)
        with self.assertRaisesRegex(commission.CommissionError, 'cannot be confirmed'):
            self.confirm('P1')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM payouts'), [(0,)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])

    def test_expired_plan_cannot_be_confirmed_and_releases_reservation(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80, 'sms': 30})
        self._rewrite_source(PA={'revenue_cents': 0})
        summary = self.sync('S2')
        self.assertEqual(summary['expired_plans'], 1)
        plan = self.report()['plans'][0]
        self.assertEqual(plan['status'], 'expired')
        self.assertEqual(plan['resolution'], 'sync_invalidated')
        report = self.channel_report()
        self.assertEqual(report['sms']['reserved_cents'], 0)
        self.assertEqual(report['email']['reserved_cents'], 0)
        with self.assertRaisesRegex(commission.CommissionError, 'cannot be confirmed'):
            self.confirm('P1')
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM payouts'), [(0,)])
        # cancelling an already invalidated plan is an idempotent no-op
        released = self.cancel('P1')
        self.assertTrue(released['replayed'])
        self.assertEqual(released['plan']['status'], 'expired')

    def test_unknown_plan_ids_are_rejected(self):
        self.init()
        with self.assertRaisesRegex(commission.CommissionError, 'unknown payout plan'):
            self.confirm('NOPE')
        with self.assertRaisesRegex(commission.CommissionError, 'unknown payout plan'):
            self.cancel('NOPE')

    # -- report ----------------------------------------------------------------

    def test_report_conservation_fields(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80, 'sms': 30})
        self.plan('P2', {'email': 10})
        self.confirm('P2')
        report = self.report()
        self.assertEqual(report['source']['as_of'], '2026-01-05T00:00:00Z')
        self.assertEqual(report['last_sync']['sync_id'], 'S1')
        rows = {row['channel']: row for row in report['channels']}
        self.assertEqual(rows['email']['accrued_cents'], 100)
        self.assertEqual(rows['email']['paid_cents'], 10)
        self.assertEqual(rows['email']['reserved_cents'], 80)
        self.assertEqual(rows['email']['available_cents'], 10)
        self.assertEqual(rows['sms']['debt_cents'], 0)
        # each channel lists every plan that touches it, with its status
        self.assertEqual(rows['email']['plans'],
                         [{'plan_id': 'P1', 'status': 'active', 'amount_cents': 80},
                          {'plan_id': 'P2', 'status': 'confirmed', 'amount_cents': 10}])
        self.assertEqual(rows['sms']['plans'],
                         [{'plan_id': 'P1', 'status': 'active', 'amount_cents': 30}])
        self.assertTrue(report['conservation_ok'])
        statuses = {p['plan_id']: p['status'] for p in report['plans']}
        self.assertEqual(statuses, {'P1': 'active', 'P2': 'confirmed'})
        self.assertEqual(report['totals']['accrued_cents'], 130)
        self.assertEqual(report['totals']['paid_cents'], 10)
        self.assertEqual(report['totals']['reserved_cents'], 110)  # P1 across two channels

    def test_report_does_not_mutate_the_ledger(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80})
        before = self.ledger.read_bytes()
        first = self.report('r1.json')
        self.assertEqual(self.ledger.read_bytes(), before)
        second = self.report('r2.json')
        self.assertEqual((self.dir / 'r1.json').read_bytes(),
                         (self.dir / 'r2.json').read_bytes())
        self.assertEqual(first, second)

    def test_report_rejects_symlink_and_same_path(self):
        self.init()
        dest = self.dir / 'link.json'
        dest.symlink_to(self.dir / 'target.json')
        with self.assertRaises(ValueError):
            commission.report(self.ledger, dest)
        with self.assertRaises(ValueError):
            commission.report(self.ledger, self.ledger)

    def test_ledger_stores_no_user_or_touch_identifiers(self):
        self.init()
        self.seed()
        self.sync('S1')
        raw = self.ledger.read_bytes()
        for identifier in (b'user-alpha', b'user-beta', b'touch-email-1',
                           b'touch-sms-1'):
            self.assertNotIn(identifier, raw)
        self.assertIn(b'PA', raw)  # purchase ids are the rights identity

    def test_source_is_only_read_never_written(self):
        self.init()
        self.seed()
        before = self.db.read_bytes()
        self.sync('S1')
        self.plan('P1', {'email': 1})
        self.confirm('P1')
        self.assertEqual(self.db.read_bytes(), before)


class CommissionFaultTests(_CommissionBase):
    RATES = {'email': 1000, 'search': 1000, 'sms': 500}

    def test_confirm_post_commit_fault_is_a_replay_without_second_payment(self):
        self.init()
        self.seed()
        self.sync('S1')
        self.plan('P1', {'email': 80, 'sms': 30})
        # No fault injection exists on the confirm path (it would mask the
        # atomic conversion), so verify the equivalent post-commit retry: a
        # second confirm must be a replay and never create a second payout.
        self.confirm('P1')
        again = self.confirm('P1')
        self.assertTrue(again['replayed'])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM payouts'), [(1,)])

    def test_sync_post_commit_fault_retry_does_not_double_post(self):
        self.init()
        self.seed()
        os.environ['VA_COMMISSION_FAULT_AFTER_COMMIT'] = '1'
        with self.assertRaises(OSError):
            self.sync('S1')
        os.environ.pop('VA_COMMISSION_FAULT_AFTER_COMMIT')
        # the transaction committed before the failure: entries are durable
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM syncs'), [(1,)])
        again = self.sync('S1')
        self.assertTrue(again['replayed'])
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])
        self.assertEqual(self.channel_report()['email']['accrued_cents'], 100)
        # a retry from a fresh sync id at the same snapshot is also entry-free
        retry = self.sync('S2')
        self.assertFalse(retry['replayed'])
        self.assertEqual(retry['entries'], 0)
        self.assertEqual(self.ledger_rows('SELECT COUNT(*) FROM entries'), [(2,)])

    def test_report_output_failure_does_not_touch_ledger(self):
        self.init()
        self.seed()
        self.sync('S1')
        before = self.ledger.read_bytes()
        dest_dir = self.dir / 'dest-dir'
        dest_dir.mkdir()
        with self.assertRaises(OSError):
            commission.report(self.ledger, dest_dir)
        self.assertEqual(self.ledger.read_bytes(), before)
        # retry works and matches the report built directly
        summary = commission.report(self.ledger, self.dir / 'r.json')
        self.assertTrue(summary['conservation_ok'])
        self.assertEqual(json.loads((self.dir / 'r.json').read_text())['totals']
                         ['accrued_cents'], 130)


class CommissionProcessTests(unittest.TestCase):
    """Real cross-process interleavings of commission-sync and payout-confirm."""

    RATES = {'email': 1000, 'search': 1000}

    def setUp(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'commission.db'
        self.barriers = self.dir / 'barriers'
        self.barriers.mkdir()
        self.env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        self.apply('b1.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('touch-email-1', 'user-alpha', '2026-01-01T00:00:00Z', 'email'),
            purchase('PA', 'user-alpha', '2026-01-01T00:10:00Z', 1000),
            touch('touch-search-1', 'user-beta', '2026-01-02T00:00:00Z', 'search'),
            purchase('PB', 'user-beta', '2026-01-02T00:10:00Z', 600),
        ]))
        self.write_json('rates.json', {'rates': self.RATES})
        self.run_ok('commission-init', '--ledger', str(self.ledger),
                    '--rates', str(self.dir / 'rates.json'))
        self.run_ok('commission-sync', '--source', str(self.db), '--ledger', str(self.ledger),
                    '--sync-id', 'S1')
        self.write_json('plan.json', {'plan_id': 'P1',
                                      'amounts': {'email': 80, 'search': 40}})
        self.run_ok('payout-plan', '--ledger', str(self.ledger),
                    '--request', str(self.dir / 'plan.json'))

    def tearDown(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td.cleanup()

    def write_json(self, name, payload):
        (self.dir / name).write_text(json.dumps(payload))

    def apply(self, name, payload):
        source = self.dir / name
        source.write_text(json.dumps(payload))
        return pipeline.apply_batch(self.db, source)

    def reassign_a_to_b(self):
        # A late touch inside PA's lookback window makes search the last
        # channel; the pipeline rewrites the historical attribution.
        self.apply('b2.json', batch('B2', '2026-01-06T00:00:00Z', [
            touch('touch-search-2', 'user-alpha', '2026-01-01T00:05:00Z', 'search')]))
        result = pipeline.build_result(pipeline.connect(self.db))
        channels = {c['id']: c['models']['last_non_direct']['channel']
                    for c in result['conversions']}
        self.assertEqual(channels, {'PA': 'search', 'PB': 'search'})

    def run_cli(self, *args, env=None):
        merged = dict(self.env)
        if env:
            merged.update(env)
        return subprocess.run([sys.executable, '-m', 'visit_attribution.cli', *args],
                              env=merged, capture_output=True, text=True, timeout=60)

    def run_ok(self, *args, env=None):
        result = self.run_cli(*args, env=env)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def start(self, *args, env=None):
        merged = dict(self.env)
        if env:
            merged.update(env)
        return subprocess.Popen([sys.executable, '-m', 'visit_attribution.cli', *args],
                                env=merged, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, text=True)

    def drain(self, proc):
        out, err = proc.communicate(timeout=60)
        return proc.returncode, out, err

    def wait_for(self, name, timeout=30):
        path = self.barriers / name
        deadline = time.time() + timeout
        while not path.exists():
            if time.time() > deadline:
                raise AssertionError('timed out waiting for barrier ' + name)
            time.sleep(0.02)

    def signal(self, name):
        (self.barriers / name).write_text('1')

    def report(self, name='report.json'):
        out = self.dir / name
        self.run_ok('commission-report', '--ledger', str(self.ledger), '--output', str(out))
        return json.loads(out.read_text())

    def channel(self, report, channel):
        return {row['channel']: row for row in report['channels']}[channel]

    # -- interleavings ---------------------------------------------------------

    def test_sync_commits_first_expires_plan_and_releases_both_reservations(self):
        self.reassign_a_to_b()
        sync = self.start('commission-sync', '--source', str(self.db),
                          '--ledger', str(self.ledger), '--sync-id', 'S2',
                          env={'VA_COMMISSION_TX_BARRIER': str(self.barriers)})
        self.wait_for('sync-tx-applied')
        # sync owns the ledger write transaction; confirm must block, not race
        confirm = self.start('payout-confirm', '--ledger', str(self.ledger),
                             '--plan-id', 'P1')
        time.sleep(0.6)
        self.assertIsNone(confirm.poll())          # blocked on the writer lock
        self.signal('sync-tx-go')
        code, out, err = self.drain(sync)
        self.assertEqual(code, 0, err)
        self.assertFalse(json.loads(out)['replayed'])
        code, out, err = self.drain(confirm)
        self.assertEqual(code, 2, err)             # expired: never paid
        self.assertIn('cannot be confirmed', err)
        report = self.report()
        self.assertEqual(self.channel(report, 'email')['accrued_cents'], 0)
        self.assertEqual(self.channel(report, 'email')['reserved_cents'], 0)
        self.assertEqual(self.channel(report, 'search')['accrued_cents'], 160)
        self.assertEqual(self.channel(report, 'search')['reserved_cents'], 0)
        plan = report['plans'][0]
        self.assertEqual(plan['status'], 'expired')
        self.assertEqual(plan['resolution'], 'sync_invalidated')
        # sync credential and entries committed once
        self.assertEqual(self.count('syncs'), 2)
        self.assertEqual(report['totals']['paid_cents'], 0)
        self.assertTrue(report['conservation_ok'])

    def test_confirm_commits_first_keeps_payment_and_forms_debt(self):
        self.reassign_a_to_b()
        sync = self.start('commission-sync', '--source', str(self.db),
                          '--ledger', str(self.ledger), '--sync-id', 'S2',
                          env={'VA_COMMISSION_SNAPSHOT_BARRIER': str(self.barriers)})
        self.wait_for('snapshot-read')             # snapshot taken, no lock yet
        code, out, err = self.drain(self.start(
            'payout-confirm', '--ledger', str(self.ledger), '--plan-id', 'P1'))
        self.assertEqual(code, 0, err)
        self.assertFalse(json.loads(out)['replayed'])
        self.signal('sync-proceed')
        code, out, err = self.drain(sync)
        self.assertEqual(code, 0, err)
        report = self.report()
        email = self.channel(report, 'email')
        search = self.channel(report, 'search')
        self.assertEqual(email['accrued_cents'], 0)
        self.assertEqual(email['paid_cents'], 80)
        self.assertEqual(email['debt_cents'], 80)          # paid is never undone
        self.assertEqual(email['available_cents'], 0)
        self.assertEqual(search['accrued_cents'], 160)
        self.assertEqual(search['paid_cents'], 40)
        self.assertEqual(search['available_cents'], 120)
        self.assertEqual(report['plans'][0]['status'], 'confirmed')
        # confirmed is idempotent and cannot be cancelled
        confirm_again = self.run_ok('payout-confirm', '--ledger', str(self.ledger),
                                    '--plan-id', 'P1')
        self.assertTrue(confirm_again['replayed'])
        cancel = self.run_cli('payout-cancel', '--ledger', str(self.ledger),
                              '--plan-id', 'P1')
        self.assertEqual(cancel.returncode, 2)
        # later positive commission offsets the debt before being reservable
        self.apply('b3.json', batch('B3', '2026-01-07T00:00:00Z', [
            touch('touch-email-3', 'user-gamma', '2026-01-03T00:00:00Z', 'email'),
            purchase('PC', 'user-gamma', '2026-01-03T00:10:00Z', 500)]))
        self.run_ok('commission-sync', '--source', str(self.db), '--ledger', str(self.ledger),
                    '--sync-id', 'S3')
        report = self.report('report2.json')
        email = self.channel(report, 'email')
        self.assertEqual(email['accrued_cents'], 50)
        self.assertEqual(email['paid_cents'], 80)
        self.assertEqual(email['debt_cents'], 30)
        self.assertEqual(email['available_cents'], 0)
        self.write_json('plan2.json', {'plan_id': 'P2', 'amounts': {'email': 1}})
        refused = self.run_cli('payout-plan', '--ledger', str(self.ledger),
                               '--request', str(self.dir / 'plan2.json'))
        self.assertEqual(refused.returncode, 2)
        self.assertIn('insufficient available commission', refused.stderr)
        self.assertEqual(self.count('plans'), 1)
        self.assertTrue(report['conservation_ok'])

    def count(self, table):
        conn = sqlite3.connect(self.ledger)
        try:
            return conn.execute('SELECT COUNT(*) FROM %s' % table).fetchone()[0]
        finally:
            conn.close()

    # -- competing reservations across processes -------------------------------

    def test_competing_plans_across_processes_reserve_without_overcommit(self):
        # P1 already reserves 80 of email's 100, leaving 20. Both new plans fit
        # individually (20 <= 20) but not together; the ledger write transaction
        # must let exactly one through, never both.
        self.write_json('planA.json', {'plan_id': 'PL-A', 'amounts': {'email': 20}})
        self.write_json('planB.json', {'plan_id': 'PL-B', 'amounts': {'email': 20}})
        procs = [
            self.start('payout-plan', '--ledger', str(self.ledger),
                       '--request', str(self.dir / name))
            for name in ('planA.json', 'planB.json')]
        codes = []
        for proc in procs:
            code, out, err = self.drain(proc)
            codes.append((code, out, err))
        winners = [out for code, out, err in codes if code == 0]
        losers = [err for code, out, err in codes if code != 0]
        self.assertEqual(len(winners), 1, codes)
        self.assertEqual(len(losers), 1, codes)
        self.assertEqual(self.count('plans'), 2)  # P1 plus exactly one new plan
        report = self.report()
        self.assertEqual(self.channel(report, 'email')['reserved_cents'], 100)
        self.assertEqual(self.channel(report, 'email')['available_cents'], 0)
        self.assertLessEqual(self.channel(report, 'email')['reserved_cents'],
                             self.channel(report, 'email')['accrued_cents'])
        self.assertTrue(report['conservation_ok'])

    # -- SIGKILL ---------------------------------------------------------------

    def _kill_blocked_process(self, args, env, barrier_name):
        proc = subprocess.Popen([sys.executable, '-m', 'visit_attribution.cli', *args],
                                env=dict(self.env, **env),
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
                                start_new_session=True)
        try:
            self.wait_for(barrier_name)
            time.sleep(0.3)
            os.killpg(proc.pid, 9)
            proc.wait(10)
        finally:
            if proc.poll() is None:
                os.killpg(proc.pid, 9)
                proc.wait(10)

    def test_sigkill_mid_sync_rolls_back_entries_rights_plans_and_credential(self):
        self.reassign_a_to_b()
        before = {
            'entries': self.dump('SELECT seq,purchase_id,channel,amount_cents,kind,sync_id '
                                 'FROM entries ORDER BY seq'),
            'rights': self.dump('SELECT purchase_id,channel,revenue_cents,commission_cents,'
                                'sync_id FROM rights ORDER BY purchase_id'),
            'plans': self.dump('SELECT plan_id,status,total_cents FROM plans ORDER BY plan_id'),
            'syncs': self.dump('SELECT sync_id FROM syncs ORDER BY sync_id'),
            'state': self.dump('SELECT as_of,generation,content_sha256 FROM source_state'),
        }
        self._kill_blocked_process(
            ('commission-sync', '--source', str(self.db), '--ledger', str(self.ledger),
             '--sync-id', 'S2'),
            {'VA_COMMISSION_TX_BARRIER': str(self.barriers)}, 'sync-tx-applied')
        after = {
            'entries': self.dump('SELECT seq,purchase_id,channel,amount_cents,kind,sync_id '
                                 'FROM entries ORDER BY seq'),
            'rights': self.dump('SELECT purchase_id,channel,revenue_cents,commission_cents,'
                                'sync_id FROM rights ORDER BY purchase_id'),
            'plans': self.dump('SELECT plan_id,status,total_cents FROM plans ORDER BY plan_id'),
            'syncs': self.dump('SELECT sync_id FROM syncs ORDER BY sync_id'),
            'state': self.dump('SELECT as_of,generation,content_sha256 FROM source_state'),
        }
        self.assertEqual(after, before)   # no half-applied sync, no expiry
        self.assertEqual(after['plans'], [('P1', 'active', 120)])
        # retry completes cleanly and is accounted exactly once
        result = self.run_ok('commission-sync', '--source', str(self.db),
                             '--ledger', str(self.ledger), '--sync-id', 'S2')
        self.assertFalse(result['replayed'])
        report = self.report()
        self.assertEqual(report['totals']['accrued_cents'], 160)
        # 100 + 60 grants, then PA's 100 reversal and 160 re-grant
        self.assertEqual(report['totals']['entries'], 4)
        self.assertEqual(report['plans'][0]['status'], 'expired')
        self.assertTrue(report['conservation_ok'])
        self.assertEqual(self.count('syncs'), 2)

    def test_sigkill_mid_confirm_leaves_plan_active_and_retry_pays_once(self):
        totals_before = self.dump('SELECT channel,SUM(amount_cents) FROM entries '
                                  'GROUP BY channel ORDER BY channel')
        self._kill_blocked_process(
            ('payout-confirm', '--ledger', str(self.ledger), '--plan-id', 'P1'),
            {'VA_COMMISSION_TX_BARRIER': str(self.barriers)}, 'confirm-tx-applied')
        self.assertEqual(self.dump('SELECT status FROM plans WHERE plan_id=?', ('P1',)),
                         [('active',)])
        self.assertEqual(self.count('payouts'), 0)
        self.assertEqual(self.count('payout_allocations'), 0)
        self.assertEqual(self.dump('SELECT channel,SUM(amount_cents) FROM entries '
                                   'GROUP BY channel ORDER BY channel'), totals_before)
        first = self.run_ok('payout-confirm', '--ledger', str(self.ledger), '--plan-id', 'P1')
        self.assertFalse(first['replayed'])
        again = self.run_ok('payout-confirm', '--ledger', str(self.ledger), '--plan-id', 'P1')
        self.assertTrue(again['replayed'])
        self.assertEqual(self.count('payouts'), 1)
        report = self.report()
        self.assertEqual(self.channel(report, 'email')['paid_cents'], 80)
        self.assertEqual(self.channel(report, 'search')['paid_cents'], 40)
        self.assertTrue(report['conservation_ok'])

    def dump(self, sql, params=()):
        conn = sqlite3.connect(self.ledger)
        try:
            return conn.execute(sql, params).fetchall()
        finally:
            conn.close()


class CommissionCliTests(unittest.TestCase):
    """CLI acceptance: every command, exit codes and unchanged legacy paths."""

    def setUp(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td = tempfile.TemporaryDirectory()
        self.dir = Path(self.td.name)
        self.db = self.dir / 'store.db'
        self.ledger = self.dir / 'commission.db'
        self.env = dict(os.environ, PYTHONPATH=str(ROOT / 'src'))
        self.write('b1.json', batch('B1', '2026-01-05T00:00:00Z', [
            touch('t1', 'u1', '2026-01-01T00:00:00Z', 'email'),
            purchase('PA', 'u1', '2026-01-01T00:10:00Z', 1000),
            touch('t2', 'u2', '2026-01-02T00:00:00Z', 'search'),
            purchase('PB', 'u2', '2026-01-02T00:10:00Z', 600)]))
        self.write('rates.json', {'rates': {'email': 1000, 'search': 1000}})
        self.write('plan.json', {'plan_id': 'PL-1', 'amounts': {'email': 80, 'search': 40}})
        self.run_ok('apply', '--db', str(self.db), '--source', str(self.dir / 'b1.json'))

    def tearDown(self):
        for var in ('VA_COMMISSION_FAULT_AFTER_COMMIT', 'VA_COMMISSION_SNAPSHOT_BARRIER',
                    'VA_COMMISSION_TX_BARRIER'):
            os.environ.pop(var, None)
        commission._snapshot_hook = None
        self.td.cleanup()

    def write(self, name, payload):
        (self.dir / name).write_text(json.dumps(payload))

    def run_cli(self, *args):
        return subprocess.run([sys.executable, '-m', 'visit_attribution.cli', *args],
                              env=self.env, capture_output=True, text=True, timeout=60)

    def run_ok(self, *args):
        result = self.run_cli(*args)
        self.assertEqual(result.returncode, 0, result.stderr)
        return json.loads(result.stdout)

    def test_all_six_commands_and_error_exit_codes(self):
        init = self.run_ok('commission-init', '--ledger', str(self.ledger),
                           '--rates', str(self.dir / 'rates.json'))
        self.assertEqual(init['channels_configured'], 2)
        again = self.run_cli('commission-init', '--ledger', str(self.ledger),
                            '--rates', str(self.dir / 'rates.json'))
        self.assertEqual(again.returncode, 2)
        bad_rates = self.dir / 'bad-rates.json'
        bad_rates.write_text(json.dumps({'rates': {'email': 20000}}))
        self.assertEqual(self.run_cli('commission-init', '--ledger',
                                      str(self.dir / 'x.db'),
                                      '--rates', str(bad_rates)).returncode, 2)

        sync = self.run_ok('commission-sync', '--source', str(self.db),
                           '--ledger', str(self.ledger), '--sync-id', 'S1')
        self.assertEqual(sync['entries'], 2)
        replay = self.run_ok('commission-sync', '--source', str(self.db),
                             '--ledger', str(self.ledger), '--sync-id', 'S1')
        self.assertTrue(replay['replayed'])
        refused = self.run_cli('commission-sync', '--source', str(self.dir / 'nope.db'),
                               '--ledger', str(self.ledger), '--sync-id', 'S2')
        self.assertEqual(refused.returncode, 2)

        plan = self.run_ok('payout-plan', '--ledger', str(self.ledger),
                           '--request', str(self.dir / 'plan.json'))
        self.assertEqual(plan['plan']['total_cents'], 120)
        self.write('short.json', {'plan_id': 'PL-2', 'amounts': {'email': 999}})
        short = self.run_cli('payout-plan', '--ledger', str(self.ledger),
                             '--request', str(self.dir / 'short.json'))
        self.assertEqual(short.returncode, 2)
        self.assertIn('insufficient available commission', short.stderr)
        self.write('bad-plan.json', {'plan_id': 'PL-3', 'amounts': {'email': 0}})
        self.assertEqual(self.run_cli('payout-plan', '--ledger', str(self.ledger),
                                      '--request', str(self.dir / 'bad-plan.json'))
                         .returncode, 2)

        confirm = self.run_ok('payout-confirm', '--ledger', str(self.ledger),
                              '--plan-id', 'PL-1')
        self.assertEqual(confirm['payout']['total_cents'], 120)
        self.assertTrue(self.run_ok('payout-confirm', '--ledger', str(self.ledger),
                                    '--plan-id', 'PL-1')['replayed'])
        self.assertEqual(self.run_cli('payout-cancel', '--ledger', str(self.ledger),
                                      '--plan-id', 'PL-1').returncode, 2)

        out = self.dir / 'report.json'
        summary = self.run_ok('commission-report', '--ledger', str(self.ledger),
                              '--output', str(out))
        self.assertTrue(summary['conservation_ok'])
        report = json.loads(out.read_text())
        self.assertEqual(report['totals']['paid_cents'], 120)
        self.assertEqual(report['plans'][0]['status'], 'confirmed')
        # report output may not clobber the ledger
        self.assertEqual(self.run_cli('commission-report', '--ledger', str(self.ledger),
                                      '--output', str(self.ledger)).returncode, 2)

    def test_cancel_path_through_cli(self):
        self.run_ok('commission-init', '--ledger', str(self.ledger),
                    '--rates', str(self.dir / 'rates.json'))
        self.run_ok('commission-sync', '--source', str(self.db),
                    '--ledger', str(self.ledger), '--sync-id', 'S1')
        self.run_ok('payout-plan', '--ledger', str(self.ledger),
                    '--request', str(self.dir / 'plan.json'))
        first = self.run_ok('payout-cancel', '--ledger', str(self.ledger),
                            '--plan-id', 'PL-1')
        self.assertFalse(first['replayed'])
        self.assertEqual(first['plan']['status'], 'cancelled')
        self.assertTrue(self.run_ok('payout-cancel', '--ledger', str(self.ledger),
                                    '--plan-id', 'PL-1')['replayed'])
        self.assertEqual(self.run_cli('payout-confirm', '--ledger', str(self.ledger),
                                      '--plan-id', 'PL-1').returncode, 2)
        out = self.dir / 'report.json'
        self.run_ok('commission-report', '--ledger', str(self.ledger), '--output', str(out))
        report = json.loads(out.read_text())
        self.assertEqual(report['totals']['reserved_cents'], 0)
        self.assertEqual(report['plans'][0]['status'], 'cancelled')

    def test_existing_commands_still_work_and_source_is_unchanged(self):
        before = self.db.read_bytes()
        self.run_ok('commission-init', '--ledger', str(self.ledger),
                    '--rates', str(self.dir / 'rates.json'))
        self.run_ok('commission-sync', '--source', str(self.db),
                    '--ledger', str(self.ledger), '--sync-id', 'S1')
        self.assertEqual(self.db.read_bytes(), before)
        export = self.run_ok('export', '--db', str(self.db),
                             '--output', str(self.dir / 'export.json'))
        self.assertEqual(export['conversion_count'], 2)
        legacy = self.run_ok('--source', str(ROOT / 'fixtures/input.json'),
                             '--output', str(self.dir / 'legacy.json'))
        self.assertEqual(legacy['result']['conversion_count'], 3)
        self.run_ok('privacy-init', '--ledger', str(self.dir / 'privacy.db'),
                    '--budget', '10')
        # an apply after sync does not modify the commission ledger
        ledger_before = self.ledger.read_bytes()
        self.write('b2.json', batch('B2', '2026-01-06T00:00:00Z', [
            touch('t9', 'u9', '2026-01-06T00:00:00Z', 'email')]))
        self.run_ok('apply', '--db', str(self.db), '--source', str(self.dir / 'b2.json'))
        self.assertEqual(self.ledger.read_bytes(), ledger_before)
